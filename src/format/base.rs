// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>
//
//! `graph.base` — the immutable snapshot. Layout:
//!
//! ```text
//! [Header 128 B][DirEntry × N, 32 B each][section bytes, 64-aligned]...[trailer 8 B]
//! ```
//!
//! Fixed-width, sorted record tables make every lookup a zero-copy binary
//! search directly against the mmap; the label heap holds all variable-width
//! strings. Section discriminants are burned: additive evolution happens via
//! new section kinds (unknown kinds are skipped on read) and header flags,
//! never by reinterpreting existing bytes.

use crate::bytesio::{self, align_up, get_u16, get_u32, get_u64, put_u16, put_u32, put_u64, Reader};
use crate::error::{Error, Result};
use crate::format::{ALIGN, DIR_ENTRY_LEN, TRAILER_LEN};
use crate::key::UrlKey;
use xxhash_rust::xxh3::xxh3_64;

/// Magic: `"GRPR"` little-endian.
pub const MAGIC: u32 = 0x5250_5247;
/// Format version; strict-equal on read.
pub const VERSION: u16 = 1;
/// Header width.
pub const HEADER_LEN: usize = 128;

/// Node record width.
pub const NODE_LEN: usize = 72;
/// Segment record width.
pub const SEG_LEN: usize = 24;
/// Edge record width.
pub const EDGE_LEN: usize = 8;
/// Lexicon record width.
pub const LEX_LEN: usize = 24;
/// Rank record width.
pub const RANK_LEN: usize = 8;

/// Sentinel for "no label" in a label-offset field.
pub const NO_LABEL: u32 = u32::MAX;

/// Node flags.
pub mod nflags {
    /// Survives eviction sweeps and is exempt from TTL-driven eviction.
    pub const PINNED: u16 = 1;
    /// Known only as an edge target; never ingested (no URL label yet).
    pub const STUB: u16 = 2;
    /// Observed gone at the source; retained for history, excluded from
    /// due-lists and default query results.
    pub const TOMBSTONE: u16 = 4;
}

/// Edge flags.
pub mod eflags {
    /// Set = enrichment tier (survives crawl-tier replacement); clear = crawl tier.
    pub const TIER_ENRICH: u8 = 1;
    /// Set = inferred (heuristic/enrichment); clear = extracted from source.
    pub const INFERRED: u8 = 2;
}

/// Segment flags.
pub mod sflags {
    /// The byte range is unknown (heading-path anchor only).
    pub const NO_RANGE: u8 = 1;
    /// Importance was set by an enricher; preserved across re-ingest while
    /// the owning document's content hash is unchanged.
    pub const LLM_SCORED: u8 = 2;
}

/// Section kinds (discriminants are burned; do not renumber).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u16)]
pub enum SectionKind {
    /// Fixed 72-B node records sorted by `url_key`.
    Nodes = 1,
    /// Fixed 24-B segment records grouped per node in document order.
    Segs = 2,
    /// CSR offsets: `(node_count + 1) × u32` into `Edges`.
    EdgeIndex = 3,
    /// Fixed 8-B edge records, per-node runs sorted by destination id.
    Edges = 4,
    /// Length-prefixed UTF-8 label heap.
    Labels = 5,
    /// Fixed 24-B token records sorted by `token_hash`.
    Lexicon = 6,
    /// LEB128 delta-encoded ascending node-id postings.
    Postings = 7,
    /// Fixed 8-B per-node rank/community records (parallel to `Nodes`).
    Ranks = 8,
}

/// Parsed header of a base file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Header {
    /// Feature flags (none defined in v1; must round-trip).
    pub flags: u16,
    /// Number of directory entries.
    pub section_count: u32,
    /// Node record count.
    pub node_count: u32,
    /// Segment record count.
    pub seg_count: u32,
    /// Edge record count.
    pub edge_count: u32,
    /// Lexicon record count.
    pub token_count: u32,
    /// Monotonic snapshot generation (bumped each compaction).
    pub generation: u64,
    /// Highest WAL op sequence folded into this base.
    pub wal_applied_seq: u64,
    /// Total file length including trailer.
    pub total_len: u64,
    /// Wall-clock creation stamp (informational).
    pub created_at_ms: u64,
}

impl Header {
    fn write_into(&self, out: &mut Vec<u8>) {
        debug_assert!(out.is_empty());
        put_u32(out, MAGIC);
        put_u16(out, VERSION);
        put_u16(out, self.flags);
        put_u32(out, HEADER_LEN as u32);
        put_u32(out, self.section_count);
        put_u32(out, self.node_count);
        put_u32(out, self.seg_count);
        put_u32(out, self.edge_count);
        put_u32(out, self.token_count);
        put_u64(out, self.generation);
        put_u64(out, self.wal_applied_seq);
        put_u64(out, self.total_len);
        put_u64(out, self.created_at_ms);
        // Reserved: pad to the checksum slot; must be zero for v1 readers.
        out.resize(HEADER_LEN - 8, 0);
        let sum = xxh3_64(&out[..HEADER_LEN - 8]);
        put_u64(out, sum);
    }

    fn parse(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < HEADER_LEN {
            return Err(Error::format("file shorter than header"));
        }
        if get_u32(bytes, 0) != MAGIC {
            return Err(Error::format("bad magic"));
        }
        let version = get_u16(bytes, 4);
        if version != VERSION {
            return Err(Error::format(format!("unsupported version {version}")));
        }
        let stored = get_u64(bytes, HEADER_LEN - 8);
        if xxh3_64(&bytes[..HEADER_LEN - 8]) != stored {
            return Err(Error::format("header checksum mismatch"));
        }
        if get_u32(bytes, 8) as usize != HEADER_LEN {
            return Err(Error::format("unexpected header length"));
        }
        // Reserved region must be zero so future writers can claim it.
        if bytes[64..HEADER_LEN - 8].iter().any(|&b| b != 0) {
            return Err(Error::format("reserved header bytes not zero"));
        }
        Ok(Self {
            flags: get_u16(bytes, 6),
            section_count: get_u32(bytes, 12),
            node_count: get_u32(bytes, 16),
            seg_count: get_u32(bytes, 20),
            edge_count: get_u32(bytes, 24),
            token_count: get_u32(bytes, 28),
            generation: get_u64(bytes, 32),
            wal_applied_seq: get_u64(bytes, 40),
            total_len: get_u64(bytes, 48),
            created_at_ms: get_u64(bytes, 56),
        })
    }
}

/// Serializes a base file: sections are appended, then `finish` lays out the
/// directory, aligns, and appends the whole-file trailer.
#[derive(Debug)]
pub struct BaseWriter {
    header: Header,
    sections: Vec<(SectionKind, Vec<u8>)>,
}

impl BaseWriter {
    /// Start a writer for the given header (counts must be filled by caller;
    /// `section_count` and `total_len` are computed at `finish`).
    #[must_use]
    pub fn new(header: Header) -> Self {
        Self { header, sections: Vec::new() }
    }

    /// Append one section.
    pub fn add_section(&mut self, kind: SectionKind, bytes: Vec<u8>) {
        self.sections.push((kind, bytes));
    }

    /// Produce the final file bytes.
    #[must_use]
    pub fn finish(mut self) -> Vec<u8> {
        let dir_end = HEADER_LEN + self.sections.len() * DIR_ENTRY_LEN;
        let mut offset = align_up(dir_end, ALIGN);
        let mut dir = Vec::with_capacity(self.sections.len() * DIR_ENTRY_LEN);
        for (kind, bytes) in &self.sections {
            put_u16(&mut dir, *kind as u16);
            put_u16(&mut dir, 0);
            put_u32(&mut dir, 0);
            put_u64(&mut dir, offset as u64);
            put_u64(&mut dir, bytes.len() as u64);
            put_u64(&mut dir, xxh3_64(bytes));
            offset = align_up(offset + bytes.len(), ALIGN);
        }
        let total_len = offset + TRAILER_LEN;
        self.header.section_count = self.sections.len() as u32;
        self.header.total_len = total_len as u64;

        let mut out = Vec::with_capacity(total_len);
        self.header.write_into(&mut out);
        out.extend_from_slice(&dir);
        for (_, bytes) in &self.sections {
            out.resize(align_up(out.len(), ALIGN), 0);
            out.extend_from_slice(bytes);
        }
        out.resize(align_up(out.len(), ALIGN), 0);
        let trailer = xxh3_64(&out);
        let mut t = Vec::with_capacity(8);
        put_u64(&mut t, trailer);
        out.extend_from_slice(&t);
        debug_assert_eq!(out.len(), total_len);
        out
    }
}

/// Parsed directory of a validated base file: the header plus each section's
/// byte range. Holds offsets, not borrows, so the owner can keep it alongside
/// the mmap it describes.
#[derive(Clone, Debug)]
pub struct BaseDir {
    /// The validated header.
    pub header: Header,
    sections: Vec<(u16, usize, usize)>,
}

impl BaseDir {
    /// Validate `bytes` end to end (magic, version, checksums, directory
    /// bounds and alignment, per-section checksums, trailer) and return the
    /// directory. This is the fuzz entry point: any byte string must produce
    /// `Ok` or `Error::Format`, never a panic.
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        let header = Header::parse(bytes)?;
        if header.total_len != bytes.len() as u64 {
            return Err(Error::format("total_len does not match file size"));
        }
        if bytes.len() < HEADER_LEN + TRAILER_LEN {
            return Err(Error::format("file too short for trailer"));
        }
        let trailer_off = bytes.len() - TRAILER_LEN;
        if xxh3_64(&bytes[..trailer_off]) != get_u64(bytes, trailer_off) {
            return Err(Error::format("file trailer checksum mismatch"));
        }
        let count = header.section_count as usize;
        let dir_end = HEADER_LEN
            .checked_add(count.checked_mul(DIR_ENTRY_LEN).ok_or_else(|| Error::format("dir overflow"))?)
            .ok_or_else(|| Error::format("dir overflow"))?;
        if dir_end > trailer_off {
            return Err(Error::format("directory exceeds file"));
        }
        let mut sections = Vec::with_capacity(count.min(bytes.len() / DIR_ENTRY_LEN + 1));
        for i in 0..count {
            let e = HEADER_LEN + i * DIR_ENTRY_LEN;
            let kind = get_u16(bytes, e);
            let offset = get_u64(bytes, e + 8) as usize;
            let length = get_u64(bytes, e + 16) as usize;
            let sum = get_u64(bytes, e + 24);
            let end = offset.checked_add(length).ok_or_else(|| Error::format("section overflow"))?;
            if offset < dir_end || end > trailer_off {
                return Err(Error::format("section out of bounds"));
            }
            if offset % ALIGN != 0 {
                return Err(Error::format("section misaligned"));
            }
            if xxh3_64(&bytes[offset..end]) != sum {
                return Err(Error::format("section checksum mismatch"));
            }
            sections.push((kind, offset, length));
        }
        let dir = Self { header, sections };
        dir.check_counts(bytes)?;
        Ok(dir)
    }

    /// Cross-check declared counts against actual section byte lengths so
    /// record accessors can index without per-access bounds errors.
    fn check_counts(&self, bytes: &[u8]) -> Result<()> {
        let n = self.header.node_count as usize;
        let expect = |kind, want: usize, name: &str| -> Result<()> {
            match self.section(bytes, kind) {
                Some(s) if s.len() == want => Ok(()),
                None if want == 0 => Ok(()),
                Some(_) => Err(Error::format(format!("{name} section length mismatch"))),
                None => Err(Error::format(format!("missing {name} section"))),
            }
        };
        expect(SectionKind::Nodes, n * NODE_LEN, "nodes")?;
        expect(SectionKind::Segs, self.header.seg_count as usize * SEG_LEN, "segs")?;
        expect(SectionKind::Edges, self.header.edge_count as usize * EDGE_LEN, "edges")?;
        expect(SectionKind::Lexicon, self.header.token_count as usize * LEX_LEN, "lexicon")?;
        if n > 0 {
            expect(SectionKind::EdgeIndex, (n + 1) * 4, "edge index")?;
            expect(SectionKind::Ranks, n * RANK_LEN, "ranks")?;
        }
        // Every label offset must resolve inside the labels heap; walking all
        // records here keeps NodeRec/SegRec accessors infallible.
        let labels = self.section(bytes, SectionKind::Labels).unwrap_or(&[]);
        let check_label = |off: u32| -> Result<()> {
            if off == NO_LABEL {
                return Ok(());
            }
            let off = off as usize;
            if off + 2 > labels.len() {
                return Err(Error::format("label offset out of bounds"));
            }
            let len = usize::from(get_u16(labels, off));
            if off + 2 + len > labels.len() {
                return Err(Error::format("label extends past heap"));
            }
            std::str::from_utf8(&labels[off + 2..off + 2 + len])
                .map_err(|_| Error::format("label not utf-8"))?;
            Ok(())
        };
        let nodes = self.section(bytes, SectionKind::Nodes).unwrap_or(&[]);
        for i in 0..n {
            let r = NodeRec(&nodes[i * NODE_LEN..(i + 1) * NODE_LEN]);
            for off in [r.url_off(), r.title_off(), r.snippet_off(), r.etag_off()] {
                check_label(off)?;
            }
            let (s, c) = (r.seg_start() as usize, usize::from(r.seg_count()));
            if s + c > self.header.seg_count as usize {
                return Err(Error::format("segment run out of bounds"));
            }
        }
        let segs = self.section(bytes, SectionKind::Segs).unwrap_or(&[]);
        for i in 0..self.header.seg_count as usize {
            check_label(SegRec(&segs[i * SEG_LEN..(i + 1) * SEG_LEN]).label_off())?;
        }
        // Edge destinations and CSR monotonicity.
        if n > 0 {
            let idx = self.section(bytes, SectionKind::EdgeIndex).unwrap_or(&[]);
            let mut prev = 0u32;
            for i in 0..=n {
                let v = get_u32(idx, i * 4);
                if v < prev || v > self.header.edge_count {
                    return Err(Error::format("edge index not monotone"));
                }
                prev = v;
            }
            if prev != self.header.edge_count {
                return Err(Error::format("edge index does not cover edges"));
            }
            let edges = self.section(bytes, SectionKind::Edges).unwrap_or(&[]);
            for i in 0..self.header.edge_count as usize {
                if get_u32(edges, i * EDGE_LEN) >= self.header.node_count {
                    return Err(Error::format("edge destination out of range"));
                }
            }
        }
        // Lexicon postings ranges + sortedness by token hash.
        let postings = self.section(bytes, SectionKind::Postings).unwrap_or(&[]);
        let lex = self.section(bytes, SectionKind::Lexicon).unwrap_or(&[]);
        let mut prev_hash = None;
        for i in 0..self.header.token_count as usize {
            let r = LexRec(&lex[i * LEX_LEN..(i + 1) * LEX_LEN]);
            if let Some(p) = prev_hash {
                if r.token_hash() <= p {
                    return Err(Error::format("lexicon not sorted"));
                }
            }
            prev_hash = Some(r.token_hash());
            let (o, l) = (r.postings_off() as usize, r.postings_len() as usize);
            if o.checked_add(l).is_none_or(|e| e > postings.len()) {
                return Err(Error::format("postings range out of bounds"));
            }
            check_label(r.label_off())?;
        }
        // Node table sortedness by url_key (binary-search precondition).
        let mut prev_key = None;
        for i in 0..n {
            let k = NodeRec(&nodes[i * NODE_LEN..(i + 1) * NODE_LEN]).url_key();
            if let Some(p) = prev_key {
                if k <= p {
                    return Err(Error::format("nodes not sorted by url_key"));
                }
            }
            prev_key = Some(k);
        }
        Ok(())
    }

    /// The byte range of `kind`, if present.
    #[must_use]
    pub fn section<'a>(&self, bytes: &'a [u8], kind: SectionKind) -> Option<&'a [u8]> {
        self.sections
            .iter()
            .find(|(k, _, _)| *k == kind as u16)
            .map(|&(_, off, len)| &bytes[off..off + len])
    }
}

/// Validate arbitrary bytes as a base file (fuzz entry point).
pub fn validate(bytes: &[u8]) -> Result<()> {
    BaseDir::parse(bytes).map(|_| ())
}

/// Zero-copy accessor over one 72-byte node record.
#[derive(Clone, Copy, Debug)]
pub struct NodeRec<'a>(pub &'a [u8]);

#[allow(missing_docs)]
impl NodeRec<'_> {
    #[must_use]
    pub fn url_key(self) -> UrlKey {
        UrlKey(get_u64(self.0, 0))
    }
    #[must_use]
    pub fn content_hash(self) -> u64 {
        get_u64(self.0, 8)
    }
    #[must_use]
    pub fn fetched_at_ms(self) -> u64 {
        get_u64(self.0, 16)
    }
    #[must_use]
    pub fn last_change_ms(self) -> u64 {
        get_u64(self.0, 24)
    }
    #[must_use]
    pub fn url_off(self) -> u32 {
        get_u32(self.0, 32)
    }
    #[must_use]
    pub fn title_off(self) -> u32 {
        get_u32(self.0, 36)
    }
    #[must_use]
    pub fn snippet_off(self) -> u32 {
        get_u32(self.0, 40)
    }
    #[must_use]
    pub fn etag_off(self) -> u32 {
        get_u32(self.0, 44)
    }
    #[must_use]
    pub fn seg_start(self) -> u32 {
        get_u32(self.0, 48)
    }
    #[must_use]
    pub fn seg_count(self) -> u16 {
        get_u16(self.0, 52)
    }
    #[must_use]
    pub fn flags(self) -> u16 {
        get_u16(self.0, 54)
    }
    #[must_use]
    pub fn interval_s(self) -> u32 {
        get_u32(self.0, 56)
    }
    #[must_use]
    pub fn checks(self) -> u16 {
        get_u16(self.0, 60)
    }
    #[must_use]
    pub fn changes(self) -> u16 {
        get_u16(self.0, 62)
    }
    #[must_use]
    pub fn in_degree(self) -> u32 {
        get_u32(self.0, 64)
    }
}

/// Serialize one node record (compaction).
#[allow(clippy::too_many_arguments)]
pub(crate) fn write_node_rec(
    out: &mut Vec<u8>,
    url_key: UrlKey,
    content_hash: u64,
    fetched_at_ms: u64,
    last_change_ms: u64,
    label_offs: [u32; 4],
    seg_start: u32,
    seg_count: u16,
    flags: u16,
    interval_s: u32,
    checks: u16,
    changes: u16,
    in_degree: u32,
) {
    put_u64(out, url_key.0);
    put_u64(out, content_hash);
    put_u64(out, fetched_at_ms);
    put_u64(out, last_change_ms);
    for off in label_offs {
        put_u32(out, off);
    }
    put_u32(out, seg_start);
    put_u16(out, seg_count);
    put_u16(out, flags);
    put_u32(out, interval_s);
    put_u16(out, checks);
    put_u16(out, changes);
    put_u32(out, in_degree);
    put_u32(out, 0); // reserved
}

/// Zero-copy accessor over one 24-byte segment record.
#[derive(Clone, Copy, Debug)]
pub struct SegRec<'a>(pub &'a [u8]);

#[allow(missing_docs)]
impl SegRec<'_> {
    #[must_use]
    pub fn seg_key(self) -> u64 {
        get_u64(self.0, 0)
    }
    #[must_use]
    pub fn byte_start(self) -> u32 {
        get_u32(self.0, 8)
    }
    #[must_use]
    pub fn byte_len(self) -> u32 {
        get_u32(self.0, 12)
    }
    #[must_use]
    pub fn label_off(self) -> u32 {
        get_u32(self.0, 16)
    }
    #[must_use]
    pub fn importance(self) -> u16 {
        get_u16(self.0, 20)
    }
    #[must_use]
    pub fn depth(self) -> u8 {
        self.0[22]
    }
    #[must_use]
    pub fn flags(self) -> u8 {
        self.0[23]
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn write_seg_rec(
    out: &mut Vec<u8>,
    seg_key: u64,
    byte_start: u32,
    byte_len: u32,
    label_off: u32,
    importance: u16,
    depth: u8,
    flags: u8,
) {
    put_u64(out, seg_key);
    put_u32(out, byte_start);
    put_u32(out, byte_len);
    put_u32(out, label_off);
    put_u16(out, importance);
    out.push(depth);
    out.push(flags);
}

/// Zero-copy accessor over one 24-byte lexicon record.
#[derive(Clone, Copy, Debug)]
pub struct LexRec<'a>(pub &'a [u8]);

#[allow(missing_docs)]
impl LexRec<'_> {
    #[must_use]
    pub fn token_hash(self) -> u64 {
        get_u64(self.0, 0)
    }
    #[must_use]
    pub fn df(self) -> u32 {
        get_u32(self.0, 8)
    }
    #[must_use]
    pub fn postings_off(self) -> u32 {
        get_u32(self.0, 12)
    }
    #[must_use]
    pub fn postings_len(self) -> u32 {
        get_u32(self.0, 16)
    }
    #[must_use]
    pub fn label_off(self) -> u32 {
        get_u32(self.0, 20)
    }
}

pub(crate) fn write_lex_rec(
    out: &mut Vec<u8>,
    token_hash: u64,
    df: u32,
    postings_off: u32,
    postings_len: u32,
    label_off: u32,
) {
    put_u64(out, token_hash);
    put_u32(out, df);
    put_u32(out, postings_off);
    put_u32(out, postings_len);
    put_u32(out, label_off);
}

/// Read a length-prefixed label at `off` from the labels heap. Offsets have
/// been validated at parse time, so this is infallible for validated files.
#[must_use]
pub fn label_at(labels: &[u8], off: u32) -> Option<&str> {
    if off == NO_LABEL {
        return None;
    }
    let off = off as usize;
    let len = usize::from(get_u16(labels, off));
    std::str::from_utf8(&labels[off + 2..off + 2 + len]).ok()
}

/// Append a label to the heap and return its offset.
pub(crate) fn push_label(labels: &mut Vec<u8>, s: &str) -> u32 {
    let off = labels.len() as u32;
    bytesio::put_str(labels, s);
    off
}

/// Binary-search the sorted node table for `key`; returns the record index.
#[must_use]
pub fn find_node(nodes: &[u8], key: UrlKey) -> Option<u32> {
    let n = nodes.len() / NODE_LEN;
    let (mut lo, mut hi) = (0usize, n);
    while lo < hi {
        let mid = usize::midpoint(lo, hi);
        let k = get_u64(nodes, mid * NODE_LEN);
        match k.cmp(&key.0) {
            std::cmp::Ordering::Less => lo = mid + 1,
            std::cmp::Ordering::Greater => hi = mid,
            std::cmp::Ordering::Equal => return Some(mid as u32),
        }
    }
    None
}

/// Decode a postings run (LEB128 deltas, ascending node ids).
pub fn decode_postings(bytes: &[u8]) -> Result<Vec<u32>> {
    let mut r = Reader::new(bytes);
    let mut out = Vec::with_capacity(bytes.len().min(r.remaining()));
    let mut prev: u64 = 0;
    while r.remaining() > 0 {
        let delta = r.varint()?;
        let id = if out.is_empty() {
            delta
        } else {
            prev.checked_add(1)
                .and_then(|v| v.checked_add(delta))
                .ok_or_else(|| Error::format("posting delta overflow"))?
        };
        if id > u64::from(u32::MAX) {
            return Err(Error::format("posting id overflow"));
        }
        out.push(id as u32);
        prev = id;
    }
    Ok(out)
}

/// Encode a postings run (caller guarantees strictly ascending ids).
pub(crate) fn encode_postings(ids: &[u32], out: &mut Vec<u8>) {
    let mut prev: Option<u32> = None;
    for &id in ids {
        let delta = match prev {
            None => u64::from(id),
            Some(p) => u64::from(id - p - 1),
        };
        bytesio::put_varint(out, delta);
        prev = Some(id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_header() -> Header {
        Header {
            flags: 0,
            section_count: 0,
            node_count: 0,
            seg_count: 0,
            edge_count: 0,
            token_count: 0,
            generation: 1,
            wal_applied_seq: 0,
            total_len: 0,
            created_at_ms: 0,
        }
    }

    #[test]
    fn empty_file_round_trips() {
        let bytes = BaseWriter::new(empty_header()).finish();
        let dir = BaseDir::parse(&bytes).unwrap();
        assert_eq!(dir.header.generation, 1);
        assert_eq!(dir.header.section_count, 0);
    }

    #[test]
    fn flipped_byte_anywhere_is_rejected() {
        let bytes = BaseWriter::new(empty_header()).finish();
        for i in 0..bytes.len() {
            let mut bad = bytes.clone();
            bad[i] ^= 0x01;
            assert!(BaseDir::parse(&bad).is_err(), "flip at {i} accepted");
        }
    }

    #[test]
    fn truncation_at_every_landmark_is_rejected() {
        let bytes = BaseWriter::new(empty_header()).finish();
        for cut in 0..bytes.len() {
            assert!(BaseDir::parse(&bytes[..cut]).is_err());
        }
    }

    #[test]
    fn postings_round_trip_and_reject_overflow() {
        let ids = vec![0, 1, 5, 6, 1000, 70_000];
        let mut enc = Vec::new();
        encode_postings(&ids, &mut enc);
        assert_eq!(decode_postings(&enc).unwrap(), ids);
        assert!(decode_postings(&[0xff; 12]).is_err());
        // Fuzz-found regression: a second delta of u64::MAX must error, not
        // overflow the accumulator.
        let mut hostile = Vec::new();
        crate::bytesio::put_varint(&mut hostile, 1);
        crate::bytesio::put_varint(&mut hostile, u64::MAX);
        assert!(decode_postings(&hostile).is_err());
    }

    #[test]
    fn node_binary_search_finds_all() {
        let mut nodes = Vec::new();
        for k in [3u64, 9, 12, 400, 500] {
            write_node_rec(
                &mut nodes,
                UrlKey(k),
                0,
                0,
                0,
                [NO_LABEL; 4],
                0,
                0,
                0,
                0,
                0,
                0,
                0,
            );
        }
        for k in [3u64, 9, 12, 400, 500] {
            assert!(find_node(&nodes, UrlKey(k)).is_some());
        }
        assert!(find_node(&nodes, UrlKey(10)).is_none());
        assert!(find_node(&nodes, UrlKey(0)).is_none());
        assert!(find_node(&nodes, UrlKey(501)).is_none());
    }
}
