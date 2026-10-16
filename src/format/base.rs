// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! `graph.base` — the immutable snapshot. Layout:
//!
//! ```text
//! [container header 128 B][directory, 32 B per section][sections, 64-aligned]
//! ```
//!
//! Fixed-width, sorted record tables make every lookup a zero-copy binary
//! search directly against the mmap; the label heap holds all variable-width
//! strings. Section discriminants are burned: additive evolution happens via
//! new section kinds (unknown kinds are skipped on read) and header flags,
//! never by reinterpreting existing bytes.

use crate::bytesio::{self, get_u16, get_u32, get_u64, put_u16, put_u32, put_u64, Reader};
use crate::container;
use crate::error::{Error, Result};
use crate::key::UrlKey;

/// Magic: `"GRPR"` little-endian.
pub const MAGIC: u32 = 0x5250_5247;
/// Format version; strict-equal on read. 3: the layout of 2 (the shared
/// container framing: header, directory and section checksums), paired with
/// a log of one record per transaction. Bumped with the log because a base
/// is always read first: a release that cannot read this log refuses the
/// store there, before it could take an empty log for its own and rewrite it.
pub const VERSION: u16 = 3;

/// Node record width.
pub const NODE_LEN: usize = 72;
/// Segment record width.
pub const SEG_LEN: usize = 24;
/// Edge record width.
pub const EDGE_LEN: usize = 8;
/// Lexicon record width.
pub const LEX_LEN: usize = 24;
/// Rank record width (rank percentile, permille).
pub const RANK_LEN: usize = 2;
/// Attribute record width: key u8, 3 zero bytes, blob offset u32, length u32.
pub const ATTR_LEN: usize = 12;
/// Trigram record width: trigram (3 bytes, big-endian in a u32), postings
/// offset and length (in u32 lexicon indices).
pub const TRI_LEN: usize = 12;

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
    /// Set = inferred (a `Related` similarity edge); clear = extracted.
    pub const INFERRED: u8 = 2;
    /// Set on a `Derives` edge = an unchanged copy (pass-through or rename).
    pub const EXACT: u8 = 4;
}

/// Segment flags.
pub mod sflags {
    /// The byte range is unknown (heading-path anchor only).
    pub const NO_RANGE: u8 = 1;
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
    /// Fixed 2-B per-node rank percentiles (parallel to `Nodes`).
    Ranks = 8,
    /// CSR offsets: `(node_count + 1) × u32` into `InEdges`.
    InEdgeIndex = 9,
    /// Fixed 8-B inbound records (source id, type, flags, weight), per-node
    /// runs sorted by source id: the reverse of `Edges`.
    InEdges = 10,
    /// CSR offsets: `(node_count + 1) × u32` into `Attrs`.
    AttrIndex = 11,
    /// Fixed 12-B attribute records, per-node runs sorted by key.
    Attrs = 12,
    /// UTF-8 attribute values, addressed by (offset, length).
    Blobs = 13,
    /// `token_count × u32` lexicon indices in token string order: prefix
    /// lookups are a binary search.
    LexOrder = 14,
    /// Fixed 12-B trigram records sorted by trigram: substring lookups
    /// intersect the postings of a term's trigrams.
    Trigrams = 15,
    /// Ascending u32 lexicon indices per trigram.
    TriPostings = 16,
}

/// The base file's container identity (see [`crate::container`]).
pub const FORMAT: container::Format = container::Format { magic: MAGIC, version: VERSION };

/// Parsed header of a base file: the container's owner area.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Header {
    /// Feature flags (none defined; must round-trip).
    pub flags: u16,
    /// Node record count.
    pub node_count: u32,
    /// Segment record count.
    pub seg_count: u32,
    /// Edge record count.
    pub edge_count: u32,
    /// Lexicon record count.
    pub token_count: u32,
    /// Monotonic snapshot generation (bumped each compaction, never reused).
    pub generation: u64,
    /// Highest WAL op sequence folded into this base.
    pub wal_applied_seq: u64,
    /// Wall-clock creation stamp (informational).
    pub created_at_ms: u64,
}

impl Header {
    fn owner(&self) -> Vec<u8> {
        let mut o = Vec::with_capacity(40);
        put_u32(&mut o, self.node_count);
        put_u32(&mut o, self.seg_count);
        put_u32(&mut o, self.edge_count);
        put_u32(&mut o, self.token_count);
        put_u64(&mut o, self.generation);
        put_u64(&mut o, self.wal_applied_seq);
        put_u64(&mut o, self.created_at_ms);
        o
    }

    fn from_owner(flags: u16, o: &[u8]) -> Self {
        Self {
            flags,
            node_count: get_u32(o, 0),
            seg_count: get_u32(o, 4),
            edge_count: get_u32(o, 8),
            token_count: get_u32(o, 12),
            generation: get_u64(o, 16),
            wal_applied_seq: get_u64(o, 24),
            created_at_ms: get_u64(o, 32),
        }
    }

    /// The header alone (its own checksum verified, nothing else): enough to
    /// learn a base's generation without validating the whole file.
    pub(crate) fn parse(bytes: &[u8]) -> Result<Self> {
        let (flags, owner) = container::peek(bytes, FORMAT)?;
        Ok(Self::from_owner(flags, &owner))
    }
}

/// The generation of a base image from its header alone (header checksum
/// verified, nothing else): for callers that verify the bytes otherwise and
/// only need to know which generation they hold.
pub fn generation_of(bytes: &[u8]) -> Result<u64> {
    Header::parse(bytes).map(|h| h.generation)
}

/// Serializes a base file.
#[derive(Debug)]
pub struct BaseWriter {
    inner: container::Writer,
}

impl BaseWriter {
    /// Start a writer for the given header.
    #[must_use]
    pub fn new(header: Header) -> Self {
        let mut inner = container::Writer::new(FORMAT);
        inner.flags(header.flags).owner(&header.owner());
        Self { inner }
    }

    /// Append one section (an empty one is omitted; absent reads as empty).
    pub fn add_section(&mut self, kind: SectionKind, bytes: Vec<u8>) {
        self.inner.section(kind as u16, bytes);
    }

    /// Produce the final file bytes.
    #[must_use]
    pub fn finish(self) -> Vec<u8> {
        self.inner.finish()
    }
}

/// Parsed directory of a validated base file: the header plus each section's
/// byte range. Holds offsets, not borrows, so the owner can keep it alongside
/// the mmap it describes.
#[derive(Clone, Debug)]
pub struct BaseDir {
    /// The validated header.
    pub header: Header,
    dir: container::Dir,
}

impl BaseDir {
    /// Validate `bytes` end to end (container checksums, then every record
    /// invariant the zero-copy accessors rely on) and return the directory.
    /// This is the fuzz entry point: any byte string must produce `Ok`,
    /// `Error::Format` or `Error::Version`, never a panic.
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        let dir = container::Dir::parse(bytes, FORMAT)?;
        let header = Header::from_owner(dir.flags, dir.owner());
        let dir = Self { header, dir };
        dir.check_counts(bytes)?;
        Ok(dir)
    }

    /// Cross-check declared counts against actual section byte lengths so
    /// record accessors can index without per-access bounds errors.
    #[allow(clippy::too_many_lines)] // one pass over every record invariant
    fn check_counts(&self, bytes: &[u8]) -> Result<()> {
        let n = self.header.node_count as usize;
        let expect = |kind, want: usize, name: &str| -> Result<()> {
            if self.section(bytes, kind).len() == want {
                Ok(())
            } else {
                Err(Error::format(format!("{name} section length mismatch")))
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
        let labels = self.section(bytes, SectionKind::Labels);
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
        let nodes = self.section(bytes, SectionKind::Nodes);
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
        let segs = self.section(bytes, SectionKind::Segs);
        for i in 0..self.header.seg_count as usize {
            check_label(SegRec(&segs[i * SEG_LEN..(i + 1) * SEG_LEN]).label_off())?;
        }
        // Edge destinations and CSR monotonicity.
        if n > 0 {
            let idx = self.section(bytes, SectionKind::EdgeIndex);
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
            let edges = self.section(bytes, SectionKind::Edges);
            for i in 0..self.header.edge_count as usize {
                if get_u32(edges, i * EDGE_LEN) >= self.header.node_count {
                    return Err(Error::format("edge destination out of range"));
                }
            }
        }
        // Inbound edges: the exact reverse of the forward CSR (same count),
        // and attribute runs resolving inside the blob heap.
        if n > 0 {
            let check_csr = |kind, total: usize, name: &str| -> Result<()> {
                let idx = self.section(bytes, kind);
                if idx.is_empty() && total == 0 {
                    return Ok(());
                }
                if idx.len() != (n + 1) * 4 {
                    return Err(Error::format(format!("{name} index length mismatch")));
                }
                let mut prev = 0u32;
                for i in 0..=n {
                    let v = get_u32(idx, i * 4);
                    if v < prev || v as usize > total {
                        return Err(Error::format(format!("{name} index not monotone")));
                    }
                    prev = v;
                }
                if prev as usize == total {
                    Ok(())
                } else {
                    Err(Error::format(format!("{name} index does not cover its records")))
                }
            };
            let in_edges = self.section(bytes, SectionKind::InEdges);
            if in_edges.len() != self.header.edge_count as usize * EDGE_LEN {
                return Err(Error::format("inbound edges length mismatch"));
            }
            check_csr(SectionKind::InEdgeIndex, self.header.edge_count as usize, "inbound edge")?;
            for i in 0..self.header.edge_count as usize {
                if get_u32(in_edges, i * EDGE_LEN) >= self.header.node_count {
                    return Err(Error::format("inbound edge source out of range"));
                }
            }
            let attrs = self.section(bytes, SectionKind::Attrs);
            if !attrs.len().is_multiple_of(ATTR_LEN) {
                return Err(Error::format("attribute section length mismatch"));
            }
            check_csr(SectionKind::AttrIndex, attrs.len() / ATTR_LEN, "attribute")?;
            let blobs = self.section(bytes, SectionKind::Blobs);
            for rec in attrs.chunks_exact(ATTR_LEN) {
                let (o, l) = (get_u32(rec, 4) as usize, get_u32(rec, 8) as usize);
                let v = o.checked_add(l).and_then(|e| blobs.get(o..e));
                if v.is_none_or(|v| std::str::from_utf8(v).is_err()) || rec[1..4] != [0; 3] {
                    return Err(Error::format("attribute value out of bounds or not utf-8"));
                }
            }
        }
        // Lexicon search indices: every id names a lexicon record.
        let vocab = self.header.token_count;
        let order = self.section(bytes, SectionKind::LexOrder);
        if order.len() != vocab as usize * 4
            || order.chunks_exact(4).any(|c| get_u32(c, 0) >= vocab)
        {
            return Err(Error::format("lexicon order out of range"));
        }
        let (tri, tri_post) = (
            self.section(bytes, SectionKind::Trigrams),
            self.section(bytes, SectionKind::TriPostings),
        );
        if !tri.len().is_multiple_of(TRI_LEN) || !tri_post.len().is_multiple_of(4) {
            return Err(Error::format("trigram section length mismatch"));
        }
        for rec in tri.chunks_exact(TRI_LEN) {
            let (o, l) = (get_u32(rec, 4) as usize, get_u32(rec, 8) as usize);
            let ids = o.checked_add(l).and_then(|e| tri_post.get(o * 4..e * 4));
            if ids.is_none_or(|ids| ids.chunks_exact(4).any(|c| get_u32(c, 0) >= vocab)) {
                return Err(Error::format("trigram postings out of range"));
            }
        }
        // Lexicon postings ranges + sortedness by token hash.
        let postings = self.section(bytes, SectionKind::Postings);
        let lex = self.section(bytes, SectionKind::Lexicon);
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

    /// The bytes of section `kind` (empty when absent), O(1).
    #[must_use]
    pub fn section<'a>(&self, bytes: &'a [u8], kind: SectionKind) -> &'a [u8] {
        self.dir.section(bytes, kind as u16)
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
            node_count: 0,
            seg_count: 0,
            edge_count: 0,
            token_count: 0,
            generation: 1,
            wal_applied_seq: 0,
            created_at_ms: 0,
        }
    }

    #[test]
    fn empty_file_round_trips() {
        let bytes = BaseWriter::new(empty_header()).finish();
        let dir = BaseDir::parse(&bytes).unwrap();
        assert_eq!(dir.header, empty_header());
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
            write_node_rec(&mut nodes, UrlKey(k), 0, 0, 0, [NO_LABEL; 4], 0, 0, 0, 0, 0, 0, 0);
        }
        for k in [3u64, 9, 12, 400, 500] {
            assert!(find_node(&nodes, UrlKey(k)).is_some());
        }
        assert!(find_node(&nodes, UrlKey(10)).is_none());
        assert!(find_node(&nodes, UrlKey(0)).is_none());
        assert!(find_node(&nodes, UrlKey(501)).is_none());
    }
}
