// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! `graph.wal` — the append-only log that extends a base generation.
//!
//! ```text
//! [WalHeader 64 B] [record]*   one record = one transaction, whole or absent
//! ```
//!
//! A transaction is appended as one checksummed record (one write, one
//! fsync), so a crash mid-append leaves a torn tail that holds none of it:
//! replay stops at the first torn record and the next writer cuts it off. A
//! record whose checksum holds but whose contents break a rule is
//! [`Error::Corrupt`], never cut, because cutting it would drop every later
//! commit. Ops speak [`UrlKey`], never dense node ids, because ids are
//! reassigned at every compaction; ops whose `seq` is already folded into the
//! base (`seq <= wal_applied_seq`) are skipped, which is what makes a crash
//! between "new base renamed" and "wal reset" harmless.
//!
//! The record and op layouts are under "format 3" below. Everything is read
//! in place, through views borrowed from the validated bytes.

use crate::bytesio::{get_u16, get_u32, get_u64, put_u16, put_u32, put_u64};
use crate::error::{Error, Result};
use crate::hash::xxh3_64;
use crate::key::UrlKey;

/// Magic: `"GRPW"` little-endian.
pub const MAGIC: u32 = 0x5750_5247;
/// WAL format version; strict-equal on read. 3: one checksummed record per
/// transaction (2: one frame per op).
pub const VERSION: u16 = 3;
/// Header width.
pub const HEADER_LEN: usize = 64;
/// Most outbound edges per node.
pub const MAX_EDGES: usize = 4096;
/// Most attributes per node.
pub const MAX_ATTRS: usize = 64;
/// Longest attribute value (bytes of UTF-8).
pub const MAX_ATTR_LEN: usize = 256 << 10;

const OP_UPSERT: u8 = 1;
const OP_SEGS: u8 = 2;
const OP_EDGES: u8 = 3;
const OP_TOUCH: u8 = 4;
const OP_REMOVE: u8 = 5;
const OP_PIN: u8 = 6;
// 7 was segment importance (enrichment), retired.
const OP_ATTRS: u8 = 8;

/// Serialize a WAL header for `base_generation`.
#[must_use]
pub fn encode_header(base_generation: u64, created_at_ms: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER_LEN);
    put_u32(&mut out, MAGIC);
    put_u16(&mut out, VERSION);
    put_u16(&mut out, 0);
    put_u32(&mut out, HEADER_LEN as u32);
    // pad to 16 for field alignment
    put_u32(&mut out, 0);
    put_u64(&mut out, base_generation);
    put_u64(&mut out, created_at_ms);
    out.resize(HEADER_LEN - 8, 0);
    let sum = xxh3_64(&out[..HEADER_LEN - 8]);
    put_u64(&mut out, sum);
    out
}

/// Parse a WAL header, returning `base_generation`.
pub fn parse_header(bytes: &[u8]) -> Result<u64> {
    if bytes.len() < HEADER_LEN {
        return Err(Error::format("wal shorter than header"));
    }
    if crate::bytesio::get_u32(bytes, 0) != MAGIC {
        return Err(Error::format("bad wal magic"));
    }
    let version = crate::bytesio::get_u16(bytes, 4);
    if version != VERSION {
        return Err(Error::Version { found: version, supported: VERSION });
    }
    let stored = crate::bytesio::get_u64(bytes, HEADER_LEN - 8);
    if xxh3_64(&bytes[..HEADER_LEN - 8]) != stored {
        return Err(Error::format("wal header checksum mismatch"));
    }
    Ok(crate::bytesio::get_u64(bytes, 16))
}

// ---- format 3: one record per transaction ---------------------------------
//
// ```text
// record: 0 len u32 | 4 tag u16 "TX" | 6 rflags u8 = COMMIT | 7 0
//         8 first_seq u64 | 16 op_count u32 | 20 0 u32
//         24 xxh3(rec[0..24] ++ rec[32..len]) u64 | 32.. ops, back to back
// op:     0 op_len u32 | 4 kind u8 | 5 oflags u8 | 6 0 u16 | 8 key u64 | 16.. body
// ```
//
// Every field sits at a fixed offset and is read in place; a record is
// validated once, whole, when it is accepted, and only then read through the
// borrowed views below. A record that is short, mis-tagged or fails its
// checksum is torn (a crash mid-append: never committed); one whose checksum
// holds but whose body breaks a rule is corrupt and is reported, never cut.

/// Width of a record's header.
pub const RECORD_HEADER_LEN: usize = 32;
/// Width of an op's header.
pub const OP_HEADER_LEN: usize = 16;
/// Largest record, which is one whole transaction (about a million upserts).
/// A larger transaction is refused at commit, before anything is written.
pub const MAX_RECORD: usize = 256 << 20;
/// Longest label, URL, title, snippet or etag (bytes of UTF-8).
pub const MAX_LABEL: usize = 2048;
/// Most segments per document.
pub const MAX_SEGS: usize = 128;
/// Largest op: a maximal attribute set (its fixed fields, 64 entry headers
/// and 1 048 246 value bytes) and so every other op.
pub const MAX_OP: usize = 1_048_246 + ATTRS_FIXED + MAX_ATTRS * ATTR_ENTRY;

const TAG: u16 = 0x5854; // "TX"
const COMMIT: u8 = 0x01;
const MIN_RECORD: usize = RECORD_HEADER_LEN + OP_HEADER_LEN;

const UPSERT_FIXED: usize = OP_HEADER_LEN + 32;
const SEGS_FIXED: usize = OP_HEADER_LEN + 8;
const SEG_ENTRY: usize = 16;
const EDGES_FIXED: usize = OP_HEADER_LEN + 8;
const EDGE_ENTRY: usize = 12;
const TOUCH_FIXED: usize = OP_HEADER_LEN + 40;
const ATTRS_FIXED: usize = OP_HEADER_LEN + 8;
const ATTR_ENTRY: usize = 8;

/// Upsert `present` bits: which optional strings follow the URL.
const HAS_TITLE: u8 = 1;
const HAS_SNIPPET: u8 = 2;
const HAS_ETAG: u8 = 4;
/// Touch `bits`.
const T_HASH: u8 = 1;
const T_ETAG: u8 = 2;
const T_TOMBSTONE: u8 = 4;

/// One outbound edge: destination key, type tag, edge flags, weight.
pub type Edge = (UrlKey, u8, u8, u16);

/// The record checksum: xxh3 over the header's first 24 bytes and the ops,
/// streamed so neither side builds a concatenated copy.
fn record_sum(rec: &[u8]) -> u64 {
    let mut h = crate::hash::Hasher::new();
    h.update(&rec[..24]);
    h.update(&rec[RECORD_HEADER_LEN..]);
    h.digest64()
}

/// A revalidation outcome with the writer-computed freshness state it
/// leaves behind (replay never re-derives policy).
#[derive(Clone, Copy, Debug)]
pub struct TouchRecord<'a> {
    /// When the check happened, epoch ms.
    pub checked_at_ms: u64,
    /// The outcome tag (`crate::ttl::Outcome::as_tag`).
    pub outcome: u8,
    /// The node's content hash after the check.
    pub content_hash: Option<u64>,
    /// The node's entity tag after the check.
    pub etag: Option<&'a str>,
    /// The revalidation interval it leads to, seconds.
    pub interval_s: u32,
    /// Revalidations observed (saturating).
    pub checks: u16,
    /// Content changes observed (saturating).
    pub changes: u16,
    /// Last observed content change, epoch ms.
    pub last_change_ms: u64,
    /// The source was observed gone.
    pub tombstone: bool,
}

/// A position in a [`RecordBuf`] to truncate back to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Mark {
    len: usize,
    ops: u32,
}

/// The encoder: a transaction's ops, encoded in place as they are staged,
/// and sealed into one record at commit. The sealed buffer is what the log
/// receives and what the overlay then serves, unchanged. Every `push_*`
/// validates first and appends nothing when it refuses.
#[derive(Debug, Default)]
pub struct RecordBuf {
    buf: Vec<u8>,
    ops: u32,
}

impl RecordBuf {
    /// An empty buffer (allocates nothing until the first op).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Ops staged.
    #[must_use]
    pub fn ops(&self) -> u32 {
        self.ops
    }

    /// True when no op is staged.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ops == 0
    }

    /// Encoded length so far, header included (0 when empty).
    #[must_use]
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    /// The current position, for [`RecordBuf::truncate`].
    #[must_use]
    pub fn mark(&self) -> Mark {
        Mark { len: self.buf.len(), ops: self.ops }
    }

    /// Drop every op staged after `mark`.
    pub fn truncate(&mut self, mark: Mark) {
        if mark.ops == 0 {
            self.buf.clear();
            self.ops = 0;
        } else if mark.len <= self.buf.len() && mark.ops <= self.ops {
            self.buf.truncate(mark.len);
            self.ops = mark.ops;
        }
    }

    /// The encoded bytes (a record once sealed).
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.buf
    }

    /// The encoded bytes, moved out.
    #[must_use]
    pub fn into_vec(self) -> Vec<u8> {
        self.buf
    }

    /// The staged ops, in order.
    pub fn op_views(&self) -> impl Iterator<Item = OpView<'_>> + '_ {
        OpWalk { bytes: &self.buf, pos: RECORD_HEADER_LEN.min(self.buf.len()) }
            .filter_map(|(_, op)| OpView::trusted(op))
    }

    /// Start an op: the record header on first use, then the op header with
    /// its length to be patched by [`RecordBuf::finish`].
    fn begin(&mut self, kind: u8, oflags: u8, key: UrlKey, body: usize) -> usize {
        if self.buf.is_empty() {
            self.buf.reserve(RECORD_HEADER_LEN + OP_HEADER_LEN + body);
            self.buf.resize(RECORD_HEADER_LEN, 0);
        }
        let at = self.buf.len();
        self.buf.reserve(OP_HEADER_LEN + body);
        put_u32(&mut self.buf, 0);
        self.buf.extend_from_slice(&[kind, oflags, 0, 0]);
        put_u64(&mut self.buf, key.0);
        at
    }

    fn finish(&mut self, at: usize) {
        let len = (self.buf.len() - at) as u32;
        self.buf[at..at + 4].copy_from_slice(&len.to_le_bytes());
        self.ops += 1;
    }

    /// Stage a node create/replace with node flags `flags`.
    pub fn push_upsert(&mut self, doc: &crate::writer::DocRecord<'_>, flags: u16) -> Result<()> {
        if doc.url.is_empty() || doc.url.len() > MAX_LABEL {
            return Err(Error::format(format!("url must be 1..={MAX_LABEL} bytes")));
        }
        let opt = [doc.title, doc.snippet, doc.etag];
        if opt.iter().flatten().any(|s| s.len() > MAX_LABEL) {
            return Err(Error::format(format!("a label exceeds {MAX_LABEL} bytes")));
        }
        let text = doc.url.len() + opt.iter().flatten().map(|s| s.len()).sum::<usize>();
        let present = opt.iter().zip([HAS_TITLE, HAS_SNIPPET, HAS_ETAG]).fold(0, |p, (s, bit)| {
            if s.is_some() {
                p | bit
            } else {
                p
            }
        });
        let at = self.begin(OP_UPSERT, 0, doc.url_key, 32 + text);
        put_u64(&mut self.buf, doc.content_hash);
        put_u64(&mut self.buf, doc.fetched_at_ms);
        put_u16(&mut self.buf, flags);
        self.buf.extend_from_slice(&[present, 0]);
        put_u16(&mut self.buf, doc.url.len() as u16);
        for s in opt {
            put_u16(&mut self.buf, s.map_or(0, |s| s.len() as u16));
        }
        put_u32(&mut self.buf, 0);
        self.buf.extend_from_slice(doc.url.as_bytes());
        for s in opt.into_iter().flatten() {
            self.buf.extend_from_slice(s.as_bytes());
        }
        self.finish(at);
        Ok(())
    }

    /// Stage a whole-set segment replacement for `key` (depth at least 1;
    /// a segment without a byte range is flagged `NO_RANGE`).
    pub fn push_segments(
        &mut self,
        key: UrlKey,
        segs: &[crate::writer::SegmentRecord<'_>],
    ) -> Result<()> {
        if segs.len() > MAX_SEGS {
            return Err(Error::format(format!("more than {MAX_SEGS} segments")));
        }
        if segs.iter().any(|s| s.label.len() > MAX_LABEL) {
            return Err(Error::format(format!("segment label exceeds {MAX_LABEL} bytes")));
        }
        let text: usize = segs.iter().map(|s| s.label.len()).sum();
        let at = self.begin(OP_SEGS, 0, key, 8 + segs.len() * SEG_ENTRY + text);
        self.buf.extend_from_slice(&[segs.len() as u8, 0, 0, 0, 0, 0, 0, 0]);
        for s in segs {
            let (start, len, flags) = match s.byte_range {
                Some((start, len)) => (start, len, 0),
                None => (0, 0, crate::format::base::sflags::NO_RANGE),
            };
            put_u32(&mut self.buf, start);
            put_u32(&mut self.buf, len);
            put_u16(&mut self.buf, s.importance);
            put_u16(&mut self.buf, s.label.len() as u16);
            self.buf.extend_from_slice(&[s.depth.max(1), flags, 0, 0]);
        }
        for s in segs {
            self.buf.extend_from_slice(s.label.as_bytes());
        }
        self.finish(at);
        Ok(())
    }

    /// Stage a whole-set outbound-edge replacement for `key`: at most
    /// [`MAX_EDGES`], destinations strictly ascending.
    pub fn push_edges(&mut self, key: UrlKey, edges: &[Edge]) -> Result<()> {
        if edges.len() > MAX_EDGES {
            return Err(Error::format(format!("more than {MAX_EDGES} edges")));
        }
        if edges.windows(2).any(|w| w[0].0 .0 >= w[1].0 .0) {
            return Err(Error::format("edge destinations must be strictly ascending"));
        }
        let at = self.begin(OP_EDGES, 0, key, 8 + edges.len() * EDGE_ENTRY);
        put_u16(&mut self.buf, edges.len() as u16);
        self.buf.extend_from_slice(&[0; 6]);
        for &(dst, etype, eflags, weight) in edges {
            put_u64(&mut self.buf, dst.0);
            self.buf.extend_from_slice(&[etype, eflags]);
            put_u16(&mut self.buf, weight);
        }
        self.finish(at);
        Ok(())
    }

    /// Stage a revalidation outcome for `key`.
    pub fn push_touch(&mut self, key: UrlKey, t: &TouchRecord<'_>) -> Result<()> {
        let etag = t.etag.unwrap_or("");
        if etag.len() > MAX_LABEL {
            return Err(Error::format(format!("etag exceeds {MAX_LABEL} bytes")));
        }
        let at = self.begin(OP_TOUCH, 0, key, 40 + etag.len());
        put_u64(&mut self.buf, t.checked_at_ms);
        put_u64(&mut self.buf, t.content_hash.unwrap_or(0));
        put_u64(&mut self.buf, t.last_change_ms);
        put_u32(&mut self.buf, t.interval_s);
        put_u16(&mut self.buf, t.checks);
        put_u16(&mut self.buf, t.changes);
        let bits = (u8::from(t.content_hash.is_some()) * T_HASH)
            | (u8::from(t.etag.is_some()) * T_ETAG)
            | (u8::from(t.tombstone) * T_TOMBSTONE);
        self.buf.extend_from_slice(&[t.outcome, bits]);
        put_u16(&mut self.buf, etag.len() as u16);
        put_u32(&mut self.buf, 0);
        self.buf.extend_from_slice(etag.as_bytes());
        self.finish(at);
        Ok(())
    }

    /// Stage a hard delete of `key`.
    pub fn push_remove(&mut self, key: UrlKey) {
        let at = self.begin(OP_REMOVE, 0, key, 0);
        self.finish(at);
    }

    /// Stage a pin or unpin of `key`.
    pub fn push_pin(&mut self, key: UrlKey, pinned: bool) {
        let at = self.begin(OP_PIN, u8::from(pinned), key, 0);
        self.finish(at);
    }

    /// Stage a whole-set attribute replacement for `key`: keys strictly
    /// ascending, at most [`MAX_ATTRS`], each value at most
    /// [`MAX_ATTR_LEN`], the op at most [`MAX_OP`].
    pub fn push_attrs(&mut self, key: UrlKey, attrs: &[(u8, &str)]) -> Result<()> {
        let parts: Vec<(u8, [&str; 3])> = attrs.iter().map(|&(k, v)| (k, [v, "", ""])).collect();
        self.push_attr_parts(key, &parts)
    }

    /// [`RecordBuf::push_attrs`] with each value the concatenation of three
    /// parts, written straight into the record (a named attribute is
    /// `name`, NUL, `value`): no value is assembled anywhere else first.
    pub(crate) fn push_attr_parts(&mut self, key: UrlKey, attrs: &[(u8, [&str; 3])]) -> Result<()> {
        let n = attrs.len();
        if n > MAX_ATTRS {
            return Err(Error::format(format!("more than {MAX_ATTRS} attributes")));
        }
        let mut total = 0usize;
        let mut prev: Option<u8> = None;
        for &(k, parts) in attrs {
            if prev.is_some_and(|p| p >= k) {
                return Err(Error::format("attribute keys must be strictly ascending"));
            }
            prev = Some(k);
            let len: usize = parts.iter().map(|p| p.len()).sum();
            if len > MAX_ATTR_LEN {
                return Err(Error::format("attribute too long"));
            }
            total += len;
        }
        if ATTRS_FIXED + n * ATTR_ENTRY + total > MAX_OP {
            return Err(Error::format(format!("attributes exceed {MAX_OP} bytes in one op")));
        }
        let at = self.begin(OP_ATTRS, 0, key, 8 + n * ATTR_ENTRY + total);
        self.buf.extend_from_slice(&[n as u8, 0, 0, 0, 0, 0, 0, 0]);
        for &(k, parts) in attrs {
            self.buf.extend_from_slice(&[k, 0, 0, 0]);
            put_u32(&mut self.buf, parts.iter().map(|p| p.len() as u32).sum());
        }
        for (_, parts) in attrs {
            for p in parts {
                self.buf.extend_from_slice(p.as_bytes());
            }
        }
        self.finish(at);
        Ok(())
    }

    /// Seal the staged ops into one record whose ops take sequence numbers
    /// `first_seq..`. Refused, with the buffer left as it was, when there is
    /// nothing staged, when the record would exceed [`MAX_RECORD`], or when
    /// the sequence numbers would overflow. Sealing again (a retried commit)
    /// rewrites the header.
    pub fn seal(&mut self, first_seq: u64) -> Result<()> {
        if self.ops == 0 {
            return Err(Error::format("a record needs at least one op"));
        }
        if self.buf.len() > MAX_RECORD {
            return Err(Error::format(format!(
                "a transaction of {} bytes exceeds the {MAX_RECORD}-byte record limit; \
                 commit it in parts",
                self.buf.len()
            )));
        }
        if first_seq == 0 || first_seq.checked_add(u64::from(self.ops) - 1).is_none() {
            return Err(Error::format("record sequence numbers out of range"));
        }
        let len = self.buf.len() as u32;
        let head = &mut self.buf[..RECORD_HEADER_LEN];
        head[0..4].copy_from_slice(&len.to_le_bytes());
        head[4..6].copy_from_slice(&TAG.to_le_bytes());
        head[6..8].copy_from_slice(&[COMMIT, 0]);
        head[8..16].copy_from_slice(&first_seq.to_le_bytes());
        head[16..20].copy_from_slice(&self.ops.to_le_bytes());
        head[20..24].copy_from_slice(&[0; 4]);
        let sum = record_sum(&self.buf);
        self.buf[24..32].copy_from_slice(&sum.to_le_bytes());
        Ok(())
    }
}

/// Walks op headers through bytes already validated (or encoded here):
/// `(offset, op bytes)`.
struct OpWalk<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Iterator for OpWalk<'a> {
    type Item = (usize, &'a [u8]);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let rest = self.bytes.get(self.pos..)?;
        if rest.len() < OP_HEADER_LEN {
            return None;
        }
        let len = get_u32(rest, 0) as usize;
        let op = rest.get(..len).filter(|_| len >= OP_HEADER_LEN)?;
        let at = self.pos;
        self.pos += len;
        Some((at, op))
    }
}

/// What lies at a position in a log image.
#[derive(Clone, Copy, Debug)]
pub enum Next<'a> {
    /// A whole, validated record.
    Record(Record<'a>),
    /// Not a committed record: short, mis-tagged or failing its checksum —
    /// what a crash mid-append leaves. Nothing at or after it is committed.
    Torn,
}

/// The record at `pos` of `bytes`, validated whole: its header, checksum
/// and every op. `Err` is [`Error::Corrupt`] only: a record whose checksum
/// holds but whose contents break a rule was written that way, and is never
/// mistaken for a torn tail (cutting it would drop every later commit).
pub fn next_record(bytes: &[u8], pos: usize) -> Result<Next<'_>> {
    let Some(rest) = bytes.get(pos..) else { return Ok(Next::Torn) };
    if rest.len() < RECORD_HEADER_LEN {
        return Ok(Next::Torn);
    }
    let len = get_u32(rest, 0) as usize;
    if !(MIN_RECORD..=MAX_RECORD).contains(&len)
        || len > rest.len()
        || get_u16(rest, 4) != TAG
        || rest[6] != COMMIT
    {
        return Ok(Next::Torn);
    }
    let rec = &rest[..len];
    if record_sum(rec) != get_u64(rec, 24) {
        return Ok(Next::Torn);
    }
    let bad = |what: &str| Error::corrupt(format!("wal record at byte {pos}: {what}"));
    if rec[7] != 0 || get_u32(rec, 20) != 0 {
        return Err(bad("non-zero reserved header bytes"));
    }
    let first_seq = get_u64(rec, 8);
    let count = get_u32(rec, 16);
    if first_seq == 0 || count == 0 || first_seq.checked_add(u64::from(count) - 1).is_none() {
        return Err(bad("sequence numbers out of range"));
    }
    let mut at = RECORD_HEADER_LEN;
    for i in 0..count {
        if len - at < OP_HEADER_LEN {
            return Err(bad(&format!("op {i} of {count} missing")));
        }
        let op_len = get_u32(rec, at) as usize;
        if !(OP_HEADER_LEN..=MAX_OP).contains(&op_len) || op_len > len - at {
            return Err(bad(&format!("op {i} length {op_len} out of range")));
        }
        validate_op(&rec[at..at + op_len])
            .map_err(|e| bad(&format!("op {i} at byte {}: {e}", pos + at)))?;
        at += op_len;
    }
    if at != len {
        return Err(bad("bytes after the last op"));
    }
    Ok(Next::Record(Record { rec, at: pos }))
}

/// A validated record (one transaction), borrowed from its image.
#[derive(Clone, Copy, Debug)]
pub struct Record<'a> {
    rec: &'a [u8],
    at: usize,
}

impl<'a> Record<'a> {
    /// Sequence number of its first op; op `i` has `first_seq() + i`.
    #[must_use]
    #[inline]
    pub fn first_seq(&self) -> u64 {
        get_u64(self.rec, 8)
    }

    /// Ops it carries (at least one).
    #[must_use]
    #[inline]
    pub fn op_count(&self) -> u32 {
        get_u32(self.rec, 16)
    }

    /// Sequence number of its last op.
    #[must_use]
    #[inline]
    pub fn last_seq(&self) -> u64 {
        self.first_seq() + u64::from(self.op_count()) - 1
    }

    /// Its length in bytes, header included.
    #[must_use]
    pub fn len(&self) -> usize {
        self.rec.len()
    }

    /// Always false: a record holds at least one op.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        false
    }

    /// Its offset in the image it was read from.
    #[must_use]
    pub fn offset(&self) -> usize {
        self.at
    }

    /// Its bytes.
    #[must_use]
    pub fn bytes(&self) -> &'a [u8] {
        self.rec
    }

    /// Its ops in order, each with its sequence number and its offset in the
    /// image the record was read from.
    pub fn ops(&self) -> impl Iterator<Item = (u64, usize, OpView<'a>)> + 'a {
        let (first, at) = (self.first_seq(), self.at);
        OpWalk { bytes: self.rec, pos: RECORD_HEADER_LEN }
            .zip(0..u64::from(self.op_count()))
            .filter_map(move |((off, op), i)| OpView::trusted(op).map(|v| (first + i, at + off, v)))
    }
}

/// Walks the records of a range already validated by [`next_record`],
/// reading only their headers.
#[derive(Clone, Debug)]
pub struct Records<'a> {
    image: &'a [u8],
    pos: usize,
    end: usize,
}

impl<'a> Records<'a> {
    /// Records of `image[pos..end]`, which must be whole records validated
    /// in this process (the log the store replayed, or the record it
    /// sealed): headers are trusted, nothing is re-checked.
    pub(crate) fn trusted(image: &'a [u8], pos: usize, end: usize) -> Self {
        Self { image, pos, end: end.min(image.len()) }
    }
}

impl<'a> Iterator for Records<'a> {
    type Item = Record<'a>;

    #[inline]
    fn next(&mut self) -> Option<Record<'a>> {
        if self.end - self.pos.min(self.end) < MIN_RECORD {
            return None;
        }
        let len = get_u32(self.image, self.pos) as usize;
        let rec = self
            .image
            .get(self.pos..self.pos + len)
            .filter(|_| len >= MIN_RECORD && self.pos + len <= self.end)?;
        let at = self.pos;
        self.pos += len;
        Some(Record { rec, at })
    }
}

/// How far a run of records goes from `pos`: the end of the last whole
/// record before a tear, the last sequence number seen, the ops counted, and
/// the corruption that stopped it, if one did. Sequence numbers must rise
/// across records, starting above `after`.
pub(crate) struct Scan {
    pub(crate) end: usize,
    pub(crate) last_seq: Option<u64>,
    pub(crate) ops: usize,
    pub(crate) corrupt: Option<Error>,
}

pub(crate) fn scan(bytes: &[u8], mut pos: usize, after: Option<u64>) -> Scan {
    let mut out = Scan { end: pos, last_seq: after, ops: 0, corrupt: None };
    loop {
        match next_record(bytes, pos) {
            Ok(Next::Record(r)) => {
                if out.last_seq.is_some_and(|l| r.first_seq() <= l) {
                    out.corrupt = Some(Error::corrupt(format!(
                        "wal record at byte {pos}: sequence regression"
                    )));
                    return out;
                }
                out.last_seq = Some(r.last_seq());
                out.ops += r.op_count() as usize;
                pos += r.len();
                out.end = pos;
            }
            Ok(Next::Torn) => return out,
            Err(e) => {
                out.corrupt = Some(e);
                return out;
            }
        }
    }
}

/// A replayed log image: its header's generation and the length of its
/// committed prefix (header plus whole records), whose records were all
/// validated.
#[derive(Clone, Debug)]
pub struct Replay<'a> {
    /// Base generation this log extends.
    pub base_generation: u64,
    /// Byte length of the committed prefix; a writable opener truncates the
    /// file to this length.
    pub good_len: usize,
    image: &'a [u8],
}

impl<'a> Replay<'a> {
    /// The committed records, in order.
    #[must_use]
    pub fn records(&self) -> Records<'a> {
        Records::trusted(self.image, HEADER_LEN, self.good_len)
    }
}

/// Replay a log image: validate its header and every record up to the first
/// torn one (that is the crash model, not an error). A corrupt record or a
/// sequence regression is [`Error::Corrupt`]; a bad header is an error too
/// ([`Error::Version`] for a well-formed one of another version).
pub fn replay(image: &[u8]) -> Result<Replay<'_>> {
    let base_generation = parse_header(image)?;
    let scanned = scan(image, HEADER_LEN, None);
    if let Some(e) = scanned.corrupt {
        return Err(e);
    }
    Ok(Replay { base_generation, good_len: scanned.end, image })
}

/// Byte length of the committed prefix of a log image — exactly the
/// [`Replay::good_len`] that [`replay`] would report when it succeeds — or
/// 0 when the header itself is invalid. This is the length that is safe to
/// copy elsewhere: it ends on a record boundary, so it never holds part of
/// a transaction.
#[must_use]
pub fn committed_len(image: &[u8]) -> usize {
    if parse_header(image).is_err() {
        return 0;
    }
    HEADER_LEN + committed_frames_len(&image[HEADER_LEN..])
}

/// Like [`committed_len`] for a run of records with no header (for example
/// the bytes appended since a previously measured record boundary): the
/// length of its whole, valid records with rising sequence numbers.
#[must_use]
pub fn committed_frames_len(frames: &[u8]) -> usize {
    scan(frames, 0, None).end
}

/// One op, validated (see [`validate_op`]) and read in place.
#[derive(Clone, Copy, Debug)]
pub enum OpView<'a> {
    /// Create or replace a document node's core record.
    Upsert(UpsertView<'a>),
    /// Replace the node's segment set.
    Segs(SegsView<'a>),
    /// Replace the node's outbound edge set.
    Edges(EdgesView<'a>),
    /// A revalidation outcome with its freshness state.
    Touch(TouchView<'a>),
    /// Hard-delete the node.
    Remove(UrlKey),
    /// Pin (`true`) or unpin the node.
    Pin(UrlKey, bool),
    /// Replace the node's attribute set.
    Attrs(AttrsView<'a>),
}

impl<'a> OpView<'a> {
    /// The view of one op validated earlier (or encoded here): no checks
    /// beyond the kind. `None` only for bytes that were never an op.
    #[inline]
    pub(crate) fn trusted(op: &'a [u8]) -> Option<Self> {
        if op.len() < OP_HEADER_LEN {
            return None;
        }
        let key = UrlKey(get_u64(op, 8));
        Some(match op[4] {
            OP_UPSERT => Self::Upsert(UpsertView { op }),
            OP_SEGS => Self::Segs(SegsView { op }),
            OP_EDGES => Self::Edges(EdgesView { op }),
            OP_TOUCH => Self::Touch(TouchView { op }),
            OP_REMOVE => Self::Remove(key),
            OP_PIN => Self::Pin(key, op[5] & 1 != 0),
            OP_ATTRS => Self::Attrs(AttrsView { op }),
            _ => return None,
        })
    }

    /// The key this op targets.
    #[must_use]
    #[inline]
    pub fn key(&self) -> UrlKey {
        match self {
            Self::Upsert(v) => v.key(),
            Self::Segs(v) => v.key(),
            Self::Edges(v) => v.key(),
            Self::Touch(v) => v.key(),
            Self::Attrs(v) => v.key(),
            Self::Remove(k) | Self::Pin(k, _) => *k,
        }
    }
}

/// UTF-8 at `op[at..at + len]`, validated when the op was accepted (so the
/// fallback is never taken).
#[inline]
fn text(op: &[u8], at: usize, len: usize) -> &str {
    std::str::from_utf8(&op[at..at + len]).unwrap_or("")
}

#[inline]
fn utf8(bytes: &[u8], what: &str) -> Result<()> {
    std::str::from_utf8(bytes)
        .map(|_| ())
        .map_err(|_| Error::format(format!("{what}: invalid utf-8")))
}

/// Validate one op (exactly `bytes`): its header, every length, count, sort
/// order, reserved byte and string against the rules of its kind. Fuzz
/// entry point: any input is `Ok` or [`Error::Format`], never a panic, and
/// nothing is allocated on success.
pub fn validate_op(bytes: &[u8]) -> Result<OpView<'_>> {
    let n = bytes.len();
    if !(OP_HEADER_LEN..=MAX_OP).contains(&n) || get_u32(bytes, 0) as usize != n {
        return Err(Error::format("op length disagrees with its header"));
    }
    let (kind, oflags) = (bytes[4], bytes[5]);
    if get_u16(bytes, 6) != 0 {
        return Err(Error::format("non-zero reserved op bytes"));
    }
    if oflags & !u8::from(kind == OP_PIN) != 0 {
        return Err(Error::format("unknown op flags"));
    }
    match kind {
        OP_UPSERT => check_upsert(bytes)?,
        OP_SEGS => check_segs(bytes)?,
        OP_EDGES => check_edges(bytes)?,
        OP_TOUCH => check_touch(bytes)?,
        OP_REMOVE | OP_PIN if n == OP_HEADER_LEN => {}
        OP_REMOVE | OP_PIN => return Err(Error::format("remove/pin carries a body")),
        OP_ATTRS => check_attrs(bytes)?,
        other => return Err(Error::format(format!("unknown op kind {other}"))),
    }
    OpView::trusted(bytes).ok_or_else(|| Error::format("unknown op kind"))
}

#[inline]
fn zeros(bytes: &[u8]) -> bool {
    bytes.iter().all(|&b| b == 0)
}

#[inline]
fn check_upsert(op: &[u8]) -> Result<()> {
    let n = op.len();
    if n < UPSERT_FIXED {
        return Err(Error::format("upsert too short"));
    }
    let present = op[34];
    if present & !(HAS_TITLE | HAS_SNIPPET | HAS_ETAG) != 0 || op[35] != 0 || !zeros(&op[44..48]) {
        return Err(Error::format("upsert: unknown bits"));
    }
    let lens = [36, 38, 40, 42].map(|at| usize::from(get_u16(op, at)));
    if lens[0] == 0 || lens.iter().any(|&l| l > MAX_LABEL) {
        return Err(Error::format("upsert: string length out of range"));
    }
    for (len, bit) in lens[1..].iter().zip([HAS_TITLE, HAS_SNIPPET, HAS_ETAG]) {
        if present & bit == 0 && *len != 0 {
            return Err(Error::format("upsert: an absent string has a length"));
        }
    }
    if lens.iter().sum::<usize>() != n - UPSERT_FIXED {
        return Err(Error::format("upsert: string lengths disagree with the op"));
    }
    let mut at = UPSERT_FIXED;
    for len in lens {
        utf8(&op[at..at + len], "upsert")?;
        at += len;
    }
    Ok(())
}

#[inline]
fn check_segs(op: &[u8]) -> Result<()> {
    let n = op.len();
    if n < SEGS_FIXED || !zeros(&op[17..24]) || usize::from(op[16]) > MAX_SEGS {
        return Err(Error::format("segments: bad header"));
    }
    let count = usize::from(op[16]);
    let mut at = SEGS_FIXED + count * SEG_ENTRY;
    if at > n {
        return Err(Error::format("segments: entries overrun the op"));
    }
    for i in 0..count {
        let e = SEGS_FIXED + i * SEG_ENTRY;
        let len = usize::from(get_u16(op, e + 10));
        if len > MAX_LABEL || get_u16(op, e + 14) != 0 || len > n - at {
            return Err(Error::format("segments: bad entry"));
        }
        utf8(&op[at..at + len], "segment label")?;
        at += len;
    }
    if at != n {
        return Err(Error::format("segments: label lengths disagree with the op"));
    }
    Ok(())
}

#[inline]
fn check_edges(op: &[u8]) -> Result<()> {
    let n = op.len();
    if n < EDGES_FIXED || !zeros(&op[18..24]) {
        return Err(Error::format("edges: bad header"));
    }
    let count = usize::from(get_u16(op, 16));
    if count > MAX_EDGES || n != EDGES_FIXED + count * EDGE_ENTRY {
        return Err(Error::format("edges: count disagrees with the op"));
    }
    let mut prev: Option<u64> = None;
    for i in 0..count {
        let dst = get_u64(op, EDGES_FIXED + i * EDGE_ENTRY);
        if prev.is_some_and(|p| p >= dst) {
            return Err(Error::format("edges: destinations not strictly ascending"));
        }
        prev = Some(dst);
    }
    Ok(())
}

#[inline]
fn check_touch(op: &[u8]) -> Result<()> {
    let n = op.len();
    if n < TOUCH_FIXED {
        return Err(Error::format("touch too short"));
    }
    let bits = op[49];
    let len = usize::from(get_u16(op, 50));
    if bits & !(T_HASH | T_ETAG | T_TOMBSTONE) != 0
        || !zeros(&op[52..56])
        || (bits & T_HASH == 0 && get_u64(op, 24) != 0)
        || (bits & T_ETAG == 0 && len != 0)
        || len > MAX_LABEL
        || n != TOUCH_FIXED + len
    {
        return Err(Error::format("touch: bad fields"));
    }
    utf8(&op[TOUCH_FIXED..], "touch etag")
}

#[inline]
fn check_attrs(op: &[u8]) -> Result<()> {
    let n = op.len();
    if n < ATTRS_FIXED || !zeros(&op[17..24]) || usize::from(op[16]) > MAX_ATTRS {
        return Err(Error::format("attributes: bad header"));
    }
    let count = usize::from(op[16]);
    let mut at = ATTRS_FIXED + count * ATTR_ENTRY;
    if at > n {
        return Err(Error::format("attributes: entries overrun the op"));
    }
    let mut prev: Option<u8> = None;
    for i in 0..count {
        let e = ATTRS_FIXED + i * ATTR_ENTRY;
        let (k, len) = (op[e], get_u32(op, e + 4) as usize);
        if prev.is_some_and(|p| p >= k)
            || !zeros(&op[e + 1..e + 4])
            || len > MAX_ATTR_LEN
            || len > n - at
        {
            return Err(Error::format("attributes: bad entry"));
        }
        prev = Some(k);
        utf8(&op[at..at + len], "attribute")?;
        at += len;
    }
    if at != n {
        return Err(Error::format("attributes: value lengths disagree with the op"));
    }
    Ok(())
}

/// A node upsert, read in place.
#[derive(Clone, Copy, Debug)]
pub struct UpsertView<'a> {
    op: &'a [u8],
}

impl<'a> UpsertView<'a> {
    /// The node's key.
    #[must_use]
    pub fn key(&self) -> UrlKey {
        UrlKey(get_u64(self.op, 8))
    }
    /// xxh3 of the fetched body.
    #[must_use]
    pub fn content_hash(&self) -> u64 {
        get_u64(self.op, 16)
    }
    /// Fetch stamp, epoch ms.
    #[must_use]
    pub fn fetched_at_ms(&self) -> u64 {
        get_u64(self.op, 24)
    }
    /// Node flags (`format::base::nflags`).
    #[must_use]
    pub fn flags(&self) -> u16 {
        get_u16(self.op, 32)
    }
    fn len_at(&self, i: usize) -> usize {
        usize::from(get_u16(self.op, 36 + 2 * i))
    }
    #[inline]
    fn field(&self, i: usize, bit: u8) -> Option<&'a str> {
        if i > 0 && self.op[34] & bit == 0 {
            return None;
        }
        let at = UPSERT_FIXED + (0..i).map(|j| self.len_at(j)).sum::<usize>();
        Some(text(self.op, at, self.len_at(i)))
    }
    /// Canonical URL.
    #[must_use]
    pub fn url(&self) -> &'a str {
        self.field(0, 0).unwrap_or("")
    }
    /// Title, if any.
    #[must_use]
    pub fn title(&self) -> Option<&'a str> {
        self.field(1, HAS_TITLE)
    }
    /// Snippet, if any.
    #[must_use]
    pub fn snippet(&self) -> Option<&'a str> {
        self.field(2, HAS_SNIPPET)
    }
    /// Entity tag, if any.
    #[must_use]
    pub fn etag(&self) -> Option<&'a str> {
        self.field(3, HAS_ETAG)
    }
}

/// A segment set, read in place.
#[derive(Clone, Copy, Debug)]
pub struct SegsView<'a> {
    op: &'a [u8],
}

/// One segment of a [`SegsView`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SegView<'a> {
    /// Heading-path label.
    pub label: &'a str,
    /// Byte offset in the source document.
    pub byte_start: u32,
    /// Byte length in the source document.
    pub byte_len: u32,
    /// Importance in 1/65535 units.
    pub importance: u16,
    /// Heading depth.
    pub depth: u8,
    /// Segment flags (`format::base::sflags`).
    pub flags: u8,
}

impl<'a> SegsView<'a> {
    /// The node's key.
    #[must_use]
    pub fn key(&self) -> UrlKey {
        UrlKey(get_u64(self.op, 8))
    }
    /// Segment count.
    #[must_use]
    pub fn len(&self) -> usize {
        usize::from(self.op[16])
    }
    /// True for an empty set.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// The segments, in document order.
    pub fn iter(&self) -> impl Iterator<Item = SegView<'a>> + 'a {
        let (op, n) = (self.op, self.len());
        let mut label = SEGS_FIXED + n * SEG_ENTRY;
        (0..n).map(move |i| {
            let e = SEGS_FIXED + i * SEG_ENTRY;
            let len = usize::from(get_u16(op, e + 10));
            let s = SegView {
                label: text(op, label, len),
                byte_start: get_u32(op, e),
                byte_len: get_u32(op, e + 4),
                importance: get_u16(op, e + 8),
                depth: op[e + 12],
                flags: op[e + 13],
            };
            label += len;
            s
        })
    }
}

/// An outbound edge set, read in place: fixed 12-byte entries.
#[derive(Clone, Copy, Debug)]
pub struct EdgesView<'a> {
    op: &'a [u8],
}

impl<'a> EdgesView<'a> {
    /// The node's key.
    #[must_use]
    pub fn key(&self) -> UrlKey {
        UrlKey(get_u64(self.op, 8))
    }
    /// Edge count.
    #[must_use]
    pub fn len(&self) -> usize {
        usize::from(get_u16(self.op, 16))
    }
    /// True for an empty set.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Edge `i`, if in range.
    #[must_use]
    pub fn get(&self, i: usize) -> Option<Edge> {
        (i < self.len()).then(|| edge_at(self.entries(), i))
    }
    /// The edges, destinations ascending.
    pub fn iter(&self) -> impl Iterator<Item = Edge> + 'a {
        let e = self.entries();
        (0..e.len() / EDGE_ENTRY).map(move |i| edge_at(e, i))
    }
    /// The raw entries, for [`edge_at`].
    #[must_use]
    pub(crate) fn entries(&self) -> &'a [u8] {
        &self.op[EDGES_FIXED..]
    }
}

/// Edge `i` of an [`EdgesView`]'s entries.
#[inline]
pub(crate) fn edge_at(entries: &[u8], i: usize) -> Edge {
    let e = i * EDGE_ENTRY;
    (UrlKey(get_u64(entries, e)), entries[e + 8], entries[e + 9], get_u16(entries, e + 10))
}

/// Edges in an [`EdgesView`]'s entries.
#[inline]
pub(crate) fn edge_count(entries: &[u8]) -> usize {
    entries.len() / EDGE_ENTRY
}

/// A revalidation outcome, read in place.
#[derive(Clone, Copy, Debug)]
pub struct TouchView<'a> {
    op: &'a [u8],
}

impl<'a> TouchView<'a> {
    /// The node's key.
    #[must_use]
    pub fn key(&self) -> UrlKey {
        UrlKey(get_u64(self.op, 8))
    }
    /// When the check happened, epoch ms.
    #[must_use]
    pub fn checked_at_ms(&self) -> u64 {
        get_u64(self.op, 16)
    }
    /// The content hash after the check.
    #[must_use]
    pub fn content_hash(&self) -> Option<u64> {
        (self.op[49] & T_HASH != 0).then(|| get_u64(self.op, 24))
    }
    /// Last observed content change, epoch ms.
    #[must_use]
    pub fn last_change_ms(&self) -> u64 {
        get_u64(self.op, 32)
    }
    /// The revalidation interval, seconds.
    #[must_use]
    pub fn interval_s(&self) -> u32 {
        get_u32(self.op, 40)
    }
    /// Revalidations observed.
    #[must_use]
    pub fn checks(&self) -> u16 {
        get_u16(self.op, 44)
    }
    /// Content changes observed.
    #[must_use]
    pub fn changes(&self) -> u16 {
        get_u16(self.op, 46)
    }
    /// The outcome tag.
    #[must_use]
    pub fn outcome(&self) -> u8 {
        self.op[48]
    }
    /// The entity tag after the check.
    #[must_use]
    #[inline]
    pub fn etag(&self) -> Option<&'a str> {
        (self.op[49] & T_ETAG != 0)
            .then(|| text(self.op, TOUCH_FIXED, usize::from(get_u16(self.op, 50))))
    }
    /// The source was observed gone.
    #[must_use]
    pub fn tombstone(&self) -> bool {
        self.op[49] & T_TOMBSTONE != 0
    }
}

/// An attribute set, read in place.
#[derive(Clone, Copy, Debug)]
pub struct AttrsView<'a> {
    op: &'a [u8],
}

impl<'a> AttrsView<'a> {
    /// The node's key.
    #[must_use]
    pub fn key(&self) -> UrlKey {
        UrlKey(get_u64(self.op, 8))
    }
    /// Attribute count.
    #[must_use]
    pub fn len(&self) -> usize {
        usize::from(self.op[16])
    }
    /// True for an empty set.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// The attributes, keys ascending.
    pub fn iter(&self) -> impl Iterator<Item = (u8, &'a str)> + 'a {
        let (op, n) = (self.op, self.len());
        let mut value = ATTRS_FIXED + n * ATTR_ENTRY;
        (0..n).map(move |i| {
            let e = ATTRS_FIXED + i * ATTR_ENTRY;
            let len = get_u32(op, e + 4) as usize;
            let v = (op[e], text(op, value, len));
            value += len;
            v
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- format 3 ----------------------------------------------------------

    use crate::writer::{DocRecord, SegmentRecord};

    fn doc(url: &str) -> DocRecord<'_> {
        DocRecord {
            url,
            url_key: UrlKey::of(url),
            content_hash: 0xAB,
            fetched_at_ms: 77,
            title: Some("Title"),
            snippet: None,
            etag: Some("\"e1\""),
            pinned: false,
        }
    }

    /// One record holding each of the seven op kinds.
    fn every_kind() -> RecordBuf {
        let mut b = RecordBuf::new();
        b.push_upsert(&doc("https://x.dev/a"), 1).unwrap();
        let segs = [
            SegmentRecord {
                label: "Install",
                byte_range: Some((10, 200)),
                depth: 1,
                importance: 60_000,
            },
            SegmentRecord {
                label: "Ünïcode › Linux", byte_range: None, depth: 0, importance: 7
            },
        ];
        b.push_segments(UrlKey(9), &segs).unwrap();
        b.push_edges(UrlKey(9), &[(UrlKey(11), 0, 0, 65_535), (UrlKey(400), 2, 3, 100)]).unwrap();
        let touch = TouchRecord {
            checked_at_ms: 5,
            outcome: 1,
            content_hash: Some(42),
            etag: Some("\"v2\""),
            interval_s: 3600,
            checks: 3,
            changes: 1,
            last_change_ms: 4,
            tombstone: true,
        };
        b.push_touch(UrlKey(9), &touch).unwrap();
        b.push_remove(UrlKey(11));
        b.push_pin(UrlKey(9), true);
        b.push_attrs(UrlKey(9), &[(1, "INT64"), (2, &"SELECT 1".repeat(1000))]).unwrap();
        b
    }

    fn record(b: &RecordBuf) -> Record<'_> {
        match next_record(b.as_bytes(), 0).unwrap() {
            Next::Record(r) => r,
            Next::Torn => panic!("a sealed record reads as torn"),
        }
    }

    #[test]
    fn a_record_round_trips_every_op_kind() {
        let mut buf = every_kind();
        buf.seal(41).unwrap();
        let rec = record(&buf);
        assert_eq!(
            (rec.first_seq(), rec.op_count(), rec.last_seq(), rec.len()),
            (41, 7, 47, buf.len())
        );
        let ops: Vec<(u64, usize, OpView<'_>)> = rec.ops().collect();
        assert_eq!(ops.iter().map(|o| o.0).collect::<Vec<_>>(), (41..=47).collect::<Vec<_>>());
        for &(_, off, _) in &ops {
            let len = get_u32(buf.as_bytes(), off) as usize;
            validate_op(&buf.as_bytes()[off..off + len]).expect("each op validates alone");
        }
        let OpView::Upsert(up) = ops[0].2 else { panic!("upsert") };
        assert_eq!(up.key(), UrlKey::of("https://x.dev/a"));
        assert_eq!((up.content_hash(), up.fetched_at_ms(), up.flags()), (0xAB, 77, 1));
        assert_eq!(
            (up.url(), up.title(), up.snippet(), up.etag()),
            ("https://x.dev/a", Some("Title"), None, Some("\"e1\""))
        );
        let OpView::Segs(sv) = ops[1].2 else { panic!("segments") };
        let segs: Vec<SegView<'_>> = sv.iter().collect();
        assert_eq!(segs.len(), 2);
        assert_eq!(
            (segs[0].label, segs[0].byte_start, segs[0].byte_len, segs[0].depth, segs[0].flags),
            ("Install", 10, 200, 1, 0)
        );
        assert_eq!((segs[1].label, segs[1].depth, segs[1].importance), ("Ünïcode › Linux", 1, 7));
        assert_eq!(segs[1].flags, crate::format::base::sflags::NO_RANGE);
        let OpView::Edges(ev) = ops[2].2 else { panic!("edges") };
        assert_eq!(
            ev.iter().collect::<Vec<_>>(),
            [(UrlKey(11), 0, 0, 65_535), (UrlKey(400), 2, 3, 100)]
        );
        assert_eq!(ev.get(1), Some((UrlKey(400), 2, 3, 100)));
        assert_eq!(ev.get(2), None);
        let OpView::Touch(tv) = ops[3].2 else { panic!("touch") };
        assert_eq!(
            (tv.checked_at_ms(), tv.outcome(), tv.content_hash(), tv.etag()),
            (5, 1, Some(42), Some("\"v2\""))
        );
        assert_eq!(
            (tv.interval_s(), tv.checks(), tv.changes(), tv.last_change_ms(), tv.tombstone()),
            (3600, 3, 1, 4, true)
        );
        assert!(matches!(ops[4].2, OpView::Remove(UrlKey(11))));
        assert!(matches!(ops[5].2, OpView::Pin(UrlKey(9), true)));
        let OpView::Attrs(av) = ops[6].2 else { panic!("attributes") };
        let attrs: Vec<(u8, &str)> = av.iter().collect();
        assert_eq!(attrs[0], (1, "INT64"));
        assert_eq!(attrs[1].1.len(), 8000);
        // The staged view walks the same ops.
        assert_eq!(buf.op_views().count(), 7);
    }

    /// Three records, cut at every byte: a whole record is read, the record
    /// the cut lands in is torn, and the committed prefix ends exactly where
    /// the last whole record does.
    #[test]
    fn a_cut_at_every_byte_tears_only_the_record_it_lands_in() {
        let mut image = Vec::new();
        let mut ends = Vec::new();
        for (i, n) in [3u64, 1, 5].into_iter().enumerate() {
            let mut b = RecordBuf::new();
            for k in 0..n {
                b.push_remove(UrlKey(k));
            }
            b.push_upsert(&doc("https://x.dev/z"), 0).unwrap();
            b.seal(1 + 10 * i as u64).unwrap();
            image.extend_from_slice(b.as_bytes());
            ends.push(image.len());
        }
        for cut in 0..=image.len() {
            let bytes = &image[..cut];
            let mut pos = 0;
            while let Next::Record(r) = next_record(bytes, pos).unwrap() {
                pos += r.len();
            }
            let want = ends.iter().copied().filter(|&e| e <= cut).max().unwrap_or(0);
            assert_eq!(pos, want, "cut {cut}");
        }
    }

    /// Rewrite a record's checksum after tampering with it, so it reads as
    /// written that way rather than torn.
    fn reseal(rec: &mut [u8]) {
        let sum = record_sum(rec);
        rec[24..32].copy_from_slice(&sum.to_le_bytes());
    }

    fn corrupt_after(mutate: impl FnOnce(&mut Vec<u8>)) -> Error {
        let mut b = every_kind();
        b.seal(1).unwrap();
        let mut rec = b.into_vec();
        mutate(&mut rec);
        let n = rec.len() as u32;
        rec[0..4].copy_from_slice(&n.to_le_bytes());
        reseal(&mut rec);
        match next_record(&rec, 0) {
            Err(e @ Error::Corrupt { .. }) => e,
            Ok(Next::Torn) => panic!("a checksummed record read as torn"),
            Ok(Next::Record(_)) => panic!("accepted a malformed record"),
            Err(e) => panic!("not corrupt: {e}"),
        }
    }

    /// Offsets of the seven ops in a sealed `every_kind` record.
    fn op_offsets() -> Vec<usize> {
        let mut b = every_kind();
        b.seal(1).unwrap();
        let offs = record(&b).ops().map(|o| o.1).collect();
        offs
    }

    /// One tampering per rule, given the op offsets.
    type Tamper = fn(&mut Vec<u8>, &[usize]);

    #[test]
    fn a_checksummed_but_malformed_record_is_corrupt_never_torn() {
        let cases: [(&str, Tamper); 16] = [
            ("unknown kind", |r, o| r[o[0] + 4] = 7),
            ("reserved op bytes", |r, o| r[o[2] + 6] = 1),
            ("reserved record byte", |r, _| r[7] = 1),
            ("reserved record word", |r, _| r[21] = 1),
            ("unknown op flags", |r, o| r[o[1] + 5] = 1),
            ("bad utf-8", |r, o| r[o[0] + 48] = 0xff),
            ("descending edges", |r, o| {
                r[o[2] + 24..o[2] + 32].copy_from_slice(&u64::MAX.to_le_bytes());
            }),
            ("descending attribute keys", |r, o| r[o[6] + 24] = 9),
            ("string lengths", |r, o| r[o[0] + 36] += 1),
            ("absent string with a length", |r, o| r[o[0] + 40] = 1),
            ("touch hash without its bit", |r, o| r[o[3] + 49] &= !1),
            ("op count too high", |r, _| r[16] = 8),
            ("op count too low", |r, _| r[16] = 6),
            ("first seq zero", |r, _| r[8..16].copy_from_slice(&0u64.to_le_bytes())),
            ("seq overflow", |r, _| r[8..16].copy_from_slice(&u64::MAX.to_le_bytes())),
            ("trailing bytes", |r, _| r.extend_from_slice(&[0; 3])),
        ];
        let offs = op_offsets();
        for (what, tamper) in cases {
            let e = corrupt_after(|r| tamper(r, &offs));
            assert!(e.to_string().contains("wal record at byte 0"), "{what}: {e}");
        }
    }

    #[test]
    fn a_short_mistagged_or_mismatched_record_is_torn() {
        let mut b = every_kind();
        b.seal(1).unwrap();
        let good = b.into_vec();
        let torn = |mutate: fn(&mut Vec<u8>)| {
            let mut r = good.clone();
            mutate(&mut r);
            matches!(next_record(&r, 0), Ok(Next::Torn))
        };
        assert!(torn(|r| r[4] ^= 1), "tag");
        assert!(torn(|r| r[6] = 2), "flags");
        assert!(torn(|r| r[100] ^= 1), "checksum");
        assert!(
            torn(|r| r[0..4].copy_from_slice(&(MIN_RECORD as u32 - 1).to_le_bytes())),
            "too short"
        );
        assert!(torn(|r| r[0..4].copy_from_slice(&u32::MAX.to_le_bytes())), "too long");
        assert!(torn(|r| r.truncate(RECORD_HEADER_LEN - 1)), "no header");
        assert!(matches!(next_record(&good, good.len() + 5), Ok(Next::Torn)), "past the end");
    }

    #[test]
    fn staging_refuses_bad_input_and_appends_nothing() {
        let mut b = RecordBuf::new();
        b.push_remove(UrlKey(1));
        let mark = b.mark();
        let long = "x".repeat(MAX_LABEL + 1);
        assert!(b.push_upsert(&doc(""), 0).is_err());
        assert!(b
            .push_upsert(&DocRecord { title: Some(&long), ..doc("https://x.dev/a") }, 0)
            .is_err());
        assert!(b.push_edges(UrlKey(1), &[(UrlKey(5), 0, 0, 0), (UrlKey(5), 0, 0, 0)]).is_err());
        assert!(b.push_edges(UrlKey(1), &vec![(UrlKey(5), 0, 0, 0); MAX_EDGES + 1]).is_err());
        assert!(b.push_attrs(UrlKey(1), &[(2, "a"), (1, "b")]).is_err());
        let touch = TouchRecord {
            checked_at_ms: 0,
            outcome: 0,
            content_hash: None,
            etag: Some(&long),
            interval_s: 0,
            checks: 0,
            changes: 0,
            last_change_ms: 0,
            tombstone: false,
        };
        assert!(b.push_touch(UrlKey(1), &touch).is_err());
        assert_eq!(b.mark(), mark, "a refused push appended something");
        b.push_pin(UrlKey(2), false);
        b.truncate(mark);
        assert_eq!(b.ops(), 1);
        b.truncate(RecordBuf::new().mark());
        assert!(b.is_empty() && b.as_bytes().is_empty());
        assert!(b.seal(1).is_err(), "an empty record");
    }

    /// The op cap admits a maximal attribute set and nothing larger, and
    /// keeps `writer::MAX_ATTR_BYTES` where it was.
    #[test]
    fn the_op_cap_admits_a_maximal_attribute_set_exactly() {
        assert_eq!(MAX_OP - ATTRS_FIXED - MAX_ATTRS * ATTR_ENTRY, 1_048_246);
        let big = "v".repeat(MAX_ATTR_LEN);
        let last = "w".repeat(1_048_246 - 3 * MAX_ATTR_LEN);
        let mut set: Vec<(u8, &str)> = (0..64u8).map(|k| (k, "")).collect();
        for (k, v) in [big.as_str(), &big, &big, &last].into_iter().enumerate() {
            set[k].1 = v;
        }
        let mut b = RecordBuf::new();
        b.push_attrs(UrlKey(1), &set).expect("exactly MAX_OP is accepted");
        assert_eq!(b.len(), RECORD_HEADER_LEN + MAX_OP);
        let one_more = format!("{last}w");
        set[3].1 = &one_more;
        assert!(RecordBuf::new().push_attrs(UrlKey(1), &set).is_err(), "MAX_OP + 1 is refused");
        b.seal(1).unwrap();
        assert!(matches!(next_record(b.as_bytes(), 0), Ok(Next::Record(_))));
    }

    /// The record cap admits a record of exactly `MAX_RECORD` bytes and
    /// refuses one byte more, leaving the buffer as it was.
    #[test]
    fn the_record_cap_admits_exactly_max_record() {
        let big = "v".repeat(MAX_ATTR_LEN);
        let fixed = ATTRS_FIXED + ATTR_ENTRY;
        let mut b = RecordBuf::new();
        let mut key = 0;
        while b.len() < MAX_RECORD {
            let room = MAX_RECORD - b.len().max(RECORD_HEADER_LEN);
            b.push_attrs(UrlKey(key), &[(0, &big[..(room - fixed).min(MAX_ATTR_LEN)])]).unwrap();
            key += 1;
        }
        assert_eq!(b.len(), MAX_RECORD);
        b.seal(1).expect("exactly MAX_RECORD is accepted");
        assert!(matches!(next_record(b.as_bytes(), 0), Ok(Next::Record(_))));
        let before = b.mark();
        b.push_pin(UrlKey(1), true);
        let e = b.seal(1).unwrap_err();
        assert!(matches!(e, Error::Format { .. }), "{e}");
        assert_eq!(b.ops(), before.ops + 1, "a refused seal keeps the ops");
    }

    // ---- replay (ported from the frame-format tests) -----------------------

    /// A log image of seven single-op records, one per op kind.
    fn image(generation: u64) -> Vec<u8> {
        let mut image = encode_header(generation, 0);
        let mut all = every_kind();
        all.seal(1).unwrap();
        for (i, (_, off, _)) in record(&all).ops().enumerate() {
            let len = get_u32(all.as_bytes(), off) as usize;
            let mut b = RecordBuf::new();
            b.buf.extend_from_slice(&[0; RECORD_HEADER_LEN]);
            b.buf.extend_from_slice(&all.as_bytes()[off..off + len]);
            b.ops = 1;
            b.seal(i as u64 + 1).unwrap();
            image.extend_from_slice(b.as_bytes());
        }
        image
    }

    fn seqs(r: &Replay<'_>) -> Vec<u64> {
        r.records().flat_map(|r| r.first_seq()..=r.last_seq()).collect()
    }

    #[test]
    fn wal_replays_and_stops_at_tear() {
        let image = image(7);
        let full = replay(&image).unwrap();
        assert_eq!(seqs(&full), (1..=7).collect::<Vec<_>>());
        assert_eq!(full.good_len, image.len());
        assert_eq!(full.base_generation, 7);
        // Truncate at every byte: never a panic, always a valid prefix.
        for cut in HEADER_LEN..image.len() {
            let r = replay(&image[..cut]).unwrap();
            assert!(seqs(&r).len() <= 7);
            assert!(r.good_len <= cut);
        }
    }

    #[test]
    fn a_bad_checksum_ends_replay_and_a_bad_header_errors() {
        let mut image = encode_header(1, 0);
        let mut b = RecordBuf::new();
        b.push_remove(UrlKey(1));
        b.seal(1).unwrap();
        image.extend_from_slice(b.as_bytes());
        let good = image.len();
        b.seal(2).unwrap();
        image.extend_from_slice(b.as_bytes());
        image[good + 26] ^= 0xff; // the second record's checksum
        let r = replay(&image).unwrap();
        assert_eq!(seqs(&r), [1]);
        assert_eq!(r.good_len, good);

        let mut bad_header = encode_header(1, 0);
        bad_header[3] ^= 0xff;
        assert!(replay(&bad_header).is_err());
    }

    #[test]
    fn committed_len_matches_replay_at_every_cut() {
        let image = image(3);
        for cut in 0..=image.len() {
            let want = replay(&image[..cut]).map_or(0, |r| r.good_len);
            assert_eq!(committed_len(&image[..cut]), want, "cut {cut}");
            if cut >= HEADER_LEN {
                assert_eq!(committed_frames_len(&image[HEADER_LEN..cut]) + HEADER_LEN, want);
            }
        }
    }

    #[test]
    fn streamed_record_checksum_equals_the_one_shot_hash() {
        let mut b = every_kind();
        b.seal(7).unwrap();
        let rec = b.as_bytes();
        let mut one_shot = rec[..24].to_vec();
        one_shot.extend_from_slice(&rec[RECORD_HEADER_LEN..]);
        assert_eq!(get_u64(rec, 24), xxh3_64(&one_shot));
    }

    #[test]
    fn sequence_regression_is_corrupt() {
        let mut image = encode_header(1, 0);
        for first in [5, 4] {
            let mut b = RecordBuf::new();
            b.push_remove(UrlKey(first));
            b.seal(first).unwrap();
            image.extend_from_slice(b.as_bytes());
        }
        assert!(matches!(replay(&image), Err(Error::Corrupt { .. })));
        // A copy never takes the regressed record either.
        assert_eq!(committed_len(&image), image.len() - (image.len() - HEADER_LEN) / 2);
        // An overlap inside one record pair is a regression too.
        let mut image = encode_header(1, 0);
        for (first, n) in [(1, 3), (3, 1)] {
            let mut b = RecordBuf::new();
            for k in 0..n {
                b.push_remove(UrlKey(k));
            }
            b.seal(first).unwrap();
            image.extend_from_slice(b.as_bytes());
        }
        assert!(matches!(replay(&image), Err(Error::Corrupt { .. })));
    }

    /// A log written by the frame format (0.4, 0.5) is a typed refusal
    /// naming both versions, read from its header alone.
    #[test]
    fn a_version_2_log_is_a_typed_refusal() {
        let mut v2 = encode_header(1, 0);
        v2[4..6].copy_from_slice(&2u16.to_le_bytes());
        let sum = xxh3_64(&v2[..HEADER_LEN - 8]);
        v2[HEADER_LEN - 8..].copy_from_slice(&sum.to_le_bytes());
        v2.extend_from_slice(&[0x55; 40]); // frames this build cannot read
        assert!(matches!(replay(&v2), Err(Error::Version { found: 2, supported: 3 })));
        assert!(matches!(parse_header(&v2), Err(Error::Version { found: 2, supported: 3 })));
        assert_eq!(committed_len(&v2), 0, "nothing of it is shipped as committed");
    }
}
