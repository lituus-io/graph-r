// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
//! `graph.wal` — the append-only op log that extends a base generation.
//!
//! ```text
//! [WalHeader 64 B] [frame]* where frame = len u32 | xxh3(seq‖payload) u64 | seq u64 | payload
//! ```
//!
//! Ops speak [`UrlKey`], never dense node ids, because ids are reassigned at
//! every compaction. Replay stops at the first torn or corrupt frame (the
//! writable opener truncates back to the last good frame); ops whose `seq` is
//! already folded into the base (`seq <= wal_applied_seq`) are skipped, which
//! is what makes a crash between "new base renamed" and "wal reset" harmless.

use crate::bytesio::{put_u16, put_u32, put_u64, Reader};
use crate::error::{Error, Result};
use crate::hash::xxh3_64;
use crate::key::UrlKey;
use compact_str::CompactString;

/// Magic: `"GRPW"` little-endian.
pub const MAGIC: u32 = 0x5750_5247;
/// WAL format version; strict-equal on read. 2: `u16` edge counts and
/// node attributes.
pub const VERSION: u16 = 2;
/// Header width.
pub const HEADER_LEN: usize = 64;
/// Maximum frame payload; bounds replay preallocation against hostile files.
pub const MAX_FRAME: usize = 1 << 20;
/// Fixed per-frame overhead before the payload.
pub const FRAME_OVERHEAD: usize = 4 + 8 + 8;

/// One decoded segment inside a [`Op::SetSegments`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OwnedSeg {
    /// Heading path label (e.g. `"Install > Linux"`).
    pub label: CompactString,
    /// Byte offset of the segment in the source document (0 + `NO_RANGE` flag
    /// when unknown).
    pub byte_start: u32,
    /// Byte length of the segment.
    pub byte_len: u32,
    /// Heading depth (1 = H1 …).
    pub depth: u8,
    /// Importance in 1/65535 units.
    pub importance: u16,
    /// Segment flags (`sflags`).
    pub flags: u8,
}

/// One decoded edge inside a [`Op::SetEdges`]: destination key, type tag,
/// edge flags, weight.
pub type OwnedEdge = (UrlKey, u8, u8, u16);

/// A WAL operation. All mutations to the graph flow through exactly these.
#[derive(Clone, Debug, PartialEq, Eq)]
#[allow(missing_docs)] // field meanings documented on the writer API
pub enum Op {
    /// Create or replace a document node's core record.
    UpsertNode {
        key: UrlKey,
        content_hash: u64,
        fetched_at_ms: u64,
        flags: u16,
        url: CompactString,
        title: Option<CompactString>,
        snippet: Option<CompactString>,
        etag: Option<CompactString>,
    },
    /// Replace the node's entire segment set (whole-set semantics).
    SetSegments { key: UrlKey, segs: Vec<OwnedSeg> },
    /// Replace the node's entire outbound edge set (whole-set semantics,
    /// destinations strictly ascending by key).
    SetEdges { key: UrlKey, edges: Vec<OwnedEdge> },
    /// Record a revalidation outcome together with the writer-computed
    /// freshness state (interval, counters), making replay trivially
    /// deterministic.
    Touch {
        key: UrlKey,
        checked_at_ms: u64,
        outcome: u8,
        content_hash: Option<u64>,
        etag: Option<CompactString>,
        interval_s: u32,
        checks: u16,
        changes: u16,
        last_change_ms: u64,
        tombstone: bool,
    },
    /// Hard-delete the node (and its segments/edges) at the next compaction.
    Remove { key: UrlKey },
    /// Pin or unpin the node.
    SetPinned { key: UrlKey, pinned: bool },
    /// Whole-set replacement of the node's attributes.
    SetAttrs { key: UrlKey, attrs: Vec<(u8, CompactString)> },
}

/// Most outbound edges per node.
pub const MAX_EDGES: usize = 4096;
/// Most attributes per node.
pub const MAX_ATTRS: usize = 64;
/// Longest attribute value (bytes of UTF-8); a frame stays under [`MAX_FRAME`].
pub const MAX_ATTR_LEN: usize = 256 << 10;

const OP_UPSERT: u8 = 1;
const OP_SEGS: u8 = 2;
const OP_EDGES: u8 = 3;
const OP_TOUCH: u8 = 4;
const OP_REMOVE: u8 = 5;
const OP_PIN: u8 = 6;
// 7 was segment importance (enrichment), retired.
const OP_ATTRS: u8 = 8;

fn put_opt_str(out: &mut Vec<u8>, s: Option<&str>) {
    match s {
        Some(s) => {
            out.push(1);
            crate::bytesio::put_str(out, s);
        }
        None => out.push(0),
    }
}

fn read_opt_str(r: &mut Reader<'_>) -> Result<Option<CompactString>> {
    Ok(match r.u8()? {
        0 => None,
        1 => Some(CompactString::from(r.str()?)),
        _ => return Err(Error::format("bad option tag")),
    })
}

impl Op {
    /// The key this op targets.
    #[must_use]
    pub fn key(&self) -> UrlKey {
        match self {
            Self::UpsertNode { key, .. }
            | Self::SetSegments { key, .. }
            | Self::SetEdges { key, .. }
            | Self::Touch { key, .. }
            | Self::Remove { key }
            | Self::SetPinned { key, .. }
            | Self::SetAttrs { key, .. } => *key,
        }
    }

    /// Serialize into `out`.
    #[allow(clippy::too_many_lines)] // one arm per op, mirroring decode
    pub fn encode(&self, out: &mut Vec<u8>) {
        match self {
            Self::UpsertNode {
                key,
                content_hash,
                fetched_at_ms,
                flags,
                url,
                title,
                snippet,
                etag,
            } => {
                out.push(OP_UPSERT);
                put_u64(out, key.0);
                put_u64(out, *content_hash);
                put_u64(out, *fetched_at_ms);
                put_u16(out, *flags);
                crate::bytesio::put_str(out, url);
                put_opt_str(out, title.as_deref());
                put_opt_str(out, snippet.as_deref());
                put_opt_str(out, etag.as_deref());
            }
            Self::SetSegments { key, segs } => {
                out.push(OP_SEGS);
                put_u64(out, key.0);
                out.push(segs.len() as u8);
                for s in segs {
                    crate::bytesio::put_str(out, &s.label);
                    put_u32(out, s.byte_start);
                    put_u32(out, s.byte_len);
                    out.push(s.depth);
                    put_u16(out, s.importance);
                    out.push(s.flags);
                }
            }
            Self::SetEdges { key, edges } => {
                out.push(OP_EDGES);
                put_u64(out, key.0);
                put_u16(out, edges.len() as u16);
                let mut prev: Option<u64> = None;
                for (dst, etype, eflags, weight) in edges {
                    let delta = match prev {
                        None => dst.0,
                        Some(p) => dst.0 - p - 1,
                    };
                    crate::bytesio::put_varint(out, delta);
                    out.push(*etype);
                    out.push(*eflags);
                    put_u16(out, *weight);
                    prev = Some(dst.0);
                }
            }
            Self::Touch {
                key,
                checked_at_ms,
                outcome,
                content_hash,
                etag,
                interval_s,
                checks,
                changes,
                last_change_ms,
                tombstone,
            } => {
                out.push(OP_TOUCH);
                put_u64(out, key.0);
                put_u64(out, *checked_at_ms);
                out.push(*outcome);
                match content_hash {
                    Some(h) => {
                        out.push(1);
                        put_u64(out, *h);
                    }
                    None => out.push(0),
                }
                put_opt_str(out, etag.as_deref());
                put_u32(out, *interval_s);
                put_u16(out, *checks);
                put_u16(out, *changes);
                put_u64(out, *last_change_ms);
                out.push(u8::from(*tombstone));
            }
            Self::Remove { key } => {
                out.push(OP_REMOVE);
                put_u64(out, key.0);
            }
            Self::SetPinned { key, pinned } => {
                out.push(OP_PIN);
                put_u64(out, key.0);
                out.push(u8::from(*pinned));
            }
            Self::SetAttrs { key, attrs } => {
                out.push(OP_ATTRS);
                put_u64(out, key.0);
                out.push(attrs.len() as u8);
                for (k, v) in attrs {
                    out.push(*k);
                    crate::bytesio::put_blob(out, v);
                }
            }
        }
    }

    /// Decode one op from `r`. Fuzz entry point: any byte string yields `Ok`
    /// or `Error::Format`, never a panic. Unknown opcodes are an error —
    /// mutations cannot be safely skipped, so new ops require a WAL version
    /// bump (upgrade path: compact with the old reader, rewrite the header).
    pub fn decode(r: &mut Reader<'_>) -> Result<Self> {
        let opcode = r.u8()?;
        let key = UrlKey(r.u64()?);
        Ok(match opcode {
            OP_UPSERT => Self::UpsertNode {
                key,
                content_hash: r.u64()?,
                fetched_at_ms: r.u64()?,
                flags: r.u16()?,
                url: CompactString::from(r.str()?),
                title: read_opt_str(r)?,
                snippet: read_opt_str(r)?,
                etag: read_opt_str(r)?,
            },
            OP_SEGS => {
                let n = usize::from(r.u8()?);
                if n > 128 {
                    return Err(Error::format("too many segments"));
                }
                let mut segs = Vec::with_capacity(n.min(r.remaining()));
                for _ in 0..n {
                    segs.push(OwnedSeg {
                        label: CompactString::from(r.str()?),
                        byte_start: r.u32()?,
                        byte_len: r.u32()?,
                        depth: r.u8()?,
                        importance: r.u16()?,
                        flags: r.u8()?,
                    });
                }
                Self::SetSegments { key, segs }
            }
            OP_EDGES => {
                let n = usize::from(r.u16()?);
                if n > MAX_EDGES {
                    return Err(Error::format("too many edges"));
                }
                let mut edges = Vec::with_capacity(n.min(r.remaining()));
                let mut prev: Option<u64> = None;
                for _ in 0..n {
                    let delta = r.varint()?;
                    let dst = match prev {
                        None => delta,
                        Some(p) => p
                            .checked_add(1)
                            .and_then(|v| v.checked_add(delta))
                            .ok_or_else(|| Error::format("edge key overflow"))?,
                    };
                    let etype = r.u8()?;
                    let eflags = r.u8()?;
                    let weight = r.u16()?;
                    edges.push((UrlKey(dst), etype, eflags, weight));
                    prev = Some(dst);
                }
                Self::SetEdges { key, edges }
            }
            OP_TOUCH => Self::Touch {
                key,
                checked_at_ms: r.u64()?,
                outcome: r.u8()?,
                content_hash: match r.u8()? {
                    0 => None,
                    1 => Some(r.u64()?),
                    _ => return Err(Error::format("bad option tag")),
                },
                etag: read_opt_str(r)?,
                interval_s: r.u32()?,
                checks: r.u16()?,
                changes: r.u16()?,
                last_change_ms: r.u64()?,
                tombstone: r.u8()? != 0,
            },
            OP_REMOVE => Self::Remove { key },
            OP_PIN => Self::SetPinned { key, pinned: r.u8()? != 0 },
            OP_ATTRS => {
                let n = usize::from(r.u8()?);
                if n > MAX_ATTRS {
                    return Err(Error::format("too many attributes"));
                }
                let mut attrs = Vec::with_capacity(n.min(r.remaining()));
                for _ in 0..n {
                    let k = r.u8()?;
                    let v = r.blob()?;
                    if v.len() > MAX_ATTR_LEN {
                        return Err(Error::format("attribute too long"));
                    }
                    attrs.push((k, CompactString::from(v)));
                }
                Self::SetAttrs { key, attrs }
            }
            other => return Err(Error::format(format!("unknown opcode {other}"))),
        })
    }
}

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
        return Err(Error::format(format!("unsupported wal version {version}")));
    }
    let stored = crate::bytesio::get_u64(bytes, HEADER_LEN - 8);
    if xxh3_64(&bytes[..HEADER_LEN - 8]) != stored {
        return Err(Error::format("wal header checksum mismatch"));
    }
    Ok(crate::bytesio::get_u64(bytes, 16))
}

/// The frame checksum: xxh3 over `seq‖payload`, streamed so neither side
/// needs a concatenated copy (the value is identical to the one-shot hash).
fn frame_sum(seq: u64, payload: &[u8]) -> u64 {
    let mut h = crate::hash::Hasher::new();
    h.update(&seq.to_le_bytes());
    h.update(payload);
    h.digest64()
}

/// Serialize one frame.
#[must_use]
pub fn encode_frame(seq: u64, op: &Op) -> Vec<u8> {
    // Encode the payload in place after a reserved frame head: one buffer.
    let mut out = vec![0u8; FRAME_OVERHEAD];
    op.encode(&mut out);
    let len = out.len() - FRAME_OVERHEAD;
    debug_assert!(len <= MAX_FRAME);
    let sum = frame_sum(seq, &out[FRAME_OVERHEAD..]);
    out[0..4].copy_from_slice(&(len as u32).to_le_bytes());
    out[4..12].copy_from_slice(&sum.to_le_bytes());
    out[12..20].copy_from_slice(&seq.to_le_bytes());
    out
}

/// One intact frame at `pos`: its sequence, op, and the offset just past it.
/// `None` at the first torn, corrupt or undecodable frame (the crash model).
fn next_frame(bytes: &[u8], pos: usize) -> Option<(u64, Op, usize)> {
    if bytes.len() - pos < FRAME_OVERHEAD {
        return None;
    }
    let len = crate::bytesio::get_u32(bytes, pos) as usize;
    if len > MAX_FRAME || bytes.len() - pos - FRAME_OVERHEAD < len {
        return None;
    }
    let sum = crate::bytesio::get_u64(bytes, pos + 4);
    let seq = crate::bytesio::get_u64(bytes, pos + 12);
    let payload = &bytes[pos + FRAME_OVERHEAD..pos + FRAME_OVERHEAD + len];
    if frame_sum(seq, payload) != sum {
        return None;
    }
    let mut r = Reader::new(payload);
    let op = Op::decode(&mut r).ok()?;
    if r.remaining() != 0 {
        return None;
    }
    Some((seq, op, pos + FRAME_OVERHEAD + len))
}

/// Byte length of the intact prefix of a WAL image — exactly the
/// [`Replay::good_len`] that [`replay`] would report, without collecting the
/// ops — or 0 when the header itself is invalid. This is the length that is
/// safe to copy elsewhere: it ends on a frame boundary and every frame in it
/// is committed.
#[must_use]
pub fn committed_len(image: &[u8]) -> usize {
    if parse_header(image).is_err() {
        return 0;
    }
    HEADER_LEN + committed_frames_len(&image[HEADER_LEN..])
}

/// Like [`committed_len`] for a run of frames with no header (for example
/// the bytes appended since a previously measured frame boundary).
#[must_use]
pub fn committed_frames_len(frames: &[u8]) -> usize {
    let mut pos = 0;
    let mut last = None;
    while let Some((seq, _, next)) = next_frame(frames, pos) {
        if last.is_some_and(|l| seq <= l) {
            break; // a regression is corruption, never a committed frame
        }
        last = Some(seq);
        pos = next;
    }
    pos
}

/// The result of replaying a WAL byte image.
#[derive(Debug)]
pub struct Replay {
    /// Ops in order, with their sequence numbers, up to the first tear.
    pub ops: Vec<(u64, Op)>,
    /// Byte length of the intact prefix (header + whole good frames); a
    /// writable opener truncates the file to this length.
    pub good_len: usize,
    /// Base generation this WAL extends.
    pub base_generation: u64,
}

/// Replay a WAL image. Corrupt/torn tails end the replay silently (that is
/// the crash model, not an error); a corrupt *header* is an error.
pub fn replay(bytes: &[u8]) -> Result<Replay> {
    let base_generation = parse_header(bytes)?;
    let (ops, used) = replay_frames(&bytes[HEADER_LEN..])?;
    Ok(Replay { ops, good_len: HEADER_LEN + used, base_generation })
}

/// Replay a run of frames with no header (for example the bytes appended
/// since a previously consumed frame boundary): the ops up to the first tear
/// and the byte length they occupy.
pub fn replay_frames(frames: &[u8]) -> Result<(Vec<(u64, Op)>, usize)> {
    let mut ops: Vec<(u64, Op)> = Vec::new();
    let mut pos = 0;
    while let Some((seq, op, next)) = next_frame(frames, pos) {
        // Sequences must be strictly increasing; a regression means foreign
        // tampering, not a torn tail.
        if ops.last().is_some_and(|&(last, _)| seq <= last) {
            return Err(Error::corrupt("wal sequence regression"));
        }
        ops.push((seq, op));
        pos = next;
    }
    Ok((ops, pos))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_ops() -> Vec<Op> {
        vec![
            Op::UpsertNode {
                key: UrlKey(9),
                content_hash: 1,
                fetched_at_ms: 2,
                flags: 0,
                url: "https://x.dev/a".into(),
                title: Some("A".into()),
                snippet: None,
                etag: Some("\"v1\"".into()),
            },
            Op::SetSegments {
                key: UrlKey(9),
                segs: vec![OwnedSeg {
                    label: "Install".into(),
                    byte_start: 10,
                    byte_len: 200,
                    depth: 1,
                    importance: 60_000,
                    flags: 0,
                }],
            },
            Op::SetEdges {
                key: UrlKey(9),
                edges: vec![(UrlKey(11), 0, 0, 65_535), (UrlKey(400), 2, 3, 100)],
            },
            Op::Touch {
                key: UrlKey(9),
                checked_at_ms: 5,
                outcome: 0,
                content_hash: None,
                etag: None,
                interval_s: 3600,
                checks: 3,
                changes: 1,
                last_change_ms: 4,
                tombstone: false,
            },
            Op::Remove { key: UrlKey(11) },
            Op::SetPinned { key: UrlKey(9), pinned: true },
            Op::SetAttrs {
                key: UrlKey(9),
                attrs: vec![(1, "INT64".into()), (2, "SELECT 1".repeat(10_000).into())],
            },
        ]
    }

    #[test]
    fn ops_round_trip() {
        for op in sample_ops() {
            let mut buf = Vec::new();
            op.encode(&mut buf);
            let mut r = Reader::new(&buf);
            assert_eq!(Op::decode(&mut r).unwrap(), op);
            assert_eq!(r.remaining(), 0);
        }
    }

    #[test]
    fn wal_replays_and_stops_at_tear() {
        let mut image = encode_header(7, 0);
        for (i, op) in sample_ops().into_iter().enumerate() {
            image.extend_from_slice(&encode_frame(i as u64 + 1, &op));
        }
        let full = replay(&image).unwrap();
        assert_eq!(full.ops.len(), 7);
        assert_eq!(full.good_len, image.len());
        assert_eq!(full.base_generation, 7);

        // Truncate at every byte: never a panic, always a valid prefix.
        for cut in HEADER_LEN..image.len() {
            let r = replay(&image[..cut]).unwrap();
            assert!(r.ops.len() <= 7);
            assert!(r.good_len <= cut);
        }
    }

    #[test]
    fn corrupt_frame_ends_replay_and_bad_header_errors() {
        let mut image = encode_header(1, 0);
        image.extend_from_slice(&encode_frame(1, &Op::Remove { key: UrlKey(1) }));
        let good = image.len();
        image.extend_from_slice(&encode_frame(2, &Op::Remove { key: UrlKey(2) }));
        image[good + 6] ^= 0xff; // corrupt second frame checksum region
        let r = replay(&image).unwrap();
        assert_eq!(r.ops.len(), 1);
        assert_eq!(r.good_len, good);

        let mut bad_header = encode_header(1, 0);
        bad_header[3] ^= 0xff;
        assert!(replay(&bad_header).is_err());
    }

    #[test]
    fn committed_len_matches_replay_at_every_cut() {
        let mut image = encode_header(3, 0);
        for (i, op) in sample_ops().into_iter().enumerate() {
            image.extend_from_slice(&encode_frame(i as u64 + 1, &op));
        }
        for cut in 0..=image.len() {
            let want = replay(&image[..cut]).map_or(0, |r| r.good_len);
            assert_eq!(committed_len(&image[..cut]), want, "cut {cut}");
            if cut >= HEADER_LEN {
                assert_eq!(committed_frames_len(&image[HEADER_LEN..cut]) + HEADER_LEN, want);
            }
        }
    }

    #[test]
    fn streamed_frame_checksum_equals_the_one_shot_hash() {
        // Pins byte-compatibility with files written by 0.2.x.
        let op = Op::Remove { key: UrlKey(42) };
        let frame = encode_frame(7, &op);
        let mut payload = Vec::new();
        op.encode(&mut payload);
        let mut one_shot = 7u64.to_le_bytes().to_vec();
        one_shot.extend_from_slice(&payload);
        assert_eq!(crate::bytesio::get_u64(&frame, 4), xxh3_64(&one_shot));
        assert_eq!(&frame[FRAME_OVERHEAD..], &payload[..]);
    }

    #[test]
    fn sequence_regression_is_corrupt() {
        let mut image = encode_header(1, 0);
        image.extend_from_slice(&encode_frame(5, &Op::Remove { key: UrlKey(1) }));
        image.extend_from_slice(&encode_frame(4, &Op::Remove { key: UrlKey(2) }));
        assert!(matches!(replay(&image), Err(Error::Corrupt { .. })));
    }
}
