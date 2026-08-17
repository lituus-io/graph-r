// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>
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
use crate::key::UrlKey;
use compact_str::CompactString;
use xxhash_rust::xxh3::xxh3_64;

/// Magic: `"GRPW"` little-endian.
pub const MAGIC: u32 = 0x5750_5247;
/// WAL format version; strict-equal on read.
pub const VERSION: u16 = 1;
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
    /// Enrichment writeback: per-segment-ordinal importance overrides.
    SetImportance { key: UrlKey, scores: Vec<(u8, u16)> },
}

const OP_UPSERT: u8 = 1;
const OP_SEGS: u8 = 2;
const OP_EDGES: u8 = 3;
const OP_TOUCH: u8 = 4;
const OP_REMOVE: u8 = 5;
const OP_PIN: u8 = 6;
const OP_IMPORTANCE: u8 = 7;

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
            | Self::SetImportance { key, .. } => *key,
        }
    }

    /// Serialize into `out`.
    pub fn encode(&self, out: &mut Vec<u8>) {
        match self {
            Self::UpsertNode { key, content_hash, fetched_at_ms, flags, url, title, snippet, etag } => {
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
                out.push(edges.len() as u8);
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
            Self::SetImportance { key, scores } => {
                out.push(OP_IMPORTANCE);
                put_u64(out, key.0);
                out.push(scores.len() as u8);
                for (ordinal, importance) in scores {
                    out.push(*ordinal);
                    put_u16(out, *importance);
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
                let n = usize::from(r.u8()?);
                if n > 64 {
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
            OP_IMPORTANCE => {
                let n = usize::from(r.u8()?);
                let mut scores = Vec::with_capacity(n.min(r.remaining()));
                for _ in 0..n {
                    let ordinal = r.u8()?;
                    scores.push((ordinal, r.u16()?));
                }
                Self::SetImportance { key, scores }
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

/// Serialize one frame.
#[must_use]
pub fn encode_frame(seq: u64, op: &Op) -> Vec<u8> {
    let mut payload = Vec::new();
    op.encode(&mut payload);
    debug_assert!(payload.len() <= MAX_FRAME);
    let mut sum_input = Vec::with_capacity(8 + payload.len());
    sum_input.extend_from_slice(&seq.to_le_bytes());
    sum_input.extend_from_slice(&payload);
    let mut out = Vec::with_capacity(FRAME_OVERHEAD + payload.len());
    put_u32(&mut out, payload.len() as u32);
    put_u64(&mut out, xxh3_64(&sum_input));
    put_u64(&mut out, seq);
    out.extend_from_slice(&payload);
    out
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
    let mut ops = Vec::new();
    let mut pos = HEADER_LEN;
    loop {
        if bytes.len() - pos < FRAME_OVERHEAD {
            break;
        }
        let len = crate::bytesio::get_u32(bytes, pos) as usize;
        if len > MAX_FRAME || bytes.len() - pos - FRAME_OVERHEAD < len {
            break;
        }
        let sum = crate::bytesio::get_u64(bytes, pos + 4);
        let seq = crate::bytesio::get_u64(bytes, pos + 12);
        let payload = &bytes[pos + FRAME_OVERHEAD..pos + FRAME_OVERHEAD + len];
        let mut sum_input = Vec::with_capacity(8 + len);
        sum_input.extend_from_slice(&seq.to_le_bytes());
        sum_input.extend_from_slice(payload);
        if xxh3_64(&sum_input) != sum {
            break;
        }
        let mut r = Reader::new(payload);
        let Ok(op) = Op::decode(&mut r) else { break };
        if r.remaining() != 0 {
            break;
        }
        // Sequences must be strictly increasing; a regression means foreign
        // tampering, not a torn tail.
        if let Some(&(last, _)) = ops.last() {
            if seq <= last {
                return Err(Error::corrupt("wal sequence regression"));
            }
        }
        ops.push((seq, op));
        pos += FRAME_OVERHEAD + len;
    }
    Ok(Replay { ops, good_len: pos, base_generation })
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
            Op::SetImportance { key: UrlKey(9), scores: vec![(0, 1000), (3, 65_535)] },
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
    fn sequence_regression_is_corrupt() {
        let mut image = encode_header(1, 0);
        image.extend_from_slice(&encode_frame(5, &Op::Remove { key: UrlKey(1) }));
        image.extend_from_slice(&encode_frame(4, &Op::Remove { key: UrlKey(2) }));
        assert!(matches!(replay(&image), Err(Error::Corrupt { .. })));
    }
}
