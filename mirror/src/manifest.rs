// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>
//
//! The `HEAD` object: which base and which WAL byte ranges make up the
//! published store, plus the optional writer lease. A small checksummed
//! binary record (the same style as graph-r's own files), written only by
//! generation-conditional puts.
//!
//! ```text
//! magic "GRPM" u32 | version u16 | flags u16 | generation u64
//! base_len u64 | base_sum u64 | lease_expires_ms u64 | holder_len u16 | holder
//! seg_count u32 | seg_count × (start u64 | len u64 | sum u64) | xxh3 u64
//! ```

use crate::{Error, Result};
use xxhash_rust::xxh3::xxh3_64;

const MAGIC: u32 = 0x4D50_5247; // "GRPM"
const VERSION: u16 = 1;
/// Upper bound on segments a manifest may list (publish coalesces long
/// before this; the bound keeps a hostile HEAD from forcing a huge alloc).
pub(crate) const MAX_SEGMENTS: usize = 4096;
/// Upper bound on the lease holder name.
pub(crate) const MAX_HOLDER: usize = 256;

/// One published WAL byte range: `[start, start + len)` of `graph.wal`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Segment {
    /// Offset in `graph.wal` (0 for the first, which carries the header).
    pub start: u64,
    /// Length in bytes; always ends on a committed frame boundary.
    pub len: u64,
    /// xxh3 of the bytes.
    pub sum: u64,
}

impl Segment {
    pub(crate) fn end(&self) -> u64 {
        self.start + self.len
    }
}

/// The writer lease recorded in `HEAD`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Lease {
    /// Who holds it.
    pub holder: String,
    /// When it lapses, Unix ms.
    pub expires_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub(crate) struct Manifest {
    /// graph-r base generation; 0 = nothing published yet (lease-only HEAD).
    pub(crate) generation: u64,
    pub(crate) base_len: u64,
    pub(crate) base_sum: u64,
    pub(crate) lease: Option<Lease>,
    pub(crate) segments: Vec<Segment>,
}

impl Manifest {
    pub(crate) fn wal_len(&self) -> u64 {
        self.segments.last().map_or(0, Segment::end)
    }

    pub(crate) fn encode(&self) -> Vec<u8> {
        let holder = self.lease.as_ref().map_or("", |l| l.holder.as_str());
        let mut o = Vec::with_capacity(64 + holder.len() + self.segments.len() * 24);
        o.extend_from_slice(&MAGIC.to_le_bytes());
        o.extend_from_slice(&VERSION.to_le_bytes());
        o.extend_from_slice(&0u16.to_le_bytes());
        o.extend_from_slice(&self.generation.to_le_bytes());
        o.extend_from_slice(&self.base_len.to_le_bytes());
        o.extend_from_slice(&self.base_sum.to_le_bytes());
        o.extend_from_slice(&self.lease.as_ref().map_or(0, |l| l.expires_ms).to_le_bytes());
        o.extend_from_slice(&(holder.len() as u16).to_le_bytes());
        o.extend_from_slice(holder.as_bytes());
        o.extend_from_slice(&(self.segments.len() as u32).to_le_bytes());
        for s in &self.segments {
            o.extend_from_slice(&s.start.to_le_bytes());
            o.extend_from_slice(&s.len.to_le_bytes());
            o.extend_from_slice(&s.sum.to_le_bytes());
        }
        let sum = xxh3_64(&o);
        o.extend_from_slice(&sum.to_le_bytes());
        o
    }

    /// Strict parse: any deviation is [`Error::Corrupt`], never a panic.
    pub(crate) fn decode(b: &[u8]) -> Result<Self> {
        let bad = |m: &str| Error::Corrupt { message: format!("HEAD: {m}") };
        if b.len() < 8 {
            return Err(bad("truncated"));
        }
        let (body, tail) = b.split_at(b.len() - 8);
        if xxh3_64(body) != u64::from_le_bytes(tail.try_into().map_err(|_| bad("truncated"))?) {
            return Err(bad("checksum mismatch"));
        }
        let mut r = Cursor { b: body, pos: 0 };
        if r.u32()? != MAGIC {
            return Err(bad("bad magic"));
        }
        let version = r.u16()?;
        if version != VERSION {
            return Err(bad(&format!("unsupported version {version}")));
        }
        let _flags = r.u16()?;
        let generation = r.u64()?;
        let base_len = r.u64()?;
        let base_sum = r.u64()?;
        let expires_ms = r.u64()?;
        let holder_len = usize::from(r.u16()?);
        if holder_len > MAX_HOLDER {
            return Err(bad("lease holder too long"));
        }
        let holder =
            std::str::from_utf8(r.take(holder_len)?).map_err(|_| bad("lease holder not utf-8"))?;
        let lease = (!holder.is_empty()).then(|| Lease { holder: holder.to_owned(), expires_ms });
        let count = r.u32()? as usize;
        if count > MAX_SEGMENTS || count * 24 != body.len() - r.pos {
            return Err(bad("segment table size"));
        }
        let mut segments = Vec::with_capacity(count);
        let mut end = 0u64;
        for _ in 0..count {
            let s = Segment { start: r.u64()?, len: r.u64()?, sum: r.u64()? };
            if s.start != end || s.len == 0 {
                return Err(bad("segments are not contiguous"));
            }
            end = s.start.checked_add(s.len).ok_or_else(|| bad("segment overflow"))?;
            segments.push(s);
        }
        if generation == 0 && (!segments.is_empty() || base_len != 0) {
            return Err(bad("data without a generation"));
        }
        if generation != 0 && segments.is_empty() {
            return Err(bad("generation without a log"));
        }
        Ok(Self { generation, base_len, base_sum, lease, segments })
    }
}

struct Cursor<'a> {
    b: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self.pos.checked_add(n).filter(|&e| e <= self.b.len());
        let end = end.ok_or_else(|| Error::Corrupt { message: "HEAD: truncated".into() })?;
        let s = &self.b[self.pos..end];
        self.pos = end;
        Ok(s)
    }
    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap_or_default()))
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap_or_default()))
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap_or_default()))
    }
}

/// Fuzzing entry point: any bytes parse or fail cleanly, and whatever parses
/// re-encodes to exactly the same bytes.
#[doc(hidden)]
pub fn fuzz(b: &[u8]) {
    if let Ok(m) = Manifest::decode(b) {
        assert_eq!(m.encode(), b);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Manifest {
        Manifest {
            generation: 7,
            base_len: 1234,
            base_sum: 99,
            lease: Some(Lease { holder: "instance-a".into(), expires_ms: 5 }),
            segments: vec![
                Segment { start: 0, len: 64, sum: 1 },
                Segment { start: 64, len: 40, sum: 2 },
            ],
        }
    }

    #[test]
    fn round_trips_and_rejects_every_corruption() {
        let m = sample();
        let b = m.encode();
        assert_eq!(Manifest::decode(&b).unwrap(), m);
        assert_eq!(Manifest::decode(&Manifest::default().encode()).unwrap(), Manifest::default());
        for i in 0..b.len() {
            let mut c = b.clone();
            c[i] ^= 0x40;
            assert!(Manifest::decode(&c).is_err(), "flip at {i}");
        }
        for cut in 0..b.len() {
            assert!(Manifest::decode(&b[..cut]).is_err(), "cut at {cut}");
        }
        fuzz(&b);
    }

    #[test]
    fn structural_rules_hold_even_with_a_valid_checksum() {
        let mut m = sample();
        m.segments[1].start = 65;
        assert!(Manifest::decode(&m.encode()).is_err(), "gap");
        let mut m = sample();
        m.segments.clear();
        assert!(Manifest::decode(&m.encode()).is_err(), "generation without log");
        let m = Manifest { generation: 0, base_len: 5, ..Manifest::default() };
        assert!(Manifest::decode(&m.encode()).is_err());
    }
}
