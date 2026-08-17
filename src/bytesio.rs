// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>
//
//! Little-endian byte codec shared by the base and WAL formats. All readers
//! return [`Error::Format`] rather than panicking on truncated or hostile
//! input, and every length-driven preallocation is capped by
//! [`Reader::remaining`] — the two rules the fuzz targets enforce.

use crate::error::{Error, Result};

/// Round `n` up to the next multiple of `align` (power of two).
#[must_use]
#[allow(clippy::manual_div_ceil)] // alignment idiom, kept explicit
pub fn align_up(n: usize, align: usize) -> usize {
    (n + align - 1) & !(align - 1)
}

/// Append helpers (writers never fail).
pub fn put_u8(out: &mut Vec<u8>, v: u8) {
    out.push(v);
}
/// Append a `u16` LE.
pub fn put_u16(out: &mut Vec<u8>, v: u16) {
    out.extend_from_slice(&v.to_le_bytes());
}
/// Append a `u32` LE.
pub fn put_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes());
}
/// Append a `u64` LE.
pub fn put_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_le_bytes());
}
/// Append a `u16`-length-prefixed UTF-8 string (caller guarantees `<= u16::MAX`).
pub fn put_str(out: &mut Vec<u8>, s: &str) {
    debug_assert!(u16::try_from(s.len()).is_ok());
    put_u16(out, s.len() as u16);
    out.extend_from_slice(s.as_bytes());
}
/// Append an unsigned LEB128 varint.
pub fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

/// Read a `u16` at a known-good offset (caller has already bounds-checked).
#[must_use]
pub fn get_u16(bytes: &[u8], off: usize) -> u16 {
    u16::from_le_bytes([bytes[off], bytes[off + 1]])
}
/// Read a `u32` at a known-good offset.
#[must_use]
pub fn get_u32(bytes: &[u8], off: usize) -> u32 {
    u32::from_le_bytes([bytes[off], bytes[off + 1], bytes[off + 2], bytes[off + 3]])
}
/// Read a `u64` at a known-good offset.
#[must_use]
pub fn get_u64(bytes: &[u8], off: usize) -> u64 {
    let mut b = [0u8; 8];
    b.copy_from_slice(&bytes[off..off + 8]);
    u64::from_le_bytes(b)
}

/// A bounds-checked forward reader over untrusted bytes.
#[derive(Debug)]
pub struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    /// Start reading at the beginning of `bytes`.
    #[must_use]
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    /// Bytes not yet consumed. Decoders cap every `Vec::with_capacity` by
    /// this value so a hostile length field cannot force an allocation
    /// larger than the input itself.
    #[must_use]
    pub fn remaining(&self) -> usize {
        self.bytes.len() - self.pos
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(n)
            .filter(|&e| e <= self.bytes.len())
            .ok_or_else(|| Error::format(format!("truncated: need {n} bytes at {}", self.pos)))?;
        let out = &self.bytes[self.pos..end];
        self.pos = end;
        Ok(out)
    }

    /// Read one byte.
    pub fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    /// Read a `u16` LE.
    pub fn u16(&mut self) -> Result<u16> {
        let b = self.take(2)?;
        Ok(u16::from_le_bytes([b[0], b[1]]))
    }
    /// Read a `u32` LE.
    pub fn u32(&mut self) -> Result<u32> {
        let b = self.take(4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }
    /// Read a `u64` LE.
    pub fn u64(&mut self) -> Result<u64> {
        let b = self.take(8)?;
        let mut a = [0u8; 8];
        a.copy_from_slice(b);
        Ok(u64::from_le_bytes(a))
    }
    /// Read `n` raw bytes.
    pub fn bytes(&mut self, n: usize) -> Result<&'a [u8]> {
        self.take(n)
    }
    /// Read a `u16`-length-prefixed UTF-8 string.
    pub fn str(&mut self) -> Result<&'a str> {
        let len = usize::from(self.u16()?);
        let raw = self.take(len)?;
        std::str::from_utf8(raw).map_err(|_| Error::format("invalid utf-8 in string"))
    }
    /// Read an unsigned LEB128 varint (max 10 bytes).
    pub fn varint(&mut self) -> Result<u64> {
        let mut v: u64 = 0;
        for shift in (0..64).step_by(7) {
            let byte = self.u8()?;
            v |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(v);
            }
        }
        Err(Error::format("varint overlong"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_all_widths() {
        let mut out = Vec::new();
        put_u8(&mut out, 7);
        put_u16(&mut out, 300);
        put_u32(&mut out, 70_000);
        put_u64(&mut out, u64::MAX - 1);
        put_str(&mut out, "héllo");
        put_varint(&mut out, 0);
        put_varint(&mut out, 127);
        put_varint(&mut out, 128);
        put_varint(&mut out, u64::MAX);

        let mut r = Reader::new(&out);
        assert_eq!(r.u8().unwrap(), 7);
        assert_eq!(r.u16().unwrap(), 300);
        assert_eq!(r.u32().unwrap(), 70_000);
        assert_eq!(r.u64().unwrap(), u64::MAX - 1);
        assert_eq!(r.str().unwrap(), "héllo");
        assert_eq!(r.varint().unwrap(), 0);
        assert_eq!(r.varint().unwrap(), 127);
        assert_eq!(r.varint().unwrap(), 128);
        assert_eq!(r.varint().unwrap(), u64::MAX);
        assert_eq!(r.remaining(), 0);
    }

    #[test]
    fn truncation_is_an_error_never_a_panic() {
        let mut out = Vec::new();
        put_str(&mut out, "abcdef");
        for cut in 0..out.len() {
            let mut r = Reader::new(&out[..cut]);
            assert!(r.str().is_err() || cut >= out.len());
        }
        let mut r = Reader::new(&[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]);
        assert!(r.varint().is_err(), "overlong varint rejected");
    }

    #[test]
    fn invalid_utf8_is_a_format_error() {
        let mut out = Vec::new();
        put_u16(&mut out, 2);
        out.extend_from_slice(&[0xff, 0xfe]);
        assert!(Reader::new(&out).str().is_err());
    }

    #[test]
    fn align_up_works() {
        assert_eq!(align_up(0, 64), 0);
        assert_eq!(align_up(1, 64), 64);
        assert_eq!(align_up(64, 64), 64);
        assert_eq!(align_up(65, 64), 128);
    }
}
