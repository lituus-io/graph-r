// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! A checksummed, sectioned, 64-aligned file container: the one on-disk
//! framing behind graph-r's base snapshot and every dependant's images.
//!
//! ```text
//! [header 128 B][directory: 32 B × sections][sections, each 64-aligned]
//! header:  0 magic u32 | 4 version u16 | 6 flags u16 | 8 header_len u32
//!         12 section_count u32 | 16 total_len u64 | 24 directory xxh3 u64
//!         32..120 owner area (88 B, the format's own fields)
//!        120 header xxh3 u64
//! entry:   kind u16 | flags u16 | 0 u32 | offset u64 | length u64 | xxh3 u64
//! ```
//!
//! Every byte is covered by exactly one checksum — header, directory, or its
//! section — so opening validates each byte once. Section lookup is O(1) by
//! kind. Readers hold offsets, not borrows, so a [`Dir`] can live beside the
//! mapping it describes. Unknown kinds are kept and ignored, so a format can
//! grow sections without breaking older readers.

use crate::bytesio::{align_up, get_u16, get_u32, get_u64, put_u16, put_u32, put_u64};
use crate::error::{Error, Result};
use crate::hash::xxh3_64;

/// Header width.
pub const HEADER_LEN: usize = 128;
/// Bytes of the header that belong to the format using the container.
pub const OWNER_LEN: usize = 88;
/// Directory entry width.
pub const DIR_ENTRY_LEN: usize = 32;
/// Section alignment.
pub const ALIGN: usize = 64;
/// Kinds below this resolve in O(1); larger kinds by scanning.
const FAST_KINDS: usize = 32;

const OWNER_AT: usize = 32;
const SUM_AT: usize = HEADER_LEN - 8;

/// Identity of a format: its magic and the one version it reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Format {
    /// Four magic bytes, as a little-endian u32.
    pub magic: u32,
    /// The version written and accepted (strict equality).
    pub version: u16,
}

/// Builds a container: the owner area, then sections in order.
#[derive(Debug)]
pub struct Writer {
    format: Format,
    flags: u16,
    owner: [u8; OWNER_LEN],
    sections: Vec<(u16, u16, Vec<u8>)>,
}

impl Writer {
    /// A writer for `format`.
    #[must_use]
    pub fn new(format: Format) -> Self {
        Self { format, flags: 0, owner: [0; OWNER_LEN], sections: Vec::new() }
    }

    /// Header flags (the format's own meaning).
    pub fn flags(&mut self, flags: u16) -> &mut Self {
        self.flags = flags;
        self
    }

    /// The format's own header fields (up to [`OWNER_LEN`] bytes, rest zero).
    pub fn owner(&mut self, bytes: &[u8]) -> &mut Self {
        let n = bytes.len().min(OWNER_LEN);
        self.owner[..n].copy_from_slice(&bytes[..n]);
        self
    }

    /// Append one section (empty sections are omitted: absent reads as empty).
    pub fn section(&mut self, kind: u16, bytes: Vec<u8>) -> &mut Self {
        self.section_flagged(kind, 0, bytes)
    }

    /// Append one section with per-section flags (the format's own meaning,
    /// e.g. "compressed"), read back by [`Dir::section_flags`].
    pub fn section_flagged(&mut self, kind: u16, flags: u16, bytes: Vec<u8>) -> &mut Self {
        if !bytes.is_empty() {
            self.sections.push((kind, flags, bytes));
        }
        self
    }

    /// The finished file.
    #[must_use]
    pub fn finish(&self) -> Vec<u8> {
        let dir_end = HEADER_LEN + self.sections.len() * DIR_ENTRY_LEN;
        let mut offset = align_up(dir_end, ALIGN);
        let mut dir = Vec::with_capacity(self.sections.len() * DIR_ENTRY_LEN);
        for (kind, flags, bytes) in &self.sections {
            put_u16(&mut dir, *kind);
            put_u16(&mut dir, *flags);
            put_u32(&mut dir, 0);
            put_u64(&mut dir, offset as u64);
            put_u64(&mut dir, bytes.len() as u64);
            put_u64(&mut dir, xxh3_64(bytes));
            offset = align_up(offset + bytes.len(), ALIGN);
        }
        let total = offset.max(dir_end);
        let mut out = Vec::with_capacity(total);
        put_u32(&mut out, self.format.magic);
        put_u16(&mut out, self.format.version);
        put_u16(&mut out, self.flags);
        put_u32(&mut out, HEADER_LEN as u32);
        put_u32(&mut out, self.sections.len() as u32);
        put_u64(&mut out, total as u64);
        put_u64(&mut out, xxh3_64(&dir));
        out.extend_from_slice(&self.owner);
        let sum = xxh3_64(&out[..SUM_AT]);
        put_u64(&mut out, sum);
        out.extend_from_slice(&dir);
        for (_, _, bytes) in &self.sections {
            out.resize(align_up(out.len(), ALIGN), 0);
            out.extend_from_slice(bytes);
        }
        out.resize(total, 0);
        out
    }
}

/// Check only the header of `bytes` (magic, version, header checksum) and
/// return its flags and owner area: a cheap peek at a file's identity fields.
pub fn peek(bytes: &[u8], format: Format) -> Result<(u16, [u8; OWNER_LEN])> {
    check_header(bytes, format)?;
    let mut owner = [0; OWNER_LEN];
    owner.copy_from_slice(&bytes[OWNER_AT..OWNER_AT + OWNER_LEN]);
    Ok((get_u16(bytes, 6), owner))
}

fn check_header(bytes: &[u8], format: Format) -> Result<()> {
    if bytes.len() < HEADER_LEN {
        return Err(Error::format("file shorter than header"));
    }
    if get_u32(bytes, 0) != format.magic {
        return Err(Error::format("bad magic"));
    }
    let version = get_u16(bytes, 4);
    if version != format.version {
        return Err(Error::Version { found: version, supported: format.version });
    }
    if xxh3_64(&bytes[..SUM_AT]) != get_u64(bytes, SUM_AT) {
        return Err(Error::format("header checksum mismatch"));
    }
    if get_u32(bytes, 8) as usize != HEADER_LEN {
        return Err(Error::format("unexpected header length"));
    }
    Ok(())
}

/// A validated container: header fields and section ranges.
#[derive(Clone, Debug)]
pub struct Dir {
    /// Header flags.
    pub flags: u16,
    owner: [u8; OWNER_LEN],
    fast: [(usize, usize, u16); FAST_KINDS],
    other: Vec<(u16, usize, usize, u16)>,
}

impl Dir {
    /// Validate `bytes` as a container of `format`: header, directory and
    /// every section checksummed once, bounds and alignment checked. Any
    /// input yields `Ok`, [`Error::Format`] or [`Error::Version`], never a
    /// panic.
    pub fn parse(bytes: &[u8], format: Format) -> Result<Self> {
        check_header(bytes, format)?;
        if get_u64(bytes, 16) != bytes.len() as u64 {
            return Err(Error::format("total length does not match file size"));
        }
        let count = get_u32(bytes, 12) as usize;
        let dir_end = count
            .checked_mul(DIR_ENTRY_LEN)
            .and_then(|l| l.checked_add(HEADER_LEN))
            .filter(|&e| e <= bytes.len())
            .ok_or_else(|| Error::format("directory exceeds file"))?;
        if xxh3_64(&bytes[HEADER_LEN..dir_end]) != get_u64(bytes, 24) {
            return Err(Error::format("directory checksum mismatch"));
        }
        let mut dir = Self {
            flags: get_u16(bytes, 6),
            owner: [0; OWNER_LEN],
            fast: [(0, 0, 0); FAST_KINDS],
            other: Vec::new(),
        };
        dir.owner.copy_from_slice(&bytes[OWNER_AT..OWNER_AT + OWNER_LEN]);
        let mut spans: Vec<(usize, usize)> = Vec::with_capacity(count.min(64));
        for i in 0..count {
            let e = HEADER_LEN + i * DIR_ENTRY_LEN;
            let (kind, sflags) = (get_u16(bytes, e), get_u16(bytes, e + 2));
            let (offset, length) = (get_u64(bytes, e + 8), get_u64(bytes, e + 16));
            let offset = usize::try_from(offset).map_err(|_| Error::format("section offset"))?;
            let length = usize::try_from(length).map_err(|_| Error::format("section length"))?;
            let end =
                offset.checked_add(length).ok_or_else(|| Error::format("section overflow"))?;
            if offset < dir_end || end > bytes.len() {
                return Err(Error::format("section out of bounds"));
            }
            if !offset.is_multiple_of(ALIGN) {
                return Err(Error::format("section misaligned"));
            }
            if xxh3_64(&bytes[offset..end]) != get_u64(bytes, e + 24) {
                return Err(Error::format("section checksum mismatch"));
            }
            if dir.range(kind).is_some() {
                return Err(Error::format("duplicate section"));
            }
            match dir.fast.get_mut(usize::from(kind)) {
                Some(slot) => *slot = (offset, length, sflags),
                None => dir.other.push((kind, offset, length, sflags)),
            }
            spans.push((offset, end));
        }
        // Alignment padding carries no data and must be zero, so every byte
        // of a file is accounted for and the bytes of a state are unique.
        spans.sort_unstable();
        let mut at = dir_end;
        for (start, end) in spans.into_iter().chain([(bytes.len(), bytes.len())]) {
            if start < at || bytes[at..start].iter().any(|&b| b != 0) {
                return Err(Error::format("sections overlap or padding is not zero"));
            }
            at = end;
        }
        Ok(dir)
    }

    /// The format's own header fields.
    #[must_use]
    pub fn owner(&self) -> &[u8; OWNER_LEN] {
        &self.owner
    }

    /// Byte range of section `kind` (empty when absent), for owners that
    /// keep offsets instead of slices.
    #[must_use]
    pub fn span(&self, kind: u16) -> std::ops::Range<usize> {
        self.range(kind).map_or(0..0, |(o, l)| o..o + l)
    }

    fn entry(&self, kind: u16) -> Option<(usize, usize, u16)> {
        match self.fast.get(usize::from(kind)) {
            Some(&(o, l, f)) => (l > 0).then_some((o, l, f)),
            None => self.other.iter().find(|e| e.0 == kind).map(|&(_, o, l, f)| (o, l, f)),
        }
    }

    fn range(&self, kind: u16) -> Option<(usize, usize)> {
        self.entry(kind).map(|(o, l, _)| (o, l))
    }

    /// The per-section flags of `kind` (0 when absent).
    #[must_use]
    pub fn section_flags(&self, kind: u16) -> u16 {
        self.entry(kind).map_or(0, |e| e.2)
    }

    /// Whether section `kind` is present.
    #[must_use]
    pub fn has(&self, kind: u16) -> bool {
        self.entry(kind).is_some()
    }

    /// Section `kind` of `bytes` (the bytes this directory was parsed from);
    /// empty when absent.
    #[must_use]
    pub fn section<'a>(&self, bytes: &'a [u8], kind: u16) -> &'a [u8] {
        self.range(kind).and_then(|(o, l)| bytes.get(o..o + l)).unwrap_or(&[])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const F: Format = Format { magic: 0x5453_4554, version: 3 };

    #[test]
    fn round_trips_sections_and_owner_fields() {
        let mut w = Writer::new(F);
        w.flags(7).owner(&[1, 2, 3]).section(2, vec![9; 100]).section(40, b"far".to_vec());
        w.section(5, Vec::new());
        let bytes = w.finish();
        assert!(bytes.len().is_multiple_of(ALIGN));
        let d = Dir::parse(&bytes, F).unwrap();
        assert_eq!((d.flags, &d.owner()[..4]), (7, &[1, 2, 3, 0][..]));
        assert_eq!(d.section(&bytes, 2), &[9; 100][..]);
        assert_eq!(d.section(&bytes, 40), b"far");
        assert!(d.section(&bytes, 5).is_empty() && d.section(&bytes, 3).is_empty());
        assert!(d.has(2) && !d.has(5) && d.section_flags(2) == 0);
        let mut w = Writer::new(F);
        w.section_flagged(1, 0b10, vec![1]).section_flagged(50, 3, vec![2]);
        let bytes = w.finish();
        let d = Dir::parse(&bytes, F).unwrap();
        assert_eq!((d.section_flags(1), d.section_flags(50), d.section_flags(9)), (0b10, 3, 0));
        let empty = Writer::new(F).finish();
        assert_eq!(empty.len(), HEADER_LEN);
        assert!(Dir::parse(&empty, F).is_ok());
    }

    #[test]
    fn every_byte_is_checked() {
        let mut w = Writer::new(F);
        w.owner(&[5; 10]).section(1, vec![1; 70]).section(2, vec![2; 3]);
        let bytes = w.finish();
        for i in 0..bytes.len() {
            let mut bad = bytes.clone();
            bad[i] ^= 0x40;
            assert!(Dir::parse(&bad, F).is_err(), "byte {i}");
        }
        assert!(Dir::parse(&bytes, Format { version: 4, ..F }).is_err());
        assert!(Dir::parse(&bytes[..bytes.len() - 1], F).is_err());
    }
}
