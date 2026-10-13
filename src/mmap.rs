// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
//! Read-only images for zero-copy readers: a memory-mapped file, or bytes
//! already in memory (an object fetched from storage) served in place. The
//! crate's single unsafe island (`#![deny(unsafe_code)]` at the root; this
//! module opts out locally).
//!
//! # Safety envelope
//!
//! A mapping is only ever taken over files that are replaced atomically by
//! rename and never truncated in place by a cooperating process (graph-r's
//! base, dependants' immutable images). In-place truncation by a foreign
//! actor is outside the contract; a SIGBUS is then accepted.

#![allow(unsafe_code)]

use crate::error::Result;
use std::fs::File;
use std::path::Path;

/// A read-only base image: a memory-mapped file, or bytes already in memory
/// (an object fetched from storage) served in place.
pub struct MappedFile {
    backing: Backing,
}

enum Backing {
    Map(memmap2::Mmap),
    Heap(bytes::Bytes),
}

impl std::fmt::Debug for MappedFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let kind = match self.backing {
            Backing::Map(_) => "mmap",
            Backing::Heap(_) => "heap",
        };
        f.debug_struct("MappedFile").field("kind", &kind).field("len", &self.bytes().len()).finish()
    }
}

impl MappedFile {
    /// Map the file at `path` read-only.
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        // SAFETY: the file is mapped read-only. Cooperating processes replace
        // it only via atomic rename (the old inode stays alive under this
        // map); hostile in-place truncation faults, which we accept.
        let mmap = unsafe { memmap2::Mmap::map(&file)? };
        Ok(Self { backing: Backing::Map(mmap) })
    }

    /// Serve `bytes` in place (no copy; no file, so no SIGBUS exposure).
    #[must_use]
    pub fn from_bytes(bytes: bytes::Bytes) -> Self {
        Self { backing: Backing::Heap(bytes) }
    }

    /// The image bytes.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        match &self.backing {
            Backing::Map(m) => m,
            Backing::Heap(b) => b,
        }
    }
}

/// View `bytes` as `f32`s in place (zero per-element decode), for formats
/// that 64-align their vector sections. Errors, never UB, when the length is
/// not a whole number of floats or the memory is not `f32`-aligned.
pub fn cast_f32(bytes: &[u8]) -> Result<&[f32]> {
    const _: () = assert!(cfg!(target_endian = "little"), "images are little-endian");
    if !bytes.len().is_multiple_of(4) {
        return Err(crate::Error::format("f32 section length is not a multiple of 4"));
    }
    if !(bytes.as_ptr() as usize).is_multiple_of(std::mem::align_of::<f32>()) {
        return Err(crate::Error::format("f32 section is not aligned"));
    }
    // SAFETY: `bytes` is `len * 4` initialized bytes, verified f32-aligned;
    // every bit pattern is a valid f32; the result borrows `bytes`.
    #[allow(clippy::cast_ptr_alignment)] // verified above
    Ok(unsafe { std::slice::from_raw_parts(bytes.as_ptr().cast::<f32>(), bytes.len() / 4) })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heap_images_are_served_in_place() {
        let b = bytes::Bytes::from_static(b"image");
        let m = MappedFile::from_bytes(b.clone());
        assert_eq!(m.bytes().as_ptr(), b.as_ptr(), "no copy");
    }

    #[test]
    fn mmap_reads_file_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("f");
        std::fs::write(&p, b"abc123").unwrap();
        let m = MappedFile::open(&p).unwrap();
        assert_eq!(m.bytes(), b"abc123");
    }

    #[test]
    fn f32_views_check_length_and_alignment() {
        let v: Vec<f32> = vec![1.0, -2.5, 3.25];
        let raw: Vec<u8> = v.iter().flat_map(|f| f.to_le_bytes()).collect();
        let mut aligned = vec![0u32; 4];
        let buf = as_bytes(&mut aligned);
        buf[..12].copy_from_slice(&raw);
        assert_eq!(cast_f32(&buf[..12]).unwrap(), &v[..]);
        assert!(cast_f32(&buf[..11]).is_err());
        assert!(cast_f32(&buf[1..13]).is_err());
    }

    fn as_bytes(words: &mut [u32]) -> &mut [u8] {
        // SAFETY: u8 has alignment 1, every byte is valid; same allocation.
        unsafe { std::slice::from_raw_parts_mut(words.as_mut_ptr().cast::<u8>(), words.len() * 4) }
    }
}
