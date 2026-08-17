// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>
//
//! On-disk formats. Two files, one discipline:
//!
//! - [`base`] — `graph.base`, the immutable mmapped snapshot: checksummed
//!   128-byte header, section directory, 64-byte-aligned sections, and a
//!   whole-file xxh3 trailer, written via tmp + fsync + rename.
//! - [`wal`] — `graph.wal`, the append-only op log: checksummed frames whose
//!   replay stops at the first torn frame.
//!
//! Every decoder returns [`crate::Error::Format`] on malformed input and caps
//! preallocation by the bytes actually remaining — never trust a length field.

pub mod base;
pub mod wal;

/// Section alignment inside `graph.base`.
pub const ALIGN: usize = 64;
/// Directory entry width (kind u16, flags u16, reserved u32, offset u64,
/// length u64, xxh3 u64).
pub const DIR_ENTRY_LEN: usize = 32;
/// Whole-file trailer width (xxh3 of everything before it).
pub const TRAILER_LEN: usize = 8;
