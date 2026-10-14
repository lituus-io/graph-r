// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
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
