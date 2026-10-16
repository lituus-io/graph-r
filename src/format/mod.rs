// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! On-disk formats. Two files, one discipline:
//!
//! - [`base`] — `graph.base`, the immutable mmapped snapshot: checksummed
//!   128-byte header, section directory, 64-byte-aligned sections, and a
//!   whole-file xxh3 trailer, written via tmp + fsync + rename.
//! - [`wal`] — `graph.wal`, the append-only log: one checksummed record per
//!   transaction, read in place; replay stops at the first torn record, and a
//!   record whose checksum holds but whose contents do not is corrupt.
//!
//! Every decoder returns [`crate::Error::Format`] on malformed input (and
//! [`crate::Error::Version`] on a well-formed header of another version) and caps
//! preallocation by the bytes actually remaining — never trust a length field.

pub mod base;
pub mod wal;
