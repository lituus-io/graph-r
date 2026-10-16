// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! # graph-r
//!
//! An embedded, persistent knowledge graph that acts as the durable backend
//! for `link-r`: link-r acquires and ranks (crawl, extract, hybrid search);
//! graph-r remembers and serves (historical graph, adaptive freshness,
//! token-budgeted local lookups). The two share one foreign key — the 64-bit
//! xxh3 of a canonical URL — so a link-r index can be absorbed and then
//! discarded while every lookup keeps resolving.
//!
//! ## Design tenets
//!
//! - **Zero-copy reads.** The base snapshot is an mmapped, checksummed,
//!   sectioned file; lookups binary-search fixed-width records in place.
//! - **Readers never block.** Snapshots pin a generation lock-free; writers
//!   (any number, across processes) take turns appending to a WAL under a
//!   commit-scoped lock and publish into an append-only overlay; compaction
//!   swaps generations under readers without pausing them.
//! - **Lifetimes over `Arc`.** [`Snapshot`], [`NodeRef`], hits, and iterators
//!   are borrow guards tied to the [`Store`]; nothing is reference-counted.
//! - **Determinism.** Compaction is byte-reproducible; ranking, communities,
//!   and query output are fully ordered with explicit tie-breaks.
//! - **References, never bodies.** Nodes hold compact lookup metadata (title,
//!   snippet, anchors, freshness); document bodies and dense vectors stay in
//!   link-r or at the source URL. Lookups answer with URLs + anchors, which
//!   is what keeps repeat token spend near zero.
//!
//! ## Quick start
//!
//! The link-r ingest path is link-r's `graph` feature (`link_r::bridge`),
//! and object-store mirroring is objstore-r's `mirror` feature; both build
//! on this crate's core, so none of them is a dependency here:
//!
//! ```toml
//! uu-graph-r = "0.6"
//! uu-link-r = { version = "0.4", features = ["graph"] }
//! ```
#![deny(unsafe_code)]
#![deny(missing_docs)]
#![deny(missing_debug_implementations)]
#![deny(rust_2018_idioms)]
#![deny(unreachable_pub)]
#![warn(clippy::all, clippy::pedantic, clippy::cargo)]
#![allow(
    clippy::module_name_repetitions,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::multiple_crate_versions,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap
)]

// Core (always built; xxh3 is the only dependency): hashing, keys, byte
// codecs, the checksummed container, errors and file primitives.
pub mod bytesio;
pub mod container;
pub mod error;
pub mod fsio;
pub mod hash;
pub mod key;
pub mod text;

/// Zero-copy read-only images (feature `mmap`).
#[cfg(feature = "mmap")]
pub mod mmap;

// The embedded graph store (feature `store`, on by default).
#[cfg(feature = "store")]
mod compact;
#[cfg(feature = "store")]
pub mod format;
#[cfg(feature = "store")]
mod overlay;
#[cfg(feature = "store")]
pub mod query;
#[cfg(feature = "store")]
pub mod rank;
#[cfg(feature = "store")]
pub mod snapshot;
#[cfg(feature = "store")]
pub mod store;
#[cfg(feature = "store")]
pub mod traverse;
#[cfg(feature = "store")]
pub mod ttl;
#[cfg(feature = "store")]
pub mod writer;

pub use error::{Error, Result};
pub use key::{EdgeType, NodeId, SegKey, UrlKey};
#[cfg(feature = "store")]
pub use query::{Anchor, Hit, QueryOpts, TokenBudget};
#[cfg(feature = "store")]
pub use snapshot::{DueItem, NodeRef, SegRef, Snapshot};
#[cfg(feature = "store")]
pub use store::{CommitLock, Config, Durability, Store};
#[cfg(feature = "store")]
pub use traverse::{Direction, EdgeRef, Hop, LendingIterator, Neighbors, Walk};
#[cfg(feature = "store")]
pub use ttl::{Outcome, TtlConfig};
#[cfg(feature = "store")]
pub use writer::{DocRecord, SegmentRecord, Touch, Txn, Writer};

/// Convenient single-line import of the working set.
#[cfg(feature = "store")]
pub mod prelude {
    pub use crate::{
        Config, DocRecord, Durability, EdgeType, Error, Hit, LendingIterator, NodeId, NodeRef,
        Outcome, QueryOpts, Result, SegmentRecord, Snapshot, Store, TokenBudget, Touch, TtlConfig,
        Txn, UrlKey, Writer,
    };
}
