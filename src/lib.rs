// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>
//
//! # graph-r
//!
//! An embedded, persistent knowledge graph that acts as the durable backend
//! for [`link-r`]: link-r acquires and ranks (crawl, extract, hybrid search);
//! graph-r remembers and serves (historical graph, adaptive freshness,
//! token-budgeted local lookups). The two share one foreign key — the 64-bit
//! xxh3 of a canonical URL — so a link-r index can be absorbed and then
//! discarded while every lookup keeps resolving.
//!
//! ## Design tenets
//!
//! - **Zero-copy reads.** The base snapshot is an mmapped, checksummed,
//!   sectioned file; lookups binary-search fixed-width records in place.
//! - **Readers never block.** Snapshots pin a generation lock-free; a single
//!   writer appends to a WAL and publishes into an append-only overlay;
//!   compaction swaps generations under readers without pausing them.
//! - **Lifetimes over `Arc`.** [`Snapshot`], [`NodeRef`], hits, and iterators
//!   are borrow guards tied to the [`Store`]; nothing is reference-counted.
//! - **Determinism.** Compaction is byte-reproducible; ranking, communities,
//!   and query output are fully ordered with explicit tie-breaks.
//! - **References, never bodies.** Nodes hold compact lookup metadata (title,
//!   snippet, anchors, freshness); document bodies and dense vectors stay in
//!   link-r or at the source URL. Lookups answer with URLs + anchors, which
//!   is what keeps repeat token spend near zero.
//!
//! ## Quick start (with link-r, feature `bridge`)
//!
//! ```toml
//! graph-r = { path = "../graph-r", features = ["bridge"] }
//! ```
//!
//! ```no_run
//! # #[cfg(feature = "bridge")]
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! use graph_r::{Config, Store};
//!
//! let store = Store::create("kb", Config::default())?;
//! let mut index = link_r::LinkIndex::in_memory()?;
//! index.update("https://docs.example.com/guide").run().await?;
//! graph_r::bridge::absorb(&store, &index)?;          // offload: graph-r now serves alone
//!
//! let snap = store.snapshot();
//! for hit in snap.query("install on linux", &graph_r::QueryOpts::default()) {
//!     println!("{} ({})", hit.url, hit.score);
//! }
//! # Ok(()) }
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

pub mod bytesio;
mod compact;
pub mod error;
pub mod format;
pub mod key;
mod os;
mod overlay;
pub mod query;
pub mod rank;
pub mod snapshot;
pub mod store;
pub mod traverse;
pub mod ttl;
pub mod writer;

#[cfg(feature = "bridge")]
pub mod bridge;
#[cfg(feature = "llm")]
pub mod enrich;

pub use error::{Error, Result};
pub use key::{EdgeType, NodeId, SegKey, UrlKey};
pub use query::{Anchor, Hit, QueryOpts, TokenBudget};
pub use snapshot::{DueItem, NodeRef, SegRef, Snapshot};
pub use store::{Config, Durability, Store};
pub use traverse::{EdgeRef, LendingIterator, Neighbors};
pub use ttl::{Outcome, TtlConfig};
pub use writer::{DocRecord, SegmentRecord, Touch, Writer};

#[cfg(feature = "llm")]
pub use enrich::{EnrichContext, Enricher, Enrichment};

/// Convenient single-line import of the working set.
pub mod prelude {
    pub use crate::{
        Config, DocRecord, Durability, EdgeType, Error, Hit, LendingIterator, NodeId, NodeRef,
        Outcome, QueryOpts, Result, SegmentRecord, Snapshot, Store, TokenBudget, Touch, TtlConfig,
        UrlKey, Writer,
    };
}
