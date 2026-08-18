// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>
//
//! The link-r bridge (feature `bridge`): absorb a link-r index into the
//! persistent graph, keep it fresh from crawl/refresh reports, and drive
//! link-r's refresh loop from graph-r due-lists.
//!
//! Division of labor: **link-r acquires and ranks** (network I/O, extraction,
//! hybrid search over its own — possibly purely in-memory — index); **graph-r
//! remembers and serves** (durable history, adaptive freshness, local
//! lookups). The shared foreign key is the canonical-URL xxh3, so after
//! [`absorb`] the link-r index can be discarded entirely and every graph-r
//! lookup keeps resolving.
//!
//! The typical loop:
//!
//! ```no_run
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! # use graph_r::{Store, Config};
//! let store = Store::create("kb", Config::default())?;
//! let mut index = link_r::LinkIndex::in_memory()?;
//!
//! // 1. Crawl and absorb (segments + edges + freshness flow in).
//! let report = index.update("https://docs.example.com/").run().await?;
//! graph_r::bridge::ingest_update(&store, &index, &report)?;
//!
//! // 2. Later: revalidate exactly what the graph says is due.
//! let due = graph_r::bridge::due_urls(&store, graph_r::bridge::now_ms(), 64);
//! if !due.is_empty() {
//!     let report = index.refresh().urls(due.iter()).ttl(std::time::Duration::ZERO).run().await?;
//!     graph_r::bridge::ingest_refresh(&store, &index, &report)?;
//! }
//! # Ok(()) }
//! ```

use crate::error::Result;
use crate::key::{EdgeType, UrlKey};
use crate::store::Store;
use crate::ttl::Outcome;
use crate::writer::{DocRecord, SegmentRecord, Touch};
use link_r::facade::{LinkIndex, PageChange, PageOutcome, RefreshReport, UpdateReport};
use std::collections::HashMap;

pub use crate::store::now_ms;

/// What an absorb/ingest pass wrote.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BridgeReport {
    /// Documents created or replaced.
    pub upserted: usize,
    /// Freshness observations recorded.
    pub touched: usize,
    /// Documents tombstoned (source gone).
    pub tombstoned: usize,
    /// Edges written (crawl tier).
    pub edges: usize,
    /// Segments written.
    pub segments: usize,
}

fn gk(k: link_r::UrlKey) -> UrlKey {
    UrlKey(k.raw())
}

/// Deterministic segment-importance prior: heading depth × position decay ×
/// keyword density, quantized to 1/65535 units (max ≈ 2.0 → 65534).
fn seg_importance(depth: u8, ordinal: usize, heading: &str, keywords: &[&str]) -> u16 {
    let base = match depth {
        0 | 1 => 1.0f32,
        2 => 0.72,
        _ => 0.5,
    };
    let pos = 1.0 / (1.0 + 0.15 * ordinal as f32);
    let mut toks = 0u32;
    let mut hits = 0u32;
    crate::query::for_each_token(heading, |t| {
        toks += 1;
        if keywords.iter().any(|k| k.eq_ignore_ascii_case(t)) {
            hits += 1;
        }
    });
    let kd = if toks == 0 { 0.0 } else { hits as f32 / toks as f32 };
    ((base * pos * (1.0 + kd)) * 32_767.0).clamp(1.0, 65_535.0) as u16
}

/// Absorb the entire index: every document, its edges, and its freshness
/// state. Segments require per-page outcomes (they are not persisted in the
/// index file), so prefer [`ingest_update`]/[`ingest_refresh`] right after a
/// crawl; `absorb` is the bulk offload path — after it returns, the link-r
/// index may be dropped or kept purely in memory.
pub fn absorb(store: &Store, index: &LinkIndex) -> Result<BridgeReport> {
    let mut report = BridgeReport::default();
    let mut w = store.writer()?;
    for doc in index.export().map_err(|e| map_linkr(&e))? {
        let m = doc.meta;
        w.upsert_node(&DocRecord {
            url: &m.url,
            url_key: gk(m.url_key),
            content_hash: m.content_hash,
            fetched_at_ms: m.fetched_at_ms,
            title: m.title.as_deref(),
            snippet: (!m.snippet.is_empty()).then_some(m.snippet.as_str()),
            etag: m.etag.as_deref(),
            pinned: m.pinned,
        })?;
        let edges: Vec<(UrlKey, EdgeType, u16)> =
            doc.edges.iter().map(|&e| (gk(e), EdgeType::Link, 65_535)).collect();
        report.edges += edges.len();
        w.set_edges(gk(m.url_key), &edges);
        report.upserted += 1;
    }
    w.commit()?;
    drop(w);
    store.compact()?;
    Ok(report)
}

/// Fold an [`UpdateReport`]'s per-page outcomes into the graph: added and
/// updated pages are (re)upserted with segments and edges, unchanged pages
/// are touched. Compacts afterwards so lookups see the new structure.
pub fn ingest_update(
    store: &Store,
    index: &LinkIndex,
    report: &UpdateReport,
) -> Result<BridgeReport> {
    ingest_pages(store, index, &report.pages)
}

/// Fold a [`RefreshReport`]'s outcomes: updated pages re-upsert, unchanged
/// pages touch (interval grows), removed pages tombstone (interval cut path
/// is driven by `Changed`). Compacts afterwards.
pub fn ingest_refresh(
    store: &Store,
    index: &LinkIndex,
    report: &RefreshReport,
) -> Result<BridgeReport> {
    ingest_pages(store, index, &report.pages)
}

fn map_linkr(e: &link_r::Error) -> crate::Error {
    crate::Error::format(format!("link-r: {e}"))
}

/// The conditional-crawl seeds stored in this graph, in link-r's vocabulary.
///
/// Produced by [`crawl_seed`]; feed both fields straight into
/// `UpdateBuilder::validators` / `known_edges`.
#[derive(Clone, Debug, Default)]
pub struct CrawlSeed {
    /// Stored `ETag` validators by canonical-URL key: each becomes an
    /// `If-None-Match`, so an unchanged page answers 304 and transfers nothing.
    pub validators: Vec<(link_r::UrlKey, compact_str::CompactString)>,
    /// Known outbound edges by canonical-URL key: they keep the crawl frontier
    /// alive through a 304, which yields no body to parse links from.
    pub known_edges: Vec<(link_r::UrlKey, Vec<link_r::Url>)>,
}

/// Extract the conditional-crawl seeds from the graph.
///
/// This is the piece that makes the whole design stateless: the link-r index
/// is meant to be absorbed and *discarded* (bodies and vectors never outlive
/// the session), and this hands the graph's durable `ETag`s and edges back to
/// a fresh index, so a re-crawl after a process restart revalidates for free —
/// zero body transfers for anything unchanged:
///
/// ```no_run
/// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
/// # let store = graph_r::Store::open("kb")?;
/// let seed = graph_r::bridge::crawl_seed(&store);
/// let mut index = link_r::LinkIndex::in_memory()?;
/// let report = index
///     .update("https://docs.example.com/")
///     .validators(seed.validators)
///     .known_edges(seed.known_edges)
///     .run()
///     .await?;
/// graph_r::bridge::ingest_update(&store, &index, &report)?;
/// # Ok(()) }
/// ```
///
/// Tombstoned and stub nodes are excluded — a stub has no URL to crawl, and a
/// tombstone was already observed gone.
#[must_use]
pub fn crawl_seed(store: &Store) -> CrawlSeed {
    let snap = store.snapshot();
    let mut seed = CrawlSeed::default();
    let mut keys: Vec<UrlKey> = Vec::new();
    snap.for_each_live(|n| {
        if let Some(etag) = n.etag {
            seed.validators.push((link_r::UrlKey(n.key.0), etag.into()));
        }
        keys.push(n.key);
    });
    for key in keys {
        let mut children = Vec::new();
        let mut it = snap.neighbors(key);
        while let Some(e) = crate::traverse::LendingIterator::next(&mut it) {
            // A stub target has no URL label yet; there is nothing to hand a
            // crawler for it.
            if let Some(u) = e.dst_url.and_then(|u| link_r::Url::parse(u).ok()) {
                children.push(u);
            }
        }
        if !children.is_empty() {
            seed.known_edges.push((link_r::UrlKey(key.0), children));
        }
    }
    seed
}

fn ingest_pages(store: &Store, index: &LinkIndex, pages: &[PageOutcome]) -> Result<BridgeReport> {
    let mut report = BridgeReport::default();
    if pages.is_empty() {
        return Ok(report);
    }
    let now = now_ms();
    let mut w = store.writer()?;

    // Only Added/Updated pages need document metadata; the touch-only outcomes
    // never read the export. Map those pages by key, then walk the export ONCE,
    // borrowing metadata and edge lists in place — the previous shape
    // materialized the whole export twice (a keyed map cloning every edge list,
    // plus a parallel metadata vec), which made a 64-page refresh against a
    // large index pay for every document it was not touching.
    let wanted: HashMap<u64, &PageOutcome> = pages
        .iter()
        .filter(|p| matches!(p.change, PageChange::Added | PageChange::Updated))
        .map(|p| (p.url_key.raw(), p))
        .collect();

    if !wanted.is_empty() {
        for doc in index.export().map_err(|e| map_linkr(&e))? {
            let Some(page) = wanted.get(&doc.meta.url_key.raw()) else { continue };
            let m = doc.meta;
            let key = UrlKey(m.url_key.raw());
            let changed = page.change == PageChange::Updated;
            w.upsert_node(&DocRecord {
                url: &m.url,
                url_key: key,
                content_hash: m.content_hash,
                fetched_at_ms: m.fetched_at_ms,
                title: m.title.as_deref(),
                snippet: (!m.snippet.is_empty()).then_some(m.snippet.as_str()),
                etag: m.etag.as_deref(),
                pinned: m.pinned,
            })?;
            let keywords: Vec<&str> =
                page.keywords.iter().map(compact_str::CompactString::as_str).collect();
            let segs: Vec<SegmentRecord<'_>> = page
                .headings
                .iter()
                .enumerate()
                .map(|(i, (depth, h))| SegmentRecord {
                    label: h.as_str(),
                    byte_range: None,
                    depth: *depth,
                    importance: seg_importance(*depth, i, h, &keywords),
                })
                .collect();
            report.segments += segs.len();
            w.set_segments(key, &segs)?;
            let graph_edges: Vec<(UrlKey, EdgeType, u16)> =
                doc.edges.iter().map(|&e| (gk(e), EdgeType::Link, 65_535)).collect();
            report.edges += graph_edges.len();
            w.set_edges(key, &graph_edges);
            report.upserted += 1;
            if changed {
                // Cut the revalidation interval: the source is moving. (An
                // Added page has no committed node yet, so a touch would be a
                // no-op — its freshness starts from the upsert itself.)
                if w.touch(
                    key,
                    Touch {
                        checked_at_ms: now,
                        outcome: Outcome::Changed,
                        content_hash: Some(m.content_hash),
                        etag: m.etag.as_deref(),
                    },
                )? {
                    report.touched += 1;
                }
            }
        }
    }

    // Touch-only outcomes: the node already exists committed, so the writer
    // computes each next interval from real prior state.
    for page in pages {
        let key = UrlKey(page.url_key.raw());
        match page.change {
            PageChange::Unchanged => {
                if w.touch(
                    key,
                    Touch {
                        checked_at_ms: now,
                        outcome: Outcome::Unchanged,
                        content_hash: None,
                        etag: None,
                    },
                )? {
                    report.touched += 1;
                }
            }
            PageChange::Removed => {
                // The source is gone; link-r evicted it, graph-r keeps the
                // node as history (tombstone) so lookups can still say "this
                // existed, here is what pointed at it".
                if w.touch(
                    key,
                    Touch {
                        checked_at_ms: now,
                        outcome: Outcome::Gone,
                        content_hash: None,
                        etag: None,
                    },
                )? {
                    report.tombstoned += 1;
                }
            }
            PageChange::Added | PageChange::Updated | PageChange::Skipped => {}
        }
    }
    w.commit()?;
    drop(w);
    store.compact()?;
    Ok(report)
}

/// The URLs most in need of revalidation at `now_ms`, most important first —
/// feed these to `index.refresh().urls(...)`, then fold the report back with
/// [`ingest_refresh`]. Owned strings so the snapshot is released before any
/// network work begins.
#[must_use]
pub fn due_urls(store: &Store, now_ms: u64, max: usize) -> Vec<String> {
    let snap = store.snapshot();
    snap.due(now_ms, max).into_iter().map(|d| d.url.to_owned()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seg_importance_orders_by_depth_position_and_keywords() {
        let h1 = seg_importance(1, 0, "Install", &[]);
        let h2 = seg_importance(2, 0, "Install", &[]);
        let h1_late = seg_importance(1, 5, "Install", &[]);
        let h1_kw = seg_importance(1, 0, "Install", &["install"]);
        assert!(h1 > h2, "H1 above H2");
        assert!(h1 > h1_late, "earlier beats later");
        assert!(h1_kw > h1, "keyword hit boosts");
    }
}
