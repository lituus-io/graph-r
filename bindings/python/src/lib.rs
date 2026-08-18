// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>
//
//! Python bindings for `graph-r` (PyO3 + maturin).
//!
//! One class, one loop. [`PyStore`] wraps the embedded store together with a
//! private Tokio runtime, so the whole acquire-and-remember cycle is a single
//! Python call: `store.sync(root)` seeds a fresh in-memory link-r index from
//! the graph's stored validators, crawls (unchanged pages answer 304 and
//! transfer nothing), ingests, compacts, and discards the index — bodies and
//! vectors never outlive the call. Lookups (`query`, `related`, `due`) answer
//! with URLs and anchors, never content.
//!
//! Every method detaches from the interpreter around Rust work, and the class
//! is deliberately not `unsendable`: consumers dispatch through thread pools
//! (`asyncio.to_thread`), so the object must be usable from any thread.
//!
//! The wheel is built against the stable ABI (`abi3-py312`), so one artifact
//! per platform serves every Python from 3.12 up.

use graphr::bridge::{self, link_r};
use graphr::{
    Config, DocRecord, Outcome, QueryOpts, SegmentRecord, Store, TokenBudget, Touch, UrlKey,
};
use pyo3::create_exception;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use std::path::Path;
use std::time::Duration;

create_exception!(graph_r, GraphRError, pyo3::exceptions::PyException);

fn map_err(e: graphr::Error) -> PyErr {
    GraphRError::new_err(e.to_string())
}

fn map_linkr(e: link_r::Error) -> PyErr {
    GraphRError::new_err(format!("link-r: {e}"))
}

/// A revalidation outcome, from the caller's vocabulary. Argument validation
/// is a `ValueError`; store failures are `GraphRError`.
fn parse_outcome(s: &str) -> PyResult<Outcome> {
    match s {
        "unchanged" => Ok(Outcome::Unchanged),
        "changed" => Ok(Outcome::Changed),
        "error" => Ok(Outcome::Error),
        "gone" => Ok(Outcome::Gone),
        other => Err(PyValueError::new_err(format!(
            "outcome must be one of 'unchanged', 'changed', 'error', 'gone'; got {other:?}"
        ))),
    }
}

fn parse_scope(scope: Option<&str>) -> PyResult<link_r::source::CrawlScope> {
    match scope {
        None | Some("path") => Ok(link_r::source::CrawlScope::PathPrefix),
        Some("host") | Some("same_host") => Ok(link_r::source::CrawlScope::SameHost),
        Some("subdomains") => Ok(link_r::source::CrawlScope::SameHostAndSubdomains),
        Some(other) => Err(PyValueError::new_err(format!(
            "scope must be one of 'path', 'host', 'subdomains'; got {other:?}"
        ))),
    }
}

/// A sub-document anchor on a hit: fetch the URL and resolve the heading (or
/// slice the byte range, when known) instead of re-reading the whole page.
#[pyclass(name = "Anchor", frozen, skip_from_py_object)]
#[derive(Clone)]
struct PyAnchor {
    #[pyo3(get)]
    label: String,
    #[pyo3(get)]
    importance: u16,
    #[pyo3(get)]
    byte_range: Option<(u32, u32)>,
}

#[pymethods]
impl PyAnchor {
    fn __repr__(&self) -> String {
        format!("Anchor(label={:?}, importance={})", self.label, self.importance)
    }
}

/// One lookup answer: a reference, never a body.
#[pyclass(name = "Hit", frozen, skip_from_py_object)]
#[derive(Clone)]
struct PyHit {
    #[pyo3(get)]
    url: String,
    #[pyo3(get)]
    title: Option<String>,
    #[pyo3(get)]
    snippet: Option<String>,
    #[pyo3(get)]
    score: f32,
    #[pyo3(get)]
    seed: bool,
    #[pyo3(get)]
    anchor: Option<PyAnchor>,
}

#[pymethods]
impl PyHit {
    fn __repr__(&self) -> String {
        format!("Hit(score={:.3}, url={:?})", self.score, self.url)
    }
}

impl From<graphr::Hit<'_>> for PyHit {
    fn from(h: graphr::Hit<'_>) -> Self {
        Self {
            url: h.url.to_owned(),
            title: h.title.map(str::to_owned),
            snippet: h.snippet.map(str::to_owned),
            score: h.score,
            seed: h.seed,
            anchor: h.anchor.map(|a| PyAnchor {
                label: a.label.to_owned(),
                importance: a.importance,
                byte_range: a.byte_range,
            }),
        }
    }
}

/// One entry of the due-for-revalidation work list.
#[pyclass(name = "DueItem", frozen, skip_from_py_object)]
#[derive(Clone)]
struct PyDueItem {
    #[pyo3(get)]
    url: String,
    #[pyo3(get)]
    etag: Option<String>,
    #[pyo3(get)]
    overdue_ms: u64,
    #[pyo3(get)]
    rank_permille: u16,
}

#[pymethods]
impl PyDueItem {
    fn __repr__(&self) -> String {
        format!("DueItem(url={:?}, overdue_ms={})", self.url, self.overdue_ms)
    }
}

/// What one `sync` did: the crawl side and the graph side of the same pass.
#[pyclass(name = "SyncReport", frozen, skip_from_py_object)]
#[derive(Clone, Copy)]
struct PySyncReport {
    #[pyo3(get)]
    added: usize,
    #[pyo3(get)]
    updated: usize,
    #[pyo3(get)]
    unchanged: usize,
    #[pyo3(get)]
    skipped: usize,
    #[pyo3(get)]
    failed: usize,
    #[pyo3(get)]
    upserted: usize,
    #[pyo3(get)]
    touched: usize,
    #[pyo3(get)]
    tombstoned: usize,
    #[pyo3(get)]
    edges: usize,
    #[pyo3(get)]
    segments: usize,
}

#[pymethods]
impl PySyncReport {
    fn __repr__(&self) -> String {
        format!(
            "SyncReport(added={}, updated={}, unchanged={}, failed={}, upserted={}, touched={})",
            self.added, self.updated, self.unchanged, self.failed, self.upserted, self.touched
        )
    }
}

/// An embedded, persistent knowledge-graph store.
///
/// Deliberately not `unsendable`: `Store` and the runtime are both `Send`,
/// and consumers dispatch through thread pools (`asyncio.to_thread`).
#[pyclass(name = "Store")]
struct PyStore {
    inner: Store,
    rt: tokio::runtime::Runtime,
}

fn runtime() -> PyResult<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| GraphRError::new_err(format!("tokio runtime: {e}")))
}

fn wrap(inner: Store) -> PyResult<PyStore> {
    Ok(PyStore { inner, rt: runtime()? })
}

#[pymethods]
impl PyStore {
    /// Create a fresh store in `path` (errors if one already exists there).
    #[staticmethod]
    fn create(path: String) -> PyResult<Self> {
        wrap(Store::create(&path, Config::default()).map_err(map_err)?)
    }

    /// Open an existing store read-write (errors if missing or locked).
    #[staticmethod]
    fn open(path: String) -> PyResult<Self> {
        wrap(Store::open(&path).map_err(map_err)?)
    }

    /// Open the store at `path`, creating it first if it does not exist.
    #[staticmethod]
    fn open_or_create(path: String) -> PyResult<Self> {
        let exists = Path::new(&path).join("graph.base").exists();
        let inner =
            if exists { Store::open(&path) } else { Store::create(&path, Config::default()) };
        wrap(inner.map_err(map_err)?)
    }

    /// Open read-only: takes no lock, so it coexists with a live writer in
    /// another process. Mutating methods raise `GraphRError`.
    #[staticmethod]
    fn open_read_only(path: String) -> PyResult<Self> {
        wrap(Store::open_read_only(&path).map_err(map_err)?)
    }

    /// Crawl `root` and absorb the result — the whole loop in one call.
    ///
    /// A fresh in-memory link-r index is seeded with the graph's stored
    /// validators and edges, so pages this store has seen before revalidate
    /// with `If-None-Match` and transfer no body when unchanged. The crawl's
    /// outcome flows into the graph (new/changed pages upserted with segments
    /// and edges, unchanged pages' freshness intervals grown, gone pages
    /// tombstoned), the store compacts, and the index is discarded — bodies
    /// and vectors never outlive the call.
    ///
    /// `scope` is one of `'path'` (default), `'host'`, `'subdomains'`.
    /// `path_contains` confines *crawling*; `index_path_contains` narrows
    /// *indexing* the same way; `extensions` (e.g. `['md']`) indexes only
    /// matching pages while still following others for links. `token` sets a
    /// bearer credential scoped to the root's host.
    #[pyo3(signature = (root, depth=2, max_pages=1000, concurrency=8, token=None, scope=None, min_delay_ms=0, path_contains=None, extensions=None, index_path_contains=None, pin=false))]
    #[allow(clippy::too_many_arguments)] // a flat keyword surface is the point for Python
    fn sync(
        &self,
        py: Python<'_>,
        root: String,
        depth: u16,
        max_pages: usize,
        concurrency: usize,
        token: Option<String>,
        scope: Option<String>,
        min_delay_ms: u64,
        path_contains: Option<Vec<String>>,
        extensions: Option<Vec<String>>,
        index_path_contains: Option<Vec<String>>,
        pin: bool,
    ) -> PyResult<PySyncReport> {
        let crawl_scope = parse_scope(scope.as_deref())?;
        py.detach(|| {
            self.rt.block_on(async {
                let seed = bridge::crawl_seed(&self.inner);
                let mut index = link_r::LinkIndex::in_memory().map_err(map_linkr)?;
                let mut update = index
                    .update(root)
                    .depth(depth)
                    .max_pages(max_pages)
                    .concurrency(concurrency)
                    .scope(crawl_scope)
                    .min_delay(Duration::from_millis(min_delay_ms))
                    .validators(seed.validators)
                    .known_edges(seed.known_edges);
                for substring in path_contains.into_iter().flatten() {
                    update = update.require_path(substring);
                }
                for ext in extensions.into_iter().flatten() {
                    update = update.accept_extension(ext);
                }
                for substring in index_path_contains.into_iter().flatten() {
                    update = update.index_path(substring);
                }
                if let Some(t) = token {
                    update = update.token(t);
                }
                if pin {
                    update = update.pin();
                }
                let crawl = update.run().await.map_err(map_linkr)?;
                let ingest = bridge::ingest_update(&self.inner, &index, &crawl).map_err(map_err)?;
                Ok(PySyncReport {
                    added: crawl.added,
                    updated: crawl.updated,
                    unchanged: crawl.unchanged,
                    skipped: crawl.skipped,
                    failed: crawl.failed,
                    upserted: ingest.upserted,
                    touched: ingest.touched,
                    tombstoned: ingest.tombstoned,
                    edges: ingest.edges,
                    segments: ingest.segments,
                })
            })
        })
    }

    /// Answer a plain-language query with ranked URL + anchor references,
    /// rendered inside `budget_tokens`. Never returns document bodies.
    #[pyo3(signature = (text, k=20, depth=3, budget_tokens=2000))]
    fn query(
        &self,
        py: Python<'_>,
        text: String,
        k: usize,
        depth: u8,
        budget_tokens: u32,
    ) -> Vec<PyHit> {
        py.detach(|| {
            let snap = self.inner.snapshot();
            let opts = QueryOpts {
                limit: k,
                depth,
                budget: TokenBudget(budget_tokens),
                ..QueryOpts::default()
            };
            snap.query(&text, &opts).into_iter().map(PyHit::from).collect()
        })
    }

    /// The `k` documents most related to `url` (outbound links and similarity
    /// edges), strongest first.
    #[pyo3(signature = (url, k=10))]
    fn related(&self, py: Python<'_>, url: String, k: usize) -> Vec<PyHit> {
        py.detach(|| {
            let snap = self.inner.snapshot();
            snap.related(UrlKey::of(&url), k).into_iter().map(PyHit::from).collect()
        })
    }

    /// Everything due for revalidation right now, most important first, with
    /// the stored validator to send as `If-None-Match`. `sync` consumes this
    /// implicitly; it is exposed for callers driving their own fetch loop.
    #[pyo3(signature = (limit=64, now_ms=None))]
    fn due(&self, py: Python<'_>, limit: usize, now_ms: Option<u64>) -> Vec<PyDueItem> {
        py.detach(|| {
            let now = now_ms.unwrap_or_else(bridge::now_ms);
            let snap = self.inner.snapshot();
            snap.due(now, limit)
                .into_iter()
                .map(|d| PyDueItem {
                    url: d.url.to_owned(),
                    etag: d.etag.map(str::to_owned),
                    overdue_ms: d.overdue_ms,
                    rank_permille: d.rank_permille,
                })
                .collect()
        })
    }

    /// Record a document from any producer — the writer seam for corpora that
    /// do not arrive through `sync`. One call stages the document, its segment
    /// anchors, and its outbound links, then commits.
    ///
    /// `segments` is a list of `(label, depth, importance)`; `links` a list of
    /// target URLs (stored as crawl-tier edges).
    #[pyo3(signature = (url, *, content_hash, title=None, snippet=None, etag=None, pinned=false, fetched_at_ms=None, segments=None, links=None))]
    #[allow(clippy::too_many_arguments)] // a flat keyword surface is the point for Python
    fn add(
        &self,
        py: Python<'_>,
        url: String,
        content_hash: u64,
        title: Option<String>,
        snippet: Option<String>,
        etag: Option<String>,
        pinned: bool,
        fetched_at_ms: Option<u64>,
        segments: Option<Vec<(String, u8, u16)>>,
        links: Option<Vec<String>>,
    ) -> PyResult<()> {
        py.detach(|| {
            let key = UrlKey::of(&url);
            let mut w = self.inner.writer().map_err(map_err)?;
            w.upsert_node(&DocRecord {
                url: &url,
                url_key: key,
                content_hash,
                fetched_at_ms: fetched_at_ms.unwrap_or_else(bridge::now_ms),
                title: title.as_deref(),
                snippet: snippet.as_deref(),
                etag: etag.as_deref(),
                pinned,
            })
            .map_err(map_err)?;
            if let Some(segs) = &segments {
                let records: Vec<SegmentRecord<'_>> = segs
                    .iter()
                    .map(|(label, depth, importance)| SegmentRecord {
                        label,
                        byte_range: None,
                        depth: *depth,
                        importance: *importance,
                    })
                    .collect();
                w.set_segments(key, &records).map_err(map_err)?;
            }
            if let Some(links) = &links {
                let edges: Vec<(UrlKey, graphr::EdgeType, u16)> =
                    links.iter().map(|l| (UrlKey::of(l), graphr::EdgeType::Link, 65_535)).collect();
                w.set_edges(key, &edges);
            }
            w.commit().map_err(map_err)?;
            Ok(())
        })
    }

    /// Record a revalidation outcome for `url`: one of `'unchanged'`,
    /// `'changed'` (with the new `content_hash`/`etag`), `'error'`, or
    /// `'gone'` (tombstones the document, preserving its history). Returns
    /// False if the URL is not a committed document.
    #[pyo3(signature = (url, outcome, *, content_hash=None, etag=None, checked_at_ms=None))]
    fn touch(
        &self,
        py: Python<'_>,
        url: String,
        outcome: String,
        content_hash: Option<u64>,
        etag: Option<String>,
        checked_at_ms: Option<u64>,
    ) -> PyResult<bool> {
        let outcome = parse_outcome(&outcome)?;
        py.detach(|| {
            let mut w = self.inner.writer().map_err(map_err)?;
            let recorded = w
                .touch(
                    UrlKey::of(&url),
                    Touch {
                        checked_at_ms: checked_at_ms.unwrap_or_else(bridge::now_ms),
                        outcome,
                        content_hash,
                        etag: etag.as_deref(),
                    },
                )
                .map_err(map_err)?;
            w.commit().map_err(map_err)?;
            Ok(recorded)
        })
    }

    /// Pin `url`: exempt from eviction sweeps (still revalidated on its TTL).
    /// Returns False if the URL is not a committed document.
    fn pin(&self, py: Python<'_>, url: String) -> PyResult<bool> {
        self.set_pin(py, &url, true)
    }

    /// Clear the pin on `url`. Returns False if the URL is unknown.
    fn unpin(&self, py: Python<'_>, url: String) -> PyResult<bool> {
        self.set_pin(py, &url, false)
    }

    /// Hard-delete `url`: severs its history, edges, and segments at the next
    /// compaction. A later re-add is a new document at the same address.
    fn remove(&self, py: Python<'_>, url: String) -> PyResult<()> {
        py.detach(|| {
            let mut w = self.inner.writer().map_err(map_err)?;
            w.remove(UrlKey::of(&url)).map_err(map_err)?;
            w.commit().map_err(map_err)?;
            Ok(())
        })
    }

    /// Fold the write-ahead log into a fresh immutable base now (also happens
    /// automatically past the configured thresholds, and after every `sync`).
    /// Returns the new generation number.
    fn compact(&self, py: Python<'_>) -> PyResult<u64> {
        py.detach(|| self.inner.compact().map(|s| s.generation).map_err(map_err))
    }

    /// The base generation currently serving reads.
    #[getter]
    fn generation(&self, py: Python<'_>) -> u64 {
        py.detach(|| self.inner.snapshot().generation())
    }

    /// Live (non-tombstoned) document count.
    fn __len__(&self, py: Python<'_>) -> usize {
        py.detach(|| self.inner.len())
    }
}

impl PyStore {
    fn set_pin(&self, py: Python<'_>, url: &str, pinned: bool) -> PyResult<bool> {
        py.detach(|| {
            let mut w = self.inner.writer().map_err(map_err)?;
            let changed = w.set_pinned(UrlKey::of(url), pinned).map_err(map_err)?;
            w.commit().map_err(map_err)?;
            Ok(changed)
        })
    }
}

/// The `graph_r` Python module.
#[pymodule]
fn graph_r(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyStore>()?;
    m.add_class::<PyHit>()?;
    m.add_class::<PyAnchor>()?;
    m.add_class::<PyDueItem>()?;
    m.add_class::<PySyncReport>()?;
    m.add("GraphRError", m.py().get_type::<GraphRError>())?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
