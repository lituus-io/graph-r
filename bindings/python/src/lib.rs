// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
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
//! The lineage model rides on the same store: typed edges (`set_edges`,
//! `out_edges`, `in_edges`), node attributes (`set_attrs`, `attrs`) and a
//! breadth-first `walk`, plus exhaustive enumeration (`urls`). Reads borrow
//! from the snapshot until the one copy each string needs to become a Python
//! object.
//!
//! Every method detaches from the interpreter around Rust work, and the class
//! is deliberately not `unsendable`: consumers dispatch through thread pools
//! (`asyncio.to_thread`), so the object must be usable from any thread.
//!
//! Writes are transactions. Each write method is a one-op transaction (the
//! commit lock is held for its append only, and its checks run against the
//! caught-up store under that lock); `with store.transaction() as tx:` holds
//! the lock across a block of reads and writes and commits them as one
//! append. Any number of processes may hold a writable `Store` on one path;
//! a writer that cannot get the lock within ten seconds raises
//! `LockedError`.
//!
//! The wheel is built against the stable ABI (`abi3-py312`), so one artifact
//! per platform serves every Python from 3.12 up.

use graphr::format::base::eflags;
use graphr::{
    CommitLock, Config, DocRecord, EdgeType, LendingIterator as _, Outcome, QueryOpts,
    SegmentRecord, Snapshot, Store, TokenBudget, Touch, Txn, UrlKey, Walk,
};
use linkr::{self as link_r, bridge};
use pyo3::create_exception;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::sync::{Condvar, Mutex, PoisonError, RwLock};
use std::thread::ThreadId;
use std::time::{Duration, Instant};

create_exception!(graph_r, GraphRError, pyo3::exceptions::PyException);
// A subclass, so callers that catch `GraphRError` keep working.
create_exception!(graph_r, FormatVersionError, GraphRError);
// Another writer held the commit lock past the wait: retry later. A subclass
// of `GraphRError`, so existing handlers keep catching it.
create_exception!(graph_r, LockedError, GraphRError);

fn map_err(e: graphr::Error) -> PyErr {
    match e {
        graphr::Error::Locked => LockedError::new_err(e.to_string()),
        e => GraphRError::new_err(e.to_string()),
    }
}

/// [`map_err`] for the paths that can meet another build's files (opens and
/// reloads): a format-version mismatch becomes a typed `FormatVersionError`
/// carrying `found` and `supported`, so callers never parse the message.
/// Needs the interpreter to set the attributes, so it runs after `detach`.
fn map_open_err(py: Python<'_>, e: graphr::Error) -> PyErr {
    let graphr::Error::Version { found, supported } = e else { return map_err(e) };
    let err = FormatVersionError::new_err(e.to_string());
    let value = err.value(py);
    match value.setattr("found", found).and_then(|()| value.setattr("supported", supported)) {
        Ok(()) => err,
        Err(set) => set,
    }
}

fn closed() -> PyErr {
    GraphRError::new_err("store is closed")
}

/// A wait in seconds from a Python number (an `int`, a `float`, or any
/// number whose `str()` is a decimal): finite and `>= 0`, else
/// `ValueError`. Read as the decimal text Python prints (for a float, the
/// shortest that round-trips) and rounded to whole microseconds with integer
/// arithmetic, which links neither float conversion nor float parsing into
/// the wheel.
fn seconds(v: &Bound<'_, PyAny>) -> PyResult<Duration> {
    let bad = || PyValueError::new_err("a wait must be a finite number of seconds >= 0");
    if v.is_instance_of::<pyo3::types::PyString>() {
        return Err(bad());
    }
    let text = v.str()?;
    micros(text.to_str()?.trim()).map(Duration::from_micros).ok_or_else(bad)
}

/// `text` (`digits[.digits][e[+-]digits]`) in whole microseconds, rounded
/// half up; `None` for anything else (a sign, `inf`, `nan`) or past `u64`.
#[inline]
fn micros(text: &str) -> Option<u64> {
    let (mantissa, exp) = match text.split_once(['e', 'E']) {
        Some((m, e)) => (m, e.parse::<i32>().ok()?),
        None => (text, 0),
    };
    let (whole, frac) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if whole.is_empty() && frac.is_empty() || whole.len() + frac.len() > 36 {
        return None;
    }
    let mut digits: u128 = 0;
    for c in whole.bytes().chain(frac.bytes()) {
        digits = digits * 10 + u128::from(c.checked_sub(b'0').filter(|d| *d < 10)?);
    }
    let scale = exp.checked_sub(i32::try_from(frac.len()).ok()?)?.checked_add(6)?;
    let us = if scale >= 0 {
        digits.checked_mul(10u128.checked_pow(scale.unsigned_abs())?)?
    } else if scale < -38 {
        0
    } else {
        let div = 10u128.pow(scale.unsigned_abs());
        (digits + div / 2) / div
    };
    u64::try_from(us).ok()
}

/// How long a write waits for another writer: `lock_wait` seconds, else
/// the store's own default (ten seconds).
#[inline]
fn lock_wait(v: Option<&Bound<'_, PyAny>>) -> PyResult<Duration> {
    v.map_or(Ok(Config::default().lock_wait), seconds)
}

/// The configuration a writable handle opens with.
#[inline]
fn writable(v: Option<&Bound<'_, PyAny>>) -> PyResult<Config> {
    Ok(Config { lock_wait: lock_wait(v)?, ..Config::default() })
}

/// Edges per node a `set_edges` call may carry (the store's own cap).
const MAX_EDGES: usize = graphr::writer::MAX_EDGES;
/// What `walk` returns at most when the caller sets no `limit`.
const WALK_LIMIT: usize = 10_000;

/// The edge-type vocabulary, by on-disk tag. `set_edges` accepts the three
/// lineage kinds; reads and `walk(kinds=...)` know all six.
const KINDS: [(&str, EdgeType); 6] = [
    ("link", EdgeType::Link),
    ("related", EdgeType::Related),
    ("redirect", EdgeType::Redirect),
    ("contains", EdgeType::Contains),
    ("derives", EdgeType::Derives),
    ("conditions", EdgeType::Conditions),
];

fn kind_name(t: EdgeType) -> &'static str {
    KINDS.iter().find(|(_, k)| *k == t).map_or("unknown", |(n, _)| n)
}

fn parse_kind(s: &str) -> PyResult<EdgeType> {
    KINDS.iter().find(|(n, _)| *n == s).map(|(_, k)| *k).ok_or_else(|| {
        PyValueError::new_err(format!(
            "kind must be one of 'link', 'related', 'redirect', 'contains', 'derives', \
             'conditions'; got {s:?}"
        ))
    })
}

fn parse_lineage_kind(s: &str) -> PyResult<EdgeType> {
    match s {
        "contains" => Ok(EdgeType::Contains),
        "derives" => Ok(EdgeType::Derives),
        "conditions" => Ok(EdgeType::Conditions),
        other => Err(PyValueError::new_err(format!(
            "kind must be one of 'contains', 'derives', 'conditions'; got {other:?}"
        ))),
    }
}

/// Which way a walk goes, from the caller's vocabulary.
#[derive(Clone, Copy)]
enum Way {
    Out,
    In,
    Both,
}

fn parse_direction(s: &str) -> PyResult<Way> {
    match s {
        "out" => Ok(Way::Out),
        "in" => Ok(Way::In),
        "both" => Ok(Way::Both),
        other => Err(PyValueError::new_err(format!(
            "direction must be one of 'out', 'in', 'both'; got {other:?}"
        ))),
    }
}

/// The url of `key` when it is a committed document (tombstoned included:
/// tombstoning keeps history, and a tombstone's edges stay). Stubs and
/// removed nodes have none.
fn doc_url<'a>(snap: &'a Snapshot<'_>, key: UrlKey) -> Option<&'a str> {
    snap.node(key).filter(|n| !n.is_stub() && !n.url.is_empty()).map(|n| n.url)
}

thread_local! {
    /// One reusable walk state per thread: once its buffers have grown, a
    /// walk allocates nothing per step.
    static WALK: RefCell<Walk> = RefCell::new(Walk::new());
}

/// Strings copied once out of a snapshot into one buffer. A snapshot is not
/// `Send`, so nothing it lends can leave `detach`; this is the single copy
/// (and the single allocation) between the mmap and the Python objects.
#[derive(Default)]
struct Strs {
    buf: String,
    ends: Vec<usize>,
}

impl Strs {
    fn with_capacity(bytes: usize, n: usize) -> Self {
        Self { buf: String::with_capacity(bytes), ends: Vec::with_capacity(n) }
    }

    fn of<'a>(items: impl Iterator<Item = &'a str> + Clone) -> Self {
        let (bytes, n) = items.clone().fold((0, 0), |(b, n), s| (b + s.len(), n + 1));
        let mut out = Self::with_capacity(bytes, n);
        for s in items {
            out.push(s);
        }
        out
    }

    fn push(&mut self, s: &str) {
        self.buf.push_str(s);
        self.ends.push(self.buf.len());
    }

    fn iter(&self) -> impl Iterator<Item = &str> + '_ {
        let starts = std::iter::once(0).chain(self.ends.iter().copied());
        starts.zip(self.ends.iter()).map(|(a, &b)| &self.buf[a..b])
    }
}

/// `(url, kind, exact)` edges, with their urls in one buffer.
struct Edges {
    urls: Strs,
    meta: Vec<(EdgeType, bool)>,
}

impl Edges {
    fn into_py<'py>(self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        PyList::new(
            py,
            self.urls.iter().zip(&self.meta).map(|(url, &(t, exact))| (url, kind_name(t), exact)),
        )
    }
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
///
/// The handle sits behind a lock so `close()` can release the mapping and
/// the writer lock deterministically: every call holds the read side for its
/// duration (one uncontended atomic), `close()` takes the write side, waits
/// for calls in flight on other threads, and drops the store.
#[pyclass(name = "Store")]
struct PyStore {
    open: RwLock<Option<Open>>,
    gate: Gate,
    /// How long a write (and a `transaction()` by default) waits for
    /// another writer.
    lock_wait: Duration,
}

/// Orders this handle's own writes around an open `transaction()`. The
/// transaction holds the commit lock between Python calls; a plain write on
/// the same handle meanwhile would wait for that lock while holding the
/// handle's writer, and the transaction's commit would then wait for it. So
/// plain writes wait here instead (bounded, like the lock), and a plain write
/// from the transaction's own thread is refused at once — it could never
/// proceed. Readers never come here.
#[derive(Default)]
struct Gate {
    state: Mutex<GateState>,
    changed: Condvar,
}

#[derive(Default)]
struct GateState {
    /// Plain writes in flight on this handle.
    writes: usize,
    /// The thread that opened the transaction in progress, if any.
    txn: Option<ThreadId>,
}

/// A plain write admitted through the gate; leaving lets a transaction in.
struct Ticket<'g>(&'g Gate);

impl Drop for Ticket<'_> {
    fn drop(&mut self) {
        self.0.state.lock().unwrap_or_else(PoisonError::into_inner).writes -= 1;
        self.0.changed.notify_all();
    }
}

impl Gate {
    /// Wait (up to `wait`) until `busy` is false; `LockedError` past it.
    fn wait_until_free<'g>(
        &'g self,
        mut st: std::sync::MutexGuard<'g, GateState>,
        wait: Duration,
        busy: impl Fn(&GateState) -> bool,
    ) -> PyResult<std::sync::MutexGuard<'g, GateState>> {
        let deadline = Instant::now() + wait;
        while busy(&st) {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Err(LockedError::new_err(
                    "a transaction on this store is still open (waited past lock_wait)",
                ));
            }
            st = self.changed.wait_timeout(st, left).unwrap_or_else(PoisonError::into_inner).0;
        }
        Ok(st)
    }

    fn write(&self, wait: Duration) -> PyResult<Ticket<'_>> {
        let st = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if st.txn == Some(std::thread::current().id()) {
            return Err(GraphRError::new_err(
                "a transaction is open on this store in this thread: write through it",
            ));
        }
        let mut st = self.wait_until_free(st, wait, |s| s.txn.is_some())?;
        st.writes += 1;
        Ok(Ticket(self))
    }

    fn begin(&self, wait: Duration) -> PyResult<()> {
        let st = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if st.txn == Some(std::thread::current().id()) {
            return Err(GraphRError::new_err("transactions on one store do not nest"));
        }
        let mut st = self.wait_until_free(st, wait, |s| s.txn.is_some() || s.writes > 0)?;
        st.txn = Some(std::thread::current().id());
        Ok(())
    }

    fn end(&self) {
        self.state.lock().unwrap_or_else(PoisonError::into_inner).txn = None;
        self.changed.notify_all();
    }
}

/// One staged write, owned: what a write method or a transaction's call
/// records, applied inside a [`Txn`] at commit.
enum Cmd {
    Add {
        url: String,
        content_hash: u64,
        title: Option<String>,
        snippet: Option<String>,
        etag: Option<String>,
        pinned: bool,
        fetched_at_ms: u64,
        segments: Option<Vec<(String, u8, u16)>>,
        links: Option<Vec<String>>,
    },
    Touch {
        key: UrlKey,
        outcome: Outcome,
        content_hash: Option<u64>,
        etag: Option<String>,
        checked_at_ms: u64,
    },
    Pin {
        key: UrlKey,
        pinned: bool,
    },
    Remove {
        key: UrlKey,
    },
    SetEdges {
        url: String,
        dsts: Vec<String>,
        staged: Vec<(UrlKey, EdgeType, u16, u8)>,
    },
    SetAttrs {
        url: String,
        mapping: HashMap<String, String>,
    },
}

/// Documents added and removed by earlier commands of the same transaction:
/// its view shows committed state only, so existence checks consult both.
#[derive(Default)]
struct Staged {
    added: HashSet<u64>,
    removed: HashSet<u64>,
}

impl Staged {
    fn is_document(&self, snap: &Snapshot<'_>, key: UrlKey) -> bool {
        self.added.contains(&key.0)
            || (!self.removed.contains(&key.0) && doc_url(snap, key).is_some())
    }

    fn note(&mut self, cmd: &Cmd) {
        match cmd {
            Cmd::Add { url, .. } => {
                let k = UrlKey::of(url).0;
                self.removed.remove(&k);
                self.added.insert(k);
            }
            Cmd::Remove { key } => {
                self.added.remove(&key.0);
                self.removed.insert(key.0);
            }
            _ => {}
        }
    }

    /// The checks a command must pass before it is staged: argument
    /// validation is a `ValueError`, a missing document a `GraphRError`.
    fn check(&self, snap: &Snapshot<'_>, cmd: &Cmd) -> PyResult<()> {
        match cmd {
            Cmd::SetEdges { url, dsts, staged } => {
                if !self.is_document(snap, bridge::key_of(url)) {
                    return Err(not_a_document(url));
                }
                match staged.iter().position(|e| !self.is_document(snap, e.0)) {
                    Some(i) => Err(not_a_document(&dsts[i])),
                    None => Ok(()),
                }
            }
            Cmd::SetAttrs { url, .. } if !self.is_document(snap, bridge::key_of(url)) => {
                Err(not_a_document(url))
            }
            _ => Ok(()),
        }
    }
}

/// Stage `cmd` in `tx`; what the write method returns (`touch`, `pin`).
fn apply(tx: &mut Txn<'_>, staged: &mut Staged, cmd: &Cmd) -> PyResult<bool> {
    staged.check(tx.snapshot(), cmd)?;
    let out = match cmd {
        Cmd::Add {
            url,
            content_hash,
            title,
            snippet,
            etag,
            pinned,
            fetched_at_ms,
            segments,
            links,
        } => {
            let key = UrlKey::of(url);
            tx.upsert_node(&DocRecord {
                url,
                url_key: key,
                content_hash: *content_hash,
                fetched_at_ms: *fetched_at_ms,
                title: title.as_deref(),
                snippet: snippet.as_deref(),
                etag: etag.as_deref(),
                pinned: *pinned,
            })
            .map_err(map_err)?;
            if let Some(segs) = segments {
                let records: Vec<SegmentRecord<'_>> = segs
                    .iter()
                    .map(|(label, depth, importance)| SegmentRecord {
                        label,
                        byte_range: None,
                        depth: *depth,
                        importance: *importance,
                    })
                    .collect();
                tx.set_segments(key, &records).map_err(map_err)?;
            }
            if let Some(links) = links {
                let edges: Vec<(UrlKey, EdgeType, u16)> =
                    links.iter().map(|l| (bridge::key_of(l), EdgeType::Link, 65_535)).collect();
                tx.set_edges(key, &edges);
            }
            true
        }
        Cmd::Touch { key, outcome, content_hash, etag, checked_at_ms } => tx
            .touch(
                *key,
                Touch {
                    checked_at_ms: *checked_at_ms,
                    outcome: *outcome,
                    content_hash: *content_hash,
                    etag: etag.as_deref(),
                },
            )
            .map_err(map_err)?,
        Cmd::Pin { key, pinned } => tx.set_pinned(*key, *pinned).map_err(map_err)?,
        Cmd::Remove { key } => {
            tx.remove(*key).map_err(map_err)?;
            true
        }
        Cmd::SetEdges { url, staged, .. } => {
            tx.set_edges_with_flags(bridge::key_of(url), staged);
            true
        }
        Cmd::SetAttrs { url, mapping } => {
            let pairs: Vec<(&str, &str)> =
                mapping.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
            // The writer refuses only the shape of the set (sizes, names).
            tx.set_named_attrs(bridge::key_of(url), &pairs)
                .map_err(|e| PyValueError::new_err(e.to_string()))?;
            true
        }
    };
    staged.note(cmd);
    Ok(out)
}

/// Commit `cmds` as one transaction (under `lock` when the caller holds it):
/// one lock, one append. Any failing command aborts the lot with nothing
/// written. Returns the last command's result.
fn commit(store: &Store, lock: Option<&CommitLock>, cmds: &[Cmd]) -> PyResult<bool> {
    let mut failed: Option<PyErr> = None;
    let body = |tx: &mut Txn<'_>| -> graphr::Result<bool> {
        let mut staged = Staged::default();
        let mut last = false;
        for cmd in cmds {
            match apply(tx, &mut staged, cmd) {
                Ok(out) => last = out,
                Err(e) => {
                    failed = Some(e);
                    return Err(graphr::Error::format("aborted"));
                }
            }
        }
        Ok(last)
    };
    let r = match lock {
        Some(l) => store.transact_with(l, body),
        None => store.transact(body),
    };
    match (r, failed) {
        (_, Some(e)) => Err(e),
        (Ok(out), None) => Ok(out),
        (Err(e), None) => Err(map_err(e)),
    }
}

struct Open {
    inner: Store,
    rt: tokio::runtime::Runtime,
}

fn runtime() -> PyResult<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| GraphRError::new_err(format!("tokio runtime: {e}")))
}

/// One SyncReport from the crawl-side and graph-side halves of a sync.
fn merge_report(
    crawl: &link_r::facade::UpdateReport,
    ingest: &bridge::BridgeReport,
) -> PySyncReport {
    PySyncReport {
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
    }
}

fn wrap(inner: Store, lock_wait: Duration) -> PyResult<PyStore> {
    Ok(PyStore {
        open: RwLock::new(Some(Open { inner, rt: runtime()? })),
        gate: Gate::default(),
        lock_wait,
    })
}

#[pymethods]
impl PyStore {
    /// Create a fresh store in `path` (errors if one already exists there).
    /// `lock_wait` (seconds; default ten) is how long a write waits for
    /// another writer before `LockedError`.
    #[staticmethod]
    #[pyo3(signature = (path, lock_wait=None))]
    fn create(
        py: Python<'_>,
        path: String,
        lock_wait: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let cfg = writable(lock_wait)?;
        let inner = py.detach(|| Store::create(&path, cfg));
        wrap(inner.map_err(|e| map_open_err(py, e))?, cfg.lock_wait)
    }

    /// Open an existing store read-write (errors if missing;
    /// `FormatVersionError` if another format version wrote it). Takes no
    /// lock: any number of writable handles, in any number of processes,
    /// may be open on one path. `lock_wait` as for `create`.
    #[staticmethod]
    #[pyo3(signature = (path, lock_wait=None))]
    fn open(py: Python<'_>, path: String, lock_wait: Option<&Bound<'_, PyAny>>) -> PyResult<Self> {
        let cfg = writable(lock_wait)?;
        let inner = py.detach(|| Store::open_cfg(&path, cfg));
        wrap(inner.map_err(|e| map_open_err(py, e))?, cfg.lock_wait)
    }

    /// Open the store at `path`, creating it first if it does not exist.
    /// Safe when many processes race to create it: one creates, the others
    /// wait for it (up to `lock_wait` seconds, default ten) and open the
    /// result.
    #[staticmethod]
    #[pyo3(signature = (path, lock_wait=None))]
    fn open_or_create(
        py: Python<'_>,
        path: String,
        lock_wait: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let cfg = writable(lock_wait)?;
        let inner = py.detach(|| Store::open_or_create(&path, cfg));
        wrap(inner.map_err(|e| map_open_err(py, e))?, cfg.lock_wait)
    }

    /// Open read-only: takes no lock, so it coexists with a live writer in
    /// another process. Mutating methods raise `GraphRError`.
    #[staticmethod]
    fn open_read_only(py: Python<'_>, path: String) -> PyResult<Self> {
        let inner = Store::open_read_only(&path).map_err(|e| map_open_err(py, e))?;
        wrap(inner, Config::default().lock_wait)
    }

    /// Acquire `source` and absorb the result — the whole loop in one call.
    ///
    /// Two source forms, routed automatically:
    ///
    /// - **A GitHub repository spec** — `https://github.com/owner/repo[/tree/ref[/dir]]`
    ///   or `github:owner/repo@ref[//dir]`. One tree-API call lists every file
    ///   with its blob SHA; files whose SHA the graph already stores are
    ///   revalidated without any fetch, so an unchanged repository costs
    ///   exactly one HTTPS request. `depth` = directory levels below the
    ///   subdir. `token` (a PAT; required for private/internal repos) is sent
    ///   only to the GitHub API and raw hosts. Crawl-only options (`scope`,
    ///   `path_contains`, `index_path_contains`, `min_delay_ms`) are invalid
    ///   here and raise `ValueError`.
    /// - **Any other http(s) URL** — the recursive crawler. `depth` = link
    ///   hops; stored `ETag`s make unchanged pages answer 304 with no body.
    ///
    /// Either way: a fresh in-memory link-r index is seeded from the graph's
    /// stored validators, the outcome flows into the graph (new/changed
    /// documents upserted with segments and edges, unchanged documents'
    /// freshness intervals grown, gone documents tombstoned), the store
    /// compacts, and the index is discarded — bodies and vectors never
    /// outlive the call.
    #[pyo3(signature = (source, depth=None, max_pages=1000, concurrency=8, token=None, scope=None, min_delay_ms=0, path_contains=None, extensions=None, index_path_contains=None, pin=false, max_bytes=None, api_base=None, raw_base=None))]
    #[allow(clippy::too_many_arguments)] // a flat keyword surface is the point for Python
    fn sync(
        &self,
        py: Python<'_>,
        source: String,
        depth: Option<u16>,
        max_pages: usize,
        concurrency: usize,
        token: Option<String>,
        scope: Option<String>,
        min_delay_ms: u64,
        path_contains: Option<Vec<String>>,
        extensions: Option<Vec<String>>,
        index_path_contains: Option<Vec<String>>,
        pin: bool,
        max_bytes: Option<u64>,
        api_base: Option<String>,
        raw_base: Option<String>,
    ) -> PyResult<PySyncReport> {
        if link_r::GithubSpec::matches(&source) {
            // Crawl-only knobs are meaningless against the tree API; passing
            // them is a caller bug, not something to ignore silently.
            if scope.is_some()
                || path_contains.is_some()
                || index_path_contains.is_some()
                || min_delay_ms != 0
            {
                return Err(PyValueError::new_err(
                    "scope/path_contains/index_path_contains/min_delay_ms are crawl options; \
                     they do not apply to a GitHub repository spec",
                ));
            }
            if api_base.is_some() != raw_base.is_some() {
                return Err(PyValueError::new_err(
                    "api_base and raw_base must be set together (GitHub Enterprise \
                     deployments move both hosts as a pair)",
                ));
            }
            return py.detach(|| {
                let _ticket = self.gate.write(self.lock_wait)?;
                self.with(|o| {
                    o.rt.block_on(async {
                        let seed = bridge::crawl_seed(&o.inner);
                        let mut index = link_r::LinkIndex::in_memory().map_err(map_linkr)?;
                        let mut update = index
                            .update_github(&source)
                            .map_err(map_linkr)?
                            .concurrency(concurrency)
                            .validators(seed.validators);
                        // GitHub Enterprise (and hermetic tests): the pairing
                        // guard above already ensured both bases move together.
                        if let (Some(api), Some(raw)) = (&api_base, &raw_base) {
                            update = update.bases(api.as_str(), raw.as_str());
                        }
                        if let Some(d) = depth {
                            update = update.depth(d);
                        }
                        if let Some(b) = max_bytes {
                            update = update.max_bytes(b);
                        }
                        for ext in extensions.into_iter().flatten() {
                            update = update.accept_extension(ext);
                        }
                        if let Some(t) = token {
                            update = update.token(t);
                        }
                        if pin {
                            update = update.pin();
                        }
                        let report = update.run().await.map_err(map_linkr)?;
                        let ingest =
                            bridge::ingest_update(&o.inner, &index, &report).map_err(map_err)?;
                        Ok(merge_report(&report, &ingest))
                    })
                })
            });
        }

        if api_base.is_some() || raw_base.is_some() {
            return Err(PyValueError::new_err(
                "api_base/raw_base apply only to a GitHub repository spec",
            ));
        }
        let crawl_scope = parse_scope(scope.as_deref())?;
        py.detach(|| {
            let _ticket = self.gate.write(self.lock_wait)?;
            self.with(|o| {
                o.rt.block_on(async {
                    let seed = bridge::crawl_seed(&o.inner);
                    let mut index = link_r::LinkIndex::in_memory().map_err(map_linkr)?;
                    let mut update = index
                        .update(source)
                        .depth(depth.unwrap_or(2))
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
                    let ingest =
                        bridge::ingest_update(&o.inner, &index, &crawl).map_err(map_err)?;
                    Ok(merge_report(&crawl, &ingest))
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
    ) -> PyResult<Vec<PyHit>> {
        py.detach(|| {
            self.read(|snap| {
                let opts = QueryOpts {
                    limit: k,
                    depth,
                    budget: TokenBudget(budget_tokens),
                    ..QueryOpts::default()
                };
                snap.query(&text, &opts).into_iter().map(PyHit::from).collect()
            })
        })
    }

    /// The `k` documents most related to `url` (outbound links and similarity
    /// edges), strongest first.
    #[pyo3(signature = (url, k=10))]
    fn related(&self, py: Python<'_>, url: String, k: usize) -> PyResult<Vec<PyHit>> {
        py.detach(|| {
            self.read(|snap| {
                snap.related(bridge::key_of(&url), k).into_iter().map(PyHit::from).collect()
            })
        })
    }

    /// Everything due for revalidation right now, most important first, with
    /// the stored validator to send as `If-None-Match`. `sync` consumes this
    /// implicitly; it is exposed for callers driving their own fetch loop.
    #[pyo3(signature = (limit=64, now_ms=None))]
    fn due(&self, py: Python<'_>, limit: usize, now_ms: Option<u64>) -> PyResult<Vec<PyDueItem>> {
        py.detach(|| {
            let now = now_ms.unwrap_or_else(bridge::now_ms);
            self.read(|snap| {
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
        let cmd = Cmd::Add {
            url: bridge::canonical(&url),
            content_hash,
            title,
            snippet,
            etag,
            pinned,
            fetched_at_ms: fetched_at_ms.unwrap_or_else(bridge::now_ms),
            segments,
            links,
        };
        self.write_one(py, cmd).map(|_| ())
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
        let cmd = Cmd::Touch {
            key: bridge::key_of(&url),
            outcome: parse_outcome(&outcome)?,
            content_hash,
            etag,
            checked_at_ms: checked_at_ms.unwrap_or_else(bridge::now_ms),
        };
        self.write_one(py, cmd)
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
        self.write_one(py, Cmd::Remove { key: bridge::key_of(&url) }).map(|_| ())
    }

    /// A transaction: `with store.transaction() as tx:` takes the commit
    /// lock at entry, waiting for another writer up to `wait` — `True` (the
    /// store's `lock_wait`, ten seconds by default), `False` (not at all), or
    /// a number of seconds — else `LockedError`. So `tx`'s reads see every
    /// writer's commits and nothing else can commit until the block ends.
    /// `tx` has the store's read and write methods; its writes are staged
    /// and committed together — one append, one fsync — when the block
    /// exits, or dropped when it raises. Reads show committed state, not the
    /// block's own staged writes. On this store, plain writes from other
    /// threads wait for the block; from the block's own thread they raise.
    #[pyo3(signature = (wait=None))]
    fn transaction(
        slf: Bound<'_, Self>,
        wait: Option<Bound<'_, PyAny>>,
    ) -> PyResult<PyTransaction> {
        let wait = match wait {
            None => slf.borrow().lock_wait,
            Some(w) => match w.extract::<bool>() {
                Ok(true) => slf.borrow().lock_wait,
                Ok(false) => Duration::ZERO,
                Err(_) => seconds(&w)?,
            },
        };
        Ok(PyTransaction {
            store: slf.unbind(),
            wait,
            lock: None,
            cmds: Vec::new(),
            staged: Staged::default(),
            phase: Phase::New,
        })
    }

    /// Fold the write-ahead log into a fresh immutable base now (also happens
    /// automatically past the configured thresholds, and after every `sync`).
    /// Returns the new generation number.
    fn compact(&self, py: Python<'_>) -> PyResult<u64> {
        py.detach(|| {
            let _ticket = self.gate.write(self.lock_wait)?;
            self.with(|o| o.inner.compact().map(|s| s.generation).map_err(map_err))
        })
    }

    /// Pick up commits/compactions made through other handles (other
    /// processes) since this handle's last load. Cheap when nothing changed:
    /// two file stats, no remap, no read; when only the log grew, only the
    /// appended bytes are read. Returns True when anything new became
    /// visible. Works on writable handles too (which also catch up at every
    /// write).
    fn reload_if_stale(&self, py: Python<'_>) -> PyResult<bool> {
        py.detach(|| self.with(|o| Ok(o.inner.reload_if_stale())))?.map_err(|e| map_open_err(py, e))
    }

    /// The base generation currently serving reads.
    #[getter]
    fn generation(&self, py: Python<'_>) -> PyResult<u64> {
        py.detach(|| self.read(|snap| snap.generation()))
    }

    /// Live (non-tombstoned) document count.
    fn __len__(&self, py: Python<'_>) -> PyResult<usize> {
        py.detach(|| self.with(|o| Ok(o.inner.len())))
    }

    /// Replace `url`'s typed outbound edges with `edges`, a list of
    /// `(dst_url, kind, exact)`: `kind` is `'contains'`, `'derives'` or
    /// `'conditions'`; `exact` marks an unchanged copy (`eflags::EXACT`).
    ///
    /// Whole-set replacement, never a merge: every earlier outbound edge of
    /// `url` is gone — `links` recorded by `add` included — and `[]` clears
    /// them. A repeated destination keeps its first entry. `url` and every
    /// destination must be committed documents (`add` them first), or the
    /// call raises `GraphRError` and changes nothing; an edge to `url` itself
    /// or more than 4096 edges is a `ValueError`. Visible to `out_edges`,
    /// `in_edges` and `walk` as soon as the call returns.
    fn set_edges(
        &self,
        py: Python<'_>,
        url: String,
        edges: Vec<(String, String, bool)>,
    ) -> PyResult<()> {
        let cmd = edges_cmd(url, edges)?;
        self.write_one(py, cmd).map(|_| ())
    }

    /// `url`'s outbound edges as `(dst_url, kind, exact)`, in destination
    /// key order (stable for a given store state). An unknown url has none.
    fn out_edges<'py>(&self, py: Python<'py>, url: String) -> PyResult<Bound<'py, PyList>> {
        let key = bridge::key_of(&url);
        py.detach(|| {
            self.read(|snap| {
                let mut found: Vec<(&str, EdgeType, bool)> = Vec::new();
                let mut it = snap.neighbors(key);
                while let Some(e) = it.next() {
                    if let Some(u) = doc_url(snap, e.dst_key) {
                        found.push((u, e.etype, e.flags & eflags::EXACT != 0));
                    }
                }
                Edges {
                    urls: Strs::of(found.iter().map(|f| f.0)),
                    meta: found.iter().map(|f| (f.1, f.2)).collect(),
                }
            })
        })?
        .into_py(py)
    }

    /// The edges pointing at `url` as `(src_url, kind, exact)`. Reads the
    /// reverse index written at compaction plus the committed edges since,
    /// so it agrees with `out_edges` at every moment — before and after
    /// `compact()`.
    fn in_edges<'py>(&self, py: Python<'py>, url: String) -> PyResult<Bound<'py, PyList>> {
        let key = bridge::key_of(&url);
        py.detach(|| {
            self.read(|snap| {
                let mut found: Vec<(&str, EdgeType, bool)> = Vec::new();
                snap.in_edges(key, |src, t, flags, _| {
                    if let Some(u) = doc_url(snap, src) {
                        found.push((u, t, flags & eflags::EXACT != 0));
                    }
                });
                Edges {
                    urls: Strs::of(found.iter().map(|f| f.0)),
                    meta: found.iter().map(|f| (f.1, f.2)).collect(),
                }
            })
        })?
        .into_py(py)
    }

    /// The documents reachable from `url` within `depth` hops, breadth
    /// first: ordered by distance, then url; each once, `url` itself never.
    /// `direction` is `'out'` (what `url` points at: upstream lineage),
    /// `'in'` (what points at it: downstream impact) or `'both'`. `kinds`
    /// limits the edge types followed (default: all). At most `limit` urls
    /// (default 10000) — the nearest ones. An unknown url reaches nothing.
    #[pyo3(signature = (url, direction="out", depth=3, kinds=None, limit=None))]
    fn walk<'py>(
        &self,
        py: Python<'py>,
        url: String,
        direction: &str,
        depth: u32,
        kinds: Option<Vec<String>>,
        limit: Option<usize>,
    ) -> PyResult<Bound<'py, PyList>> {
        let way = parse_direction(direction)?;
        // Known kinds are tags 0..=6: one bit each.
        let mask: u8 = match &kinds {
            None => u8::MAX,
            Some(ks) => {
                ks.iter().try_fold(0u8, |m, k| parse_kind(k).map(|t| m | 1 << t.as_tag()))?
            }
        };
        let follow = move |t: EdgeType| t.as_tag() < 8 && mask & (1 << t.as_tag()) != 0;
        let limit = limit.unwrap_or(WALK_LIMIT);
        let key = bridge::key_of(&url);
        let urls = py.detach(|| {
            self.read(|snap| {
                if limit == 0 || doc_url(snap, key).is_none() {
                    return Strs::default();
                }
                let mut reached: Vec<(u32, &str)> = Vec::new();
                // Expanding stops once `limit` documents are in hand. Every
                // node up to that distance was still reached (its frontier
                // was expanded before the count filled), so the nearest
                // `limit` by (distance, url) are exactly the first `limit`
                // after sorting.
                let visit = |h: graphr::Hop| match doc_url(snap, h.key) {
                    Some(u) => {
                        reached.push((h.depth, u));
                        reached.len() < limit
                    }
                    None => false, // a stub or removed node: not a document
                };
                WALK.with_borrow_mut(|w| match way {
                    Way::Out => snap.walk(key, graphr::Direction::Out, depth, follow, w, visit),
                    Way::In => snap.walk(key, graphr::Direction::In, depth, follow, w, visit),
                    Way::Both => snap.walk_both(key, depth, follow, w, visit),
                });
                reached.sort_unstable();
                reached.truncate(limit);
                Strs::of(reached.iter().map(|r| r.1))
            })
        })?;
        PyList::new(py, urls.iter())
    }

    /// Replace `url`'s attributes with `mapping` (`str -> str`): values
    /// for large text beside a node (a table's SQL, a column's type).
    /// Whole-set replacement; `{}` clears. Limits: at most 64 names, each
    /// 1-255 UTF-8 bytes without NUL; a name plus its value at most 262143
    /// bytes; all values together under 1 MiB — beyond any of them
    /// `ValueError`. `url` must be a committed document (`GraphRError`).
    fn set_attrs(
        &self,
        py: Python<'_>,
        url: String,
        mapping: HashMap<String, String>,
    ) -> PyResult<()> {
        self.write_one(py, Cmd::SetAttrs { url, mapping }).map(|_| ())
    }

    /// `url`'s attributes as a `dict` (`{}` for an unknown url).
    fn attrs<'py>(&self, py: Python<'py>, url: String) -> PyResult<Bound<'py, PyDict>> {
        let key = bridge::key_of(&url);
        let pairs = py.detach(|| {
            self.read(|snap| {
                let mut out = Strs::default();
                snap.named_attrs(key, |name, value| {
                    out.push(name);
                    out.push(value);
                });
                out
            })
        })?;
        let dict = PyDict::new(py);
        let mut it = pairs.iter();
        while let (Some(name), Some(value)) = (it.next(), it.next()) {
            dict.set_item(name, value)?;
        }
        Ok(dict)
    }

    /// Every committed, non-tombstoned document url starting with `prefix`,
    /// sorted. Exhaustive, not ranked: one pass over the node records
    /// (O(documents)) plus a sort of the matches — the store keeps no
    /// url-ordered index.
    #[pyo3(signature = (prefix=""))]
    fn urls<'py>(&self, py: Python<'py>, prefix: &str) -> PyResult<Bound<'py, PyList>> {
        let urls = py.detach(|| {
            self.read(|snap| {
                let mut found: Vec<&str> = Vec::new();
                snap.for_each_live(|n| {
                    if !n.url.is_empty() && n.url.starts_with(prefix) {
                        found.push(n.url);
                    }
                });
                found.sort_unstable();
                Strs::of(found.iter().copied())
            })
        })?;
        PyList::new(py, urls.iter())
    }

    /// Release the store now: the mapping, the open files and the private
    /// runtime. Waits for calls in flight on other threads; every later
    /// call raises `GraphRError("store is closed")`. Idempotent.
    fn close(&self, py: Python<'_>) {
        py.detach(|| {
            let open = self.open.write().unwrap_or_else(PoisonError::into_inner).take();
            drop(open);
        });
    }
}

fn not_a_document(url: &str) -> PyErr {
    GraphRError::new_err(format!("not a committed document: {url:?}"))
}

impl PyStore {
    /// Run `f` on the open store, or refuse once `close()` has run. Call it
    /// inside `detach`: it may wait for a `close()` in progress.
    fn with<R>(&self, f: impl FnOnce(&Open) -> PyResult<R>) -> PyResult<R> {
        let open = self.open.read().unwrap_or_else(PoisonError::into_inner);
        f(open.as_ref().ok_or_else(closed)?)
    }

    /// Run `f` on a fresh snapshot of the open store.
    fn read<R>(&self, f: impl FnOnce(&Snapshot<'_>) -> R) -> PyResult<R> {
        self.with(|o| Ok(f(&o.inner.snapshot())))
    }

    fn set_pin(&self, py: Python<'_>, url: &str, pinned: bool) -> PyResult<bool> {
        self.write_one(py, Cmd::Pin { key: bridge::key_of(url), pinned })
    }

    /// One write as a one-op transaction, through the gate.
    fn write_one(&self, py: Python<'_>, cmd: Cmd) -> PyResult<bool> {
        py.detach(|| {
            let _ticket = self.gate.write(self.lock_wait)?;
            self.with(|o| commit(&o.inner, None, std::slice::from_ref(&cmd)))
        })
    }
}

/// `set_edges` arguments, validated: a `ValueError` for a bad shape, before
/// any store work.
fn edges_cmd(url: String, edges: Vec<(String, String, bool)>) -> PyResult<Cmd> {
    if edges.len() > MAX_EDGES {
        return Err(PyValueError::new_err(format!(
            "{} edges exceed the per-node cap of {MAX_EDGES}",
            edges.len()
        )));
    }
    let key = bridge::key_of(&url);
    let mut staged: Vec<(UrlKey, EdgeType, u16, u8)> = Vec::with_capacity(edges.len());
    let mut dsts = Vec::with_capacity(edges.len());
    for (dst, kind, exact) in edges {
        let dst_key = bridge::key_of(&dst);
        if dst_key == key {
            return Err(PyValueError::new_err(format!("an edge from {url:?} to itself")));
        }
        let flags = if exact { eflags::EXACT } else { 0 };
        staged.push((dst_key, parse_lineage_kind(&kind)?, 65_535, flags));
        dsts.push(dst);
    }
    Ok(Cmd::SetEdges { url, dsts, staged })
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    New,
    Open,
    Done,
}

/// A transaction on a [`PyStore`]: the commit lock held from `__enter__` to
/// `__exit__`, writes staged in between and committed as one append.
#[pyclass(name = "Transaction")]
struct PyTransaction {
    store: Py<PyStore>,
    /// How long `__enter__` waits for the commit lock.
    wait: Duration,
    lock: Option<CommitLock>,
    cmds: Vec<Cmd>,
    staged: Staged,
    phase: Phase,
}

impl PyTransaction {
    fn open(&self) -> PyResult<()> {
        match self.phase {
            Phase::Open => Ok(()),
            Phase::New => Err(GraphRError::new_err("use the transaction in a `with` block")),
            Phase::Done => Err(GraphRError::new_err("the transaction has finished")),
        }
    }

    /// Check `cmd` against the caught-up store and stage it; returns what
    /// the write will return at commit (nothing can commit in between).
    fn stage(&mut self, py: Python<'_>, cmd: Cmd) -> PyResult<bool> {
        self.open()?;
        let store = self.store.bind(py).borrow();
        let store: &PyStore = &store;
        let staged = &self.staged;
        let out = py.detach(|| {
            store.with(|o| {
                let snap = o.inner.snapshot();
                staged.check(&snap, &cmd)?;
                Ok(match &cmd {
                    // Both see committed nodes only, exactly as at commit.
                    Cmd::Touch { key, .. } => snap.node(*key).is_some(),
                    Cmd::Pin { key, .. } => {
                        staged.added.contains(&key.0) || snap.node(*key).is_some()
                    }
                    _ => true,
                })
            })
        })?;
        self.staged.note(&cmd);
        self.cmds.push(cmd);
        Ok(out)
    }

    fn store<'py>(&self, py: Python<'py>) -> PyResult<PyRef<'py, PyStore>> {
        self.open()?;
        Ok(self.store.bind(py).borrow())
    }
}

#[pymethods]
impl PyTransaction {
    fn __enter__<'py>(
        mut slf: PyRefMut<'py, Self>,
        py: Python<'py>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        if slf.phase != Phase::New {
            return Err(GraphRError::new_err("a transaction is entered once"));
        }
        let handle = slf.store.clone_ref(py);
        let store = handle.bind(py).borrow();
        let store: &PyStore = &store;
        let wait = slf.wait;
        let lock = py.detach(|| {
            store.gate.begin(wait)?;
            let lock = store.with(|o| o.inner.commit_lock_within(wait).map_err(map_err));
            if lock.is_err() {
                store.gate.end();
            }
            lock
        })?;
        slf.lock = Some(lock);
        slf.phase = Phase::Open;
        Ok(slf)
    }

    /// Commit (no exception) or drop (an exception) the staged writes, then
    /// release the lock. Never swallows the block's exception.
    fn __exit__(
        &mut self,
        py: Python<'_>,
        exc_type: Option<Bound<'_, PyAny>>,
        _exc_value: Option<Bound<'_, PyAny>>,
        _traceback: Option<Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        self.open()?;
        self.phase = Phase::Done;
        let lock = self.lock.take();
        let cmds = std::mem::take(&mut self.cmds);
        let store = self.store.bind(py).borrow();
        let store: &PyStore = &store;
        let raised = exc_type.is_some_and(|t| !t.is_none());
        py.detach(|| {
            let r = match (&lock, raised || cmds.is_empty()) {
                (Some(l), false) => store.with(|o| commit(&o.inner, Some(l), &cmds)).map(|_| ()),
                _ => Ok(()),
            };
            drop(lock);
            store.gate.end();
            r
        })?;
        Ok(false)
    }

    /// See `Store.add`; staged until the block commits.
    #[pyo3(signature = (url, *, content_hash, title=None, snippet=None, etag=None, pinned=false, fetched_at_ms=None, segments=None, links=None))]
    #[allow(clippy::too_many_arguments)]
    fn add(
        &mut self,
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
        let cmd = Cmd::Add {
            url: bridge::canonical(&url),
            content_hash,
            title,
            snippet,
            etag,
            pinned,
            fetched_at_ms: fetched_at_ms.unwrap_or_else(bridge::now_ms),
            segments,
            links,
        };
        self.stage(py, cmd).map(|_| ())
    }

    /// See `Store.touch` (a committed document only, as there).
    #[pyo3(signature = (url, outcome, *, content_hash=None, etag=None, checked_at_ms=None))]
    fn touch(
        &mut self,
        py: Python<'_>,
        url: String,
        outcome: String,
        content_hash: Option<u64>,
        etag: Option<String>,
        checked_at_ms: Option<u64>,
    ) -> PyResult<bool> {
        let cmd = Cmd::Touch {
            key: bridge::key_of(&url),
            outcome: parse_outcome(&outcome)?,
            content_hash,
            etag,
            checked_at_ms: checked_at_ms.unwrap_or_else(bridge::now_ms),
        };
        self.stage(py, cmd)
    }

    /// See `Store.pin`.
    fn pin(&mut self, py: Python<'_>, url: String) -> PyResult<bool> {
        self.stage(py, Cmd::Pin { key: bridge::key_of(&url), pinned: true })
    }

    /// See `Store.unpin`.
    fn unpin(&mut self, py: Python<'_>, url: String) -> PyResult<bool> {
        self.stage(py, Cmd::Pin { key: bridge::key_of(&url), pinned: false })
    }

    /// See `Store.remove`.
    fn remove(&mut self, py: Python<'_>, url: String) -> PyResult<()> {
        self.stage(py, Cmd::Remove { key: bridge::key_of(&url) }).map(|_| ())
    }

    /// See `Store.set_edges`; documents added earlier in the block count.
    fn set_edges(
        &mut self,
        py: Python<'_>,
        url: String,
        edges: Vec<(String, String, bool)>,
    ) -> PyResult<()> {
        let cmd = edges_cmd(url, edges)?;
        self.stage(py, cmd).map(|_| ())
    }

    /// See `Store.set_attrs`; documents added earlier in the block count.
    fn set_attrs(
        &mut self,
        py: Python<'_>,
        url: String,
        mapping: HashMap<String, String>,
    ) -> PyResult<()> {
        self.stage(py, Cmd::SetAttrs { url, mapping }).map(|_| ())
    }

    /// See `Store.query`.
    #[pyo3(signature = (text, k=20, depth=3, budget_tokens=2000))]
    fn query(
        &self,
        py: Python<'_>,
        text: String,
        k: usize,
        depth: u8,
        budget_tokens: u32,
    ) -> PyResult<Vec<PyHit>> {
        self.store(py)?.query(py, text, k, depth, budget_tokens)
    }

    /// See `Store.related`.
    #[pyo3(signature = (url, k=10))]
    fn related(&self, py: Python<'_>, url: String, k: usize) -> PyResult<Vec<PyHit>> {
        self.store(py)?.related(py, url, k)
    }

    /// See `Store.due`.
    #[pyo3(signature = (limit=64, now_ms=None))]
    fn due(&self, py: Python<'_>, limit: usize, now_ms: Option<u64>) -> PyResult<Vec<PyDueItem>> {
        self.store(py)?.due(py, limit, now_ms)
    }

    /// See `Store.out_edges`.
    fn out_edges<'py>(&self, py: Python<'py>, url: String) -> PyResult<Bound<'py, PyList>> {
        self.store(py)?.out_edges(py, url)
    }

    /// See `Store.in_edges`.
    fn in_edges<'py>(&self, py: Python<'py>, url: String) -> PyResult<Bound<'py, PyList>> {
        self.store(py)?.in_edges(py, url)
    }

    /// See `Store.walk`.
    #[pyo3(signature = (url, direction="out", depth=3, kinds=None, limit=None))]
    fn walk<'py>(
        &self,
        py: Python<'py>,
        url: String,
        direction: &str,
        depth: u32,
        kinds: Option<Vec<String>>,
        limit: Option<usize>,
    ) -> PyResult<Bound<'py, PyList>> {
        self.store(py)?.walk(py, url, direction, depth, kinds, limit)
    }

    /// See `Store.attrs`.
    fn attrs<'py>(&self, py: Python<'py>, url: String) -> PyResult<Bound<'py, PyDict>> {
        self.store(py)?.attrs(py, url)
    }

    /// See `Store.urls`.
    #[pyo3(signature = (prefix=""))]
    fn urls<'py>(&self, py: Python<'py>, prefix: &str) -> PyResult<Bound<'py, PyList>> {
        self.store(py)?.urls(py, prefix)
    }

    /// See `Store.generation`.
    #[getter]
    fn generation(&self, py: Python<'_>) -> PyResult<u64> {
        self.store(py)?.generation(py)
    }

    /// Committed live documents (staged ones are not counted).
    fn __len__(&self, py: Python<'_>) -> PyResult<usize> {
        self.store(py)?.__len__(py)
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
    m.add_class::<PyTransaction>()?;
    m.add("GraphRError", m.py().get_type::<GraphRError>())?;
    m.add("FormatVersionError", m.py().get_type::<FormatVersionError>())?;
    m.add("LockedError", m.py().get_type::<LockedError>())?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
