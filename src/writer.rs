// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! Writers and transactions. Mutations are encoded straight into one log
//! record as they are staged, made durable on [`Writer::commit`] (commit
//! lock → catch up → seal → one append → fsync per policy → unlock), and
//! that very buffer is then published into the current generation's
//! overlay, where new snapshots see the whole transaction at once. Freshness
//! math runs *here*, at touch time, and the computed state travels inside
//! the op — replay never re-derives policy. [`Txn`] is the serializable
//! form: the lock is held from its read view to its append.

use crate::error::{Error, Result};
use crate::format::base::nflags;
use crate::format::wal::{self, OpView, RecordBuf, TouchRecord};
use crate::key::{EdgeType, UrlKey};
use crate::snapshot::Snapshot;
use crate::store::{CommitLock, Durability, Held, Seen, Store};
use crate::ttl::Outcome;
use std::io::Write as _;
use std::sync::atomic::Ordering;
use std::sync::MutexGuard;

/// Maximum outbound edges per node (wide joins and conditions included).
pub use crate::format::wal::MAX_EDGES;
/// Maximum attributes per node, and the longest attribute value.
pub use crate::format::wal::{MAX_ATTRS, MAX_ATTR_LEN};
/// Maximum label length persisted (bytes of UTF-8), and segments per document.
pub use crate::format::wal::{MAX_LABEL, MAX_SEGS};
/// Most value bytes one node's attribute set may hold in total: what fits a
/// single log op ([`wal::MAX_OP`], its fixed fields and per-value headers
/// included).
pub const MAX_ATTR_BYTES: usize = wal::MAX_OP - (wal::OP_HEADER_LEN + 8) - MAX_ATTRS * 8;
/// Longest attribute name accepted by [`Writer::set_named_attrs`].
pub const MAX_ATTR_NAME: usize = 255;

/// A document to upsert — borrowed, produced by the caller or the link-r
/// bridge. Everything here is compact lookup metadata; bodies and vectors
/// are deliberately not representable.
#[derive(Clone, Copy, Debug)]
pub struct DocRecord<'a> {
    /// Canonical URL.
    pub url: &'a str,
    /// Durable key (xxh3 of `url`).
    pub url_key: UrlKey,
    /// xxh3 of the fetched body.
    pub content_hash: u64,
    /// Fetch stamp, epoch ms.
    pub fetched_at_ms: u64,
    /// Title, if extracted.
    pub title: Option<&'a str>,
    /// Distilled snippet (≤ ~180 chars by convention).
    pub snippet: Option<&'a str>,
    /// Entity tag from the fetch, for conditional revalidation.
    pub etag: Option<&'a str>,
    /// Exempt from eviction sweeps.
    pub pinned: bool,
}

/// A sub-document segment to record — an anchor reference, never content.
#[derive(Clone, Copy, Debug)]
pub struct SegmentRecord<'a> {
    /// Heading-path label (e.g. `"Install > Linux"`).
    pub label: &'a str,
    /// Byte range in the source document, when known.
    pub byte_range: Option<(u32, u32)>,
    /// Heading depth (1 = H1 …).
    pub depth: u8,
    /// Importance in 1/65535 units (deterministic prior or enrichment score).
    pub importance: u16,
}

/// A revalidation observation to record against a node.
#[derive(Clone, Copy, Debug)]
pub struct Touch<'a> {
    /// When the check happened, epoch ms.
    pub checked_at_ms: u64,
    /// What was observed.
    pub outcome: Outcome,
    /// New content hash when `outcome == Changed`.
    pub content_hash: Option<u64>,
    /// New entity tag, if the server sent one.
    pub etag: Option<&'a str>,
}

pub(crate) struct WriteState {
    next_seq: u64,
    /// The pending transaction, encoded as it is staged.
    staged: RecordBuf,
    pub(crate) ops_since_compact: usize,
    pub(crate) wal_bytes: u64,
    commits_since_sync: u32,
    wal: Option<std::fs::File>,
    /// Generation of the newest base written to disk. A compaction whose
    /// in-memory install was refused (every ring slot pinned) still leaves a
    /// durable base behind; numbering from this, not from the served
    /// snapshot, means a generation number is never reused.
    pub(crate) disk_generation: u64,
    /// What this handle last saw of the files (see `Store::sync`).
    pub(crate) seen: Seen,
    /// The lock file, kept open between commits (unlocked) so a commit costs
    /// one lock call, not an open as well.
    pub(crate) lock_file: Option<std::fs::File>,
    /// The wait-queue file (`graph.wait`), kept open likewise.
    pub(crate) wait_file: Option<std::fs::File>,
}

#[allow(clippy::missing_fields_in_debug)] // file handle and op buffer are noise
impl std::fmt::Debug for WriteState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteState")
            .field("next_seq", &self.next_seq)
            .field("staged", &self.staged.ops())
            .finish()
    }
}

impl WriteState {
    pub(crate) fn new(
        next_seq: u64,
        replayed_ops: usize,
        wal_bytes: u64,
        disk_generation: u64,
        seen: Seen,
    ) -> Self {
        Self {
            next_seq,
            staged: RecordBuf::new(),
            ops_since_compact: replayed_ops,
            wal_bytes,
            commits_since_sync: 0,
            wal: None,
            disk_generation,
            seen,
            lock_file: None,
            wait_file: None,
        }
    }

    /// Forget the open log handle: the log may have been replaced.
    pub(crate) fn drop_wal_handle(&mut self) {
        self.wal = None;
    }

    /// Advance to what a catch-up found: the served generation's next
    /// sequence number and overlay size, and the log offset consumed.
    pub(crate) fn adopt(&mut self, next_seq: u64, overlay_ops: usize, generation: u64) {
        self.next_seq = self.next_seq.max(next_seq);
        self.ops_since_compact = overlay_ops;
        self.wal_bytes = self.seen.wal_pos;
        self.disk_generation = self.disk_generation.max(generation);
    }

    /// Unlock and keep the descriptor for the next commit.
    pub(crate) fn release(&mut self, held: Held) {
        let _ = held.0.unlock();
        self.lock_file = Some(held.0);
    }

    pub(crate) fn reset_after_compact(&mut self, next_seq: u64) {
        self.next_seq = next_seq;
        self.ops_since_compact = 0;
        self.wal_bytes = u64::try_from(crate::format::wal::HEADER_LEN).unwrap_or(64);
        self.commits_since_sync = 0;
        self.wal = None; // reopen against the fresh log on next commit
    }

    pub(crate) fn staged_is_empty(&self) -> bool {
        self.staged.is_empty()
    }

    pub(crate) fn last_committed_seq(&self) -> u64 {
        self.next_seq.saturating_sub(1)
    }
}

/// A handle's writer; holds the handle's writer mutex for its lifetime (so
/// threads sharing one handle take turns). Stage mutations, then
/// [`Writer::commit`] to make them durable and visible. Writers on other
/// handles stage concurrently; commits take turns on the commit lock.
#[derive(Debug)]
pub struct Writer<'s> {
    store: &'s Store,
    state: MutexGuard<'s, WriteState>,
    /// Committed-state view, cached for the duration of one staging batch.
    ///
    /// Several staging methods read committed state (segment identity for
    /// enrichment carry, the other tier's edges, freshness counters, node
    /// existence). Taking a fresh snapshot per staged op folds the overlay
    /// arena every time — during a bulk ingest that made staging O(batch ×
    /// pending-ops). Nothing on this handle can commit while this writer holds
    /// the mutex, so one snapshot is as current as a fresh one for the whole
    /// batch (other handles' commits arrive at the next catch-up; a
    /// read-modify-write that must see them is a [`Store::transact`]).
    /// [`Writer::commit`] drops it, both because the world changes there and
    /// so the pinned generation slot is free before a catch-up or an
    /// auto-compaction installs another.
    snap: Option<Snapshot<'s>>,
}

fn check_label(name: &str, s: &str) -> Result<()> {
    if s.len() > MAX_LABEL {
        return Err(Error::format(format!("{name} label exceeds {MAX_LABEL} bytes")));
    }
    Ok(())
}

impl<'s> Writer<'s> {
    pub(crate) fn new(store: &'s Store, state: MutexGuard<'s, WriteState>) -> Self {
        Self { store, state, snap: None }
    }

    /// The cached committed-state view (created on first use per batch).
    fn view(&mut self) -> &Snapshot<'s> {
        if self.snap.is_none() {
            self.snap = Some(self.store.snapshot());
        }
        self.snap.as_ref().expect("just filled")
    }

    /// Stage a node create/replace.
    pub fn upsert_node(&mut self, doc: &DocRecord<'_>) -> Result<()> {
        check_label("url", doc.url)?;
        if doc.url.is_empty() {
            return Err(Error::format("url must not be empty"));
        }
        for (name, v) in [("title", doc.title), ("snippet", doc.snippet), ("etag", doc.etag)] {
            if let Some(v) = v {
                check_label(name, v)?;
            }
        }
        let flags = if doc.pinned { nflags::PINNED } else { 0 };
        self.state.staged.push_upsert(doc, flags)
    }

    /// Stage a whole-set segment replacement for `key`.
    pub fn set_segments(&mut self, key: UrlKey, segs: &[SegmentRecord<'_>]) -> Result<()> {
        self.state.staged.push_segments(key, segs)
    }

    /// Stage a whole-set outbound-edge replacement for `key`. Edges are
    /// deduplicated by destination (first occurrence wins — document order is
    /// salience order) and stored sorted by destination key.
    pub fn set_edges(&mut self, key: UrlKey, edges: &[(UrlKey, EdgeType, u16)]) {
        self.stage_edges(key, edges.iter().map(|&(d, t, w)| (d, t, w, 0)));
    }

    /// [`Writer::set_edges`] with per-edge flags; only caller-settable ones
    /// are kept (`eflags::EXACT`: an unchanged copy along `Derives`).
    pub fn set_edges_with_flags(&mut self, key: UrlKey, edges: &[(UrlKey, EdgeType, u16, u8)]) {
        let mask = crate::format::base::eflags::EXACT;
        self.stage_edges(key, edges.iter().map(|&(d, t, w, f)| (d, t, w, f & mask)));
    }

    fn stage_edges(
        &mut self,
        key: UrlKey,
        edges: impl Iterator<Item = (UrlKey, EdgeType, u16, u8)>,
    ) {
        let mut seen = std::collections::HashSet::new();
        let mut owned: Vec<wal::Edge> = Vec::new();
        for (dst, etype, weight, mut flags) in edges {
            if dst == key || !seen.insert(dst.0) {
                continue;
            }
            if matches!(etype, EdgeType::Related) {
                flags |= crate::format::base::eflags::INFERRED;
            }
            owned.push((dst, etype.as_tag(), flags, weight));
            if owned.len() == MAX_EDGES {
                break;
            }
        }
        owned.sort_unstable_by_key(|(dst, _, _, _)| dst.0);
        // Deduplicated, capped and sorted above: the encoder cannot refuse it.
        let staged = self.state.staged.push_edges(key, &owned);
        debug_assert!(staged.is_ok(), "normalized edges refused");
    }

    /// Stage a whole-set replacement of `key`'s attributes: small keyed
    /// values beside the node (a column's type, a table's producing SQL, a
    /// description's provenance). Keys are the caller's; a later key wins
    /// over an earlier duplicate. Values longer than [`MAX_ATTR_LEN`], more
    /// than [`MAX_ATTRS`] keys, or a set whose encoding would not fit one log
    /// frame ([`MAX_ATTR_BYTES`] in total) are refused.
    pub fn set_attrs(&mut self, key: UrlKey, attrs: &[(u8, &str)]) -> Result<()> {
        if attrs.len() > MAX_ATTRS {
            return Err(Error::format("too many attributes"));
        }
        let mut owned: Vec<(u8, [&str; 3])> = Vec::with_capacity(attrs.len());
        for &(k, v) in attrs.iter().rev() {
            if v.len() > MAX_ATTR_LEN {
                return Err(Error::format("attribute too long"));
            }
            if !owned.iter().any(|(o, _)| *o == k) {
                owned.push((k, [v, "", ""]));
            }
        }
        // Bound the whole op, so it always fits one record entry.
        if owned.iter().map(|(_, v)| v[0].len()).sum::<usize>() > MAX_ATTR_BYTES {
            return Err(Error::format(format!(
                "attributes exceed {MAX_ATTR_BYTES} bytes in total"
            )));
        }
        owned.sort_unstable_by_key(|(k, _)| *k);
        self.state.staged.push_attr_parts(key, &owned)
    }

    /// [`Writer::set_attrs`] with string names: a whole-set replacement of
    /// `key`'s attributes as `name → value`. Each record is stored as
    /// `name NUL value` under keys `0..n` in name order, so
    /// [`crate::Snapshot::named_attrs`] reads names and values back as
    /// borrowed slices and any producer (Rust or a binding) agrees on the
    /// encoding. A later duplicate name wins. Names must be non-empty, at
    /// most [`MAX_ATTR_NAME`] bytes and free of NUL; each `name NUL value`
    /// must fit [`MAX_ATTR_LEN`], and the whole set [`MAX_ATTR_BYTES`].
    pub fn set_named_attrs(&mut self, key: UrlKey, attrs: &[(&str, &str)]) -> Result<()> {
        if attrs.len() > MAX_ATTRS {
            return Err(Error::format(format!("more than {MAX_ATTRS} attributes")));
        }
        let mut named: Vec<(&str, &str)> = Vec::with_capacity(attrs.len());
        for &(name, value) in attrs.iter().rev() {
            if name.is_empty() || name.len() > MAX_ATTR_NAME || name.contains('\0') {
                return Err(Error::format(format!(
                    "attribute name must be 1..={MAX_ATTR_NAME} bytes without NUL"
                )));
            }
            if name.len() + 1 + value.len() > MAX_ATTR_LEN {
                return Err(Error::format(format!(
                    "attribute {name:?} exceeds {MAX_ATTR_LEN} bytes"
                )));
            }
            if !named.iter().any(|(n, _)| *n == name) {
                named.push((name, value));
            }
        }
        named.sort_unstable_by_key(|(n, _)| *n);
        let total: usize = named.iter().map(|(n, v)| n.len() + 1 + v.len()).sum();
        if total > MAX_ATTR_BYTES {
            return Err(Error::format(format!(
                "attributes exceed {MAX_ATTR_BYTES} bytes in total"
            )));
        }
        // Each record is `name NUL value`, written straight into the log
        // record: no value is assembled anywhere first.
        let parts: Vec<(u8, [&str; 3])> =
            named.iter().enumerate().map(|(k, &(n, v))| (k as u8, [n, "\0", v])).collect();
        self.state.staged.push_attr_parts(key, &parts)
    }

    /// Record a revalidation outcome. Freshness policy (interval growth/cut,
    /// counters) is computed here from the node's current state and the store's
    /// [`crate::TtlConfig`], and the resulting op carries the node's *full*
    /// effective state so replay never re-derives policy.
    ///
    /// Returns `false`, staging nothing, when `key` is not a **committed** node.
    /// That is the important word: the state is read through a snapshot, which
    /// sees committed ops only, so a node staged earlier in this same batch is
    /// not yet visible. Commit the upsert before touching it — the link-r bridge
    /// already does.
    ///
    /// The same rule means two touches of one key inside a single uncommitted
    /// batch do not accumulate: both are computed from the pre-batch state and
    /// the second wins under last-wins overlay folding. Commit between
    /// observations of the same document. Pinned by `touch_requires_a_committed_node`
    /// in `tests/regression.rs`.
    pub fn touch(&mut self, key: UrlKey, t: Touch<'_>) -> Result<bool> {
        if let Some(e) = t.etag {
            check_label("etag", e)?;
        }
        // An upsert staged earlier in this batch is the node's newest hash
        // and etag. Its etag is copied out (onto the stack) because staging
        // the touch may move the buffer it lies in.
        let mut etag_buf = [0u8; MAX_LABEL];
        let mut staged: Option<(u64, Option<usize>)> = None;
        for op in self.state.staged.op_views() {
            if let OpView::Upsert(u) = op {
                if u.key() == key {
                    let etag = u.etag().map(|e| {
                        etag_buf[..e.len()].copy_from_slice(e.as_bytes());
                        e.len()
                    });
                    staged = Some((u.content_hash(), etag));
                }
            }
        }
        let staged_etag = |len: Option<usize>| {
            len.map(|n| std::str::from_utf8(&etag_buf[..n]).unwrap_or_default())
        };
        let snap = self.snap.get_or_insert_with(|| self.store.snapshot());
        let Some(node) = snap.node(key) else { return Ok(false) };
        let cfg = snap.ttl();
        let interval_s = cfg.next_interval(node.interval_s.max(1), t.outcome);
        let changed = t.outcome == Outcome::Changed;
        // The op carries the node's *full* effective state, not a delta:
        // touches fold last-wins in the overlay, so an unchanged check must
        // not erase the hash/etag a prior change observed — nor the ones an
        // upsert staged earlier in this same batch.
        let (base_hash, base_etag) = match staged {
            Some((h, e)) => (h, staged_etag(e)),
            None => (node.content_hash, node.etag),
        };
        let rec = TouchRecord {
            checked_at_ms: t.checked_at_ms,
            outcome: t.outcome.as_tag(),
            content_hash: Some(t.content_hash.unwrap_or(base_hash)),
            etag: t.etag.or(base_etag),
            interval_s,
            checks: node.checks.saturating_add(1),
            changes: if changed { node.changes.saturating_add(1) } else { node.changes },
            last_change_ms: if changed { t.checked_at_ms } else { node.last_change_ms },
            tombstone: t.outcome == Outcome::Gone,
        };
        self.state.staged.push_touch(key, &rec)?;
        Ok(true)
    }

    /// Stage a pin/unpin. Returns `false` (staging nothing) when `key` is
    /// neither committed nor upserted earlier in this same batch — a pin on a
    /// document that does not exist must not lie in wait for a future upsert.
    pub fn set_pinned(&mut self, key: UrlKey, pinned: bool) -> Result<bool> {
        let exists = self
            .state
            .staged
            .op_views()
            .any(|op| matches!(op, OpView::Upsert(u) if u.key() == key))
            || self.view().node(key).is_some();
        if !exists {
            return Ok(false);
        }
        self.state.staged.push_pin(key, pinned);
        Ok(true)
    }

    /// Stage a hard delete.
    pub fn remove(&mut self, key: UrlKey) -> Result<()> {
        self.state.staged.push_remove(key);
        Ok(())
    }

    /// Drop every op staged on this handle and not yet committed. Staged
    /// ops otherwise outlive a failed commit (a retry commits them) and a
    /// dropped writer (the handle's next commit includes them); a
    /// transaction refused for good — larger than [`wal::MAX_RECORD`] — is
    /// discarded with this, then staged again in parts.
    pub fn discard(&mut self) {
        self.state.staged = RecordBuf::new();
    }

    /// Make staged ops durable and visible: take the commit lock (waiting up
    /// to [`crate::Config::lock_wait`], else [`Error::Locked`] with the ops
    /// still staged), catch up with other writers, seal the staged ops into
    /// one record, append it to the WAL in one write, fsync per the
    /// configured [`Durability`], publish it to the overlay, auto-compact
    /// past thresholds, unlock. Returns the last committed sequence number.
    ///
    /// `Err` means nothing was appended and nothing published, and the ops
    /// stay staged: a transaction larger than [`wal::MAX_RECORD`] is a
    /// typed [`Error::Format`] before anything is written. `Ok` means the
    /// whole transaction is durable (per policy) and visible at once. An
    /// inline compaction that cannot run leaves [`Store::compaction_due`]
    /// set instead of failing the commit.
    pub fn commit(&mut self) -> Result<u64> {
        // The cached view is about to go stale, and its pinned generation slot
        // must be free before a catch-up or auto-compaction installs another.
        self.snap = None;
        if self.state.staged.is_empty() {
            return Ok(self.state.next_seq.saturating_sub(1));
        }
        let held = self.store.acquire(&mut self.state, self.store.cfg.lock_wait)?;
        let r = self.store.sync(&mut self.state, true).and_then(|_| self.append_held());
        self.state.release(held);
        r
    }

    /// The commit itself, with the commit lock held and the handle caught
    /// up: one record, one write, one fsync. Everything that can refuse runs
    /// before the first byte is written; a failed write is cut back off the
    /// log. Steady state, nothing here allocates: the staged buffer is
    /// sealed in place, written, and moved into the overlay as is.
    fn append_held(&mut self) -> Result<u64> {
        if self.state.staged.is_empty() {
            return Ok(self.state.next_seq.saturating_sub(1));
        }
        let count = self.state.staged.ops();
        let first_seq = self.state.next_seq;
        // Over the record cap: refused here, nothing written, ops kept.
        self.state.staged.seal(first_seq)?;
        // The current generation is stable: installs on this handle happen
        // only under its writer mutex, which we hold. Room in its overlay is
        // made now, so publishing after the append cannot fail: a durable
        // commit that this handle did not account for is what reuses
        // sequence numbers.
        let cur = self.store.current.load(Ordering::Acquire);
        let guard = self.store.gens[cur]
            .inner
            .try_read()
            .map_err(|_| Error::busy("current slot mid-install"))?;
        let inner = guard.as_ref().ok_or_else(|| Error::corrupt("current slot empty"))?;
        if !inner.log.reserve(count as usize, self.state.staged.len()) {
            return Err(Error::busy("overlay arena full"));
        }
        if self.state.wal.is_none() {
            self.state.wal =
                Some(std::fs::OpenOptions::new().append(true).open(&self.store.wal_file)?);
        }
        let durability = self.store.cfg.durability;
        let state = &mut *self.state;
        let Some(f) = state.wal.as_mut() else { return Err(Error::corrupt("log handle missing")) };
        let sync = match durability {
            Durability::Always => true,
            Durability::Batch(n) => state.commits_since_sync + 1 >= n.max(1),
            Durability::Never => false,
        };
        let record = state.staged.as_bytes();
        let appended = f.write_all(record).and_then(|()| f.flush()).and_then(|()| {
            #[cfg(test)]
            crate::store::probe::bump(&crate::store::probe::WRITES, 1);
            if !sync {
                return Ok(());
            }
            #[cfg(test)]
            crate::store::probe::bump(&crate::store::probe::FSYNCS, 1);
            f.sync_all()
        });
        // A test can fail an append whose bytes are already durable and
        // cannot be cut back off: the worst a failing disk does.
        #[cfg(test)]
        let (appended, stuck) = match appended {
            Ok(()) if crate::store::probe::fail_append() => (
                Err(std::io::Error::other("injected append failure")),
                crate::store::probe::stuck(),
            ),
            other => (other, false),
        };
        #[cfg(not(test))]
        let stuck = false;
        if let Err(e) = appended {
            // Nothing of a failed append may survive: cut the log back to
            // where it was (best effort; the next committer's repair cuts a
            // torn record anyway), and forget what this handle knows of the
            // file, so its next commit re-reads it whole rather than trust a
            // length the cut may not have restored.
            state.wal = None;
            if !stuck {
                if let Ok(f) = std::fs::OpenOptions::new().write(true).open(&self.store.wal_file) {
                    let _ = f.set_len(state.seen.wal_pos);
                    let _ = f.sync_all();
                }
            }
            state.seen.stamp = None;
            return Err(e.into());
        }
        // Durable: account for it, then publish. Nothing below can fail.
        let len = record.len() as u64;
        state.commits_since_sync = if sync { 0 } else { state.commits_since_sync + 1 };
        state.seen.appended(len);
        state.wal_bytes += len;
        state.next_seq += u64::from(count);
        state.ops_since_compact += count as usize;
        let record = std::mem::take(&mut state.staged).into_vec();
        crate::overlay::publish_records(&inner.log, record, 0, first_seq - 1);
        drop(guard);
        let last = self.state.next_seq - 1;
        if self.state.ops_since_compact >= self.store.cfg.compact_after_ops
            || self.state.wal_bytes >= self.store.cfg.compact_after_wal_bytes
        {
            // The commit is durable and visible whatever happens here: a
            // compaction that cannot run now (every ring slot pinned, a full
            // disk) leaves it due for the next commit or [`Store::compact`].
            if !self.store.compact_inline.load(Ordering::Relaxed)
                || crate::compact::compact_locked(self.store, &mut self.state).is_err()
            {
                self.store.due.store(true, Ordering::Release);
            }
        }
        Ok(last)
    }
}

/// A serializable transaction: the commit lock is held from before its read
/// view is taken until its single append lands. Made by [`Store::transact`];
/// stage through it like a [`Writer`], read through [`Txn::snapshot`].
#[derive(Debug)]
pub struct Txn<'t> {
    w: Writer<'t>,
}

impl<'t> Txn<'t> {
    /// The caught-up committed state — every writer's commits up to the
    /// moment the lock was taken, and nothing can commit after it until this
    /// transaction does. Ops staged in this transaction are not in it.
    #[must_use]
    pub fn snapshot(&self) -> &Snapshot<'t> {
        self.w.snap.as_ref().expect("a transaction holds its view for its whole life")
    }

    /// [`Writer::upsert_node`].
    pub fn upsert_node(&mut self, doc: &DocRecord<'_>) -> Result<()> {
        self.w.upsert_node(doc)
    }

    /// [`Writer::set_segments`].
    pub fn set_segments(&mut self, key: UrlKey, segs: &[SegmentRecord<'_>]) -> Result<()> {
        self.w.set_segments(key, segs)
    }

    /// [`Writer::set_edges`].
    pub fn set_edges(&mut self, key: UrlKey, edges: &[(UrlKey, EdgeType, u16)]) {
        self.w.set_edges(key, edges);
    }

    /// [`Writer::set_edges_with_flags`].
    pub fn set_edges_with_flags(&mut self, key: UrlKey, edges: &[(UrlKey, EdgeType, u16, u8)]) {
        self.w.set_edges_with_flags(key, edges);
    }

    /// [`Writer::set_attrs`].
    pub fn set_attrs(&mut self, key: UrlKey, attrs: &[(u8, &str)]) -> Result<()> {
        self.w.set_attrs(key, attrs)
    }

    /// [`Writer::set_named_attrs`].
    pub fn set_named_attrs(&mut self, key: UrlKey, attrs: &[(&str, &str)]) -> Result<()> {
        self.w.set_named_attrs(key, attrs)
    }

    /// [`Writer::touch`], computed from this transaction's caught-up view.
    pub fn touch(&mut self, key: UrlKey, t: Touch<'_>) -> Result<bool> {
        self.w.touch(key, t)
    }

    /// [`Writer::set_pinned`].
    pub fn set_pinned(&mut self, key: UrlKey, pinned: bool) -> Result<bool> {
        self.w.set_pinned(key, pinned)
    }

    /// [`Writer::remove`].
    pub fn remove(&mut self, key: UrlKey) -> Result<()> {
        self.w.remove(key)
    }
}

/// [`Store::transact`] / [`Store::transact_with`]: lock (unless the caller
/// holds `lock`), catch up, run `f` on a caught-up view, then commit what it
/// staged in one append — or, on `Err`, drop it with nothing written.
pub(crate) fn transact<F, R>(
    store: &Store,
    lock: Option<&CommitLock>,
    wait: std::time::Duration,
    f: F,
) -> Result<R>
where
    F: for<'t> FnOnce(&mut Txn<'t>) -> Result<R>,
{
    let mut txn = Txn { w: store.writer()? };
    let held = match lock {
        Some(_) => None,
        None => Some(store.acquire(&mut txn.w.state, wait)?),
    };
    let r = run(store, &mut txn, f);
    if let Some(h) = held {
        txn.w.state.release(h);
    }
    r
}

fn run<'t, F, R>(store: &'t Store, txn: &mut Txn<'t>, f: F) -> Result<R>
where
    F: for<'x> FnOnce(&mut Txn<'x>) -> Result<R>,
{
    store.sync(&mut txn.w.state, true)?;
    txn.w.snap = Some(store.snapshot());
    // Ops a dropped, uncommitted writer left staged on this handle are kept
    // (a writer's next commit includes them); an abort removes only its own.
    let mark = txn.w.state.staged.mark();
    let out = f(txn);
    txn.w.snap = None; // free the pinned slot before any auto-compaction
                       // Either way a failed transaction leaves nothing of its own staged: a
                       // caller retries by running it again.
    match out.and_then(|r| txn.w.append_held().map(|_| r)) {
        Ok(r) => Ok(r),
        Err(e) => {
            txn.w.state.staged.truncate(mark);
            Err(e)
        }
    }
}
