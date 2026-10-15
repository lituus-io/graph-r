// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! Writers and transactions. Mutations are staged as WAL ops, made durable
//! on [`Writer::commit`] (commit lock → catch up → encode → append → fsync
//! per policy → unlock), then published into the current generation's
//! overlay where new snapshots see them immediately. Freshness math runs
//! *here*, at touch time, and the computed state travels inside the op —
//! replay never re-derives policy. [`Txn`] is the serializable form: the
//! lock is held from its read view to its append.

use crate::error::{Error, Result};
use crate::format::base::{nflags, sflags};
use crate::format::wal::{encode_frame, Op, OwnedSeg};
use crate::key::{EdgeType, UrlKey};
use crate::snapshot::Snapshot;
use crate::store::{wal_path, CommitLock, Durability, Held, Seen, Store};
use crate::ttl::Outcome;
use compact_str::CompactString;
use std::io::Write as _;
use std::sync::atomic::Ordering;
use std::sync::MutexGuard;

/// Maximum label length persisted (bytes of UTF-8).
pub const MAX_LABEL: usize = 2048;
/// Maximum segments per document.
pub const MAX_SEGS: usize = 128;
/// Maximum outbound edges per node (wide joins and conditions included).
pub use crate::format::wal::MAX_EDGES;
/// Maximum attributes per node, and the longest attribute value.
pub use crate::format::wal::{MAX_ATTRS, MAX_ATTR_LEN};
/// Most value bytes one node's attribute set may hold in total: what fits a
/// single log frame (the op's fixed fields and per-value headers included).
pub const MAX_ATTR_BYTES: usize = crate::format::wal::MAX_FRAME - (1 + 8 + 1) - MAX_ATTRS * (1 + 4);
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
    staged: Vec<Op>,
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
            .field("staged", &self.staged.len())
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
            staged: Vec::new(),
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
        self.state.staged.push(Op::UpsertNode {
            key: doc.url_key,
            content_hash: doc.content_hash,
            fetched_at_ms: doc.fetched_at_ms,
            flags,
            url: doc.url.into(),
            title: doc.title.map(Into::into),
            snippet: doc.snippet.map(Into::into),
            etag: doc.etag.map(Into::into),
        });
        Ok(())
    }

    /// Stage a whole-set segment replacement for `key`.
    pub fn set_segments(&mut self, key: UrlKey, segs: &[SegmentRecord<'_>]) -> Result<()> {
        if segs.len() > MAX_SEGS {
            return Err(Error::format(format!("more than {MAX_SEGS} segments")));
        }
        let mut owned = Vec::with_capacity(segs.len());
        for s in segs {
            check_label("segment", s.label)?;
            let (importance, extra_flags) = (s.importance, 0);
            let (byte_start, byte_len, range_flag) = match s.byte_range {
                Some((start, len)) => (start, len, 0),
                None => (0, 0, sflags::NO_RANGE),
            };
            owned.push(OwnedSeg {
                label: s.label.into(),
                byte_start,
                byte_len,
                depth: s.depth.max(1),
                importance,
                flags: range_flag | extra_flags,
            });
        }
        self.state.staged.push(Op::SetSegments { key, segs: owned });
        Ok(())
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
        let mut owned: Vec<(UrlKey, u8, u8, u16)> = Vec::new();
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
        self.state.staged.push(Op::SetEdges { key, edges: owned });
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
        let mut owned: Vec<(u8, CompactString)> = Vec::with_capacity(attrs.len());
        for &(k, v) in attrs.iter().rev() {
            if v.len() > MAX_ATTR_LEN {
                return Err(Error::format("attribute too long"));
            }
            if !owned.iter().any(|(o, _)| *o == k) {
                owned.push((k, CompactString::from(v)));
            }
        }
        // Replay reads an oversized frame as a torn tail and a writable open
        // truncates there, so an op that does not fit one frame would lose
        // itself and every later commit on reopen. Bound the whole op.
        if owned.iter().map(|(_, v)| v.len()).sum::<usize>() > MAX_ATTR_BYTES {
            return Err(Error::format(format!(
                "attributes exceed {MAX_ATTR_BYTES} bytes in total"
            )));
        }
        owned.sort_unstable_by_key(|(k, _)| *k);
        self.state.staged.push(Op::SetAttrs { key, attrs: owned });
        Ok(())
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
        let mut owned: Vec<(u8, CompactString)> = Vec::with_capacity(named.len());
        let mut total = 0usize;
        for (k, (name, value)) in named.into_iter().enumerate() {
            let mut rec = CompactString::with_capacity(name.len() + 1 + value.len());
            rec.push_str(name);
            rec.push('\0');
            rec.push_str(value);
            total += rec.len();
            owned.push((k as u8, rec));
        }
        if total > MAX_ATTR_BYTES {
            return Err(Error::format(format!(
                "attributes exceed {MAX_ATTR_BYTES} bytes in total"
            )));
        }
        self.state.staged.push(Op::SetAttrs { key, attrs: owned });
        Ok(())
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
        let staged = self.state.staged.iter().rev().find_map(|op| match op {
            Op::UpsertNode { key: k, content_hash, etag, .. } if *k == key => {
                Some((*content_hash, etag.clone()))
            }
            _ => None,
        });
        let (interval_s, checks, changes, last_change_ms, content_hash, etag) = {
            let snap = self.view();
            let Some(node) = snap.node(key) else { return Ok(false) };
            let cfg = snap.ttl();
            let next = cfg.next_interval(node.interval_s.max(1), t.outcome);
            let checks = node.checks.saturating_add(1);
            let changes = if t.outcome == Outcome::Changed {
                node.changes.saturating_add(1)
            } else {
                node.changes
            };
            let last_change =
                if t.outcome == Outcome::Changed { t.checked_at_ms } else { node.last_change_ms };
            // The op carries the node's *full* effective state, not a delta:
            // touches fold last-wins in the overlay, so an unchanged check
            // must not erase the hash/etag a prior change observed — nor the
            // ones an upsert staged earlier in this same batch.
            let (base_hash, base_etag) = match staged {
                Some((h, e)) => (h, e),
                None => (node.content_hash, node.etag.map(Into::into)),
            };
            let hash = t.content_hash.unwrap_or(base_hash);
            let etag: Option<compact_str::CompactString> = t.etag.map(Into::into).or(base_etag);
            (next, checks, changes, last_change, hash, etag)
        };
        self.state.staged.push(Op::Touch {
            key,
            checked_at_ms: t.checked_at_ms,
            outcome: t.outcome.as_tag(),
            content_hash: Some(content_hash),
            etag,
            interval_s,
            checks,
            changes,
            last_change_ms,
            tombstone: t.outcome == Outcome::Gone,
        });
        Ok(true)
    }

    /// Stage a pin/unpin. Returns `false` (staging nothing) when `key` is
    /// neither committed nor upserted earlier in this same batch — a pin on a
    /// document that does not exist must not lie in wait for a future upsert.
    pub fn set_pinned(&mut self, key: UrlKey, pinned: bool) -> Result<bool> {
        let exists = self
            .state
            .staged
            .iter()
            .any(|op| matches!(op, Op::UpsertNode { key: k, .. } if *k == key))
            || self.view().node(key).is_some();
        if !exists {
            return Ok(false);
        }
        self.state.staged.push(Op::SetPinned { key, pinned });
        Ok(true)
    }

    /// Stage a hard delete.
    pub fn remove(&mut self, key: UrlKey) -> Result<()> {
        self.state.staged.push(Op::Remove { key });
        Ok(())
    }

    /// Make staged ops durable and visible: take the commit lock (waiting up
    /// to [`crate::Config::lock_wait`], else [`Error::Locked`] with the ops
    /// still staged), catch up with other writers, encode frames, append to
    /// the WAL, fsync per the configured [`Durability`], publish to the
    /// overlay, auto-compact past thresholds, unlock. Returns the last
    /// committed sequence number.
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
    /// up. A failed append is cut back off the log, so it writes nothing.
    fn append_held(&mut self) -> Result<u64> {
        if self.state.staged.is_empty() {
            return Ok(self.state.next_seq.saturating_sub(1));
        }
        let staged = std::mem::take(&mut self.state.staged);
        let count = staged.len() as u64;
        let first_seq = self.state.next_seq;
        let mut buf = Vec::new();
        for (i, op) in staged.iter().enumerate() {
            buf.extend_from_slice(&encode_frame(first_seq + i as u64, op));
        }
        // Append + flush + fsync-per-policy.
        if self.state.wal.is_none() {
            self.state.wal =
                Some(std::fs::OpenOptions::new().append(true).open(wal_path(&self.store.dir))?);
        }
        let durability = self.store.cfg.durability;
        let state = &mut *self.state;
        let f = state.wal.as_mut().expect("opened above");
        let sync = match durability {
            Durability::Always => true,
            Durability::Batch(n) => state.commits_since_sync + 1 >= n.max(1),
            Durability::Never => false,
        };
        let appended = f.write_all(&buf).and_then(|()| f.flush()).and_then(|()| {
            if !sync {
                return Ok(());
            }
            #[cfg(test)]
            crate::store::probe::bump(&crate::store::probe::FSYNCS, 1);
            f.sync_all()
        });
        if let Err(e) = appended {
            // Nothing of a failed append may survive: cut the log back to
            // where it was (best effort; the next committer's repair would
            // treat a partial frame as a torn tail anyway).
            state.wal = None;
            if let Ok(f) = std::fs::OpenOptions::new().write(true).open(wal_path(&self.store.dir)) {
                let _ = f.set_len(state.seen.wal_pos);
                let _ = f.sync_all();
            }
            return Err(e.into());
        }
        state.commits_since_sync = if sync { 0 } else { state.commits_since_sync + 1 };
        state.seen.appended(buf.len() as u64);
        // Publish into the current generation (stable: installs on this
        // handle happen only under its writer mutex, which we hold).
        let cur = self.store.current.load(Ordering::Acquire);
        {
            let guard = self.store.gens[cur]
                .inner
                .try_read()
                .map_err(|_| Error::busy("current slot mid-install"))?;
            let inner = guard.as_ref().ok_or_else(|| Error::corrupt("current slot empty"))?;
            for (i, op) in staged.into_iter().enumerate() {
                if !inner.arena.publish(first_seq + i as u64, op) {
                    return Err(Error::busy("overlay arena full"));
                }
            }
        }
        self.state.wal_bytes += buf.len() as u64;
        self.state.next_seq += count;
        self.state.ops_since_compact += count as usize;
        let last = self.state.next_seq - 1;
        if self.state.ops_since_compact >= self.store.cfg.compact_after_ops
            || self.state.wal_bytes >= self.store.cfg.compact_after_wal_bytes
        {
            if self.store.compact_inline.load(Ordering::Relaxed) {
                crate::compact::compact_locked(self.store, &mut self.state)?;
            } else {
                // Off the hot path: the owner compacts when it chooses.
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
    let mark = txn.w.state.staged.len();
    let out = f(txn);
    txn.w.snap = None; // free the pinned slot before any auto-compaction
    match out {
        Ok(r) => txn.w.append_held().map(|_| r),
        Err(e) => {
            txn.w.state.staged.truncate(mark);
            Err(e)
        }
    }
}
