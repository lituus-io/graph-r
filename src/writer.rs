// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>
//
//! The single writer. Mutations are staged as WAL ops, made durable on
//! [`Writer::commit`] (encode → append → fsync per policy), then published
//! into the current generation's overlay where new snapshots see them
//! immediately. Freshness math runs *here*, at touch time, and the computed
//! state travels inside the op — replay never re-derives policy.

use crate::error::{Error, Result};
use crate::format::base::{nflags, sflags};
use crate::format::wal::{encode_frame, Op, OwnedSeg};
use crate::key::{EdgeType, SegKey, UrlKey};
use crate::store::{wal_path, Durability, Store};
use crate::ttl::Outcome;
use std::io::Write as _;
use std::sync::atomic::Ordering;
use std::sync::MutexGuard;

/// Maximum label length persisted (bytes of UTF-8).
pub const MAX_LABEL: usize = 2048;
/// Maximum segments per document.
pub const MAX_SEGS: usize = 128;
/// Maximum outbound edges per document (mirrors link-r).
pub const MAX_EDGES: usize = 64;

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
    pub(crate) fn new(next_seq: u64, replayed_ops: usize, wal_bytes: u64) -> Self {
        Self {
            next_seq,
            staged: Vec::new(),
            ops_since_compact: replayed_ops,
            wal_bytes,
            commits_since_sync: 0,
            wal: None,
        }
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

/// The exclusive write handle; holds the store's writer mutex for its
/// lifetime. Stage mutations, then [`Writer::commit`] to make them durable
/// and visible.
#[derive(Debug)]
pub struct Writer<'s> {
    store: &'s Store,
    state: MutexGuard<'s, WriteState>,
}

fn check_label(name: &str, s: &str) -> Result<()> {
    if s.len() > MAX_LABEL {
        return Err(Error::format(format!("{name} label exceeds {MAX_LABEL} bytes")));
    }
    Ok(())
}

impl<'s> Writer<'s> {
    pub(crate) fn new(store: &'s Store, state: MutexGuard<'s, WriteState>) -> Self {
        Self { store, state }
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

    /// Stage a whole-set segment replacement for `key`. Enrichment-scored
    /// importance survives for segments whose heading path (and therefore
    /// [`SegKey`]) is unchanged.
    pub fn set_segments(&mut self, key: UrlKey, segs: &[SegmentRecord<'_>]) -> Result<()> {
        if segs.len() > MAX_SEGS {
            return Err(Error::format(format!("more than {MAX_SEGS} segments")));
        }
        // Carry forward LLM-scored importance by segment identity.
        let carried: Vec<(u64, u16)> = {
            let snap = self.store.snapshot();
            snap.segments(key)
                .iter()
                .filter(|s| s.flags & sflags::LLM_SCORED != 0)
                .map(|s| (s.key.0, s.importance))
                .collect()
        };
        let mut owned = Vec::with_capacity(segs.len());
        for s in segs {
            check_label("segment", s.label)?;
            let seg_key = SegKey::derive(key, s.label);
            let (importance, extra_flags) = match carried.iter().find(|(k, _)| *k == seg_key.0) {
                Some(&(_, imp)) => (imp, sflags::LLM_SCORED),
                None => (s.importance, 0),
            };
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
        self.set_edges_flagged(key, edges, 0);
    }

    pub(crate) fn set_edges_flagged(
        &mut self,
        key: UrlKey,
        edges: &[(UrlKey, EdgeType, u16)],
        extra_flags: u8,
    ) {
        // Tier-scoped replacement over a whole-set op: edges belonging to the
        // *other* tier are preserved, so a crawl re-ingest can never destroy
        // enrichment edges and vice versa. New edges win a destination tie.
        let writing_enrich = extra_flags & crate::format::base::eflags::TIER_ENRICH != 0;
        let preserved: Vec<(UrlKey, u8, u8, u16)> = {
            let snap = self.store.snapshot();
            let mut kept = Vec::new();
            let mut it = snap.neighbors(key);
            while let Some(e) = crate::traverse::LendingIterator::next(&mut it) {
                if e.is_enrichment() != writing_enrich {
                    kept.push((e.dst_key, e.etype.as_tag(), e.flags, e.weight));
                }
            }
            kept
        };
        let mut seen = std::collections::HashSet::with_capacity(edges.len().min(MAX_EDGES));
        let mut owned: Vec<(UrlKey, u8, u8, u16)> = Vec::with_capacity(edges.len().min(MAX_EDGES));
        for &(dst, etype, weight) in edges {
            if dst == key || !seen.insert(dst.0) {
                continue;
            }
            let mut flags = extra_flags;
            if matches!(etype, EdgeType::Related) {
                flags |= crate::format::base::eflags::INFERRED;
            }
            owned.push((dst, etype.as_tag(), flags, weight));
            if owned.len() == MAX_EDGES {
                break;
            }
        }
        for (dst, etype, flags, weight) in preserved {
            if owned.len() == MAX_EDGES {
                break;
            }
            if seen.insert(dst.0) {
                owned.push((dst, etype, flags, weight));
            }
        }
        owned.sort_unstable_by_key(|(dst, _, _, _)| dst.0);
        self.state.staged.push(Op::SetEdges { key, edges: owned });
    }

    /// Record a revalidation outcome. Freshness policy (interval growth/cut,
    /// counters) is computed here from the node's current state and the
    /// store's [`crate::TtlConfig`]; returns `false` when `key` is unknown.
    pub fn touch(&mut self, key: UrlKey, t: Touch<'_>) -> Result<bool> {
        if let Some(e) = t.etag {
            check_label("etag", e)?;
        }
        let (interval_s, checks, changes, last_change_ms, content_hash, etag) = {
            let snap = self.store.snapshot();
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
            // must not erase the hash/etag a prior change observed.
            let hash = t.content_hash.unwrap_or(node.content_hash);
            let etag: Option<compact_str::CompactString> =
                t.etag.map(Into::into).or_else(|| node.etag.map(Into::into));
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

    /// Stage enrichment importance overrides `(segment ordinal, importance)`.
    pub fn set_importance(&mut self, key: UrlKey, scores: &[(u8, u16)]) -> Result<()> {
        if scores.len() > MAX_SEGS {
            return Err(Error::format("too many importance overrides"));
        }
        self.state.staged.push(Op::SetImportance { key, scores: scores.to_vec() });
        Ok(())
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
            || self.store.snapshot().node(key).is_some();
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

    /// Number of staged (uncommitted) ops.
    #[must_use]
    pub fn staged(&self) -> usize {
        self.state.staged.len()
    }

    /// Make staged ops durable and visible: encode frames, append to the
    /// WAL, fsync per the configured [`Durability`], publish to the overlay,
    /// then auto-compact past thresholds. Returns the last committed
    /// sequence number.
    pub fn commit(&mut self) -> Result<u64> {
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
        {
            let state = &mut *self.state;
            let f = state.wal.as_mut().expect("opened above");
            f.write_all(&buf)?;
            f.flush()?;
            let sync = match self.store.cfg.durability {
                Durability::Always => true,
                Durability::Batch(n) => {
                    state.commits_since_sync += 1;
                    state.commits_since_sync >= n.max(1)
                }
                Durability::Never => false,
            };
            if sync {
                f.sync_all()?;
                state.commits_since_sync = 0;
            }
        }
        // Publish into the current generation (stable while we hold the
        // writer mutex — only the writer compacts).
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
            crate::compact::compact_locked(self.store, &mut self.state)?;
        }
        Ok(last)
    }
}
