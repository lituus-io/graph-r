// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>
//
//! The committed-op overlay: an append-only, lock-free arena of WAL ops that
//! have been fsynced but not yet folded into a base snapshot, plus the merged
//! per-key view a snapshot builds over it.
//!
//! The arena is chunked so published entries never move — a reader holding a
//! `&Op` borrow stays valid for its snapshot's lifetime. Publication is a
//! `OnceLock` cell initialization followed by a `Release` bump of the
//! frontier; a snapshot captures the frontier once with `Acquire` and only
//! ever reads below it, which is the entire snapshot-isolation story. No
//! `unsafe`, no locks on the read path.

use crate::format::wal::{Op, OwnedEdge, OwnedSeg};
use compact_str::CompactString;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::OnceLock;

/// Chunk 0 capacity; chunk `k` holds `BASE_CAP << k` entries.
const BASE_CAP: usize = 64;
/// Number of chunks (total capacity ≈ 67M ops — far beyond any compaction
/// threshold; hitting the end returns an error upstream rather than UB).
const CHUNKS: usize = 20;

type Cell = OnceLock<(u64, Op)>;

/// Append-only arena of committed ops.
pub(crate) struct Arena {
    chunks: [OnceLock<Box<[Cell]>>; CHUNKS],
    frontier: AtomicUsize,
}

impl std::fmt::Debug for Arena {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Arena").field("len", &self.len()).finish()
    }
}

fn locate(i: usize) -> (usize, usize) {
    let x = i / BASE_CAP + 1;
    let k = (usize::BITS - 1 - x.leading_zeros()) as usize;
    let before = BASE_CAP * ((1 << k) - 1);
    (k, i - before)
}

impl Arena {
    pub(crate) fn new() -> Self {
        Self { chunks: std::array::from_fn(|_| OnceLock::new()), frontier: AtomicUsize::new(0) }
    }

    /// Committed entry count (Acquire: pairs with the publish Release).
    pub(crate) fn len(&self) -> usize {
        self.frontier.load(Ordering::Acquire)
    }

    fn cell(&self, i: usize) -> Option<&Cell> {
        let (k, off) = locate(i);
        let chunk = self.chunks.get(k)?;
        let chunk = chunk.get_or_init(|| {
            (0..BASE_CAP << k).map(|_| OnceLock::new()).collect::<Vec<_>>().into_boxed_slice()
        });
        Some(&chunk[off])
    }

    /// Publish one committed op. Single-producer: only the writer (holding
    /// the store's write mutex) calls this, so `frontier` is not contended.
    /// Returns `false` when the arena is full (caller must compact).
    pub(crate) fn publish(&self, seq: u64, op: Op) -> bool {
        let i = self.frontier.load(Ordering::Relaxed);
        let Some(cell) = self.cell(i) else { return false };
        let ok = cell.set((seq, op)).is_ok();
        debug_assert!(ok, "arena cell double-initialized");
        self.frontier.store(i + 1, Ordering::Release);
        true
    }

    /// Iterate entries `[0, upto)`; `upto` must be a frontier value captured
    /// from `len()`, so every cell below it is initialized.
    pub(crate) fn committed(&self, upto: usize) -> impl Iterator<Item = &(u64, Op)> + '_ {
        (0..upto).map(move |i| {
            self.cell(i).and_then(OnceLock::get).expect("cell below frontier is initialized")
        })
    }
}

/// The folded per-key effect of the overlay ops visible to one snapshot.
#[derive(Debug, Default)]
pub(crate) struct OverlayNode {
    pub(crate) core: Option<CoreFields>,
    pub(crate) segs: Option<Vec<OwnedSeg>>,
    pub(crate) edges: Option<Vec<OwnedEdge>>,
    pub(crate) touch: Option<TouchFields>,
    /// Freshness counters carried across an upsert (checks, changes,
    /// `last_change_ms`): history survives re-ingest; only `Remove` clears it.
    pub(crate) carried: Option<(u16, u16, u64)>,
    pub(crate) pinned: Option<bool>,
    pub(crate) importance: Vec<(u8, u16)>,
    pub(crate) removed: bool,
    /// A `Remove` happened in this overlay: a later resurrecting upsert must
    /// not inherit the (not yet compacted) base record's history or pin.
    pub(crate) severed: bool,
}

/// Last-written node core (from `Op::UpsertNode`).
#[derive(Debug, Clone)]
pub(crate) struct CoreFields {
    pub(crate) content_hash: u64,
    pub(crate) fetched_at_ms: u64,
    pub(crate) flags: u16,
    pub(crate) url: CompactString,
    pub(crate) title: Option<CompactString>,
    pub(crate) snippet: Option<CompactString>,
    pub(crate) etag: Option<CompactString>,
}

/// Last-written freshness state (from `Op::Touch`; the writer pre-computes
/// cumulative counters, so last-wins folding is exact).
#[derive(Debug, Clone)]
pub(crate) struct TouchFields {
    pub(crate) checked_at_ms: u64,
    pub(crate) content_hash: Option<u64>,
    pub(crate) etag: Option<CompactString>,
    pub(crate) interval_s: u32,
    pub(crate) checks: u16,
    pub(crate) changes: u16,
    pub(crate) last_change_ms: u64,
    pub(crate) tombstone: bool,
}

/// Merged overlay view for a snapshot: key → folded state, in op order.
#[derive(Debug, Default)]
pub(crate) struct OverlayView {
    pub(crate) map: HashMap<u64, OverlayNode>,
}

impl OverlayView {
    pub(crate) fn build<'a>(ops: impl Iterator<Item = &'a (u64, Op)>) -> Self {
        let mut map: HashMap<u64, OverlayNode> = HashMap::new();
        for (_, op) in ops {
            let entry = map.entry(op.key().0).or_default();
            match op {
                Op::UpsertNode { content_hash, fetched_at_ms, flags, url, title, snippet, etag, .. } => {
                    // An upsert resurrects a removed key and supersedes prior
                    // freshness (the fetch that produced it is the new stamp).
                    // Pins are sticky: once any pin state is known within this
                    // overlay, fold it forward as the authoritative override
                    // (base-generation stickiness is applied at read time).
                    const PINNED: u16 = crate::format::base::nflags::PINNED;
                    let upsert_pin = *flags & PINNED != 0;
                    let prior_core_pin =
                        entry.core.as_ref().is_some_and(|c| c.flags & PINNED != 0);
                    // Explicit pin state ORs with the upsert; otherwise a pin
                    // can only be *proven* here (any pinning upsert), never
                    // disproven — an all-unpinned history stays deferred so
                    // read time can apply base-generation stickiness.
                    let folded_pin = match entry.pinned {
                        Some(prev) => Some(upsert_pin || prev),
                        None if upsert_pin || prior_core_pin => Some(true),
                        None => None,
                    };
                    entry.removed = false;
                    entry.carried = entry
                        .touch
                        .as_ref()
                        .map(|t| (t.checks, t.changes, t.last_change_ms))
                        .or(entry.carried);
                    entry.touch = None;
                    entry.core = Some(CoreFields {
                        content_hash: *content_hash,
                        fetched_at_ms: *fetched_at_ms,
                        flags: *flags,
                        url: url.clone(),
                        title: title.clone(),
                        snippet: snippet.clone(),
                        etag: etag.clone(),
                    });
                    entry.pinned = folded_pin;
                }
                Op::SetSegments { segs, .. } => entry.segs = Some(segs.clone()),
                Op::SetEdges { edges, .. } => entry.edges = Some(edges.clone()),
                Op::Touch {
                    checked_at_ms,
                    content_hash,
                    etag,
                    interval_s,
                    checks,
                    changes,
                    last_change_ms,
                    tombstone,
                    ..
                } => {
                    entry.touch = Some(TouchFields {
                        checked_at_ms: *checked_at_ms,
                        content_hash: *content_hash,
                        etag: etag.clone(),
                        interval_s: *interval_s,
                        checks: *checks,
                        changes: *changes,
                        last_change_ms: *last_change_ms,
                        tombstone: *tombstone,
                    });
                }
                Op::Remove { .. } => {
                    *entry = OverlayNode { removed: true, severed: true, ..OverlayNode::default() };
                }
                Op::SetPinned { pinned, .. } => entry.pinned = Some(*pinned),
                Op::SetImportance { scores, .. } => {
                    for &(ordinal, imp) in scores {
                        match entry.importance.iter_mut().find(|(o, _)| *o == ordinal) {
                            Some(slot) => slot.1 = imp,
                            None => entry.importance.push((ordinal, imp)),
                        }
                    }
                }
            }
        }
        Self { map }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key::UrlKey;

    #[test]
    fn locate_walks_chunk_boundaries() {
        assert_eq!(locate(0), (0, 0));
        assert_eq!(locate(63), (0, 63));
        assert_eq!(locate(64), (1, 0));
        assert_eq!(locate(191), (1, 127));
        assert_eq!(locate(192), (2, 0));
    }

    #[test]
    fn publish_then_read_below_frontier() {
        let a = Arena::new();
        for i in 0..200u64 {
            assert!(a.publish(i + 1, Op::Remove { key: UrlKey(i) }));
        }
        assert_eq!(a.len(), 200);
        let seen: Vec<u64> = a.committed(200).map(|(s, _)| *s).collect();
        assert_eq!(seen, (1..=200).collect::<Vec<_>>());
    }

    #[test]
    fn view_folds_last_writer_wins_and_remove_clears() {
        let key = UrlKey(5);
        let ops = [
            (1, Op::SetPinned { key, pinned: true }),
            (2, Op::Remove { key }),
            (3, Op::UpsertNode {
                key,
                content_hash: 9,
                fetched_at_ms: 10,
                flags: 0,
                url: "https://x.dev/a".into(),
                title: None,
                snippet: None,
                etag: None,
            }),
        ];
        let view = OverlayView::build(ops.iter());
        let n = &view.map[&5];
        assert!(!n.removed, "upsert resurrects");
        assert!(n.pinned.is_none(), "remove cleared the earlier pin");
        assert_eq!(n.core.as_ref().unwrap().content_hash, 9);
    }
}
