// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>
//
//! Read views. A [`Snapshot`] pins one base generation plus the overlay ops
//! committed before it was taken; everything it returns — node refs, edge
//! refs, hits, due items — borrows either the mmap or the overlay arena, so
//! reads are zero-copy and never invalidated by concurrent commits.
//!
//! Structural results (neighbors, ranks, communities, the lexicon behind
//! query seeds) reflect the last **compaction**; point lookups, freshness
//! and due-lists additionally see every committed op. The link-r bridge
//! compacts after ingest, so in that flow the distinction disappears.
//!
//! Cost model: taking a snapshot is O(1); the first read that consults the
//! overlay folds the pending ops (bounded by the compaction threshold) into
//! a per-snapshot view, cached for the snapshot's lifetime — so batch many
//! reads per snapshot rather than one snapshot per read when a writer is
//! active.

use crate::error::Result;
use crate::format::base::{self, nflags, NodeRec, SegRec};
use crate::key::{NodeId, SegKey, UrlKey};
use crate::overlay::{OverlayNode, OverlayView};
use crate::store::{GenInner, Store};
use crate::traverse::{Neighbors, NeighborsInner};
use crate::ttl::TtlConfig;
use std::cell::OnceCell;
use std::sync::RwLockReadGuard;

/// A pinned, immutable read view of the store.
pub struct Snapshot<'s> {
    store: &'s Store,
    guard: RwLockReadGuard<'s, Option<GenInner>>,
    frontier: usize,
    view: OnceCell<OverlayView>,
    hub_threshold: OnceCell<u32>,
    live: OnceCell<usize>,
}

impl std::fmt::Debug for Snapshot<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Snapshot")
            .field("generation", &self.generation())
            .field("overlay_ops", &self.frontier)
            .finish()
    }
}

/// A document node resolved through a snapshot. All strings are borrowed.
#[derive(Clone, Copy, Debug)]
pub struct NodeRef<'s> {
    /// Durable key.
    pub key: UrlKey,
    /// Dense id in the current base generation; `None` for nodes that only
    /// exist in the overlay (committed after the last compaction).
    pub id: Option<NodeId>,
    /// Canonical URL ("" for stubs).
    pub url: &'s str,
    /// Title, if known.
    pub title: Option<&'s str>,
    /// Distilled snippet, if known.
    pub snippet: Option<&'s str>,
    /// Last seen entity tag, if any (needed for conditional revalidation).
    pub etag: Option<&'s str>,
    /// xxh3 of the last-seen body.
    pub content_hash: u64,
    /// Last successful fetch/revalidation stamp.
    pub fetched_at_ms: u64,
    /// Last observed content change stamp.
    pub last_change_ms: u64,
    /// Current adaptive revalidation interval, seconds.
    pub interval_s: u32,
    /// Revalidations observed (saturating).
    pub checks: u16,
    /// Content changes observed (saturating).
    pub changes: u16,
    /// Node flags (`format::base::nflags`).
    pub flags: u16,
    /// Rank percentile in permille (0 for overlay-fresh nodes).
    pub rank_permille: u16,
}

impl NodeRef<'_> {
    /// Pinned nodes are exempt from eviction sweeps.
    #[must_use]
    pub fn pinned(&self) -> bool {
        self.flags & nflags::PINNED != 0
    }
    /// True for edge-target stubs never ingested themselves.
    #[must_use]
    pub fn is_stub(&self) -> bool {
        self.flags & nflags::STUB != 0
    }
    /// True when the source was observed gone.
    #[must_use]
    pub fn is_tombstone(&self) -> bool {
        self.flags & nflags::TOMBSTONE != 0
    }
}

/// A sub-document segment reference: an anchor, never content.
#[derive(Clone, Copy, Debug)]
pub struct SegRef<'s> {
    /// Durable segment key.
    pub key: SegKey,
    /// Heading-path label.
    pub label: &'s str,
    /// Byte range in the source document, when known.
    pub byte_range: Option<(u32, u32)>,
    /// Heading depth (1 = H1 …).
    pub depth: u8,
    /// Importance in 1/65535 units.
    pub importance: u16,
    /// Raw segment flags (`format::base::sflags`).
    pub flags: u8,
}

/// One entry of a due-for-revalidation work list.
#[derive(Clone, Copy, Debug)]
pub struct DueItem<'s> {
    /// Canonical URL to revalidate.
    pub url: &'s str,
    /// Durable key.
    pub key: UrlKey,
    /// Last seen entity tag to send as `If-None-Match`.
    pub etag: Option<&'s str>,
    /// How far past due, in milliseconds.
    pub overdue_ms: u64,
    /// Rank percentile (higher = revalidate first).
    pub rank_permille: u16,
}

impl<'s> Snapshot<'s> {
    pub(crate) fn new(store: &'s Store, guard: RwLockReadGuard<'s, Option<GenInner>>) -> Self {
        let frontier = guard.as_ref().map_or(0, |g| g.arena.len());
        Self {
            store,
            guard,
            frontier,
            view: OnceCell::new(),
            hub_threshold: OnceCell::new(),
            live: OnceCell::new(),
        }
    }

    fn inner(&self) -> &GenInner {
        self.guard.as_ref().expect("snapshot pins a populated slot")
    }

    pub(crate) fn ttl(&self) -> &TtlConfig {
        &self.store.cfg.ttl
    }

    pub(crate) fn overlay(&self) -> &OverlayView {
        self.view.get_or_init(|| OverlayView::build(self.inner().arena.committed(self.frontier)))
    }

    /// The base generation this snapshot pins.
    #[must_use]
    pub fn generation(&self) -> u64 {
        self.inner().dir.header.generation
    }

    /// Committed-but-uncompacted op count visible to this snapshot.
    #[must_use]
    pub fn pending_ops(&self) -> usize {
        self.frontier
    }

    // ---- raw section access -------------------------------------------------

    pub(crate) fn bytes(&self) -> &[u8] {
        self.inner().bytes()
    }
    pub(crate) fn nodes_bytes(&self) -> &[u8] {
        self.inner().dir.section(self.bytes(), base::SectionKind::Nodes).unwrap_or(&[])
    }
    pub(crate) fn labels(&self) -> &[u8] {
        self.inner().dir.section(self.bytes(), base::SectionKind::Labels).unwrap_or(&[])
    }
    pub(crate) fn segs_bytes(&self) -> &[u8] {
        self.inner().dir.section(self.bytes(), base::SectionKind::Segs).unwrap_or(&[])
    }
    pub(crate) fn edges_bytes(&self) -> &[u8] {
        self.inner().dir.section(self.bytes(), base::SectionKind::Edges).unwrap_or(&[])
    }
    pub(crate) fn edge_index(&self) -> &[u8] {
        self.inner().dir.section(self.bytes(), base::SectionKind::EdgeIndex).unwrap_or(&[])
    }
    pub(crate) fn ranks_bytes(&self) -> &[u8] {
        self.inner().dir.section(self.bytes(), base::SectionKind::Ranks).unwrap_or(&[])
    }
    pub(crate) fn lexicon(&self) -> &[u8] {
        self.inner().dir.section(self.bytes(), base::SectionKind::Lexicon).unwrap_or(&[])
    }
    pub(crate) fn postings(&self) -> &[u8] {
        self.inner().dir.section(self.bytes(), base::SectionKind::Postings).unwrap_or(&[])
    }

    /// Node count in the base (including stubs/tombstones).
    #[must_use]
    pub fn base_len(&self) -> usize {
        self.inner().dir.header.node_count as usize
    }

    pub(crate) fn rec(&self, id: NodeId) -> NodeRec<'_> {
        let i = id.0 as usize;
        NodeRec(&self.nodes_bytes()[i * base::NODE_LEN..(i + 1) * base::NODE_LEN])
    }

    pub(crate) fn base_node_id(&self, key: UrlKey) -> Option<NodeId> {
        base::find_node(self.nodes_bytes(), key).map(NodeId)
    }

    /// (key, url) of a base node — used by edge resolution.
    pub(crate) fn node_identity(&self, id: NodeId) -> (UrlKey, Option<&str>) {
        let rec = self.rec(id);
        (rec.url_key(), base::label_at(self.labels(), rec.url_off()))
    }

    pub(crate) fn rank_permille_of(&self, id: NodeId) -> u16 {
        let ranks = self.ranks_bytes();
        let off = id.0 as usize * base::RANK_LEN;
        if off + base::RANK_LEN <= ranks.len() {
            crate::bytesio::get_u16(ranks, off)
        } else {
            0
        }
    }

    /// Community id of a base node (smallest member node id), if ranked.
    #[must_use]
    pub fn community_of(&self, id: NodeId) -> Option<u32> {
        let ranks = self.ranks_bytes();
        let off = id.0 as usize * base::RANK_LEN;
        (off + base::RANK_LEN <= ranks.len()).then(|| crate::bytesio::get_u32(ranks, off + 4))
    }

    // ---- merged lookups -----------------------------------------------------

    /// Look up a node by durable key, merging base and overlay state.
    #[must_use]
    pub fn node(&self, key: UrlKey) -> Option<NodeRef<'_>> {
        let patch = self.overlay().map.get(&key.0);
        if let Some(p) = patch {
            if p.removed {
                return None;
            }
        }
        let base_id = self.base_node_id(key);
        match (base_id, patch.and_then(|p| p.core.as_ref())) {
            (_, Some(core)) => {
                // Overlay core wins wholesale (it is the newest fetch).
                let p = patch.expect("core implies patch");
                let mut flags = core.flags;
                // Pin resolution: an explicit overlay pin state (from
                // SetPinned or folded upsert history) is authoritative;
                // otherwise pins are sticky across the base generation —
                // an unpinned re-ingest never silently unpins.
                match p.pinned {
                    Some(pinned) => {
                        flags =
                            if pinned { flags | nflags::PINNED } else { flags & !nflags::PINNED };
                    }
                    None => {
                        if let (Some(id), false) = (base_id, p.severed) {
                            if self.rec(id).flags() & nflags::PINNED != 0 {
                                flags |= nflags::PINNED;
                            }
                        }
                    }
                }
                let (mut fetched, mut interval, mut checks, mut changes, mut last_change) =
                    (core.fetched_at_ms, self.ttl().base_s, 0u16, 0u16, 0u64);
                let mut content_hash = core.content_hash;
                let mut etag: Option<&str> = core.etag.as_deref();
                if let Some(t) = &p.touch {
                    fetched = t.checked_at_ms;
                    interval = t.interval_s;
                    checks = t.checks;
                    changes = t.changes;
                    last_change = t.last_change_ms;
                    if let Some(h) = t.content_hash {
                        content_hash = h;
                    }
                    if let Some(e) = t.etag.as_deref() {
                        etag = Some(e);
                    }
                    if t.tombstone {
                        flags |= nflags::TOMBSTONE;
                    }
                } else {
                    // No touch since the upsert: counters (history) always
                    // carry forward — from overlay history first, else from
                    // the base record; the interval keeps its learned value
                    // only when the content is in fact unchanged (a re-absorb
                    // of an identical page must not reset backoff).
                    if let Some((c, ch, lc)) = p.carried {
                        checks = c;
                        changes = ch;
                        last_change = lc;
                    } else if let (Some(id), false) = (base_id, p.severed) {
                        let rec = self.rec(id);
                        checks = rec.checks();
                        changes = rec.changes();
                        last_change = rec.last_change_ms();
                    }
                    if let (Some(id), false) = (base_id, p.severed) {
                        let rec = self.rec(id);
                        if rec.content_hash() == content_hash {
                            interval = rec.interval_s().max(1);
                        }
                    }
                }
                Some(NodeRef {
                    key,
                    id: base_id,
                    url: core.url.as_str(),
                    title: core.title.as_deref(),
                    snippet: core.snippet.as_deref(),
                    etag,
                    content_hash,
                    fetched_at_ms: fetched,
                    last_change_ms: last_change,
                    interval_s: interval,
                    checks,
                    changes,
                    flags,
                    rank_permille: base_id.map_or(0, |id| self.rank_permille_of(id)),
                })
            }
            (Some(id), None) => Some(self.base_node_ref(id, patch)),
            (None, None) => None,
        }
    }

    pub(crate) fn base_node_ref<'a>(
        &'a self,
        id: NodeId,
        patch: Option<&'a OverlayNode>,
    ) -> NodeRef<'a> {
        let rec = self.rec(id);
        let labels = self.labels();
        let mut flags = rec.flags();
        let (mut fetched, mut interval, mut checks, mut changes, mut last_change) = (
            rec.fetched_at_ms(),
            rec.interval_s(),
            rec.checks(),
            rec.changes(),
            rec.last_change_ms(),
        );
        let mut content_hash = rec.content_hash();
        let mut etag = base::label_at(labels, rec.etag_off());
        if let Some(p) = patch {
            if let Some(pinned) = p.pinned {
                flags = if pinned { flags | nflags::PINNED } else { flags & !nflags::PINNED };
            }
            if let Some(t) = &p.touch {
                fetched = t.checked_at_ms;
                interval = t.interval_s;
                checks = t.checks;
                changes = t.changes;
                last_change = t.last_change_ms;
                if let Some(h) = t.content_hash {
                    content_hash = h;
                }
                if let Some(e) = t.etag.as_deref() {
                    etag = Some(e);
                }
                if t.tombstone {
                    flags |= nflags::TOMBSTONE;
                }
            }
        }
        NodeRef {
            key: rec.url_key(),
            id: Some(id),
            url: base::label_at(labels, rec.url_off()).unwrap_or(""),
            title: base::label_at(labels, rec.title_off()),
            snippet: base::label_at(labels, rec.snippet_off()),
            etag,
            content_hash,
            fetched_at_ms: fetched,
            last_change_ms: last_change,
            interval_s: interval,
            checks,
            changes,
            flags,
            rank_permille: self.rank_permille_of(id),
        }
    }

    /// Node by dense id in this generation.
    #[must_use]
    pub fn node_by_id(&self, id: NodeId) -> Option<NodeRef<'_>> {
        if id.0 as usize >= self.base_len() {
            return None;
        }
        let key = self.rec(id).url_key();
        self.node(key)
    }

    /// Live (non-stub, non-tombstone, non-removed) document count.
    #[must_use]
    pub fn len(&self) -> usize {
        *self.live.get_or_init(|| {
            let mut n = 0usize;
            self.for_each_live(|_| n += 1);
            n
        })
    }

    /// True when no live documents exist.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Visit every live node (base then overlay-fresh, both in key order for
    /// determinism where it matters to callers).
    pub(crate) fn for_each_live<'a>(&'a self, mut f: impl FnMut(NodeRef<'a>)) {
        let view = self.overlay();
        for i in 0..self.base_len() {
            let id = NodeId(i as u32);
            let key = self.rec(id).url_key();
            let patch = view.map.get(&key.0);
            if patch.is_some_and(|p| p.removed) {
                continue;
            }
            let nref = match patch.and_then(|p| p.core.as_ref()) {
                Some(_) => self.node(key).expect("core implies live"),
                None => self.base_node_ref(id, patch),
            };
            if nref.is_stub() || nref.is_tombstone() {
                continue;
            }
            f(nref);
        }
        // Overlay-fresh nodes (no base record), in ascending key order.
        let mut fresh: Vec<u64> = view
            .map
            .iter()
            .filter(|(k, p)| {
                !p.removed && p.core.is_some() && self.base_node_id(UrlKey(**k)).is_none()
            })
            .map(|(k, _)| *k)
            .collect();
        fresh.sort_unstable();
        for k in fresh {
            if let Some(nref) = self.node(UrlKey(k)) {
                if !nref.is_tombstone() {
                    f(nref);
                }
            }
        }
    }

    /// Outbound edges of `key`. Overlay whole-set replacements win over the
    /// base CSR run.
    #[must_use]
    pub fn neighbors(&self, key: UrlKey) -> Neighbors<'_> {
        if let Some(p) = self.overlay().map.get(&key.0) {
            if p.removed {
                return Neighbors { inner: NeighborsInner::Empty };
            }
            if let Some(edges) = &p.edges {
                return Neighbors { inner: NeighborsInner::Overlay { snap: self, edges, idx: 0 } };
            }
            if p.severed {
                // Resurrected after a remove: the base record's old edges are
                // history the remove already discarded.
                return Neighbors { inner: NeighborsInner::Empty };
            }
        }
        let Some(id) = self.base_node_id(key) else {
            return Neighbors { inner: NeighborsInner::Empty };
        };
        let idx = self.edge_index();
        if idx.is_empty() {
            return Neighbors { inner: NeighborsInner::Empty };
        }
        let start = crate::bytesio::get_u32(idx, id.0 as usize * 4) as usize;
        let end = crate::bytesio::get_u32(idx, (id.0 as usize + 1) * 4) as usize;
        Neighbors {
            inner: NeighborsInner::Base { snap: self, edges: self.edges_bytes(), idx: start, end },
        }
    }

    /// The node's segments (anchors). Overlay whole-set replacement wins.
    #[must_use]
    pub fn segments(&self, key: UrlKey) -> Vec<SegRef<'_>> {
        if let Some(p) = self.overlay().map.get(&key.0) {
            if p.removed {
                return Vec::new();
            }
            if p.segs.is_none() && p.severed {
                return Vec::new();
            }
            if let Some(segs) = &p.segs {
                let mut out: Vec<SegRef<'_>> = segs
                    .iter()
                    .map(|s| SegRef {
                        key: SegKey::derive(key, &s.label),
                        label: s.label.as_str(),
                        byte_range: (s.flags & base::sflags::NO_RANGE == 0)
                            .then_some((s.byte_start, s.byte_len)),
                        depth: s.depth,
                        importance: s.importance,
                        flags: s.flags,
                    })
                    .collect();
                self.apply_importance_overrides(key, &mut out);
                return out;
            }
        }
        let Some(id) = self.base_node_id(key) else { return Vec::new() };
        let rec = self.rec(id);
        let segs = self.segs_bytes();
        let labels = self.labels();
        let start = rec.seg_start() as usize;
        let mut out = Vec::with_capacity(usize::from(rec.seg_count()));
        for i in start..start + usize::from(rec.seg_count()) {
            let s = SegRec(&segs[i * base::SEG_LEN..(i + 1) * base::SEG_LEN]);
            out.push(SegRef {
                key: SegKey(s.seg_key()),
                label: base::label_at(labels, s.label_off()).unwrap_or(""),
                byte_range: (s.flags() & base::sflags::NO_RANGE == 0)
                    .then(|| (s.byte_start(), s.byte_len())),
                depth: s.depth(),
                importance: s.importance(),
                flags: s.flags(),
            });
        }
        self.apply_importance_overrides(key, &mut out);
        out
    }

    fn apply_importance_overrides(&self, key: UrlKey, segs: &mut [SegRef<'_>]) {
        if let Some(p) = self.overlay().map.get(&key.0) {
            for &(ordinal, imp) in &p.importance {
                if let Some(s) = segs.get_mut(usize::from(ordinal)) {
                    s.importance = imp;
                    s.flags |= base::sflags::LLM_SCORED;
                }
            }
        }
    }

    /// Shortest forward path (over outbound edges) from `from` to `to`,
    /// bounded by `max_depth` hops. Deterministic: neighbors expand in
    /// storage order, ties broken by first arrival.
    #[must_use]
    pub fn path(&self, from: UrlKey, to: UrlKey, max_depth: u8) -> Option<Vec<UrlKey>> {
        if from == to {
            return Some(vec![from]);
        }
        let mut prev: std::collections::HashMap<u64, u64> = std::collections::HashMap::new();
        let mut frontier = vec![from];
        prev.insert(from.0, from.0);
        for _ in 0..max_depth {
            let mut next = Vec::new();
            for &u in &frontier {
                let mut it = self.neighbors(u);
                while let Some(e) = crate::traverse::LendingIterator::next(&mut it) {
                    let v = e.dst_key;
                    if prev.contains_key(&v.0) {
                        continue;
                    }
                    prev.insert(v.0, u.0);
                    if v == to {
                        let mut path = vec![v.0];
                        let mut cur = u.0;
                        while cur != from.0 {
                            path.push(cur);
                            cur = prev[&cur];
                        }
                        path.push(from.0);
                        path.reverse();
                        return Some(path.into_iter().map(UrlKey).collect());
                    }
                    next.push(v);
                }
            }
            if next.is_empty() {
                return None;
            }
            frontier = next;
        }
        None
    }

    /// The p99 total-degree hub threshold for this generation (BFS refuses to
    /// expand *through* nodes at or above it).
    #[must_use]
    pub fn hub_threshold(&self) -> u32 {
        *self.hub_threshold.get_or_init(|| {
            let n = self.base_len();
            if n == 0 {
                return u32::MAX;
            }
            let idx = self.edge_index();
            let mut degrees: Vec<u32> = (0..n)
                .map(|i| {
                    let out = if idx.is_empty() {
                        0
                    } else {
                        crate::bytesio::get_u32(idx, (i + 1) * 4)
                            - crate::bytesio::get_u32(idx, i * 4)
                    };
                    out + self.rec(NodeId(i as u32)).in_degree()
                })
                .collect();
            degrees.sort_unstable();
            let p99 = degrees[(n * 99 / 100).min(n - 1)];
            p99.max(8)
        })
    }

    /// Everything due for revalidation at `now_ms`, most important first.
    #[must_use]
    pub fn due(&self, now_ms: u64, max: usize) -> Vec<DueItem<'_>> {
        let cfg = self.ttl();
        let mut out: Vec<DueItem<'_>> = Vec::new();
        // Pinned nodes still revalidate: pinning guards eviction, not TTL.
        self.for_each_live(|n| {
            let next = cfg.next_check_at_ms(n.fetched_at_ms, n.interval_s.max(1), n.rank_permille);
            if next <= now_ms && !n.url.is_empty() {
                out.push(DueItem {
                    url: n.url,
                    key: n.key,
                    etag: n.etag,
                    overdue_ms: now_ms - next,
                    rank_permille: n.rank_permille,
                });
            }
        });
        out.sort_by(|a, b| {
            b.rank_permille
                .cmp(&a.rank_permille)
                .then(b.overdue_ms.cmp(&a.overdue_ms))
                .then(a.key.0.cmp(&b.key.0))
        });
        out.truncate(max);
        out
    }

    /// Documents worth pinning: highest-ranked live nodes not already pinned.
    #[must_use]
    pub fn pin_suggestions(&self, k: usize) -> Vec<&str> {
        let mut ranked: Vec<(u16, u64, &str)> = Vec::new();
        self.for_each_live(|n| {
            if !n.pinned() && !n.url.is_empty() {
                ranked.push((n.rank_permille, n.key.0, n.url));
            }
        });
        ranked.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
        ranked.into_iter().take(k).map(|(_, _, u)| u).collect()
    }

    /// Keys touched by the overlay visible to this snapshot (unsorted).
    pub(crate) fn overlay_keys(&self) -> Vec<u64> {
        self.overlay().map.keys().copied().collect()
    }

    /// Validate internal invariants cheaply (used by tests).
    pub fn check(&self) -> Result<()> {
        // Force the overlay view and lexicon parse; any panic here would be a
        // bug, any error a corrupt file that BaseDir::parse should have
        // rejected already.
        let _ = self.overlay();
        let _ = self.hub_threshold();
        Ok(())
    }
}
