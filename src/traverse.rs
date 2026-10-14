// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
//! Zero-alloc traversal. [`LendingIterator`] is the crate's GAT seam: each
//! step lends a view tied to the iterator's borrow, so implementations are
//! free to reuse internal state per step without allocating — and future
//! sources that materialize per-step stay possible behind the same trait.

use crate::key::{EdgeType, NodeId, UrlKey};

/// A GAT-based iterator whose items may borrow from the iterator itself.
pub trait LendingIterator {
    /// The lent item type.
    type Item<'a>
    where
        Self: 'a;
    /// Advance and lend the next item.
    fn next(&mut self) -> Option<Self::Item<'_>>;
}

/// One outbound edge as seen from a snapshot.
#[derive(Clone, Copy, Debug)]
pub struct EdgeRef<'a> {
    /// Destination node id in the current base generation, when resolved.
    pub dst: Option<NodeId>,
    /// Destination durable key (always present).
    pub dst_key: UrlKey,
    /// Destination canonical URL, when the destination is a known node with
    /// a URL label (stubs have none).
    pub dst_url: Option<&'a str>,
    /// Edge type.
    pub etype: EdgeType,
    /// Raw edge flags (`format::base::eflags`).
    pub flags: u8,
    /// Weight in 1/65535 units.
    pub weight: u16,
}

impl EdgeRef<'_> {
    /// True when the edge was inferred rather than extracted from source.
    #[must_use]
    pub fn is_inferred(&self) -> bool {
        self.flags & crate::format::base::eflags::INFERRED != 0
    }
}

/// Lending iterator over a node's outbound edges. Constructed by
/// [`crate::Snapshot::neighbors`]; each step resolves the destination lazily
/// against the snapshot, allocating nothing.
#[derive(Debug)]
pub struct Neighbors<'s> {
    pub(crate) inner: NeighborsInner<'s>,
}

#[derive(Debug)]
pub(crate) enum NeighborsInner<'s> {
    /// CSR run in the base file.
    Base { snap: &'s crate::snapshot::Snapshot<'s>, edges: &'s [u8], idx: usize, end: usize },
    /// Whole-set replacement living in the overlay.
    Overlay {
        snap: &'s crate::snapshot::Snapshot<'s>,
        edges: &'s [crate::format::wal::OwnedEdge],
        idx: usize,
    },
    /// The node has no edges.
    Empty,
}

impl LendingIterator for Neighbors<'_> {
    type Item<'a>
        = EdgeRef<'a>
    where
        Self: 'a;

    fn next(&mut self) -> Option<EdgeRef<'_>> {
        match &mut self.inner {
            NeighborsInner::Base { snap, edges, idx, end } => {
                if idx >= end {
                    return None;
                }
                let rec = &edges[*idx * crate::format::base::EDGE_LEN
                    ..(*idx + 1) * crate::format::base::EDGE_LEN];
                *idx += 1;
                let dst = NodeId(crate::bytesio::get_u32(rec, 0));
                let (dst_key, dst_url) = snap.node_identity(dst);
                Some(EdgeRef {
                    dst: Some(dst),
                    dst_key,
                    dst_url,
                    etype: EdgeType::from_tag(rec[4]),
                    flags: rec[5],
                    weight: crate::bytesio::get_u16(rec, 6),
                })
            }
            NeighborsInner::Overlay { snap, edges, idx } => {
                let (dst_key, etype, flags, weight) = *edges.get(*idx)?;
                *idx += 1;
                let dst = snap.base_node_id(dst_key);
                let dst_url = dst.and_then(|id| snap.node_identity(id).1);
                Some(EdgeRef {
                    dst,
                    dst_key,
                    dst_url,
                    etype: EdgeType::from_tag(etype),
                    flags,
                    weight,
                })
            }
            NeighborsInner::Empty => None,
        }
    }
}

/// Which edges a [`crate::Snapshot::walk`] follows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    /// Outbound: from a node to what it points at (a column to the columns
    /// it derives from — upstream lineage).
    Out,
    /// Inbound: from a node to what points at it (a column to the columns
    /// computed from it — downstream impact).
    In,
}

/// One node reached by a walk.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Hop {
    /// The node reached.
    pub key: UrlKey,
    /// The node it was reached from.
    pub from: UrlKey,
    /// The type of the edge crossed.
    pub etype: EdgeType,
    /// Hops from the start (1 for direct neighbours).
    pub depth: u32,
    /// Every edge on the path carries `eflags::EXACT` (an unchanged copy).
    pub exact_chain: bool,
}

/// Reusable walk state (visited set and frontiers). Keep one per thread:
/// once its buffers have grown, walks allocate nothing.
#[derive(Debug, Default)]
pub struct Walk {
    seen: std::collections::HashSet<u64>,
    frontier: Vec<(UrlKey, bool)>,
    next: Vec<(UrlKey, bool)>,
}

impl Walk {
    /// Empty walk state.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl crate::Snapshot<'_> {
    /// Breadth-first walk from `from` along edges in `dir` whose type
    /// `follow` accepts, at most `max_depth` hops. `visit` sees every node
    /// reached once, nearest first, and returns whether to continue past it
    /// (so "the nearest described ancestor" is a walk that stops at
    /// described nodes).
    pub fn walk(
        &self,
        from: UrlKey,
        dir: Direction,
        max_depth: u32,
        follow: impl Fn(EdgeType) -> bool,
        w: &mut Walk,
        visit: impl FnMut(Hop) -> bool,
    ) {
        let out = dir == Direction::Out;
        self.walk_along(from, out, !out, max_depth, follow, w, visit);
    }

    /// [`crate::Snapshot::walk`] along outbound **and** inbound edges at once: the
    /// lineage neighbourhood of a node (what it derives from and what derives
    /// from it, and so on outwards). Each node is reached once, at its
    /// shortest distance over either direction; a node's outbound edges are
    /// offered before its inbound ones.
    pub fn walk_both(
        &self,
        from: UrlKey,
        max_depth: u32,
        follow: impl Fn(EdgeType) -> bool,
        w: &mut Walk,
        visit: impl FnMut(Hop) -> bool,
    ) {
        self.walk_along(from, true, true, max_depth, follow, w, visit);
    }

    #[allow(clippy::too_many_arguments, clippy::fn_params_excessive_bools)] // private seam of the two walks
    fn walk_along(
        &self,
        from: UrlKey,
        out: bool,
        inbound: bool,
        max_depth: u32,
        follow: impl Fn(EdgeType) -> bool,
        w: &mut Walk,
        mut visit: impl FnMut(Hop) -> bool,
    ) {
        w.seen.clear();
        w.frontier.clear();
        w.seen.insert(from.0);
        w.frontier.push((from, true));
        let exact = crate::format::base::eflags::EXACT;
        for depth in 1..=max_depth {
            w.next.clear();
            for k in 0..w.frontier.len() {
                let (node, chain) = w.frontier[k];
                let mut step = |key: UrlKey, etype: EdgeType, flags: u8, w: &mut Walk| {
                    if !follow(etype) || !w.seen.insert(key.0) {
                        return;
                    }
                    let hop = Hop {
                        key,
                        from: node,
                        etype,
                        depth,
                        exact_chain: chain && flags & exact != 0,
                    };
                    if visit(hop) {
                        w.next.push((key, hop.exact_chain));
                    }
                };
                if out {
                    let mut it = self.neighbors(node);
                    while let Some(e) = it.next() {
                        step(e.dst_key, e.etype, e.flags, w);
                    }
                }
                if inbound {
                    self.in_edges(node, |src, etype, flags, _| step(src, etype, flags, w));
                }
            }
            if w.next.is_empty() {
                break;
            }
            std::mem::swap(&mut w.frontier, &mut w.next);
        }
    }
}
