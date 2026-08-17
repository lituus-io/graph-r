// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>
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
    /// True when the edge came from the enrichment tier.
    #[must_use]
    pub fn is_enrichment(&self) -> bool {
        self.flags & crate::format::base::eflags::TIER_ENRICH != 0
    }
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
