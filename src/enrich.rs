// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>
//
//! The LLM seam (feature `llm`). graph-r ships no model client and no vendor
//! SDK — just this trait. An enricher sees compact borrowed context (node,
//! anchors, neighbor titles), proposes segment-importance scores and related
//! edges, and everything it returns is stamped enrichment-tier + inferred so
//! crawl-tier replacement can never destroy it and provenance stays honest.
//!
//! The future type is a GAT, matching link-r's async-trait style: sync
//! enrichers use [`std::future::Ready`]; async ones name their future (or
//! box it themselves if they prefer — their call).
//!
//! # Wiring any HTTP LLM endpoint
//!
//! ```no_run
//! # #[cfg(feature = "llm")] mod demo {
//! use graph_r::{EnrichContext, Enricher, Enrichment};
//!
//! struct MyEnricher;
//! impl Enricher for MyEnricher {
//!     type Error = String;
//!     type Fut<'a> = std::future::Ready<Result<Enrichment, String>>;
//!     fn enrich<'a>(&'a self, ctx: EnrichContext<'a>) -> Self::Fut<'a> {
//!         // Serialize ctx into your prompt, call any HTTP client, parse the
//!         // model's JSON into scores/edges. Shown here as a no-op.
//!         let _ = &ctx;
//!         std::future::ready(Ok(Enrichment::default()))
//!     }
//! }
//! # }
//! ```

use crate::key::UrlKey;
use crate::snapshot::{NodeRef, SegRef};
use smallvec::SmallVec;

/// What an enricher proposes. Confidence below 0.5 is dropped at apply time
/// (drop-on-ambiguity: a weak guess is worse than no edge).
#[derive(Clone, Debug, Default)]
pub struct Enrichment {
    /// Per-segment-ordinal importance overrides (1/65535 units).
    pub importance: SmallVec<[(u8, u16); 16]>,
    /// Proposed related-document edges `(target, weight 1/65535, confidence 0..=1)`.
    pub related: SmallVec<[(UrlKey, u16, f32); 8]>,
}

/// Borrowed context handed to an enricher — everything it may ground on,
/// nothing it could leak bodies from.
#[derive(Clone, Debug)]
pub struct EnrichContext<'a> {
    /// The node being enriched.
    pub node: NodeRef<'a>,
    /// Its segment anchors.
    pub segments: &'a [SegRef<'a>],
    /// Titles of its one-hop neighbors, for grounding.
    pub neighbor_titles: &'a [&'a str],
}

/// A proposer of enrichment for one node. GAT future; no vendor coupling.
pub trait Enricher {
    /// Enricher-defined error (surfaced to the caller, never persisted).
    type Error: std::fmt::Display;
    /// The future returned by [`Enricher::enrich`].
    type Fut<'a>: std::future::Future<Output = Result<Enrichment, Self::Error>> + 'a
    where
        Self: 'a;
    /// Propose enrichment for the node in `ctx`.
    fn enrich<'a>(&'a self, ctx: EnrichContext<'a>) -> Self::Fut<'a>;
}

impl crate::writer::Writer<'_> {
    /// Apply an enrichment proposal to `key`: importance overrides land as
    /// segment scores, related edges land enrichment-tier + inferred with the
    /// proposal confidence folded into the weight. Proposals with confidence
    /// `< 0.5` are dropped.
    pub fn apply_enrichment(&mut self, key: UrlKey, e: &Enrichment) -> crate::Result<()> {
        if !e.importance.is_empty() {
            self.set_importance(key, &e.importance)?;
        }
        let kept: Vec<(UrlKey, crate::key::EdgeType, u16)> = e
            .related
            .iter()
            .filter(|(_, _, conf)| *conf >= 0.5)
            .map(|&(dst, weight, conf)| {
                let scaled = (f32::from(weight) * conf) as u16;
                (dst, crate::key::EdgeType::Related, scaled.max(1))
            })
            .collect();
        if !kept.is_empty() {
            self.set_edges_flagged(
                key,
                &kept,
                crate::format::base::eflags::TIER_ENRICH | crate::format::base::eflags::INFERRED,
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn low_confidence_proposals_are_dropped() {
        let e = Enrichment {
            importance: SmallVec::new(),
            related: SmallVec::from_vec(vec![
                (UrlKey(1), 60_000, 0.9),
                (UrlKey(2), 60_000, 0.4), // dropped
            ]),
        };
        let kept: Vec<_> = e.related.iter().filter(|(_, _, c)| *c >= 0.5).collect();
        assert_eq!(kept.len(), 1);
    }
}
