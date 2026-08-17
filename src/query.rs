// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>
//
//! Local lookups that answer with **references** — URL + anchor + snippet —
//! inside a token budget, never with document bodies. The recipe:
//!
//! 1. IDF-weighted tiered lexical scoring over node/segment labels
//!    (exact ×1000, prefix ×100, substring ×1), scaled by squared
//!    term-coverage so partial matches of many terms beat a lucky exact
//!    match of one.
//! 2. Seed selection with a per-term guarantee (every query term seats its
//!    best node) capped at a few seeds.
//! 3. Bounded BFS expansion that refuses to expand *through* hub nodes
//!    (≥ p99 degree), decaying by hop, edge weight, and confidence.
//! 4. Fusion with persisted importance, then deterministic seeds-first
//!    rendering that stops before the token budget is exceeded.

use crate::format::base::{self, LexRec};
use crate::key::{EdgeType, NodeId, UrlKey};
use crate::snapshot::Snapshot;
use crate::traverse::LendingIterator;
use compact_str::CompactString;
use std::collections::HashMap;

/// Token allowance for a rendered answer (~4 bytes/token estimate).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TokenBudget(pub u32);

impl Default for TokenBudget {
    fn default() -> Self {
        Self(2000)
    }
}

/// Query options.
#[derive(Clone, Copy, Debug)]
pub struct QueryOpts {
    /// Maximum hits returned (budget permitting).
    pub limit: usize,
    /// BFS expansion depth from the seeds.
    pub depth: u8,
    /// Maximum seed nodes.
    pub max_seeds: usize,
    /// Output token budget.
    pub budget: TokenBudget,
}

impl Default for QueryOpts {
    fn default() -> Self {
        Self { limit: 20, depth: 3, max_seeds: 3, budget: TokenBudget::default() }
    }
}

/// A sub-document anchor attached to a hit: fetch `url` and slice by the
/// byte range (or resolve the heading) instead of re-reading the whole page.
#[derive(Clone, Copy, Debug)]
pub struct Anchor<'s> {
    /// Heading-path label.
    pub label: &'s str,
    /// Byte range in the source document, when known.
    pub byte_range: Option<(u32, u32)>,
    /// Segment importance in 1/65535 units.
    pub importance: u16,
}

/// One lookup answer entry. Borrowed from the snapshot; cheap to produce.
#[derive(Clone, Copy, Debug)]
pub struct Hit<'s> {
    /// Durable key.
    pub key: UrlKey,
    /// Canonical URL.
    pub url: &'s str,
    /// Title, if known.
    pub title: Option<&'s str>,
    /// Distilled snippet, if known.
    pub snippet: Option<&'s str>,
    /// Best matching segment anchor, if any.
    pub anchor: Option<Anchor<'s>>,
    /// Fused relevance score (higher is better).
    pub score: f32,
    /// True when this hit seeded the traversal (direct lexical match).
    pub seed: bool,
}

const TIER_EXACT: f32 = 1000.0;
const TIER_PREFIX: f32 = 100.0;
const TIER_SUBSTR: f32 = 1.0;
const HOP_DECAY: f32 = 0.5;
const CONF_INFERRED: f32 = 0.6;
const FUSE_TRAVERSAL: f32 = 0.8;
const FUSE_RANK: f32 = 0.2;

/// Run `f` for every lowercase token (Unicode-alphanumeric runs) of `text`.
/// One tokenizer for indexing (compaction) and querying, so they always
/// agree — the same rule link-r applies to its BM25 terms.
pub(crate) fn for_each_token(text: &str, mut f: impl FnMut(&str)) {
    let mut buf = String::new();
    for run in text.split(|c: char| !c.is_alphanumeric()) {
        if run.is_empty() {
            continue;
        }
        if run.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit()) {
            f(run);
        } else {
            buf.clear();
            buf.extend(run.chars().flat_map(char::to_lowercase));
            f(&buf);
        }
    }
}

fn dedup_terms(query: &str) -> Vec<CompactString> {
    let mut terms: Vec<CompactString> = Vec::new();
    for_each_token(query, |t| {
        if t.len() >= 2 && !terms.iter().any(|x| x == t) {
            terms.push(CompactString::from(t));
        }
    });
    terms
}

#[derive(Default, Clone, Copy)]
struct NodeScore {
    tiered: f32,
    matched: u32,
    last_term: u32,
    best_term_tier: f32,
}

impl<'s> Snapshot<'s> {
    /// Answer a plain-language `query` with ranked URL + anchor references.
    #[must_use]
    #[allow(clippy::too_many_lines)] // the scoring pipeline reads best as one pass
    pub fn query(&'s self, query: &str, opts: &QueryOpts) -> Vec<Hit<'s>> {
        let terms = dedup_terms(query);
        if terms.is_empty() {
            return Vec::new();
        }
        let n_base = self.base_len();
        let n_docs = n_base.max(1) as f32;
        let lex = self.lexicon();
        let labels = self.labels();
        let lex_count = lex.len() / base::LEX_LEN;

        // ---- lexical scoring over the base lexicon --------------------------
        let mut scores: HashMap<u32, NodeScore> = HashMap::new();
        let mut per_term_best: Vec<Option<(f32, u32)>> = vec![None; terms.len()];
        for (ti, term) in terms.iter().enumerate() {
            let hash = xxhash_rust::xxh3::xxh3_64(term.as_bytes());
            let mut df_for_idf: u32 = 0;
            // (node id → best tier weight) for this term.
            let mut tiers: HashMap<u32, f32> = HashMap::new();
            for i in 0..lex_count {
                let rec = LexRec(&lex[i * base::LEX_LEN..(i + 1) * base::LEX_LEN]);
                let token = base::label_at(labels, rec.label_off()).unwrap_or("");
                let tier = if rec.token_hash() == hash || token == term.as_str() {
                    TIER_EXACT
                } else if token.starts_with(term.as_str()) {
                    TIER_PREFIX
                } else if token.contains(term.as_str()) {
                    TIER_SUBSTR
                } else {
                    continue;
                };
                if tier >= TIER_EXACT {
                    df_for_idf = df_for_idf.max(rec.df());
                }
                let range =
                    rec.postings_off() as usize..(rec.postings_off() + rec.postings_len()) as usize;
                if let Ok(ids) = base::decode_postings(&self.postings()[range]) {
                    for id in ids {
                        let e = tiers.entry(id).or_insert(0.0);
                        if tier > *e {
                            *e = tier;
                        }
                    }
                }
            }
            let idf = (1.0 + n_docs / (1.0 + df_for_idf as f32)).ln().max(0.1);
            for (id, tier) in tiers {
                let contribution = tier * idf;
                let s = scores.entry(id).or_default();
                if s.last_term != ti as u32 + 1 {
                    s.matched += 1;
                    s.last_term = ti as u32 + 1;
                }
                s.tiered += contribution;
                if contribution > s.best_term_tier {
                    s.best_term_tier = contribution;
                }
                let best = per_term_best[ti];
                if best.is_none_or(|(b, bid)| contribution > b || (contribution >= b && id < bid)) {
                    per_term_best[ti] = Some((contribution, id));
                }
            }
        }

        // Coverage² scaling.
        let mut base_scores: Vec<(u32, f32)> = scores
            .iter()
            .map(|(&id, s)| {
                let coverage = s.matched as f32 / terms.len() as f32;
                (id, s.tiered * coverage * coverage)
            })
            .collect();
        base_scores.sort_by(|a, b| {
            b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal).then(a.0.cmp(&b.0))
        });

        // ---- overlay-fresh nodes (not yet compacted): direct matches --------
        let mut fresh_hits: Vec<(u64, f32)> = Vec::new();
        for k in self.overlay_keys() {
            if self.base_node_id(UrlKey(k)).is_some() {
                continue;
            }
            let Some(node) = self.node(UrlKey(k)) else { continue };
            if node.is_tombstone() || node.url.is_empty() {
                continue;
            }
            let mut hay: Vec<CompactString> = Vec::new();
            let searchable = node.url.split_once("://").map_or(node.url, |(_, r)| r);
            for text in [Some(searchable), node.title, node.snippet].into_iter().flatten() {
                for_each_token(text, |t| hay.push(CompactString::from(t)));
            }
            let mut tiered = 0.0f32;
            let mut matched = 0u32;
            for term in &terms {
                let tier = hay
                    .iter()
                    .map(|tok| {
                        if tok == term {
                            TIER_EXACT
                        } else if tok.starts_with(term.as_str()) {
                            TIER_PREFIX
                        } else if tok.contains(term.as_str()) {
                            TIER_SUBSTR
                        } else {
                            0.0
                        }
                    })
                    .fold(0.0f32, f32::max);
                if tier > 0.0 {
                    matched += 1;
                    tiered += tier * (1.0 + n_docs).ln();
                }
            }
            if matched > 0 {
                let coverage = matched as f32 / terms.len() as f32;
                fresh_hits.push((k, tiered * coverage * coverage));
            }
        }
        fresh_hits.sort_by(|a, b| {
            b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal).then(a.0.cmp(&b.0))
        });

        // ---- seed selection --------------------------------------------------
        let mut seeds: Vec<u32> = Vec::new();
        for best in per_term_best.iter().flatten() {
            if !seeds.contains(&best.1) {
                seeds.push(best.1);
            }
        }
        seeds.sort_unstable();
        seeds.truncate(opts.max_seeds);
        for &(id, _) in &base_scores {
            if seeds.len() >= opts.max_seeds {
                break;
            }
            if !seeds.contains(&id) {
                seeds.push(id);
            }
        }

        // ---- bounded, hub-refusing BFS --------------------------------------
        let hub = self.hub_threshold();
        let score_of: HashMap<u32, f32> = base_scores.iter().copied().collect();
        let mut arrival: HashMap<u32, f32> = HashMap::new();
        let mut frontier: Vec<(u32, f32)> = Vec::new();
        for &s in &seeds {
            let sc = score_of.get(&s).copied().unwrap_or(0.0);
            arrival.insert(s, sc);
            frontier.push((s, sc));
        }
        for _hop in 0..opts.depth {
            let mut next: Vec<(u32, f32)> = Vec::new();
            for &(u, u_score) in &frontier {
                let u_key = self.rec(NodeId(u)).url_key();
                let is_seed = seeds.contains(&u);
                let degree = {
                    let idx = self.edge_index();
                    let out = if idx.is_empty() {
                        0
                    } else {
                        crate::bytesio::get_u32(idx, (u as usize + 1) * 4)
                            - crate::bytesio::get_u32(idx, u as usize * 4)
                    };
                    out + self.rec(NodeId(u)).in_degree()
                };
                if !is_seed && degree >= hub {
                    continue; // hubs may be reported, never traversed through
                }
                let mut it = self.neighbors(u_key);
                while let Some(e) = it.next() {
                    let Some(dst) = e.dst else { continue };
                    let conf = if e.is_inferred() { CONF_INFERRED } else { 1.0 };
                    let w = f32::from(e.weight.max(1)) / 65_535.0;
                    let sc = u_score * HOP_DECAY * w * conf;
                    if sc <= 0.0 {
                        continue;
                    }
                    let slot = arrival.entry(dst.0).or_insert(0.0);
                    if sc > *slot {
                        *slot = sc;
                        next.push((dst.0, sc));
                    }
                }
            }
            if next.is_empty() {
                break;
            }
            frontier = next;
        }

        // ---- fusion + hit assembly ------------------------------------------
        let max_arrival = arrival.values().copied().fold(0.0f32, f32::max).max(f32::MIN_POSITIVE);
        let max_fresh =
            fresh_hits.iter().map(|&(_, s)| s).fold(0.0f32, f32::max).max(f32::MIN_POSITIVE);
        let mut ranked: Vec<(bool, f32, u64)> = Vec::new();
        for (&id, &a) in &arrival {
            let node_id = NodeId(id);
            let rec = self.rec(node_id);
            if rec.flags() & base::nflags::TOMBSTONE != 0 || rec.flags() & base::nflags::STUB != 0 {
                continue;
            }
            let key = rec.url_key();
            if self.overlay().map.get(&key.0).is_some_and(|p| p.removed) {
                continue;
            }
            let fused = FUSE_TRAVERSAL * (a / max_arrival)
                + FUSE_RANK * f32::from(self.rank_permille_of(node_id)) / 1000.0;
            ranked.push((seeds.contains(&id), fused, key.0));
        }
        for &(k, s) in &fresh_hits {
            ranked.push((true, FUSE_TRAVERSAL * (s / max_fresh), k));
        }
        ranked.sort_by(|a, b| {
            b.0.cmp(&a.0)
                .then(b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal))
                .then(a.2.cmp(&b.2))
        });

        // ---- budgeted render -------------------------------------------------
        let mut out: Vec<Hit<'s>> = Vec::new();
        let mut spent: u32 = 0;
        for (seed, score, key) in ranked {
            if out.len() >= opts.limit {
                break;
            }
            let Some(node) = self.node(UrlKey(key)) else { continue };
            let anchor = self.best_anchor(UrlKey(key), &terms);
            let cost = estimate_tokens(node.url, node.title, node.snippet, anchor.as_ref());
            if spent + cost > opts.budget.0 {
                break;
            }
            spent += cost;
            out.push(Hit {
                key: UrlKey(key),
                url: node.url,
                title: node.title,
                snippet: node.snippet,
                anchor,
                score,
                seed,
            });
        }
        out
    }

    fn best_anchor(&'s self, key: UrlKey, terms: &[CompactString]) -> Option<Anchor<'s>> {
        let segs = self.segments(key);
        let mut best: Option<(u32, u16, Anchor<'s>)> = None;
        for s in segs {
            let mut matches = 0u32;
            for_each_token(s.label, |tok| {
                if terms.iter().any(|t| tok == t.as_str() || tok.starts_with(t.as_str())) {
                    matches += 1;
                }
            });
            if matches == 0 {
                continue;
            }
            let cand =
                Anchor { label: s.label, byte_range: s.byte_range, importance: s.importance };
            let better = match &best {
                None => true,
                Some((m, imp, _)) => matches > *m || (matches == *m && s.importance > *imp),
            };
            if better {
                best = Some((matches, s.importance, cand));
            }
        }
        best.map(|(_, _, a)| a)
    }

    /// Documents most related to `key`: outbound links and similarity edges,
    /// strongest first.
    #[must_use]
    pub fn related(&'s self, key: UrlKey, k: usize) -> Vec<Hit<'s>> {
        let mut ranked: Vec<(f32, u64)> = Vec::new();
        let mut it = self.neighbors(key);
        while let Some(e) = it.next() {
            let conf = if e.is_inferred() { CONF_INFERRED } else { 1.0 };
            let bonus = if matches!(e.etype, EdgeType::Link) { 0.25 } else { 0.0 };
            ranked.push((f32::from(e.weight) / 65_535.0 * conf + bonus, e.dst_key.0));
        }
        ranked.sort_by(|a, b| {
            b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal).then(a.1.cmp(&b.1))
        });
        let mut out = Vec::new();
        for (score, dst) in ranked {
            if out.len() >= k {
                break;
            }
            let Some(node) = self.node(UrlKey(dst)) else { continue };
            if node.is_stub() || node.is_tombstone() {
                continue;
            }
            out.push(Hit {
                key: UrlKey(dst),
                url: node.url,
                title: node.title,
                snippet: node.snippet,
                anchor: None,
                score,
                seed: false,
            });
        }
        out
    }

    /// The community around `key`: its stable id, a hub-derived name, and the
    /// top members by rank.
    #[must_use]
    pub fn community_summary(&'s self, key: UrlKey, top: usize) -> Option<CommunitySummary<'s>> {
        let id = self.base_node_id(key)?;
        let community = self.community_of(id)?;
        let mut members: Vec<(u16, u32)> = (0..self.base_len() as u32)
            .filter(|&i| self.community_of(NodeId(i)) == Some(community))
            .map(|i| (self.rank_permille_of(NodeId(i)), i))
            .collect();
        members.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
        let size = members.len();
        let name = members
            .first()
            .and_then(|&(_, i)| self.node_by_id(NodeId(i)))
            .and_then(|n| n.title.or(Some(n.url)))
            .unwrap_or("");
        let top_urls = members
            .iter()
            .take(top)
            .filter_map(|&(_, i)| self.node_by_id(NodeId(i)).map(|n| n.url))
            .collect();
        Some(CommunitySummary { id: community, name, size, top_urls })
    }

    /// Cross-community edges, strongest first — the connections most likely
    /// to be surprising, one representative per community pair.
    #[must_use]
    pub fn surprises(&'s self, k: usize) -> Vec<(Hit<'s>, Hit<'s>)> {
        let mut seen: std::collections::HashSet<(u32, u32)> = std::collections::HashSet::new();
        let mut ranked: Vec<(f32, u64, u64)> = Vec::new();
        for i in 0..self.base_len() as u32 {
            let src = NodeId(i);
            let Some(ca) = self.community_of(src) else { continue };
            let src_key = self.rec(src).url_key();
            let mut it = self.neighbors(src_key);
            while let Some(e) = it.next() {
                let Some(dst) = e.dst else { continue };
                let Some(cb) = self.community_of(dst) else { continue };
                if ca == cb {
                    continue;
                }
                let pair = (ca.min(cb), ca.max(cb));
                if !seen.insert(pair) {
                    continue;
                }
                let conf = if e.is_inferred() { CONF_INFERRED } else { 1.0 };
                ranked.push((f32::from(e.weight) / 65_535.0 * conf, src_key.0, e.dst_key.0));
            }
        }
        ranked.sort_by(|a, b| {
            b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal).then(a.1.cmp(&b.1))
        });
        let mut out = Vec::new();
        for (score, a, b) in ranked.into_iter().take(k) {
            let (Some(na), Some(nb)) = (self.node(UrlKey(a)), self.node(UrlKey(b))) else {
                continue;
            };
            let hit = |n: crate::snapshot::NodeRef<'s>| Hit {
                key: n.key,
                url: n.url,
                title: n.title,
                snippet: n.snippet,
                anchor: None,
                score,
                seed: false,
            };
            out.push((hit(na), hit(nb)));
        }
        out
    }
}

/// A community overview.
#[derive(Clone, Debug)]
pub struct CommunitySummary<'s> {
    /// Stable community id (smallest member node id).
    pub id: u32,
    /// Hub-derived display name (title of the highest-ranked member).
    pub name: &'s str,
    /// Member count.
    pub size: usize,
    /// Top member URLs by rank.
    pub top_urls: Vec<&'s str>,
}

fn estimate_tokens(
    url: &str,
    title: Option<&str>,
    snippet: Option<&str>,
    anchor: Option<&Anchor<'_>>,
) -> u32 {
    let bytes = url.len()
        + title.map_or(0, str::len)
        + snippet.map_or(0, str::len)
        + anchor.map_or(0, |a| a.label.len() + 12)
        + 16; // structural overhead per rendered entry
    (bytes as u32).div_ceil(4)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenizer_lowercases_and_splits() {
        let mut toks = Vec::new();
        for_each_token("Install FooBar v2.1 (Löwe)", |t| toks.push(t.to_owned()));
        assert_eq!(toks, ["install", "foobar", "v2", "1", "löwe"]);
    }

    #[test]
    fn dedup_terms_keeps_order_and_drops_singles() {
        let t = dedup_terms("the the auth Auth a flow");
        assert_eq!(t, ["the", "auth", "flow"]);
    }

    #[test]
    fn token_estimate_counts_all_parts() {
        let a = Anchor { label: "Install", byte_range: None, importance: 1 };
        let with = estimate_tokens("https://x/y", Some("T"), Some("Snip"), Some(&a));
        let without = estimate_tokens("https://x/y", Some("T"), Some("Snip"), None);
        assert!(with > without);
    }
}
