// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>
//
//! Deterministic structure metrics, computed at compaction and persisted in
//! the `Ranks` section:
//!
//! - **Importance**: `PageRank` over crawl `Link` edges (damping 0.85, fixed 20
//!   iterations, f64 accumulation in ascending node order — bit-stable on a
//!   platform), reported as a percentile in permille. Degree < 2 nodes are
//!   floored to 0 so leaf noise never tops a ranking.
//! - **Communities**: weighted label propagation with a total order on every
//!   tie (smallest label wins), hub-muted for the first sweeps so navigation
//!   pages don't glue unrelated clusters together. A community's id is its
//!   smallest member node id — stable across rebuilds of the same graph.

/// Per-node outbound edges: `(dst, etype_tag, flags, weight)`.
pub(crate) type EdgeRow = (u32, u8, u8, u16);

const DAMPING: f64 = 0.85;
const ITERATIONS: usize = 20;
const LPA_SWEEPS: usize = 10;
const HUB_MUTE_SWEEPS: usize = 3;
const HUB_PERCENTILE: usize = 95;

/// Compute `(rank_permille, community)` for `n` nodes.
pub(crate) fn compute(n: usize, out_edges: &[Vec<EdgeRow>]) -> (Vec<u16>, Vec<u32>) {
    debug_assert_eq!(out_edges.len(), n);
    if n == 0 {
        return (Vec::new(), Vec::new());
    }
    let rank = pagerank(n, out_edges);
    let permille = percentile_permille(n, out_edges, &rank);
    let community = label_propagation(n, out_edges);
    (permille, community)
}

fn pagerank(n: usize, out_edges: &[Vec<EdgeRow>]) -> Vec<f64> {
    let link_out: Vec<usize> = out_edges
        .iter()
        .map(|es| es.iter().filter(|(_, t, _, _)| *t == 0).count())
        .collect();
    let teleport = (1.0 - DAMPING) / n as f64;
    let mut rank = vec![1.0 / n as f64; n];
    let mut next = vec![0.0f64; n];
    for _ in 0..ITERATIONS {
        next.fill(0.0);
        let mut dangling = 0.0f64;
        for (u, es) in out_edges.iter().enumerate() {
            if link_out[u] == 0 {
                dangling += rank[u];
                continue;
            }
            let share = rank[u] / link_out[u] as f64;
            for &(dst, etype, _, _) in es {
                if etype == 0 {
                    next[dst as usize] += share;
                }
            }
        }
        let dangling_share = dangling / n as f64;
        for v in &mut next {
            *v = teleport + DAMPING * (*v + dangling_share);
        }
        std::mem::swap(&mut rank, &mut next);
    }
    rank
}

fn percentile_permille(n: usize, out_edges: &[Vec<EdgeRow>], rank: &[f64]) -> Vec<u16> {
    let mut in_degree = vec![0u32; n];
    for es in out_edges {
        for &(dst, _, _, _) in es {
            in_degree[dst as usize] += 1;
        }
    }
    // Sort ids by (rank, id) ascending; percentile = position / (n-1).
    let mut order: Vec<u32> = (0..n as u32).collect();
    order.sort_by(|&a, &b| {
        rank[a as usize]
            .partial_cmp(&rank[b as usize])
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.cmp(&b))
    });
    let mut permille = vec![0u16; n];
    for (pos, &id) in order.iter().enumerate() {
        let p = if n == 1 { 1000 } else { (pos * 1000 / (n - 1)) as u16 };
        let degree = in_degree[id as usize] + out_edges[id as usize].len() as u32;
        permille[id as usize] = if degree < 2 { 0 } else { p };
    }
    permille
}

fn label_propagation(n: usize, out_edges: &[Vec<EdgeRow>]) -> Vec<u32> {
    // Undirected weighted adjacency; inferred edges count 0.6.
    let mut adj: Vec<Vec<(u32, f64)>> = vec![Vec::new(); n];
    for (u, es) in out_edges.iter().enumerate() {
        for &(dst, _, flags, weight) in es {
            let conf = if flags & crate::format::base::eflags::INFERRED != 0 { 0.6 } else { 1.0 };
            let w = f64::from(weight.max(1)) / 65_535.0 * conf;
            adj[u].push((dst, w));
            adj[dst as usize].push((u as u32, w));
        }
    }
    let degrees: Vec<usize> = adj.iter().map(Vec::len).collect();
    let mut sorted = degrees.clone();
    sorted.sort_unstable();
    let hub_cut = sorted[(n * HUB_PERCENTILE / 100).min(n - 1)].max(8);

    let mut label: Vec<u32> = (0..n as u32).collect();
    let mut votes: std::collections::HashMap<u32, f64> = std::collections::HashMap::new();
    for sweep in 0..LPA_SWEEPS {
        let mut changed = false;
        for u in 0..n {
            if adj[u].is_empty() {
                continue;
            }
            votes.clear();
            for &(v, w) in &adj[u] {
                // Hubs stay silent for the first sweeps so they cannot glue
                // unrelated clusters; afterwards they vote like anyone.
                if sweep < HUB_MUTE_SWEEPS && adj[v as usize].len() >= hub_cut {
                    continue;
                }
                *votes.entry(label[v as usize]).or_insert(0.0) += w;
            }
            let Some(new) = votes
                .iter()
                .map(|(&l, &w)| (l, w))
                .max_by(|a, b| {
                    a.1.partial_cmp(&b.1)
                        .unwrap_or(std::cmp::Ordering::Equal)
                        .then(b.0.cmp(&a.0)) // smaller label wins ties
                })
                .map(|(l, _)| l)
            else {
                continue;
            };
            if new != label[u] {
                label[u] = new;
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }
    // Canonicalize: a community's id is its smallest member node id.
    let mut min_member: std::collections::HashMap<u32, u32> = std::collections::HashMap::new();
    for (u, &l) in label.iter().enumerate() {
        min_member.entry(l).and_modify(|m| *m = (*m).min(u as u32)).or_insert(u as u32);
    }
    label.iter().map(|l| min_member[l]).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn link(dst: u32) -> EdgeRow {
        (dst, 0, 0, 65_535)
    }

    #[test]
    fn two_cliques_split_into_two_communities() {
        // 0-1-2 fully linked; 3-4-5 fully linked; one weak bridge 2->3.
        let edges = vec![
            vec![link(1), link(2)],
            vec![link(0), link(2)],
            vec![link(0), link(1), (3, 0, crate::format::base::eflags::INFERRED, 100)],
            vec![link(4), link(5)],
            vec![link(3), link(5)],
            vec![link(3), link(4)],
        ];
        let (permille, comm) = compute(6, &edges);
        assert_eq!(comm[0], comm[1]);
        assert_eq!(comm[1], comm[2]);
        assert_eq!(comm[3], comm[4]);
        assert_eq!(comm[4], comm[5]);
        assert_ne!(comm[0], comm[3]);
        assert_eq!(comm[0], 0, "community id is smallest member");
        assert_eq!(comm[3], 3);
        assert_eq!(permille.len(), 6);
    }

    #[test]
    fn pagerank_prefers_the_pointed_at() {
        // Everyone links to 0; 0 links to 1.
        let edges = vec![vec![link(1)], vec![link(0)], vec![link(0)], vec![link(0)]];
        let (permille, _) = compute(4, &edges);
        assert!(permille[0] > permille[2]);
        assert!(permille[1] > permille[2], "0 passes rank to 1");
    }

    #[test]
    fn deterministic_across_runs() {
        let edges: Vec<Vec<EdgeRow>> =
            (0..50).map(|i| vec![link((i + 1) % 50), link((i * 7 + 3) % 50)]).collect();
        let a = compute(50, &edges);
        let b = compute(50, &edges);
        assert_eq!(a, b);
    }

    #[test]
    fn leaf_noise_floored_to_zero() {
        // Node 2 has a single edge (degree 1) — floored.
        let edges = vec![vec![link(1)], vec![link(0)], vec![link(0)]];
        let (permille, _) = compute(3, &edges);
        assert_eq!(permille[2], 0);
    }
}
