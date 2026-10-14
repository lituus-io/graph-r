// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! Importance, computed at compaction and persisted in the `Ranks` section:
//! `PageRank` over crawl `Link` edges (damping 0.85, fixed 20 iterations, f64
//! accumulation in ascending node order — bit-stable on a platform), reported
//! as a percentile in permille. Degree < 2 nodes are floored to 0 so leaf
//! noise never tops a ranking.

/// Per-node outbound edges: `(dst, etype_tag, flags, weight)`.
pub(crate) type EdgeRow = (u32, u8, u8, u16);

const DAMPING: f64 = 0.85;
const ITERATIONS: usize = 20;
/// Rank percentile (permille) for each of `n` nodes.
pub(crate) fn compute(n: usize, out_edges: &[Vec<EdgeRow>]) -> Vec<u16> {
    debug_assert_eq!(out_edges.len(), n);
    if n == 0 {
        return Vec::new();
    }
    let rank = pagerank(n, out_edges);
    percentile_permille(n, out_edges, &rank)
}

fn pagerank(n: usize, out_edges: &[Vec<EdgeRow>]) -> Vec<f64> {
    let link_out: Vec<usize> =
        out_edges.iter().map(|es| es.iter().filter(|(_, t, _, _)| *t == 0).count()).collect();
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

#[cfg(test)]
mod tests {
    use super::*;

    fn link(dst: u32) -> EdgeRow {
        (dst, 0, 0, 65_535)
    }

    #[test]
    fn pagerank_prefers_the_pointed_at() {
        // Everyone links to 0; 0 links to 1.
        let edges = vec![vec![link(1)], vec![link(0)], vec![link(0)], vec![link(0)]];
        let permille = compute(4, &edges);
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
        let permille = compute(3, &edges);
        assert_eq!(permille[2], 0);
    }
}
