// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>
//
//! Criterion benches for the store's hot paths, including the load-bearing
//! claim: reader latency stays flat while a writer commits and compacts.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use graph_r::prelude::*;
use std::hint::black_box;

fn url_of(n: usize) -> String {
    format!("https://bench.dev/section/{}/page/{n}", n % 37)
}

/// Build a store with `n` interlinked documents, compacted.
fn build_store(dir: &std::path::Path, n: usize) -> Store {
    let store = Store::create(dir, Config::default()).unwrap();
    let mut w = store.writer().unwrap();
    for i in 0..n {
        let url = url_of(i);
        w.upsert_node(&DocRecord {
            url: &url,
            url_key: UrlKey::of(&url),
            content_hash: i as u64,
            fetched_at_ms: 1000,
            title: Some("Benchmark Page Title"),
            snippet: Some("a snippet of distilled content for ranking and rendering"),
            etag: Some("\"tag\""),
            pinned: false,
        })
        .unwrap();
        w.set_segments(
            UrlKey::of(&url),
            &[
                SegmentRecord {
                    label: "Overview",
                    byte_range: Some((0, 512)),
                    depth: 1,
                    importance: 60_000,
                },
                SegmentRecord {
                    label: "Details and configuration",
                    byte_range: Some((512, 2048)),
                    depth: 2,
                    importance: 40_000,
                },
            ],
        )
        .unwrap();
        // Ring + skip links give the graph structure without hubs.
        let e1 = url_of((i + 1) % n.max(1));
        let e2 = url_of((i * 7 + 3) % n.max(1));
        w.set_edges(
            UrlKey::of(&url),
            &[(UrlKey::of(&e1), EdgeType::Link, 65_535), (UrlKey::of(&e2), EdgeType::Link, 40_000)],
        );
        if i % 512 == 511 {
            w.commit().unwrap();
        }
    }
    w.commit().unwrap();
    drop(w);
    store.compact().unwrap();
    store
}

fn bench_reads(c: &mut Criterion) {
    let mut group = c.benchmark_group("read");
    for &n in &[1_000usize, 10_000] {
        let dir = tempfile::tempdir().unwrap();
        let store = build_store(dir.path(), n);
        let snap = store.snapshot();
        let key = UrlKey::of(&url_of(n / 2));
        group.bench_with_input(BenchmarkId::new("point_lookup", n), &n, |b, _| {
            b.iter(|| black_box(snap.node(black_box(key))));
        });
        group.bench_with_input(BenchmarkId::new("neighbors_walk", n), &n, |b, _| {
            b.iter(|| {
                let mut it = snap.neighbors(key);
                let mut total = 0u32;
                while let Some(e) = graph_r::traverse::LendingIterator::next(&mut it) {
                    total += u32::from(e.weight);
                }
                black_box(total)
            });
        });
        group.bench_with_input(BenchmarkId::new("query", n), &n, |b, _| {
            b.iter(|| {
                black_box(snap.query("benchmark configuration details", &QueryOpts::default()))
            });
        });
        group.bench_with_input(BenchmarkId::new("due_scan", n), &n, |b, _| {
            b.iter(|| black_box(snap.due(u64::MAX / 2, 64)));
        });
    }
    group.finish();
}

fn bench_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("write");
    group.bench_function("commit_100_upserts_fsync_never", |b| {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::create(
            dir.path(),
            Config {
                durability: Durability::Never,
                compact_after_ops: usize::MAX,
                compact_after_wal_bytes: u64::MAX,
                ..Config::default()
            },
        )
        .unwrap();
        let mut round = 0usize;
        b.iter(|| {
            let mut w = store.writer().unwrap();
            for i in 0..100 {
                let url = url_of(round * 100 + i);
                w.upsert_node(&DocRecord {
                    url: &url,
                    url_key: UrlKey::of(&url),
                    content_hash: i as u64,
                    fetched_at_ms: 1,
                    title: Some("t"),
                    snippet: None,
                    etag: None,
                    pinned: false,
                })
                .unwrap();
            }
            round += 1;
            black_box(w.commit().unwrap())
        });
    });
    group.bench_function("compact_10k_nodes", |b| {
        let dir = tempfile::tempdir().unwrap();
        let store = build_store(dir.path(), 10_000);
        b.iter(|| {
            // One touch so each compaction has work, then fold.
            let mut w = store.writer().unwrap();
            w.touch(
                UrlKey::of(&url_of(1)),
                Touch {
                    checked_at_ms: 2000,
                    outcome: Outcome::Unchanged,
                    content_hash: None,
                    etag: None,
                },
            )
            .unwrap();
            w.commit().unwrap();
            drop(w);
            black_box(store.compact().unwrap())
        });
    });
    group.finish();
}

/// The concurrency claim, measured: reader p50 while a writer churns.
fn bench_read_under_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_under_write");
    let dir = tempfile::tempdir().unwrap();
    let store = build_store(dir.path(), 10_000);
    let key = UrlKey::of(&url_of(5_000));

    // Baseline: idle writer — same shape as the contended case (fresh
    // snapshot per read) so the comparison isolates writer interference.
    group.bench_function("lookup_idle_writer", |b| {
        b.iter(|| {
            let snap = store.snapshot();
            black_box(snap.node(black_box(key)).map(|n| n.content_hash))
        });
    });

    // Contended: a writer thread commits continuously (fsync off so the
    // contention is structural, not disk-bound).
    let stop = std::sync::atomic::AtomicBool::new(false);
    std::thread::scope(|scope| {
        scope.spawn(|| {
            let mut round = 0u64;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let mut w = store.writer().unwrap();
                let url = url_of((round % 10_000) as usize);
                let _ = w.touch(
                    UrlKey::of(&url),
                    Touch {
                        checked_at_ms: round + 10,
                        outcome: Outcome::Unchanged,
                        content_hash: None,
                        etag: None,
                    },
                );
                let _ = w.commit();
                round += 1;
            }
        });
        group.bench_function("lookup_under_write", |b| {
            b.iter(|| {
                let snap = store.snapshot();
                black_box(snap.node(black_box(key)).map(|n| n.content_hash))
            });
        });
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
    });
    group.finish();
}

criterion_group!(benches, bench_reads, bench_writes, bench_read_under_write);
criterion_main!(benches);
