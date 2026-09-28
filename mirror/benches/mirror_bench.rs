// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>
//
//! Publish latency per commit batch, pull-to-open time, and the no-change
//! refresh check, against the in-memory and local-directory backends.

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use futures::executor::block_on;
use graph_r::prelude::*;
use graph_r_mirror::Mirror;
use objstore_r::{LocalDir, Mem};

fn commit_batch(store: &Store, start: u64, n: u64) {
    let mut w = store.writer().unwrap();
    for i in start..start + n {
        let url = format!("https://bench.dev/{i}");
        w.upsert_node(&DocRecord {
            url: &url,
            url_key: UrlKey::of(&url),
            content_hash: i,
            fetched_at_ms: 1,
            title: Some("A page about benchmarking the mirror"),
            snippet: Some("Snippet text that is representative of a distilled page summary."),
            etag: None,
            pinned: false,
        })
        .unwrap();
    }
    w.commit().unwrap();
}

fn seeded(docs: u64) -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let store =
        Store::create(dir.path(), Config { durability: Durability::Never, ..Config::default() })
            .unwrap();
    store.set_compact_inline(false);
    commit_batch(&store, 0, docs);
    store.compact().unwrap();
    (dir, store)
}

fn publish(c: &mut Criterion) {
    let mut g = c.benchmark_group("publish");
    let (_d, store) = seeded(2000);
    let mem = Mem::default();
    let m = Mirror::new(&mem, "kb").unwrap();
    block_on(m.publish(&store)).unwrap();
    let mut next = 10_000;
    g.bench_function("tail_16_docs_mem", |b| {
        b.iter(|| {
            commit_batch(&store, next, 16);
            next += 16;
            block_on(m.publish(&store)).unwrap()
        });
    });
    g.bench_function("nothing_new_mem", |b| b.iter(|| block_on(m.publish(&store)).unwrap()));
    g.finish();
}

fn pull(c: &mut Criterion) {
    let mut g = c.benchmark_group("pull");
    let (_d, store) = seeded(2000);
    commit_batch(&store, 5000, 200);
    let remote_dir = tempfile::tempdir().unwrap();
    let local = LocalDir::open(remote_dir.path()).unwrap();
    block_on(Mirror::new(&local, "kb").unwrap().publish(&store)).unwrap();
    g.bench_function("pull_and_open_2200_docs_localdir", |b| {
        b.iter_batched(
            || tempfile::tempdir().unwrap(),
            |dst| {
                let m = Mirror::new(&local, "kb").unwrap();
                block_on(m.pull_into(dst.path())).unwrap().unwrap();
                let s = Store::open_read_only(dst.path()).unwrap();
                assert_eq!(s.len(), 2200);
                dst
            },
            BatchSize::PerIteration,
        );
    });
    let dst = tempfile::tempdir().unwrap();
    let m = Mirror::new(&local, "kb").unwrap();
    block_on(m.pull_into(dst.path())).unwrap();
    let replica = Store::open_read_only(dst.path()).unwrap();
    g.bench_function("refresh_unchanged_localdir", |b| {
        b.iter(|| block_on(m.refresh_into(&replica)).unwrap())
    });
    g.finish();
}

criterion_group!(benches, publish, pull);
criterion_main!(benches);
