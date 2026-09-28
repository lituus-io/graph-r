// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>
//
//! The knowledge base at 1 000 pages on the in-memory backend: replica
//! hydration, fused lookups, the unchanged refresh check, and an incremental
//! crawl + publish (one page changed, 304s for the rest).

#[path = "../tests/support.rs"]
mod support;

use criterion::{Criterion, criterion_group, criterion_main};
use graph_r_kb::{KnowledgeBase, LookupOpts, WriterOptions};
use link_r::facade::{DefaultEmbedder, default_embedder};
use objstore_r::Mem;
use support::{ROOT, Site};

const N: usize = 1000;

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
}

fn bench(c: &mut Criterion) {
    let rt = rt();
    let site = Site::with_pages(N);
    let mem = Mem::default();
    let dir = tempfile::tempdir().unwrap();
    let mut kb = rt.block_on(async {
        let mut kb = KnowledgeBase::<_, DefaultEmbedder>::writer(
            &mem,
            "kb",
            dir.path(),
            default_embedder().unwrap(),
            WriterOptions::new("bench"),
        )
        .await
        .unwrap();
        let r = kb.sync_crawl(&site, ROOT, |c| c.depth(64).max_pages(N + 1)).await.unwrap();
        assert!(r.added >= N, "{r:?}");
        kb
    });

    let mut g = c.benchmark_group("kb_1000_pages");
    g.bench_function("replica_hydration", |b| {
        b.iter(|| {
            rt.block_on(KnowledgeBase::<_, DefaultEmbedder>::replica(
                &mem,
                "kb",
                default_embedder().unwrap(),
            ))
            .unwrap()
            .unwrap()
        });
    });
    let mut replica = rt
        .block_on(KnowledgeBase::<_, DefaultEmbedder>::replica(
            &mem,
            "kb",
            default_embedder().unwrap(),
        ))
        .unwrap()
        .unwrap();
    let opts = LookupOpts::default();
    g.bench_function("fused_lookup", |b| {
        b.iter(|| rt.block_on(replica.lookup("configure webhooks with tokens", &opts)).unwrap());
    });
    g.bench_function("graph_only_query", |b| {
        b.iter(|| {
            replica
                .graph()
                .snapshot()
                .query("webhooks tokens", &graph_r::QueryOpts::default())
                .len()
        });
    });
    g.bench_function("replica_refresh_unchanged", |b| {
        b.iter(|| rt.block_on(replica.refresh()).unwrap())
    });
    let mut edit = 0usize;
    g.sample_size(10);
    g.bench_function("incremental_crawl_publish", |b| {
        b.iter(|| {
            edit += 1;
            site.edit(&format!("p{}", edit % N), "edited", &format!("Edited page number {edit}."));
            rt.block_on(kb.sync_crawl(&site, ROOT, |c| c.depth(64).max_pages(N + 1))).unwrap()
        });
    });
    g.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
