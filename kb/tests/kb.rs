// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>
//
//! The knowledge base end to end: conditional crawls through the bridge,
//! fenced publishing, zero-copy replicas, fused lookups, due-list refresh,
//! restarts and custom embedders — on memory and local-directory backends.

mod support;

use graph_r_kb::{Error, KnowledgeBase, LookupOpts, WriterOptions};
use link_r::HashEmbedder;
use link_r::facade::default_embedder;
use objstore_r::{LocalDir, Mem, ObjectStore};
use std::sync::atomic::Ordering;
use support::{ROOT, Site};

type Kb<'o, O> = KnowledgeBase<'o, O, link_r::facade::DefaultEmbedder>;

async fn writer<'o, O: ObjectStore>(
    objects: &'o O,
    dir: &std::path::Path,
    holder: &str,
) -> Kb<'o, O> {
    KnowledgeBase::writer(
        objects,
        "kb/docs",
        dir,
        default_embedder().unwrap(),
        WriterOptions::new(holder),
    )
    .await
    .unwrap()
}

async fn urls<'o, O: ObjectStore>(kb: &Kb<'o, O>, q: &str) -> Vec<String> {
    kb.lookup(q, &LookupOpts::default()).await.unwrap().into_iter().map(|h| h.url).collect()
}

#[tokio::test(flavor = "current_thread")]
async fn crawl_publish_replicate_and_look_up() {
    let site = Site::new();
    let mem = Mem::default();
    let dir = tempfile::tempdir().unwrap();
    let mut kb = writer(&mem, dir.path(), "w1").await;
    let r = kb.sync_crawl(&site, ROOT, |c| c.depth(3)).await.unwrap();
    assert_eq!(r.added, 4, "{r:?}");
    assert!(r.graph_docs >= 4 && r.uploaded > 0, "{r:?}");
    assert_eq!(kb.graph().len(), 4);

    let hits = kb.lookup("install linux", &LookupOpts::default()).await.unwrap();
    assert_eq!(hits[0].url, format!("{ROOT}install"));
    assert!(
        hits[0].graph_rank.is_some() && hits[0].link_rank.is_some(),
        "both rankings agree: {:?}",
        hits[0]
    );
    assert!(hits.iter().any(|h| h.anchor.is_some()), "graph anchors come through");

    // A replica needs no disk and answers identically.
    let replica = Kb::replica(&mem, "kb/docs", default_embedder().unwrap()).await.unwrap().unwrap();
    assert!(replica.is_replica() && replica.graph().is_image());
    for q in ["install linux", "api authentication tokens", "licensing questions", "widget"] {
        assert_eq!(urls(&replica, q).await, urls(&kb, q).await, "{q}");
    }
    assert!(!replica.related(&format!("{ROOT}install"), 3).unwrap().is_empty());
    assert!(
        Kb::replica(&mem, "kb/elsewhere", default_embedder().unwrap()).await.unwrap().is_none()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn recrawls_are_conditional_and_replicas_follow() {
    let site = Site::new();
    let mem = Mem::default();
    let dir = tempfile::tempdir().unwrap();
    let mut kb = writer(&mem, dir.path(), "w1").await;
    kb.sync_crawl(&site, ROOT, |c| c.depth(3)).await.unwrap();
    let mut replica =
        Kb::replica(&mem, "kb/docs", default_embedder().unwrap()).await.unwrap().unwrap();
    assert!(!replica.refresh().await.unwrap(), "nothing new");

    // Nothing changed: every page revalidates with a 304, nothing uploads.
    site.reset_counters();
    let r = kb.sync_crawl(&site, ROOT, |c| c.depth(3)).await.unwrap();
    assert_eq!(site.full.load(Ordering::Relaxed), 0, "no bodies transferred");
    assert_eq!(site.not_modified.load(Ordering::Relaxed), 4);
    assert_eq!((r.added, r.updated, r.unchanged), (0, 0, 4), "{r:?}");

    // One page changes: one body, and the replica sees it.
    site.edit("faq", "FAQ", "Questions about quantum entanglement of widgets.");
    site.reset_counters();
    let r = kb.sync_crawl(&site, ROOT, |c| c.depth(3)).await.unwrap();
    assert_eq!(site.full.load(Ordering::Relaxed), 1);
    assert_eq!(r.updated, 1, "{r:?}");
    assert!(replica.refresh().await.unwrap());
    assert_eq!(urls(&replica, "quantum entanglement").await[0], format!("{ROOT}faq"));
    assert_eq!(
        urls(&replica, "quantum entanglement").await,
        urls(&kb, "quantum entanglement").await
    );
}

#[tokio::test(flavor = "current_thread")]
async fn due_pages_are_revalidated_through_the_graph() {
    let site = Site::new();
    let mem = Mem::default();
    let dir = tempfile::tempdir().unwrap();
    let mut kb = writer(&mem, dir.path(), "w1").await;
    kb.sync_crawl(&site, ROOT, |c| c.depth(3)).await.unwrap();
    site.edit("api", "API reference", "Webhooks and rate limits of the widget api.");
    site.reset_counters();
    let far_future = graph_r::store::now_ms() + 365 * 24 * 3600 * 1000;
    let r = kb.refresh_due(&&site, far_future, 100).await.unwrap();
    assert_eq!(site.not_modified.load(Ordering::Relaxed), 3, "{r:?}");
    assert_eq!(site.full.load(Ordering::Relaxed), 1);
    assert_eq!((r.updated, r.unchanged), (1, 3), "{r:?}");
    assert_eq!(urls(&kb, "webhooks rate limits").await[0], format!("{ROOT}api"));
    // Nothing due right now.
    let r = kb.refresh_due(&&site, graph_r::store::now_ms(), 100).await.unwrap();
    assert_eq!(r, graph_r_kb::SyncReport::default());
}

#[tokio::test(flavor = "current_thread")]
async fn one_writer_at_a_time_and_restarts_resume() {
    let site = Site::new();
    let mem = Mem::default();
    let (d1, d2) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let mut kb = writer(&mem, d1.path(), "w1").await;
    kb.sync_crawl(&site, ROOT, |c| c.depth(3)).await.unwrap();
    let second = KnowledgeBase::writer(
        &mem,
        "kb/docs",
        d2.path(),
        default_embedder().unwrap(),
        WriterOptions::new("w2"),
    )
    .await;
    assert!(matches!(second, Err(Error::Mirror(graph_r_mirror::Error::Leased { .. }))));
    let before = urls(&kb, "install linux").await;
    kb.release().await.unwrap();
    drop(kb);

    // A new instance (fresh tmpfs) resumes exactly where the old one was.
    let mut kb = writer(&mem, d2.path(), "w2").await;
    assert_eq!(kb.graph().len(), 4);
    assert_eq!(urls(&kb, "install linux").await, before);
    site.reset_counters();
    kb.sync_crawl(&site, ROOT, |c| c.depth(3)).await.unwrap();
    assert_eq!(site.full.load(Ordering::Relaxed), 0, "validators survived the restart");
    let mut replica =
        Kb::replica(&mem, "kb/docs", default_embedder().unwrap()).await.unwrap().unwrap();
    assert!(
        replica.sync_crawl(&site, ROOT, |c| c).await.is_err_and(|e| matches!(e, Error::ReadOnly))
    );
    assert!(!replica.refresh().await.unwrap());
}

#[tokio::test(flavor = "current_thread")]
async fn local_directory_backend_and_a_custom_embedder() {
    let site = Site::new();
    let remote = tempfile::tempdir().unwrap();
    let objects = LocalDir::open(remote.path()).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let mut kb = KnowledgeBase::writer(
        &objects,
        "kb",
        dir.path(),
        HashEmbedder::new(64),
        WriterOptions::new("w"),
    )
    .await
    .unwrap();
    kb.sync_crawl(&site, ROOT, |c| c.depth(3)).await.unwrap();
    let replica =
        KnowledgeBase::replica(&objects, "kb", HashEmbedder::new(64)).await.unwrap().unwrap();
    assert_eq!(
        replica.lookup("install linux", &LookupOpts::default()).await.unwrap()[0].url,
        format!("{ROOT}install")
    );
    // The index is refused with a different embedder.
    assert!(KnowledgeBase::replica(&objects, "kb", default_embedder().unwrap()).await.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn a_failed_publish_is_retried_by_the_next_one() {
    let site = Site::new();
    let mem = Mem::default();
    let dir = tempfile::tempdir().unwrap();
    let mut kb = writer(&mem, dir.path(), "w1").await;
    kb.sync_crawl(&site, ROOT, |c| c.depth(3)).await.unwrap();
    site.edit("install", "Install", "Install on freebsd from ports.");
    for step in 0..6 {
        let e = objstore_r::Error::Unavailable { message: "injected".into(), retriable: true };
        mem.inject_after(step, objstore_r::mem::Fault::LostResponse(e));
        let _ = kb.sync_crawl(&site, ROOT, |c| c.depth(3)).await;
        mem.clear_faults();
        kb.publish().await.unwrap_or_else(|e| panic!("step {step}: {e}"));
    }
    let replica = Kb::replica(&mem, "kb/docs", default_embedder().unwrap()).await.unwrap().unwrap();
    assert_eq!(urls(&replica, "freebsd ports").await[0], format!("{ROOT}install"));
    assert_eq!(urls(&replica, "freebsd ports").await, urls(&kb, "freebsd ports").await);
}
