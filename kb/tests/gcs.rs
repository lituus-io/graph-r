// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>
//
//! The knowledge base on Google Cloud Storage's JSON API:
//! - `OBJSTORE_FAKE_GCS=http://127.0.0.1:4443` — fake-gcs-server;
//! - `OBJSTORE_LIVE_BUCKET=<bucket>` — a real bucket via Application Default
//!   Credentials, under a unique `objstore-r-live/kb-…` prefix that is deleted
//!   afterwards (nothing else in the bucket is touched).
//!
//! Each test is skipped when its variable is unset.

#![cfg(feature = "gcs")]

mod support;

use graph_r_kb::{KnowledgeBase, LookupOpts, WriterOptions};
use link_r::facade::{DefaultEmbedder, default_embedder};
use objstore_r::ObjectStore;
use objstore_r::gcs::{Gcs, GcsConfig};
use objstore_r::token::{Adc, Static, TokenProvider};
use std::time::Instant;
use support::{ROOT, Site};

async fn exercise<T: TokenProvider>(gcs: &Gcs<T>, prefix: &str) {
    let site = Site::new();
    let dir = tempfile::tempdir().unwrap();
    let t = Instant::now();
    let mut kb = KnowledgeBase::<_, DefaultEmbedder>::writer(
        gcs,
        prefix,
        dir.path(),
        default_embedder().unwrap(),
        WriterOptions::new("live-writer"),
    )
    .await
    .unwrap();
    eprintln!("writer open (empty): {:?}", t.elapsed());
    let t = Instant::now();
    let r = kb.sync_crawl(&site, ROOT, |c| c.depth(3)).await.unwrap();
    eprintln!("crawl + bridge + publish: {:?} ({r:?})", t.elapsed());
    assert_eq!(r.added, 4);

    let t = Instant::now();
    let mut replica =
        KnowledgeBase::<_, DefaultEmbedder>::replica(gcs, prefix, default_embedder().unwrap())
            .await
            .unwrap()
            .unwrap();
    eprintln!("replica hydration (graph image + link image, concurrent): {:?}", t.elapsed());
    let t = Instant::now();
    let hits = replica.lookup("install linux", &LookupOpts::default()).await.unwrap();
    eprintln!("fused lookup: {:?}", t.elapsed());
    assert_eq!(hits[0].url, format!("{ROOT}install"));

    let t = Instant::now();
    assert!(!replica.refresh().await.unwrap());
    eprintln!("replica refresh, unchanged: {:?}", t.elapsed());

    site.edit("faq", "FAQ", "Questions about quantum entanglement of widgets.");
    let t = Instant::now();
    let r = kb.sync_crawl(&site, ROOT, |c| c.depth(3)).await.unwrap();
    eprintln!("incremental crawl + publish: {:?} ({r:?})", t.elapsed());
    assert_eq!(r.updated, 1);
    let t = Instant::now();
    assert!(replica.refresh().await.unwrap());
    eprintln!("replica refresh, changed: {:?}", t.elapsed());
    let hits = replica.lookup("quantum entanglement", &LookupOpts::default()).await.unwrap();
    assert_eq!(hits[0].url, format!("{ROOT}faq"));

    // A restarted writer resumes from the bucket.
    kb.release().await.unwrap();
    drop(kb);
    let dir2 = tempfile::tempdir().unwrap();
    let t = Instant::now();
    let kb = KnowledgeBase::<_, DefaultEmbedder>::writer(
        gcs,
        prefix,
        dir2.path(),
        default_embedder().unwrap(),
        WriterOptions::new("live-writer-2"),
    )
    .await
    .unwrap();
    eprintln!("writer restart (pull graph + load links): {:?}", t.elapsed());
    assert_eq!(kb.graph().len(), 4);
    kb.release().await.unwrap();

    for m in gcs.list(&format!("{prefix}/")).await.unwrap() {
        gcs.delete(&m.key).await.unwrap();
    }
    assert!(gcs.list(&format!("{prefix}/")).await.unwrap().is_empty(), "prefix cleaned up");
}

#[tokio::test(flavor = "current_thread")]
async fn knowledge_base_on_the_emulator() {
    let Ok(endpoint) = std::env::var("OBJSTORE_FAKE_GCS") else {
        eprintln!("OBJSTORE_FAKE_GCS unset: skipped");
        return;
    };
    let bucket = format!("kb-{}", std::process::id());
    create_bucket(&endpoint, &bucket).await;
    let cfg = GcsConfig { endpoint, resumable_threshold: 4096, ..GcsConfig::default() };
    let gcs = Gcs::new(&bucket, Static::none(), cfg).unwrap();
    exercise(&gcs, "kb/docs").await;
}

#[tokio::test(flavor = "current_thread")]
async fn knowledge_base_on_a_live_bucket() {
    let Ok(bucket) = std::env::var("OBJSTORE_LIVE_BUCKET") else {
        eprintln!("OBJSTORE_LIVE_BUCKET unset: skipped");
        return;
    };
    let gcs = Gcs::new(&bucket, Adc::discover().unwrap(), GcsConfig::default()).unwrap();
    let prefix = format!("objstore-r-live/kb-{}-{}", std::process::id(), graph_r::store::now_ms());
    exercise(&gcs, &prefix).await;
}

async fn create_bucket(endpoint: &str, name: &str) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let hostport = endpoint.trim_start_matches("http://");
    let mut s = tokio::net::TcpStream::connect(hostport).await.unwrap();
    let body = format!("{{\"name\":\"{name}\"}}");
    let req = format!(
        "POST /storage/v1/b?project=test HTTP/1.1\r\nhost: {hostport}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    s.write_all(req.as_bytes()).await.unwrap();
    let mut sink = Vec::new();
    let _ = s.read_to_end(&mut sink).await;
}
