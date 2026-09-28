// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>
//
//! The mirror protocol against Google Cloud Storage's JSON API, via
//! fake-gcs-server (`OBJSTORE_FAKE_GCS=http://127.0.0.1:4443`). Skipped when
//! the variable is unset. Never touches a real bucket.

#![cfg(feature = "gcs")]

#[path = "../../tests/support/golden.rs"]
mod golden;

use graph_r::prelude::*;
use graph_r_mirror::{Error, Mirror};
use objstore_r::gcs::{Gcs, GcsConfig};
use objstore_r::token::Static;

async fn bucket(endpoint: &str) -> Gcs<Static> {
    let name = format!("mirror-{}", std::process::id());
    let _ = reqwest_create(endpoint, &name).await;
    let cfg = GcsConfig {
        endpoint: endpoint.to_owned(),
        resumable_threshold: 2048,
        ..GcsConfig::default()
    };
    Gcs::new(&name, Static::none(), cfg).unwrap()
}

/// Create the bucket through the emulator's JSON API (plain TCP; the crate
/// keeps its HTTP client private).
async fn reqwest_create(endpoint: &str, name: &str) -> std::io::Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let hostport = endpoint.trim_start_matches("http://");
    let mut s = tokio::net::TcpStream::connect(hostport).await?;
    let body = format!("{{\"name\":\"{name}\"}}");
    let req = format!(
        "POST /storage/v1/b?project=test HTTP/1.1\r\nhost: {hostport}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    s.write_all(req.as_bytes()).await?;
    let mut sink = Vec::new();
    s.read_to_end(&mut sink).await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn mirror_round_trip_on_the_emulator() {
    let Ok(endpoint) = std::env::var("OBJSTORE_FAKE_GCS") else {
        eprintln!("OBJSTORE_FAKE_GCS unset: skipped");
        return;
    };
    let objects = bucket(&endpoint).await;
    let prefix = format!("t{}", graph_r::store::now_ms());
    let wdir = tempfile::tempdir().unwrap();
    let store = Store::create(wdir.path(), Config::default()).unwrap();
    golden::seed(&store);
    let w = Mirror::new(&objects, &prefix).unwrap();
    w.acquire_lease("writer-1", 60_000).await.unwrap();
    w.publish(&store).await.unwrap();

    let rdir = tempfile::tempdir().unwrap();
    let r = Mirror::new(&objects, &prefix).unwrap();
    r.pull_into(rdir.path()).await.unwrap().unwrap();
    let replica = Store::open_read_only(rdir.path()).unwrap();
    assert_eq!(golden::dump(&replica), golden::dump(&store));

    golden::churn(&store);
    w.publish(&store).await.unwrap();
    assert!(r.refresh_into(&replica).await.unwrap());
    store.compact().unwrap();
    w.publish(&store).await.unwrap();
    assert!(r.refresh_into(&replica).await.unwrap());
    assert!(!r.refresh_into(&replica).await.unwrap());
    assert_eq!(golden::dump(&replica), golden::dump(&store));

    assert!(matches!(r.acquire_lease("writer-2", 60_000).await, Err(Error::Leased { .. })));
    w.release_lease().await.unwrap();
    // Clean the prefix up.
    use objstore_r::ObjectStore;
    for m in objects.list(&format!("{prefix}/")).await.unwrap() {
        objects.delete(&m.key).await.unwrap();
    }
}
