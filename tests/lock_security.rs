// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! The commit lock's files. Their paths are fixed — `<store dir>/graph.lock`
//! and its wait queue `<store dir>/graph.wait` — so nothing a caller writes
//! (a url, a key, an attribute) can steer where either is created, and
//! either planted as a symlink to somewhere else is refused rather than
//! followed.

#![cfg(feature = "store")]
use graph_r::prelude::*;
use std::collections::BTreeSet;
use std::path::Path;

fn names(dir: &Path) -> BTreeSet<String> {
    std::fs::read_dir(dir).unwrap().map(|e| e.unwrap().file_name().into_string().unwrap()).collect()
}

fn doc(url: &str) -> DocRecord<'_> {
    DocRecord {
        url,
        url_key: UrlKey::of(url),
        content_hash: 1,
        fetched_at_ms: 1,
        title: Some("../graph.lock"),
        snippet: None,
        etag: None,
        pinned: false,
    }
}

#[test]
fn no_url_key_or_attribute_steers_the_lock_file() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("s");
    let a = Store::open_or_create(&dir, Config::default()).unwrap();
    let b = Store::open(&dir).unwrap();
    let hostile = [
        "../graph.lock",
        "../../escape",
        "/tmp/graph.lock",
        "graph.lock",
        "..\\..\\graph.lock",
        "s/../../graph.lock",
    ];
    for (i, url) in hostile.into_iter().enumerate() {
        let store = if i % 2 == 0 { &a } else { &b };
        store
            .transact(|tx| {
                tx.upsert_node(&doc(url))?;
                tx.set_named_attrs(UrlKey::of(url), &[("../graph.lock", url)])
            })
            .unwrap();
    }
    a.compact().unwrap();
    assert_eq!(names(root.path()), BTreeSet::from(["s".to_owned()]), "nothing beside the store");
    let want: BTreeSet<String> = ["graph.base", "graph.lock", "graph.wait", "graph.wal"]
        .into_iter()
        .map(str::to_owned)
        .collect();
    assert_eq!(names(&dir), want, "only the store's own files inside it");
    assert_eq!(a.len(), hostile.len());
}

#[cfg(unix)]
#[test]
fn a_lock_file_planted_as_a_symlink_is_refused_not_followed() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("s");
    let store = Store::create(&dir, Config::default()).unwrap();
    let target = root.path().join("elsewhere");
    std::fs::remove_file(dir.join(graph_r::store::LOCK_FILE)).unwrap();
    std::os::unix::fs::symlink(&target, dir.join(graph_r::store::LOCK_FILE)).unwrap();
    let r = store.transact(|tx| tx.upsert_node(&doc("https://x.dev/a")));
    assert!(matches!(r, Err(Error::Corrupt { .. })), "{r:?}");
    assert!(!target.exists(), "the lock was created through the symlink");
    let opened = Store::open_or_create(root.path().join("t"), Config::default()).unwrap();
    drop(opened);
    assert!(!target.exists());
}

#[cfg(unix)]
#[test]
fn a_wait_file_planted_as_a_symlink_is_refused_not_followed() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("s");
    let store = Store::create(&dir, Config::default()).unwrap();
    store.transact(|tx| tx.upsert_node(&doc("https://x.dev/a"))).unwrap();
    let target = root.path().join("elsewhere");
    let other = Store::open(&dir).unwrap();
    let _ = std::fs::remove_file(dir.join(graph_r::store::WAIT_FILE));
    std::os::unix::fs::symlink(&target, dir.join(graph_r::store::WAIT_FILE)).unwrap();
    let r = other.transact(|tx| tx.upsert_node(&doc("https://x.dev/b")));
    assert!(matches!(r, Err(Error::Corrupt { .. })), "{r:?}");
    assert!(!target.exists(), "the wait file was created through the symlink");
}
