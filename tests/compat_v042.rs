// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! 0.5 changed who may write a store and when the lock is held, not a byte
//! of what is written. `tests/fixtures/store_v042` was written by the 0.4.2
//! code path (a compacted base plus an uncompacted log of touches, a removal,
//! a pin and attributes) and `store_v042.dump` is everything 0.4.2 read back
//! from it. This build must read the same, write byte-identical files for
//! the same ops, and keep writing to the old store.

#![cfg(feature = "store")]
#[path = "support/golden.rs"]
mod golden;

use golden::{churn, dump, seed, URLS};
use graph_r::format::wal;
use graph_r::prelude::*;
use std::path::{Path, PathBuf};

fn fixture() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

/// A private copy, so opening (and committing to) it never touches the
/// checked-in files.
fn copy_of_fixture() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    for f in [graph_r::store::BASE_FILE, graph_r::store::WAL_FILE] {
        std::fs::copy(fixture().join("store_v042").join(f), dir.path().join(f)).unwrap();
    }
    dir
}

fn attrs(store: &Store) -> Vec<(String, String)> {
    let mut v = Vec::new();
    store.snapshot().named_attrs(UrlKey::of(URLS[0]), |n, val| v.push((n.into(), val.into())));
    v
}

fn want_attrs() -> Vec<(String, String)> {
    vec![("owner".into(), "data".into()), ("type".into(), "TABLE".into())]
}

#[test]
fn a_0_4_2_store_reads_identically() {
    let want = std::fs::read_to_string(fixture().join("store_v042.dump")).unwrap();
    let dir = copy_of_fixture();
    let ro = Store::open_read_only(dir.path()).unwrap();
    assert_eq!(dump(&ro), want, "read-only view differs from 0.4.2's");
    assert_eq!(attrs(&ro), want_attrs());
    let rw = Store::open(dir.path()).unwrap();
    assert_eq!(dump(&rw), want, "read-write view differs from 0.4.2's");
    assert_eq!(attrs(&rw), want_attrs());
}

#[test]
fn the_same_ops_write_byte_identical_files() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), Config::default()).unwrap();
    seed(&store);
    store.compact().unwrap();
    churn(&store);
    store
        .transact(|tx| {
            tx.set_named_attrs(UrlKey::of(URLS[0]), &[("type", "TABLE"), ("owner", "data")])
        })
        .unwrap();
    drop(store);
    let read = |d: &Path, f: &str| std::fs::read(d.join(f)).unwrap();
    let old = fixture().join("store_v042");
    assert_eq!(
        read(dir.path(), graph_r::store::BASE_FILE),
        read(&old, graph_r::store::BASE_FILE),
        "base bytes differ from 0.4.2's"
    );
    let (new_wal, old_wal) =
        (read(dir.path(), graph_r::store::WAL_FILE), read(&old, graph_r::store::WAL_FILE));
    // The header differs only in its creation time (and so its checksum).
    assert_eq!(new_wal[..24], old_wal[..24], "log header differs from 0.4.2's");
    assert_eq!(new_wal[wal::HEADER_LEN..], old_wal[wal::HEADER_LEN..], "log frames differ");
}

#[test]
fn a_0_4_2_store_keeps_taking_commits() {
    let dir = copy_of_fixture();
    let a = Store::open(dir.path()).unwrap();
    let b = Store::open(dir.path()).unwrap();
    let url = "https://docs.x.dev/new";
    for store in [&a, &b] {
        store
            .transact(|tx| {
                tx.upsert_node(&DocRecord {
                    url,
                    url_key: UrlKey::of(url),
                    content_hash: tx.snapshot().len() as u64,
                    fetched_at_ms: 1,
                    title: None,
                    snippet: None,
                    etag: None,
                    pinned: false,
                })
            })
            .unwrap();
    }
    a.compact().unwrap();
    drop((a, b));
    let reopened = Store::open_read_only(dir.path()).unwrap();
    assert_eq!(reopened.len(), 4);
    assert_eq!(reopened.snapshot().node(UrlKey::of(url)).unwrap().content_hash, 4);
    assert_eq!(attrs(&reopened), want_attrs());
}
