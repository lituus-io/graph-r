// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! The 0.6 golden. `tests/fixtures/store_v060` was written by this format
//! (base 3, log 3) from the ops that wrote `store_v042` — the same seed, a
//! compaction, the same churn and named attributes — and `store_v060.dump`
//! is everything a reader observes in it. A later build must read it the
//! same, write byte-identical files for the same ops, and keep writing to
//! it. The dump is also 0.4.2's, byte for byte: the log format changed, not
//! one answer.

#![cfg(feature = "store")]
#[path = "support/golden.rs"]
mod golden;

use golden::{churn, dump, seed, URLS};
use graph_r::format::wal;
use graph_r::prelude::*;
use graph_r::store::{BASE_FILE, WAL_FILE};
use std::path::{Path, PathBuf};

fn fixture() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

/// A private copy, so opening (and committing to) it never touches the
/// checked-in files.
fn copy_of_fixture() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    for f in [BASE_FILE, WAL_FILE] {
        std::fs::copy(fixture().join("store_v060").join(f), dir.path().join(f)).unwrap();
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
fn a_0_6_0_store_reads_identically() {
    let want = std::fs::read_to_string(fixture().join("store_v060.dump")).unwrap();
    let dir = copy_of_fixture();
    let ro = Store::open_read_only(dir.path()).unwrap();
    assert_eq!(dump(&ro), want, "read-only view differs from 0.6.0's");
    assert_eq!(attrs(&ro), want_attrs());
    let rw = Store::open(dir.path()).unwrap();
    assert_eq!(dump(&rw), want, "read-write view differs from 0.6.0's");
    assert_eq!(attrs(&rw), want_attrs());
}

/// The golden's ops, written by this build into `dir`.
fn build(dir: &Path) {
    let store = Store::create(dir, Config::default()).unwrap();
    seed(&store);
    store.compact().unwrap();
    churn(&store);
    store
        .transact(|tx| {
            tx.set_named_attrs(UrlKey::of(URLS[0]), &[("type", "TABLE"), ("owner", "data")])
        })
        .unwrap();
}

/// Built and read by this build, the golden's ops answer every question
/// exactly as 0.4.2 answered them over its own format.
#[test]
fn every_answer_is_the_one_0_4_2_gave() {
    let dir = tempfile::tempdir().unwrap();
    build(dir.path());
    let want = std::fs::read_to_string(fixture().join("store_v042.dump")).unwrap();
    assert_eq!(dump(&Store::open_read_only(dir.path()).unwrap()), want);
}

/// The base layout did not change: the two goldens differ only in the
/// version field and the header checksum that covers it.
#[test]
fn the_base_differs_from_0_4_2_only_in_its_version() {
    let read = |d: &str| std::fs::read(fixture().join(d).join(BASE_FILE)).unwrap();
    let (old, new) = (read("store_v042"), read("store_v060"));
    assert_eq!(old.len(), new.len());
    let differ: Vec<usize> = (0..old.len()).filter(|&i| old[i] != new[i]).collect();
    assert!(differ.iter().all(|&i| (4..6).contains(&i) || (120..128).contains(&i)), "{differ:?}");
    assert_eq!((old[4], new[4]), (2, 3));
}

#[test]
fn the_same_ops_write_byte_identical_files() {
    let dir = tempfile::tempdir().unwrap();
    build(dir.path());
    let read = |d: &Path, f: &str| std::fs::read(d.join(f)).unwrap();
    let golden = fixture().join("store_v060");
    assert_eq!(read(dir.path(), BASE_FILE), read(&golden, BASE_FILE), "base bytes differ");
    let (new_wal, old_wal) = (read(dir.path(), WAL_FILE), read(&golden, WAL_FILE));
    // The header differs only in its creation time (and so its checksum).
    assert_eq!(new_wal[..24], old_wal[..24], "log header differs");
    assert_eq!(new_wal[wal::HEADER_LEN..], old_wal[wal::HEADER_LEN..], "log records differ");
    // Two commits (the churn and the attributes), two records.
    assert_eq!(wal::replay(&new_wal).unwrap().records().count(), 2);
}

#[test]
fn a_0_6_0_store_keeps_taking_commits() {
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
