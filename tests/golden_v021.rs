// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>
//
//! Compatibility with stores written by graph-r 0.2.1.
//!
//! `tests/fixtures/v0.2.1/` was produced by the released 0.2.1 code running
//! `tests/support/golden.rs` (seed + churn, three layouts: everything in the
//! WAL, everything compacted, and a compacted base with a live WAL), together
//! with a dump of everything observable through the public API. Every later
//! version must open those files and observe exactly the same thing — and
//! must keep writing the same bytes.

#[path = "support/golden.rs"]
mod golden;

use graph_r::{Config, Store};
use std::path::{Path, PathBuf};

const LAYOUTS: [&str; 3] = ["wal", "compacted", "mixed"];

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/v0.2.1").join(name)
}

/// Copy a fixture into a temp dir (opening read-write may repair or lock).
fn copy(layout: &str) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    for f in ["graph.base", "graph.wal"] {
        std::fs::copy(fixture(layout).join(f), dir.path().join(f)).unwrap();
    }
    dir
}

fn expected(layout: &str) -> String {
    std::fs::read_to_string(fixture(&format!("{layout}.dump"))).unwrap()
}

#[test]
fn every_020_layout_reads_identically_read_write_and_read_only() {
    for layout in LAYOUTS {
        let dir = copy(layout);
        let ro = Store::open_read_only(dir.path()).unwrap();
        assert_eq!(golden::dump(&ro), expected(layout), "{layout} read-only");
        drop(ro);
        let rw = Store::open(dir.path()).unwrap();
        assert_eq!(golden::dump(&rw), expected(layout), "{layout} read-write");
    }
}

#[test]
fn opening_does_not_rewrite_a_clean_020_store() {
    for layout in LAYOUTS {
        let dir = copy(layout);
        drop(Store::open(dir.path()).unwrap());
        for f in ["graph.base", "graph.wal"] {
            assert_eq!(
                std::fs::read(dir.path().join(f)).unwrap(),
                std::fs::read(fixture(layout).join(f)).unwrap(),
                "{layout}/{f} changed on open"
            );
        }
    }
}

#[test]
fn compaction_output_is_byte_identical_to_020() {
    // Folding the 0.2.1 WAL layout with today's code must give exactly the
    // base 0.2.1 wrote when it compacted the same history.
    let dir = copy("wal");
    let store = Store::open(dir.path()).unwrap();
    store.compact().unwrap();
    drop(store);
    assert_eq!(
        std::fs::read(dir.path().join("graph.base")).unwrap(),
        std::fs::read(fixture("compacted").join("graph.base")).unwrap()
    );
}

#[test]
fn wal_frames_are_byte_identical_to_020() {
    // Same ops through today's writer: identical frames (the 64-byte header
    // carries a creation timestamp, so only the frames are compared).
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), Config::default()).unwrap();
    golden::seed(&store);
    golden::churn(&store);
    drop(store);
    let ours = std::fs::read(dir.path().join("graph.wal")).unwrap();
    let theirs = std::fs::read(fixture("wal").join("graph.wal")).unwrap();
    assert_eq!(ours[64..], theirs[64..]);
    assert_eq!(graph_r::format::wal::committed_len(&theirs), theirs.len());
}

#[test]
fn a_020_store_keeps_working_after_new_writes() {
    let dir = copy("mixed");
    let store = Store::open(dir.path()).unwrap();
    golden::churn(&store); // idempotent-ish second round on top of 0.2.1 files
    store.compact().unwrap();
    let after = golden::dump(&store);
    drop(store);
    let reopened = Store::open_read_only(dir.path()).unwrap();
    assert_eq!(golden::dump(&reopened), after);
}
