// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! `tests/fixtures/store_v042` was written by the 0.4.2 code path (a
//! compacted base plus an uncompacted log of touches, a removal, a pin and
//! attributes), in the format every release from 0.4.0 to 0.5.1 wrote (base
//! 2, log 2). 0.6 logs a transaction as one record, a format those releases
//! cannot read, and so writes base 3; this build refuses such a store with a
//! typed `Error::Version` read from its base, and writes nothing to it: a
//! consumer drains it with the release that wrote it and rebuilds, never has
//! it rewritten underneath it. (`compat_v060` holds the same store written
//! by this format.)

#![cfg(feature = "store")]

use graph_r::prelude::*;
use graph_r::store::{BASE_FILE, WAL_FILE};
use std::path::{Path, PathBuf};

fn fixture() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/store_v042")
}

/// Every file in `dir`, with its bytes.
fn contents(dir: &Path) -> Vec<(String, Vec<u8>)> {
    let mut v: Vec<(String, Vec<u8>)> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| {
            let e = e.unwrap();
            (e.file_name().to_string_lossy().into_owned(), std::fs::read(e.path()).unwrap())
        })
        .collect();
    v.sort();
    v
}

/// One way of opening a store.
type Opener = fn(&Path) -> graph_r::Result<Store>;

#[test]
fn a_0_4_2_store_is_a_typed_refusal() {
    let dir = tempfile::tempdir().unwrap();
    for f in [BASE_FILE, WAL_FILE] {
        std::fs::copy(fixture().join(f), dir.path().join(f)).unwrap();
    }
    let before = contents(dir.path());
    let opens: [(&str, Opener); 4] = [
        ("open", |d| Store::open(d)),
        ("open_cfg", |d| Store::open_cfg(d, Config::default())),
        ("open_or_create", |d| Store::open_or_create(d, Config::default())),
        ("open_read_only", |d| Store::open_read_only(d)),
    ];
    for (name, open) in opens {
        match open(dir.path()) {
            Err(Error::Version { found: 2, supported: 3 }) => {}
            other => panic!("{name}: expected Version {{ found: 2, supported: 3 }}, got {other:?}"),
        }
        assert_eq!(contents(dir.path()), before, "{name} changed the store on disk");
    }
    let text = Store::open(dir.path()).unwrap_err().to_string();
    assert!(text.starts_with("format error: unsupported version 2"), "{text}");
}
