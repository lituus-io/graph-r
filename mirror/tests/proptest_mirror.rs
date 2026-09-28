// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>
//
//! Random interleavings of commits, compactions, publishes and replica
//! refreshes: after every refresh the replica reads exactly what the writer
//! read at its last publish, and a fresh pull is byte-identical to the
//! writer's committed files.

#[path = "../../tests/support/golden.rs"]
mod golden;

use futures::executor::block_on;
use graph_r::prelude::*;
use graph_r_mirror::Mirror;
use objstore_r::Mem;
use proptest::prelude::*;

#[derive(Debug, Clone)]
enum Op {
    Commit(u8),
    Compact,
    Publish,
    Refresh,
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (1u8..6).prop_map(Op::Commit),
        1 => Just(Op::Compact),
        3 => Just(Op::Publish),
        2 => Just(Op::Refresh),
    ]
}

fn commit(store: &Store, next: &mut u64, n: u8) {
    let mut w = store.writer().unwrap();
    for _ in 0..n {
        let url = format!("https://p.dev/{}", *next % 23); // overwrites happen too
        w.upsert_node(&DocRecord {
            url: &url,
            url_key: UrlKey::of(&url),
            content_hash: *next,
            fetched_at_ms: *next,
            title: Some("t"),
            snippet: None,
            etag: None,
            pinned: (*next).is_multiple_of(5),
        })
        .unwrap();
        *next += 1;
    }
    w.commit().unwrap();
}

fn state(store: &Store) -> Vec<(String, u64)> {
    let snap = store.snapshot();
    (0..23)
        .filter_map(|i| {
            let url = format!("https://p.dev/{i}");
            snap.node(UrlKey::of(&url)).map(|n| (n.url.to_owned(), n.content_hash))
        })
        .collect()
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: std::env::var("PROPTEST_CASES").ok().and_then(|v| v.parse().ok()).unwrap_or(48),
        ..ProptestConfig::default()
    })]

    #[test]
    fn replicas_track_publishes(ops in prop::collection::vec(op(), 1..40)) {
        let wdir = tempfile::tempdir().unwrap();
        let store = Store::create(wdir.path(), Config { compact_after_ops: 17, ..Config::default() }).unwrap();
        let mem = Mem::default();
        let w = Mirror::new(&mem, "p").unwrap();
        block_on(w.publish(&store)).unwrap();
        let rdir = tempfile::tempdir().unwrap();
        let r = Mirror::new(&mem, "p").unwrap();
        block_on(r.pull_into(rdir.path())).unwrap();
        let replica = Store::open_read_only(rdir.path()).unwrap();
        let mut published = state(&store);
        let mut next = 0;
        for op in &ops {
            match op {
                Op::Commit(n) => commit(&store, &mut next, *n),
                Op::Compact => { store.compact().unwrap(); }
                Op::Publish => { block_on(w.publish(&store)).unwrap(); published = state(&store); }
                Op::Refresh => {
                    block_on(r.refresh_into(&replica)).unwrap();
                    prop_assert_eq!(state(&replica), published.clone());
                }
            }
        }
        block_on(w.publish(&store)).unwrap();
        let fresh = tempfile::tempdir().unwrap();
        block_on(Mirror::new(&mem, "p").unwrap().pull_into(fresh.path())).unwrap();
        for f in ["graph.base", "graph.wal"] {
            prop_assert_eq!(std::fs::read(fresh.path().join(f)).unwrap(), std::fs::read(wdir.path().join(f)).unwrap());
        }
        block_on(r.refresh_into(&replica)).unwrap();
        prop_assert_eq!(golden::dump(&replica), golden::dump(&store));
    }
}
