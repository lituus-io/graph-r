// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>
//
//! End-to-end mirror behaviour on the in-memory and local-directory backends
//! (the GCS backend runs the same protocol in tests/gcs.rs).

#[path = "../../tests/support/golden.rs"]
mod golden;

use futures::executor::block_on;
use graph_r::prelude::*;
use graph_r_mirror::{Error, Mirror, COALESCE_SEGMENTS};
use objstore_r::mem::Fault;
use objstore_r::{Bytes, Cond, LocalDir, Mem, ObjectStore};
use std::path::Path;

fn page(store: &Store, i: u64) {
    let url = format!("https://m.dev/{i}");
    let mut w = store.writer().unwrap();
    w.upsert_node(&DocRecord {
        url: &url,
        url_key: UrlKey::of(&url),
        content_hash: i,
        fetched_at_ms: i,
        title: Some("page"),
        snippet: Some("mirror test page"),
        etag: None,
        pinned: false,
    })
    .unwrap();
    w.commit().unwrap();
}

fn files(dir: &Path) -> (Vec<u8>, Vec<u8>) {
    (std::fs::read(dir.join("graph.base")).unwrap(), std::fs::read(dir.join("graph.wal")).unwrap())
}

fn writer() -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), Config::default()).unwrap();
    (dir, store)
}

#[test]
fn nothing_published_pulls_nothing_and_leaves_the_directory_alone() {
    let mem = Mem::default();
    let dst = tempfile::tempdir().unwrap();
    assert_eq!(block_on(Mirror::new(&mem, "kb").unwrap().pull_into(dst.path())).unwrap(), None);
    assert_eq!(std::fs::read_dir(dst.path()).unwrap().count(), 0);
}

#[test]
fn pulled_files_are_byte_identical_and_read_identically() {
    for layout in ["wal", "compacted", "mixed"] {
        let (dir, store) = writer();
        golden::seed(&store);
        if layout != "wal" {
            store.compact().unwrap();
        }
        golden::churn(&store);
        if layout == "compacted" {
            store.compact().unwrap();
        }
        let mem = Mem::default();
        let m = Mirror::new(&mem, "kb/x").unwrap();
        block_on(m.publish(&store)).unwrap();
        let dst = tempfile::tempdir().unwrap();
        let pulled =
            block_on(Mirror::new(&mem, "kb/x").unwrap().pull_into(dst.path())).unwrap().unwrap();
        assert_eq!(pulled.generation, store.generation());
        assert_eq!(files(dst.path()), files(dir.path()), "{layout}");
        let replica = Store::open(dst.path()).unwrap();
        assert_eq!(golden::dump(&replica), golden::dump(&store), "{layout}");
    }
}

#[test]
fn publish_ships_only_new_frames_then_a_new_base_after_compaction() {
    let (dir, store) = writer();
    let mem = Mem::default();
    let m = Mirror::new(&mem, "").unwrap();
    let first = block_on(m.publish(&store)).unwrap();
    assert!(first.new_base);
    let again = block_on(m.publish(&store)).unwrap();
    assert_eq!(again.uploaded, 0, "nothing new");

    let before = mem.keys().len();
    page(&store, 1);
    let p = block_on(m.publish(&store)).unwrap();
    assert!(!p.new_base && p.uploaded > 0 && p.uploaded < 1024, "{p:?}");
    assert_eq!(mem.keys().len(), before + 1, "one new segment");

    store.compact().unwrap();
    let p = block_on(m.publish(&store)).unwrap();
    assert!(p.new_base);
    assert_eq!(p.generation, 2);
    let keys = mem.keys();
    assert!(
        keys.iter().all(|k| !k.contains("0000000000000001")),
        "old generation cleaned: {keys:?}"
    );
    assert_eq!(keys.len(), 3, "HEAD + base + one segment: {keys:?}");

    let dst = tempfile::tempdir().unwrap();
    block_on(Mirror::new(&mem, "").unwrap().pull_into(dst.path())).unwrap();
    assert_eq!(files(dst.path()), files(dir.path()));
}

#[test]
fn long_segment_runs_are_coalesced() {
    let (dir, store) = writer();
    let mem = Mem::default();
    let m = Mirror::new(&mem, "kb").unwrap();
    block_on(m.publish(&store)).unwrap();
    for i in 0..(COALESCE_SEGMENTS as u64 + 5) {
        page(&store, i);
        block_on(m.publish(&store)).unwrap();
    }
    let segs = mem.keys().into_iter().filter(|k| k.ends_with(".seg")).count();
    assert!(segs <= 6, "coalesced, then a few new tails: {segs}");
    let dst = tempfile::tempdir().unwrap();
    block_on(Mirror::new(&mem, "kb").unwrap().pull_into(dst.path())).unwrap();
    assert_eq!(files(dst.path()), files(dir.path()));
}

#[test]
fn a_replica_follows_every_step_with_minimal_reads() {
    let (_dir, store) = writer();
    let remote = tempfile::tempdir().unwrap();
    let objects = LocalDir::open(remote.path()).unwrap();
    let w = Mirror::new(&objects, "kb").unwrap();
    golden::seed(&store);
    block_on(w.publish(&store)).unwrap();

    let dst = tempfile::tempdir().unwrap();
    let r = Mirror::new(&objects, "kb").unwrap();
    block_on(r.pull_into(dst.path())).unwrap();
    let replica = Store::open_read_only(dst.path()).unwrap();
    assert!(!block_on(r.refresh_into(&replica)).unwrap(), "unchanged");

    golden::churn(&store);
    block_on(w.publish(&store)).unwrap();
    assert!(block_on(r.refresh_into(&replica)).unwrap());
    assert_eq!(golden::dump(&replica), golden::dump(&store));

    store.compact().unwrap();
    page(&store, 99);
    block_on(w.publish(&store)).unwrap();
    assert!(block_on(r.refresh_into(&replica)).unwrap());
    assert_eq!(golden::dump(&replica), golden::dump(&store));
    assert_eq!(replica.generation(), store.generation());
    assert!(!block_on(r.refresh_into(&replica)).unwrap());
}

#[test]
fn refresh_after_coalescing_appends_only_the_missing_suffix() {
    let (dir, store) = writer();
    let mem = Mem::default();
    let w = Mirror::new(&mem, "kb").unwrap();
    block_on(w.publish(&store)).unwrap();
    let dst = tempfile::tempdir().unwrap();
    let r = Mirror::new(&mem, "kb").unwrap();
    block_on(r.pull_into(dst.path())).unwrap();
    let replica = Store::open_read_only(dst.path()).unwrap();
    for i in 0..(COALESCE_SEGMENTS as u64 * 2) {
        page(&store, i);
        block_on(w.publish(&store)).unwrap();
        if i % 7 == 0 {
            block_on(r.refresh_into(&replica)).unwrap();
            assert_eq!(files(dst.path()).1, std::fs::read(dir.path().join("graph.wal")).unwrap());
        }
    }
    block_on(r.refresh_into(&replica)).unwrap();
    assert_eq!(files(dst.path()), files(dir.path()));
    assert_eq!(replica.len(), store.len());
}

#[test]
fn two_writers_from_one_state_exactly_one_wins() {
    let (_d, origin) = writer();
    page(&origin, 1);
    let mem = Mem::default();
    block_on(Mirror::new(&mem, "kb").unwrap().publish(&origin)).unwrap();
    drop(origin);

    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (ma, mb) = (Mirror::new(&mem, "kb").unwrap(), Mirror::new(&mem, "kb").unwrap());
    block_on(ma.pull_into(da.path())).unwrap();
    block_on(mb.pull_into(db.path())).unwrap();
    let (a, b) = (Store::open(da.path()).unwrap(), Store::open(db.path()).unwrap());
    page(&a, 2);
    page(&b, 3);
    block_on(ma.publish(&a)).unwrap();
    assert!(matches!(block_on(mb.publish(&b)), Err(Error::Fenced { .. })));
    // The loser can never slip in later either.
    page(&b, 4);
    assert!(matches!(block_on(mb.publish(&b)), Err(Error::Fenced { .. })));
    // And a fresh store that never pulled cannot overwrite the published one.
    let (_dc, c) = writer();
    page(&c, 5);
    assert!(block_on(Mirror::new(&mem, "kb").unwrap().publish(&c)).is_err());
}

#[test]
fn a_writable_store_blocks_pulls_into_its_directory() {
    let (dir, store) = writer();
    let mem = Mem::default();
    block_on(Mirror::new(&mem, "kb").unwrap().publish(&store)).unwrap();
    let err = block_on(Mirror::new(&mem, "kb").unwrap().pull_into(dir.path())).unwrap_err();
    assert!(matches!(err, Error::Graph(graph_r::Error::Locked)), "{err}");
}

#[test]
fn leases_exclude_and_expire() {
    let (_d, store) = writer();
    let mem = Mem::default();
    let a = Mirror::new(&mem, "kb").unwrap();
    let b = Mirror::new(&mem, "kb").unwrap();
    block_on(a.acquire_lease("a", 60_000)).unwrap();
    assert!(matches!(block_on(b.acquire_lease("b", 60_000)), Err(Error::Leased { .. })));
    block_on(a.publish(&store)).unwrap();
    assert_eq!(a.lease().unwrap().holder, "a");
    // Without the lease, b may not publish even with fresh state.
    let (_d2, other) = writer();
    assert!(matches!(block_on(b.publish(&other)), Err(Error::Leased { .. })));
    block_on(a.release_lease()).unwrap();
    block_on(b.acquire_lease("b", 1)).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(5));
    block_on(a.acquire_lease("a", 60_000)).expect("an expired lease can be taken over");
    assert!(matches!(
        block_on(b.publish(&other)),
        Err(Error::Leased { .. } | Error::Fenced { .. })
    ));
}

#[test]
fn a_crash_at_any_step_leaves_a_pullable_committed_state() {
    // Fail each object-store operation of a publish in turn — before it
    // applies, and after it applies with the reply lost. Whatever happened,
    // the remote must pull to either the old or the new state, and a retry
    // must converge.
    for lost in [false, true] {
        for step in 0..8 {
            let (dir, store) = writer();
            golden::seed(&store);
            let mem = Mem::default();
            let m = Mirror::new(&mem, "kb").unwrap();
            block_on(m.publish(&store)).unwrap();
            let old = golden::dump(&store);
            golden::churn(&store);
            store.compact().unwrap();
            page(&store, 7);
            let new = golden::dump(&store);
            let e = objstore_r::Error::Unavailable { message: "injected".into(), retriable: true };
            mem.inject_after(step, if lost { Fault::LostResponse(e) } else { Fault::Fail(e) });
            let first = block_on(m.publish(&store));
            mem.clear_faults();

            let dst = tempfile::tempdir().unwrap();
            block_on(Mirror::new(&mem, "kb").unwrap().pull_into(dst.path())).unwrap();
            let seen = golden::dump(&Store::open_read_only(dst.path()).unwrap());
            assert!(seen == old || seen == new, "step {step} lost={lost}: a torn state");
            if first.is_ok() {
                assert_eq!(seen, new, "step {step}: success means published");
            }
            block_on(m.publish(&store))
                .unwrap_or_else(|e| panic!("step {step} lost={lost}: retry failed: {e}"));
            let dst = tempfile::tempdir().unwrap();
            block_on(Mirror::new(&mem, "kb").unwrap().pull_into(dst.path())).unwrap();
            assert_eq!(
                files(dst.path()),
                files(dir.path()),
                "step {step} lost={lost}: retry converges"
            );
        }
    }
}

#[test]
fn tampered_objects_are_refused() {
    let (_d, store) = writer();
    golden::seed(&store);
    store.compact().unwrap();
    golden::churn(&store);
    let mem = Mem::default();
    block_on(Mirror::new(&mem, "kb").unwrap().publish(&store)).unwrap();
    for victim in mem.keys() {
        let original = block_on(mem.get(&victim)).unwrap().unwrap().bytes;
        let mut bad = original.to_vec();
        let at = bad.len() / 2;
        bad[at] ^= 0x01;
        block_on(mem.put(&victim, Bytes::from(bad), Cond::None)).unwrap();
        let dst = tempfile::tempdir().unwrap();
        let r = block_on(Mirror::new(&mem, "kb").unwrap().pull_into(dst.path()));
        assert!(matches!(r, Err(Error::Corrupt { .. })), "{victim}: {r:?}");
        assert!(!dst.path().join("graph.base").exists() || victim.ends_with(".seg"), "{victim}");
        block_on(mem.put(&victim, original, Cond::None)).unwrap();
    }
}

#[test]
fn a_directory_the_mirror_did_not_produce_is_checked_before_publishing() {
    let (_d, store) = writer();
    page(&store, 1);
    let mem = Mem::default();
    block_on(Mirror::new(&mem, "kb").unwrap().publish(&store)).unwrap();
    page(&store, 2);
    // A new Mirror (e.g. after a restart) on the same, genuinely newer store.
    let p = block_on(Mirror::new(&mem, "kb").unwrap().publish(&store)).unwrap();
    assert!(p.uploaded > 0 && !p.new_base);
}

#[test]
fn prefixes_are_validated_and_isolated() {
    let mem = Mem::default();
    assert!(Mirror::new(&mem, "../x").is_err());
    assert!(Mirror::new(&mem, "a//b").is_err());
    let (_d, s1) = writer();
    page(&s1, 1);
    let (_d2, s2) = writer();
    page(&s2, 2);
    page(&s2, 3);
    block_on(Mirror::new(&mem, "one").unwrap().publish(&s1)).unwrap();
    block_on(Mirror::new(&mem, "two/").unwrap().publish(&s2)).unwrap();
    let dst = tempfile::tempdir().unwrap();
    block_on(Mirror::new(&mem, "one").unwrap().pull_into(dst.path())).unwrap();
    assert_eq!(Store::open_read_only(dst.path()).unwrap().len(), 1);
}
