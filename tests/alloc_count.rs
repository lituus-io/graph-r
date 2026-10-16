// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! Counted, not clocked: allocations, through a counting global allocator
//! in this test binary alone. The counts are exact and per thread, so they
//! hold on any machine and any load.
//!
//! - Opening a log allocates the same whether it holds a thousand ops or
//!   ten thousand: the log is read once and served in place, never decoded
//!   op by op.
//! - Folding the overlay for a snapshot allocates only its hash map.
//! - A steady-state commit allocates nothing after staging: the staged
//!   buffer is sealed, written and moved into the overlay as is.
//! - Replay validates in place: no allocation per record, and none for a
//!   hostile length field.

#![cfg(feature = "store")]
// A global allocator is `unsafe impl GlobalAlloc` by the trait's own
// signature. This one forwards every call to `System` unchanged and only
// counts; it exists in this test binary, never in the library.
#![allow(unsafe_code)]

use graph_r::format::wal;
use graph_r::prelude::*;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

struct Counting;

thread_local! {
    static ALLOCS: Cell<u64> = const { Cell::new(0) };
}

fn bump() {
    // `try_with`: an allocation during thread teardown is simply not counted.
    let _ = ALLOCS.try_with(|c| c.set(c.get() + 1));
}

// SAFETY: every method forwards its arguments to `System` unchanged and
// returns what `System` returns; counting touches only a thread-local cell.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        bump();
        // SAFETY: the caller's contract is passed through to `System`.
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        bump();
        // SAFETY: as above.
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        bump();
        // SAFETY: as above.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: as above.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static COUNTING: Counting = Counting;

/// Run `f`, returning its result and the allocations it made on this thread.
fn counted<R>(f: impl FnOnce() -> R) -> (R, u64) {
    let before = ALLOCS.with(Cell::get);
    let r = f();
    (r, ALLOCS.with(Cell::get) - before)
}

fn url_of(i: usize) -> String {
    format!("https://alloc.dev/section/{}/page/{i}", i % 13)
}

fn doc(url: &str) -> DocRecord<'_> {
    DocRecord {
        url,
        url_key: UrlKey::of(url),
        content_hash: 1,
        fetched_at_ms: 1,
        title: Some("A page title long enough to live on the heap"),
        snippet: Some("a snippet"),
        etag: None,
        pinned: false,
    }
}

/// A store whose log holds `ops` upserts (in transactions of 100), never
/// compacted.
fn store_with(dir: &std::path::Path, ops: usize) {
    let cfg = Config {
        compact_after_ops: usize::MAX,
        compact_after_wal_bytes: u64::MAX,
        durability: Durability::Never,
        ..Config::default()
    };
    let store = Store::create(dir, cfg).unwrap();
    for start in (0..ops).step_by(100) {
        let urls: Vec<String> = (start..(start + 100).min(ops)).map(url_of).collect();
        store.transact(|tx| urls.iter().try_for_each(|u| tx.upsert_node(&doc(u)))).unwrap();
    }
}

#[test]
fn opening_a_log_allocates_independently_of_its_op_count() {
    let (small, large) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    store_with(small.path(), 1_000);
    store_with(large.path(), 10_000);
    let (s, a_small) = counted(|| Store::open_read_only(small.path()).unwrap());
    let (l, a_large) = counted(|| Store::open_read_only(large.path()).unwrap());
    assert_eq!((s.len(), l.len()), (1_000, 10_000));
    assert!(
        a_large <= a_small + 8,
        "opening 10k ops made {a_large} allocations, 1k ops {a_small}: they must not scale with ops"
    );
    // The first overlay read folds every op; only the map allocates.
    let snap = l.snapshot();
    let (k1, k2) = (UrlKey::of(&url_of(5_000)), UrlKey::of(&url_of(7)));
    let (node, fold) = counted(|| snap.node(k1).map(|n| n.content_hash));
    assert_eq!(node, Some(1));
    assert!(fold < 64, "folding 10k ops made {fold} allocations");
    let (url, again) = counted(|| snap.node(k2).map(|n| n.url.len()));
    assert_eq!(url, Some(url_of(7).len()));
    assert_eq!(again, 0, "a read of the folded view allocates nothing");
}

/// A commit's catch-up stats the store's two files by path (that is how it
/// sees another process replace one). On Windows the standard library turns
/// every path it passes to the OS into a heap-allocated UTF-16 copy, so those
/// two stats are two allocations there; elsewhere a path call allocates
/// nothing. Exact, not a bound: a third allocation fails on every platform.
const OS_PATH_ALLOCS: u64 = if cfg!(windows) { 2 } else { 0 };

#[test]
fn a_steady_state_commit_allocates_nothing_after_staging() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), Config::default()).unwrap();
    let mut w = store.writer().unwrap();
    w.upsert_node(&doc("https://alloc.dev/warm")).unwrap();
    w.commit().unwrap(); // opens the log handle, the lock files, the first chunks
    for round in 0..20 {
        for i in 0..3 {
            w.upsert_node(&doc(&url_of(round * 3 + i))).unwrap();
        }
        let (last, n) = counted(|| w.commit().unwrap());
        assert_eq!(last, 1 + (round as u64 + 1) * 3);
        assert_eq!(n, OS_PATH_ALLOCS, "commit {round} allocated {n} times after staging");
    }
    drop(w);
    assert_eq!(store.len(), 61);
}

#[test]
fn replay_validates_in_place() {
    let dir = tempfile::tempdir().unwrap();
    store_with(dir.path(), 1_000);
    let image = std::fs::read(dir.path().join(graph_r::store::WAL_FILE)).unwrap();
    let (ops, n) = counted(|| {
        let replayed = wal::replay(&image).unwrap();
        replayed.records().flat_map(|r| r.ops()).count()
    });
    assert_eq!(ops, 1_000);
    assert_eq!(n, 0, "replaying 1 000 ops allocated {n} times");
    // A hostile length field: torn, with nothing allocated for it.
    let mut hostile = wal::encode_header(1, 0);
    hostile.extend_from_slice(&u32::MAX.to_le_bytes());
    hostile.extend_from_slice(&0x5854u16.to_le_bytes());
    hostile.extend_from_slice(&[1, 0]);
    hostile.resize(wal::HEADER_LEN + wal::RECORD_HEADER_LEN, 0xAB);
    let (good, n) = counted(|| wal::replay(&hostile).map(|r| r.good_len));
    assert_eq!(good.unwrap(), wal::HEADER_LEN);
    assert_eq!(n, 0, "a hostile length allocated {n} times");
}
