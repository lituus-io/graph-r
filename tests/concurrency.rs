// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>
//
//! Concurrency stress: many readers under a mutating + compacting writer.
//! The invariants: reads never fail, never observe torn state (a snapshot's
//! view is frozen at creation), and the writer is never blocked by readers.

use graph_r::prelude::*;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

fn url_of(n: u64) -> String {
    format!("https://c.dev/page/{n}")
}

#[test]
fn readers_never_block_or_tear_under_write_and_compaction() {
    let dir = tempfile::tempdir().unwrap();
    // Force frequent compactions during the run.
    let cfg = Config { compact_after_ops: 64, ..Config::default() };
    let store = Store::create(dir.path(), cfg).unwrap();

    // Seed one generation so readers always have something.
    {
        let mut w = store.writer().unwrap();
        for n in 0..8 {
            let url = url_of(n);
            w.upsert_node(&DocRecord {
                url: &url,
                url_key: UrlKey::of(&url),
                content_hash: n,
                fetched_at_ms: 1,
                title: Some("seed"),
                snippet: None,
                etag: None,
                pinned: false,
            })
            .unwrap();
        }
        w.commit().unwrap();
    }
    store.compact().unwrap();

    let stop = AtomicBool::new(false);
    let reads = AtomicU64::new(0);
    let writes = AtomicU64::new(0);

    std::thread::scope(|scope| {
        // 8 readers hammering point lookups + queries + due lists.
        for _ in 0..8 {
            scope.spawn(|| {
                let mut local = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    let snap = store.snapshot();
                    // A snapshot must be internally frozen: the same lookup
                    // twice within one snapshot gives the same answer.
                    let k = UrlKey::of(&url_of(local % 8));
                    let a = snap.node(k).map(|n| (n.content_hash, n.checks));
                    let b = snap.node(k).map(|n| (n.content_hash, n.checks));
                    assert_eq!(a, b, "torn read within one snapshot");
                    let _ = snap.query("seed page", &QueryOpts::default());
                    let _ = snap.due(u64::MAX / 2, 4);
                    local += 1;
                    reads.fetch_add(1, Ordering::Relaxed);
                }
            });
        }
        // 1 writer upserting + touching, triggering auto-compaction.
        scope.spawn(|| {
            for round in 0..400u64 {
                let mut w = store.writer().unwrap();
                let url = url_of(round % 16);
                w.upsert_node(&DocRecord {
                    url: &url,
                    url_key: UrlKey::of(&url),
                    content_hash: round,
                    fetched_at_ms: round + 2,
                    title: Some("live"),
                    snippet: Some("mutating under readers"),
                    etag: None,
                    pinned: false,
                })
                .unwrap();
                w.touch(
                    UrlKey::of(&url),
                    Touch {
                        checked_at_ms: round + 3,
                        outcome: Outcome::Unchanged,
                        content_hash: None,
                        etag: None,
                    },
                )
                .unwrap();
                match w.commit() {
                    Ok(_) => {}
                    // Auto-compaction may momentarily find all spare slots
                    // pinned by the reader threads; that is a documented,
                    // retryable condition — never a panic or a corruption.
                    Err(Error::Busy { .. }) => {}
                    Err(e) => panic!("writer failed: {e}"),
                }
                writes.fetch_add(1, Ordering::Relaxed);
            }
            stop.store(true, Ordering::Relaxed);
        });
    });

    assert!(writes.load(Ordering::Relaxed) == 400);
    assert!(reads.load(Ordering::Relaxed) > 0);

    // The store is consistent afterwards.
    store.compact().unwrap();
    let snap = store.snapshot();
    snap.check().unwrap();
    assert_eq!(snap.len(), 16);
}
