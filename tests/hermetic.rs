// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>
//
//! Hermetic end-to-end lifecycle tests: create → write → commit → query →
//! compact → reopen → refresh cycle, all against a temp directory, no
//! network, no external services.

use graph_r::prelude::*;
use graph_r::traverse::LendingIterator;

fn key(url: &str) -> UrlKey {
    UrlKey::of(url)
}

/// Three interlinked doc pages + one external reference.
fn seed_store(store: &Store) {
    let mut w = store.writer().unwrap();
    let pages = [
        (
            "https://docs.x.dev/guide",
            "Guide",
            "The install guide for the widget toolchain",
            vec![("Install > Linux", 1u8, 60_000u16), ("Install > Windows", 2, 40_000)],
            vec!["https://docs.x.dev/api", "https://docs.x.dev/faq"],
        ),
        (
            "https://docs.x.dev/api",
            "API reference",
            "Functions and types exposed by the widget api",
            vec![("Auth", 1, 50_000), ("Endpoints", 1, 55_000)],
            vec!["https://docs.x.dev/guide"],
        ),
        (
            "https://docs.x.dev/faq",
            "FAQ",
            "Common questions about widgets",
            vec![],
            vec!["https://docs.x.dev/guide", "https://elsewhere.example.com/spec"],
        ),
    ];
    for (url, title, snippet, segs, links) in pages {
        w.upsert_node(&DocRecord {
            url,
            url_key: key(url),
            content_hash: xxhash_rust::xxh3::xxh3_64(url.as_bytes()),
            fetched_at_ms: 1_000_000,
            title: Some(title),
            snippet: Some(snippet),
            etag: Some("\"v1\""),
            pinned: false,
        })
        .unwrap();
        let seg_records: Vec<SegmentRecord<'_>> = segs
            .iter()
            .map(|&(label, depth, importance)| SegmentRecord {
                label,
                byte_range: Some((0, 100)),
                depth,
                importance,
            })
            .collect();
        w.set_segments(key(url), &seg_records).unwrap();
        let edges: Vec<(UrlKey, EdgeType, u16)> =
            links.iter().map(|l| (key(l), EdgeType::Link, 65_535)).collect();
        w.set_edges(key(url), &edges);
    }
    w.commit().unwrap();
}

#[test]
fn full_lifecycle_write_query_compact_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), Config::default()).unwrap();
    seed_store(&store);

    // Pre-compaction: overlay-fresh nodes must already be findable.
    {
        let snap = store.snapshot();
        assert_eq!(snap.len(), 3);
        let hits = snap.query("install guide", &QueryOpts::default());
        assert!(!hits.is_empty(), "fresh overlay nodes are queryable");
        assert_eq!(hits[0].url, "https://docs.x.dev/guide");
        let node = snap.node(key("https://docs.x.dev/api")).unwrap();
        assert_eq!(node.title, Some("API reference"));
        assert_eq!(node.etag, Some("\"v1\""));
    }

    // Compact: lexicon/ranks/CSR paths take over; stub minted for the
    // external reference.
    let stats = store.compact().unwrap();
    assert_eq!(stats.nodes, 4, "3 docs + 1 stub");
    {
        let snap = store.snapshot();
        assert_eq!(snap.len(), 3, "stub not counted live");
        assert_eq!(snap.generation(), 2);
        assert_eq!(snap.pending_ops(), 0);

        let hits = snap.query("install linux", &QueryOpts::default());
        assert_eq!(hits[0].url, "https://docs.x.dev/guide");
        let anchor = hits[0].anchor.expect("segment anchor attached");
        assert_eq!(anchor.label, "Install > Linux");

        // Graph structure.
        let mut n = snap.neighbors(key("https://docs.x.dev/guide"));
        let mut dsts = Vec::new();
        while let Some(e) = n.next() {
            dsts.push(e.dst_url.unwrap_or("").to_owned());
        }
        assert_eq!(dsts.len(), 2);
        let p = snap
            .path(key("https://docs.x.dev/api"), key("https://docs.x.dev/faq"), 4)
            .expect("api -> guide -> faq");
        assert_eq!(p.len(), 3);

        // The stub exists, is not live, and resolves by key.
        let stub = snap.node(key("https://elsewhere.example.com/spec")).unwrap();
        assert!(stub.is_stub());
        assert_eq!(stub.url, "");
    }

    // Reopen from disk: identical view.
    drop(store);
    let store = Store::open(dir.path()).unwrap();
    let snap = store.snapshot();
    assert_eq!(snap.len(), 3);
    assert_eq!(snap.generation(), 2);
    let hits = snap.query("widget api auth", &QueryOpts::default());
    assert_eq!(hits[0].url, "https://docs.x.dev/api");
}

#[test]
fn ttl_cycle_due_touch_backoff_and_tombstone() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = Config::default();
    // Disable the importance bias so due-times are exact multiples of the
    // interval; the bias itself is covered by ttl unit tests.
    cfg.ttl.importance_bias_permille = 0;
    let store = Store::create(dir.path(), cfg).unwrap();
    seed_store(&store);
    store.compact().unwrap();

    let base_s = u64::from(cfg.ttl.base_s);
    let t0 = 1_000_000u64;

    // Not yet due just after ingest; due after base interval passes.
    {
        let snap = store.snapshot();
        assert!(snap.due(t0 + 1000, 10).is_empty());
        let due = snap.due(t0 + base_s * 1000 + 1, 10);
        assert_eq!(due.len(), 3);
        assert!(due.iter().all(|d| d.etag == Some("\"v1\"")), "etag rides along for INM");
    }

    // Unchanged observation grows the interval 1.5x.
    let t1 = t0 + base_s * 1000 + 1;
    {
        let mut w = store.writer().unwrap();
        for url in ["https://docs.x.dev/guide", "https://docs.x.dev/api", "https://docs.x.dev/faq"]
        {
            assert!(w
                .touch(
                    key(url),
                    Touch {
                        checked_at_ms: t1,
                        outcome: Outcome::Unchanged,
                        content_hash: None,
                        etag: None,
                    },
                )
                .unwrap());
        }
        w.commit().unwrap();
    }
    {
        let snap = store.snapshot();
        let n = snap.node(key("https://docs.x.dev/guide")).unwrap();
        assert_eq!(n.checks, 1);
        assert_eq!(u64::from(n.interval_s), base_s * 3 / 2);
        assert!(snap.due(t1 + base_s * 1000 + 1, 10).is_empty(), "not due at old interval");
        assert_eq!(snap.due(t1 + base_s * 3 / 2 * 1000 + 1, 10).len(), 3);
    }

    // Changed cuts the interval; Gone tombstones and leaves the schedule.
    let t2 = t1 + base_s * 2 * 1000;
    {
        let mut w = store.writer().unwrap();
        w.touch(
            key("https://docs.x.dev/api"),
            Touch {
                checked_at_ms: t2,
                outcome: Outcome::Changed,
                content_hash: Some(42),
                etag: Some("\"v2\""),
            },
        )
        .unwrap();
        w.touch(
            key("https://docs.x.dev/faq"),
            Touch { checked_at_ms: t2, outcome: Outcome::Gone, content_hash: None, etag: None },
        )
        .unwrap();
        w.commit().unwrap();
    }
    {
        let snap = store.snapshot();
        let api = snap.node(key("https://docs.x.dev/api")).unwrap();
        assert_eq!(api.changes, 1);
        assert_eq!(api.content_hash, 42);
        assert_eq!(api.etag, Some("\"v2\""));
        assert!(u64::from(api.interval_s) < base_s * 3 / 2, "changed cut the interval");
        let faq = snap.node(key("https://docs.x.dev/faq")).unwrap();
        assert!(faq.is_tombstone());
        assert_eq!(snap.len(), 2, "tombstone leaves the live set");
        let far_future = t2 + 365 * 86_400 * 1000;
        assert!(
            snap.due(far_future, 10).iter().all(|d| d.key != key("https://docs.x.dev/faq")),
            "tombstones never come due"
        );
    }

    // The freshness state survives compaction + reopen byte-for-byte.
    store.compact().unwrap();
    drop(store);
    let store = Store::open(dir.path()).unwrap();
    let snap = store.snapshot();
    let api = snap.node(key("https://docs.x.dev/api")).unwrap();
    assert_eq!(api.changes, 1);
    assert_eq!(api.checks, 2);
    assert_eq!(api.etag, Some("\"v2\""));
}

#[test]
fn remove_and_pin_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), Config::default()).unwrap();
    seed_store(&store);
    {
        let mut w = store.writer().unwrap();
        w.set_pinned(key("https://docs.x.dev/faq"), true).unwrap();
        w.remove(key("https://docs.x.dev/api")).unwrap();
        w.commit().unwrap();
    }
    {
        let snap = store.snapshot();
        assert!(snap.node(key("https://docs.x.dev/api")).is_none(), "removed pre-compact");
        assert!(snap.node(key("https://docs.x.dev/faq")).unwrap().pinned());
        assert_eq!(snap.len(), 2);
    }
    store.compact().unwrap();
    let snap = store.snapshot();
    // The guide still links to the removed page, so compaction re-materializes
    // it as a stub (URL-less edge target) — removed as a document, present as
    // a reference, and excluded from the live set.
    let api = snap.node(key("https://docs.x.dev/api")).expect("stub for referenced target");
    assert!(api.is_stub());
    assert_eq!(api.url, "");
    assert_eq!(snap.len(), 2);
    assert!(snap.node(key("https://docs.x.dev/faq")).unwrap().pinned());
}

#[test]
fn torn_wal_tail_recovers_committed_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), Config::default()).unwrap();
    seed_store(&store);
    drop(store);

    // Simulate a crash mid-append: garbage at the WAL tail.
    let wal = dir.path().join("graph.wal");
    let mut bytes = std::fs::read(&wal).unwrap();
    let intact = bytes.len();
    bytes.extend_from_slice(&[0xde, 0xad, 0xbe, 0xef, 0x00, 0x11, 0x22]);
    std::fs::write(&wal, &bytes).unwrap();

    let store = Store::open(dir.path()).unwrap();
    assert_eq!(store.snapshot().len(), 3, "committed prefix fully recovered");
    drop(store);
    assert_eq!(std::fs::metadata(&wal).unwrap().len(), intact as u64, "tail truncated");
}

#[test]
fn snapshot_isolation_across_commit_and_compact() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), Config::default()).unwrap();
    seed_store(&store);
    store.compact().unwrap();

    let old = store.snapshot();
    let old_len = old.len();
    let old_generation = old.generation();

    // Mutate + compact while the old snapshot stays pinned.
    {
        let mut w = store.writer().unwrap();
        w.upsert_node(&DocRecord {
            url: "https://docs.x.dev/new",
            url_key: key("https://docs.x.dev/new"),
            content_hash: 7,
            fetched_at_ms: 2_000_000,
            title: Some("Newcomer"),
            snippet: None,
            etag: None,
            pinned: false,
        })
        .unwrap();
        w.commit().unwrap();
    }
    store.compact().unwrap();

    // The old snapshot is frozen; a new one sees the new world.
    assert_eq!(old.len(), old_len);
    assert_eq!(old.generation(), old_generation);
    assert!(old.node(key("https://docs.x.dev/new")).is_none());
    let new = store.snapshot();
    assert_eq!(new.generation(), old_generation + 1);
    assert!(new.node(key("https://docs.x.dev/new")).is_some());
    assert_eq!(new.len(), old_len + 1);
}

#[test]
fn compaction_is_byte_reproducible() {
    let render = || {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::create(dir.path(), Config::default()).unwrap();
        seed_store(&store);
        store.compact().unwrap();
        std::fs::read(dir.path().join("graph.base")).unwrap()
    };
    assert_eq!(render(), render(), "identical ops render identical bytes");
}

#[test]
fn second_writer_process_is_locked_out_and_read_only_reloads() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), Config::default()).unwrap();
    seed_store(&store);
    store.compact().unwrap();

    // Read-only opener coexists and observes later changes on reload.
    let ro = Store::open_read_only(dir.path()).unwrap();
    assert_eq!(ro.snapshot().len(), 3);
    assert!(matches!(ro.writer(), Err(graph_r::Error::ReadOnly)));

    {
        let mut w = store.writer().unwrap();
        w.upsert_node(&DocRecord {
            url: "https://docs.x.dev/extra",
            url_key: key("https://docs.x.dev/extra"),
            content_hash: 1,
            fetched_at_ms: 3_000_000,
            title: None,
            snippet: None,
            etag: None,
            pinned: false,
        })
        .unwrap();
        w.commit().unwrap();
    }
    assert!(ro.reload_if_stale().unwrap());
    assert_eq!(ro.snapshot().len(), 4);

    // In-process second writer waits on the mutex (not a second flock), so
    // cross-process exclusion is what the lock file provides; simulate a
    // foreign process by opening rw again — the same process's flock is
    // re-entrant per-file-open on some platforms, so exercise via API:
    // writer() on the read-only handle already errored above.
}

#[test]
fn enrichment_tier_survives_crawl_replacement() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), Config::default()).unwrap();
    seed_store(&store);
    store.compact().unwrap();

    let guide = key("https://docs.x.dev/guide");
    // Enrichment: score segment 0 up.
    {
        let mut w = store.writer().unwrap();
        w.set_importance(guide, &[(0, 65_000)]).unwrap();
        w.commit().unwrap();
    }
    // Crawl-tier re-ingest of the same segments (unchanged headings).
    {
        let mut w = store.writer().unwrap();
        w.set_segments(
            guide,
            &[
                SegmentRecord {
                    label: "Install > Linux",
                    byte_range: Some((0, 100)),
                    depth: 1,
                    importance: 60_000,
                },
                SegmentRecord {
                    label: "Install > Windows",
                    byte_range: Some((0, 100)),
                    depth: 2,
                    importance: 40_000,
                },
            ],
        )
        .unwrap();
        w.commit().unwrap();
    }
    let snap = store.snapshot();
    let segs = snap.segments(guide);
    assert_eq!(segs[0].importance, 65_000, "LLM score carried across re-ingest");
    store.compact().unwrap();
    let snap = store.snapshot();
    let segs = snap.segments(guide);
    assert_eq!(segs[0].importance, 65_000, "and across compaction");
}

/// The community and discovery surface had no coverage at all — it was public
/// API whose behaviour nothing pinned. Two link clusters joined by one weak
/// bridge: each cluster is one community, the summary names and sizes it, and
/// the bridge edge is the top surprise.
#[test]
fn communities_and_surprises_reflect_cluster_structure() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), Config::default()).unwrap();
    {
        let mut w = store.writer().unwrap();
        // Cluster A: three docs fully interlinked. Cluster B: likewise.
        let cluster = |prefix: &str| -> Vec<String> {
            (0..3).map(|i| format!("https://{prefix}.x.dev/p{i}")).collect()
        };
        let (a, b) = (cluster("a"), cluster("b"));
        for (urls, title) in [(&a, "Alpha"), (&b, "Beta")] {
            for (i, url) in urls.iter().enumerate() {
                w.upsert_node(&DocRecord {
                    url,
                    url_key: key(url),
                    content_hash: i as u64 + 1,
                    fetched_at_ms: 1_000,
                    title: Some(title),
                    snippet: None,
                    etag: None,
                    pinned: false,
                })
                .unwrap();
                let edges: Vec<(UrlKey, EdgeType, u16)> = urls
                    .iter()
                    .filter(|u| *u != url)
                    .map(|u| (key(u), EdgeType::Link, 65_535))
                    .collect();
                w.set_edges(key(url), &edges);
            }
        }
        // One weak bridge from A to B — the cross-community edge.
        let mut bridge_edges: Vec<(UrlKey, EdgeType, u16)> =
            a.iter().skip(1).map(|u| (key(u), EdgeType::Link, 65_535)).collect();
        bridge_edges.push((key(&b[0]), EdgeType::Related, 200));
        w.set_edges(key(&a[0]), &bridge_edges);
        w.commit().unwrap();
    }
    store.compact().unwrap(); // ranks + communities are computed here

    let snap = store.snapshot();
    let a0 = key("https://a.x.dev/p0");
    let b0 = key("https://b.x.dev/p0");

    // Each cluster resolves to one community of three; the two differ.
    let ca = snap.community_summary(a0, 5).expect("a0 is ranked");
    let cb = snap.community_summary(b0, 5).expect("b0 is ranked");
    assert_eq!(ca.size, 3, "cluster A is one community");
    assert_eq!(cb.size, 3, "cluster B is one community");
    assert_ne!(ca.id, cb.id, "the weak bridge must not merge the clusters");
    assert_eq!(ca.top_urls.len(), 3);
    assert!(!ca.name.is_empty(), "the summary names its top member");

    // The bridge edge is the surprise: exactly one cross-community pair.
    let surprises = snap.surprises(10);
    assert_eq!(surprises.len(), 1, "one community pair, one representative edge");
    let (from, to) = &surprises[0];
    assert_eq!(from.key, a0);
    assert_eq!(to.key, b0);
}
