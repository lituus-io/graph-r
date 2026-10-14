// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! Shared by the golden-fixture generator (run once against the released
//! 0.2.1 code) and `tests/golden_v021.rs` (run against every later version):
//! the same seed ops, and a textual dump of everything a reader can observe
//! through the public API. Uses only API that exists in 0.2.1.

#![allow(dead_code)]

use graph_r::prelude::*;
use graph_r::traverse::LendingIterator;
use std::fmt::Write as _;

pub const URLS: [&str; 6] = [
    "https://docs.x.dev/guide",
    "https://docs.x.dev/api",
    "https://docs.x.dev/faq",
    "https://docs.x.dev/changelog",
    "https://docs.x.dev/old",
    "https://elsewhere.example.com/spec",
];
pub const QUERIES: [&str; 5] = ["install guide", "api auth", "widget", "linux", "changelog 2024"];
pub const NOW: u64 = 10_000_000_000;

fn key(url: &str) -> UrlKey {
    UrlKey::of(url)
}

/// url, title, snippet, segments (label, depth, importance), links.
type Page<'a> = (&'a str, &'a str, &'a str, &'a [(&'a str, u8, u16)], &'a [&'a str]);

/// First batch: pages, segments, links.
pub fn seed(store: &Store) {
    let mut w = store.writer().unwrap();
    let pages: [Page<'_>; 5] = [
        (
            URLS[0],
            "Guide",
            "The install guide for the widget toolchain",
            &[("Install > Linux", 2, 60_000), ("Install > Windows", 2, 40_000)],
            &[URLS[1], URLS[2]],
        ),
        (
            URLS[1],
            "API reference",
            "Functions and types exposed by the widget api",
            &[("Auth", 1, 50_000), ("Endpoints", 1, 55_000)],
            &[URLS[0]],
        ),
        (URLS[2], "FAQ", "Common questions about widgets", &[], &[URLS[0], URLS[5]]),
        (
            URLS[3],
            "Changelog",
            "Release notes 2024 and 2025",
            &[("2025", 1, 30_000), ("2024", 1, 20_000)],
            &[URLS[1]],
        ),
        (URLS[4], "Old page", "Deprecated widget docs", &[], &[]),
    ];
    for (i, (url, title, snippet, segs, links)) in pages.into_iter().enumerate() {
        w.upsert_node(&DocRecord {
            url,
            url_key: key(url),
            content_hash: 0xC0FFEE + i as u64,
            fetched_at_ms: 1_000_000 + i as u64,
            title: Some(title),
            snippet: Some(snippet),
            etag: Some("\"v1\""),
            pinned: i == 1,
        })
        .unwrap();
        let recs: Vec<SegmentRecord<'_>> = segs
            .iter()
            .map(|&(label, depth, importance)| SegmentRecord {
                label,
                byte_range: Some((10, 100)),
                depth,
                importance,
            })
            .collect();
        w.set_segments(key(url), &recs).unwrap();
        let edges: Vec<(UrlKey, EdgeType, u16)> =
            links.iter().map(|l| (key(l), EdgeType::Link, 65_535)).collect();
        w.set_edges(key(url), &edges);
    }
    w.commit().unwrap();
}

/// Second batch: revalidations, a removal, a pin change.
pub fn churn(store: &Store) {
    let mut w = store.writer().unwrap();
    w.touch(
        key(URLS[0]),
        Touch {
            checked_at_ms: 2_000_000,
            outcome: Outcome::Unchanged,
            content_hash: None,
            etag: None,
        },
    )
    .unwrap();
    w.touch(
        key(URLS[3]),
        Touch {
            checked_at_ms: 2_100_000,
            outcome: Outcome::Changed,
            content_hash: Some(0xBEEF),
            etag: Some("\"v2\""),
        },
    )
    .unwrap();
    w.touch(
        key(URLS[2]),
        Touch { checked_at_ms: 2_200_000, outcome: Outcome::Gone, content_hash: None, etag: None },
    )
    .unwrap();
    w.remove(key(URLS[4])).unwrap();
    w.set_pinned(key(URLS[3]), true).unwrap();
    w.commit().unwrap();
}

/// Everything observable, in a stable textual form.
pub fn dump(store: &Store) -> String {
    let snap = store.snapshot();
    let mut o = String::new();
    writeln!(o, "len {}", snap.len()).unwrap();
    for url in URLS {
        let k = key(url);
        match snap.node(k) {
            None => writeln!(o, "node {url}: none").unwrap(),
            Some(n) => writeln!(
                o,
                "node {url}: url={:?} title={:?} snippet={:?} etag={:?} hash={:x} fetched={} changed={} interval={} checks={} changes={} flags={:x} pinned={} stub={} tomb={}",
                n.url, n.title, n.snippet, n.etag, n.content_hash, n.fetched_at_ms, n.last_change_ms, n.interval_s,
                n.checks, n.changes, n.flags, n.pinned(), n.is_stub(), n.is_tombstone()
            )
            .unwrap(),
        }
        for s in snap.segments(k) {
            writeln!(
                o,
                "  seg {:?} {:?} d{} i{} f{}",
                s.label, s.byte_range, s.depth, s.importance, s.flags
            )
            .unwrap();
        }
        let mut it = snap.neighbors(k);
        while let Some(e) = it.next() {
            writeln!(
                o,
                "  edge -> {:x} {:?} {:?} f{} w{}",
                e.dst_key.0, e.dst_url, e.etype, e.flags, e.weight
            )
            .unwrap();
        }
    }
    for q in QUERIES {
        writeln!(o, "query {q:?}").unwrap();
        for h in snap.query(q, &QueryOpts::default()) {
            writeln!(
                o,
                "  {} {:?} {:.4} seed={}",
                h.url,
                h.anchor.map(|a| (a.label, a.byte_range, a.importance)),
                h.score,
                h.seed
            )
            .unwrap();
        }
    }
    let mut depth = None;
    snap.walk(
        key(URLS[3]),
        graph_r::Direction::Out,
        4,
        |_| true,
        &mut graph_r::Walk::new(),
        |h| {
            if h.key == key(URLS[2]) {
                depth = Some(h.depth);
            }
            depth.is_none()
        },
    );
    writeln!(o, "reach {depth:?}").unwrap();
    for d in snap.due(NOW, 16) {
        writeln!(o, "due {} {:?}", d.url, d.etag).unwrap();
    }
    o
}
