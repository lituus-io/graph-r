// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
//! The lineage model: containment, value lineage with exact copies, row
//! conditions, node attributes, inbound edges and walks — each checked from
//! the overlay (before compaction) and from the base (after), which are
//! separate code paths that must agree.

use graph_r::format::base::eflags::EXACT;
use graph_r::prelude::*;
use graph_r::{Direction, Walk};

fn node(w: &mut Writer<'_>, url: &str) -> UrlKey {
    let key = UrlKey::of(url);
    w.upsert_node(&DocRecord {
        url,
        url_key: key,
        content_hash: 1,
        fetched_at_ms: 1,
        title: Some(url.rsplit('/').next().unwrap()),
        snippet: None,
        etag: None,
        pinned: false,
    })
    .unwrap();
    key
}

struct Fixture {
    raw: UrlKey,
    raw_status: UrlKey,
    raw_amount: UrlKey,
    stg: UrlKey,
    stg_status: UrlKey,
    stg_total: UrlKey,
}

const SQL: u8 = 1;
const TYPE: u8 = 2;

/// raw(status, amount) → stg(status copied, total derived), stg filtered on
/// raw.status. stg carries a large producing-SQL attribute.
fn build(store: &Store) -> Fixture {
    let mut w = store.writer().unwrap();
    let raw = node(&mut w, "sql://bq/p/raw/orders");
    let raw_status = node(&mut w, "sql://bq/p/raw/orders#status");
    let raw_amount = node(&mut w, "sql://bq/p/raw/orders#amount");
    let stg = node(&mut w, "sql://bq/p/stg/orders");
    let stg_status = node(&mut w, "sql://bq/p/stg/orders#status");
    let stg_total = node(&mut w, "sql://bq/p/stg/orders#total");
    w.set_edges(raw, &[(raw_status, EdgeType::Contains, 1), (raw_amount, EdgeType::Contains, 1)]);
    w.set_edges(
        stg,
        &[
            (stg_status, EdgeType::Contains, 1),
            (stg_total, EdgeType::Contains, 1),
            (raw_status, EdgeType::Conditions, 1),
        ],
    );
    w.set_edges_with_flags(stg_status, &[(raw_status, EdgeType::Derives, 1, EXACT)]);
    w.set_edges(
        stg_total,
        &[(raw_amount, EdgeType::Derives, 1), (raw_status, EdgeType::Derives, 1)],
    );
    let sql = format!("SELECT {} FROM raw", "x, ".repeat(30_000));
    w.set_attrs(stg, &[(SQL, &sql), (TYPE, "TABLE")]).unwrap();
    w.set_attrs(stg_total, &[(TYPE, "NUMERIC")]).unwrap();
    w.commit().unwrap();
    Fixture { raw, raw_status, raw_amount, stg, stg_status, stg_total }
}

fn check(store: &Store, f: &Fixture) {
    let snap = store.snapshot();
    // Attributes, large values included.
    assert!(snap.attr(f.stg, SQL).unwrap().len() > 90_000);
    assert_eq!(snap.attr(f.stg, TYPE), Some("TABLE"));
    assert_eq!(snap.attr(f.stg_total, TYPE), Some("NUMERIC"));
    assert_eq!(snap.attr(f.raw, TYPE), None);
    // Inbound: who contains, derives from or is conditioned on raw.status.
    let mut into_status = Vec::new();
    snap.in_edges(f.raw_status, |src, t, flags, _| into_status.push((src, t, flags & EXACT != 0)));
    into_status.sort_by_key(|e| e.0 .0);
    let mut want = vec![
        (f.raw, EdgeType::Contains, false),
        (f.stg, EdgeType::Conditions, false),
        (f.stg_status, EdgeType::Derives, true),
        (f.stg_total, EdgeType::Derives, false),
    ];
    want.sort_by_key(|e| e.0 .0);
    assert_eq!(into_status, want);
    // Upstream of stg.status along value lineage: an exact copy of raw.status.
    let mut w = Walk::new();
    let mut up = Vec::new();
    snap.walk(
        f.stg_status,
        Direction::Out,
        8,
        |t| t == EdgeType::Derives,
        &mut w,
        |h| {
            up.push((h.key, h.depth, h.exact_chain));
            true
        },
    );
    assert_eq!(up, vec![(f.raw_status, 1, true)]);
    // Downstream impact of raw.status: values (Derives) and rows (Conditions).
    let mut down = Vec::new();
    snap.walk(
        f.raw_status,
        Direction::In,
        8,
        |t| matches!(t, EdgeType::Derives | EdgeType::Conditions),
        &mut w,
        |h| {
            down.push((h.key, h.etype));
            true
        },
    );
    down.sort_by_key(|e| e.0 .0);
    let mut want = vec![
        (f.stg, EdgeType::Conditions),
        (f.stg_status, EdgeType::Derives),
        (f.stg_total, EdgeType::Derives),
    ];
    want.sort_by_key(|e| e.0 .0);
    assert_eq!(down, want);
    // A visitor that stops: only direct dependants.
    let mut n = 0;
    snap.walk(
        f.raw_amount,
        Direction::In,
        8,
        |_| true,
        &mut w,
        |_| {
            n += 1;
            false
        },
    );
    assert_eq!(n, 2, "raw (contains) and stg.total (derives)");
}

#[test]
fn lineage_reads_the_same_from_overlay_and_base() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), Config::default()).unwrap();
    store.set_compact_inline(false);
    let f = build(&store);
    check(&store, &f);
    store.compact().unwrap();
    check(&store, &f);
    // A read-only replica opened from disk agrees.
    check(&Store::open_read_only(dir.path()).unwrap(), &f);

    // Overlay changes over a compacted base: replaced edges and attributes
    // hide the base's, and inbound edges follow.
    let mut w = store.writer().unwrap();
    w.set_edges(f.stg_total, &[(f.raw_amount, EdgeType::Derives, 1)]);
    w.set_attrs(f.stg, &[(TYPE, "VIEW")]).unwrap();
    w.commit().unwrap();
    drop(w);
    let snap = store.snapshot();
    assert_eq!((snap.attr(f.stg, TYPE), snap.attr(f.stg, SQL)), (Some("VIEW"), None));
    let mut srcs = Vec::new();
    snap.in_edges(f.raw_status, |src, _, _, _| srcs.push(src));
    assert!(!srcs.contains(&f.stg_total), "the replaced edge set no longer points here");
    let mut srcs = Vec::new();
    snap.in_edges(f.raw_amount, |src, _, _, _| srcs.push(src));
    assert_eq!(
        srcs.iter().filter(|&&s| s == f.stg_total).count(),
        1,
        "exactly once, from the overlay"
    );
}

#[test]
fn wide_nodes_and_limits() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), Config::default()).unwrap();
    let mut w = store.writer().unwrap();
    let hub = node(&mut w, "sql://bq/p/mart/wide");
    let cols: Vec<(UrlKey, EdgeType, u16)> = (0..4_096)
        .map(|i| (UrlKey::of(&format!("sql://bq/p/raw/t#c{i}")), EdgeType::Derives, 1))
        .collect();
    w.set_edges(hub, &cols);
    assert!(w.set_attrs(hub, &[(1, &"x".repeat(graph_r::writer::MAX_ATTR_LEN + 1))]).is_err());
    let many: Vec<(u8, &str)> = (0..=graph_r::writer::MAX_ATTRS as u8).map(|k| (k, "v")).collect();
    assert!(w.set_attrs(hub, &many).is_err());
    w.commit().unwrap();
    drop(w);
    store.compact().unwrap();
    let snap = store.snapshot();
    let mut it = snap.neighbors(hub);
    let mut n = 0;
    while it.next().is_some() {
        n += 1;
    }
    assert_eq!(n, 4_096, "a 4096-column join is not truncated");
    let mut ins = 0;
    snap.in_edges(UrlKey::of("sql://bq/p/raw/t#c4095"), |_, _, _, _| ins += 1);
    assert_eq!(ins, 1, "stub targets have inbound edges too");
}
