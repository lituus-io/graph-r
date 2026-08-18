// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>
//
//! The permanent index of fixed defects.
//!
//! Every bug that has ever escaped into this crate gets a named test here, with
//! a note on what broke and how it was found. The model-based proptest in
//! `proptest_model.rs` re-derives most of these generatively, but a generative
//! failure reports a shrunk op sequence, not a diagnosis — these tests turn each
//! one back into a sentence.
//!
//! Some defects are additionally pinned as unit tests next to the code that
//! owns them (noted per test). That is deliberate: the unit test guards the
//! function, this suite guards the behaviour a caller depends on.

use graph_r::prelude::*;

fn doc<'a>(url: &'a str, hash: u64, pinned: bool) -> DocRecord<'a> {
    DocRecord {
        url,
        url_key: UrlKey::of(url),
        content_hash: hash,
        fetched_at_ms: 1_000,
        title: Some("T"),
        snippet: Some("S"),
        etag: Some("\"v1\""),
        pinned,
    }
}

// ---- overlay folding semantics --------------------------------------------

/// Found by the model proptest during development.
///
/// A pin is a retention decision made by a human; a re-crawl is a mechanical
/// event. An unpinned upsert of an already-pinned document silently cleared the
/// pin, so the next eviction sweep deleted a document the user had explicitly
/// asked to keep. Only an explicit unpin may clear a pin.
#[test]
fn an_unpinned_upsert_never_clears_an_existing_pin() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), Config::default()).unwrap();
    let url = "https://x.dev/keep";

    {
        let mut w = store.writer().unwrap();
        w.upsert_node(&doc(url, 1, true)).unwrap();
        w.commit().unwrap();
    }
    store.compact().unwrap(); // pin now lives in the base generation

    {
        let mut w = store.writer().unwrap();
        w.upsert_node(&doc(url, 2, false)).unwrap(); // re-crawl, content changed
        w.commit().unwrap();
    }
    assert!(
        store.snapshot().node(UrlKey::of(url)).unwrap().pinned(),
        "pin must survive a re-crawl"
    );

    // Across compaction too — the stickiness must be persisted, not just folded.
    store.compact().unwrap();
    assert!(
        store.snapshot().node(UrlKey::of(url)).unwrap().pinned(),
        "pin must survive compaction"
    );
}

/// Found by the model proptest during development.
///
/// An explicit unpin is the one thing that *does* clear a pin. The fix for the
/// test above must not overshoot into "a pin can never be removed".
#[test]
fn an_explicit_unpin_does_clear_the_pin() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), Config::default()).unwrap();
    let url = "https://x.dev/keep";

    {
        let mut w = store.writer().unwrap();
        w.upsert_node(&doc(url, 1, true)).unwrap();
        w.commit().unwrap();
    }
    store.compact().unwrap();

    {
        let mut w = store.writer().unwrap();
        assert!(w.set_pinned(UrlKey::of(url), false).unwrap(), "key exists, so this must stage");
        w.commit().unwrap();
    }
    assert!(!store.snapshot().node(UrlKey::of(url)).unwrap().pinned());
}

/// A pin staged against a key that does not exist must not lie in wait for a
/// future upsert to adopt it.
#[test]
fn pinning_an_unknown_key_stages_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), Config::default()).unwrap();
    let mut w = store.writer().unwrap();
    assert!(!w.set_pinned(UrlKey::of("https://x.dev/ghost"), true).unwrap());
    assert_eq!(w.staged(), 0, "nothing may be staged for a key that does not exist");
}

/// Found by the model proptest during development.
///
/// Freshness counters are history: how many times a source was checked, how many
/// times it actually changed, and when it last changed. A re-ingest used to
/// reset them to zero, which destroyed the signal the adaptive TTL is computed
/// from — every re-crawl made a well-understood document look brand new.
#[test]
fn freshness_history_survives_a_re_ingest() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), Config::default()).unwrap();
    let url = "https://x.dev/a";
    let key = UrlKey::of(url);

    {
        let mut w = store.writer().unwrap();
        w.upsert_node(&doc(url, 1, false)).unwrap();
        w.commit().unwrap();
    }
    // Three observations, one of which is a change.
    for (at, outcome) in
        [(2_000, Outcome::Unchanged), (3_000, Outcome::Changed), (4_000, Outcome::Unchanged)]
    {
        let mut w = store.writer().unwrap();
        w.touch(key, Touch { checked_at_ms: at, outcome, content_hash: None, etag: None }).unwrap();
        w.commit().unwrap();
    }
    // A NodeRef borrows its snapshot, so the snapshot has to outlive it.
    let last_change = {
        let snap = store.snapshot();
        let before = snap.node(key).unwrap();
        assert_eq!(before.checks, 3);
        assert_eq!(before.changes, 1);
        assert_eq!(before.last_change_ms, 3_000);
        before.last_change_ms
    };

    // Re-ingest the same document: the counters are history and must carry.
    {
        let mut w = store.writer().unwrap();
        w.upsert_node(&doc(url, 1, false)).unwrap();
        w.commit().unwrap();
    }
    let snap = store.snapshot();
    let after = snap.node(key).unwrap();
    assert_eq!(after.checks, 3, "checks are history, not per-generation state");
    assert_eq!(after.changes, 1, "changes are history");
    assert_eq!(after.last_change_ms, last_change, "last change stamp is history");
}

/// Found by the model proptest during development.
///
/// Touches fold last-wins in the overlay, so a `Touch` carries the node's whole
/// effective freshness state rather than a delta. When it did not, an
/// `Unchanged` check landing after a `Changed` one erased the hash and ETag the
/// change had observed — and the next conditional request went out with no
/// validator, re-downloading a body that had not moved.
#[test]
fn an_unchanged_touch_does_not_erase_the_hash_or_etag_of_a_prior_change() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), Config::default()).unwrap();
    let url = "https://x.dev/a";
    let key = UrlKey::of(url);

    {
        let mut w = store.writer().unwrap();
        w.upsert_node(&doc(url, 1, false)).unwrap();
        w.commit().unwrap();
    }
    // A change observes a new body and a new validator…
    {
        let mut w = store.writer().unwrap();
        assert!(w
            .touch(
                key,
                Touch {
                    checked_at_ms: 2_000,
                    outcome: Outcome::Changed,
                    content_hash: Some(0xDEAD_BEEF),
                    etag: Some("\"v2\""),
                },
            )
            .unwrap());
        w.commit().unwrap();
    }
    // …and an unchanged check follows, carrying no hash and no validator of its
    // own. Both land in the same uncompacted overlay, where touches fold
    // last-wins, so the second op has to carry the first's observations forward.
    {
        let mut w = store.writer().unwrap();
        assert!(w
            .touch(
                key,
                Touch {
                    checked_at_ms: 3_000,
                    outcome: Outcome::Unchanged,
                    content_hash: None,
                    etag: None,
                },
            )
            .unwrap());
        w.commit().unwrap();
    }

    let snap = store.snapshot();
    let n = snap.node(key).unwrap();
    assert_eq!(
        n.content_hash, 0xDEAD_BEEF,
        "the observed hash must survive a later unchanged check"
    );
    assert_eq!(n.etag, Some("\"v2\""), "the observed etag must survive a later unchanged check");
    assert_eq!(n.checks, 2, "both observations must be counted");
    assert_eq!(n.changes, 1);
}

/// Not a fixed defect -- a constraint worth pinning, found while writing the
/// test above.
///
/// `touch` computes the node's next freshness state from the *committed* store,
/// so a node that exists only as a staged upsert is not yet visible to it and
/// the touch is refused (`Ok(false)`) rather than silently recording freshness
/// against a document that may never be committed. Callers must commit the
/// upsert first; the link-r bridge already does.
///
/// The same rule means two touches of one key inside a single uncommitted batch
/// do not accumulate -- the second is computed from pre-batch state and wins.
/// Threading one snapshot through the writer's staging batch would remove this
/// sharp edge; it is deferred rather than changed under a release.
#[test]
fn touch_requires_a_committed_node() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), Config::default()).unwrap();
    let url = "https://x.dev/fresh";
    let key = UrlKey::of(url);

    let mut w = store.writer().unwrap();
    w.upsert_node(&doc(url, 1, false)).unwrap(); // staged, not committed
    let recorded = w
        .touch(
            key,
            Touch {
                checked_at_ms: 2_000,
                outcome: Outcome::Unchanged,
                content_hash: None,
                etag: None,
            },
        )
        .unwrap();
    assert!(!recorded, "a touch against an uncommitted node must report that it did nothing");
}

/// Found by the model proptest during development.
///
/// A `Remove` discards a document's history. A later upsert of the same URL is a
/// *new* document that happens to reuse the address — it must not silently
/// inherit the removed node's edges, segments, pin, or counters from a base
/// generation that has not been compacted away yet.
#[test]
fn a_remove_severs_base_history_for_a_resurrected_key() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), Config::default()).unwrap();
    let url = "https://x.dev/a";
    let key = UrlKey::of(url);

    {
        let mut w = store.writer().unwrap();
        w.upsert_node(&doc(url, 1, true)).unwrap();
        w.set_segments(
            key,
            &[SegmentRecord { label: "Old", byte_range: None, depth: 1, importance: 5 }],
        )
        .unwrap();
        w.set_edges(key, &[(UrlKey::of("https://x.dev/other"), EdgeType::Link, 65_535)]);
        w.touch(
            key,
            Touch {
                checked_at_ms: 2_000,
                outcome: Outcome::Changed,
                content_hash: Some(2),
                etag: None,
            },
        )
        .unwrap();
        w.commit().unwrap();
    }
    store.compact().unwrap(); // history is now in the base generation

    {
        let mut w = store.writer().unwrap();
        w.remove(key).unwrap();
        w.upsert_node(&doc(url, 99, false)).unwrap(); // resurrect, same address
        w.commit().unwrap();
    }

    let snap = store.snapshot();
    let n = snap.node(key).unwrap();
    assert_eq!(n.content_hash, 99, "the resurrected document is the new one");
    assert!(!n.pinned(), "a removed node's pin must not be inherited");
    assert_eq!(n.checks, 0, "a removed node's counters must not be inherited");
    assert_eq!(n.changes, 0);
    assert!(snap.segments(key).is_empty(), "a removed node's segments must not be inherited");
    let mut it = snap.neighbors(key);
    assert!(
        graph_r::traverse::LendingIterator::next(&mut it).is_none(),
        "a removed node's edges must not be inherited"
    );
}

// ---- decoder hardening ----------------------------------------------------

/// Found by fuzzing `decode_postings`.
///
/// Postings are ascending node ids stored as deltas. The accumulator added the
/// delta without checking, so a crafted second delta of `u64::MAX` wrapped and
/// produced a descending, in-bounds-looking id — corrupting a query's candidate
/// set from a file that passed every checksum. Also pinned as a unit test beside
/// the decoder in `src/format/base.rs`.
#[test]
fn a_hostile_postings_delta_cannot_wrap_the_accumulator() {
    let mut hostile = Vec::new();
    graph_r::bytesio::put_varint(&mut hostile, 1);
    graph_r::bytesio::put_varint(&mut hostile, u64::MAX);
    assert!(graph_r::format::base::decode_postings(&hostile).is_err());
}

// ---- determinism and growth -----------------------------------------------

/// The headline anti-bloat assertion, and the reason compaction is specified as
/// byte-reproducible rather than merely correct.
///
/// Re-compacting unchanged state must produce a byte-identical base. If it does
/// not, every no-op refresh cycle rewrites the file, a content-addressed cache
/// of it misses forever, and the store grows without the graph changing. Fifty
/// rounds is far past any plausible one-off nondeterminism (hash iteration
/// order, timestamps, allocation addresses).
#[test]
fn fifty_no_op_compactions_are_byte_identical() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), Config::default()).unwrap();
    {
        let mut w = store.writer().unwrap();
        for i in 0..32u64 {
            let url = format!("https://x.dev/p{i}");
            w.upsert_node(&doc(&url, i, i % 4 == 0)).unwrap();
            w.set_segments(
                UrlKey::of(&url),
                &[SegmentRecord {
                    label: "Install > Linux",
                    byte_range: Some((0, 10)),
                    depth: 1,
                    importance: 100,
                }],
            )
            .unwrap();
            let targets: Vec<(UrlKey, EdgeType, u16)> = (0..4u64)
                .map(|j| {
                    (
                        UrlKey::of(&format!("https://x.dev/p{}", (i + j + 1) % 32)),
                        EdgeType::Link,
                        65_535,
                    )
                })
                .collect();
            w.set_edges(UrlKey::of(&url), &targets);
        }
        w.commit().unwrap();
    }
    store.compact().unwrap();

    let base_path = dir.path().join("graph.base");
    let first = std::fs::read(&base_path).unwrap();
    assert!(!first.is_empty());

    // Exactly three things may legitimately differ between two compactions of
    // identical state, and all three are the generation counter or a checksum
    // covering it:
    //   - byte 32..40   the `generation` field, which must increment
    //   - byte 120..128 the header checksum, which covers `generation`
    //   - the last 8 bytes, the whole-file trailer checksum
    // Everything in between -- the section directory (including every per-section
    // content checksum) and every section payload -- must be identical. That is
    // the property that matters: no content drift means no growth, and a
    // content-addressed cache of the sections stays valid across a no-op sync.
    const HEADER_LEN: usize = 128;
    const TRAILER_LEN: usize = 8;
    let body = |b: &[u8]| b[HEADER_LEN..b.len() - TRAILER_LEN].to_vec();
    let first_body = body(&first);

    for round in 0..50 {
        store.compact().unwrap();
        let again = std::fs::read(&base_path).unwrap();
        assert_eq!(
            again.len(),
            first.len(),
            "round {round}: base size drifted, so the store grows on no-op syncs"
        );
        assert_eq!(
            body(&again),
            first_body,
            "round {round}: recompacting unchanged state produced different section bytes"
        );
    }
}

/// A no-op compaction must not invent work either: the live document count and
/// every document's identity stay put across repeated compaction.
#[test]
fn repeated_compaction_preserves_the_live_set() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), Config::default()).unwrap();
    {
        let mut w = store.writer().unwrap();
        for i in 0..8u64 {
            let url = format!("https://x.dev/p{i}");
            w.upsert_node(&doc(&url, i, false)).unwrap();
        }
        w.commit().unwrap();
    }
    store.compact().unwrap();
    let expected = store.snapshot().len();
    assert_eq!(expected, 8);

    for _ in 0..10 {
        store.compact().unwrap();
        assert_eq!(store.snapshot().len(), expected);
    }
}
