// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>
//
//! Security tests: the untrusted-input boundary and the access-control
//! boundary, asserted deliberately rather than incidentally.
//!
//! graph-r reads two files it must treat as hostile — `graph.base` and
//! `graph.wal` — and accepts records from callers it does not control. The
//! invariant in both directions is the same: **a typed error, never a panic, an
//! over-allocation, or a silent truncation of meaning**. The fuzz targets prove
//! that property over random bytes; these tests pin the specific cases that
//! matter, so a regression is a named failure rather than a fuzz run that
//! happens to get unlucky.

use graph_r::format::{base, wal};
use graph_r::prelude::*;
use graph_r::writer::{MAX_EDGES, MAX_LABEL, MAX_SEGS};

fn seed(dir: &std::path::Path) -> Store {
    let store = Store::create(dir, Config::default()).unwrap();
    {
        let mut w = store.writer().unwrap();
        let url = "https://x.dev/a";
        w.upsert_node(&DocRecord {
            url,
            url_key: UrlKey::of(url),
            content_hash: 7,
            fetched_at_ms: 1,
            title: Some("A"),
            snippet: Some("body"),
            etag: Some("\"v1\""),
            pinned: false,
        })
        .unwrap();
        w.commit().unwrap();
    }
    store.compact().unwrap();
    store
}

// ---- access control -------------------------------------------------------

/// The cross-process writer lock is the only thing standing between two BLI-style
/// processes and a corrupted store. A second writer must be refused, not queued.
#[test]
fn second_writer_on_the_same_store_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let _first = seed(dir.path());
    match Store::open(dir.path()) {
        Err(Error::Locked) => {}
        Ok(_) => panic!("a second writer acquired the lock"),
        Err(e) => panic!("expected Error::Locked, got {e}"),
    }
}

/// A read-only handle must refuse every mutating entry point. This is what lets
/// a subprocess read a store while another process is writing it.
#[test]
fn read_only_store_refuses_every_mutation() {
    let dir = tempfile::tempdir().unwrap();
    let writer = seed(dir.path());
    drop(writer); // release the flock so the read-only open is the only handle

    let ro = Store::open_read_only(dir.path()).unwrap();
    assert!(matches!(ro.writer().err(), Some(Error::ReadOnly)), "writer() must refuse");
    assert!(matches!(ro.compact().err(), Some(Error::ReadOnly)), "compact() must refuse");

    // Reads still work: refusing writes must not refuse service.
    let snap = ro.snapshot();
    assert_eq!(snap.len(), 1);
}

/// A read-only open must not take the lock, or it would lock out the writer it
/// is meant to coexist with.
#[test]
fn read_only_open_does_not_take_the_writer_lock() {
    let dir = tempfile::tempdir().unwrap();
    let _writer = seed(dir.path()); // holds the lock for the whole test
    let ro = Store::open_read_only(dir.path()).expect("read-only must open under a live writer");
    assert_eq!(ro.snapshot().len(), 1);
}

// ---- hostile files --------------------------------------------------------

/// Every byte of the base file is covered by a checksum. Flipping any one of
/// them must be rejected at parse time, before a single record is read — that
/// whole-file validation is what makes the zero-copy accessors infallible.
#[test]
fn bit_flip_anywhere_in_the_base_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let store = seed(dir.path());
    drop(store);
    let good = std::fs::read(dir.path().join("graph.base")).unwrap();

    for i in 0..good.len() {
        let mut bad = good.clone();
        bad[i] ^= 0x01;
        assert!(base::validate(&bad).is_err(), "flipped byte {i} was accepted");
    }
}

/// Truncation at any offset must be a typed error, never a panic or a read past
/// the end of the mapping.
#[test]
fn truncation_at_any_offset_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let store = seed(dir.path());
    drop(store);
    let good = std::fs::read(dir.path().join("graph.base")).unwrap();

    for cut in 0..good.len() {
        assert!(base::validate(&good[..cut]).is_err(), "truncation at {cut} was accepted");
    }
}

/// Arbitrary bytes in place of a base file must not panic. This is the fuzz
/// invariant, pinned for a handful of shapes that have historically been
/// interesting: empty, all-zero, all-ones, and a valid header with a hostile
/// section count.
#[test]
fn arbitrary_bytes_as_a_base_file_never_panic() {
    for bytes in [Vec::new(), vec![0u8; 128], vec![0xffu8; 4096], b"GRPR".to_vec(), {
        let mut v = b"GRPR".to_vec();
        v.extend_from_slice(&u32::MAX.to_le_bytes());
        v.resize(1024, 0xab);
        v
    }] {
        // Must return, either way — the assertion is that it does not unwind.
        let _ = base::validate(&bytes);
    }
}

/// A torn WAL tail is the normal crash shape, not corruption: replay keeps the
/// intact prefix and the store opens. Trailing garbage must never be executed as
/// an op.
#[test]
fn torn_wal_tail_recovers_the_committed_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let store = seed(dir.path());
    {
        let mut w = store.writer().unwrap();
        let url = "https://x.dev/b";
        w.upsert_node(&DocRecord {
            url,
            url_key: UrlKey::of(url),
            content_hash: 9,
            fetched_at_ms: 2,
            title: Some("B"),
            snippet: None,
            etag: None,
            pinned: false,
        })
        .unwrap();
        w.commit().unwrap();
    }
    drop(store);

    // Append garbage, as a half-written frame would look after a crash.
    let wal_path = dir.path().join("graph.wal");
    let mut raw = std::fs::read(&wal_path).unwrap();
    let intact_len = raw.len();
    raw.extend_from_slice(&[0xde, 0xad, 0xbe, 0xef, 0x00, 0x01, 0x02]);
    std::fs::write(&wal_path, &raw).unwrap();

    // Replay stops at the tear and reports the intact length.
    let replayed = wal::replay(&std::fs::read(&wal_path).unwrap()).unwrap();
    assert_eq!(replayed.good_len, intact_len, "torn tail must not be replayed");

    // And the store opens, sees both documents, and repairs the file.
    let reopened = Store::open(dir.path()).unwrap();
    assert_eq!(reopened.snapshot().len(), 2);
    assert_eq!(
        std::fs::metadata(&wal_path).unwrap().len(),
        intact_len as u64,
        "a writable open must truncate the torn tail"
    );
}

/// A WAL whose sequence numbers go backwards is tampering, not a tear, and must
/// be reported as corruption rather than silently accepted.
#[test]
fn wal_sequence_regression_is_corruption() {
    let mut image = wal::encode_header(1, 0);
    image.extend_from_slice(&wal::encode_frame(5, &wal::Op::Remove { key: UrlKey(1) }));
    image.extend_from_slice(&wal::encode_frame(4, &wal::Op::Remove { key: UrlKey(2) }));
    assert!(matches!(wal::replay(&image), Err(Error::Corrupt { .. })));
}

// ---- resource exhaustion --------------------------------------------------

/// A hostile postings run must not be trusted to size an allocation. The
/// accumulator is checked, so a huge delta is an error rather than an overflow
/// or a multi-gigabyte `Vec`.
#[test]
fn hostile_postings_error_instead_of_over_allocating() {
    // A continuation-byte run with no terminator.
    assert!(base::decode_postings(&[0xff; 64]).is_err());

    // A second delta of u64::MAX must not wrap the accumulator. This is the
    // exact shape a fuzz run found; see the unit regression in format/base.rs.
    let mut hostile = Vec::new();
    graph_r::bytesio::put_varint(&mut hostile, 1);
    graph_r::bytesio::put_varint(&mut hostile, u64::MAX);
    assert!(base::decode_postings(&hostile).is_err());
}

/// Frame lengths are bounded before any allocation, so a hostile length field
/// cannot make replay reserve an arbitrary buffer.
#[test]
fn oversized_wal_frame_length_is_not_allocated() {
    let mut image = wal::encode_header(1, 0);
    // A frame claiming far more payload than MAX_FRAME, with no payload behind it.
    image.extend_from_slice(&u32::MAX.to_le_bytes()); // len
    image.extend_from_slice(&0u64.to_le_bytes()); // checksum
    image.extend_from_slice(&1u64.to_le_bytes()); // seq
    let replayed = wal::replay(&image).unwrap();
    assert!(replayed.ops.is_empty(), "an unbacked frame must not be replayed");
    assert_eq!(replayed.good_len, wal::HEADER_LEN);
}

/// Caller-supplied labels are bounded. An over-long URL, title, snippet, or etag
/// is refused with a typed error rather than being written to disk.
#[test]
fn oversized_labels_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), Config::default()).unwrap();
    let mut w = store.writer().unwrap();
    let huge = "x".repeat(MAX_LABEL + 1);

    let err = w
        .upsert_node(&DocRecord {
            url: &huge,
            url_key: UrlKey::of(&huge),
            content_hash: 0,
            fetched_at_ms: 0,
            title: None,
            snippet: None,
            etag: None,
            pinned: false,
        })
        .unwrap_err();
    assert!(matches!(err, Error::Format { .. }), "over-long url must be a format error");

    // An empty URL is equally invalid: it would produce a node indistinguishable
    // from an edge-target stub.
    let err = w
        .upsert_node(&DocRecord {
            url: "",
            url_key: UrlKey::of(""),
            content_hash: 0,
            fetched_at_ms: 0,
            title: None,
            snippet: None,
            etag: None,
            pinned: false,
        })
        .unwrap_err();
    assert!(matches!(err, Error::Format { .. }), "empty url must be a format error");
}

/// The per-document segment count is bounded, and exceeding it is an error —
/// not a silent drop, which would lose anchors without telling the caller.
#[test]
fn too_many_segments_is_an_error_not_a_silent_drop() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), Config::default()).unwrap();
    let mut w = store.writer().unwrap();
    let url = "https://x.dev/a";
    w.upsert_node(&DocRecord {
        url,
        url_key: UrlKey::of(url),
        content_hash: 0,
        fetched_at_ms: 0,
        title: None,
        snippet: None,
        etag: None,
        pinned: false,
    })
    .unwrap();

    let labels: Vec<String> = (0..=MAX_SEGS).map(|i| format!("H{i}")).collect();
    let segs: Vec<SegmentRecord<'_>> = labels
        .iter()
        .map(|l| SegmentRecord { label: l, byte_range: None, depth: 1, importance: 1 })
        .collect();
    assert!(matches!(w.set_segments(UrlKey::of(url), &segs), Err(Error::Format { .. })));
}

/// The outbound-edge set is capped rather than rejected — a hub page legitimately
/// links to hundreds of targets. The cap must hold, and it must keep the *first*
/// edges (document order is salience order), not an arbitrary subset.
#[test]
fn edge_count_is_capped_in_document_order() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), Config::default()).unwrap();
    let url = "https://x.dev/hub";
    let targets: Vec<String> = (0..MAX_EDGES * 4).map(|i| format!("https://x.dev/t{i}")).collect();
    {
        let mut w = store.writer().unwrap();
        w.upsert_node(&DocRecord {
            url,
            url_key: UrlKey::of(url),
            content_hash: 0,
            fetched_at_ms: 0,
            title: None,
            snippet: None,
            etag: None,
            pinned: false,
        })
        .unwrap();
        let edges: Vec<(UrlKey, EdgeType, u16)> =
            targets.iter().map(|t| (UrlKey::of(t), EdgeType::Link, 65_535)).collect();
        w.set_edges(UrlKey::of(url), &edges);
        w.commit().unwrap();
    }

    let snap = store.snapshot();
    let mut count = 0usize;
    let mut seen: Vec<u64> = Vec::new();
    let mut it = snap.neighbors(UrlKey::of(url));
    while let Some(e) = graph_r::traverse::LendingIterator::next(&mut it) {
        seen.push(e.dst_key.0);
        count += 1;
    }
    assert_eq!(count, MAX_EDGES, "edge set must be capped at MAX_EDGES");

    // Every retained edge must come from the first MAX_EDGES of the input, not
    // from a hash-random slice of the whole list.
    let kept_prefix: std::collections::HashSet<u64> =
        targets.iter().take(MAX_EDGES).map(|t| UrlKey::of(t).0).collect();
    assert!(seen.iter().all(|k| kept_prefix.contains(k)), "cap must keep the prominent prefix");
}

// ---- content containment --------------------------------------------------

/// graph-r's central promise is that it stores references, never bodies. The
/// record type has no body field, so the guarantee is structural — this test
/// pins the observable half: nothing beyond the compact metadata a caller passed
/// reaches the base file.
#[test]
fn only_supplied_metadata_reaches_the_base_file() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), Config::default()).unwrap();
    let url = "https://x.dev/page";
    {
        let mut w = store.writer().unwrap();
        w.upsert_node(&DocRecord {
            url,
            url_key: UrlKey::of(url),
            content_hash: 1,
            fetched_at_ms: 1,
            title: Some("Title"),
            snippet: Some("SNIPPET-MARKER"),
            etag: None,
            pinned: false,
        })
        .unwrap();
        w.set_segments(
            UrlKey::of(url),
            &[SegmentRecord {
                label: "Heading",
                byte_range: Some((0, 4096)),
                depth: 1,
                importance: 1,
            }],
        )
        .unwrap();
        w.commit().unwrap();
    }
    store.compact().unwrap();

    let bytes = std::fs::read(dir.path().join("graph.base")).unwrap();
    let haystack = String::from_utf8_lossy(&bytes);
    // What the caller supplied is present…
    assert!(haystack.contains(url));
    assert!(haystack.contains("SNIPPET-MARKER"));
    assert!(haystack.contains("Heading"));
    // …and the byte range is recorded as an offset, not as content: a segment
    // declaring 4096 bytes must not grow the file by anything like 4096 bytes.
    assert!(
        bytes.len() < 4096,
        "a 4 KiB byte_range must store an offset, not the region ({} bytes)",
        bytes.len()
    );
}
