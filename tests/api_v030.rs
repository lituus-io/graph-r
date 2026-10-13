// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
//! The additive 0.3 surface: accessors, off-path compaction, cheap staleness
//! checks, WAL measurement helpers, and temp-file hygiene.

#![cfg(feature = "store")] // the store is what these exercise
use graph_r::format::wal;
use graph_r::prelude::*;
use graph_r::store::{BASE_FILE, LOCK_FILE, WAL_FILE};

fn doc(url: &str, hash: u64) -> DocRecord<'_> {
    DocRecord {
        url,
        url_key: UrlKey::of(url),
        content_hash: hash,
        fetched_at_ms: 1,
        title: Some("t"),
        snippet: None,
        etag: None,
        pinned: false,
    }
}

fn write(store: &Store, url: &str, hash: u64) {
    let mut w = store.writer().unwrap();
    w.upsert_node(&doc(url, hash)).unwrap();
    w.commit().unwrap();
}

#[test]
fn accessors_report_directory_and_generation() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), Config::default()).unwrap();
    assert_eq!(store.dir(), dir.path());
    assert_eq!(store.generation(), 1);
    write(&store, "https://x.dev/a", 1);
    store.compact().unwrap();
    assert_eq!(store.generation(), 2);
    assert_eq!(store.generation(), store.snapshot().generation());
    for f in [BASE_FILE, WAL_FILE, LOCK_FILE] {
        assert!(dir.path().join(f).exists(), "{f}");
    }
}

#[test]
fn compaction_can_move_off_the_commit_path() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = Config { compact_after_ops: 3, ..Config::default() };
    let store = Store::create(dir.path(), cfg).unwrap();
    store.set_compact_inline(false);
    for i in 0..10 {
        write(&store, &format!("https://x.dev/{i}"), i);
    }
    assert_eq!(store.generation(), 1, "no inline compaction");
    assert!(store.compaction_due());
    store.compact().unwrap();
    assert!(!store.compaction_due());
    assert_eq!(store.generation(), 2);
    assert_eq!(store.len(), 10);

    // Back on: the default behaviour returns.
    store.set_compact_inline(true);
    for i in 10..13 {
        write(&store, &format!("https://x.dev/{i}"), i);
    }
    assert_eq!(store.generation(), 3);
    assert!(!store.compaction_due());
}

#[test]
fn inline_compaction_is_still_the_default() {
    let dir = tempfile::tempdir().unwrap();
    let store =
        Store::create(dir.path(), Config { compact_after_ops: 2, ..Config::default() }).unwrap();
    write(&store, "https://x.dev/a", 1);
    write(&store, "https://x.dev/b", 2);
    assert_eq!(store.generation(), 2);
    assert!(!store.compaction_due());
}

#[test]
fn reload_sees_wal_growth_compaction_and_nothing_else() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), Config::default()).unwrap();
    let reader = Store::open_read_only(dir.path()).unwrap();
    for _ in 0..3 {
        assert!(!reader.reload_if_stale().unwrap(), "unchanged files: no reload");
    }
    write(&store, "https://x.dev/a", 1);
    assert!(reader.reload_if_stale().unwrap(), "WAL growth");
    assert_eq!(reader.snapshot().len(), 1);
    assert!(!reader.reload_if_stale().unwrap());
    write(&store, "https://x.dev/b", 2);
    write(&store, "https://x.dev/c", 3);
    assert!(reader.reload_if_stale().unwrap());
    assert_eq!(reader.snapshot().len(), 3);
    store.compact().unwrap();
    assert!(reader.reload_if_stale().unwrap(), "new generation");
    assert_eq!(reader.generation(), 2);
    assert_eq!(reader.snapshot().len(), 3);
    write(&store, "https://x.dev/d", 4);
    assert!(reader.reload_if_stale().unwrap(), "WAL growth after a compaction");
    assert_eq!(reader.snapshot().len(), 4);
    assert!(!reader.reload_if_stale().unwrap());
}

#[test]
fn committed_len_is_what_a_copy_may_take() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), Config::default()).unwrap();
    write(&store, "https://x.dev/a", 1);
    write(&store, "https://x.dev/b", 2);
    let image = std::fs::read(dir.path().join(WAL_FILE)).unwrap();
    assert_eq!(wal::committed_len(&image), image.len());
    // A torn append is excluded; the prefix replays to the same ops.
    let mut torn = image.clone();
    torn.extend_from_slice(&[1, 2, 3, 4, 5, 6, 7]);
    assert_eq!(wal::committed_len(&torn), image.len());
    assert_eq!(wal::committed_len(&image[..10]), 0, "no header, nothing committed");
    let first = wal::HEADER_LEN + wal::committed_frames_len(&image[wal::HEADER_LEN..]);
    assert_eq!(first, image.len());
}

#[test]
fn stale_temporaries_do_not_block_writes() {
    // A crash can leave `.graph.base.<pid>.tmp`-style files behind; they are
    // never reused, so compaction keeps working.
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), Config::default()).unwrap();
    std::fs::write(dir.path().join(format!(".graph.base.{}.0.tmp", std::process::id())), b"junk")
        .unwrap();
    std::fs::write(dir.path().join(format!(".graph.base.{}.tmp", std::process::id())), b"junk")
        .unwrap();
    write(&store, "https://x.dev/a", 1);
    store.compact().unwrap();
    store.compact().unwrap();
    assert_eq!(store.len(), 1);
}

#[test]
fn many_stores_in_one_process_compact_concurrently() {
    let dirs: Vec<_> = (0..8).map(|_| tempfile::tempdir().unwrap()).collect();
    std::thread::scope(|s| {
        for d in &dirs {
            s.spawn(move || {
                let store = Store::create(d.path(), Config::default()).unwrap();
                for i in 0..20 {
                    write(&store, &format!("https://x.dev/{i}"), i);
                    store.compact().unwrap();
                }
                assert_eq!(store.len(), 20);
            });
        }
    });
}

#[test]
fn an_image_store_serves_from_memory_and_follows_the_writer() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), Config::default()).unwrap();
    write(&store, "https://x.dev/a", 1);
    let read = |f: &str| std::fs::read(dir.path().join(f)).unwrap();
    let base = bytes::Bytes::from(read(BASE_FILE));
    let image = Store::open_image(base.clone(), &read(WAL_FILE)).unwrap();
    assert!(image.is_image() && image.dir().as_os_str().is_empty());
    assert_eq!(image.snapshot().len(), 1);
    assert!(matches!(image.writer(), Err(Error::ReadOnly)));
    assert!(!image.reload_if_stale().unwrap(), "no files to watch");

    // New frames, same generation.
    write(&store, "https://x.dev/b", 2);
    assert!(image.reload_image(None, &read(WAL_FILE)).unwrap());
    assert!(!image.reload_image(None, &read(WAL_FILE)).unwrap(), "nothing new");
    assert_eq!(image.snapshot().len(), 2);

    // A compaction: the log alone cannot be applied; with the new base it can.
    store.compact().unwrap();
    write(&store, "https://x.dev/c", 3);
    assert!(image.reload_image(None, &read(WAL_FILE)).is_err());
    assert!(image
        .reload_image(Some(bytes::Bytes::from(read(BASE_FILE))), &read(WAL_FILE))
        .unwrap());
    assert_eq!(image.generation(), 2);
    assert_eq!(image.snapshot().len(), 3);
    let q = image.snapshot().query("t", &QueryOpts::default()).len();
    assert_eq!(q, store.snapshot().query("t", &QueryOpts::default()).len());
}

#[test]
fn image_stores_reject_hostile_bytes_and_read_any_alignment() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), Config::default()).unwrap();
    write(&store, "https://x.dev/a", 1);
    store.compact().unwrap();
    let base = std::fs::read(dir.path().join(BASE_FILE)).unwrap();
    let wal = std::fs::read(dir.path().join(WAL_FILE)).unwrap();
    for shift in 0..8 {
        let mut buf = vec![0u8; shift];
        buf.extend_from_slice(&base);
        let img = Store::open_image(bytes::Bytes::from(buf).slice(shift..), &wal).unwrap();
        assert_eq!(img.snapshot().len(), 1, "shift {shift}");
    }
    for at in (0..base.len()).step_by(7) {
        let mut bad = base.clone();
        bad[at] ^= 0x20;
        assert!(Store::open_image(bytes::Bytes::from(bad), &wal).is_err(), "flip at {at}");
    }
    let mut torn = wal.clone();
    torn.extend_from_slice(&[9, 9, 9]);
    assert_eq!(Store::open_image(bytes::Bytes::from(base), &torn).unwrap().snapshot().len(), 1);
}
