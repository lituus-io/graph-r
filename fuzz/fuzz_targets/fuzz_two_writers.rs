// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! Two writable handles on one store directory, driven by an arbitrary
//! schedule of commits, multi-op transactions, removals, compactions,
//! aborted transactions, handle restarts and crashes (a strict prefix of a
//! real multi-op record left at the end of the log, as a writer killed
//! mid-append leaves it). After every step a fresh read-only open must show
//! exactly the committed documents: no committed op is ever lost, nothing
//! uncommitted ever appears, and a transaction is all or none.

#![no_main]
use graph_r::format::wal::RecordBuf;
use graph_r::{Config, DocRecord, Store, UrlKey};
use std::collections::BTreeSet;
use std::io::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};

fn url(k: u8) -> String {
    format!("https://f.dev/{}", k % 16)
}

fn doc(url: &str) -> DocRecord<'_> {
    DocRecord {
        url,
        url_key: UrlKey::of(url),
        content_hash: 1,
        fetched_at_ms: 1,
        title: None,
        snippet: None,
        etag: None,
        pinned: false,
    }
}

fn live(store: &Store) -> BTreeSet<String> {
    let mut v = BTreeSet::new();
    store.snapshot().for_each_live(|n| {
        v.insert(n.url.to_owned());
    });
    v
}

static RUN: AtomicU64 = AtomicU64::new(0);

libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    let dir = std::env::temp_dir().join(format!(
        "graph-r-fuzz-two-writers-{}-{}",
        std::process::id(),
        RUN.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let cfg = Config { compact_after_ops: 8, ..Config::default() };
    let mut handles = [Some(Store::create(&dir, cfg).unwrap()), None];
    handles[1] = Some(Store::open_cfg(&dir, cfg).unwrap());
    let mut model: BTreeSet<String> = BTreeSet::new();
    for pair in data.chunks(2).take(64) {
        let (op, arg) = (pair[0], pair.get(1).copied().unwrap_or(0));
        let h = usize::from(op & 1);
        let store = handles[h].as_ref().unwrap();
        match (op >> 1) % 7 {
            0 => {
                let u = url(arg);
                store.transact(|tx| tx.upsert_node(&doc(&u))).unwrap();
                model.insert(u);
            }
            1 => {
                let u = url(arg);
                let mut w = store.writer().unwrap();
                w.remove(UrlKey::of(&u)).unwrap();
                w.commit().unwrap();
                model.remove(&u);
            }
            2 => {
                store.compact().unwrap();
            }
            3 => {
                let u = url(arg);
                let r: graph_r::Result<()> = store.transact(|tx| {
                    tx.upsert_node(&doc(&u))?;
                    Err(graph_r::Error::format("abort"))
                });
                assert!(r.is_err());
            }
            4 => {
                // A crash mid-append: a strict prefix of a real record of
                // two or three ops, the sequence it would have had.
                let raw = std::fs::read(dir.join(graph_r::store::WAL_FILE)).unwrap();
                let next = graph_r::format::wal::replay(&raw)
                    .ok()
                    .and_then(|r| r.records().last().map(|r| r.last_seq() + 1))
                    .unwrap_or(1);
                let mut rec = RecordBuf::new();
                for i in 0..2 + arg % 2 {
                    rec.push_upsert(&doc(&url(arg.wrapping_add(i))), 0).unwrap();
                }
                rec.seal(next.max(1)).unwrap();
                let frame = rec.into_vec();
                let cut = 1 + usize::from(arg) % (frame.len() - 1);
                let mut f = std::fs::OpenOptions::new()
                    .append(true)
                    .open(dir.join(graph_r::store::WAL_FILE))
                    .unwrap();
                f.write_all(&frame[..cut]).unwrap();
            }
            5 => {
                // A transaction of two or three ops: all of it, or none.
                let urls: Vec<String> =
                    (0..2 + arg % 2).map(|i| url(arg.wrapping_mul(3).wrapping_add(i))).collect();
                let remove_first = arg & 0x80 != 0;
                store
                    .transact(|tx| {
                        if remove_first {
                            tx.remove(UrlKey::of(&urls[0]))?;
                        }
                        urls.iter()
                            .skip(usize::from(remove_first))
                            .try_for_each(|u| tx.upsert_node(&doc(u)))
                    })
                    .unwrap();
                if remove_first {
                    model.remove(&urls[0]);
                }
                for u in urls.iter().skip(usize::from(remove_first)) {
                    model.insert(u.clone());
                }
            }
            _ => {
                // The process holding this handle restarts.
                handles[h] = None;
                handles[h] = Some(Store::open_cfg(&dir, cfg).unwrap());
            }
        }
        let fresh = Store::open_read_only(&dir).unwrap();
        assert_eq!(live(&fresh), model, "a fresh open disagrees with what was committed");
    }
    for store in handles.iter().flatten() {
        store.reload_if_stale().unwrap();
        assert_eq!(live(store), model, "a handle did not converge");
    }
    drop(handles);
    let _ = std::fs::remove_dir_all(&dir);
});
