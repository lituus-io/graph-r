// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! A scripted run of transactions (one to four upserts or removals each),
//! then a crash inside the last record: the log cut at any byte of it, or a
//! bit flipped anywhere in it. However it is damaged, a fresh open shows
//! exactly the earlier transactions — the damaged one absent whole — and
//! the next commit lands after them.

#![no_main]
use graph_r::{Config, DocRecord, Durability, Store, UrlKey};
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};

fn url(k: u8) -> String {
    format!("https://c.dev/{}", k % 24)
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
    let Some((&damage, script)) = data.split_first() else { return };
    let dir = std::env::temp_dir().join(format!(
        "graph-r-fuzz-crash-cut-{}-{}",
        std::process::id(),
        RUN.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    // The damage is applied to the file itself, so syncing it adds nothing.
    let cfg = Config { durability: Durability::Never, ..Config::default() };
    let store = Store::create(&dir, cfg).unwrap();
    let wal = dir.join(graph_r::store::WAL_FILE);
    let mut model: BTreeSet<String> = BTreeSet::new();
    let mut before_last = model.clone();
    let mut last_start = 0u64;
    for txn in script.chunks(5).take(16) {
        let n = 1 + usize::from(txn[0] % 4);
        let ops: Vec<(bool, String)> =
            txn[1..].iter().take(n).map(|&b| (b & 0x80 != 0, url(b))).collect();
        if ops.is_empty() {
            continue;
        }
        before_last = model.clone();
        last_start = std::fs::metadata(&wal).unwrap().len();
        store
            .transact(|tx| {
                ops.iter().try_for_each(|(remove, u)| {
                    if *remove {
                        tx.remove(UrlKey::of(u))
                    } else {
                        tx.upsert_node(&doc(u))
                    }
                })
            })
            .unwrap();
        for (remove, u) in &ops {
            if *remove {
                model.remove(u);
            } else {
                model.insert(u.clone());
            }
        }
    }
    drop(store);
    let mut bytes = std::fs::read(&wal).unwrap();
    let start = last_start as usize;
    if start >= graph_r::format::wal::HEADER_LEN && start < bytes.len() {
        let span = bytes.len() - start;
        let at = start + usize::from(damage) * span / 256;
        if damage & 1 == 0 {
            bytes.truncate(at.max(start)); // cut inside the last record
        } else {
            let i = at.min(bytes.len() - 1);
            bytes[i] ^= 1 << (damage % 8); // flip one bit in it
        }
        std::fs::write(&wal, &bytes).unwrap();
        model = before_last;
    }
    assert_eq!(live(&Store::open_read_only(&dir).unwrap()), model, "read-only open");
    let store = Store::open(&dir).unwrap();
    assert_eq!(live(&store), model, "writable open");
    store.transact(|tx| tx.upsert_node(&doc("https://c.dev/after"))).unwrap();
    model.insert("https://c.dev/after".into());
    drop(store);
    assert_eq!(live(&Store::open_read_only(&dir).unwrap()), model, "after the next commit");
    let _ = std::fs::remove_dir_all(&dir);
});
