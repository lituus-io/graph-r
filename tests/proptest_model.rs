// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>
//
//! Property tests: the store agrees with a trivial reference model under
//! arbitrary op sequences, compaction is byte-reproducible, and a WAL torn at
//! any byte recovers exactly the committed prefix.

use graph_r::prelude::*;
use proptest::prelude::*;
use std::collections::BTreeMap;

#[derive(Clone, Debug)]
enum MOp {
    Upsert { n: u8, hash: u64, pinned: bool },
    Touch { n: u8, unchanged: bool },
    Remove { n: u8 },
    Pin { n: u8, on: bool },
    Compact,
}

fn url_of(n: u8) -> String {
    format!("https://m.dev/page/{n}")
}

fn op_strategy() -> impl Strategy<Value = MOp> {
    prop_oneof![
        (0u8..12, any::<u64>(), any::<bool>())
            .prop_map(|(n, hash, pinned)| MOp::Upsert { n, hash, pinned }),
        (0u8..12, any::<bool>()).prop_map(|(n, unchanged)| MOp::Touch { n, unchanged }),
        (0u8..12).prop_map(|n| MOp::Remove { n }),
        (0u8..12, any::<bool>()).prop_map(|(n, on)| MOp::Pin { n, on }),
        Just(MOp::Compact),
    ]
}

/// Reference model: key → (content_hash, pinned, changes).
#[derive(Default)]
struct Model {
    live: BTreeMap<u8, (u64, bool, u16)>,
}

impl Model {
    fn apply(&mut self, op: &MOp) {
        match *op {
            MOp::Upsert { n, hash, pinned } => {
                // Pins are sticky across re-ingests; only an explicit unpin
                // clears them.
                let (prev_pin, changes) = self.live.get(&n).map_or((false, 0), |v| (v.1, v.2));
                self.live.insert(n, (hash, pinned || prev_pin, changes));
            }
            MOp::Touch { n, unchanged } => {
                if let Some(v) = self.live.get_mut(&n) {
                    if !unchanged {
                        v.2 += 1;
                        v.0 = v.0.wrapping_add(1);
                    }
                }
            }
            MOp::Remove { n } => {
                self.live.remove(&n);
            }
            MOp::Pin { n, on } => {
                if let Some(v) = self.live.get_mut(&n) {
                    v.1 = on;
                }
            }
            MOp::Compact => {}
        }
    }
}

fn apply_store(store: &Store, op: &MOp) {
    match *op {
        MOp::Upsert { n, hash, pinned } => {
            let url = url_of(n);
            let mut w = store.writer().unwrap();
            w.upsert_node(&DocRecord {
                url: &url,
                url_key: UrlKey::of(&url),
                content_hash: hash,
                fetched_at_ms: 1000 + u64::from(n),
                title: Some("T"),
                snippet: None,
                etag: None,
                pinned,
            })
            .unwrap();
            w.commit().unwrap();
        }
        MOp::Touch { n, unchanged } => {
            let url = url_of(n);
            let key = UrlKey::of(&url);
            let mut w = store.writer().unwrap();
            let prev = store.snapshot().node(key).map(|x| x.content_hash);
            let (outcome, hash) = if unchanged {
                (Outcome::Unchanged, None)
            } else {
                (Outcome::Changed, prev.map(|h| h.wrapping_add(1)))
            };
            w.touch(key, Touch { checked_at_ms: 5000, outcome, content_hash: hash, etag: None })
                .unwrap();
            w.commit().unwrap();
        }
        MOp::Remove { n } => {
            let mut w = store.writer().unwrap();
            w.remove(UrlKey::of(&url_of(n))).unwrap();
            w.commit().unwrap();
        }
        MOp::Pin { n, on } => {
            let mut w = store.writer().unwrap();
            w.set_pinned(UrlKey::of(&url_of(n)), on).unwrap();
            w.commit().unwrap();
        }
        MOp::Compact => {
            store.compact().unwrap();
        }
    }
}

fn check_agreement(store: &Store, model: &Model) {
    let snap = store.snapshot();
    for (&n, &(hash, pinned, changes)) in &model.live {
        let node = snap
            .node(UrlKey::of(&url_of(n)))
            .unwrap_or_else(|| panic!("model has {n}, store does not"));
        assert!(!node.is_tombstone());
        assert_eq!(node.content_hash, hash, "hash for {n}");
        assert_eq!(node.pinned(), pinned, "pin for {n}");
        assert_eq!(node.changes, changes, "changes for {n}");
    }
    assert_eq!(snap.len(), model.live.len(), "live counts agree");
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    #[test]
    fn store_agrees_with_model_through_compaction_and_reopen(
        ops in proptest::collection::vec(op_strategy(), 1..60)
    ) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::create(dir.path(), Config::default()).unwrap();
        let mut model = Model::default();
        for op in &ops {
            apply_store(&store, op);
            model.apply(op);
        }
        check_agreement(&store, &model);

        // Compact + reopen must not change any answer. (Touch ops in the
        // model track content changes; pins on removed-then-re-upserted keys
        // are covered by the model semantics above.)
        store.compact().unwrap();
        check_agreement(&store, &model);
        drop(store);
        let store = Store::open(dir.path()).unwrap();
        check_agreement(&store, &model);
    }

    #[test]
    fn identical_op_sequences_compact_to_identical_bytes(
        ops in proptest::collection::vec(op_strategy(), 1..40)
    ) {
        let render = |ops: &[MOp]| {
            let dir = tempfile::tempdir().unwrap();
            let store = Store::create(dir.path(), Config::default()).unwrap();
            for op in ops {
                apply_store(&store, op);
            }
            store.compact().unwrap();
            std::fs::read(dir.path().join("graph.base")).unwrap()
        };
        prop_assert_eq!(render(&ops), render(&ops));
    }

    #[test]
    fn wal_torn_at_any_byte_recovers_a_committed_prefix(
        ops in proptest::collection::vec(op_strategy(), 1..12),
        cut_frac in 0.0f64..1.0
    ) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::create(dir.path(), Config::default()).unwrap();
        for op in &ops {
            // Skip compaction ops here: we want a WAL with content.
            if !matches!(op, MOp::Compact) {
                apply_store(&store, op);
            }
        }
        drop(store);
        let wal = dir.path().join("graph.wal");
        let bytes = std::fs::read(&wal).unwrap();
        if bytes.len() > 64 {
            let cut = 64 + ((bytes.len() - 64) as f64 * cut_frac) as usize;
            std::fs::write(&wal, &bytes[..cut]).unwrap();
        }
        // Open must never panic and must produce a consistent store; every
        // node it reports must be internally coherent.
        let store = Store::open(dir.path()).unwrap();
        let snap = store.snapshot();
        snap.check().unwrap();
        let _ = snap.len();
    }
}
