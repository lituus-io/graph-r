// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! Arbitrary bytes as a single log op: a typed error, or a view whose every
//! accessor reads in bounds.

#![no_main]
use graph_r::format::wal::{self, OpView};

libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    let Ok(op) = wal::validate_op(data) else { return };
    let mut n = op.key().0;
    match op {
        OpView::Upsert(u) => {
            n ^= (u.url().len() + u.title().map_or(0, str::len)) as u64;
            n ^= (u.snippet().map_or(0, str::len) + u.etag().map_or(0, str::len)) as u64;
            n ^= u.content_hash() ^ u.fetched_at_ms() ^ u64::from(u.flags());
            assert!(!u.url().is_empty(), "an accepted upsert has a URL");
        }
        OpView::Segs(s) => {
            n ^= s.iter().map(|g| g.label.len() as u64 + u64::from(g.depth)).sum::<u64>()
        }
        OpView::Edges(e) => {
            let dsts: Vec<u64> = e.iter().map(|d| d.0 .0).collect();
            assert!(dsts.windows(2).all(|w| w[0] < w[1]), "accepted edges ascend");
            n ^= dsts.len() as u64;
        }
        OpView::Touch(t) => {
            n ^= t.checked_at_ms() ^ t.last_change_ms() ^ u64::from(t.interval_s());
            n ^= t.etag().map_or(0, |e| e.len() as u64) ^ t.content_hash().unwrap_or(0);
        }
        OpView::Remove(_) | OpView::Pin(_, _) => {}
        OpView::Attrs(a) => {
            let keys: Vec<u8> = a.iter().map(|(k, v)| k ^ v.len() as u8).collect();
            n ^= keys.len() as u64;
        }
    }
    std::hint::black_box(n);
});
