// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! Arbitrary bytes as a `graph.wal` image, raw and with every record
//! re-sealed: replay must stop at any tear, report corruption as a typed
//! error, and never panic or over-allocate — and every accessor of every
//! record and op it accepts must read in bounds.

#![no_main]
#[path = "sealed.rs"]
mod sealed;

use graph_r::format::wal::{self, OpView};

fn touch_all(op: OpView<'_>) -> usize {
    let mut n = op.key().0 as usize;
    match op {
        OpView::Upsert(u) => {
            n ^= u.url().len() ^ u.content_hash() as usize ^ u.fetched_at_ms() as usize;
            n ^= usize::from(u.flags());
            for s in [u.title(), u.snippet(), u.etag()].into_iter().flatten() {
                n ^= s.len();
            }
        }
        OpView::Segs(s) => {
            n ^= s.len();
            for seg in s.iter() {
                n ^= seg.label.len() ^ seg.byte_start as usize ^ seg.byte_len as usize;
                n ^= usize::from(seg.importance) ^ usize::from(seg.depth) ^ usize::from(seg.flags);
            }
        }
        OpView::Edges(e) => {
            for (dst, t, f, w) in e.iter() {
                n ^= dst.0 as usize ^ usize::from(t) ^ usize::from(f) ^ usize::from(w);
            }
            n ^= e.get(e.len()).is_some() as usize;
        }
        OpView::Touch(t) => {
            n ^= t.checked_at_ms() as usize ^ t.content_hash().unwrap_or(0) as usize;
            n ^= t.last_change_ms() as usize ^ t.interval_s() as usize;
            n ^= usize::from(t.checks()) ^ usize::from(t.changes()) ^ usize::from(t.outcome());
            n ^= t.etag().map_or(0, str::len) ^ usize::from(t.tombstone());
        }
        OpView::Remove(_) | OpView::Pin(_, _) => {}
        OpView::Attrs(a) => {
            for (k, v) in a.iter() {
                n ^= usize::from(k) ^ v.len();
            }
        }
    }
    n
}

libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    check(data);
    check(&sealed::sealed_image(data));
});

fn check(data: &[u8]) {
    let Ok(replayed) = wal::replay(data) else { return };
    assert!(replayed.good_len <= data.len());
    let mut last = 0;
    let mut sink = 0usize;
    for r in replayed.records() {
        assert!(r.first_seq() > last, "records out of sequence");
        assert!(r.last_seq() >= r.first_seq());
        last = r.last_seq();
        let mut count = 0;
        for (seq, _, op) in r.ops() {
            assert_eq!(seq, r.first_seq() + count);
            sink ^= touch_all(op);
            count += 1;
        }
        assert_eq!(count, u64::from(r.op_count()));
    }
    std::hint::black_box(sink);
}
