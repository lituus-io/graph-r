// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>
//
//! Arbitrary bytes as a (base image, log image) pair for `Store::open_image`:
//! a typed error or a fully usable read-only store — never a panic — and an
//! accepted store answers queries and passes its own consistency check.

#![no_main]
libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    let Some((&split, rest)) = data.split_first() else { return };
    let cut = (usize::from(split) * rest.len()) / 255;
    let (base, wal) = rest.split_at(cut.min(rest.len()));
    if let Ok(store) = graph_r::Store::open_image(bytes::Bytes::copy_from_slice(base), wal) {
        let snap = store.snapshot();
        let _ = snap.len();
        let _ = snap.query("a b", &graph_r::QueryOpts::default());
        let _ = snap.check();
        let _ = store.reload_image(None, wal);
    }
});
