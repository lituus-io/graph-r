// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>
//
//! Arbitrary bytes as a single WAL op payload: typed error or clean decode.

#![no_main]
libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    let mut r = graph_r::bytesio::Reader::new(data);
    let _ = graph_r::format::wal::Op::decode(&mut r);
});
