// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! Arbitrary bytes as a postings run (LEB128 delta-encoded node ids).

#![no_main]
libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    let _ = graph_r::format::base::decode_postings(data);
});
