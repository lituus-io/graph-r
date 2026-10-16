// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! Arbitrary bytes as a single log op: typed error or a clean view.

#![no_main]
libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    let _ = graph_r::format::wal::validate_op(data);
});
