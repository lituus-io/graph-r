// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>
//
//! Arbitrary bytes as a `graph.base` file: must validate or reject with a
//! typed error — never panic, never over-allocate.

#![no_main]
libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    let _ = graph_r::format::base::validate(data);
});
