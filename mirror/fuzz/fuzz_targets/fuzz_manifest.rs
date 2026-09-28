// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>
//
//! Arbitrary bytes as a remote `HEAD`: a clean parse or a typed error, and
//! anything accepted re-encodes to exactly the same bytes.

#![no_main]
libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    graph_r_mirror::fuzz_manifest(data);
});
