// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
//! Arbitrary bytes as a `graph.wal` image: replay must stop at any tear and
//! never panic or over-allocate.

#![no_main]
libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    let _ = graph_r::format::wal::replay(data);
});
