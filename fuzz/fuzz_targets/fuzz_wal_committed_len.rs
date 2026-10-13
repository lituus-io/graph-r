// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
//! Arbitrary bytes as a WAL image: `committed_len` must never panic and must
//! agree exactly with `replay`, which is what makes it safe to ship a prefix.

#![no_main]
libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    let n = graph_r::format::wal::committed_len(data);
    assert!(n <= data.len());
    match graph_r::format::wal::replay(data) {
        Ok(r) => assert_eq!(n, r.good_len),
        Err(_) => {}
    }
    if data.len() >= 64 {
        let f = graph_r::format::wal::committed_frames_len(&data[64..]);
        assert!(f <= data.len() - 64);
    }
});
