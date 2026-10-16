// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! Arbitrary bytes as a WAL image (raw, and with every record re-sealed so
//! it passes its checksum): `committed_len` must never panic, must
//! agree exactly with `replay` (which is what makes it safe to ship a
//! prefix), and must equal the header plus `committed_frames_len` of the
//! rest, which is how a mirror measures a log it extends.

#![no_main]
#[path = "sealed.rs"]
mod sealed;

use graph_r::format::wal;

libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    check(data);
    check(&sealed::sealed_image(data));
});

fn check(data: &[u8]) {
    let n = wal::committed_len(data);
    assert!(n <= data.len());
    if let Ok(r) = wal::replay(data) {
        assert_eq!(n, r.good_len, "committed_len disagrees with replay");
    }
    if data.len() >= wal::HEADER_LEN {
        let f = wal::committed_frames_len(&data[wal::HEADER_LEN..]);
        assert!(f <= data.len() - wal::HEADER_LEN);
        if wal::parse_header(data).is_ok() {
            assert_eq!(wal::HEADER_LEN + f, n, "the two measures of one log disagree");
        }
        // A committed prefix is itself wholly committed.
        if n >= wal::HEADER_LEN {
            assert_eq!(wal::committed_len(&data[..n]), n);
        }
    }
}
