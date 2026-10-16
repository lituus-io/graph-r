// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! Shared by the log-image targets: arbitrary bytes made into a log whose
//! records all pass their checksum, so the fuzzer explores the rules that
//! follow it (op layouts, counts, sequence order) rather than stopping at
//! the first random 64-bit sum.

use graph_r::format::wal;
use graph_r::hash::Hasher;

/// A log header, then records cut from `data`: per record one byte of body
/// length (in 8-byte units), one of sequence step (0 regresses), one of op
/// count, then the body; each record's length, tag, flags and checksum set
/// so it reads as written.
pub fn sealed_image(data: &[u8]) -> Vec<u8> {
    let mut image = wal::encode_header(1, 0);
    let mut rest = data;
    let mut next_seq = 1u64;
    while let [len, step, count, tail @ ..] = rest {
        let body = (usize::from(*len) * 8).min(tail.len());
        let first =
            next_seq.saturating_add(u64::from(*step)).saturating_sub(1).max(u64::from(*step == 0));
        let ops = u32::from(*count % 8);
        let at = image.len();
        image.extend_from_slice(&[0; wal::RECORD_HEADER_LEN]);
        image.extend_from_slice(&tail[..body]);
        let rec = &mut image[at..];
        let n = rec.len() as u32;
        rec[..4].copy_from_slice(&n.to_le_bytes());
        rec[4..6].copy_from_slice(&0x5854u16.to_le_bytes());
        rec[6] = 1;
        rec[8..16].copy_from_slice(&first.to_le_bytes());
        rec[16..20].copy_from_slice(&ops.to_le_bytes());
        let mut h = Hasher::new();
        h.update(&rec[..24]);
        h.update(&rec[wal::RECORD_HEADER_LEN..]);
        let sum = h.digest64();
        rec[24..32].copy_from_slice(&sum.to_le_bytes());
        next_seq = first.saturating_add(u64::from(ops));
        rest = &tail[body..];
    }
    image
}
