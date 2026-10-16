// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! One record, three ways, never a panic and (run with
//! `-malloc_limit_mb=64`) never an allocation sized by a length field:
//!
//! - arbitrary bytes decoded at a fuzzed offset: `Torn`, a typed `Corrupt`,
//!   or a record whose ops all read in bounds;
//! - the same bytes re-sealed, so the checksum holds and the body rules
//!   themselves are fuzzed;
//! - a round trip: ops built from the bytes, staged through `RecordBuf`,
//!   sealed and decoded — every view must read back exactly what was staged.

#![no_main]
use graph_r::format::base::sflags;
use graph_r::format::wal::{self, Next, OpView, RecordBuf, TouchRecord};
use graph_r::hash::Hasher;
use graph_r::writer::{DocRecord, SegmentRecord};
use graph_r::UrlKey;

fn decode(bytes: &[u8], pos: usize) {
    match wal::next_record(bytes, pos) {
        Ok(Next::Record(r)) => {
            assert!(pos + r.len() <= bytes.len());
            assert_eq!(r.offset(), pos);
            let ops = r.ops().count();
            assert_eq!(ops, r.op_count() as usize, "a record yields every op it declares");
        }
        Ok(Next::Torn) => {}
        Err(graph_r::Error::Corrupt { .. }) => {}
        Err(e) => panic!("next_record returned a non-Corrupt error: {e}"),
    }
}

/// A byte cursor that yields zeros once exhausted.
struct Bytes<'a>(&'a [u8]);

impl Bytes<'_> {
    fn u8(&mut self) -> u8 {
        let Some((&b, rest)) = self.0.split_first() else { return 0 };
        self.0 = rest;
        b
    }
    fn u16(&mut self) -> u16 {
        u16::from_le_bytes([self.u8(), self.u8()])
    }
    fn u64(&mut self) -> u64 {
        (0..8).fold(0, |v, i| v | u64::from(self.u8()) << (8 * i))
    }
    /// A string of up to `max` chars, multi-byte ones included.
    fn text(&mut self, max: u8) -> String {
        let n = self.u8() % (max + 1);
        (0..n).map(|_| ['a', 'é', '€', '𝄞', '/', ' '][usize::from(self.u8() % 6)]).collect()
    }
    fn opt(&mut self, max: u8) -> Option<String> {
        (self.u8() & 1 == 1).then(|| self.text(max))
    }
}

/// A staged segment: label, byte range, depth, importance.
type Seg = (String, Option<(u32, u32)>, u8, u16);

/// What was staged, to compare against what is read back.
enum Spec {
    Upsert(String, Option<String>, Option<String>, Option<String>, u64, u64, u16),
    Segs(Vec<Seg>),
    Edges(Vec<wal::Edge>),
    Touch(u64, u8, Option<u64>, Option<String>, u32, u16, u16, u64, bool),
    Remove,
    Pin(bool),
    Attrs(Vec<(u8, String)>),
}

fn stage(b: &mut Bytes<'_>, rec: &mut RecordBuf) -> Option<(UrlKey, Spec)> {
    let key = UrlKey(b.u64() % 16);
    let spec = match b.u8() % 7 {
        0 => {
            let url = format!("u{}", b.text(20));
            let (title, snippet, etag) = (b.opt(30), b.opt(30), b.opt(8));
            let (hash, at, flags) = (b.u64(), b.u64(), b.u16());
            let doc = DocRecord {
                url: &url,
                url_key: key,
                content_hash: hash,
                fetched_at_ms: at,
                title: title.as_deref(),
                snippet: snippet.as_deref(),
                etag: etag.as_deref(),
                pinned: false,
            };
            rec.push_upsert(&doc, flags).ok()?;
            Spec::Upsert(url, title, snippet, etag, hash, at, flags)
        }
        1 => {
            let n = b.u8() % 5;
            let segs: Vec<Seg> = (0..n)
                .map(|_| {
                    let range = (b.u8() & 1 == 1).then(|| (u32::from(b.u16()), u32::from(b.u16())));
                    (b.text(12), range, b.u8(), b.u16())
                })
                .collect();
            let recs: Vec<SegmentRecord<'_>> = segs
                .iter()
                .map(|(l, r, d, i)| SegmentRecord {
                    label: l,
                    byte_range: *r,
                    depth: *d,
                    importance: *i,
                })
                .collect();
            rec.push_segments(key, &recs).ok()?;
            Spec::Segs(segs)
        }
        2 => {
            let mut dst = 0u64;
            let edges: Vec<wal::Edge> = (0..b.u8() % 7)
                .map(|_| {
                    dst += 1 + u64::from(b.u8());
                    (UrlKey(dst), b.u8(), b.u8(), b.u16())
                })
                .collect();
            rec.push_edges(key, &edges).ok()?;
            Spec::Edges(edges)
        }
        3 => {
            let (at, outcome) = (b.u64(), b.u8());
            let hash = (b.u8() & 1 == 1).then(|| b.u64());
            let etag = b.opt(8);
            let (interval, checks, changes, last, gone) =
                (u32::from(b.u16()), b.u16(), b.u16(), b.u64(), b.u8() & 1 == 1);
            let t = TouchRecord {
                checked_at_ms: at,
                outcome,
                content_hash: hash,
                etag: etag.as_deref(),
                interval_s: interval,
                checks,
                changes,
                last_change_ms: last,
                tombstone: gone,
            };
            rec.push_touch(key, &t).ok()?;
            Spec::Touch(at, outcome, hash, etag, interval, checks, changes, last, gone)
        }
        4 => {
            rec.push_remove(key);
            Spec::Remove
        }
        5 => {
            let on = b.u8() & 1 == 1;
            rec.push_pin(key, on);
            Spec::Pin(on)
        }
        _ => {
            let mut k = 0u8;
            let attrs: Vec<(u8, String)> = (0..b.u8() % 4)
                .map(|_| {
                    k = k.saturating_add(1 + b.u8() % 8);
                    (k, b.text(40))
                })
                .collect();
            let refs: Vec<(u8, &str)> = attrs.iter().map(|(k, v)| (*k, v.as_str())).collect();
            rec.push_attrs(key, &refs).ok()?;
            Spec::Attrs(attrs)
        }
    };
    Some((key, spec))
}

fn same(op: OpView<'_>, key: UrlKey, spec: &Spec) {
    assert_eq!(op.key(), key);
    match (op, spec) {
        (OpView::Upsert(u), Spec::Upsert(url, title, snippet, etag, hash, at, flags)) => {
            assert_eq!(u.url(), url);
            assert_eq!(u.title(), title.as_deref());
            assert_eq!(u.snippet(), snippet.as_deref());
            assert_eq!(u.etag(), etag.as_deref());
            assert_eq!((u.content_hash(), u.fetched_at_ms(), u.flags()), (*hash, *at, *flags));
        }
        (OpView::Segs(s), Spec::Segs(want)) => {
            let got: Vec<_> = s.iter().collect();
            assert_eq!(got.len(), want.len());
            for (g, (label, range, depth, importance)) in got.iter().zip(want) {
                assert_eq!(g.label, label);
                assert_eq!((g.depth, g.importance), ((*depth).max(1), *importance));
                match range {
                    Some((a, b)) => assert_eq!((g.byte_start, g.byte_len, g.flags), (*a, *b, 0)),
                    None => assert_eq!(g.flags, sflags::NO_RANGE),
                }
            }
        }
        (OpView::Edges(e), Spec::Edges(want)) => assert_eq!(&e.iter().collect::<Vec<_>>(), want),
        (
            OpView::Touch(t),
            Spec::Touch(at, outcome, hash, etag, interval, checks, changes, last, gone),
        ) => {
            assert_eq!((t.checked_at_ms(), t.outcome(), t.content_hash()), (*at, *outcome, *hash));
            assert_eq!(t.etag(), etag.as_deref());
            assert_eq!((t.interval_s(), t.checks(), t.changes()), (*interval, *checks, *changes));
            assert_eq!((t.last_change_ms(), t.tombstone()), (*last, *gone));
        }
        (OpView::Remove(_), Spec::Remove) => {}
        (OpView::Pin(_, on), Spec::Pin(want)) => assert_eq!(on, *want),
        (OpView::Attrs(a), Spec::Attrs(want)) => {
            let got: Vec<(u8, String)> = a.iter().map(|(k, v)| (k, v.to_owned())).collect();
            assert_eq!(&got, want);
        }
        _ => panic!("an op read back as another kind"),
    }
}

libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    let Some((&at, rest)) = data.split_first() else { return };
    let pos = usize::from(at) % 8;
    decode(rest, pos);
    // Make the checksum hold: the body rules are now all that stand
    // between the bytes and acceptance.
    let mut raw = rest.to_vec();
    if raw.len() >= wal::RECORD_HEADER_LEN {
        let len = raw.len() as u32;
        raw[..4].copy_from_slice(&len.to_le_bytes());
        raw[4..6].copy_from_slice(&0x5854u16.to_le_bytes());
        raw[6] = 1;
        let mut h = Hasher::new();
        h.update(&raw[..24]);
        h.update(&raw[wal::RECORD_HEADER_LEN..]);
        let sum = h.digest64();
        raw[24..32].copy_from_slice(&sum.to_le_bytes());
        decode(&raw, 0);
    }
    // Round trip.
    let mut b = Bytes(rest);
    let mut rec = RecordBuf::new();
    let mut specs = Vec::new();
    for _ in 0..8 {
        if let Some(s) = stage(&mut b, &mut rec) {
            specs.push(s);
        }
    }
    if specs.is_empty() {
        return;
    }
    rec.seal(u64::from(at) + 1).unwrap();
    let Ok(Next::Record(r)) = wal::next_record(rec.as_bytes(), 0) else {
        panic!("a sealed record did not read back");
    };
    assert_eq!(r.op_count() as usize, specs.len());
    for ((_, _, op), (key, spec)) in r.ops().zip(&specs) {
        same(op, *key, spec);
    }
});
