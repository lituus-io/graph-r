// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! The committed-op overlay: an append-only, lock-free arena of the log
//! records made durable since the base was compacted, plus the merged
//! per-key view a snapshot builds over it.
//!
//! The arena keeps records as the bytes they were written as: a commit's
//! sealed buffer, or the log read at open or catch-up, is moved in whole as
//! one *segment* and never copied, decoded or moved again. Each op gets a
//! 16-byte entry (its sequence number and where it lies), and every read
//! goes through a borrowed view of the validated bytes. Segments and entries
//! live in chunked `OnceLock` cells, so a published one never moves and a
//! reader's borrow stays valid for its snapshot's lifetime.
//!
//! Publication fills a segment cell and the entries, then bumps the
//! frontier once with `Release`; a snapshot captures the frontier once with
//! `Acquire` and only ever reads below it, so it sees every op of a record
//! or none. That is the whole snapshot-isolation story. No `unsafe`, no
//! locks on the read path.

use crate::format::wal::{self, OpView};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::OnceLock;

/// Chunk 0 capacity; chunk `k` holds `BASE_CAP << k` cells.
const BASE_CAP: usize = 64;
/// Number of chunks: about 67M entries (and segments), far beyond any
/// compaction threshold; reaching the end is a refusal, never UB.
const CHUNKS: usize = 20;
const CAPACITY: usize = BASE_CAP * ((1 << CHUNKS) - 1);
/// An entry locates its op as `segment << OFF_BITS | offset`: segments of
/// up to 256 GiB, and 2^26 of them (more than [`CAPACITY`]).
const OFF_BITS: u32 = 38;
const OFF_MASK: u64 = (1 << OFF_BITS) - 1;

fn locate(i: usize) -> (usize, usize) {
    let x = i / BASE_CAP + 1;
    let k = (usize::BITS - 1 - x.leading_zeros()) as usize;
    let before = BASE_CAP * ((1 << k) - 1);
    (k, i - before)
}

/// One published op: its sequence number and its location.
#[derive(Default)]
struct Entry {
    seq: AtomicU64,
    at: AtomicU64,
}

/// Where an op lies in the arena; resolved with [`LogArena::view`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OpRef(u64);

/// A chunked array of cells: chunk `k` holds `BASE_CAP << k` of them.
type Chunks<T> = [OnceLock<Box<[T]>>; CHUNKS];

/// Append-only arena of committed log records.
pub(crate) struct LogArena {
    /// Segments: whole records, as written.
    segs: Chunks<OnceLock<Vec<u8>>>,
    entries: Chunks<Entry>,
    nsegs: AtomicUsize,
    frontier: AtomicUsize,
}

#[allow(clippy::missing_fields_in_debug)] // the cells are noise; counts say it all
impl std::fmt::Debug for LogArena {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogArena")
            .field("ops", &self.len())
            .field("segments", &self.nsegs.load(Ordering::Relaxed))
            .finish()
    }
}

/// Cell `i` of a chunked array, materializing its chunk on first use.
fn cell<T: Default>(chunks: &Chunks<T>, i: usize) -> Option<&T> {
    let (k, off) = locate(i);
    let chunk = chunks.get(k)?.get_or_init(|| {
        (0..BASE_CAP << k).map(|_| T::default()).collect::<Vec<_>>().into_boxed_slice()
    });
    chunk.get(off)
}

impl LogArena {
    pub(crate) fn new() -> Self {
        Self {
            segs: std::array::from_fn(|_| OnceLock::new()),
            entries: std::array::from_fn(|_| OnceLock::new()),
            nsegs: AtomicUsize::new(0),
            frontier: AtomicUsize::new(0),
        }
    }

    /// Committed op count (Acquire: pairs with the publish Release).
    pub(crate) fn len(&self) -> usize {
        self.frontier.load(Ordering::Acquire)
    }

    /// Make room for one segment of `bytes` holding `ops` ops, before
    /// anything is written anywhere: `false` when the arena cannot take it
    /// (the caller must compact). After `true`, [`LogArena::publish`] of
    /// such a segment cannot fail and allocates nothing.
    pub(crate) fn reserve(&self, ops: usize, bytes: usize) -> bool {
        #[cfg(test)]
        if crate::store::probe::full() {
            return false;
        }
        let from = self.frontier.load(Ordering::Relaxed);
        let seg = self.nsegs.load(Ordering::Relaxed);
        let Some(end) = from.checked_add(ops) else { return false };
        if end > CAPACITY || seg >= CAPACITY || bytes as u64 > OFF_MASK {
            return false;
        }
        let _ = cell(&self.segs, seg);
        let mut i = from;
        while i < end {
            let _ = cell(&self.entries, i);
            i = BASE_CAP * ((1 << (locate(i).0 + 1)) - 1);
        }
        true
    }

    /// Publish one segment, reserved beforehand with [`LogArena::reserve`].
    /// `ops` reads the stored segment and yields `(seq, offset)` for each op
    /// to make visible, in sequence order. Single-producer: only the holder
    /// of the store's write mutex calls this. The frontier moves once, after
    /// every entry is in place, so a snapshot sees the whole segment or none.
    pub(crate) fn publish<'s, I>(&'s self, seg: Vec<u8>, ops: impl FnOnce(&'s [u8]) -> I)
    where
        I: Iterator<Item = (u64, usize)>,
    {
        let s = self.nsegs.load(Ordering::Relaxed);
        let Some(slot) = cell(&self.segs, s) else { return };
        let fresh = slot.set(seg).is_ok();
        debug_assert!(fresh, "segment cell double-initialized");
        self.nsegs.store(s + 1, Ordering::Relaxed);
        let bytes = slot.get().map_or(&[][..], Vec::as_slice);
        let mut i = self.frontier.load(Ordering::Relaxed);
        for (seq, off) in ops(bytes) {
            let Some(e) = cell(&self.entries, i) else { break };
            e.seq.store(seq, Ordering::Relaxed);
            e.at.store((s as u64) << OFF_BITS | off as u64, Ordering::Relaxed);
            i += 1;
        }
        self.frontier.store(i, Ordering::Release);
    }

    fn entry(&self, i: usize) -> Option<(u64, OpRef)> {
        let e = cell(&self.entries, i)?;
        Some((e.seq.load(Ordering::Relaxed), OpRef(e.at.load(Ordering::Relaxed))))
    }

    /// Sequence number of the last published op (0 when none): readers
    /// publish what follows it, so a log is diffed by sequence, never by
    /// count.
    pub(crate) fn last_seq(&self) -> u64 {
        match self.len() {
            0 => 0,
            n => self.entry(n - 1).map_or(0, |e| e.0),
        }
    }

    /// Entries `[0, upto)`; `upto` must be a frontier value captured from
    /// `len()`, so every entry below it is published.
    pub(crate) fn committed(&self, upto: usize) -> impl Iterator<Item = (u64, OpRef)> + '_ {
        (0..upto).filter_map(move |i| self.entry(i))
    }

    /// The op `r` points at, read in place. `None` only for a reference
    /// this arena never issued.
    pub(crate) fn view(&self, r: OpRef) -> Option<OpView<'_>> {
        let seg = cell(&self.segs, (r.0 >> OFF_BITS) as usize)?.get()?;
        let off = (r.0 & OFF_MASK) as usize;
        let len = crate::bytesio::get_u32(seg.get(off..off + 4)?, 0) as usize;
        OpView::trusted(seg.get(off..off + len)?)
    }

    /// Take back the first segment of an arena that was never served (a
    /// generation that could not be installed), to publish it elsewhere.
    pub(crate) fn take_first_segment(&mut self) -> Option<Vec<u8>> {
        self.segs[0].get_mut()?.get_mut(0)?.take()
    }
}

/// The folded per-key effect of the overlay ops visible to one snapshot:
/// references into the arena, never copies.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct OverlayNode {
    /// Last upsert.
    pub(crate) core: Option<OpRef>,
    /// Last segment set.
    pub(crate) segs: Option<OpRef>,
    /// Last edge set.
    pub(crate) edges: Option<OpRef>,
    /// Last attribute set.
    pub(crate) attrs: Option<OpRef>,
    /// Last touch since the last upsert (the writer pre-computes cumulative
    /// counters, so last-wins folding is exact).
    pub(crate) touch: Option<OpRef>,
    /// Freshness counters carried across an upsert (checks, changes,
    /// `last_change_ms`): history survives re-ingest; only `Remove` clears it.
    pub(crate) carried: Option<(u16, u16, u64)>,
    pub(crate) pinned: Option<bool>,
    pub(crate) removed: bool,
    /// A `Remove` happened in this overlay: a later resurrecting upsert must
    /// not inherit the (not yet compacted) base record's history or pin.
    pub(crate) severed: bool,
}

/// Hashes a [`crate::UrlKey`] (already an xxh3 digest) with one finalizer
/// round over the key and a per-map secret, instead of `SipHash`: the fold's
/// cost is these inserts. The secret comes from the process's random hash
/// state, so URLs chosen to collide (a crawled page picks its links) cannot
/// aim at buckets they cannot see.
#[derive(Clone, Copy, Debug)]
pub(crate) struct KeyHasher(u64);

impl std::hash::Hasher for KeyHasher {
    fn finish(&self) -> u64 {
        crate::hash::fmix64(self.0)
    }
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 = self.0.rotate_left(8) ^ u64::from(b);
        }
    }
    fn write_u64(&mut self, v: u64) {
        self.0 ^= v;
    }
}

/// Builds [`KeyHasher`]s seeded with one map's secret.
#[derive(Clone, Copy, Debug)]
pub(crate) struct KeyState(u64);

impl Default for KeyState {
    fn default() -> Self {
        use std::hash::BuildHasher as _;
        Self(std::collections::hash_map::RandomState::new().hash_one(0x5EED_u64))
    }
}

impl std::hash::BuildHasher for KeyState {
    type Hasher = KeyHasher;
    fn build_hasher(&self) -> KeyHasher {
        KeyHasher(self.0)
    }
}

/// A map keyed by a key's digest.
pub(crate) type KeyMap<V> = HashMap<u64, V, KeyState>;

/// Merged overlay view for a snapshot: key → folded state, in op order.
#[derive(Debug, Default)]
pub(crate) struct OverlayView {
    pub(crate) map: KeyMap<OverlayNode>,
}

impl OverlayView {
    pub(crate) fn build(log: &LogArena, upto: usize) -> Self {
        const PINNED: u16 = crate::format::base::nflags::PINNED;
        let mut map: KeyMap<OverlayNode> = KeyMap::default();
        for (_, r) in log.committed(upto) {
            let Some(op) = log.view(r) else { continue };
            let entry = map.entry(op.key().0).or_default();
            match op {
                OpView::Upsert(u) => {
                    // An upsert resurrects a removed key and supersedes prior
                    // freshness (the fetch that produced it is the new stamp).
                    // Pins are sticky: once any pin state is known within this
                    // overlay, fold it forward as the authoritative override
                    // (base-generation stickiness is applied at read time).
                    let upsert_pin = u.flags() & PINNED != 0;
                    let prior_core_pin = entry
                        .core
                        .and_then(|c| log.view(c))
                        .is_some_and(|c| matches!(c, OpView::Upsert(c) if c.flags() & PINNED != 0));
                    // Explicit pin state ORs with the upsert; otherwise a pin
                    // can only be *proven* here (any pinning upsert), never
                    // disproven — an all-unpinned history stays deferred so
                    // read time can apply base-generation stickiness.
                    let folded_pin = match entry.pinned {
                        Some(prev) => Some(upsert_pin || prev),
                        None if upsert_pin || prior_core_pin => Some(true),
                        None => None,
                    };
                    entry.removed = false;
                    entry.carried = match entry.touch.and_then(|t| log.view(t)) {
                        Some(OpView::Touch(t)) => {
                            Some((t.checks(), t.changes(), t.last_change_ms()))
                        }
                        _ => entry.carried,
                    };
                    entry.touch = None;
                    entry.core = Some(r);
                    entry.pinned = folded_pin;
                }
                OpView::Segs(_) => entry.segs = Some(r),
                OpView::Edges(_) => entry.edges = Some(r),
                OpView::Attrs(_) => entry.attrs = Some(r),
                OpView::Touch(_) => entry.touch = Some(r),
                OpView::Remove(_) => {
                    *entry = OverlayNode { removed: true, severed: true, ..OverlayNode::default() };
                }
                OpView::Pin(_, pinned) => entry.pinned = Some(pinned),
            }
        }
        Self { map }
    }
}

/// Publish `rec` (one sealed record, or a run of whole records starting at
/// `from`) into `log`, every op of it.
pub(crate) fn publish_records(log: &LogArena, rec: Vec<u8>, from: usize) {
    log.publish(rec, move |seg| {
        wal::Records::trusted(seg, from, seg.len())
            .flat_map(|r| r.ops())
            .map(|(seq, off, _)| (seq, off))
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::wal::RecordBuf;
    use crate::key::UrlKey;
    use crate::writer::DocRecord;

    fn removes(first: u64, n: u64) -> Vec<u8> {
        let mut b = RecordBuf::new();
        for k in 0..n {
            b.push_remove(UrlKey(k));
        }
        b.seal(first).unwrap();
        b.into_vec()
    }

    fn publish(a: &LogArena, rec: Vec<u8>) {
        let ops = wal::Records::trusted(&rec, 0, rec.len()).map(|r| r.op_count() as usize).sum();
        assert!(a.reserve(ops, rec.len()));
        publish_records(a, rec, 0);
    }

    #[test]
    fn locate_walks_chunk_boundaries() {
        assert_eq!(locate(0), (0, 0));
        assert_eq!(locate(63), (0, 63));
        assert_eq!(locate(64), (1, 0));
        assert_eq!(locate(191), (1, 127));
        assert_eq!(locate(192), (2, 0));
    }

    #[test]
    fn publish_then_read_below_frontier() {
        let a = LogArena::new();
        for i in 0..200u64 {
            publish(&a, removes(i + 1, 1));
        }
        assert_eq!(a.len(), 200);
        assert_eq!(a.last_seq(), 200);
        let seen: Vec<u64> = a.committed(200).map(|(s, _)| s).collect();
        assert_eq!(seen, (1..=200).collect::<Vec<_>>());
        let keys: Vec<u64> = a.committed(200).map(|(_, r)| a.view(r).unwrap().key().0).collect();
        assert_eq!(keys, vec![0; 200], "each op resolves in place");
    }

    /// A record is published whole: a reader polling the frontier while
    /// records of `N` ops land only ever sees a multiple of `N`.
    #[test]
    fn a_reader_never_sees_part_of_a_record() {
        const N: u64 = 7;
        const RECORDS: u64 = 2_000;
        let a = LogArena::new();
        let done = std::sync::atomic::AtomicBool::new(false);
        std::thread::scope(|s| {
            s.spawn(|| {
                let mut polls = 0u64;
                while !done.load(Ordering::Acquire) || polls == 0 {
                    let len = a.len();
                    assert_eq!(len % N as usize, 0, "saw {len} ops: part of a record");
                    polls += 1;
                }
            });
            for c in 0..RECORDS {
                publish(&a, removes(c * N + 1, N));
            }
            done.store(true, Ordering::Release);
        });
        assert_eq!(a.len(), (N * RECORDS) as usize);
    }

    #[test]
    fn reserve_refuses_past_capacity() {
        let a = LogArena::new();
        assert!(!a.reserve(CAPACITY + 1, 64));
        assert!(!a.reserve(usize::MAX, 64));
        assert!(!a.reserve(1, OFF_MASK as usize + 1), "a segment past the offset range");
        assert!(a.reserve(1, 64));
        assert_eq!(a.len(), 0, "reserving publishes nothing");
    }

    #[test]
    fn view_folds_last_writer_wins_and_remove_clears() {
        let key = UrlKey::of("https://x.dev/a");
        let mut b = RecordBuf::new();
        b.push_pin(key, true);
        b.push_remove(key);
        let doc = DocRecord {
            url: "https://x.dev/a",
            url_key: key,
            content_hash: 9,
            fetched_at_ms: 10,
            title: None,
            snippet: None,
            etag: None,
            pinned: false,
        };
        b.push_upsert(&doc, 0).unwrap();
        b.seal(1).unwrap();
        let a = LogArena::new();
        publish(&a, b.into_vec());
        let view = OverlayView::build(&a, a.len());
        let n = &view.map[&key.0];
        assert!(!n.removed, "upsert resurrects");
        assert!(n.pinned.is_none(), "remove cleared the earlier pin");
        let Some(OpView::Upsert(core)) = n.core.and_then(|r| a.view(r)) else { panic!("core") };
        assert_eq!(core.content_hash(), 9);
    }
}
