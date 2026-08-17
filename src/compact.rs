// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>
//
//! Compaction: fold the base + overlay into a fresh base generation.
//!
//! The fold reads every node through the *snapshot* merge path — the same
//! code queries use — so base+overlay semantics exist exactly once. Output
//! is deterministic end to end: nodes sorted by key, segments in document
//! order, edges by destination id, lexicon by token hash; identical logical
//! state always renders byte-identical files (the property proptests pin).
//!
//! Crash safety: the new base (carrying `wal_applied_seq`) is renamed into
//! place before the WAL is reset; replay of the old WAL against the new base
//! skips everything already folded, so any crash interleaving recovers.

use crate::error::{Error, Result};
use crate::format::base::{
    self, encode_postings, nflags, push_label, write_lex_rec, write_node_rec, write_seg_rec,
    BaseWriter, Header, SectionKind, NO_LABEL,
};
use crate::format::wal;
use crate::key::{SegKey, UrlKey};
use crate::rank;
use crate::store::{atomic_write, base_path, now_ms, wal_path, Store};
use crate::traverse::LendingIterator;
use crate::writer::WriteState;
use compact_str::CompactString;
use std::collections::BTreeMap;
use std::sync::atomic::Ordering;

/// What a compaction produced.
#[derive(Clone, Copy, Debug)]
pub struct CompactStats {
    /// New generation number.
    pub generation: u64,
    /// Nodes written (including stubs and tombstones).
    pub nodes: usize,
    /// Segments written.
    pub segs: usize,
    /// Edges written.
    pub edges: usize,
    /// Base file size in bytes.
    pub bytes: usize,
}

struct Model {
    url: CompactString,
    title: Option<CompactString>,
    snippet: Option<CompactString>,
    etag: Option<CompactString>,
    content_hash: u64,
    fetched_at_ms: u64,
    last_change_ms: u64,
    interval_s: u32,
    checks: u16,
    changes: u16,
    flags: u16,
    segs: Vec<(u64, u32, u32, CompactString, u16, u8, u8)>,
    edges: Vec<(u64, u8, u8, u16)>,
}

/// Render an empty base for `generation` (store creation).
pub(crate) fn render_empty_base(generation: u64) -> Vec<u8> {
    BaseWriter::new(Header {
        flags: 0,
        section_count: 0,
        node_count: 0,
        seg_count: 0,
        edge_count: 0,
        token_count: 0,
        generation,
        wal_applied_seq: 0,
        total_len: 0,
        created_at_ms: 0,
    })
    .finish()
}

/// Fold the current generation + overlay into a new base and install it.
/// Caller holds the writer mutex; staged ops must already be committed.
#[allow(clippy::too_many_lines)] // one linear render pass; splitting obscures the layout
pub(crate) fn compact_locked(store: &Store, state: &mut WriteState) -> Result<CompactStats> {
    debug_assert!(state.staged_is_empty(), "compact with staged ops");
    let snap = store.snapshot();
    let old_generation = snap.generation();
    let new_generation = old_generation + 1;

    // ---- 1. collect models through the snapshot merge path ------------------
    let mut keys: Vec<u64> = Vec::with_capacity(snap.base_len() + 64);
    for i in 0..snap.base_len() {
        keys.push(snap.rec(crate::key::NodeId(i as u32)).url_key().0);
    }
    for k in snap.overlay_keys() {
        keys.push(k);
    }
    keys.sort_unstable();
    keys.dedup();

    let mut models: BTreeMap<u64, Model> = BTreeMap::new();
    for &k in &keys {
        let key = UrlKey(k);
        let Some(n) = snap.node(key) else { continue }; // removed
        let mut edges: Vec<(u64, u8, u8, u16)> = Vec::new();
        let mut it = snap.neighbors(key);
        while let Some(e) = it.next() {
            edges.push((e.dst_key.0, e.etype.as_tag(), e.flags, e.weight));
        }
        edges.sort_unstable_by_key(|&(dst, _, _, _)| dst);
        edges.dedup_by_key(|e| e.0);
        let segs = snap
            .segments(key)
            .iter()
            .map(|s| {
                (
                    s.key.0,
                    s.byte_range.map_or(0, |(a, _)| a),
                    s.byte_range.map_or(0, |(_, b)| b),
                    CompactString::from(s.label),
                    s.importance,
                    s.depth,
                    s.flags,
                )
            })
            .collect();
        models.insert(
            k,
            Model {
                url: n.url.into(),
                title: n.title.map(Into::into),
                snippet: n.snippet.map(Into::into),
                etag: n.etag.map(Into::into),
                content_hash: n.content_hash,
                fetched_at_ms: n.fetched_at_ms,
                last_change_ms: n.last_change_ms,
                interval_s: n.interval_s,
                checks: n.checks,
                changes: n.changes,
                flags: n.flags,
                segs,
                edges,
            },
        );
    }

    // ---- 2. stub management --------------------------------------------------
    // Referenced targets that are not nodes become stubs; stubs nothing
    // references any more are dropped.
    let mut referenced: std::collections::HashSet<u64> = std::collections::HashSet::new();
    for m in models.values() {
        for &(dst, _, _, _) in &m.edges {
            referenced.insert(dst);
        }
    }
    models.retain(|k, m| m.flags & nflags::STUB == 0 || referenced.contains(k));
    let missing: Vec<u64> =
        referenced.iter().copied().filter(|k| !models.contains_key(k)).collect();
    for k in missing {
        models.insert(
            k,
            Model {
                url: CompactString::default(),
                title: None,
                snippet: None,
                etag: None,
                content_hash: 0,
                fetched_at_ms: 0,
                last_change_ms: 0,
                interval_s: 0,
                checks: 0,
                changes: 0,
                flags: nflags::STUB,
                segs: Vec::new(),
                edges: Vec::new(),
            },
        );
    }

    // ---- 3. assign dense ids and render sections ----------------------------
    let id_of: std::collections::HashMap<u64, u32> =
        models.keys().enumerate().map(|(i, &k)| (k, i as u32)).collect();
    let n = models.len();

    let mut labels = Vec::new();
    let mut nodes = Vec::with_capacity(n * base::NODE_LEN);
    let mut segs_bytes = Vec::new();
    let mut edge_index = Vec::with_capacity((n + 1) * 4);
    let mut edges_bytes = Vec::new();
    let mut edge_rows: Vec<Vec<rank::EdgeRow>> = Vec::with_capacity(n);
    let mut in_degree = vec![0u32; n];
    let mut seg_total = 0u32;
    let mut edge_total = 0u32;
    let mut max_fetched = 0u64;

    // First pass: in-degrees + per-node resolved edge rows (dst → dense id).
    for m in models.values() {
        let mut rows: Vec<rank::EdgeRow> = m
            .edges
            .iter()
            .map(|&(dst, etype, flags, weight)| (id_of[&dst], etype, flags, weight))
            .collect();
        rows.sort_unstable_by_key(|&(dst, _, _, _)| dst);
        for &(dst, _, _, _) in &rows {
            in_degree[dst as usize] += 1;
        }
        edge_rows.push(rows);
    }

    // Lexicon accumulation: token hash → (label, ascending unique node ids).
    let mut lexicon: BTreeMap<u64, (CompactString, Vec<u32>)> = BTreeMap::new();
    let push_tokens =
        |text: &str, id: u32, lexicon: &mut BTreeMap<u64, (CompactString, Vec<u32>)>| {
            crate::query::for_each_token(text, |tok| {
                let hash = xxhash_rust::xxh3::xxh3_64(tok.as_bytes());
                let entry =
                    lexicon.entry(hash).or_insert_with(|| (CompactString::from(tok), Vec::new()));
                if entry.1.last() != Some(&id) {
                    entry.1.push(id);
                }
            });
        };

    for (i, (&key, m)) in models.iter().enumerate() {
        let id = i as u32;
        let url_off = if m.url.is_empty() { NO_LABEL } else { push_label(&mut labels, &m.url) };
        let title_off = m.title.as_deref().map_or(NO_LABEL, |s| push_label(&mut labels, s));
        let snippet_off = m.snippet.as_deref().map_or(NO_LABEL, |s| push_label(&mut labels, s));
        let etag_off = m.etag.as_deref().map_or(NO_LABEL, |s| push_label(&mut labels, s));

        let seg_start = seg_total;
        for (seg_key, byte_start, byte_len, label, importance, depth, flags) in &m.segs {
            let label_off = push_label(&mut labels, label);
            write_seg_rec(
                &mut segs_bytes,
                *seg_key,
                *byte_start,
                *byte_len,
                label_off,
                *importance,
                *depth,
                *flags,
            );
            seg_total += 1;
        }

        edge_index.extend_from_slice(&edge_total.to_le_bytes());
        for &(dst, etype, eflags, weight) in &edge_rows[i] {
            edges_bytes.extend_from_slice(&dst.to_le_bytes());
            edges_bytes.push(etype);
            edges_bytes.push(eflags);
            edges_bytes.extend_from_slice(&weight.to_le_bytes());
            edge_total += 1;
        }

        max_fetched = max_fetched.max(m.fetched_at_ms);
        write_node_rec(
            &mut nodes,
            UrlKey(key),
            m.content_hash,
            m.fetched_at_ms,
            m.last_change_ms,
            [url_off, title_off, snippet_off, etag_off],
            seg_start,
            m.segs.len() as u16,
            m.flags,
            m.interval_s,
            m.checks,
            m.changes,
            in_degree[i],
        );

        // Index lookup text: url without scheme, title, snippet, seg labels.
        if !m.url.is_empty() {
            let searchable = m.url.split_once("://").map_or(m.url.as_str(), |(_, r)| r);
            push_tokens(searchable, id, &mut lexicon);
        }
        if let Some(t) = &m.title {
            push_tokens(t, id, &mut lexicon);
        }
        if let Some(s) = &m.snippet {
            push_tokens(s, id, &mut lexicon);
        }
        for (_, _, _, label, _, _, _) in &m.segs {
            push_tokens(label, id, &mut lexicon);
        }
    }
    edge_index.extend_from_slice(&edge_total.to_le_bytes());

    // Segment identity sanity: seg keys must match their heading path.
    debug_assert!(models.iter().all(|(&k, m)| m
        .segs
        .iter()
        .all(|(sk, _, _, label, _, _, _)| *sk == SegKey::derive(UrlKey(k), label).0)));

    let mut lex_bytes = Vec::with_capacity(lexicon.len() * base::LEX_LEN);
    let mut postings_bytes = Vec::new();
    for (&hash, (label, ids)) in &lexicon {
        let off = postings_bytes.len() as u32;
        encode_postings(ids, &mut postings_bytes);
        let len = postings_bytes.len() as u32 - off;
        let label_off = push_label(&mut labels, label);
        write_lex_rec(&mut lex_bytes, hash, ids.len() as u32, off, len, label_off);
    }

    let (rank_permille, community) = rank::compute(n, &edge_rows);
    let mut ranks_bytes = Vec::with_capacity(n * base::RANK_LEN);
    for i in 0..n {
        ranks_bytes.extend_from_slice(&rank_permille[i].to_le_bytes());
        ranks_bytes.extend_from_slice(&0u16.to_le_bytes());
        ranks_bytes.extend_from_slice(&community[i].to_le_bytes());
    }

    let wal_applied_seq = state.last_committed_seq();
    let mut w = BaseWriter::new(Header {
        flags: 0,
        section_count: 0,
        node_count: n as u32,
        seg_count: seg_total,
        edge_count: edge_total,
        token_count: lexicon.len() as u32,
        generation: new_generation,
        wal_applied_seq,
        total_len: 0,
        created_at_ms: max_fetched, // deterministic, informative enough
    });
    if n > 0 {
        w.add_section(SectionKind::Nodes, nodes);
        if seg_total > 0 {
            w.add_section(SectionKind::Segs, segs_bytes);
        }
        w.add_section(SectionKind::EdgeIndex, edge_index);
        if edge_total > 0 {
            w.add_section(SectionKind::Edges, edges_bytes);
        }
        w.add_section(SectionKind::Labels, labels);
        if !lexicon.is_empty() {
            w.add_section(SectionKind::Lexicon, lex_bytes);
            w.add_section(SectionKind::Postings, postings_bytes);
        }
        w.add_section(SectionKind::Ranks, ranks_bytes);
    }
    let bytes = w.finish();
    drop(snap); // release the read pin before touching the ring

    // ---- 4. swap files (base first — see module docs), then the WAL ---------
    atomic_write(&base_path(&store.dir), &bytes)?;
    atomic_write(&wal_path(&store.dir), &wal::encode_header(new_generation, now_ms()))?;
    state.reset_after_compact(wal_applied_seq + 1);

    // ---- 5. install into a free ring slot -----------------------------------
    let map = crate::os::MappedFile::open(&base_path(&store.dir))?;
    let dir = base::BaseDir::parse(map.bytes())?;
    let mut pending = Some(crate::store::GenInner::from_parts(map, dir));
    let cur = store.current.load(Ordering::Acquire);
    for (i, slot) in store.gens.iter().enumerate() {
        if i == cur {
            continue;
        }
        if let Ok(mut wslot) = slot.inner.try_write() {
            *wslot = pending.take();
            drop(wslot);
            slot.seq.store(new_generation, Ordering::Release);
            store.current.store(i, Ordering::Release);
            break;
        }
    }
    if pending.is_some() {
        // Every other slot is pinned by a long-lived snapshot. The new base
        // is durable on disk; the in-memory swap happens on the next attempt.
        return Err(Error::busy("all generation slots pinned by snapshots"));
    }

    Ok(CompactStats {
        generation: new_generation,
        nodes: n,
        segs: seg_total as usize,
        edges: edge_total as usize,
        bytes: bytes.len(),
    })
}
