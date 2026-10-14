// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
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
use crate::fsio::now_ms;
use crate::key::{SegKey, UrlKey};
use crate::rank;
use crate::store::{atomic_write, base_path, wal_path, Store};
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
    attrs: Vec<(u8, CompactString)>,
}

/// Render an empty base for `generation` (store creation).
pub(crate) fn render_empty_base(generation: u64) -> Vec<u8> {
    BaseWriter::new(Header {
        flags: 0,
        node_count: 0,
        seg_count: 0,
        edge_count: 0,
        token_count: 0,
        generation,
        wal_applied_seq: 0,
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
    let new_generation = old_generation.max(state.disk_generation) + 1;

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
                attrs: {
                    let mut a = Vec::new();
                    snap.attrs(key, |k, v| a.push((k, CompactString::from(v))));
                    a
                },
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
                attrs: Vec::new(),
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
            crate::text::for_each_term(text, |tok| {
                let hash = crate::hash::xxh3_64(tok.as_bytes());
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

    // Reverse CSR (sources ascending per destination: rows are visited in
    // source order) and per-node attribute runs over one blob heap.
    let mut inbound: Vec<Vec<(u32, u8, u8, u16)>> = vec![Vec::new(); n];
    for (src, rows) in edge_rows.iter().enumerate() {
        for &(dst, etype, flags, weight) in rows {
            inbound[dst as usize].push((src as u32, etype, flags, weight));
        }
    }
    let mut in_index = Vec::with_capacity((n + 1) * 4);
    let mut in_bytes = Vec::with_capacity(edge_total as usize * base::EDGE_LEN);
    let mut in_total = 0u32;
    for rows in &inbound {
        in_index.extend_from_slice(&in_total.to_le_bytes());
        for &(src, etype, flags, weight) in rows {
            in_bytes.extend_from_slice(&src.to_le_bytes());
            in_bytes.push(etype);
            in_bytes.push(flags);
            in_bytes.extend_from_slice(&weight.to_le_bytes());
            in_total += 1;
        }
    }
    in_index.extend_from_slice(&in_total.to_le_bytes());
    let mut attr_index = Vec::with_capacity((n + 1) * 4);
    let mut attr_bytes = Vec::new();
    let mut blobs = Vec::new();
    let mut attr_total = 0u32;
    for m in models.values() {
        attr_index.extend_from_slice(&attr_total.to_le_bytes());
        for (k, v) in &m.attrs {
            attr_bytes.extend_from_slice(&[*k, 0, 0, 0]);
            attr_bytes.extend_from_slice(&(blobs.len() as u32).to_le_bytes());
            attr_bytes.extend_from_slice(&(v.len() as u32).to_le_bytes());
            blobs.extend_from_slice(v.as_bytes());
            attr_total += 1;
        }
    }
    attr_index.extend_from_slice(&attr_total.to_le_bytes());

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
    // Search indices over the lexicon (index = position in hash order):
    // string order for prefixes, byte trigrams for substrings.
    let tokens: Vec<&str> = lexicon.values().map(|(t, _)| t.as_str()).collect();
    let mut order: Vec<u32> = (0..tokens.len() as u32).collect();
    order.sort_unstable_by(|&a, &b| tokens[a as usize].cmp(tokens[b as usize]));
    let lex_order: Vec<u8> = order.iter().flat_map(|i| i.to_le_bytes()).collect();
    let mut trigrams: BTreeMap<u32, Vec<u32>> = BTreeMap::new();
    for (i, t) in tokens.iter().enumerate() {
        for w in t.as_bytes().windows(3) {
            let ids = trigrams.entry(crate::hash::trigram(w)).or_default();
            if ids.last() != Some(&(i as u32)) {
                ids.push(i as u32);
            }
        }
    }
    let (mut tri_bytes, mut tri_post) = (Vec::new(), Vec::new());
    for (g, ids) in &trigrams {
        tri_bytes.extend_from_slice(&g.to_le_bytes());
        tri_bytes.extend_from_slice(&((tri_post.len() / 4) as u32).to_le_bytes());
        tri_bytes.extend_from_slice(&(ids.len() as u32).to_le_bytes());
        tri_post.extend(ids.iter().flat_map(|i| i.to_le_bytes()));
    }

    let rank_permille = rank::compute(n, &edge_rows);
    let ranks_bytes: Vec<u8> = rank_permille.iter().flat_map(|r| r.to_le_bytes()).collect();

    let wal_applied_seq = state.last_committed_seq();
    let mut w = BaseWriter::new(Header {
        flags: 0,
        node_count: n as u32,
        seg_count: seg_total,
        edge_count: edge_total,
        token_count: lexicon.len() as u32,
        generation: new_generation,
        wal_applied_seq,
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
        w.add_section(SectionKind::InEdgeIndex, in_index);
        w.add_section(SectionKind::InEdges, in_bytes);
        if attr_total > 0 {
            w.add_section(SectionKind::AttrIndex, attr_index);
            w.add_section(SectionKind::Attrs, attr_bytes);
            w.add_section(SectionKind::Blobs, blobs);
        }
        w.add_section(SectionKind::Ranks, ranks_bytes);
        w.add_section(SectionKind::LexOrder, lex_order);
        w.add_section(SectionKind::Trigrams, tri_bytes);
        w.add_section(SectionKind::TriPostings, tri_post);
    }
    let bytes = w.finish();
    drop(snap); // release the read pin before touching the ring

    // ---- 4. swap files (base first — see module docs), then the WAL ---------
    atomic_write(&base_path(&store.dir), &bytes)?;
    state.disk_generation = new_generation;
    atomic_write(&wal_path(&store.dir), &wal::encode_header(new_generation, now_ms()))?;
    state.reset_after_compact(wal_applied_seq + 1);
    store.due.store(false, Ordering::Release);

    // ---- 5. install into a free ring slot -----------------------------------
    let map = crate::mmap::MappedFile::open(&base_path(&store.dir))?;
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

#[cfg(test)]
mod tests {
    use crate::key::EdgeType;
    use crate::store::Config;
    use crate::writer::DocRecord;
    use crate::{Store, UrlKey};

    fn doc(url: &str) -> DocRecord<'_> {
        DocRecord {
            url,
            url_key: UrlKey::of(url),
            content_hash: 1,
            fetched_at_ms: 1,
            title: None,
            snippet: None,
            etag: None,
            pinned: false,
        }
    }

    /// An edge to a document that has not been ingested still has to resolve to
    /// *something*, or the graph would silently lose the fact that the link
    /// exists. Compaction materializes those targets as stubs: addressable,
    /// countable as in-degree, but excluded from the live set.
    #[test]
    fn a_referenced_but_unknown_target_becomes_a_stub() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::create(dir.path(), Config::default()).unwrap();
        let src = "https://x.dev/a";
        let dst = "https://x.dev/never-ingested";
        {
            let mut w = store.writer().unwrap();
            w.upsert_node(&doc(src)).unwrap();
            w.set_edges(UrlKey::of(src), &[(UrlKey::of(dst), EdgeType::Link, 65_535)]);
            w.commit().unwrap();
        }
        store.compact().unwrap();

        let snap = store.snapshot();
        let stub = snap.node(UrlKey::of(dst)).expect("the target must exist as a stub");
        assert!(stub.is_stub(), "an un-ingested edge target is a stub");
        assert_eq!(stub.url, "", "a stub carries no URL label until it is ingested");
        assert_eq!(snap.len(), 1, "stubs are not live documents");
    }

    /// A stub only exists to anchor an edge. Once nothing points at it, keeping
    /// it would leak a node per removed link, so compaction drops it.
    #[test]
    fn a_stub_is_dropped_once_nothing_references_it() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::create(dir.path(), Config::default()).unwrap();
        let src = "https://x.dev/a";
        let dst = "https://x.dev/never-ingested";
        {
            let mut w = store.writer().unwrap();
            w.upsert_node(&doc(src)).unwrap();
            w.set_edges(UrlKey::of(src), &[(UrlKey::of(dst), EdgeType::Link, 65_535)]);
            w.commit().unwrap();
        }
        store.compact().unwrap();
        assert!(store.snapshot().node(UrlKey::of(dst)).is_some(), "stub present while referenced");

        // Drop the edge, then compact: the stub has nothing holding it up.
        {
            let mut w = store.writer().unwrap();
            w.set_edges(UrlKey::of(src), &[]);
            w.commit().unwrap();
        }
        store.compact().unwrap();
        assert!(
            store.snapshot().node(UrlKey::of(dst)).is_none(),
            "an unreferenced stub must not survive compaction"
        );
    }

    /// Ingesting a document that was previously only a stub must promote it in
    /// place, keeping the inbound edge that created it.
    #[test]
    fn ingesting_a_stub_promotes_it_and_keeps_its_inbound_edge() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::create(dir.path(), Config::default()).unwrap();
        let src = "https://x.dev/a";
        let dst = "https://x.dev/later";
        {
            let mut w = store.writer().unwrap();
            w.upsert_node(&doc(src)).unwrap();
            w.set_edges(UrlKey::of(src), &[(UrlKey::of(dst), EdgeType::Link, 65_535)]);
            w.commit().unwrap();
        }
        store.compact().unwrap();

        {
            let mut w = store.writer().unwrap();
            w.upsert_node(&doc(dst)).unwrap();
            w.commit().unwrap();
        }
        store.compact().unwrap();

        let snap = store.snapshot();
        let promoted = snap.node(UrlKey::of(dst)).unwrap();
        assert!(!promoted.is_stub(), "ingesting a stub must promote it");
        assert_eq!(promoted.url, dst);
        assert_eq!(snap.len(), 2, "both documents are now live");

        // The edge that created the stub still resolves to it.
        let mut it = snap.neighbors(UrlKey::of(src));
        let e = crate::traverse::LendingIterator::next(&mut it).expect("edge survives promotion");
        assert_eq!(e.dst_key, UrlKey::of(dst));
        assert_eq!(e.dst_url, Some(dst), "the edge now resolves to a real URL");
    }
}
