# Changelog

All notable changes to graph-r are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versioning is
[SemVer](https://semver.org/spec/v2.0.0.html).

## 0.1.0

Initial release: an embedded, persistent knowledge graph that serves as the
durable backend for [link-r](https://github.com/lituus-io/link-r).

### Storage

- Two files. `graph.base` — an immutable, mmapped, checksummed, sectioned
  snapshot (sorted fixed-width 72-byte node records for zero-copy binary search,
  CSR edges, label heap, lexicon with delta-varint postings, persisted ranks).
  `graph.wal` — an append-only op log with per-frame xxh3; torn tails are
  truncated on open.
- Compaction folds base + WAL into a new **byte-reproducible** base via atomic
  rename. A crash at any point in the sequence recovers: WAL replay skips
  sequences already folded into the base.
- Every file is validated end to end before a single record is read — magic,
  version, header and per-section checksums, directory bounds and alignment,
  label-heap offsets, CSR monotonicity, edge destinations, lexicon ordering, and
  node sortedness — which is what makes the zero-copy accessors infallible.

### Concurrency

- Any number of snapshot readers against a 4-slot generation ring; one writer,
  serialized in-process by a mutex and across processes by `flock`.
- Snapshots are lifetime-bound borrow guards: no `Arc`, no async runtime, and a
  single audited `unsafe` island (`src/os.rs`: mmap + flock) under a crate-wide
  `#![deny(unsafe_code)]`.

### Freshness and intelligence

- Adaptive per-node revalidation interval — grown on an unchanged check, cut
  sharply on change, clamped, and biased by importance — with `due()` work-lists
  carrying stored ETags for conditional revalidation. Tombstones preserve the
  history of sources that have gone away.
- Deterministic PageRank and hub-muted label-propagation communities with stable
  ids, computed at compaction and persisted.
- Token-budgeted lookups that answer with URLs and heading anchors, never
  document bodies: IDF-weighted tiered lexical scoring, per-term seed
  guarantees, hub-refusing bounded BFS, and seeds-first rendering.

### Tiers and extension seams

- Crawl-tier writes never destroy enrichment-tier edges or scores, and vice
  versa; segments are keyed by heading path so enrichment survives a re-crawl.
- Features: `default = []` (the pure embedded store), `bridge` (link-r ingest
  and due-list refresh loop), `llm` (a vendor-free `Enricher` trait seam).

### Verification

- Unit, hermetic lifecycle, bridge end-to-end, and concurrency-stress suites.
- A model-based property test checking arbitrary op sequences against a
  reference model through compaction and reopen, plus byte-reproducible
  compaction and torn-WAL-at-any-byte recovery.
- Four fuzz targets over every decoder: arbitrary bytes must produce a typed
  error, never a panic or an over-allocation.
- Criterion benchmarks, and zero clippy warnings across all features and
  targets.
