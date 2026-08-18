# Changelog

All notable changes to graph-r are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versioning is
[SemVer](https://semver.org/spec/v2.0.0.html).

## 0.2.1

### Python bindings

- `Store.reload_if_stale()` is now exposed: read-only handles pick up commits
  and compactions made by a writing process — cheap when nothing changed (a
  header comparison, no remap), returning True when a new generation or fresh
  WAL frames became visible. The Rust API always had it; the binding gap made
  long-lived readers (e.g. an MCP server holding a read-only store while a
  background sync writes) serve stale results until reopen.

## 0.2.0

### GitHub repositories as a first-class source

`Store.sync` now routes by source form: a GitHub repository spec
(`https://github.com/owner/repo[/tree/ref[/dir]]` or
`github:owner/repo@ref[//dir]`) goes through link-r's new tree-API source —
one API call lists every file with its blob SHA, files whose SHA the graph
already stores are revalidated **without any fetch**, and only new/changed
blobs transfer. An unchanged repository costs exactly one HTTPS request per
sync, from a cold start included: the SHAs ride the existing `etag` →
`crawl_seed` → validators plumbing, unchanged.

- `depth` means directory levels below the subdir on this path (link hops on
  the crawl path, as before); `max_bytes` skips oversized files from the
  tree's own size field, no fetch issued.
- `token` (a PAT) reaches only the GitHub API and raw hosts — never a host a
  document links to. Public, private, and internal repositories all work.
- `api_base`/`raw_base` (set together) target GitHub Enterprise.
- Crawl-only options passed with a GitHub spec raise `ValueError` rather than
  being silently ignored.

### Cross-platform persistence and memory, tested

- The binding CI job now runs the full suite on Linux, macOS, and Windows on
  every push. New `test_persistence.py` pins the file contract per-OS:
  write→close→reopen→write-again cycles, stores under paths with spaces and
  non-ASCII characters, fifty open/close cycles proving the lock releases
  every time, a cross-process write via a child interpreter, and
  `os.replace` stamp atomicity beside the store.
- New `test_memory.py` pins the harness read path's zero-growth property:
  hundreds of query/due cycles — and a reader running under a live writer —
  hold RSS flat and leave no Python-heap accumulation.

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

### The stateless loop and the Python wheel

- `bridge::crawl_seed` extracts the graph's stored validators and edges in
  link-r's vocabulary, so a *fresh* index revalidates a known site with zero
  body transfers — the "absorb and discard the index" design now survives a
  process restart.
- Change detection in the bridge is graph-relative: a changed page arriving as
  `Added` from an ephemeral index still cuts its refresh interval, because the
  committed content hash — not the crawl's index — decides what changed.
- Python bindings (`pip install graph-r`, abi3, Python ≥ 3.12): one `Store`
  class carrying the whole loop — `sync` (crawl → absorb → compact in a single
  call, interpreter detached), `query`/`related`/`due`, the `add`/`touch`
  writer seam, `pin`/`remove`/`compact`. Usable from any thread.

### Verification

- Unit, hermetic lifecycle, bridge end-to-end, and concurrency-stress suites.
- A model-based property test checking arbitrary op sequences against a
  reference model through compaction and reopen, plus byte-reproducible
  compaction and torn-WAL-at-any-byte recovery.
- Four fuzz targets over every decoder: arbitrary bytes must produce a typed
  error, never a panic or an over-allocation.
- Criterion benchmarks, and zero clippy warnings across all features and
  targets.
