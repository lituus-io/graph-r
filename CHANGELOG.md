# Changelog

All notable changes to graph-r are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versioning is
[SemVer](https://semver.org/spec/v2.0.0.html).

## 0.4.1

- Security: every lockfile (crate, Python binding, fuzz) refreshed to
  patched releases, among them rustls 0.23.46 (RUSTSEC-2026-0346, -0347,
  -0348, -0349) and the earlier rustls and quinn-proto fixes
  (RUSTSEC-2026-0285, -0338 to -0342). The PyPI wheels are built from these.
- The Security workflow audits all three lockfiles, not only the crate's.
- CI: store tests and benches gated on the `store` feature; a regression
  test replaces the base file by rename, as the store does (Windows).

## 0.4.0

Breaking: base format v2 and WAL format v2 (stores from 0.3 must be rebuilt
or re-ingested); several unused APIs removed. Every capability a consumer
(kb, mirror, the Python binding) uses is kept.

**One foundation for the stack**
- The core (no features: xxh3 alone) is now what link-r, objstore-r and
  crab-r build on: `text` (tokenizer, case folding, char n-grams, stopwords,
  `estimate_tokens` / `bytes_for_tokens`) moved here from link-r and is the
  rule graph-r's lexicon and link-r's BM25 both use; `hash::fmix64` and
  `hash::trigram`; `fsio::{lock, sync_dir, write_atomic_with}`;
  `bytesio::{put_f32, put_len_prefixed}` and `Reader::{f32, len_prefixed}`;
  `mmap::cast_f32`; per-section flags in `container`
  (`Writer::section_flagged`, `Dir::{section_flags, has}`).
- Published as **uu-graph-r** on crates.io (the library is still
  `graph_r`), one self-contained crate. Its satellites moved to the crates
  they compose with, so the four libraries form no dependency cycle: the
  link-r bridge is `link_r::bridge` (uu-link-r feature `graph`; the `bridge`
  feature here is gone), the knowledge base is `link_r::kb` (feature `kb`),
  and store mirroring is `objstore_r::mirror` (uu-objstore-r feature
  `mirror`), built on the shared `objstore_r::fenced` protocol.
  `graph_r::store::now_ms` is `graph_r::fsio::now_ms`.

**Fixed (independent review)**
- A read-only reader that caught a compaction between its two renames
  reported `Corrupt`; it now re-reads and, if the window persists, returns
  `Busy`. Only a writable open still treats a newer log as damage.
- A compaction refused in memory (every ring slot pinned) left a durable
  base whose generation number the next compaction reused; readers then kept
  the first base and skipped ops by count. Generations are now never reused,
  and readers diff the log by sequence.
- An empty log on a writable open (external truncation) made the next
  commit write a headerless file; the header is now restored.
- `reload_image` had no guard around the single-producer overlay; reloads
  are now one at a time per handle, and both reloads are no-ops on writable
  handles.
- The bridge never grew the interval of a known, unchanged page re-fetched
  without a validator (no ETag), so it was re-crawled at the base interval
  forever.

**Faster, cheaper**
- `reload_if_stale` reads only the log bytes appended since the last reload.
- Queries look tokens up through the hash order, a string-order index and a
  trigram index instead of scanning the vocabulary, and score into reused
  dense buffers: 9× faster at 10k documents, 2.2 ms at 100k with every
  document matching. `due` selects instead of sorting everything.
- The bridge compacts after an ingest only when it added or changed
  documents.
- Opening validates each byte once (header, directory, section checksums; no
  whole-file trailer) and finds sections in O(1). Mirror no longer validates
  a base twice.
- Community detection, computed at every compaction and read by nothing,
  is gone; rank records shrink from 8 to 2 bytes.

**New**
- Lineage model: `EdgeType::{Contains, Derives, Conditions}`, the
  `eflags::EXACT` copy flag (`Writer::set_edges_with_flags`), inbound edges
  (`Snapshot::in_edges`, written as a reverse CSR at compaction), node
  attributes for large values (`Writer::set_attrs`, `Snapshot::attr(s)`),
  and one reusable, allocation-free walk (`Snapshot::walk` with `Walk`,
  `Direction::{Out, In}`). Up to 4096 edges per node.
- A core build (`default-features = false`) of hashing, keys, byte codecs,
  `container` (the checksummed sectioned file framing) and `fsio` (atomic
  writes, the cross-process lock, the clock), with xxhash-rust as the only
  dependency; `mmap` exposes `MappedFile`; `store` (default) is the store.

**Removed** (no consumer used them): the `llm` feature and `enrich`, segment
importance overrides, edge tiers, `community_summary`, `surprises`, `path`
(use `walk`), `pending_ops`, `Writer::staged`, `Snapshot::check`, the
`*_with` bridge variants (the plain ones are generic over the embedder), and
the `libc` and `thiserror` dependencies (std's `File::try_lock` now holds the
writer lock on every platform, released by the OS on crash).

- `hash`: the crate's xxh3 family (`xxh3_64`, `xxh3_64_seeded`, `xxh3_128`,
  streaming `Hasher`) as thin public wrappers, so dependants fingerprint with
  the exact function behind `UrlKey` without a second hashing crate.

## 0.3.0

Additive release: every 0.2 API, file format and default behaviour is
unchanged (`cargo semver-checks` against 0.2.1: no breaking change), and
stores written by 0.2.1 open and read identically (golden fixtures in
`tests/fixtures/v0.2.1`, including byte-identical compaction output and WAL
frames).

### New: `graph-r-mirror` (separate crate in `mirror/`)

Keeps a store directory in sync with an object store (Google Cloud Storage,
a local directory, or memory) through objstore-r. The files are moved byte
for byte, so a store can live on tmpfs on a stateless instance and be durable
in a bucket: `pull_into`, incremental `publish` (only new WAL frames; the new
base after a compaction; coalescing of long segment runs), `refresh_into` for
read-only replicas, a `HEAD` compare-and-swap that fences a second writer,
and an optional writer lease. `load_image` / `refresh_image` keep a replica
entirely in memory; the base and all segments are fetched concurrently. It is a separate crate so graph-r's own
manifest gains no dependency.

### New: `graph-r-kb` (separate crate in `kb/`)

graph-r + link-r as **one knowledge base** on one object store: a writer
(lease-holding, graph on tmpfs, durability in the bucket) crawls
conditionally — validators and known links come from the graph, so unchanged
pages cost a 304 — absorbs through the bridge, and publishes both halves
concurrently, fenced; `refresh_due` revalidates exactly what the graph's
adaptive freshness says is due. A replica needs no disk: the graph base and
the link index are served in place from the fetched bytes, and refreshes cost
two metadata requests when nothing changed. `lookup` fuses graph-r's
anchored, graph-aware ranking with link-r's hybrid search (reciprocal-rank
fusion; the graph work overlaps a remote query embedding). Tested end to end
on memory, local-directory, fake-gcs-server and a live bucket, with a
writer-vs-replica property test.

### New API

- `Store::open_image(base, wal)` and `Store::reload_image`: read-only stores
  served straight from memory (e.g. objects fetched from a bucket) — the base
  is used in place, fully validated, no file or mapping; `Store::is_image`.
- `bridge::{absorb_with, ingest_update_with, ingest_refresh_with}`: the
  bridge for link-r indexes built with any embedder.
- `Store::dir()`, `Store::generation()`.
- `Store::set_compact_inline(bool)` and `Store::compaction_due()`: move
  compaction off the commit path (inline remains the default).
- `format::wal::committed_len` and `committed_frames_len`: the intact prefix
  `replay` would report, without collecting ops.
- `store::{BASE_FILE, WAL_FILE, LOCK_FILE}`.

### Fixes and performance

- Loading is one validated code path for files and images; replaying a
  generation no longer clones every op into the overlay twice.
- `Store::reload_if_stale` is now cheap when nothing changed (two `stat`
  calls; no remap, no read — the 0.2.1 notes promised this, but every call
  re-mapped the base and re-read the whole WAL), and re-reads only the log
  when only the log grew.
- Temporary files are named by pid **and** a process-wide counter and created
  exclusively, so two stores or threads in one process can never collide on a
  temporary, and a leftover from a crash is never reused.
- Windows: the writer lock is now an OS file lock (`LockFileEx` via std),
  released by the kernel when the process dies. 0.2 created the lock file
  exclusively and relied on deleting it, so a crashed writer left the store
  locked until someone removed `graph.lock` by hand. (Do not run a 0.2 and a
  0.3 writer on the same directory concurrently on Windows; unix is
  unaffected — both use `flock`.)
- WAL framing hashes `seq‖payload` by streaming (identical checksums) instead
  of building a concatenated copy per frame, on both encode and replay.

### Security

- Lockfiles (crate and Python binding) move to rustls 0.23.45 for
  RUSTSEC-2026-0285 (TLS 1.3 handshake messages accepted across encryption
  levels); reached through link-r's HTTP fetcher under `bridge`.

### Tooling

- New fuzz targets `fuzz_wal_committed_len` and `fuzz_open_image` (and `fuzz_manifest` in
  `mirror/fuzz`).
- CI: a mirror job (memory, local directory, GCS emulator), a semver job
  (additive-only against the previous tag), the 0.2.1 golden suite in the
  security workflow, and `scripts/ci-local.sh` running every gate locally.
- `rust-version` is now 1.89 (std file locking); the pinned nightly is
  unchanged.

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
