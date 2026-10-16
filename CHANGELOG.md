# Changelog

All notable changes to graph-r are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versioning is
[SemVer](https://semver.org/spec/v2.0.0.html).

## 0.6.0

**One record per transaction.** The log is now one checksummed record per
transaction, so a transaction is atomic on disk, in memory and across
processes. Opening a 0.4 or 0.5 store is a typed refusal (see *Format* below):
drain, then rebuild.

- A commit (or a `transact` block) is sealed into one record — length, tag,
  first sequence, op count and an xxh3 over the rest — and appended with one
  write and one fsync. A crash mid-append leaves a torn record: replay
  ignores it and the next committer cuts it off, so that transaction is
  absent whole and every earlier one present. (0.5 logged one frame per op,
  so a crash could keep a prefix of a transaction.)
- Whole records only, everywhere: a commit, a catch-up from another
  process's appends, an image reload and a replay each publish whole records
  with one move of the overlay frontier, so no snapshot ever holds part of a
  transaction. A catch-up that finds another writer mid-append leaves that
  record for its next look; `committed_len` and `committed_frames_len` keep
  their signatures and now count whole records, so a mirror ships whole
  transactions.
- A record whose checksum holds but whose contents break a rule (an unknown
  op, a lying count, overlapping string lengths, unsorted edges or keys,
  bad UTF-8, non-zero reserved bytes, a sequence regression) is
  `Error::Corrupt` naming its byte offset, and is never cut: 0.5 cut a
  malformed frame as a torn tail, silently dropping every commit after it.
- **Zero-copy log.** The log is read once at open (never mapped: it is cut
  in place) and served as is: the bytes a commit sealed, or a catch-up read,
  become the overlay without decoding into owned ops. Ops are read through
  borrowed views (`OpView`, `UpsertView`, `SegsView`, `EdgesView`,
  `TouchView`, `AttrsView`); strings are `&str` over bytes validated once
  when the record was accepted. Staging encodes straight into the record
  (`RecordBuf`), named attributes included.
- Measured on one machine against 0.5.1: opening a log of 1k / 10k / 100k
  ops is 2.3x / 3.3x / 3.45x faster (100k: 22.2 ms to 6.44 ms); the first
  overlay read of a snapshot 3.4x (492 us to 147 us); a 100-upsert commit
  28% less CPU. Counted: opening 10k ops allocates 23 times (0.5.1: 20 031),
  folding them 13 (20 013), and a steady-state commit allocates nothing of
  its own after staging (21; on Windows the standard library's UTF-16 copy
  of each path it hands the OS adds 2, for the two stats that notice another
  process's compaction). The wheel's extension is no larger than 0.5.1's
  (4 949 712 bytes on macOS arm64; same C-API imports).
- **Commit fixes** (all from 0.5.1): room in the overlay is reserved before
  the append, so a durable commit can no longer go unaccounted and reuse
  its sequence numbers, which left the store unopenable (`wal sequence
  regression`); a failed append keeps the writer's staged ops (a failed
  transaction drops only its own) and forces a full re-read of the log; an
  inline compaction that cannot run leaves `compaction_due()` set instead of
  failing a commit that is already durable.
- A transaction larger than `wal::MAX_RECORD` (256 MiB, about a million
  upserts) is refused at commit with `Error::Format`, before anything is
  written, its ops still staged; `Writer::discard` drops them.
  `set_edges` and `set_edges_with_flags` stay infallible.
- `Store::commit_lock_within(wait)`: a commit lock with a wait of the
  caller's choosing.
- **Format.** Log format 3 and base format 3 (the base layout is unchanged:
  only its version field and header checksum differ from 0.5's). A 0.4/0.5
  store opened by 0.6, and a 0.6 store opened by 0.4/0.5, are refused with
  `Error::Version { found, supported }` — `{2, 3}` and `{3, 2}` — before any
  repair, files untouched. The base is bumped too because every release
  reads it first: a 0.5 writer that found a 0.6 store's log empty would
  otherwise have rewritten it as its own. There is no in-place upgrade and
  no mixing of 0.5 and 0.6 writers on one store: drain, then rebuild. A
  store from the 0.5 line is also the only kind 0.5.x's sequence-reuse
  defect can have damaged; 0.6.0 supersedes it.
- **Removed API** (the frame format): `wal::Op`, `OwnedSeg`, `OwnedEdge`,
  `encode_frame`, `replay_frames`, `MAX_FRAME`, `FRAME_OVERHEAD` and
  `Replay::ops`; `wal::replay` returns a `Replay` that borrows its image and
  walks `records()`. `writer::MAX_LABEL` and `MAX_SEGS` are re-exported from
  `wal`, unchanged; `MAX_ATTR_BYTES` stays 1 048 246.
- The overlay's maps hash a key digest with one finalizer round under a
  per-map secret, instead of SipHash.
- Python: `Store.open`, `open_or_create` and `create` take `lock_wait=None`
  (seconds); `transaction(wait=...)` takes `True`, `False` or seconds. A 0.4
  or 0.5 store raises `FormatVersionError(found=2, supported=3)`.
- Tests: a reader in one process never sees part of another process's
  transaction (fails on 0.5.1); a crash cut anywhere in a real record loses
  that transaction whole; the model proptest cuts a log of multi-op
  transactions at any byte and requires exactly the first k; refusals leave
  every file byte-identical; counted allocations; new fuzz targets
  `fuzz_record_decode` and `fuzz_crash_cut`; `scripts/xver`, run in CI, has
  the released 0.5.1 open 0.6 stores (an empty log included) and requires
  `Version { found: 3, supported: 2 }` with the files unchanged.
- Builds with uu-link-r 0.4.3 and uu-objstore-r 0.2.5.

## 0.5.1

**Fair commit lock.** In 0.5.0 a writer waiting for the commit lock polled it
with backoff up to 50 ms, while a writer committing back to back held it
through each fsync and freed it for microseconds. The waiter almost never
landed in that gap: next to one busy writer an occasional commit waited 1.5 s
at the median and 5.6 s at worst, some were refused with `Locked` after the
full `lock_wait`, and next to two busy writers one of those starved too.

- A writer that has to wait now announces itself with a shared lock on a
  new file, `graph.wait`, for as long as it waits. A writer that finds
  anyone announced does not take the commit lock ahead of them, even when it
  is free: it joins the queue and stands back one pause, in which every
  earlier waiter retries. A no-wait call (`try_transact`, `try_commit_lock`,
  Python `wait=False`) is refused with `Locked` rather than jump the queue.
- Retries back off from 250 µs to 2 ms (was 1 ms to 50 ms), so a freed lock
  is taken within about 2 ms under contention.
- Measured with one busy writer: the occasional commit waits 4.9 ms at the
  median and 14 ms at worst, and none is refused; two busy writers now split
  the lock evenly. Uncontended, a commit is one probe of `graph.wait` plus
  the one attempt on the commit lock it was before, with no measurable
  change in commit latency (2.96 ms median, fsync-bound).
- `graph.wait` is opened like `graph.lock`: a fixed name in the store
  directory, and a non-regular file there (a planted symlink) is refused,
  never followed. Writers from 0.5.0 and 0.4.x ignore it; exclusion is still
  the commit lock alone, so mixing versions stays correct and only the
  older writers are not fair.
- New multi-process test: two writers committing back to back and a third
  with a 300 ms `lock_wait`; all 40 of the third's commits go through. On
  0.5.0's lock it got 1 of 40.

## 0.5.0

Concurrent writers. The on-disk format is unchanged — base and log bytes are
identical to 0.4.x for the same ops, and 0.4.x stores open as they are.

**Many writers per store.** Opening a store read-write takes no lock any
more: any number of writable handles, in one process or many, may be open on
one directory. What is exclusive is the commit. `Writer::commit`,
`Store::transact` and compaction take the `flock` on `graph.lock` only around
their append (and fsync), retrying a busy lock with exponential backoff (1 ms
doubling to 50 ms) for up to the new `Config::lock_wait` (default 10 s), then
`Error::Locked` — which now means "waited past `lock_wait`", with the ops
still staged. Under the lock a committer first catches up: two `stat`s; a new
base (another handle compacted) is installed through the generation ring; a
grown log has only its appended bytes read; the sequence counter advances to
the file's. Sequence numbers stay strictly increasing across every writer.

- `Store::transact(|tx| ...)`: a serializable read-modify-write. The lock is
  taken and the handle caught up before the closure runs and held until its
  single append — one lock and one fsync however many ops it stages.
  `Txn::snapshot()` is the caught-up committed state; `Err` from the closure
  aborts with nothing written. `Store::try_transact` never waits (`Locked` at
  once), for callers that retry on their own schedule.
- `Store::commit_lock` / `try_commit_lock` + `Store::transact_with`: the same
  transaction across several calls (what the Python binding's `with` block
  uses). `CommitLock` and `Txn` are exported.
- `Store::open_or_create`: safe when many processes race to create one store.
- A torn tail left by a writer that died mid-append is truncated by the next
  committer, under the lock (and at open when the lock happens to be free).
- `reload_if_stale` now works on writable handles too; on one whose `Writer`
  is busy on another thread it returns `false` instead of waiting.
- A failed append is cut back off the log, so it writes nothing.
- A `graph.lock` that is not a regular file (a planted symlink) is refused
  rather than followed.
- Readers never wait: a writer parked on the lock holds nothing a snapshot
  needs.

**Fixed: data loss on concurrent first creation.** Two processes creating
the same store at the same moment could both lay down a fresh base and log;
the later one replaced a store the other had already committed to, silently
dropping that commit (and a racing opener could fail with "No such file or
directory"). Creation now happens once, under the commit lock, never
replacing a file that exists — the log is written before the base, so a base
on disk always has its log beside it — and a racing `open_or_create` waits
for the creator and opens the result.

**Behaviour changes.** A second `Store::open` on a directory with a live
writable handle no longer fails with `Error::Locked` — it opens. Code that
relied on the open-time lock for exclusion must use the commit lock
(`commit_lock`) instead. `Config` gained a field, so a struct literal needs
`..Config::default()`. Contention is polled, not queued: under sustained
back-to-back commits a waiter is not guaranteed a turn before `lock_wait`.
The log has no batch marker, so a power loss during a multi-op append can
still leave a prefix of it (as a multi-op commit always could).

**Python**
- Writable `Store`s coexist across processes; each write method is a one-op
  transaction whose checks (`set_edges`, `set_attrs`) run under the lock.
- `with store.transaction(wait=True) as tx:` — the lock is held from
  `__enter__` to `__exit__`; `tx` has the store's read and write methods, its
  writes commit together at exit (one append, one fsync) or are dropped when
  the block raises. Plain writes on the same store wait for the block from
  other threads and raise from the block's own thread.
- `graph_r.LockedError(GraphRError)`: a typed "retry later", so callers stop
  matching message text.
- `open_or_create` is safe under concurrent creation (the fix above).

## 0.4.2

Additive release: format v2 unchanged, every 0.4.1 API kept.

**Fixed: a large attribute set lost data on the next open.** Each attribute
value was capped at 256 KiB but a node's whole set was not, so a set whose
values totalled more than about 1 MiB (four maximal values) was written as
one log record over the 1 MiB record limit. The commit succeeded and the values read back in that session, but
the next writable open treated the oversized record as a torn tail and
truncated the log there — silently discarding that attribute set **and every
commit made after it**. `Writer::set_attrs` now refuses a set over
`writer::MAX_ATTR_BYTES` (what fits one record) before anything is staged.
Stores that never wrote such a set are unaffected.

**Python: the lineage model, enumeration, a typed format error, `close()`**
- `Store.set_edges(url, [(dst, kind, exact)])` with `kind` in `contains`,
  `derives`, `conditions` and `exact` the copy flag: a whole-set replacement
  of `url`'s outbound edges (never a merge; `[]` clears). `url` and every
  destination must be committed documents (`GraphRError`); an unknown kind,
  a self-edge or more than 4096 edges is a `ValueError`.
- `Store.out_edges(url)` / `Store.in_edges(url)`: `(url, kind, exact)`
  lists. Both see an edge as soon as `set_edges` returns — `in_edges` reads
  the reverse index from the last compaction plus every edge committed since.
- `Store.walk(url, direction="out"|"in"|"both", depth=3, kinds=None,
  limit=None)`: breadth-first, ordered by distance then url, each node once,
  the start excluded, at most 10 000 urls by default. Built on
  `Snapshot::walk` with a per-thread reusable `Walk`, outside the GIL.
- `Store.set_attrs(url, {name: value})` / `Store.attrs(url)`: string
  attributes (up to 64 names, 255-byte names, 256 KiB per name+value, 1 MiB
  per node); `{}` for an unknown url.
- `Store.urls(prefix="")`: every live document url under a prefix, sorted —
  exhaustive where `query` is ranked. One pass over the documents.
- `Store.close()`: releases the mapping, the writer lock and the runtime now;
  later calls raise `GraphRError("store is closed")`; idempotent.
- `FormatVersionError(GraphRError)` with `found` and `supported`, raised by
  every open (and `reload_if_stale`) on a store of another format version.
- Type stubs cover the whole `Store` surface.
- Ids are canonicalised exactly as `add` stores them, which drops a
  `#fragment`: `bq://p/d/t#c` and `bq://p/d/t` are one document. Give a
  column its own path segment. Now stated in the README and pinned by a test.

**Rust**
- `Error::Version { found, supported }`: a well-formed file of another format
  version (base or log) is now its own variant instead of `Error::Format`.
  Its message keeps the `format error: unsupported version N` prefix. A
  caller that matched `Error::Format` to detect an old store should match
  `Error::Version`.
- `Writer::set_named_attrs` / `Snapshot::named_attrs`: string-named
  attributes in one shared encoding (`name NUL value`, keys in name order),
  read back borrowed; `writer::MAX_ATTR_NAME`.
- `Snapshot::walk_both`: one walk along outbound and inbound edges, each node
  at its shortest distance either way.

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
