# graph-r

An embedded, persistent knowledge graph that acts as the durable backend for
[link-r](../link-r): link-r **acquires and ranks** (crawl, extract, hybrid
search over a possibly in-memory index); graph-r **remembers and serves**
(historical graph, adaptive freshness, token-budgeted local lookups). The two
share one foreign key — the 64-bit xxh3 of a canonical URL — so a link-r index
can be absorbed and then discarded while every lookup keeps resolving.

The point: answer "where is the resource that explains X?" **locally**, with a
URL and a heading anchor, inside a token budget — instead of re-crawling,
re-searching, or shipping page bodies to a model on every question.

## Division of responsibilities

|            | **link-r** (acquire & rank)              | **graph-r** (remember & serve)                          |
|------------|------------------------------------------|---------------------------------------------------------|
| Creates    | Fetches pages, extracts, builds the searchable index | Graph nodes (docs + segments), typed edges, ranks, freshness records |
| Updates    | Executes crawls & conditional refreshes (network) | Decides *what/when* to refresh (adaptive TTL due-lists); absorbs deltas tier-scoped |
| Deletes    | Evicts documents from its index          | Tombstones history; prunes at compaction                |
| Lookup     | Hybrid dense+BM25 search over full text  | Query → URL + anchor refs, related, lineage walks — zero network |

## Quick start (with link-r)

```toml
uu-graph-r = "0.4"
uu-link-r = { version = "0.4", features = ["graph"] }   # the crawl → graph bridge
```

```rust,no_run
# async fn demo() -> Result<(), Box<dyn std::error::Error>> {
use graph_r::{Config, QueryOpts, Store};

let store = Store::create("kb", Config::default())?;
let mut index = link_r::LinkIndex::in_memory()?;   // link-r stays in memory

// 1. Crawl once, absorb into the durable graph.
let report = index.update("https://docs.example.com/").run().await?;
link_r::bridge::ingest_update(&store, &index, &report)?;

// 2. Look things up locally — URLs + anchors, never bodies.
let snap = store.snapshot();
for hit in snap.query("install on linux", &QueryOpts::default()) {
    println!("{}  {}", hit.url, hit.anchor.map_or("", |a| a.label));
}

// 3. Later: revalidate exactly what the graph says is due (304s are free).
let due = link_r::bridge::due_urls(&store, graph_r::fsio::now_ms(), 64);
if !due.is_empty() {
    let report = index.refresh().urls(due.iter()).ttl(std::time::Duration::ZERO).run().await?;
    link_r::bridge::ingest_refresh(&store, &index, &report)?;
}
# Ok(()) }
```

Without link-r, graph-r is a standalone embedded graph store: feed it
`DocRecord`/`SegmentRecord`s from any source through `Store::writer()`.

## One knowledge base: graph-r + link-r on object storage (link-r feature `kb`)

```rust,ignore
use link_r::kb::{KnowledgeBase, LookupOpts, WriterOptions};

let gcs = objstore_r::Gcs::new("my-bucket", objstore_r::token::Adc::discover()?, Default::default())?;

// Writer (one instance, lease-fenced): conditional crawl -> bridge -> publish.
let mut kb = KnowledgeBase::writer(&gcs, "kb/docs", tmpfs_dir, embedder, WriterOptions::new("instance-7")).await?;
kb.sync_crawl(link_r::HttpFetcher::new(link_r::AnonymousAuth)?, "https://docs.example.com/", |c| c.depth(3)).await?;
kb.refresh_due(&fetcher, graph_r::fsio::now_ms(), 256).await?;   // adaptive freshness

// Replicas (any number, no disk): hydrate in place, refresh cheaply, look up locally.
let mut replica = KnowledgeBase::replica(&gcs, "kb/docs", embedder).await?.expect("published");
replica.refresh().await?;
for hit in replica.lookup("install on linux", &LookupOpts::default()).await? {
    println!("{} {:?}", hit.url, hit.anchor);   // URL + heading anchor, never the body
}
```

Measured on a live bucket: replica hydration ~80 ms, unchanged refresh ~27 ms
(two concurrent metadata requests), fused lookup ~0.2 ms. At 1 000 pages in
memory: hydration 1 ms, fused lookup 0.6 ms, graph query 0.09 ms.

## Durable on object storage (objstore-r feature `mirror`)

[objstore-r](https://github.com/lituus-io/objstore-r)'s `mirror` module keeps
a store directory in sync with Google Cloud Storage (or a local directory, or
memory). graph-r itself is untouched: the mirror moves
its files byte for byte, so a stateless instance can keep the store on tmpfs
and still lose nothing.

```rust,ignore
use objstore_r::mirror::Mirror;

let objects = objstore_r::Gcs::new("my-bucket", objstore_r::token::Adc::discover()?, Default::default())?;
let mirror = Mirror::new(&objects, "kb/runbooks")?;

mirror.pull_into(dir).await?;                 // rebuild graph.base + graph.wal (verified)
let store = graph_r::Store::open(dir)?;       // the unchanged API
mirror.acquire_lease("instance-7", 60_000).await?;
// ... writer.commit() as usual ...
mirror.publish(&store).await?;                // ships only the new WAL frames
```

Remote layout: `HEAD` (a checksummed manifest, compare-and-swap only),
`base/{generation}.base`, and `wal/{generation}/{start}-{end}.seg`. A second
writer publishing from the same state is fenced; replicas follow with
`refresh_into` (one metadata request when nothing changed). For flat commit
latency, `Store::set_compact_inline(false)` plus `Store::compaction_due()`
move compaction off the commit path.

## What's inside

- **Storage**: two files. `graph.base` — an immutable, mmapped, checksummed,
  sectioned snapshot (fixed-width sorted node records, CSR edges, label heap,
  lexicon + delta-varint postings, ranks) read zero-copy; `graph.wal` — an
  append-only op log with per-frame checksums, torn tails truncated on open.
  Compaction folds both into a new byte-reproducible base via atomic rename.
- **Concurrency**: any number of snapshot readers, one writer (in-process
  mutex + cross-process `flock`). Snapshots are borrow guards over a pinned
  generation — readers never block on commits, and compaction swaps
  generations under them. No `Arc`, no async runtime, one `unsafe` island
  (mmap + flock).
- **Freshness**: every document carries an adaptive revalidation interval —
  grown 1.5× per unchanged check, cut 4× on change, clamped, biased by
  importance — and `due()` emits the work list, with stored ETags for
  conditional revalidation.
- **Structure**: deterministic PageRank at compaction, fused into lookups.
- **Lookups**: IDF-weighted tiered lexical scoring (exact/prefix/substring),
  per-term seed guarantees, hub-refusing bounded BFS, importance fusion, and
  seeds-first rendering that stops inside a token budget. Results are URLs +
  segment anchors (`byte_range` when known) — never content.
- **Segments**: sub-document anchors (heading path, depth, byte range,
  importance) keyed stably by heading, so a re-crawl keeps their identity.
- **Lineage**: containment, value derivation (exact copies flagged) and row
  conditions as typed edges with reverse adjacency; one allocation-free
  `walk` up or down; large per-node attributes (SQL text, types, provenance).

## Features

| Feature  | Adds                                                       | Default |
|----------|------------------------------------------------------------|---------|
| *(none)* | The shared core: xxh3, keys, byte codecs, container, files, text rules | |
| `mmap`   | Zero-copy images: memory-mapped files or bytes in place    |         |
| `store`  | The embedded graph store, lookups, lineage walks, TTL engine | ✓     |

The link-r ingest path is link-r's `graph` feature, object-store publishing
is objstore-r's `mirror` feature, and both together are link-r's `kb`
feature: graph-r's core depends on neither, so the four crates form no
cycle.

Future document types (pdf, xml, …) arrive as feature-gated link-r extractors
and flow through link-r's bridge unchanged — the `DocRecord`/`SegmentRecord` seam
is the stable extension point. No harness coupling anywhere: this is a plain
Rust library, embeddable in any runtime.

## Testing

`cargo test` (unit + hermetic lifecycle), `cargo test --test
proptest_model` (model-based agreement through compaction/reopen,
byte-reproducibility, torn-WAL recovery), `cargo test --test concurrency`
(8 readers under a compacting writer), `cargo bench` (criterion, including
read-latency-under-write), `cargo +nightly fuzz run <target>` (every decoder:
arbitrary bytes must produce a typed error, never a panic or an
over-allocation).

## Python

The same loop, one class, from PyPI (`pip install uu-graph-r`, then
`import graph_r`; Python >= 3.12, one abi3 wheel per platform):

```python
import graph_r

store = graph_r.Store.open_or_create("kb")
store.sync("https://docs.example.com/", depth=2)   # crawl -> absorb -> compact
for hit in store.query("install on linux"):
    print(hit.url, hit.anchor.label if hit.anchor else "")
```

`sync` seeds a fresh in-memory link-r index from the graph's stored
validators, so a re-sync of an unchanged site transfers zero bodies — from a
cold start included. The index is discarded when the call returns; bodies and
vectors never outlive it. Safe from any thread (`asyncio.to_thread` works);
blocking calls detach from the interpreter. See `bindings/python/graph_r.pyi`
for the full surface (`add`/`touch` for non-crawl producers, `due`, `related`,
`pin`, `remove`, `compact`).

## License

Dual-licensed:

- **AGPL-3.0-or-later** for open-source use. See [LICENSE](LICENSE).
- **Commercial**, for use in proprietary or closed-source software without the
  AGPL's copyleft requirements. Contact spicyzhug@gmail.com.

Copyright (c) 2024-2026 Lituus-io. All rights reserved.
