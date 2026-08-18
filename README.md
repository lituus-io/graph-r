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
| Creates    | Fetches pages, extracts, builds the searchable index | Graph nodes (docs + segments), typed edges, ranks, communities, freshness records |
| Updates    | Executes crawls & conditional refreshes (network) | Decides *what/when* to refresh (adaptive TTL due-lists); absorbs deltas tier-scoped |
| Deletes    | Evicts documents from its index          | Tombstones history; prunes at compaction                |
| Lookup     | Hybrid dense+BM25 search over full text  | Query → URL + anchor refs, related, path, communities — zero network |

## Quick start (feature `bridge`)

```toml
graph-r = { path = "../graph-r", features = ["bridge"] }
```

```rust,no_run
# async fn demo() -> Result<(), Box<dyn std::error::Error>> {
use graph_r::{Config, QueryOpts, Store};

let store = Store::create("kb", Config::default())?;
let mut index = link_r::LinkIndex::in_memory()?;   // link-r stays in memory

// 1. Crawl once, absorb into the durable graph.
let report = index.update("https://docs.example.com/").run().await?;
graph_r::bridge::ingest_update(&store, &index, &report)?;

// 2. Look things up locally — URLs + anchors, never bodies.
let snap = store.snapshot();
for hit in snap.query("install on linux", &QueryOpts::default()) {
    println!("{}  {}", hit.url, hit.anchor.map_or("", |a| a.label));
}

// 3. Later: revalidate exactly what the graph says is due (304s are free).
let due = graph_r::bridge::due_urls(&store, graph_r::bridge::now_ms(), 64);
if !due.is_empty() {
    let report = index.refresh().urls(due.iter()).ttl(std::time::Duration::ZERO).run().await?;
    graph_r::bridge::ingest_refresh(&store, &index, &report)?;
}
# Ok(()) }
```

Without `bridge`, graph-r is a standalone embedded graph store: feed it
`DocRecord`/`SegmentRecord`s from any source through `Store::writer()`.

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
- **Structure**: deterministic PageRank + label-propagation communities
  (hub-muted, stable ids), computed at compaction and persisted.
- **Lookups**: IDF-weighted tiered lexical scoring (exact/prefix/substring),
  per-term seed guarantees, hub-refusing bounded BFS, importance fusion, and
  seeds-first rendering that stops inside a token budget. Results are URLs +
  segment anchors (`byte_range` when known) — never content.
- **Segments**: sub-document anchors (heading path, depth, byte range,
  importance) keyed stably by heading, so enrichment scores survive re-crawls.
- **Tiers**: crawl-tier writes never destroy enrichment-tier edges/scores and
  vice versa.
- **LLM seam** (feature `llm`): a vendor-free `Enricher` trait — propose
  segment importance and related edges from any model; confidence < 0.5 is
  dropped, everything is stamped inferred + enrichment-tier. The default path
  never needs a model or a network.

## Features

| Feature  | Adds                                            | Default |
|----------|-------------------------------------------------|---------|
| *(none)* | The embedded graph store, lookups, TTL engine   | ✓       |
| `bridge` | link-r ingestion + due-list refresh loop        |         |
| `llm`    | The `Enricher` trait seam (zero deps)           |         |

Future document types (pdf, xml, …) arrive as feature-gated link-r extractors
and flow through the bridge unchanged — the `DocRecord`/`SegmentRecord` seam
is the stable extension point. No harness coupling anywhere: this is a plain
Rust library, embeddable in any runtime.

## Testing

`cargo test` (unit + hermetic lifecycle + bridge e2e), `cargo test --test
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
