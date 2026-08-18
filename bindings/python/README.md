# graph-r

`pip install uu-graph-r` → `import graph_r` (Python ≥ 3.12, one `abi3` wheel
per platform).

An embedded, persistent knowledge graph. Point it at a URL once; it crawls,
distills each page into compact lookup metadata (title, snippet, heading
anchors, links), and remembers. From then on, lookups answer **locally** —
with URLs and anchors inside a token budget, never document bodies — and
`sync` keeps the graph fresh for almost nothing: stored `ETag` validators are
replayed on every crawl, so unchanged pages answer `304` and transfer no body,
even after a process restart.

```python
import graph_r

store = graph_r.Store.open_or_create("kb")
store.sync("https://docs.example.com/", depth=2)   # crawl → absorb → compact

for hit in store.query("install on linux"):
    print(hit.url, hit.anchor.label if hit.anchor else "", hit.score)

store.sync("https://docs.example.com/", depth=2)   # unchanged: zero bodies
```

- **One class, one loop.** `Store.sync` seeds an in-memory crawler from the
  graph, crawls, absorbs, compacts, and discards the crawler — bodies and
  vectors never outlive the call.
- **References, never bodies.** `query`, `related`, and `due` return URLs,
  titles, snippets, and heading anchors; fetch the page yourself if you want
  the content.
- **Adaptive freshness.** Every document carries a revalidation interval that
  grows while unchanged and cuts sharply on change; `due()` lists what to
  check next, ranked by importance.
- **Any producer.** `add`/`touch` are the writer seam for corpora that do not
  arrive by crawl.
- **Thread-safe.** Every method detaches from the interpreter; driving the
  store through `asyncio.to_thread` is the intended pattern.
- **Durable.** An mmapped, checksummed store with write-ahead logging,
  crash-safe compaction, and lock-protected single-writer / many-reader
  access — implemented in Rust, shipped as one `abi3` wheel per platform
  (Python ≥ 3.12).

Dual-licensed AGPL-3.0-or-later / commercial (contact spicyzhug@gmail.com).
