# graph-r

[![CI](https://github.com/lituus-io/graph-r/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/lituus-io/graph-r/actions/workflows/ci.yml)
[![Security](https://github.com/lituus-io/graph-r/actions/workflows/security.yml/badge.svg?branch=main)](https://github.com/lituus-io/graph-r/actions/workflows/security.yml)
[![Fuzz](https://github.com/lituus-io/graph-r/actions/workflows/fuzz.yml/badge.svg?branch=main)](https://github.com/lituus-io/graph-r/actions/workflows/fuzz.yml)
[![Benchmark](https://github.com/lituus-io/graph-r/actions/workflows/benchmark.yml/badge.svg?branch=main)](https://github.com/lituus-io/graph-r/actions/workflows/benchmark.yml)
[![Release](https://github.com/lituus-io/graph-r/actions/workflows/release.yaml/badge.svg)](https://github.com/lituus-io/graph-r/releases/latest)
[![crates.io](https://img.shields.io/crates/v/uu-graph-r?label=crates.io)](https://crates.io/crates/uu-graph-r)
[![docs.rs](https://img.shields.io/docsrs/uu-graph-r?label=docs.rs)](https://docs.rs/uu-graph-r)
[![PyPI](https://img.shields.io/pypi/v/uu-graph-r?label=PyPI)](https://pypi.org/project/uu-graph-r/)
[![Python](https://img.shields.io/pypi/pyversions/uu-graph-r)](https://pypi.org/project/uu-graph-r/)
[![License](https://img.shields.io/badge/license-AGPL--3.0--or--later-blue)](https://github.com/lituus-io/graph-r/blob/main/LICENSE)

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
- **Lineage.** Typed edges (`contains`, `derives` with an exact-copy flag,
  `conditions`) between documents, read in both directions, a breadth-first
  `walk` up, down or both ways, and string attributes for large values:

  ```python
  store.set_edges(view_col, [(table_col, "derives", True)])   # replaces the set
  store.walk(table, direction="in", depth=3, kinds=["derives"])  # downstream
  store.set_attrs(view, {"sql": producing_sql, "type": "VIEW"})
  ```

  **Ids are canonicalised.** Every method keys a url exactly as `add`
  stores it: scheme and host lower-cased; userinfo, a default port, one
  trailing slash and the `#fragment` dropped; query parameters sorted (a
  string that does not parse as a URL is kept verbatim). So `bq://p/d/t#c` and
  `bq://p/d/t` are the *same* document — give a column its own path
  segment (`bq://p/d/t/c`).
- **Exhaustive listing.** `urls(prefix)` returns every live document under a
  prefix, sorted — not ranked like `query`.
- **Deterministic release.** `close()` drops the mapping and the open files
  now, so the directory can be deleted (Windows refuses while a file is
  mapped). A store written
  by another format version raises `FormatVersionError` (a `GraphRError`)
  with `found` and `supported`, and is left untouched: a 0.4 or 0.5 store
  raises `found=2, supported=3` — drain it with the release that wrote it,
  then rebuild.
- **Thread-safe.** Every method detaches from the interpreter; driving the
  store through `asyncio.to_thread` is the intended pattern.
- **Many writers.** Any number of processes may hold a writable `Store` on
  one path; `open_or_create` is safe when they race to create it. Each write
  holds the commit lock only for its append; `with store.transaction() as
  tx:` holds it across a block, so a read-modify-write is serializable and
  the block commits as one append. A writer that cannot get the lock within
  `lock_wait` seconds (`Store.open(path, lock_wait=0.5)`; ten by default)
  raises `LockedError` (a `GraphRError`); `transaction(wait=...)` takes
  `True`, `False` or a number of seconds.
- **Atomic.** A write or a block is one checksummed log record: a crash
  mid-write loses that write whole, never part of it, and nothing before it.
- **Durable.** An mmapped, checksummed store with write-ahead logging,
  crash-safe compaction, and many-writer / many-reader access — implemented
  in Rust, shipped as one `abi3` wheel per platform
  (Python ≥ 3.12).

Dual-licensed AGPL-3.0-or-later / commercial (contact spicyzhug@gmail.com).
