# Copyright: lituus-io, all rights reserved.
# Author: terekete <spicyzhug@gmail.com>
#
# Memory profile of the harness read path. A harness session holds one store
# and calls query/due hundreds of times; the mmapped read path must not
# accumulate per call. Ceilings are generous-but-binding: they catch a leak's
# order of magnitude, not allocator noise.

import gc
import resource
import sys
import tracemalloc

import pytest

import graph_r

RSS_CEILING_KIB = 64 * 1024  # 64 MiB of growth across the whole run = a leak
PY_ALLOC_CEILING_BYTES = 8 * 1024 * 1024

if sys.platform == "win32":  # resource is POSIX-only; RSS half skips there
    pytest.skip("resource.getrusage is POSIX-only", allow_module_level=True)


def _rss_kib() -> int:
    ru = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # ru_maxrss is bytes on macOS, KiB on Linux.
    return ru // 1024 if sys.platform == "darwin" else ru


def test_hundreds_of_reads_hold_rss_flat(tmp_path):
    store = graph_r.Store.create(str(tmp_path / "kb"))
    for i in range(200):
        store.add(
            f"https://x.dev/p{i}",
            content_hash=i + 1,
            title=f"Page {i}",
            snippet=f"content words for page {i} widget install",
            segments=[("Install", 1, 100), ("Usage", 2, 80)],
            links=[f"https://x.dev/p{(i + 1) % 200}"],
        )
    store.compact()

    queries = ["widget install", "page content", "usage", "nothing matches this"]
    # Warm-up: fault the mmap in and let caches settle before measuring.
    for q in queries:
        store.query(q)
    gc.collect()
    tracemalloc.start()
    before_py, _ = tracemalloc.get_traced_memory()
    before_rss = _rss_kib()

    for i in range(500):
        hits = store.query(queries[i % len(queries)], k=10)
        assert isinstance(hits, list)
        store.due(limit=8, now_ms=10_000_000_000_000)
        if i % 50 == 0:
            store.related("https://x.dev/p1", k=5)

    gc.collect()
    after_py, _ = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    rss_growth = _rss_kib() - before_rss
    py_growth = after_py - before_py

    assert rss_growth < RSS_CEILING_KIB, f"RSS grew {rss_growth} KiB over 500 read cycles"
    assert py_growth < PY_ALLOC_CEILING_BYTES, f"Python heap grew {py_growth} B"


def test_reader_under_live_writer_holds_flat(tmp_path):
    """The harness shape during a background sync: a read-only handle keeps
    answering (via reload_if_stale-backed reopen) while the writer commits."""
    path = str(tmp_path / "kb")
    writer = graph_r.Store.create(path)
    writer.add("https://x.dev/seed", content_hash=1, title="Seed", snippet="seed words")
    writer.compact()

    reader = graph_r.Store.open_read_only(path)
    gc.collect()
    before_rss = _rss_kib()
    for i in range(120):
        writer.add(
            f"https://x.dev/live{i}", content_hash=100 + i, title="Live", snippet="live doc"
        )
        assert isinstance(reader.query("seed words"), list)
        if i % 40 == 0:
            writer.compact()
    growth = _rss_kib() - before_rss
    assert growth < RSS_CEILING_KIB, f"RSS grew {growth} KiB with reader under writer"
