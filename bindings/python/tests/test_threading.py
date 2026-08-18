# Copyright: lituus-io, all rights reserved.
# Author: terekete <spicyzhug@gmail.com>
#
# Cross-thread access to a Store. The link-r binding shipped (pre-release)
# with `#[pyclass(unsendable)]`, which raises the moment the object is touched
# off its creating thread — exactly what `asyncio.to_thread` does. These tests
# exist so that defect class cannot recur here.

import asyncio
import concurrent.futures as futures
import threading

import pytest

import graph_r


def test_store_is_usable_from_another_thread(tmp_path):
    store = graph_r.Store.create(str(tmp_path / "kb"))
    store.add("https://x.dev/a", content_hash=1, title="A")
    creating = threading.get_ident()

    def use_it():
        assert threading.get_ident() != creating
        return len(store), len(store.query("anything"))

    with futures.ThreadPoolExecutor(max_workers=1) as pool:
        count, _hits = pool.submit(use_it).result(timeout=30)
    assert count == 1


def test_store_survives_many_workers(tmp_path):
    store = graph_r.Store.create(str(tmp_path / "kb"))
    store.add("https://x.dev/a", content_hash=1)
    seen = set()

    def touch(_):
        seen.add(threading.get_ident())
        return len(store)

    with futures.ThreadPoolExecutor(max_workers=4) as pool:
        assert list(pool.map(touch, range(24))) == [1] * 24
    assert len(seen) > 1, "expected the pool to use more than one worker"


def test_asyncio_to_thread_round_trip(tmp_path):
    async def main():
        store = graph_r.Store.create(str(tmp_path / "kb"))
        await asyncio.to_thread(
            store.add, "https://x.dev/a", content_hash=1, title="Alpha", snippet="alpha words"
        )
        hits = await asyncio.to_thread(store.query, "alpha")
        return [h.url for h in hits]

    assert asyncio.run(main()) == ["https://x.dev/a"]


def test_concurrent_readers_under_a_writer(tmp_path):
    """Readers on worker threads while the main thread writes and compacts:
    the generation-ring design under the exact threading Python produces."""
    store = graph_r.Store.create(str(tmp_path / "kb"))
    store.add("https://x.dev/seed", content_hash=1, title="Seed")
    stop = threading.Event()
    failures = []

    def read():
        try:
            while not stop.is_set():
                len(store)
                store.query("seed")
                store.due(limit=4, now_ms=10_000_000_000_000)
        except Exception as exc:  # noqa: BLE001 - the assertion is "no exception"
            failures.append(exc)

    with futures.ThreadPoolExecutor(max_workers=3) as pool:
        readers = [pool.submit(read) for _ in range(3)]
        for i in range(50):
            store.add(f"https://x.dev/p{i}", content_hash=i + 2, title="Live")
        store.compact()
        stop.set()
        for r in readers:
            r.result(timeout=60)
    assert not failures, failures
    assert len(store) == 51


def test_errors_map_from_worker_threads(tmp_path):
    def boom():
        graph_r.Store.open(str(tmp_path / "nowhere"))

    with futures.ThreadPoolExecutor(max_workers=1) as pool:
        with pytest.raises(graph_r.GraphRError):
            pool.submit(boom).result(timeout=30)
