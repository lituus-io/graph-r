# Author: spicyzhug@gmail.com
# Copyright: lituus-io, all rights reserved
#
# Several writers on one store: writable handles coexist, every write is a
# transaction under a commit-scoped lock, `transaction()` makes a
# read-modify-write serializable across processes, and a writer that cannot
# get the lock in time raises the typed `LockedError`.

import os
import subprocess
import sys
import textwrap
import threading
import time

import pytest

import graph_r


def race(code, go, args_list, timeout=120):
    """Start one interpreter per args tuple, release them together on the go
    file once all are up, and require every one to exit cleanly."""
    procs = [
        subprocess.Popen(
            [sys.executable, "-c", textwrap.dedent(code), *map(str, args)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for args in args_list
    ]
    release_when_ready(go, len(procs))
    for p in procs:
        _, err = p.communicate(timeout=timeout)
        assert p.returncode == 0, err


BARRIER = """
import os, sys, time
def ready_then_go(go, me):
    open(f"{go}.ready{me}", "w").close()
    while not os.path.exists(go):
        time.sleep(0.0005)
"""


def release_when_ready(go, n):
    import time

    deadline = time.monotonic() + 60
    while not all(os.path.exists(f"{go}.ready{i}") for i in range(n)):
        assert time.monotonic() < deadline, "children never came up"
        time.sleep(0.001)
    open(go, "w").close()


def test_locked_error_is_typed_and_a_graph_r_error():
    assert issubclass(graph_r.LockedError, graph_r.GraphRError)


def test_two_writable_handles_both_write_and_converge(tmp_path):
    path = str(tmp_path / "kb")
    a = graph_r.Store.create(path)
    b = graph_r.Store.open(path)
    a.add("https://x.dev/a", content_hash=1)
    b.add("https://x.dev/b", content_hash=2)
    a.add("https://x.dev/c", content_hash=3)
    assert a.urls() == ["https://x.dev/a", "https://x.dev/b", "https://x.dev/c"]
    assert b.reload_if_stale() is True
    assert b.urls() == a.urls()


def test_a_transaction_commits_once_at_exit(tmp_path):
    path = str(tmp_path / "kb")
    store = graph_r.Store.create(path)
    other = graph_r.Store.open(path)
    with store.transaction() as tx:
        tx.add("https://x.dev/a", content_hash=1)
        tx.add("https://x.dev/b", content_hash=2)
        # Documents added earlier in the block count for edges and attrs.
        tx.set_edges("https://x.dev/a", [("https://x.dev/b", "derives", False)])
        tx.set_attrs("https://x.dev/b", {"type": "TABLE"})
        assert len(tx) == 0, "reads show committed state, not staged writes"
        assert tx.urls() == []
    assert store.urls() == ["https://x.dev/a", "https://x.dev/b"]
    assert store.out_edges("https://x.dev/a") == [("https://x.dev/b", "derives", False)]
    other.reload_if_stale()
    assert other.attrs("https://x.dev/b") == {"type": "TABLE"}


def test_an_exception_aborts_with_nothing_written(tmp_path):
    path = str(tmp_path / "kb")
    store = graph_r.Store.create(path)
    store.add("https://x.dev/keep", content_hash=1)
    wal = os.path.join(path, "graph.wal")
    before = open(wal, "rb").read()
    with pytest.raises(RuntimeError):
        with store.transaction() as tx:
            tx.add("https://x.dev/lost", content_hash=2)
            tx.remove("https://x.dev/keep")
            raise RuntimeError("changed my mind")
    assert open(wal, "rb").read() == before
    assert store.urls() == ["https://x.dev/keep"]
    with pytest.raises(graph_r.GraphRError, match="finished"):
        tx.add("https://x.dev/late", content_hash=3)


def test_a_bad_write_in_a_block_aborts_the_whole_block(tmp_path):
    store = graph_r.Store.create(str(tmp_path / "kb"))
    with pytest.raises(graph_r.GraphRError, match="not a committed document"):
        with store.transaction() as tx:
            tx.add("https://x.dev/a", content_hash=1)
            tx.set_edges("https://x.dev/a", [("https://x.dev/missing", "derives", False)])
    assert store.urls() == []


def test_waiting_and_not_waiting_for_another_writer(tmp_path):
    path = str(tmp_path / "kb")
    a = graph_r.Store.create(path)
    b = graph_r.Store.open(path)
    with a.transaction():
        with pytest.raises(graph_r.LockedError):
            with b.transaction(wait=False):
                pass
    # Free again: the non-waiting form succeeds at once.
    with b.transaction(wait=False) as tx:
        tx.add("https://x.dev/b", content_hash=1)
    assert b.urls() == ["https://x.dev/b"]


def test_lock_wait_bounds_how_long_a_write_waits(tmp_path):
    path = str(tmp_path / "kb")
    a = graph_r.Store.create(path)
    b = graph_r.Store.open(path, lock_wait=0.05)
    with a.transaction():
        # A plain write and a transaction's default both wait b's 50 ms, not
        # the ten-second default.
        start = time.monotonic()
        with pytest.raises(graph_r.LockedError):
            b.add("https://x.dev/b", content_hash=1)
        with pytest.raises(graph_r.LockedError):
            with b.transaction():
                pass
        assert time.monotonic() - start < 5
    b.add("https://x.dev/b", content_hash=1)
    assert b.urls() == ["https://x.dev/b"]


def test_transaction_wait_takes_seconds(tmp_path):
    path = str(tmp_path / "kb")
    a = graph_r.Store.create(path)
    b = graph_r.Store.open_or_create(path)  # a ten-second lock_wait
    with a.transaction():
        start = time.monotonic()
        for wait in (0, 0.0, 0.05, False):
            with pytest.raises(graph_r.LockedError):
                with b.transaction(wait=wait):
                    pass
        assert time.monotonic() - start < 5, "a wait in seconds was not honoured"
    with b.transaction(wait=0.5) as tx:
        tx.add("https://x.dev/b", content_hash=1)
    with b.transaction(wait=True) as tx:
        tx.add("https://x.dev/c", content_hash=1)
    assert b.urls() == ["https://x.dev/b", "https://x.dev/c"]


@pytest.mark.parametrize("ok", [0, 2, 0.25, 1e-05, 1.5e2, 0.0000001])
def test_a_wait_in_any_decimal_form_opens(tmp_path, ok):
    path = str(tmp_path / "kb")
    graph_r.Store.create(path, lock_wait=ok)
    store = graph_r.Store.open(path, lock_wait=ok)
    with store.transaction(wait=ok) as tx:
        tx.add("https://x.dev/a", content_hash=1)
    assert store.urls() == ["https://x.dev/a"]


@pytest.mark.parametrize("bad", [-1, -0.5, float("nan"), float("inf"), 1e30, True])
def test_a_wait_must_be_a_finite_number_of_seconds(tmp_path, bad):
    path = str(tmp_path / "kb")
    store = graph_r.Store.create(path)
    for opener in (graph_r.Store.open, graph_r.Store.open_or_create):
        with pytest.raises(ValueError):
            opener(path, lock_wait=bad)
    with pytest.raises(ValueError):
        graph_r.Store.create(str(tmp_path / "other"), lock_wait=bad)
    if bad is not True:  # transaction(wait=True) means the store's own wait
        with pytest.raises(ValueError):
            store.transaction(wait=bad)
    with pytest.raises(ValueError):
        store.transaction(wait="soon")


def test_plain_writes_and_a_block_on_one_store(tmp_path):
    store = graph_r.Store.create(str(tmp_path / "kb"))
    done = threading.Event()
    with store.transaction() as tx:
        # From the block's own thread a plain write could never proceed.
        with pytest.raises(graph_r.GraphRError, match="write through it"):
            store.add("https://x.dev/x", content_hash=1)
        # From another thread it waits for the block, then lands.
        t = threading.Thread(
            target=lambda: (store.add("https://x.dev/t", content_hash=2), done.set())
        )
        t.start()
        t.join(0.2)
        assert not done.is_set(), "a plain write landed inside the block"
        assert store.urls() == [], "reads never wait"
        tx.add("https://x.dev/tx", content_hash=3)
    t.join(30)
    assert done.is_set()
    assert store.urls() == ["https://x.dev/t", "https://x.dev/tx"]


def test_many_processes_create_one_store_at_once(tmp_path):
    """Eight interpreters open_or_create a never-created store at the same
    moment and add one document each, ten times over: all eight survive."""
    child = BARRIER + """
import graph_r
path, me, go = sys.argv[1], sys.argv[2], sys.argv[3]
ready_then_go(go, me)
store = graph_r.Store.open_or_create(path)
store.add(f"https://x.dev/w{me}", content_hash=1)
store.close()
"""
    for rnd in range(10):
        path, go = str(tmp_path / f"s{rnd}"), str(tmp_path / f"go{rnd}")
        race(child, go, [(path, i, go) for i in range(8)])
        store = graph_r.Store.open_read_only(path)
        assert store.urls() == [f"https://x.dev/w{i}" for i in range(8)], f"round {rnd}"


def test_transactions_allocate_ids_across_processes(tmp_path):
    """The read-modify-write a transaction exists for: four processes each
    allocate ten ids from the committed document count. No id repeats."""
    path = str(tmp_path / "kb")
    graph_r.Store.create(path).close()
    go = str(tmp_path / "go")
    child = BARRIER + """
import graph_r
path, me, go = sys.argv[1], sys.argv[2], sys.argv[3]
store = graph_r.Store.open(path)
ready_then_go(go, me)
for _ in range(10):
    with store.transaction() as tx:
        n = len(tx)
        tx.add(f"https://x.dev/id/{n}", content_hash=n, title=me)
    time.sleep(0.001)
"""
    race(child, go, [(path, i, go) for i in range(4)])
    store = graph_r.Store.open_read_only(path)
    assert sorted(store.urls()) == sorted(f"https://x.dev/id/{i}" for i in range(40))
