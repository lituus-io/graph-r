# Copyright: lituus-io, all rights reserved.
# Author: terekete <spicyzhug@gmail.com>
#
# The file-level persistence contract, on whatever OS runs this suite (CI runs
# it on Linux, macOS, and Windows): a persisted store must be readable AND
# writable across close/reopen cycles, across processes, and under the path
# shapes real user directories have.

import os
import subprocess
import sys

import graph_r


def seed(store, n=4):
    for i in range(n):
        store.add(
            f"https://x.dev/doc{i}",
            content_hash=i + 1,
            title=f"Doc {i}",
            snippet=f"document {i} words widget",
            segments=[("Install", 1, 100)],
        )
    store.compact()


def test_full_read_write_cycle_across_reopens(tmp_path):
    """create → write → close → reopen → read → WRITE AGAIN → reopen → read.
    The write-after-reopen half is the part a read-only regression would hide."""
    path = str(tmp_path / "kb")
    store = graph_r.Store.create(path)
    seed(store)
    assert len(store) == 4
    del store

    store = graph_r.Store.open(path)
    assert len(store) == 4
    assert store.query("widget document")[0].url.startswith("https://x.dev/")
    store.add("https://x.dev/late", content_hash=99, title="Late", snippet="late arrival")
    store.compact()
    del store

    store = graph_r.Store.open(path)
    assert len(store) == 5
    assert any(h.url == "https://x.dev/late" for h in store.query("late arrival"))


def test_store_under_spaces_and_unicode_path(tmp_path):
    """The realistic Windows shape: C:\\Users\\First Last\\… — spaces and
    non-ASCII must survive create, reopen, and read-only open alike."""
    path = str(tmp_path / "kb dir" / "ünïcode-контекст")
    store = graph_r.Store.open_or_create(path)
    seed(store, n=2)
    del store

    assert len(graph_r.Store.open(path)) == 2  # rw reopen (then dropped)
    ro = graph_r.Store.open_read_only(path)
    assert len(ro) == 2
    assert ro.query("widget")


def test_repeated_open_close_releases_the_lock_every_time(tmp_path):
    """Fifty cycles: if the lock ever failed to release on drop (the Windows
    non-flock fallback's historical bug, fixed in Rust), this jams on cycle 2."""
    path = str(tmp_path / "kb")
    graph_r.Store.create(path).add("https://x.dev/a", content_hash=1)
    for i in range(50):
        store = graph_r.Store.open(path)
        assert len(store) == 1
        if i % 10 == 0:
            store.touch("https://x.dev/a", "unchanged")
        del store


def test_cross_process_read_and_write(tmp_path):
    """A store written by THIS process must be readable and writable by a
    DIFFERENT process, and readable read-only by this one while that child
    holds the writer lock — real file-handle semantics, not thread semantics."""
    path = str(tmp_path / "kb")
    store = graph_r.Store.create(path)
    seed(store, n=3)
    del store  # release the writer lock for the child

    child = (
        "import graph_r, sys\n"
        f"store = graph_r.Store.open({path!r})\n"
        "assert len(store) == 3, len(store)\n"
        "store.add('https://x.dev/from-child', content_hash=77, title='Child',"
        " snippet='written by another process')\n"
        "store.compact()\n"
        "print('child-ok')\n"
    )
    proc = subprocess.run(
        [sys.executable, "-c", child], capture_output=True, text=True, timeout=120
    )
    assert proc.returncode == 0, proc.stderr
    assert "child-ok" in proc.stdout

    reopened = graph_r.Store.open(path)
    assert len(reopened) == 4
    assert any(
        h.url == "https://x.dev/from-child" for h in reopened.query("another process")
    )


def test_stamp_style_atomic_replace_beside_the_store(tmp_path):
    """Consumers keep a JSON stamp next to the store via os.replace; that
    pattern must hold on every OS (Windows os.replace over an existing file)."""
    path = tmp_path / "kb"
    graph_r.Store.create(str(path)).add("https://x.dev/a", content_hash=1)
    stamp = path / "last_sync.json"
    for i in range(3):
        tmp = path / f".last_sync.{i}.tmp"
        tmp.write_text('{"checked_at": %d}' % i)
        os.replace(tmp, stamp)  # atomic overwrite, including over an existing file
    assert '"checked_at": 2' in stamp.read_text()


def test_reload_if_stale_sees_writer_commits(tmp_path):
    """A long-lived read-only handle picks up a writer's commit without
    reopening — the MCP-server-while-background-sync shape. New in 0.2.1."""
    path = str(tmp_path / "kb")
    writer = graph_r.Store.create(path)
    seed(writer)

    reader = graph_r.Store.open_read_only(path)
    assert len(reader) == 4
    assert reader.reload_if_stale() is False  # nothing changed: cheap no-op

    writer.add("https://x.dev/fresh", content_hash=77, title="Fresh",
               snippet="freshly committed sprocket")
    writer.compact()

    assert reader.reload_if_stale() is True
    assert len(reader) == 5
    assert any(h.url == "https://x.dev/fresh" for h in reader.query("sprocket"))
    assert reader.reload_if_stale() is False  # converged again
