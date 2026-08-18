# Copyright: lituus-io, all rights reserved.
# Author: terekete <spicyzhug@gmail.com>
#
# Hermetic smoke tests for the graph_r Python bindings: the writer seam,
# lookups, freshness, retention, and persistence — no network anywhere.

import pytest

import graph_r


def seeded(path) -> "graph_r.Store":
    """Three interlinked docs through the writer seam."""
    store = graph_r.Store.create(str(path))
    store.add(
        "https://docs.x.dev/guide",
        content_hash=1,
        title="Guide",
        snippet="The install guide for the widget toolchain",
        etag='"v1"',
        segments=[("Install > Linux", 1, 60000), ("Install > Windows", 2, 40000)],
        links=["https://docs.x.dev/api", "https://docs.x.dev/faq"],
    )
    store.add(
        "https://docs.x.dev/api",
        content_hash=2,
        title="API reference",
        snippet="Functions and types exposed by the widget api",
        links=["https://docs.x.dev/guide"],
    )
    store.add(
        "https://docs.x.dev/faq",
        content_hash=3,
        title="FAQ",
        snippet="Common questions about widgets",
        links=["https://docs.x.dev/guide"],
    )
    store.compact()
    return store


def test_add_query_and_anchors(tmp_path):
    store = seeded(tmp_path / "kb")
    assert len(store) == 3

    hits = store.query("install guide widget")
    assert hits, "expected hits"
    top = hits[0]
    assert top.url == "https://docs.x.dev/guide"
    assert top.seed
    assert top.anchor is not None and "Install" in top.anchor.label
    assert top.snippet and "install guide" in top.snippet


def test_related_follows_edges(tmp_path):
    store = seeded(tmp_path / "kb")
    urls = [h.url for h in store.related("https://docs.x.dev/guide", k=5)]
    assert "https://docs.x.dev/api" in urls
    assert "https://docs.x.dev/faq" in urls


def test_due_and_touch_drive_freshness(tmp_path):
    store = seeded(tmp_path / "kb")
    far_future = 10_000_000_000_000
    due = store.due(limit=10, now_ms=far_future)
    assert len(due) == 3, "everything is overdue in the far future"
    assert due[0].etag == '"v1"' or any(d.etag == '"v1"' for d in due)

    # An unchanged check grows the interval; the doc stays due only until the
    # new interval passes. A gone check tombstones: the doc leaves the schedule
    # and the live count.
    assert store.touch("https://docs.x.dev/faq", "gone")
    assert len(store) == 2
    assert all(d.url != "https://docs.x.dev/faq" for d in store.due(limit=10, now_ms=far_future))

    assert not store.touch("https://never.x.dev/absent", "unchanged"), "unknown URL records nothing"


def test_pin_unpin_and_remove(tmp_path):
    store = seeded(tmp_path / "kb")
    assert store.pin("https://docs.x.dev/guide")
    assert store.unpin("https://docs.x.dev/guide")
    assert not store.pin("https://never.x.dev/absent")

    store.remove("https://docs.x.dev/faq")
    store.compact()
    assert len(store) == 2


def test_persistence_and_read_only(tmp_path):
    path = tmp_path / "kb"
    store = seeded(path)
    generation = store.generation
    del store  # release the writer lock

    reopened = graph_r.Store.open(str(path))
    assert len(reopened) == 3
    assert reopened.generation == generation
    assert reopened.query("widget api")[0].url == "https://docs.x.dev/api"
    del reopened

    ro = graph_r.Store.open_read_only(str(path))
    assert len(ro) == 3
    with pytest.raises(graph_r.GraphRError):
        ro.add("https://docs.x.dev/new", content_hash=9)
    with pytest.raises(graph_r.GraphRError):
        ro.compact()


def test_open_or_create_both_ways(tmp_path):
    path = str(tmp_path / "kb")
    first = graph_r.Store.open_or_create(path)
    first.add("https://x.dev/a", content_hash=1)
    del first
    again = graph_r.Store.open_or_create(path)
    assert len(again) == 1


def test_second_writer_is_refused(tmp_path):
    path = str(tmp_path / "kb")
    first = graph_r.Store.create(path)
    with pytest.raises(graph_r.GraphRError):
        graph_r.Store.open(path)
    del first
    graph_r.Store.open(path)  # released on drop


def test_bad_outcome_is_a_value_error(tmp_path):
    store = graph_r.Store.create(str(tmp_path / "kb"))
    with pytest.raises(ValueError):
        store.touch("https://x.dev/a", "sideways")
    with pytest.raises(ValueError):
        store.sync("https://x.dev/", scope="galaxy")


def test_module_reports_a_version():
    assert isinstance(graph_r.__version__, str) and graph_r.__version__
