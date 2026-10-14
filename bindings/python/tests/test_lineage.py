# Author: support@lituus.io
# Copyright: lituus-io, all rights reserved
#
# The lineage model through Python: typed edges in both directions, node
# attributes, the breadth-first walk, exhaustive enumeration, the typed
# format-version refusal and close(). Hermetic — no network anywhere.

import concurrent.futures as futures
import os
import pathlib
import random
import shutil
import threading
from collections import deque

import pytest

import graph_r

# Lineage ids are urls, canonicalised exactly as `add` stores them — so a
# fragment would be dropped. Columns are a path segment below their table.
T = "bq://p/raw/orders"
T_ID = "bq://p/raw/orders/id"
T_AMT = "bq://p/raw/orders/amount"
V = "bq://p/stg/orders"
V_ID = "bq://p/stg/orders/id"
V_TOTAL = "bq://p/stg/orders/total"
ALL = [T, T_ID, T_AMT, V, V_ID, V_TOTAL]

FIXTURES = pathlib.Path(__file__).resolve().parents[3] / "tests" / "fixtures"


def add_all(store, urls):
    for i, u in enumerate(urls):
        store.add(u, content_hash=i + 1, title=u.rsplit("/", 1)[-1])


def lineage(path):
    """table → view → column: the view derives from the table and contains its
    columns; each view column derives from table columns (id is an exact copy);
    the view is filtered on raw.id."""
    store = graph_r.Store.create(str(path))
    add_all(store, ALL)
    store.set_edges(T, [(T_ID, "contains", False), (T_AMT, "contains", False)])
    store.set_edges(
        V,
        [
            (T, "derives", False),
            (V_ID, "contains", False),
            (V_TOTAL, "contains", False),
            (T_ID, "conditions", False),
        ],
    )
    store.set_edges(V_ID, [(T_ID, "derives", True)])
    store.set_edges(V_TOTAL, [(T_AMT, "derives", False), (T_ID, "derives", False)])
    return store


# ---- edges ------------------------------------------------------------------


def test_out_and_in_edges_carry_kind_and_exact(tmp_path):
    store = lineage(tmp_path / "g")
    assert store.out_edges(V_ID) == [(T_ID, "derives", True)]
    assert sorted(store.out_edges(V)) == sorted(
        [(T, "derives", False), (V_ID, "contains", False), (V_TOTAL, "contains", False), (T_ID, "conditions", False)]
    )
    assert sorted(store.in_edges(T_ID)) == sorted(
        [(T, "contains", False), (V, "conditions", False), (V_ID, "derives", True), (V_TOTAL, "derives", False)]
    )
    assert store.out_edges("bq://p/never/added") == []
    assert store.in_edges("bq://p/never/added") == []


def test_set_edges_replaces_the_whole_set_never_merges(tmp_path):
    store = lineage(tmp_path / "g")
    store.set_edges(V_TOTAL, [(T_AMT, "derives", False)])
    assert store.out_edges(V_TOTAL) == [(T_AMT, "derives", False)]
    assert (V_TOTAL, "derives", False) not in store.in_edges(T_ID), "the replaced edge is gone inbound too"
    # A repeated destination keeps its first entry.
    store.set_edges(V_TOTAL, [(T_AMT, "conditions", False), (T_AMT, "derives", True)])
    assert store.out_edges(V_TOTAL) == [(T_AMT, "conditions", False)]
    # [] clears.
    store.set_edges(V_TOTAL, [])
    assert store.out_edges(V_TOTAL) == []
    # `links` recorded by `add` are outbound edges too, and are replaced.
    store.add("https://x.dev/page", content_hash=9, links=[T])
    assert store.out_edges("https://x.dev/page") == [(T, "link", False)]
    store.set_edges("https://x.dev/page", [(V, "derives", False)])
    assert store.out_edges("https://x.dev/page") == [(V, "derives", False)]


def test_set_edges_refusals_change_nothing(tmp_path):
    store = lineage(tmp_path / "g")
    before = store.out_edges(V)
    with pytest.raises(ValueError, match="kind must be one of"):
        store.set_edges(V, [(T, "feeds", False)])
    with pytest.raises(ValueError, match="kind must be one of"):
        store.set_edges(V, [(T, "link", False)])  # links come from `add`
    with pytest.raises(ValueError, match="itself"):
        store.set_edges(V, [(V, "derives", False)])
    with pytest.raises(graph_r.GraphRError, match="not a committed document"):
        store.set_edges("bq://p/never/added", [(T, "derives", False)])
    with pytest.raises(graph_r.GraphRError, match="not a committed document"):
        store.set_edges(V, [(T, "derives", False), ("bq://p/never/added", "derives", False)])
    assert store.out_edges(V) == before
    assert store.out_edges("bq://p/never/added") == []


def test_the_per_node_cap_is_named_and_reachable(tmp_path):
    store = graph_r.Store.create(str(tmp_path / "g"))
    hub = "bq://p/mart/wide"
    cols = [f"bq://p/raw/t/c{i}" for i in range(4097)]
    add_all(store, [hub] + cols)
    with pytest.raises(ValueError, match="4096"):
        store.set_edges(hub, [(c, "derives", False) for c in cols])
    store.set_edges(hub, [(c, "derives", False) for c in cols[:4096]])
    assert len(store.out_edges(hub)) == 4096
    store.compact()
    assert len(store.out_edges(hub)) == 4096
    assert store.in_edges(cols[4095]) == [(hub, "derives", False)]


def test_edges_are_visible_on_return_before_and_after_compaction(tmp_path):
    """Each call commits, so a new edge is visible to out_edges, in_edges and
    walk the moment set_edges returns: in_edges reads the reverse index from
    the last compaction plus every edge committed since. Compaction changes
    nothing observable. A read-only handle in another process sees the edge
    after reload_if_stale()."""
    path = tmp_path / "g"
    store = graph_r.Store.create(str(path))
    store.compact()  # an empty base: every edge below lives only in the log
    add_all(store, ALL)
    ro = graph_r.Store.open_read_only(str(path))
    store.set_edges(V_ID, [(T_ID, "derives", True)])
    before = (store.out_edges(V_ID), store.in_edges(T_ID), store.walk(T_ID, "in"))
    assert before == ([(T_ID, "derives", True)], [(V_ID, "derives", True)], [V_ID])
    assert ro.in_edges(T_ID) == [], "a read-only handle sees nothing until it reloads"
    assert ro.reload_if_stale()
    assert ro.in_edges(T_ID) == [(V_ID, "derives", True)]
    store.compact()
    assert (store.out_edges(V_ID), store.in_edges(T_ID), store.walk(T_ID, "in")) == before
    # Replaced after compaction: the base's reverse run is hidden at once.
    store.set_edges(V_ID, [(T_AMT, "derives", False)])
    assert store.in_edges(T_ID) == []
    assert store.in_edges(T_AMT) == [(V_ID, "derives", False)]
    # Removing the source drops its edges in both directions immediately.
    store.remove(V_ID)
    assert store.in_edges(T_AMT) == []
    store.compact()
    assert store.in_edges(T_AMT) == []
    # Removing a destination hides it from its sources' edges.
    store.set_edges(V, [(T, "derives", False)])
    store.remove(T)
    assert store.out_edges(V) == []
    store.compact()
    assert store.out_edges(V) == []


# ---- walk -------------------------------------------------------------------


def test_walk_up_and_down_a_three_level_lineage(tmp_path):
    store = lineage(tmp_path / "g")
    for phase in ("log", "base"):
        # Upstream of a view column, along value lineage only.
        assert store.walk(V_ID, "out", kinds=["derives"]) == [T_ID], phase
        # Upstream of the view, every kind: depth 1 then depth 2, url order.
        assert store.walk(V, "out", depth=1) == sorted([T, T_ID, V_ID, V_TOTAL]), phase
        assert store.walk(V, "out", depth=2) == sorted([T, T_ID, V_ID, V_TOTAL]) + [T_AMT], phase
        # Downstream impact of raw.id: who derives from it or is filtered on it.
        assert store.walk(T_ID, "in", depth=1, kinds=["derives", "conditions"]) == sorted([V, V_ID, V_TOTAL]), phase
        assert store.walk(T_ID, "in", depth=1, kinds=["contains"]) == [T], phase
        # Downstream from the table: the view (derives), then its columns.
        assert store.walk(T, "in", depth=1) == [V], phase
        assert store.walk(T, "in", depth=1, kinds=["contains"]) == [], phase
        # Both ways from a view column: table column and view, then the rest.
        assert store.walk(V_ID, "both", depth=1) == sorted([T_ID, V]), phase
        assert store.walk(V_ID, "both", depth=2) == sorted([T_ID, V]) + sorted([T, V_TOTAL]), phase
        assert store.walk(V_ID, "both", depth=3) == sorted([T_ID, V]) + sorted([T, V_TOTAL]) + [T_AMT], phase
        # Depth 0 and unknown starts reach nothing.
        assert store.walk(V, "out", depth=0) == [], phase
        assert store.walk("bq://p/never/added", "both", depth=9) == [], phase
        store.compact()


def test_walk_visits_each_node_once_through_cycles_and_never_the_start(tmp_path):
    store = graph_r.Store.create(str(tmp_path / "g"))
    ring = [f"bq://p/cyc/n{i}" for i in range(5)]
    add_all(store, ring)
    for i, u in enumerate(ring):
        store.set_edges(u, [(ring[(i + 1) % 5], "derives", False), (ring[(i + 2) % 5], "derives", False)])
    for direction in ("out", "in", "both"):
        got = store.walk(ring[0], direction, depth=50)
        assert sorted(got) == sorted(ring[1:]) and len(got) == 4, direction


def test_walk_limit_keeps_the_nearest_and_the_default_cap_holds(tmp_path):
    store = graph_r.Store.create(str(tmp_path / "g"))
    root = "bq://p/fan/root"
    near = [f"bq://p/fan/near{i:02}" for i in range(20)]
    far = [f"bq://p/fan/far{i:02}" for i in range(20)]
    add_all(store, [root] + near + far)
    store.set_edges(root, [(u, "derives", False) for u in near])
    for i, u in enumerate(near):
        store.set_edges(u, [(far[i], "derives", False)])
    assert store.walk(root, depth=2, limit=5) == near[:5]
    assert store.walk(root, depth=2, limit=25) == near + far[:5]
    assert store.walk(root, depth=2, limit=0) == []
    assert store.walk(root, depth=2) == near + far


def test_walk_default_cap_is_ten_thousand(tmp_path):
    store = graph_r.Store.create(str(tmp_path / "g"))
    root = "bq://p/cap/root"
    hubs = [f"bq://p/cap/h{i}" for i in range(3)]
    leaves = [[f"bq://p/cap/h{h}/l{i:04}" for i in range(4096)] for h in range(3)]
    add_all(store, [root] + hubs + [u for ls in leaves for u in ls])
    store.set_edges(root, [(h, "contains", False) for h in hubs])
    for h, ls in zip(hubs, leaves):
        store.set_edges(h, [(u, "contains", False) for u in ls])
    got = store.walk(root, depth=2)
    assert len(got) == 10_000
    assert got[:3] == hubs and got == sorted(got[:3]) + sorted(got[3:])
    assert len(store.walk(root, depth=2, limit=20_000)) == 3 + 3 * 4096


def test_walk_argument_refusals(tmp_path):
    store = lineage(tmp_path / "g")
    with pytest.raises(ValueError, match="direction"):
        store.walk(V, "up")
    with pytest.raises(ValueError, match="kind must be one of"):
        store.walk(V, kinds=["feeds"])
    with pytest.raises(OverflowError):
        store.walk(V, depth=-1)


def reference_walk(adj, removed, start, direction, depth, kinds, limit):
    """A plain breadth-first search over the same edge sets: distance first,
    then url; the start excluded; removed nodes neither reported nor crossed."""
    if start in removed or limit == 0:
        return []
    nbrs = {}
    for src, edges in adj.items():
        if src in removed:
            continue
        for dst, kind in edges:
            if dst in removed or (kinds is not None and kind not in kinds):
                continue
            if direction in ("out", "both"):
                nbrs.setdefault(src, set()).add(dst)
            if direction in ("in", "both"):
                nbrs.setdefault(dst, set()).add(src)
    dist = {start: 0}
    q = deque([start])
    while q:
        u = q.popleft()
        if dist[u] == depth:
            continue
        for v in nbrs.get(u, ()):
            if v not in dist:
                dist[v] = dist[u] + 1
                q.append(v)
    del dist[start]
    return [u for _, u in sorted((d, u) for u, d in dist.items())][:limit]


@pytest.mark.parametrize("seed", range(12))
def test_walk_equals_a_reference_bfs_over_random_graphs(tmp_path, seed):
    """Seeded randomized differential: random typed edge sets (rewritten,
    with removals and compactions interleaved so walks cross log and base),
    against reference_walk on every direction, depth, kind filter and limit."""
    rng = random.Random(seed)
    n = rng.randint(4, 40)
    urls = [f"bq://p/fz{seed}/n{i:02}" for i in range(n)]
    store = graph_r.Store.create(str(tmp_path / "g"))
    add_all(store, urls)
    kinds_all = ["contains", "derives", "conditions"]
    adj = {}
    removed = set()
    for round_ in range(rng.randint(1, 4)):
        for src in rng.sample(urls, rng.randint(1, n)):
            if src in removed:
                continue
            candidates = [u for u in urls if u != src and u not in removed]
            dsts = rng.sample(candidates, rng.randint(0, min(len(candidates), 6)))
            edges = [(d, rng.choice(kinds_all), rng.random() < 0.3) for d in dsts]
            store.set_edges(src, edges)
            adj[src] = [(d, kind) for d, kind, _ in edges]
        if rng.random() < 0.5:
            store.compact()
        if rng.random() < 0.3 and len(removed) < n // 3:
            victim = rng.choice([u for u in urls if u not in removed])
            store.remove(victim)
            removed.add(victim)
            adj.pop(victim, None)
        for _ in range(25):
            start = rng.choice(urls)
            direction = rng.choice(["out", "in", "both"])
            depth = rng.randint(0, 6)
            kinds = None if rng.random() < 0.4 else rng.sample(kinds_all, rng.randint(1, 3))
            limit = None if rng.random() < 0.5 else rng.randint(0, n)
            want = reference_walk(adj, removed, start, direction, depth, kinds, 10_000 if limit is None else limit)
            got = store.walk(start, direction, depth=depth, kinds=kinds, limit=limit)
            assert got == want, (seed, round_, start, direction, depth, kinds, limit)


# ---- attributes -------------------------------------------------------------


def test_attrs_round_trip_replace_and_persist(tmp_path):
    path = tmp_path / "g"
    store = lineage(path)
    sql = "SELECT id, amount FROM raw.orders WHERE " + "id > 0 AND " * 20_000 + "TRUE"
    store.set_attrs(V, {"sql": sql, "type": "VIEW", "note": "", "ünï": "çödé"})
    assert store.attrs(V) == {"sql": sql, "type": "VIEW", "note": "", "ünï": "çödé"}
    assert store.attrs(T) == {}
    assert store.attrs("bq://p/never/added") == {}
    store.compact()
    assert store.attrs(V)["sql"] == sql
    store.set_attrs(V, {"type": "TABLE"})
    assert store.attrs(V) == {"type": "TABLE"}, "whole-set replacement"
    store.set_attrs(V, {})
    assert store.attrs(V) == {}
    store.set_attrs(V_TOTAL, {"type": "NUMERIC"})
    del store
    assert graph_r.Store.open(str(path)).attrs(V_TOTAL) == {"type": "NUMERIC"}


def test_attrs_refuse_beyond_their_documented_limits(tmp_path):
    store = lineage(tmp_path / "g")
    store.set_attrs(V, {"keep": "me"})
    refused = [
        {f"k{i}": "v" for i in range(65)},  # more than 64 names
        {"": "v"},  # empty name
        {"a\0b": "v"},  # NUL in a name
        {"n" * 256: "v"},  # name over 255 bytes
        {"k": "v" * (256 * 1024 - 1)},  # name + NUL + value over 256 KiB
        {c: "v" * (256 * 1024 - 8) for c in "abcd"},  # over 1 MiB together
    ]
    for mapping in refused:
        with pytest.raises(ValueError):
            store.set_attrs(V, mapping)
    assert store.attrs(V) == {"keep": "me"}, "a refusal changes nothing"
    # Exactly at each limit is accepted.
    store.set_attrs(V, {"k": "v" * (256 * 1024 - 2)})
    store.set_attrs(V, {f"k{i}": "v" for i in range(64)})
    store.set_attrs(V, {"n" * 255: "v"})
    with pytest.raises(graph_r.GraphRError, match="not a committed document"):
        store.set_attrs("bq://p/never/added", {"a": "b"})
    with pytest.raises(TypeError):
        store.set_attrs(V, {"a": 1})


# ---- urls -------------------------------------------------------------------


@pytest.mark.parametrize("n", [1, 40])
def test_urls_are_exhaustive_where_ranked_query_is_not(tmp_path, n):
    store = graph_r.Store.create(str(tmp_path / "g"))
    want = sorted(f"https://mem.x.dev/entry/{i:03}" for i in range(n))
    for i, u in enumerate(want):
        store.add(u, content_hash=i + 1, title="memory entry")
    store.add("https://kb.x.dev/other", content_hash=999, title="memory entry")
    assert store.urls("https://mem.x.dev/") == want
    assert store.urls() == sorted(want + ["https://kb.x.dev/other"])
    assert store.urls("https://nothing.x.dev/") == []
    store.compact()
    assert store.urls("https://mem.x.dev/") == want


def test_urls_at_ten_thousand_documents_across_log_and_base(tmp_path):
    store = graph_r.Store.create(str(tmp_path / "g"))
    want = [f"https://mem.x.dev/e/{i:05}" for i in range(10_000)]
    for i, u in enumerate(want):
        store.add(u, content_hash=i + 1)
        if i == 6_000:
            store.compact()  # some in the base, some only in the log
    assert store.urls() == want
    assert store.urls("https://mem.x.dev/e/099") == want[9_900:10_000]


def test_urls_follow_remove_and_tombstone(tmp_path):
    store = graph_r.Store.create(str(tmp_path / "g"))
    urls = [f"https://mem.x.dev/e/{i}" for i in range(6)]
    add_all(store, urls)
    store.compact()
    store.remove(urls[0])
    assert store.touch(urls[1], "gone")
    want = urls[2:]
    assert store.urls() == want
    store.compact()
    assert store.urls() == want and len(store) == len(want)
    store.add(urls[0], content_hash=7)  # a re-add is a new document
    assert store.urls() == sorted(want + [urls[0]])


def test_ids_are_canonicalised_like_add_so_a_fragment_is_dropped(tmp_path):
    """Pinned for consumers: every method keys a url exactly as `add` stores
    it, and canonicalisation drops a `#fragment`. Two ids differing only by
    fragment are ONE document — a column id must be its own path segment."""
    store = graph_r.Store.create(str(tmp_path / "g"))
    store.add("bq://p/d/t", content_hash=1)
    store.add("bq://p/d/t#c", content_hash=2)
    assert len(store) == 1 and store.urls() == ["bq://p/d/t"]
    store.set_attrs("bq://p/d/t#c", {"type": "INT64"})
    assert store.attrs("bq://p/d/t") == {"type": "INT64"}, "the fragment addressed the table"
    store.add("bq://p/d/t/c", content_hash=3)
    assert store.urls() == ["bq://p/d/t", "bq://p/d/t/c"], "a path segment is its own document"


# ---- format version ---------------------------------------------------------


def copy_v1(tmp_path):
    """A store written by the published 0.2.1 release (format v1): built once
    in a throwaway venv with `pip install uu-graph-r==0.2.1`, one compacted
    document plus one in the log, and kept byte-exact under tests/fixtures."""
    dst = tmp_path / "v1"
    dst.mkdir()
    for f in ("graph.base", "graph.wal"):
        shutil.copyfile(FIXTURES / "format_v1" / f, dst / f)
    return str(dst)


@pytest.mark.parametrize("opener", ["open", "open_or_create", "open_read_only"])
def test_a_format_v1_store_raises_format_version_error(tmp_path, opener):
    path = copy_v1(tmp_path)
    with pytest.raises(graph_r.FormatVersionError) as info:
        getattr(graph_r.Store, opener)(path)
    assert (info.value.found, info.value.supported) == (1, 2)
    assert isinstance(info.value, graph_r.GraphRError), "existing handlers keep catching it"
    assert "unsupported version 1" in str(info.value)
    # Damage is not a version mismatch.
    with open(os.path.join(path, "graph.base"), "r+b") as f:
        f.write(b"XXXX")
    with pytest.raises(graph_r.GraphRError) as info:
        getattr(graph_r.Store, opener)(path)
    assert not isinstance(info.value, graph_r.FormatVersionError)


# ---- close ------------------------------------------------------------------


def test_close_releases_the_store_and_the_directory_can_be_deleted(tmp_path):
    path = tmp_path / "g"
    store = lineage(path)
    store.set_attrs(V, {"type": "VIEW"})
    with pytest.raises(graph_r.GraphRError, match="locked"):
        graph_r.Store.open(str(path))
    store.close()
    store.close()  # idempotent
    reopened = graph_r.Store.open(str(path))  # the writer lock is free
    assert reopened.attrs(V) == {"type": "VIEW"}
    reopened.close()
    shutil.rmtree(path)  # nothing mapped or locked: deletable on every OS
    assert not path.exists()


CALLS = [
    lambda s: s.query("x"),
    lambda s: s.related(V),
    lambda s: s.due(),
    lambda s: s.add("https://x.dev/z", content_hash=1),
    lambda s: s.touch(V, "unchanged"),
    lambda s: s.pin(V),
    lambda s: s.unpin(V),
    lambda s: s.remove(V),
    lambda s: s.compact(),
    lambda s: s.reload_if_stale(),
    lambda s: s.generation,
    lambda s: len(s),
    lambda s: s.sync("http://127.0.0.1:9/"),
    lambda s: s.set_edges(V, []),
    lambda s: s.out_edges(V),
    lambda s: s.in_edges(V),
    lambda s: s.walk(V),
    lambda s: s.set_attrs(V, {}),
    lambda s: s.attrs(V),
    lambda s: s.urls(),
]


def test_every_call_after_close_raises_store_is_closed(tmp_path):
    store = lineage(tmp_path / "g")
    store.close()
    for call in CALLS:
        with pytest.raises(graph_r.GraphRError, match="^store is closed$"):
            call(store)


def test_close_waits_for_calls_in_flight_on_other_threads(tmp_path):
    store = graph_r.Store.create(str(tmp_path / "g"))
    add_all(store, [f"https://x.dev/{i}" for i in range(200)])
    started = threading.Event()
    outcomes = []

    def reader():
        started.set()
        while True:
            try:
                assert len(store.urls()) == 200
            except graph_r.GraphRError as e:
                outcomes.append(str(e))
                return

    with futures.ThreadPoolExecutor(max_workers=4) as pool:
        running = [pool.submit(reader) for _ in range(4)]
        started.wait(10)
        store.close()
        for r in running:
            r.result(timeout=30)
    assert outcomes == ["store is closed"] * 4
    shutil.rmtree(tmp_path / "g")


# ---- security ---------------------------------------------------------------


def test_no_method_reads_or_writes_outside_the_store_directory(tmp_path):
    """Urls are keys, never paths: a url naming a file elsewhere is just an
    unknown node, and the store directory holds only its own three files."""
    secret = tmp_path / "secret.txt"
    secret.write_text("do-not-read")
    store = lineage(tmp_path / "g")
    for probe in (str(secret), f"file://{secret}", "../secret.txt", "../../../../etc/passwd"):
        assert store.out_edges(probe) == [] and store.in_edges(probe) == []
        assert store.walk(probe, "both", depth=9) == []
        assert store.attrs(probe) == {}
        assert store.urls(probe) == []
        with pytest.raises(graph_r.GraphRError, match="not a committed document"):
            store.set_edges(probe, [(T, "derives", False)])
        with pytest.raises(graph_r.GraphRError, match="not a committed document"):
            store.set_edges(V, [(probe, "derives", False)])
        with pytest.raises(graph_r.GraphRError, match="not a committed document"):
            store.set_attrs(probe, {"a": "b"})
    store.compact()
    assert sorted(os.listdir(tmp_path)) == ["g", "secret.txt"]
    assert sorted(os.listdir(tmp_path / "g")) == ["graph.base", "graph.lock", "graph.wal"]
    assert secret.read_text() == "do-not-read"


# ---- type stubs -------------------------------------------------------------


def test_the_stubs_cover_the_whole_runtime_surface():
    """The shipped .pyi is what a consumer's type checker sees; a method
    missing there is typed as Any. Every public name on the module and on
    Store must be declared."""
    import ast

    tree = ast.parse((pathlib.Path(__file__).resolve().parents[1] / "graph_r.pyi").read_text())
    classes = {n.name: n for n in tree.body if isinstance(n, ast.ClassDef)}
    declared = {
        t.id for n in tree.body if isinstance(n, (ast.Assign, ast.AnnAssign))
        for t in (n.targets if isinstance(n, ast.Assign) else [n.target]) if isinstance(t, ast.Name)
    } | set(classes)
    import types

    public = {n for n in dir(graph_r) if not n.startswith("_")}
    public = {n for n in public if not isinstance(getattr(graph_r, n), types.ModuleType)}
    assert public <= declared, public - declared
    store_stub = {n.name for n in classes["Store"].body if isinstance(n, ast.FunctionDef)}
    runtime = {n for n in dir(graph_r.Store) if not n.startswith("_")} | {"__len__"}
    assert runtime <= store_stub, runtime - store_stub
