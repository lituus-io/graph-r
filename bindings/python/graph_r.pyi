# Author: spicyzhug@gmail.com
# Copyright: lituus-io, all rights reserved
#
# Type stubs for the graph_r native extension module.

from types import TracebackType
from typing import Dict, List, Literal, Optional, Sequence, Tuple, Type

__version__: str

#: (Stub-only aliases.) Edge types written by ``set_edges``.
_LineageKind = Literal["contains", "derives", "conditions"]
#: Edge types ``walk(kinds=...)`` follows: the lineage kinds plus crawl edges
#: (``link`` from ``add(links=...)``, ``related``, ``redirect``).
_WalkKind = Literal["link", "related", "redirect", "contains", "derives", "conditions"]
#: Every edge type a read reports: the above, and ``unknown`` for a type a
#: later release writes.
_EdgeKind = Literal[_WalkKind, "unknown"]
#: One edge: ``(other_url, kind, exact)``.
_Edge = Tuple[str, _EdgeKind, bool]

class GraphRError(Exception):
    """Raised when a graph-r (or embedded link-r) operation fails, and by
    every call on a closed store (``"store is closed"``)."""

class FormatVersionError(GraphRError):
    """The store on disk was written in a format version this build does not
    read (an older store to rebuild, or a newer one). Raised by ``open``,
    ``open_or_create`` and ``open_read_only`` (and ``reload_if_stale``)."""

    found: int
    """The version the file declares."""
    supported: int
    """The one version this build reads and writes."""

class LockedError(GraphRError):
    """Another writer held the store's commit lock past the wait (ten
    seconds; none for ``transaction(wait=False)``). Nothing was written:
    retry later. A subclass of ``GraphRError``, so existing handlers keep
    catching it."""

class Anchor:
    """A sub-document anchor on a hit: fetch the URL and resolve the heading
    (or slice the byte range, when known) instead of re-reading the page."""

    label: str
    importance: int
    byte_range: Optional[Tuple[int, int]]
    def __repr__(self) -> str: ...

class Hit:
    """One lookup answer: a reference, never a body."""

    url: str
    title: Optional[str]
    snippet: Optional[str]
    score: float
    seed: bool
    anchor: Optional[Anchor]
    def __repr__(self) -> str: ...

class DueItem:
    """One entry of the due-for-revalidation work list."""

    url: str
    etag: Optional[str]
    overdue_ms: int
    rank_permille: int
    def __repr__(self) -> str: ...

class SyncReport:
    """What one sync did: the crawl side and the graph side of one pass.

    The crawl-side counts (``added``/``updated``/``unchanged``/``skipped``/
    ``failed``) are relative to the sync's own ephemeral index, which is fresh
    every call — so a page this store has known for months still arrives as
    ``added`` after a restart. The graph side is the memory: ``upserted``,
    ``touched`` (revalidations *and* real content changes, which cut that
    page's refresh interval), ``tombstoned``, ``edges``, ``segments``.
    """

    added: int
    updated: int
    unchanged: int
    skipped: int
    failed: int
    upserted: int
    touched: int
    tombstoned: int
    edges: int
    segments: int
    def __repr__(self) -> str: ...

class Store:
    """An embedded, persistent knowledge-graph store.

    Safe to use from any thread (e.g. via ``asyncio.to_thread``); blocking
    calls detach from the interpreter so other Python threads keep running.

    Any number of writable handles, in any number of processes, may be open
    on one path. Every write is a transaction: the commit lock is held only
    for its append, and a writer that cannot get it within ten seconds raises
    ``LockedError``. ``transaction()`` holds it across a block.
    """

    @staticmethod
    def create(path: str) -> "Store":
        """Create a fresh store in ``path`` (errors if one already exists)."""
    @staticmethod
    def open(path: str) -> "Store":
        """Open an existing store read-write (errors if missing;
        ``FormatVersionError`` if another format version wrote it). Takes no
        lock: writable handles coexist."""
    @staticmethod
    def open_or_create(path: str) -> "Store":
        """Open the store at ``path``, creating it first if absent. Safe when
        many processes race to create it: one creates, the others wait for it
        and open the result."""
    @staticmethod
    def open_read_only(path: str) -> "Store":
        """Open read-only; coexists with a live writer in another process."""
    def sync(
        self,
        source: str,
        depth: Optional[int] = None,
        max_pages: int = 1000,
        concurrency: int = 8,
        token: Optional[str] = None,
        scope: Optional[str] = None,
        min_delay_ms: int = 0,
        path_contains: Optional[List[str]] = None,
        extensions: Optional[List[str]] = None,
        index_path_contains: Optional[List[str]] = None,
        pin: bool = False,
        max_bytes: Optional[int] = None,
        api_base: Optional[str] = None,
        raw_base: Optional[str] = None,
    ) -> SyncReport:
        """Acquire ``source`` and absorb the result — the whole loop in one call.

        Two source forms, routed automatically:

        - **A GitHub repository spec** —
          ``https://github.com/owner/repo[/tree/ref[/dir]]`` or
          ``github:owner/repo@ref[//dir]``. One tree-API call lists every file
          with its blob SHA; files whose SHA the graph already stores are
          revalidated without any fetch, so an unchanged repository costs
          exactly one HTTPS request. ``depth`` = directory levels below the
          subdir (default unlimited). ``token`` (a PAT; required for
          private/internal repos) is sent only to the GitHub API and raw
          hosts. ``api_base``/``raw_base`` (set together) point at a GitHub
          Enterprise deployment. Crawl-only options raise ``ValueError``.
        - **Any other http(s) URL** — the recursive crawler. ``depth`` = link
          hops (default 2); stored ``ETag``s make unchanged pages answer 304
          with no body. ``scope`` is ``'path'`` (default), ``'host'``, or
          ``'subdomains'``.

        Either way the outcome flows into the graph (new/changed documents
        upserted with segments and edges, unchanged documents' freshness
        intervals grown, gone documents tombstoned), the store compacts, and
        the ephemeral index is discarded.
        """
    def query(self, text: str, k: int = 20, depth: int = 3, budget_tokens: int = 2000) -> List[Hit]:
        """Ranked URL + anchor references for a plain-language query, rendered
        inside ``budget_tokens``. Never returns document bodies."""
    def related(self, url: str, k: int = 10) -> List[Hit]:
        """The ``k`` documents most related to ``url``, strongest first."""
    def due(self, limit: int = 64, now_ms: Optional[int] = None) -> List[DueItem]:
        """Everything due for revalidation, most important first. ``sync``
        consumes this implicitly; exposed for custom fetch loops."""
    def add(
        self,
        url: str,
        *,
        content_hash: int,
        title: Optional[str] = None,
        snippet: Optional[str] = None,
        etag: Optional[str] = None,
        pinned: bool = False,
        fetched_at_ms: Optional[int] = None,
        segments: Optional[List[Tuple[str, int, int]]] = None,
        links: Optional[List[str]] = None,
    ) -> None:
        """Record a document from any producer — the writer seam for corpora
        that do not arrive through ``sync``. ``segments`` is a list of
        ``(label, depth, importance)``; ``links`` a list of target URLs."""
    def touch(
        self,
        url: str,
        outcome: str,
        *,
        content_hash: Optional[int] = None,
        etag: Optional[str] = None,
        checked_at_ms: Optional[int] = None,
    ) -> bool:
        """Record a revalidation outcome: ``'unchanged'``, ``'changed'``,
        ``'error'``, or ``'gone'`` (tombstones, preserving history). Returns
        False if ``url`` is not a committed document."""
    def pin(self, url: str) -> bool:
        """Exempt ``url`` from eviction sweeps (still revalidated on TTL)."""
    def unpin(self, url: str) -> bool:
        """Clear the pin on ``url``."""
    def remove(self, url: str) -> None:
        """Hard-delete ``url``: severs history, edges, and segments at the
        next compaction. A later re-add is a new document."""
    def compact(self) -> int:
        """Fold the WAL into a fresh immutable base; returns the generation."""
    def reload_if_stale(self) -> bool:
        """Pick up commits and compactions made through other handles.

        Cheap when nothing changed: two file stats, no remap, no read. When
        only the log grew, only the appended bytes are read. Works on writable
        handles too (which also catch up at every write).
        Returns True when anything new became visible.
        """
        ...

    @property
    def generation(self) -> int:
        """The base generation currently serving reads."""
    def __len__(self) -> int:
        """Live (non-tombstoned) document count."""
    def set_edges(self, url: str, edges: Sequence[Tuple[str, _LineageKind, bool]]) -> None:
        """Replace ``url``'s outbound edges with ``edges``: ``(dst_url, kind,
        exact)``, ``exact`` marking an unchanged copy along ``derives``.

        Whole-set replacement, never a merge — earlier edges (``links`` from
        ``add`` included) are gone, and ``[]`` clears. A repeated destination
        keeps its first entry. ``url`` and every destination must be committed
        documents, else ``GraphRError`` and nothing changes. An unknown kind,
        an edge to ``url`` itself, or more than 4096 edges is a ``ValueError``.
        Visible to ``out_edges``, ``in_edges`` and ``walk`` on return.
        """
    def out_edges(self, url: str) -> List[_Edge]:
        """``url``'s outbound edges as ``(dst_url, kind, exact)``, in a stable
        order. ``[]`` for an unknown url; edges to removed documents are
        omitted."""
    def in_edges(self, url: str) -> List[_Edge]:
        """The edges pointing at ``url`` as ``(src_url, kind, exact)``. Always
        agrees with ``out_edges`` — before and after ``compact()``."""
    def walk(
        self,
        url: str,
        direction: Literal["out", "in", "both"] = "out",
        depth: int = 3,
        kinds: Optional[Sequence[_WalkKind]] = None,
        limit: Optional[int] = None,
    ) -> List[str]:
        """The documents reachable from ``url`` within ``depth`` hops, breadth
        first: ordered by distance, then url; each once; ``url`` itself never.

        ``'out'`` follows what ``url`` points at (upstream lineage), ``'in'``
        what points at it (downstream impact), ``'both'`` either. ``kinds``
        limits the edge types followed (default all). At most ``limit`` urls,
        the nearest ones (default 10000). ``[]`` for an unknown url.
        """
    def set_attrs(self, url: str, mapping: Dict[str, str]) -> None:
        """Replace ``url``'s attributes (``{}`` clears). At most 64 names of
        1-255 UTF-8 bytes without NUL; a name plus its value at most 262143
        bytes; all values together under 1 MiB — else ``ValueError``.
        ``url`` must be a committed document (``GraphRError``)."""
    def attrs(self, url: str) -> Dict[str, str]:
        """``url``'s attributes (``{}`` for an unknown url)."""
    def urls(self, prefix: str = "") -> List[str]:
        """Every live (committed, non-tombstoned) document url starting with
        ``prefix``, sorted. Exhaustive, not ranked; one pass over the
        documents plus a sort of the matches."""
    def transaction(self, wait: bool = True) -> "Transaction":
        """``with store.transaction() as tx:`` — a serializable block.

        The commit lock is taken at entry (waiting up to ten seconds, else
        ``LockedError``; at once with ``wait=False``), so ``tx``'s reads see
        every writer's commits and no other writer can commit until the block
        ends. ``tx`` has the store's read and write methods; the writes are
        staged and committed together — one append, one fsync — at exit, or
        dropped when the block raises. Reads show committed state, not the
        block's own staged writes. On this store, plain writes from other
        threads wait for the block; from the block's own thread they raise.
        """
    def close(self) -> None:
        """Release the mapping, the open files and the runtime now, after
        calls in flight on other threads finish. Later calls raise
        ``GraphRError("store is closed")``. Idempotent."""

class Transaction:
    """A block made by ``Store.transaction()``; use it with ``with``.

    Writes are checked when called (``set_edges``/``set_attrs`` accept
    documents added earlier in the block) and committed at exit; an error
    from the store's limits surfaces at exit and aborts the whole block.
    """

    def __enter__(self) -> "Transaction": ...
    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> bool: ...
    def add(
        self,
        url: str,
        *,
        content_hash: int,
        title: Optional[str] = None,
        snippet: Optional[str] = None,
        etag: Optional[str] = None,
        pinned: bool = False,
        fetched_at_ms: Optional[int] = None,
        segments: Optional[List[Tuple[str, int, int]]] = None,
        links: Optional[List[str]] = None,
    ) -> None: ...
    def touch(
        self,
        url: str,
        outcome: str,
        *,
        content_hash: Optional[int] = None,
        etag: Optional[str] = None,
        checked_at_ms: Optional[int] = None,
    ) -> bool:
        """As ``Store.touch``: a committed document only."""
    def pin(self, url: str) -> bool: ...
    def unpin(self, url: str) -> bool: ...
    def remove(self, url: str) -> None: ...
    def set_edges(self, url: str, edges: Sequence[Tuple[str, _LineageKind, bool]]) -> None: ...
    def set_attrs(self, url: str, mapping: Dict[str, str]) -> None: ...
    def query(self, text: str, k: int = 20, depth: int = 3, budget_tokens: int = 2000) -> List[Hit]: ...
    def related(self, url: str, k: int = 10) -> List[Hit]: ...
    def due(self, limit: int = 64, now_ms: Optional[int] = None) -> List[DueItem]: ...
    def out_edges(self, url: str) -> List[_Edge]: ...
    def in_edges(self, url: str) -> List[_Edge]: ...
    def walk(
        self,
        url: str,
        direction: Literal["out", "in", "both"] = "out",
        depth: int = 3,
        kinds: Optional[Sequence[_WalkKind]] = None,
        limit: Optional[int] = None,
    ) -> List[str]: ...
    def attrs(self, url: str) -> Dict[str, str]: ...
    def urls(self, prefix: str = "") -> List[str]: ...
    @property
    def generation(self) -> int: ...
    def __len__(self) -> int:
        """Committed live documents (the block's staged ones not counted)."""
