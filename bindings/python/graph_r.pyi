# Copyright: lituus-io, all rights reserved.
# Author: terekete <spicyzhug@gmail.com>
#
# Type stubs for the graph_r native extension module.

from typing import List, Optional, Tuple

__version__: str

class GraphRError(Exception):
    """Raised when a graph-r (or embedded link-r) operation fails."""

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
    """

    @staticmethod
    def create(path: str) -> "Store":
        """Create a fresh store in ``path`` (errors if one already exists)."""
    @staticmethod
    def open(path: str) -> "Store":
        """Open an existing store read-write (errors if missing or locked)."""
    @staticmethod
    def open_or_create(path: str) -> "Store":
        """Open the store at ``path``, creating it first if absent."""
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
    @property
    def generation(self) -> int:
        """The base generation currently serving reads."""
    def __len__(self) -> int:
        """Live (non-tombstoned) document count."""
