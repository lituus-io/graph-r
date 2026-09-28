// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>
//
//! # graph-r-kb
//!
//! One knowledge base from the three libraries, on one object store:
//!
//! - **link-r** acquires and ranks: crawling and conditional re-crawls,
//!   extraction, hybrid dense + BM25 search.
//! - **graph-r** remembers and serves: the historical link graph, heading
//!   anchors, adaptive freshness (what is due for revalidation), token-budgeted
//!   local lookups.
//! - **objstore-r** makes both durable on Google Cloud Storage (or a local
//!   directory, or memory) through `graph-r-mirror` and `link-r-objstore`.
//!
//! ```text
//! {prefix}/graph/…   graph-r store (HEAD + base + WAL segments)
//! {prefix}/links/…   link-r index (HEAD + .lnkr image)
//! ```
//!
//! A **writer** ([`KnowledgeBase::writer`]) holds the graph's lease, keeps the
//! graph in a local directory (tmpfs on a stateless instance; durability is
//! the bucket), syncs sources through the bridge and publishes both halves
//! concurrently, fenced. A **replica** ([`KnowledgeBase::replica`]) needs no
//! disk at all: the graph base and the link index are served in place from
//! the fetched bytes, and [`KnowledgeBase::refresh`] costs two metadata
//! requests when nothing changed.
//!
//! [`KnowledgeBase::lookup`] fuses graph-r's anchored, graph-aware lookup with
//! link-r's hybrid search by reciprocal-rank fusion — no network.
//!
//! Futures are unboxed (`impl Future` over borrowed state); no `Arc`, no
//! `dyn`, no spawned tasks: concurrency is joined futures on the caller's
//! task.

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations, rust_2018_idioms, unreachable_pub)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions, clippy::missing_errors_doc)]
// House style: public operations return explicit `impl Future + 'a` over borrowed
// state (the lifetime contract is in the signature), never `async fn`.
#![allow(clippy::manual_async_fn)]

pub use graph_r;
pub use graph_r_mirror;
pub use link_r;
pub use link_r_objstore;
pub use objstore_r::{self, ObjectStore};

use compact_str::CompactString;
use futures_util::future::try_join;
use graph_r::{Config, Durability, QueryOpts, Store, UrlKey as GraphKey};
use graph_r_mirror::Mirror;
use link_r::facade::{DefaultEmbedder, RefreshOptions};
use link_r::source::{CrawlSource, Source};
use link_r::{Embedder, Fetcher, Filter, LinkIndex, SourceRef};
use link_r_objstore::Remote;
use std::collections::HashMap;
use std::future::Future;
use std::path::Path;
use std::time::Duration;

/// Result alias.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// What can go wrong.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The graph store.
    #[error("graph: {0}")]
    Graph(#[from] graph_r::Error),
    /// The link index.
    #[error("links: {0}")]
    Links(#[from] link_r::Error),
    /// Publishing or loading the graph.
    #[error("graph remote: {0}")]
    Mirror(#[from] graph_r_mirror::Error),
    /// Publishing or loading the link index.
    #[error("links remote: {0}")]
    Remote(#[from] link_r_objstore::Error),
    /// The operation needs the writer role.
    #[error("this knowledge base is a read-only replica")]
    ReadOnly,
}

/// How a writer runs.
#[derive(Clone, Debug)]
pub struct WriterOptions {
    /// Lease holder name (e.g. the instance id).
    pub holder: String,
    /// Lease length; every publish renews it.
    pub lease_ttl: Duration,
    /// Local graph store configuration. Defaults to no fsync and no inline
    /// compaction: durability is the bucket, and [`KnowledgeBase::publish`]
    /// compacts when due, off the commit path.
    pub graph: Config,
}

impl WriterOptions {
    /// Defaults for `holder`.
    #[must_use]
    pub fn new(holder: impl Into<String>) -> Self {
        Self {
            holder: holder.into(),
            lease_ttl: Duration::from_secs(120),
            graph: Config { durability: Durability::Never, ..Config::default() },
        }
    }
}

/// What a sync or refresh did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SyncReport {
    /// Pages added to the link index.
    pub added: usize,
    /// Pages updated.
    pub updated: usize,
    /// Pages revalidated unchanged (a 304 or an identical body).
    pub unchanged: usize,
    /// Pages removed.
    pub removed: usize,
    /// Pages that failed to fetch.
    pub failed: usize,
    /// Graph documents written through the bridge.
    pub graph_docs: usize,
    /// Bytes uploaded by the publish that followed.
    pub uploaded: u64,
}

/// One fused lookup answer. Owned and small: a URL, where to look in it, and
/// why it ranked.
#[derive(Clone, Debug, PartialEq)]
pub struct KbHit {
    /// Canonical URL.
    pub url: String,
    /// Title, if known.
    pub title: Option<CompactString>,
    /// Distilled snippet, if known.
    pub snippet: Option<CompactString>,
    /// Best heading anchor (label, byte range) from the graph, if any.
    pub anchor: Option<(CompactString, Option<(u32, u32)>)>,
    /// Fused score (higher is better).
    pub score: f32,
    /// Rank in the graph lookup (0-based), if it appeared there.
    pub graph_rank: Option<u32>,
    /// Rank in the link-r hybrid search (0-based), if it appeared there.
    pub link_rank: Option<u32>,
}

/// Lookup knobs.
#[derive(Clone, Debug)]
pub struct LookupOpts {
    /// Hits returned.
    pub limit: usize,
    /// graph-r lookup options (depth, seeds, token budget).
    pub graph: QueryOpts,
    /// link-r metadata prefilter.
    pub filter: Filter,
    /// Link-graph boost for link-r's ranking (0 = off).
    pub graph_boost: f32,
    /// Weights of the two rankings in the fusion.
    pub graph_weight: f32,
    /// See `graph_weight`.
    pub link_weight: f32,
    /// Reciprocal-rank-fusion constant.
    pub rrf_k: f32,
}

impl Default for LookupOpts {
    fn default() -> Self {
        Self {
            limit: 10,
            graph: QueryOpts::default(),
            filter: Filter::All,
            graph_boost: 0.0,
            graph_weight: 1.0,
            link_weight: 1.0,
            rrf_k: 60.0,
        }
    }
}

/// Weighted reciprocal-rank contribution of a 0-based rank.
fn rrf(weight: f32, k: f32, rank: usize) -> f32 {
    weight / (k + f32::from(u16::try_from(rank).unwrap_or(u16::MAX)) + 1.0)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    Writer,
    Replica,
}

/// graph-r + link-r as one knowledge base on an object store. See the crate
/// docs.
#[derive(Debug)]
pub struct KnowledgeBase<'o, O: ObjectStore, E: Embedder = DefaultEmbedder> {
    graph: Store,
    links: LinkIndex<E>,
    graph_remote: Mirror<'o, O>,
    link_remote: Remote<'o, O>,
    role: Role,
}

fn remotes<'o, O: ObjectStore>(
    objects: &'o O,
    prefix: &str,
) -> Result<(Mirror<'o, O>, Remote<'o, O>)> {
    let p = prefix.trim_end_matches('/');
    let join = |part: &str| if p.is_empty() { part.to_owned() } else { format!("{p}/{part}") };
    Ok((Mirror::new(objects, &join("graph"))?, Remote::new(objects, &join("links"))?))
}

impl<'o, O: ObjectStore, E: Embedder + 'o> KnowledgeBase<'o, O, E> {
    /// Become the writer: take the graph's lease, hydrate the graph into
    /// `dir` and the link index into memory concurrently (or start empty when
    /// nothing is published yet).
    pub fn writer<'a>(
        objects: &'o O,
        prefix: &'a str,
        dir: &'a Path,
        embedder: E,
        opts: WriterOptions,
    ) -> impl Future<Output = Result<Self>> + 'a
    where
        'o: 'a,
    {
        async move {
            let (graph_remote, link_remote) = remotes(objects, prefix)?;
            let ttl = u64::try_from(opts.lease_ttl.as_millis()).unwrap_or(u64::MAX);
            graph_remote.acquire_lease(&opts.holder, ttl).await?;
            let pull = async { graph_remote.pull_into(dir).await.map_err(Error::from) };
            let load =
                async { link_remote.load_or_empty_with(embedder).await.map_err(Error::from) };
            let (pulled, (links, _)) = try_join(pull, load).await?;
            let graph = if pulled.is_some() || dir.join(graph_r::store::BASE_FILE).exists() {
                Store::open_cfg(dir, opts.graph)?
            } else {
                Store::create(dir, opts.graph)?
            };
            graph.set_compact_inline(false);
            Ok(Self { graph, links, graph_remote, link_remote, role: Role::Writer })
        }
    }

    /// Open a replica served from memory: the graph base and the link index
    /// are used in place from the fetched bytes (fetched concurrently). `None`
    /// until both halves have been published.
    pub fn replica<'a>(
        objects: &'o O,
        prefix: &'a str,
        embedder: E,
    ) -> impl Future<Output = Result<Option<Self>>> + 'a
    where
        'o: 'a,
    {
        async move {
            let (graph_remote, link_remote) = remotes(objects, prefix)?;
            let graph = async { graph_remote.load_image().await.map_err(Error::from) };
            let links = async { link_remote.load_with(embedder).await.map_err(Error::from) };
            let (graph, links) = try_join(graph, links).await?;
            let (Some(graph), Some(links)) = (graph, links) else { return Ok(None) };
            Ok(Some(Self { graph, links, graph_remote, link_remote, role: Role::Replica }))
        }
    }

    /// The graph store (snapshots, traversal, due-lists).
    #[must_use]
    pub fn graph(&self) -> &Store {
        &self.graph
    }

    /// The link index (search, related, export).
    #[must_use]
    pub fn links(&self) -> &LinkIndex<E> {
        &self.links
    }

    /// True for a replica.
    #[must_use]
    pub fn is_replica(&self) -> bool {
        self.role == Role::Replica
    }

    fn need_writer(&self) -> Result<()> {
        if self.role == Role::Writer { Ok(()) } else { Err(Error::ReadOnly) }
    }

    /// Ingest from any link-r [`Source`], absorb the result into the graph
    /// through the bridge, and publish.
    pub fn sync<'a, S: Source>(
        &'a mut self,
        source: &'a S,
        root: &'a SourceRef,
        embed_batch: usize,
    ) -> impl Future<Output = Result<SyncReport>> + 'a {
        async move {
            self.need_writer()?;
            let report = self.links.ingest_from(source, root, embed_batch, false).await?;
            let bridged = graph_r::bridge::ingest_update_with(&self.graph, &self.links, &report)?;
            let mut out = SyncReport {
                added: report.added,
                updated: report.updated,
                unchanged: report.unchanged,
                failed: report.failed,
                graph_docs: bridged.upserted + bridged.touched + bridged.tombstoned,
                ..SyncReport::default()
            };
            out.uploaded = self.publish().await?;
            Ok(out)
        }
    }

    /// Crawl `url` with `fetcher` — conditionally: validators and known links
    /// come from the graph, so unchanged pages cost a 304 and no body — then
    /// absorb and publish. `configure` tunes the crawl (depth, scope, limits).
    pub fn sync_crawl<'a, F: Fetcher + 'a>(
        &'a mut self,
        fetcher: F,
        url: &'a str,
        configure: impl FnOnce(CrawlSource<F>) -> CrawlSource<F> + 'a,
    ) -> impl Future<Output = Result<SyncReport>> + 'a {
        async move {
            self.need_writer()?;
            let seed = graph_r::bridge::crawl_seed(&self.graph);
            let source = configure(
                CrawlSource::new(fetcher).validators(seed.validators).known_edges(seed.known_edges),
            );
            let root = SourceRef::http(url)?;
            let mut report = self.links.ingest_from(&source, &root, 64, false).await?;
            report.failed = source.failed_count() as usize;
            for u in source.take_revalidated() {
                report.unchanged += 1;
                report.pages.push(link_r::facade::PageOutcome {
                    url: link_r::canonicalize(&u),
                    url_key: link_r::UrlKey::from_url(&u),
                    change: link_r::facade::PageChange::Unchanged,
                    title: None,
                    headings: Vec::new(),
                    keywords: Vec::new(),
                });
            }
            let bridged = graph_r::bridge::ingest_update_with(&self.graph, &self.links, &report)?;
            let mut out = SyncReport {
                added: report.added,
                updated: report.updated,
                unchanged: report.unchanged,
                failed: report.failed,
                graph_docs: bridged.upserted + bridged.touched + bridged.tombstoned,
                ..SyncReport::default()
            };
            out.uploaded = self.publish().await?;
            Ok(out)
        }
    }

    /// Revalidate what the graph says is due (at most `max` pages, adaptive
    /// per-page intervals), conditionally, then absorb and publish.
    pub fn refresh_due<'a, F: Fetcher>(
        &'a mut self,
        fetcher: &'a F,
        now_ms: u64,
        max: usize,
    ) -> impl Future<Output = Result<SyncReport>> + 'a {
        async move {
            self.need_writer()?;
            let due = graph_r::bridge::due_urls(&self.graph, now_ms, max);
            if due.is_empty() {
                return Ok(SyncReport::default());
            }
            let urls = due.iter().filter_map(|u| link_r::UrlKey::parse(u).ok()).collect();
            let opts = RefreshOptions {
                urls: Some(urls),
                ttl: Duration::ZERO,
                ..RefreshOptions::default()
            };
            let report = self.links.refresh_with(fetcher, opts).await?;
            let bridged = graph_r::bridge::ingest_refresh_with(&self.graph, &self.links, &report)?;
            let mut out = SyncReport {
                updated: report.refreshed,
                unchanged: report.unchanged,
                removed: report.removed,
                failed: report.failed,
                graph_docs: bridged.upserted + bridged.touched + bridged.tombstoned,
                ..SyncReport::default()
            };
            out.uploaded = self.publish().await?;
            Ok(out)
        }
    }

    /// Settle and publish both halves concurrently: compact the graph if due,
    /// freeze the link index into its zero-copy image, then ship. Returns the
    /// bytes uploaded (0 when nothing changed). Renews the lease.
    pub fn publish(&mut self) -> impl Future<Output = Result<u64>> + '_ {
        async move {
            self.need_writer()?;
            if self.graph.compaction_due() {
                self.graph.compact()?;
            }
            self.links.freeze()?;
            let g = async { self.graph_remote.publish(&self.graph).await.map_err(Error::from) };
            let l = async { self.link_remote.publish(&self.links).await.map_err(Error::from) };
            let (g, l) = try_join(g, l).await?;
            Ok(g.uploaded + if l.uploaded { l.len } else { 0 })
        }
    }

    /// Replica refresh: both halves concurrently, each one metadata request
    /// when unchanged. Returns whether anything new became visible.
    pub fn refresh(&mut self) -> impl Future<Output = Result<bool>> + '_ {
        async move {
            if self.role == Role::Writer {
                return Ok(false); // the writer is the source of truth
            }
            let Self { graph, links, graph_remote, link_remote, .. } = self;
            let g = async { graph_remote.refresh_image(graph).await.map_err(Error::from) };
            let l = async { link_remote.load_if_changed_into(links).await.map_err(Error::from) };
            let (g, l) = try_join(g, l).await?;
            Ok(g || l)
        }
    }

    /// Give the writer lease up (so another instance can take over at once).
    pub fn release(&self) -> impl Future<Output = Result<()>> + '_ {
        async move { Ok(self.graph_remote.release_lease().await?) }
    }

    /// Fused lookup: graph-r's anchored, graph-aware ranking and link-r's
    /// hybrid dense + BM25 ranking, combined by weighted reciprocal-rank
    /// fusion on the canonical URL. Local only — no network.
    pub fn lookup<'a>(
        &'a self,
        query: &'a str,
        opts: &'a LookupOpts,
    ) -> impl Future<Output = Result<Vec<KbHit>>> + 'a {
        async move {
            let wide = opts.limit.max(1) * 3;
            // Joined, links first: its first poll sends the query embedding
            // (a network call for a remote embedder), then the graph lookup
            // runs while that is in flight.
            let links = self.links.search_ranked(query, wide, &opts.filter, opts.graph_boost);
            let graph = async {
                let snap = self.graph.snapshot();
                let graph_opts = QueryOpts { limit: wide, ..opts.graph };
                snap.query(query, &graph_opts)
                    .iter()
                    .enumerate()
                    .map(|(rank, h)| KbHit {
                        url: h.url.to_owned(),
                        title: h.title.map(CompactString::from),
                        snippet: h.snippet.map(CompactString::from),
                        anchor: h.anchor.map(|a| (CompactString::from(a.label), a.byte_range)),
                        score: rrf(opts.graph_weight, opts.rrf_k, rank),
                        graph_rank: u32::try_from(rank).ok(),
                        link_rank: None,
                    })
                    .collect::<Vec<KbHit>>()
            };
            let (links, mut fused) = futures_util::future::join(links, graph).await;
            let links = links?;
            fused.reserve(links.len());
            let by_url: HashMap<String, usize> =
                fused.iter().enumerate().map(|(i, h)| (h.url.clone(), i)).collect();
            for (rank, r) in links.iter().enumerate() {
                let score = rrf(opts.link_weight, opts.rrf_k, rank);
                if let Some(&i) = by_url.get(&r.url) {
                    let hit = &mut fused[i];
                    hit.score += score;
                    hit.link_rank = u32::try_from(rank).ok();
                    if hit.title.is_none() {
                        hit.title = r.title.as_deref().map(CompactString::from);
                    }
                    if hit.snippet.is_none() && !r.snippet.is_empty() {
                        hit.snippet = Some(CompactString::from(r.snippet.as_str()));
                    }
                } else {
                    fused.push(KbHit {
                        url: r.url.clone(),
                        title: r.title.as_deref().map(CompactString::from),
                        snippet: (!r.snippet.is_empty())
                            .then(|| CompactString::from(r.snippet.as_str())),
                        anchor: None,
                        score,
                        graph_rank: None,
                        link_rank: u32::try_from(rank).ok(),
                    });
                }
            }
            drop(by_url);
            fused.sort_by(|a, b| b.score.total_cmp(&a.score).then_with(|| a.url.cmp(&b.url)));
            fused.truncate(opts.limit);
            Ok(fused)
        }
    }

    /// Documents related to `url`: link-graph neighbours from link-r and
    /// graph-r's related set, fused. Local only.
    pub fn related(&self, url: &str, k: usize) -> Result<Vec<KbHit>> {
        let from_links = self.links.related(url, k * 2)?;
        let snap = self.graph.snapshot();
        let from_graph = snap.related(GraphKey::of(url), k * 2);
        let mut out: Vec<KbHit> = Vec::new();
        let mut by_url: HashMap<String, usize> = HashMap::new();
        for (rank, h) in from_graph.iter().enumerate() {
            by_url.insert(h.url.to_owned(), out.len());
            out.push(KbHit {
                url: h.url.to_owned(),
                title: h.title.map(CompactString::from),
                snippet: h.snippet.map(CompactString::from),
                anchor: h.anchor.map(|a| (CompactString::from(a.label), a.byte_range)),
                score: rrf(1.0, 60.0, rank),
                graph_rank: u32::try_from(rank).ok(),
                link_rank: None,
            });
        }
        for (rank, r) in from_links.iter().enumerate() {
            let s = rrf(1.0, 60.0, rank);
            match by_url.get(&r.url) {
                Some(&i) => {
                    out[i].score += s;
                    out[i].link_rank = u32::try_from(rank).ok();
                }
                None => out.push(KbHit {
                    url: r.url.clone(),
                    title: r.title.as_deref().map(CompactString::from),
                    snippet: None,
                    anchor: None,
                    score: s,
                    graph_rank: None,
                    link_rank: u32::try_from(rank).ok(),
                }),
            }
        }
        out.sort_by(|a, b| b.score.total_cmp(&a.score).then_with(|| a.url.cmp(&b.url)));
        out.truncate(k);
        Ok(out)
    }
}
