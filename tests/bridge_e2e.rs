// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>
//
//! Hermetic end-to-end bridge tests (feature `bridge`): a filesystem corpus
//! flows through link-r extraction into the graph store, serves standalone
//! lookups, and closes the refresh loop — no network anywhere.

#![cfg(feature = "bridge")]

use graph_r::traverse::LendingIterator;
use graph_r::{bridge, Config, QueryOpts, Store, UrlKey};
use link_r::facade::RefreshOptions;
use link_r::{FsSource, LinkIndex, SourceRef};
use std::io::Write as _;

fn write_corpus(dir: &std::path::Path) {
    let a = "# Alpha Install\n\nHow to install the alpha toolchain end to end.\n\n\
             ## Alpha Linux\n\nInstall on linux with the package manager.\n\n\
             See [the spec](https://spec.example.com/alpha) for details.\n";
    let b = "# Beta Usage\n\nUsing the beta interface for daily work.\n\n\
             ## Beta Shortcuts\n\nKeyboard shortcuts for the beta.\n";
    std::fs::File::create(dir.join("alpha.md")).unwrap().write_all(a.as_bytes()).unwrap();
    std::fs::File::create(dir.join("beta.md")).unwrap().write_all(b.as_bytes()).unwrap();
}

fn build_index(dir: &std::path::Path) -> (LinkIndex, link_r::facade::UpdateReport) {
    let mut index = LinkIndex::in_memory().unwrap();
    let root = SourceRef::fs(dir);
    let report = futures::executor::block_on(async {
        index.ingest_from(&FsSource, &root, 8, false).await.unwrap()
    });
    (index, report)
}

#[test]
fn corpus_flows_into_graph_and_serves_standalone() {
    let corpus = tempfile::tempdir().unwrap();
    write_corpus(corpus.path());
    let (index, report) = build_index(corpus.path());
    assert_eq!(report.added, 2);
    assert_eq!(report.pages.len(), 2);

    let kb = tempfile::tempdir().unwrap();
    let store = Store::create(kb.path(), Config::default()).unwrap();
    let bridge_report = bridge::ingest_update(&store, &index, &report).unwrap();
    assert_eq!(bridge_report.upserted, 2);
    assert!(bridge_report.segments >= 4, "two headings per doc");
    assert!(bridge_report.edges >= 1, "external spec link became an edge");

    // Cross-crate key contract: graph-r's UrlKey::of(canonical url) must equal
    // link-r's UrlKey for the same document.
    for doc in index.export().unwrap() {
        assert_eq!(UrlKey::of(&doc.meta.url).raw(), doc.meta.url_key.raw());
    }

    // The index can now be dropped entirely — graph-r serves alone.
    let alpha_key = {
        let exported: Vec<_> = index.export().unwrap().map(|d| d.meta.url.clone()).collect();
        let alpha_url = exported.iter().find(|u| u.contains("alpha")).unwrap().clone();
        drop(index);
        UrlKey::of(&alpha_url)
    };

    let snap = store.snapshot();
    assert_eq!(snap.len(), 2);
    let alpha = snap.node(alpha_key).expect("alpha node");
    assert_eq!(alpha.title.unwrap(), "Alpha Install");
    assert!(alpha.snippet.is_some());

    // Segments carry heading anchors with the deterministic importance prior.
    let segs = snap.segments(alpha_key);
    assert_eq!(segs.len(), 2);
    assert_eq!(segs[0].label, "Alpha Install");
    assert_eq!(segs[0].depth, 1);
    assert!(segs[0].importance > segs[1].importance, "H1 first outranks H2 later");

    // The external link exists as a stub edge target.
    let mut it = snap.neighbors(alpha_key);
    let mut found_spec = false;
    while let Some(e) = it.next() {
        if e.dst_key == UrlKey::of("https://spec.example.com/alpha") {
            found_spec = true;
        }
    }
    assert!(found_spec, "spec link persisted as a graph edge");

    // Token-budgeted lookup answers with the URL + heading anchor.
    let hits = snap.query("alpha install linux", &QueryOpts::default());
    assert!(!hits.is_empty());
    assert_eq!(hits[0].key, alpha_key);
    let anchor = hits[0].anchor.expect("anchor");
    assert!(anchor.label.starts_with("Alpha"));
}

/// A refresh fetcher over the file corpus: one page changed, one unchanged.
struct MockRefresh {
    changed_url: String,
    new_body: &'static [u8],
}

impl link_r::Fetcher for MockRefresh {
    type FetchFuture<'a>
        = std::future::Ready<link_r::Result<link_r::Fetched<'a>>>
    where
        Self: 'a;
    fn fetch<'a>(
        &'a self,
        resource: &'a link_r::Resource,
        _opts: link_r::FetchOptions<'a>,
    ) -> Self::FetchFuture<'a> {
        let url = link_r::canonicalize(&resource.url);
        let result = if url == self.changed_url {
            Ok(link_r::Fetched {
                meta: link_r::FetchMeta {
                    kind: link_r::ResourceKind::Markdown,
                    etag: None,
                    status: 200,
                    final_url: None,
                },
                payload: link_r::DocPayload::Owned(bytes::Bytes::from_static(self.new_body)),
            })
        } else {
            Err(link_r::Error::not_modified(resource.url.as_str()))
        };
        std::future::ready(result)
    }
}

#[test]
fn due_list_drives_refresh_and_freshness_feeds_back() {
    let corpus = tempfile::tempdir().unwrap();
    write_corpus(corpus.path());
    let (mut index, report) = build_index(corpus.path());

    let kb = tempfile::tempdir().unwrap();
    let mut cfg = Config::default();
    cfg.ttl.importance_bias_permille = 0;
    let store = Store::create(kb.path(), cfg).unwrap();
    bridge::ingest_update(&store, &index, &report).unwrap();

    // Everything comes due after the base interval.
    let far = bridge::now_ms() + u64::from(cfg.ttl.base_s) * 1000 + 60_000;
    let due = bridge::due_urls(&store, far, 16);
    assert_eq!(due.len(), 2, "both documents due");

    let alpha_url = due.iter().find(|u| u.contains("alpha")).unwrap().clone();
    let beta_url = due.iter().find(|u| u.contains("beta")).unwrap().clone();
    let fetcher = MockRefresh {
        changed_url: alpha_url.clone(),
        new_body: b"# Alpha Install\n\nCompletely rewritten alpha guide today.\n",
    };
    let opts = RefreshOptions {
        urls: Some(due.iter().filter_map(|u| link_r::UrlKey::parse(u).ok()).collect()),
        ttl: std::time::Duration::ZERO,
        max_age: None,
        evict_unreachable: true,
        concurrency: 2,
    };
    let refresh =
        futures::executor::block_on(async { index.refresh_with(&fetcher, opts).await.unwrap() });
    assert_eq!(refresh.refreshed, 1);
    assert_eq!(refresh.unchanged, 1);

    bridge::ingest_refresh(&store, &index, &refresh).unwrap();
    let snap = store.snapshot();
    let alpha = snap.node(UrlKey::of(&alpha_url)).unwrap();
    let beta = snap.node(UrlKey::of(&beta_url)).unwrap();
    assert_eq!(alpha.changes, 1, "change observed");
    assert!(alpha.interval_s < cfg.ttl.base_s, "changed page rechecks sooner");
    assert_eq!(beta.checks, 1, "unchanged page touched");
    assert!(beta.interval_s > cfg.ttl.base_s, "unchanged page backs off");
}

#[test]
fn absorb_offloads_whole_index() {
    let corpus = tempfile::tempdir().unwrap();
    write_corpus(corpus.path());
    let (index, _) = build_index(corpus.path());

    let kb = tempfile::tempdir().unwrap();
    let store = Store::create(kb.path(), Config::default()).unwrap();
    let report = bridge::absorb(&store, &index).unwrap();
    assert_eq!(report.upserted, 2);
    drop(index);

    let snap = store.snapshot();
    assert_eq!(snap.len(), 2);
    let hits = snap.query("beta shortcuts", &QueryOpts::default());
    assert!(!hits.is_empty());
    assert!(hits[0].url.contains("beta"));
}

/// The stateless-loop seeds: after absorbing a crawl and discarding the index,
/// `crawl_seed` must hand back every stored validator and edge so a fresh crawl
/// revalidates for free. (The crawl side of that contract is proven in link-r's
/// `conditional_recrawl` suite; this pins the extraction side.)
#[test]
fn crawl_seed_extracts_validators_and_edges_from_the_graph() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create(dir.path(), Config::default()).unwrap();
    {
        let mut w = store.writer().unwrap();
        for (url, etag, links) in [
            ("https://x.dev/docs", Some("\"r1\""), vec!["https://x.dev/docs/a"]),
            ("https://x.dev/docs/a", Some("\"a1\""), vec!["https://x.dev/docs/b"]),
            ("https://x.dev/docs/b", None, vec![]), // no validator stored
        ] {
            w.upsert_node(&graph_r::DocRecord {
                url,
                url_key: UrlKey::of(url),
                content_hash: 1,
                fetched_at_ms: 1_000,
                title: None,
                snippet: None,
                etag,
                pinned: false,
            })
            .unwrap();
            let edges: Vec<(UrlKey, graph_r::EdgeType, u16)> =
                links.iter().map(|l| (UrlKey::of(l), graph_r::EdgeType::Link, 65_535)).collect();
            w.set_edges(UrlKey::of(url), &edges);
        }
        w.commit().unwrap();
    }
    store.compact().unwrap();

    let seed = bridge::crawl_seed(&store);

    // Validators: exactly the two documents that stored an etag, keyed by the
    // same xxh3 link-r would compute for the canonical URL.
    let mut validators: Vec<(u64, String)> =
        seed.validators.iter().map(|(k, e)| (k.raw(), e.to_string())).collect();
    validators.sort();
    let mut expected = vec![
        (UrlKey::of("https://x.dev/docs").0, "\"r1\"".to_string()),
        (UrlKey::of("https://x.dev/docs/a").0, "\"a1\"".to_string()),
    ];
    expected.sort();
    assert_eq!(validators, expected);

    // Known edges: both linking documents, with resolvable target URLs. The
    // edge to /docs/b resolves because b is a real node with a URL label.
    assert_eq!(seed.known_edges.len(), 2);
    let edges_of = |url: &str| -> Vec<String> {
        seed.known_edges
            .iter()
            .find(|(k, _)| k.raw() == UrlKey::of(url).0)
            .map(|(_, kids)| kids.iter().map(std::string::ToString::to_string).collect())
            .unwrap_or_default()
    };
    assert_eq!(edges_of("https://x.dev/docs"), vec!["https://x.dev/docs/a".to_string()]);
    assert_eq!(edges_of("https://x.dev/docs/a"), vec!["https://x.dev/docs/b".to_string()]);

    // A tombstoned document leaves the seed set: it was observed gone.
    {
        let mut w = store.writer().unwrap();
        w.touch(
            UrlKey::of("https://x.dev/docs/a"),
            graph_r::Touch {
                checked_at_ms: 2_000,
                outcome: graph_r::Outcome::Gone,
                content_hash: None,
                etag: None,
            },
        )
        .unwrap();
        w.commit().unwrap();
    }
    let seed = bridge::crawl_seed(&store);
    assert!(
        !seed.validators.iter().any(|(k, _)| k.raw() == UrlKey::of("https://x.dev/docs/a").0),
        "a tombstoned document must not be re-crawled"
    );
}
