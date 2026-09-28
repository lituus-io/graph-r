// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>
//
//! Random sequences of site edits, conditional crawls, due-list refreshes and
//! replica refreshes: after every replica refresh, the replica (served from
//! memory) answers every lookup exactly like the writer, and holds the same
//! graph.

mod support;

use graph_r_kb::{KnowledgeBase, LookupOpts, WriterOptions};
use link_r::facade::{DefaultEmbedder, default_embedder};
use objstore_r::Mem;
use proptest::prelude::*;
use support::{ROOT, Site};

const PAGES: [&str; 4] = ["", "install", "api", "faq"];
const WORDS: [&str; 6] = ["quantum", "linux", "tokens", "licensing", "webhooks", "ports"];
const QUERIES: [&str; 5] =
    ["quantum", "install linux", "api tokens", "licensing", "webhooks ports"];

#[derive(Debug, Clone)]
enum Op {
    Edit(usize, usize),
    Crawl,
    RefreshDue,
    ReplicaRefresh,
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        3 => (0..PAGES.len(), 0..WORDS.len()).prop_map(|(p, w)| Op::Edit(p, w)),
        2 => Just(Op::Crawl),
        1 => Just(Op::RefreshDue),
        2 => Just(Op::ReplicaRefresh),
    ]
}

async fn answers(kb: &KnowledgeBase<'_, Mem, DefaultEmbedder>) -> Vec<Vec<String>> {
    let mut out = Vec::new();
    for q in QUERIES {
        out.push(
            kb.lookup(q, &LookupOpts::default())
                .await
                .unwrap()
                .into_iter()
                .map(|h| h.url)
                .collect(),
        );
    }
    out
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: std::env::var("PROPTEST_CASES").ok().and_then(|v| v.parse().ok()).unwrap_or(24),
        ..ProptestConfig::default()
    })]

    #[test]
    fn replicas_answer_like_the_writer(ops in prop::collection::vec(op(), 1..16)) {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            let site = Site::new();
            let mem = Mem::default();
            let dir = tempfile::tempdir().unwrap();
            let mut kb = KnowledgeBase::writer(&mem, "p", dir.path(), default_embedder().unwrap(), WriterOptions::new("w"))
                .await
                .unwrap();
            kb.sync_crawl(&site, ROOT, |c| c.depth(3)).await.unwrap();
            let mut replica = KnowledgeBase::<_, DefaultEmbedder>::replica(&mem, "p", default_embedder().unwrap())
                .await
                .unwrap()
                .unwrap();
            let far = graph_r::store::now_ms() + 365 * 24 * 3600 * 1000;
            for op in &ops {
                match op {
                    Op::Edit(p, w) => {
                        let title = if PAGES[*p].is_empty() { "Widget docs" } else { PAGES[*p] };
                        site.edit(PAGES[*p], title, &format!("A page about {} things.", WORDS[*w]));
                    }
                    Op::Crawl => { kb.sync_crawl(&site, ROOT, |c| c.depth(3)).await.unwrap(); }
                    Op::RefreshDue => { kb.refresh_due(&&site, far, 16).await.unwrap(); }
                    Op::ReplicaRefresh => {
                        replica.refresh().await.unwrap();
                        assert_eq!(answers(&replica).await, answers(&kb).await, "after {ops:?}");
                        assert_eq!(replica.graph().len(), kb.graph().len());
                        assert_eq!(replica.links().len(), kb.links().len());
                    }
                }
            }
            kb.publish().await.unwrap();
            replica.refresh().await.unwrap();
            assert_eq!(answers(&replica).await, answers(&kb).await);
        });
    }
}
