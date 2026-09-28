// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>
//
//! A mock documentation site behind a real link-r `Fetcher`: HTML pages that
//! link to each other, ETags, 304 on a matching `If-None-Match`, counters of
//! full fetches and revalidations, and edits between runs.

#![allow(dead_code)]

use bytes::Bytes;
use link_r::{DocPayload, FetchMeta, FetchOptions, Fetched, Fetcher, Resource, ResourceKind};
use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

pub const ROOT: &str = "https://docs.site.test/";

#[derive(Debug, Default)]
pub struct Site {
    pages: Mutex<BTreeMap<String, (String, u32)>>, // url -> (html, version)
    pub full: AtomicUsize,
    pub not_modified: AtomicUsize,
}

fn page(title: &str, body: &str, links: &[&str]) -> String {
    let a: String = links.iter().map(|l| format!("<a href=\"{l}\">{l}</a> ")).collect();
    format!(
        "<html><head><title>{title}</title></head><body><h1>{title}</h1><h2>Details</h2><p>{body}</p>{a}</body></html>"
    )
}

impl Site {
    pub fn new() -> Self {
        let s = Self::default();
        s.put(
            ROOT,
            &page(
                "Widget docs",
                "Start here for the widget toolchain.",
                &["/install", "/api", "/faq"],
            ),
        );
        s.put(
            &format!("{ROOT}install"),
            &page(
                "Install",
                "Install the widget toolchain on linux with the package manager.",
                &["/", "/api"],
            ),
        );
        s.put(
            &format!("{ROOT}api"),
            &page(
                "API reference",
                "Authentication tokens and endpoints of the widget api.",
                &["/install"],
            ),
        );
        s.put(
            &format!("{ROOT}faq"),
            &page("FAQ", "Common questions about widget licensing.", &["/"]),
        );
        s
    }

    /// A larger site: `n` pages in a ring with cross links, varied vocabulary.
    pub fn with_pages(n: usize) -> Self {
        const TOPICS: [&str; 12] = [
            "install",
            "linux",
            "windows",
            "api",
            "tokens",
            "webhooks",
            "billing",
            "quotas",
            "regions",
            "networking",
            "storage",
            "security",
        ];
        let s = Self::default();
        let mut root_links = Vec::new();
        for i in 0..n {
            let a = TOPICS[i % TOPICS.len()];
            let b = TOPICS[(i * 7 + 3) % TOPICS.len()];
            let links = [format!("/p{}", (i + 1) % n), format!("/p{}", (i * 13 + 5) % n)];
            let link_refs: Vec<&str> = links.iter().map(String::as_str).collect();
            s.put(
                &format!("{ROOT}p{i}"),
                &page(
                    &format!("{a} {b} guide {i}"),
                    &format!("How to configure {a} with {b} for page {i}."),
                    &link_refs,
                ),
            );
            if i < 64 {
                root_links.push(format!("/p{i}"));
            }
        }
        let refs: Vec<&str> = root_links.iter().map(String::as_str).collect();
        s.put(ROOT, &page("Index", "All guides.", &refs));
        s
    }

    pub fn put(&self, url: &str, html: &str) {
        let mut p = self.pages.lock().unwrap();
        let v = p.get(url).map_or(1, |(_, v)| v + 1);
        p.insert(url.to_owned(), (html.to_owned(), v));
    }

    pub fn edit(&self, path: &str, title: &str, body: &str) {
        self.put(&format!("{ROOT}{path}"), &page(title, body, &["/"]));
    }

    pub fn reset_counters(&self) {
        self.full.store(0, Ordering::Relaxed);
        self.not_modified.store(0, Ordering::Relaxed);
    }
}

impl Fetcher for &Site {
    type FetchFuture<'a>
        = std::future::Ready<link_r::Result<Fetched<'a>>>
    where
        Self: 'a;

    fn fetch<'a>(
        &'a self,
        resource: &'a Resource,
        opts: FetchOptions<'a>,
    ) -> Self::FetchFuture<'a> {
        let url = resource.url.as_str().to_owned();
        let pages = self.pages.lock().unwrap();
        let out = match pages.get(&url) {
            None => Err(link_r::Error::not_found(url)),
            Some((html, v)) => {
                let etag = format!("\"v{v}\"");
                if opts.if_none_match == Some(etag.as_str()) {
                    self.not_modified.fetch_add(1, Ordering::Relaxed);
                    Err(link_r::Error::not_modified(url))
                } else {
                    self.full.fetch_add(1, Ordering::Relaxed);
                    Ok(Fetched {
                        meta: FetchMeta {
                            kind: ResourceKind::Html,
                            etag: Some(etag.into()),
                            status: 200,
                            final_url: None,
                        },
                        payload: DocPayload::Owned(Bytes::from(html.clone())),
                    })
                }
            }
        };
        std::future::ready(out)
    }
}
