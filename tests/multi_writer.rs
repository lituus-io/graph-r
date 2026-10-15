// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! Several writers on one store directory, as real processes.
//!
//! The parent test re-executes this very test binary with `--exact child`
//! and a role in the environment; [`child`] is a no-op unless that role is
//! set, so a normal `cargo test` run passes it trivially. Processes start
//! together on a "go" file, so their opens and commits genuinely race.

#![cfg(feature = "store")]
use graph_r::prelude::*;
use graph_r::Error;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

const ROLE: &str = "GRAPH_R_MW_ROLE";
const ARGS: &str = "GRAPH_R_MW_ARGS";

fn url_of(writer: usize, n: usize) -> String {
    format!("https://mw.dev/w{writer}/d{n}")
}

fn doc(url: &str) -> DocRecord<'_> {
    DocRecord {
        url,
        url_key: UrlKey::of(url),
        content_hash: 7,
        fetched_at_ms: 1,
        title: None,
        snippet: None,
        etag: None,
        pinned: false,
    }
}

fn spawn(role: &str, args: &[String]) -> Child {
    Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "child", "--nocapture", "--test-threads=1"])
        .env(ROLE, role)
        .env(ARGS, args.join("\n"))
        .stdout(std::process::Stdio::null())
        .spawn()
        .unwrap()
}

fn wait_all(children: Vec<Child>) {
    for mut c in children {
        let status = c.wait().unwrap();
        assert!(status.success(), "a child process failed: {status}");
    }
}

/// Announce this child is up, then wait for the parent's go: process start-up
/// takes longer than a child's whole run, so without this the "race" would
/// run one child after another.
fn ready_then_go(go: &Path, id: usize) {
    std::fs::write(go.with_extension(format!("ready{id}")), b"").unwrap();
    wait_for(go);
}

/// Release `n` children waiting in [`ready_then_go`] at once.
fn go_when_ready(go: &Path, n: usize) {
    for id in 0..n {
        wait_for(&go.with_extension(format!("ready{id}")));
    }
    std::fs::write(go, b"").unwrap();
}

/// Poll until `go` exists (a short sleep, never a hot spin).
fn wait_for(go: &Path) {
    let start = Instant::now();
    while !go.exists() {
        assert!(start.elapsed() < Duration::from_secs(60), "never released");
        std::thread::sleep(Duration::from_micros(200));
    }
}

fn live_urls(store: &Store) -> Vec<String> {
    let mut v = Vec::new();
    store.snapshot().for_each_live(|n| v.push(n.url.to_owned()));
    v.sort();
    v
}

/// The child half: dispatches on the role the parent set.
#[test]
fn child() {
    let Ok(role) = std::env::var(ROLE) else { return };
    let args: Vec<String> = std::env::var(ARGS).unwrap().lines().map(str::to_owned).collect();
    match role.as_str() {
        // open_or_create a never-created store, then one commit.
        "create" => {
            let (dir, id, go) = (PathBuf::from(&args[0]), args[1].parse().unwrap(), &args[2]);
            ready_then_go(Path::new(go), id);
            let store = Store::open_or_create(&dir, Config::default()).unwrap();
            let url = url_of(id, 0);
            let mut w = store.writer().unwrap();
            w.upsert_node(&doc(&url)).unwrap();
            w.commit().unwrap();
        }
        // M commits of one document each, then converge and report.
        "commits" => {
            let dir = PathBuf::from(&args[0]);
            let id: usize = args[1].parse().unwrap();
            let (m, n): (usize, usize) = (args[2].parse().unwrap(), args[3].parse().unwrap());
            // Writer 0 compacts every this many commits (0: never); nobody
            // compacts inline, so a writer that lost a frame could never
            // fold it back in from its own memory.
            let compact_every: usize = args[4].parse().unwrap();
            let cfg = Config { compact_after_ops: usize::MAX, ..Config::default() };
            let store = Store::open_cfg(&dir, cfg).unwrap();
            ready_then_go(&dir.join("go"), id);
            for k in 0..m {
                let url = url_of(id, k);
                let mut w = store.writer().unwrap();
                w.upsert_node(&doc(&url)).unwrap();
                w.commit().unwrap();
                drop(w);
                // A little work between documents, outside the lock, as a
                // real producer has: it lets the other writers take turns.
                std::thread::sleep(Duration::from_millis(1));
                if id == 0 && compact_every > 0 && k % compact_every == compact_every - 1 {
                    store.compact().unwrap();
                }
            }
            std::fs::write(dir.join(format!("done.{id}")), b"").unwrap();
            for i in 0..n {
                wait_for(&dir.join(format!("done.{i}")));
            }
            store.reload_if_stale().unwrap();
            std::fs::write(dir.join(format!("out.{id}")), live_urls(&store).join("\n")).unwrap();
        }
        // Hold the commit lock until told to let go.
        "hold" => {
            let dir = PathBuf::from(&args[0]);
            let store = Store::open(&dir).unwrap();
            let held = store.commit_lock().unwrap();
            std::fs::write(dir.join("held"), b"").unwrap();
            wait_for(&dir.join("release"));
            drop(held);
        }
        // Commit back to back, no pause, until told to stop.
        "hot" => {
            let (dir, id): (PathBuf, usize) = (PathBuf::from(&args[0]), args[1].parse().unwrap());
            let store = Store::open(&dir).unwrap();
            ready_then_go(&dir.join("go"), id);
            let mut k = 0;
            while !dir.join("stop").exists() {
                let url = url_of(id, k % 64);
                store.transact(|tx| tx.upsert_node(&doc(&url))).unwrap();
                k += 1;
            }
            std::fs::write(dir.join(format!("hot.{id}")), k.to_string()).unwrap();
        }
        // A writer with a short patience committing now and then: report how
        // many commits went through and how many were refused as Locked.
        "polite" => {
            let (dir, id): (PathBuf, usize) = (PathBuf::from(&args[0]), args[1].parse().unwrap());
            let commits: usize = args[2].parse().unwrap();
            let cfg = Config { lock_wait: Duration::from_millis(300), ..Config::default() };
            let store = Store::open_cfg(&dir, cfg).unwrap();
            ready_then_go(&dir.join("go"), id);
            let (mut ok, mut locked) = (0, 0);
            for k in 0..commits {
                let url = url_of(id, k);
                match store.transact(|tx| tx.upsert_node(&doc(&url))) {
                    Ok(()) => ok += 1,
                    Err(Error::Locked) => locked += 1,
                    Err(e) => panic!("{e}"),
                }
                std::thread::sleep(Duration::from_millis(20));
            }
            std::fs::write(dir.join("polite"), format!("{ok} {locked}")).unwrap();
        }
        other => panic!("unknown role {other}"),
    }
}

/// Two writers commit back to back, with no pause between commits, while a
/// third commits now and then with a 300 ms patience. Each of the busy pair
/// frees the lock for microseconds between its fsyncs; without a fair queue
/// the third would almost never land in that gap and be refused. With it,
/// every one of its commits goes through.
#[test]
fn writers_committing_back_to_back_cannot_starve_another() {
    const COMMITS: usize = 40;
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("s");
    drop(Store::create(&dir, Config::default()).unwrap());
    let d = dir.display().to_string();
    let hot: Vec<Child> = (0..2).map(|id| spawn("hot", &[d.clone(), id.to_string()])).collect();
    let polite = spawn("polite", &[d.clone(), "2".into(), COMMITS.to_string()]);
    go_when_ready(&dir.join("go"), 3);
    wait_all(vec![polite]);
    std::fs::write(dir.join("stop"), b"").unwrap();
    wait_all(hot);
    let got = std::fs::read_to_string(dir.join("polite")).unwrap();
    assert_eq!(got, format!("{COMMITS} 0"), "commits through, refused as Locked");
    for id in 0..2 {
        let n: usize =
            std::fs::read_to_string(dir.join(format!("hot.{id}"))).unwrap().parse().unwrap();
        assert!(n > COMMITS, "busy writer {id} kept committing too ({n})");
    }
}

/// Eight processes open_or_create the same never-created store at the same
/// moment and commit one document each, fifty times over. Every process
/// succeeds and every document survives: no creator lays a fresh base or log
/// over a store another process already committed to.
#[test]
fn concurrent_first_creation_loses_nothing() {
    const N: usize = 8;
    const ROUNDS: usize = 50;
    let root = tempfile::tempdir().unwrap();
    for round in 0..ROUNDS {
        let dir = root.path().join(format!("s{round}"));
        let go = root.path().join(format!("go{round}"));
        let children: Vec<Child> = (0..N)
            .map(|id| {
                spawn(
                    "create",
                    &[dir.display().to_string(), id.to_string(), go.display().to_string()],
                )
            })
            .collect();
        go_when_ready(&go, N);
        wait_all(children);
        let store = Store::open_read_only(&dir).unwrap();
        let want: Vec<String> = {
            let mut v: Vec<String> = (0..N).map(|id| url_of(id, 0)).collect();
            v.sort();
            v
        };
        assert_eq!(live_urls(&store), want, "round {round}: a committed document was lost");
    }
}

/// N processes × M commits, released together: exactly N·M frames with
/// strictly increasing sequence numbers 1..=N·M, every document present, and
/// every handle converged to the same set.
fn n_by_m(n: usize, m: usize, compact_every: usize) {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("s");
    drop(Store::create(&dir, Config::default()).unwrap());
    let args = |id: usize| {
        [dir.display().to_string(), id.to_string(), m.to_string(), n.to_string()]
            .into_iter()
            .chain([compact_every.to_string()])
            .collect::<Vec<_>>()
    };
    let children: Vec<Child> = (0..n).map(|id| spawn("commits", &args(id))).collect();
    go_when_ready(&dir.join("go"), n);
    wait_all(children);
    let mut want: Vec<String> = (0..n).flat_map(|i| (0..m).map(move |k| url_of(i, k))).collect();
    want.sort();
    let store = Store::open_read_only(&dir).unwrap();
    assert_eq!(live_urls(&store), want, "every committed document is present");
    for id in 0..n {
        let out = std::fs::read_to_string(dir.join(format!("out.{id}"))).unwrap();
        assert_eq!(out, want.join("\n"), "handle {id} converged to the same set");
    }
    if compact_every == 0 {
        let raw = std::fs::read(dir.join(graph_r::store::WAL_FILE)).unwrap();
        let seqs: Vec<u64> =
            graph_r::format::wal::replay(&raw).unwrap().ops.iter().map(|o| o.0).collect();
        assert_eq!(seqs, (1..=(n * m) as u64).collect::<Vec<_>>(), "N·M frames, in sequence");
    }
}

#[test]
fn many_processes_commit_to_one_log() {
    n_by_m(4, 50, 0);
}

/// The same race with compactions landing in the middle of it.
#[test]
fn many_processes_commit_through_compactions() {
    n_by_m(4, 50, 5);
}

/// Two writable handles in one process, on two threads, both committing:
/// `flock` excludes them from each other exactly as it would two processes.
#[test]
fn two_handles_in_one_process_both_write() {
    let dir = tempfile::tempdir().unwrap();
    let a = Store::create(dir.path(), Config::default()).unwrap();
    let b = Store::open(dir.path()).unwrap();
    std::thread::scope(|s| {
        for (id, store) in [(0, &a), (1, &b)] {
            s.spawn(move || {
                for k in 0..100 {
                    let url = url_of(id, k);
                    store.transact(|tx| tx.upsert_node(&doc(&url))).unwrap();
                }
            });
        }
    });
    a.reload_if_stale().unwrap();
    b.reload_if_stale().unwrap();
    assert_eq!(live_urls(&a).len(), 200);
    assert_eq!(live_urls(&a), live_urls(&b));
    let raw = std::fs::read(dir.path().join(graph_r::store::WAL_FILE)).unwrap();
    let seqs: Vec<u64> =
        graph_r::format::wal::replay(&raw).unwrap().ops.iter().map(|o| o.0).collect();
    assert_eq!(seqs, (1..=200).collect::<Vec<_>>());
}

/// Readers never wait on a writer: while another process holds the commit
/// lock and a writer on this handle is parked waiting for it, a reader on
/// another thread of the same handle finishes a fixed amount of work —
/// snapshots, lookups, reloads — before the writer gets anywhere.
#[test]
fn readers_finish_while_a_writer_waits_for_another_process() {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("s");
    let store = Store::create(&dir, Config::default()).unwrap();
    store.transact(|tx| tx.upsert_node(&doc(&url_of(0, 0)))).unwrap();
    let holder = spawn("hold", &[dir.display().to_string()]);
    wait_for(&dir.join("held"));
    let committed = AtomicBool::new(false);
    let reads = AtomicU64::new(0);
    std::thread::scope(|s| {
        s.spawn(|| {
            store.transact(|tx| tx.upsert_node(&doc(&url_of(0, 1)))).unwrap();
            committed.store(true, Ordering::SeqCst);
        });
        // Give the writer time to park on the lock, then read on another
        // thread; this one only watches the clock.
        std::thread::sleep(Duration::from_millis(50));
        s.spawn(|| {
            for _ in 0..1_000 {
                let snap = store.snapshot();
                assert!(snap.node(UrlKey::of(&url_of(0, 0))).is_some());
                assert!(!store.reload_if_stale().unwrap());
                reads.fetch_add(1, Ordering::SeqCst);
            }
        });
        let start = Instant::now();
        while reads.load(Ordering::SeqCst) < 1_000 && start.elapsed() < Duration::from_secs(5) {
            std::thread::sleep(Duration::from_millis(1));
        }
        let (done, waiting) = (reads.load(Ordering::SeqCst), !committed.load(Ordering::SeqCst));
        std::fs::write(dir.join("release"), b"").unwrap(); // never leave the holder hanging
        assert_eq!(done, 1_000, "reads stalled behind a writer waiting for the lock");
        assert!(waiting, "the writer cannot have committed while the lock was held");
    });
    wait_all(vec![holder]);
    assert!(committed.load(Ordering::SeqCst));
    assert_eq!(store.len(), 2);
}
