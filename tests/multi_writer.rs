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
/// Ops in the transaction the "crash" child dies writing.
const CRASH_OPS: usize = 5;
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

/// Every op's sequence number in a log image, in log order.
fn seqs(raw: &[u8]) -> Vec<u64> {
    let replayed = graph_r::format::wal::replay(raw).unwrap();
    replayed.records().flat_map(|r| r.first_seq()..=r.last_seq()).collect()
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
        // M transactions of K fresh documents each.
        "ktx" => {
            let (dir, id): (PathBuf, usize) = (PathBuf::from(&args[0]), args[1].parse().unwrap());
            let (m, k): (usize, usize) = (args[2].parse().unwrap(), args[3].parse().unwrap());
            let cfg = Config { compact_after_ops: usize::MAX, ..Config::default() };
            let store = Store::open_cfg(&dir, cfg).unwrap();
            ready_then_go(&dir.join("go"), id);
            for t in 0..m {
                let urls: Vec<String> = (0..k).map(|i| url_of(id, t * k + i)).collect();
                store.transact(|tx| urls.iter().try_for_each(|u| tx.upsert_node(&doc(u)))).unwrap();
            }
        }
        // Die mid-append: write a strict prefix of the record this
        // transaction would have been, then exit without unwinding.
        "crash" => {
            let dir = PathBuf::from(&args[0]);
            let cut: usize = args[1].parse().unwrap();
            let raw = std::fs::read(dir.join(graph_r::store::WAL_FILE)).unwrap();
            let next = seqs(&raw).last().map_or(1, |s| s + 1);
            let mut rec = graph_r::format::wal::RecordBuf::new();
            for i in 0..CRASH_OPS {
                rec.push_upsert(&doc(&url_of(9, i)), 0).unwrap();
            }
            rec.seal(next).unwrap();
            let rec = rec.into_vec();
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(dir.join(graph_r::store::WAL_FILE))
                .unwrap();
            std::io::Write::write_all(&mut f, &rec[..cut.min(rec.len() - 1)]).unwrap();
            f.sync_all().unwrap();
            std::process::exit(0);
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

/// N processes × M commits, released together: exactly N·M ops with
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
        assert_eq!(seqs(&raw), (1..=(n * m) as u64).collect::<Vec<_>>(), "N·M ops, in sequence");
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
    assert_eq!(seqs(&raw), (1..=200).collect::<Vec<_>>());
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

/// N processes commit K-op transactions while a reader in this process
/// catches up on one thread and takes snapshots on another: no snapshot ever
/// holds part of a transaction, so the live count is always a multiple of
/// K. (0.5.1 fails this: a catch-up published another process's ops one at
/// a time, and could read part of an append in progress.)
#[test]
fn a_reader_never_sees_part_of_another_process_transaction() {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    const N: usize = 3;
    const M: usize = 40;
    const K: usize = 7;
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("s");
    drop(Store::create(&dir, Config::default()).unwrap());
    let reader = Store::open_read_only(&dir).unwrap();
    let d = dir.display().to_string();
    let children: Vec<Child> = (0..N)
        .map(|id| spawn("ktx", &[d.clone(), id.to_string(), M.to_string(), K.to_string()]))
        .collect();
    let done = AtomicBool::new(false);
    let (polls, seen) = (AtomicU64::new(0), AtomicU64::new(0));
    std::thread::scope(|s| {
        s.spawn(|| {
            while !done.load(Ordering::Acquire) {
                reader.reload_if_stale().unwrap();
            }
            reader.reload_if_stale().unwrap();
        });
        s.spawn(|| loop {
            let finished = done.load(Ordering::Acquire);
            let len = reader.snapshot().len();
            assert_eq!(len % K, 0, "a snapshot holds {len} documents: part of a transaction");
            polls.fetch_add(1, Ordering::Relaxed);
            seen.fetch_max(len as u64, Ordering::Relaxed);
            if finished {
                break;
            }
        });
        go_when_ready(&dir.join("go"), N);
        wait_all(children);
        done.store(true, Ordering::Release);
    });
    assert!(seen.load(Ordering::Relaxed) > 0, "the reader saw commits land");
    assert!(polls.load(Ordering::Relaxed) > 0);
    assert_eq!(reader.len(), N * M * K);
}

/// A writer killed mid-append leaves a strict prefix of its transaction's
/// record. Whatever the cut, the next committer removes it: that
/// transaction is absent whole, every earlier one present, and the log
/// holds only whole records.
#[test]
fn a_transaction_cut_by_a_crash_is_absent_whole() {
    let root = tempfile::tempdir().unwrap();
    for cut in [1, 31, 32, 100, 250, usize::MAX] {
        let dir = root.path().join(format!("s{cut}"));
        let store = Store::create(&dir, Config::default()).unwrap();
        for t in 0..3 {
            store
                .transact(|tx| (0..4).try_for_each(|i| tx.upsert_node(&doc(&url_of(t, i)))))
                .unwrap();
        }
        drop(store);
        wait_all(vec![spawn("crash", &[dir.display().to_string(), cut.to_string()])]);
        let store = Store::open(&dir).unwrap();
        store.transact(|tx| tx.upsert_node(&doc(&url_of(7, 0)))).unwrap();
        let mut want: Vec<String> =
            (0..3).flat_map(|t| (0..4).map(move |i| url_of(t, i))).collect();
        want.push(url_of(7, 0));
        want.sort();
        assert_eq!(live_urls(&store), want, "cut at {cut}: not the earlier transactions alone");
        let raw = std::fs::read(dir.join(graph_r::store::WAL_FILE)).unwrap();
        assert_eq!(
            graph_r::format::wal::committed_len(&raw),
            raw.len(),
            "cut at {cut}: torn bytes left"
        );
        assert_eq!(seqs(&raw), (1..=13).collect::<Vec<_>>(), "cut at {cut}");
        drop(store);
        assert_eq!(live_urls(&Store::open_read_only(&dir).unwrap()), want);
    }
}
