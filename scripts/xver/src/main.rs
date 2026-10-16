// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! Every store this checkout writes must be refused by the released 0.5.1
//! with `Error::Version { found: 3, supported: 2 }`, by every opener, before
//! anything is written: every file byte-identical afterwards. The case that
//! matters most is a store whose log is empty or missing, which 0.5.1 would
//! otherwise take for its own and rewrite (it reads the base first, so only
//! the base's version can refuse it there). Exits 1 on any failure.

use std::path::{Path, PathBuf};
use std::process::ExitCode;

/// Every file in `dir` with its bytes.
fn files(dir: &Path) -> Vec<(String, Vec<u8>)> {
    let mut v: Vec<(String, Vec<u8>)> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| {
            let e = e.unwrap();
            (e.file_name().to_string_lossy().into_owned(), std::fs::read(e.path()).unwrap())
        })
        .collect();
    v.sort();
    v
}

fn doc(url: &str) -> new::DocRecord<'_> {
    new::DocRecord {
        url,
        url_key: new::UrlKey::of(url),
        content_hash: 1,
        fetched_at_ms: 1,
        title: Some("t"),
        snippet: None,
        etag: None,
        pinned: false,
    }
}

/// A 0.6 store in `dir`, shaped by `case`.
fn write(dir: &Path, case: &str) {
    let store = new::Store::create(dir, new::Config::default()).unwrap();
    let commit = |urls: &[&str]| {
        store.transact(|tx| urls.iter().try_for_each(|u| tx.upsert_node(&doc(u)))).unwrap();
    };
    match case {
        "fresh" => {}
        "records" => {
            commit(&["https://x.dev/a", "https://x.dev/b", "https://x.dev/c"]);
            commit(&["https://x.dev/d"]);
        }
        "compacted" => {
            commit(&["https://x.dev/a", "https://x.dev/b"]);
            store.compact().unwrap();
        }
        "compacted+records" => {
            commit(&["https://x.dev/a"]);
            store.compact().unwrap();
            commit(&["https://x.dev/b", "https://x.dev/c"]);
        }
        other => unreachable!("{other}"),
    }
}

/// What 0.5.1 made of `dir` through `open`, or why it is wrong.
fn check(dir: &Path, opener: &str) -> Result<(), String> {
    let before = files(dir);
    let opened = match opener {
        "open" => old::Store::open(dir),
        "open_cfg" => old::Store::open_cfg(dir, old::Config::default()),
        "open_or_create" => old::Store::open_or_create(dir, old::Config::default()),
        "open_read_only" => old::Store::open_read_only(dir),
        other => unreachable!("{other}"),
    };
    let outcome = match opened {
        Err(old::Error::Version { found: 3, supported: 2 }) => Ok(()),
        Err(e) => Err(format!("refused, but not as Version {{ found: 3, supported: 2 }}: {e:?}")),
        Ok(_) => Err("opened it".to_owned()),
    };
    let after = files(dir);
    if after != before {
        let names = |v: &[(String, Vec<u8>)]| v.iter().map(|f| f.0.clone()).collect::<Vec<_>>();
        let changed: Vec<&String> =
            after.iter().filter(|f| !before.contains(f)).map(|f| &f.0).collect();
        return Err(format!(
            "{}; files changed: {changed:?} (before {:?})",
            outcome.err().unwrap_or_else(|| "refused".to_owned()),
            names(&before)
        ));
    }
    outcome
}

fn main() -> ExitCode {
    let root: PathBuf = std::env::temp_dir().join(format!("graph-r-xver-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let cases = ["fresh", "records", "compacted", "compacted+records"];
    let logs = ["as written", "empty", "missing"];
    let openers = ["open", "open_cfg", "open_or_create", "open_read_only"];
    let mut failed = 0;
    println!("uu-graph-r 0.5.1 opening stores written by this checkout:");
    for case in cases {
        for log in logs {
            for opener in openers {
                let dir = root.join(format!("{case}-{log}-{opener}").replace([' ', '+'], "_"));
                write(&dir, case);
                let wal = dir.join(new::store::WAL_FILE);
                match log {
                    "empty" => std::fs::write(&wal, b"").unwrap(),
                    "missing" => std::fs::remove_file(&wal).unwrap(),
                    _ => {}
                }
                let r = check(&dir, opener);
                println!(
                    "  {:<4} {case:<18} log {log:<10} {opener:<15} {}",
                    if r.is_ok() { "ok" } else { "FAIL" },
                    r.as_ref().err().map_or("Version { found: 3, supported: 2 }, untouched", |e| e)
                );
                failed += usize::from(r.is_err());
            }
        }
    }
    let _ = std::fs::remove_dir_all(&root);
    let total = cases.len() * logs.len() * openers.len();
    println!("{} of {total} refused untouched", total - failed);
    if failed == 0 {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
