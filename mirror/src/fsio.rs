// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>
//
//! Local file plumbing: the store directory's writer lock, atomic multi-part
//! writes, durable appends.

use crate::{Error, Result};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

/// Take the store directory's writer lock (`graph.lock`) for as long as the
/// returned handle lives. std's lock is `flock` on unix — the very lock
/// graph-r takes — so a writable `Store` and a mirror can never both write
/// one directory. Never blocks.
pub(crate) fn lock_dir(dir: &Path) -> Result<File> {
    fs::create_dir_all(dir)?;
    let f = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(dir.join(graph_r::store::LOCK_FILE))?;
    match f.try_lock() {
        Ok(()) => Ok(f),
        Err(fs::TryLockError::WouldBlock) => Err(Error::Graph(graph_r::Error::Locked)),
        Err(fs::TryLockError::Error(e)) => Err(e.into()),
    }
}

/// Write the concatenation of `parts` to `path` atomically: unique sibling
/// temporary, fsync, rename, directory fsync. The parts are written in turn,
/// never joined in memory.
pub(crate) fn write_atomic(path: &Path, parts: &[&[u8]]) -> Result<()> {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let dir =
        path.parent().ok_or_else(|| Error::Corrupt { message: "path has no parent".into() })?;
    let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("file");
    let tmp = dir.join(format!(
        ".{name}.mirror.{}.{}.tmp",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let write = || -> std::io::Result<()> {
        let mut f = OpenOptions::new().write(true).create_new(true).open(&tmp)?;
        for p in parts {
            f.write_all(p)?;
        }
        f.sync_all()?;
        fs::rename(&tmp, path)?;
        sync_dir(dir);
        Ok(())
    };
    write().map_err(|e| {
        let _ = fs::remove_file(&tmp);
        e.into()
    })
}

/// Append `parts` to an existing file and fsync.
pub(crate) fn append(path: &Path, parts: &[&[u8]]) -> Result<()> {
    let mut f = OpenOptions::new().append(true).open(path)?;
    for p in parts {
        f.write_all(p)?;
    }
    f.sync_all()?;
    Ok(())
}

fn sync_dir(dir: &Path) {
    if let Ok(d) = File::open(dir) {
        let _ = d.sync_all();
    }
}
