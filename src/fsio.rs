// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
//! File primitives shared by the store, the mirror and dependants: atomic
//! replacement, the cross-process writer lock, and the millisecond clock.
//! Safe std only — the lock is `File::try_lock` (`flock` on unix, `LockFileEx` on
//! Windows), which every platform releases when the process dies.

use std::fs::{self, File, OpenOptions};
use std::io::Write as _;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

/// Write the concatenation of `parts` to `path` atomically: a unique sibling
/// temporary, fsync, rename, then a best-effort directory fsync so the rename
/// itself is durable. The parts are written in turn, never joined in memory.
pub fn write_atomic(path: &Path, parts: &[&[u8]]) -> std::io::Result<()> {
    write_atomic_with(path, parts, |_| Ok(()))
}

/// [`write_atomic`], running `finish` on the written temporary before it is
/// synced and renamed (to stamp metadata such as the modification time).
pub fn write_atomic_with(
    path: &Path,
    parts: &[&[u8]],
    finish: impl FnOnce(&File) -> std::io::Result<()>,
) -> std::io::Result<()> {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let dir = path.parent().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "path has no parent")
    })?;
    let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("file");
    // pid + a process-wide counter: two writers in one process never share a
    // temporary, and `create_new` refuses a stale leftover.
    let tmp = dir.join(format!(
        ".{name}.{}.{}.tmp",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let write = || -> std::io::Result<()> {
        let mut f = OpenOptions::new().write(true).create_new(true).open(&tmp)?;
        for p in parts {
            f.write_all(p)?;
        }
        finish(&f)?;
        f.sync_all()?;
        fs::rename(&tmp, path)
    };
    if let Err(e) = write() {
        let _ = fs::remove_file(&tmp);
        return Err(e);
    }
    sync_dir(dir);
    Ok(())
}

/// Best-effort fsync of a directory, so renames and removals in it are
/// durable (a no-op where directories cannot be opened).
pub fn sync_dir(dir: &Path) {
    if let Ok(d) = File::open(dir) {
        let _ = d.sync_all();
    }
}

/// An exclusive, non-blocking, cross-process lock on `path` (created if
/// absent), held until the returned file is dropped. `Ok(None)` when another
/// process holds it.
pub fn try_lock(path: &Path) -> std::io::Result<Option<File>> {
    let file = OpenOptions::new().create(true).truncate(false).write(true).open(path)?;
    match file.try_lock() {
        Ok(()) => Ok(Some(file)),
        Err(fs::TryLockError::WouldBlock) => Ok(None),
        Err(fs::TryLockError::Error(e)) => Err(e),
    }
}

/// An exclusive cross-process lock on `path` (created if absent), waiting
/// for any holder; held until the returned file is dropped.
pub fn lock(path: &Path) -> std::io::Result<File> {
    let file = OpenOptions::new().create(true).truncate(false).write(true).open(path)?;
    file.lock()?;
    Ok(file)
}

/// Milliseconds since the Unix epoch (0 if the clock precedes it) — the
/// timestamp convention every graph-r API uses.
#[must_use]
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn atomic_writes_replace_whole_and_locks_exclude() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("f");
        write_atomic(&p, &[b"ab", b"cd"]).unwrap();
        write_atomic(&p, &[b"xyz"]).unwrap();
        assert_eq!(fs::read(&p).unwrap(), b"xyz");
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1, "no temporaries left");
        let l = dir.path().join("lock");
        let held = try_lock(&l).unwrap();
        assert!(held.is_some());
        assert!(try_lock(&l).unwrap().is_none(), "second holder refused");
        drop(held);
        assert!(try_lock(&l).unwrap().is_some(), "released on drop");
        let held = lock(&l).unwrap();
        assert!(try_lock(&l).unwrap().is_none(), "the blocking lock excludes too");
        drop(held);
        let stamped = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_000_000);
        write_atomic_with(&p, &[b"m"], |f| f.set_modified(stamped)).unwrap();
        assert_eq!(fs::metadata(&p).unwrap().modified().unwrap(), stamped);
        assert!(now_ms() > 1_700_000_000_000);
    }
}
