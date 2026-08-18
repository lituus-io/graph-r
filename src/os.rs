// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>
//
//! The crate's single unsafe island: memory-mapping the immutable base file
//! and the cross-process writer lock. Nothing else in the crate may use
//! `unsafe` (`#![deny(unsafe_code)]` at the crate root; this module opts out
//! locally, mirroring link-r's `index::mmap` contract).
//!
//! # Safety envelope
//!
//! - The mmap is only ever taken over `graph.base`, which is replaced
//!   atomically via rename and never truncated in place by any cooperating
//!   process; the flock protocol makes an in-place truncation a foreign-actor
//!   scenario, in which case a SIGBUS is accepted (same posture as link-r).
//! - The lock is `flock(LOCK_EX | LOCK_NB)` on a dedicated zero-byte file:
//!   advisory, per-open-file, and released automatically by the kernel when
//!   the process dies — which is exactly why it beats a PID file.

#![allow(unsafe_code)]

use crate::error::{Error, Result};
use std::fs::File;
use std::path::Path;

/// A read-only memory-mapped file.
pub(crate) struct MappedFile {
    mmap: memmap2::Mmap,
}

impl std::fmt::Debug for MappedFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MappedFile").field("len", &self.mmap.len()).finish()
    }
}

impl MappedFile {
    /// Map `path` read-only.
    pub(crate) fn open(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        // SAFETY: the file is mapped read-only. Cooperating processes replace
        // it only via atomic rename (the old inode stays alive under this
        // map); hostile in-place truncation faults, which we accept.
        let mmap = unsafe { memmap2::Mmap::map(&file)? };
        Ok(Self { mmap })
    }

    /// The mapped bytes.
    pub(crate) fn bytes(&self) -> &[u8] {
        &self.mmap
    }
}

/// Holds the exclusive writer lock for a store directory until dropped.
#[derive(Debug)]
pub(crate) struct LockFile {
    /// Kept open for the lifetime of the lock. On unix nothing ever *reads*
    /// this — holding the descriptor open is itself the lock, and closing it on
    /// drop is what releases the flock — so the field is intentionally inert
    /// there. The non-unix fallback does read it, to close the handle before
    /// unlinking the file.
    #[cfg_attr(unix, allow(dead_code))]
    file: Option<File>,
    /// Only the non-unix fallback needs the path: there, lock ownership *is*
    /// the file's existence, so releasing means unlinking it.
    #[cfg(not(unix))]
    path: std::path::PathBuf,
}

#[cfg(unix)]
impl LockFile {
    /// Try to take the exclusive advisory lock; `Error::Locked` if another
    /// process holds it. Never blocks.
    pub(crate) fn acquire(path: &Path) -> Result<Self> {
        use std::os::unix::io::AsRawFd;
        let file =
            std::fs::OpenOptions::new().create(true).truncate(false).write(true).open(path)?;
        // SAFETY: plain FFI call on an owned, open fd; no memory is shared.
        let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if rc == 0 {
            Ok(Self { file: Some(file) })
        } else {
            let err = std::io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::EWOULDBLOCK) {
                Err(Error::Locked)
            } else {
                Err(err.into())
            }
        }
    }
}

#[cfg(not(unix))]
impl LockFile {
    /// Weaker non-unix fallback: exclusive lock-file creation. Unlike flock this
    /// does not self-release on process *death* — a lock left behind by a killed
    /// process surfaces as `Error::Locked` and must be removed by hand. It does
    /// release on a normal drop; see the `Drop` impl below.
    pub(crate) fn acquire(path: &Path) -> Result<Self> {
        match std::fs::OpenOptions::new().create_new(true).write(true).open(path) {
            Ok(file) => Ok(Self { file: Some(file), path: path.to_path_buf() }),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Err(Error::Locked),
            Err(e) => Err(e.into()),
        }
    }
}

#[cfg(not(unix))]
impl Drop for LockFile {
    /// Release the lock by unlinking the file.
    ///
    /// This body used to be empty under a comment promising exactly this, which
    /// made the lock permanent: because ownership here is the file's existence,
    /// a store directory could be opened once and then never again — every later
    /// `Store::open` saw the leftover file and returned `Error::Locked`.
    ///
    /// The handle is closed first. Windows refuses to unlink a file that still
    /// has an open handle without `FILE_SHARE_DELETE`, which `File::open` does
    /// not request, so dropping the handle has to happen before the unlink.
    fn drop(&mut self) {
        drop(self.file.take());
        let _ = std::fs::remove_file(&self.path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mmap_reads_file_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("f");
        std::fs::write(&p, b"abc123").unwrap();
        let m = MappedFile::open(&p).unwrap();
        assert_eq!(m.bytes(), b"abc123");
    }

    #[cfg(unix)]
    #[test]
    fn lock_is_released_on_drop() {
        // flock is per open file *description*, so dropping the handle closes
        // the fd and releases the lock. This pins the cross-open release
        // behavior the store relies on when a writer is dropped and reopened.
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("graph.lock");
        let l1 = LockFile::acquire(&p).unwrap();
        drop(l1);
        let _l2 = LockFile::acquire(&p).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn a_second_live_lock_is_refused_even_in_one_process() {
        // A separate `open()` creates a separate open file description, so
        // flock conflicts with it — including within a single process. An
        // earlier comment here claimed the opposite (that only the in-process
        // mutex serializes same-process writers); it was wrong, and
        // tests/security.rs pins the same guarantee at the Store level.
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("graph.lock");
        let _held = LockFile::acquire(&p).unwrap();
        assert!(
            matches!(LockFile::acquire(&p), Err(Error::Locked)),
            "a second live flock on the same path must be refused"
        );
    }
}
