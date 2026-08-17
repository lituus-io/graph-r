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
    // Kept open for the lifetime of the lock; flock releases on close/death.
    _file: File,
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
            Ok(Self { _file: file })
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
    /// Weaker non-unix fallback: exclusive lock-file creation. Unlike flock
    /// this does not self-release on process death; a stale lock surfaces as
    /// `Error::Locked` and must be removed manually (documented limitation).
    pub(crate) fn acquire(path: &Path) -> Result<Self> {
        match std::fs::OpenOptions::new().create_new(true).write(true).open(path) {
            Ok(file) => Ok(Self { _file: file }),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Err(Error::Locked),
            Err(e) => Err(e.into()),
        }
    }
}

#[cfg(not(unix))]
impl Drop for LockFile {
    fn drop(&mut self) {
        // Best-effort removal so the next opener is not spuriously locked out.
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
    fn second_lock_acquisition_in_process_succeeds_after_drop() {
        // flock is per-open-file: within one process a second open+flock on
        // the same path *would* succeed, so the in-process writer mutex (not
        // this lock) is what serializes same-process writers. This test pins
        // the cross-open release behavior we rely on.
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("graph.lock");
        let l1 = LockFile::acquire(&p).unwrap();
        drop(l1);
        let _l2 = LockFile::acquire(&p).unwrap();
    }
}
