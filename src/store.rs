// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>
//
//! The store: a directory holding `graph.base` (immutable snapshot),
//! `graph.wal` (append-only op log) and `graph.lock` (writer exclusion).
//!
//! # Concurrency model
//!
//! - Any number of snapshots; at most one writer per directory. Two layers
//!   enforce that, and they cover different scopes. The `flock` taken at
//!   [`Store::open`] admits only one read-write *handle* per directory —
//!   `flock` is per open file description, so a second handle is refused with
//!   [`Error::Locked`] whether it is opened by another process **or by this
//!   one**. The mutex inside a handle then serializes [`Writer`]s across
//!   threads sharing it. [`Store::open_read_only`] takes no lock, so readers
//!   always coexist with a live writer.
//! - A fixed ring of generation slots each holds an mmapped base plus its
//!   committed-op overlay behind an `RwLock`. Readers only ever `try_read` —
//!   the sole moment a `try_read` can fail is the microseconds in which a
//!   writer installs a *different* generation into a free slot, so readers
//!   spin-retry instead of blocking. Snapshots are `RwLockReadGuard`-backed
//!   borrow guards: a pinned generation cannot be recycled under them, and
//!   nothing is reference-counted.
//! - Compaction writes a new base + fresh WAL, installs it into a free slot,
//!   and swaps the current index; existing snapshots keep reading the old
//!   generation untouched until they drop.

use crate::compact;
use crate::error::{Error, Result};
use crate::format::{base, wal};
use crate::os::{LockFile, MappedFile};
use crate::overlay::Arena;
use crate::snapshot::Snapshot;
use crate::ttl::TtlConfig;
use crate::writer::{WriteState, Writer};
use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, RwLock};

/// Number of generation ring slots. Three retired generations may stay pinned
/// by long-lived snapshots while a fourth is installed.
const GEN_SLOTS: usize = 4;

/// When `commit` must fsync the WAL.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Durability {
    /// fsync on every commit (default).
    Always,
    /// fsync every `n` commits; a crash may lose up to the uncommitted tail.
    Batch(u32),
    /// Never fsync explicitly; durability follows the OS page cache.
    Never,
}

/// Store configuration.
#[derive(Clone, Copy, Debug)]
pub struct Config {
    /// WAL fsync policy.
    pub durability: Durability,
    /// Fold the WAL into a new base after this many committed ops.
    pub compact_after_ops: usize,
    /// … or after the WAL grows past this many bytes, whichever first.
    pub compact_after_wal_bytes: u64,
    /// Adaptive-freshness policy applied by [`Writer::touch`].
    pub ttl: TtlConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            durability: Durability::Always,
            compact_after_ops: 4096,
            compact_after_wal_bytes: 4 << 20,
            ttl: TtlConfig::default(),
        }
    }
}

/// One loaded generation: the mmapped base and its overlay arena.
#[derive(Debug)]
pub(crate) struct GenInner {
    map: MappedFile,
    pub(crate) dir: base::BaseDir,
    pub(crate) arena: Arena,
}

impl GenInner {
    pub(crate) fn bytes(&self) -> &[u8] {
        self.map.bytes()
    }

    pub(crate) fn from_parts(map: MappedFile, dir: base::BaseDir) -> Self {
        Self { map, dir, arena: Arena::new() }
    }
}

#[derive(Debug)]
pub(crate) struct GenSlot {
    /// Generation number for staleness checks; 0 = never populated.
    pub(crate) seq: AtomicU64,
    pub(crate) inner: RwLock<Option<GenInner>>,
}

/// An embedded, persistent knowledge-graph store.
#[derive(Debug)]
pub struct Store {
    pub(crate) dir: PathBuf,
    pub(crate) cfg: Config,
    read_only: bool,
    _lock: Option<LockFile>,
    pub(crate) current: AtomicUsize,
    pub(crate) gens: [GenSlot; GEN_SLOTS],
    pub(crate) write: Mutex<WriteState>,
}

/// Paths inside a store directory.
pub(crate) fn base_path(dir: &Path) -> PathBuf {
    dir.join("graph.base")
}
pub(crate) fn wal_path(dir: &Path) -> PathBuf {
    dir.join("graph.wal")
}
fn lock_path(dir: &Path) -> PathBuf {
    dir.join("graph.lock")
}

/// Write `bytes` to `path` atomically (sibling temp + fsync + rename), then
/// best-effort fsync the directory so the rename itself is durable.
pub(crate) fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let dir = path.parent().ok_or_else(|| Error::format("path has no parent"))?;
    let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("file");
    let tmp = dir.join(format!(".{name}.{}.tmp", std::process::id()));
    let write = || -> std::io::Result<()> {
        let mut f = fs::File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
        Ok(())
    };
    if let Err(e) = write() {
        let _ = fs::remove_file(&tmp);
        return Err(e.into());
    }
    if let Err(e) = fs::rename(&tmp, path) {
        let _ = fs::remove_file(&tmp);
        return Err(e.into());
    }
    if let Ok(d) = fs::File::open(dir) {
        let _ = d.sync_all();
    }
    Ok(())
}

/// Milliseconds since the Unix epoch (0 if the clock precedes it) — the
/// timestamp convention every graph-r API uses.
#[must_use]
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

impl Store {
    /// Create a fresh store in `dir` (created if absent; must not already
    /// contain a store) and open it read-write.
    pub fn create(dir: impl AsRef<Path>, cfg: Config) -> Result<Self> {
        let dir = dir.as_ref();
        fs::create_dir_all(dir)?;
        if base_path(dir).exists() {
            return Err(Error::format("store already exists; use open"));
        }
        let empty = compact::render_empty_base(1);
        atomic_write(&base_path(dir), &empty)?;
        atomic_write(&wal_path(dir), &wal::encode_header(1, now_ms()))?;
        Self::open_with(dir, cfg, false)
    }

    /// Open an existing store read-write. Takes the exclusive writer lock
    /// (`Error::Locked` if another process holds it) and repairs a torn WAL
    /// tail by truncating to the last intact frame.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        Self::open_with(dir.as_ref(), Config::default(), false)
    }

    /// Open with an explicit configuration.
    pub fn open_cfg(dir: impl AsRef<Path>, cfg: Config) -> Result<Self> {
        Self::open_with(dir.as_ref(), cfg, false)
    }

    /// Open read-only: no lock taken, no repair performed; replay simply
    /// stops at a torn tail. Use [`Store::reload_if_stale`] to observe
    /// another process's commits and compactions.
    pub fn open_read_only(dir: impl AsRef<Path>) -> Result<Self> {
        Self::open_with(dir.as_ref(), Config::default(), true)
    }

    fn open_with(dir: &Path, cfg: Config, read_only: bool) -> Result<Self> {
        let lock = if read_only { None } else { Some(LockFile::acquire(&lock_path(dir))?) };
        let (inner, next_seq, wal_ops, wal_bytes) = load_generation(dir, read_only)?;
        let generation = inner.dir.header.generation;
        let store = Self {
            dir: dir.to_path_buf(),
            cfg,
            read_only,
            _lock: lock,
            current: AtomicUsize::new(0),
            gens: std::array::from_fn(|_| GenSlot {
                seq: AtomicU64::new(0),
                inner: RwLock::new(None),
            }),
            write: Mutex::new(WriteState::new(next_seq, wal_ops.len(), wal_bytes)),
        };
        *store.gens[0].inner.write().expect("fresh lock") = Some(inner);
        store.gens[0].seq.store(generation, Ordering::Release);
        Ok(store)
    }

    /// Number of live (non-tombstoned, non-stub) documents in the current
    /// snapshot view.
    #[must_use]
    pub fn len(&self) -> usize {
        self.snapshot().len()
    }

    /// True when the store holds no live documents.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Pin the current generation and return a read view. Never blocks in
    /// steady state; spins only across a writer's slot-install window.
    #[must_use]
    pub fn snapshot(&self) -> Snapshot<'_> {
        loop {
            let cur = self.current.load(Ordering::Acquire);
            if let Ok(guard) = self.gens[cur].inner.try_read() {
                if guard.is_some() && self.current.load(Ordering::Acquire) == cur {
                    return Snapshot::new(self, guard);
                }
            }
            std::hint::spin_loop();
        }
    }

    /// Acquire the single writer. `Error::ReadOnly` on read-only stores.
    ///
    /// Blocks while another thread holds a [`Writer`] on *this* handle. A second
    /// handle on the same directory cannot exist at all — [`Store::open`] would
    /// have been refused with [`Error::Locked`] — so this mutex is the only
    /// contention a caller can encounter.
    pub fn writer(&self) -> Result<Writer<'_>> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }
        let state = self.write.lock().map_err(|_| Error::corrupt("writer mutex poisoned"))?;
        Ok(Writer::new(self, state))
    }

    /// Fold the WAL into a new base generation now. Also triggered
    /// automatically by [`Writer::commit`] past the configured thresholds.
    pub fn compact(&self) -> Result<compact::CompactStats> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }
        let mut state = self.write.lock().map_err(|_| Error::corrupt("writer mutex poisoned"))?;
        compact::compact_locked(self, &mut state)
    }

    /// For read-only stores: pick up commits/compactions made by the writing
    /// process. Returns `true` when anything new became visible.
    pub fn reload_if_stale(&self) -> Result<bool> {
        let cur = self.current.load(Ordering::Acquire);
        let cur_gen = self.gens[cur].seq.load(Ordering::Acquire);
        let (inner, _, wal_ops, _) = load_generation(&self.dir, true)?;
        let new_gen = inner.dir.header.generation;
        if new_gen == cur_gen {
            // Same generation: publish any WAL frames we have not seen yet.
            let guard = self.gens[cur].inner.try_read().map_err(|_| Error::busy("slot busy"))?;
            let Some(g) = guard.as_ref() else { return Ok(false) };
            let have = g.arena.len();
            if wal_ops.len() <= have {
                return Ok(false);
            }
            for (seq, op) in wal_ops.into_iter().skip(have) {
                if !g.arena.publish(seq, op) {
                    return Err(Error::busy("overlay arena full"));
                }
            }
            return Ok(true);
        }
        // New generation: install into a free slot and swap.
        for (i, slot) in self.gens.iter().enumerate() {
            if i == cur {
                continue;
            }
            if let Ok(mut w) = slot.inner.try_write() {
                let fresh = GenInner { map: inner.map, dir: inner.dir, arena: Arena::new() };
                for (seq, op) in wal_ops {
                    let _ = fresh.arena.publish(seq, op);
                }
                *w = Some(fresh);
                drop(w);
                slot.seq.store(new_gen, Ordering::Release);
                self.current.store(i, Ordering::Release);
                return Ok(true);
            }
        }
        Err(Error::busy("all generation slots pinned by snapshots"))
    }
}

/// Load `graph.base` + replay `graph.wal` for `dir`. Returns the generation,
/// the next WAL sequence, the replayed ops (already filtered to those newer
/// than the base), and the WAL byte length.
#[allow(clippy::type_complexity)]
fn load_generation(
    dir: &Path,
    read_only: bool,
) -> Result<(GenInner, u64, Vec<(u64, wal::Op)>, u64)> {
    let map = MappedFile::open(&base_path(dir))?;
    let bdir = base::BaseDir::parse(map.bytes())?;
    let header = bdir.header;

    let wal_file = wal_path(dir);
    let raw = match fs::read(&wal_file) {
        Ok(raw) => raw,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound && read_only => Vec::new(),
        Err(e) => return Err(e.into()),
    };
    let (ops, good_len, next_seq) = if raw.is_empty() {
        (Vec::new(), 0, header.wal_applied_seq + 1)
    } else {
        let replayed = wal::replay(&raw)?;
        if replayed.base_generation > header.generation {
            return Err(Error::corrupt("wal is newer than base"));
        }
        if replayed.base_generation < header.generation {
            // Stale log from before the last compaction; every op is already
            // folded. A writable opener resets it.
            if !read_only {
                atomic_write(&wal_file, &wal::encode_header(header.generation, now_ms()))?;
            }
            (Vec::new(), wal::HEADER_LEN, header.wal_applied_seq + 1)
        } else {
            if !read_only && replayed.good_len < raw.len() {
                // Torn tail: truncate back to the last intact frame.
                let f = fs::OpenOptions::new().write(true).open(&wal_file)?;
                f.set_len(replayed.good_len as u64)?;
                f.sync_all()?;
            }
            let next = replayed
                .ops
                .last()
                .map_or(header.wal_applied_seq + 1, |&(s, _)| s.max(header.wal_applied_seq) + 1);
            let ops: Vec<(u64, wal::Op)> =
                replayed.ops.into_iter().filter(|&(s, _)| s > header.wal_applied_seq).collect();
            (ops, replayed.good_len, next)
        }
    };

    let inner = GenInner { map, dir: bdir, arena: Arena::new() };
    for (seq, op) in &ops {
        if !inner.arena.publish(*seq, op.clone()) {
            return Err(Error::busy("overlay arena full during replay"));
        }
    }
    Ok((inner, next_seq, ops, good_len as u64))
}
