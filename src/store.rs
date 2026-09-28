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
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
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
    /// Compact inside `commit` past the thresholds (the 0.2 behaviour).
    pub(crate) compact_inline: AtomicBool,
    /// Thresholds reached while inline compaction is off.
    pub(crate) due: AtomicBool,
    /// What the files looked like at the last load, for cheap staleness checks.
    stamp: Mutex<Option<Stamp>>,
}

/// The base file's identity plus the WAL length. Equal stamps mean nothing a
/// reader could observe has changed: the base is only ever replaced by rename
/// (new inode, or new creation time where there are no inodes) and the WAL
/// only grows within a generation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Stamp {
    base: FileId,
    wal_len: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct FileId {
    len: u64,
    modified: Option<std::time::SystemTime>,
    #[cfg(unix)]
    ino: (u64, u64),
    #[cfg(not(unix))]
    created: Option<std::time::SystemTime>,
}

impl FileId {
    fn of(md: &fs::Metadata) -> Self {
        #[cfg(unix)]
        use std::os::unix::fs::MetadataExt as _;
        Self {
            len: md.len(),
            modified: md.modified().ok(),
            #[cfg(unix)]
            ino: (md.dev(), md.ino()),
            #[cfg(not(unix))]
            created: md.created().ok(),
        }
    }
}

impl Stamp {
    /// Two `stat` calls; `None` when either file cannot be examined (the
    /// caller then falls back to a full load, which reports the real error).
    fn read(dir: &Path) -> Option<Self> {
        let base = FileId::of(&fs::metadata(base_path(dir)).ok()?);
        let wal_len = match fs::metadata(wal_path(dir)) {
            Ok(md) => md.len(),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => 0,
            Err(_) => return None,
        };
        Some(Self { base, wal_len })
    }
}

/// File name of the immutable base snapshot inside a store directory.
pub const BASE_FILE: &str = "graph.base";
/// File name of the append-only op log inside a store directory.
pub const WAL_FILE: &str = "graph.wal";
/// File name of the writer lock inside a store directory.
pub const LOCK_FILE: &str = "graph.lock";

/// Paths inside a store directory.
pub(crate) fn base_path(dir: &Path) -> PathBuf {
    dir.join(BASE_FILE)
}
pub(crate) fn wal_path(dir: &Path) -> PathBuf {
    dir.join(WAL_FILE)
}
fn lock_path(dir: &Path) -> PathBuf {
    dir.join(LOCK_FILE)
}

/// Write `bytes` to `path` atomically (sibling temp + fsync + rename), then
/// best-effort fsync the directory so the rename itself is durable.
pub(crate) fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    static TMP_SEQ: AtomicU64 = AtomicU64::new(0);
    let dir = path.parent().ok_or_else(|| Error::format("path has no parent"))?;
    let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("file");
    // pid + a process-wide counter: two stores (or threads) in one process
    // can never share a temporary, and `create_new` refuses a stale leftover.
    let seq = TMP_SEQ.fetch_add(1, Ordering::Relaxed);
    let tmp = dir.join(format!(".{name}.{}.{seq}.tmp", std::process::id()));
    let write = || -> std::io::Result<()> {
        let mut f = fs::OpenOptions::new().write(true).create_new(true).open(&tmp)?;
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
        let stamp = Stamp::read(dir);
        let built = load_generation(dir, read_only)?;
        Ok(Self::assemble(dir.to_path_buf(), cfg, read_only, lock, stamp, built))
    }

    /// Serve a store straight from memory: `base` is a `graph.base` image and
    /// `wal` a `graph.wal` image (possibly empty), e.g. objects fetched from a
    /// bucket. The base is read in place — no copy, no file, no mapping — and
    /// fully validated exactly like [`Store::open`]. The store is read-only
    /// and has no directory ([`Store::dir`] is empty); keep it current with
    /// [`Store::reload_image`].
    pub fn open_image(base: bytes::Bytes, wal: &[u8]) -> Result<Self> {
        let built = build_generation(MappedFile::from_bytes(base), wal)?;
        Ok(Self::assemble(PathBuf::new(), Config::default(), true, None, None, built))
    }

    /// Keep an image store current. Pass the new base when the generation
    /// changed (a compaction), else `None` and the current log image: frames
    /// not yet visible are published. Returns `true` when anything new
    /// became visible. Works on file-backed read-only stores too.
    pub fn reload_image(&self, base: Option<bytes::Bytes>, wal: &[u8]) -> Result<bool> {
        match base {
            Some(b) => self.install_or_apply(MappedFile::from_bytes(b), wal),
            None => match self.apply_wal_bytes(wal)? {
                Some(changed) => Ok(changed),
                None => {
                    Err(Error::format("log image belongs to another generation; pass its base"))
                }
            },
        }
    }

    /// True for a store served from memory ([`Store::open_image`]).
    #[must_use]
    pub fn is_image(&self) -> bool {
        self.dir.as_os_str().is_empty()
    }

    fn assemble(
        dir: PathBuf,
        cfg: Config,
        read_only: bool,
        lock: Option<LockFile>,
        stamp: Option<Stamp>,
        built: Built,
    ) -> Self {
        let generation = built.inner.dir.header.generation;
        let store = Self {
            dir,
            cfg,
            read_only,
            _lock: lock,
            current: AtomicUsize::new(0),
            gens: std::array::from_fn(|_| GenSlot {
                seq: AtomicU64::new(0),
                inner: RwLock::new(None),
            }),
            write: Mutex::new(WriteState::new(
                built.next_seq,
                built.replayed,
                built.good_len as u64,
            )),
            compact_inline: AtomicBool::new(true),
            due: AtomicBool::new(false),
            stamp: Mutex::new(stamp),
        };
        *store.gens[0].inner.write().expect("fresh lock") = Some(built.inner);
        store.gens[0].seq.store(generation, Ordering::Release);
        store
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

    /// The directory this store lives in.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// The generation currently served (advances by one per compaction, or
    /// when [`Store::reload_if_stale`] installs a newer one).
    #[must_use]
    pub fn generation(&self) -> u64 {
        self.gens[self.current.load(Ordering::Acquire)].seq.load(Ordering::Acquire)
    }

    /// Whether [`Writer::commit`] compacts inline once the configured
    /// thresholds are reached (the default, and the 0.2 behaviour). Services
    /// that must keep commit latency flat turn it off and call
    /// [`Store::compact`] off the hot path whenever [`Store::compaction_due`]
    /// says so.
    pub fn set_compact_inline(&self, inline: bool) {
        self.compact_inline.store(inline, Ordering::Relaxed);
    }

    /// True when a threshold was reached while inline compaction was off and
    /// no compaction has run since.
    #[must_use]
    pub fn compaction_due(&self) -> bool {
        self.due.load(Ordering::Acquire)
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
    ///
    /// Cheap when nothing changed: two `stat` calls, no mapping, no reads.
    /// When only the WAL grew, the base is not re-mapped.
    pub fn reload_if_stale(&self) -> Result<bool> {
        if self.is_image() {
            return Ok(false); // no files to watch; see reload_image
        }
        let now = Stamp::read(&self.dir);
        let mut last = self.stamp.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if let (Some(n), Some(l)) = (now, *last) {
            if n == l {
                return Ok(false);
            }
            if n.base == l.base {
                if let Some(changed) = self.reload_wal_only()? {
                    *last = now;
                    return Ok(changed);
                }
            }
        }
        let changed = self.reload_full()?;
        *last = now;
        Ok(changed)
    }

    /// Same base file: replay the WAL and publish frames not yet seen.
    /// `None` when the WAL belongs to another generation (full load needed).
    fn reload_wal_only(&self) -> Result<Option<bool>> {
        match fs::read(wal_path(&self.dir)) {
            Ok(raw) => self.apply_wal_bytes(&raw),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Some(false)),
            Err(e) => Err(e.into()),
        }
    }

    /// Publish the frames of a log image that the current generation has not
    /// seen. `None` when the image belongs to another generation.
    fn apply_wal_bytes(&self, raw: &[u8]) -> Result<Option<bool>> {
        if raw.is_empty() {
            return Ok(Some(false));
        }
        let cur = self.current.load(Ordering::Acquire);
        let guard = self.gens[cur].inner.try_read().map_err(|_| Error::busy("slot busy"))?;
        let Some(g) = guard.as_ref() else { return Ok(None) };
        let replayed = wal::replay(raw)?;
        if replayed.base_generation != g.dir.header.generation {
            return Ok(None);
        }
        let applied = g.dir.header.wal_applied_seq;
        let have = g.arena.len();
        let mut fresh =
            replayed.ops.into_iter().filter(|&(s, _)| s > applied).skip(have).peekable();
        if fresh.peek().is_none() {
            return Ok(Some(false));
        }
        for (seq, op) in fresh {
            if !g.arena.publish(seq, op) {
                return Err(Error::busy("overlay arena full"));
            }
        }
        Ok(Some(true))
    }

    fn reload_full(&self) -> Result<bool> {
        let map = MappedFile::open(&base_path(&self.dir))?;
        let raw = match fs::read(wal_path(&self.dir)) {
            Ok(raw) => raw,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(e.into()),
        };
        self.install_or_apply(map, &raw)
    }

    /// Same generation as served: publish new frames. Otherwise rebuild the
    /// generation from `map` + `raw` and install it into a free ring slot.
    fn install_or_apply(&self, map: MappedFile, raw: &[u8]) -> Result<bool> {
        let cur = self.current.load(Ordering::Acquire);
        let cur_gen = self.gens[cur].seq.load(Ordering::Acquire);
        let new_gen = base::Header::parse(map.bytes())?.generation;
        if new_gen == cur_gen {
            return Ok(self.apply_wal_bytes(raw)?.unwrap_or(false));
        }
        let built = build_generation(map, raw)?;
        for (i, slot) in self.gens.iter().enumerate() {
            if i == cur {
                continue;
            }
            if let Ok(mut w) = slot.inner.try_write() {
                *w = Some(built.inner);
                drop(w);
                slot.seq.store(new_gen, Ordering::Release);
                self.current.store(i, Ordering::Release);
                return Ok(true);
            }
        }
        Err(Error::busy("all generation slots pinned by snapshots"))
    }
}

/// A generation rebuilt from a base image and a log image, before any disk
/// side effects.
struct Built {
    inner: GenInner,
    next_seq: u64,
    replayed: usize,
    good_len: usize,
    log: LogState,
}

/// What the log image turned out to be.
#[derive(Clone, Copy, PartialEq, Eq)]
enum LogState {
    /// No log at all.
    Empty,
    /// A log from before the base's generation: every op is already folded.
    Stale,
    /// The base's own log; `torn` when bytes follow the last intact frame.
    Current { torn: bool },
}

/// Validate `map` as a base and replay `raw` against it. Pure: shared by the
/// file-backed and in-memory (image) paths.
fn build_generation(map: MappedFile, raw: &[u8]) -> Result<Built> {
    let bdir = base::BaseDir::parse(map.bytes())?;
    let header = bdir.header;
    let inner = GenInner { map, dir: bdir, arena: Arena::new() };
    if raw.is_empty() {
        return Ok(Built {
            inner,
            next_seq: header.wal_applied_seq + 1,
            replayed: 0,
            good_len: 0,
            log: LogState::Empty,
        });
    }
    let replayed = wal::replay(raw)?;
    if replayed.base_generation > header.generation {
        return Err(Error::corrupt("wal is newer than base"));
    }
    if replayed.base_generation < header.generation {
        return Ok(Built {
            inner,
            next_seq: header.wal_applied_seq + 1,
            replayed: 0,
            good_len: wal::HEADER_LEN,
            log: LogState::Stale,
        });
    }
    let torn = replayed.good_len < raw.len();
    let next_seq = replayed
        .ops
        .last()
        .map_or(header.wal_applied_seq + 1, |&(s, _)| s.max(header.wal_applied_seq) + 1);
    let mut count = 0;
    for (seq, op) in replayed.ops.into_iter().filter(|&(s, _)| s > header.wal_applied_seq) {
        if !inner.arena.publish(seq, op) {
            return Err(Error::busy("overlay arena full during replay"));
        }
        count += 1;
    }
    Ok(Built {
        inner,
        next_seq,
        replayed: count,
        good_len: replayed.good_len,
        log: LogState::Current { torn },
    })
}

/// Load `graph.base` + replay `graph.wal` for `dir`; a writable opener also
/// resets a stale log and truncates a torn tail.
fn load_generation(dir: &Path, read_only: bool) -> Result<Built> {
    let map = MappedFile::open(&base_path(dir))?;
    let wal_file = wal_path(dir);
    let raw = match fs::read(&wal_file) {
        Ok(raw) => raw,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound && read_only => Vec::new(),
        Err(e) => return Err(e.into()),
    };
    let built = build_generation(map, &raw)?;
    if !read_only {
        match built.log {
            LogState::Stale => {
                let generation = built.inner.dir.header.generation;
                atomic_write(&wal_file, &wal::encode_header(generation, now_ms()))?;
            }
            LogState::Current { torn: true } => {
                let f = fs::OpenOptions::new().write(true).open(&wal_file)?;
                f.set_len(built.good_len as u64)?;
                f.sync_all()?;
            }
            LogState::Empty | LogState::Current { torn: false } => {}
        }
    }
    Ok(built)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::writer::DocRecord;
    use crate::UrlKey;

    fn doc(url: &str, hash: u64) -> DocRecord<'_> {
        DocRecord {
            url,
            url_key: UrlKey::of(url),
            content_hash: hash,
            fetched_at_ms: 1,
            title: None,
            snippet: None,
            etag: None,
            pinned: false,
        }
    }

    fn write(store: &Store, url: &str, hash: u64) {
        let mut w = store.writer().unwrap();
        w.upsert_node(&doc(url, hash)).unwrap();
        w.commit().unwrap();
    }

    /// A snapshot is frozen at the moment it is taken. Commits landing afterwards
    /// are invisible to it, which is the whole reason readers never need a lock.
    #[test]
    fn a_snapshot_does_not_observe_later_commits() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::create(dir.path(), Config::default()).unwrap();
        write(&store, "https://x.dev/a", 1);

        let before = store.snapshot();
        assert_eq!(before.len(), 1);

        write(&store, "https://x.dev/b", 2);
        assert_eq!(before.len(), 1, "an existing snapshot must not grow");
        assert_eq!(store.snapshot().len(), 2, "a fresh snapshot sees the commit");
    }

    /// Compaction installs a new generation into a free ring slot and swaps the
    /// current pointer. A snapshot taken beforehand keeps reading the generation
    /// it pinned, untouched, until it drops.
    #[test]
    fn compaction_leaves_an_existing_snapshot_on_its_own_generation() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::create(dir.path(), Config::default()).unwrap();
        write(&store, "https://x.dev/a", 1);
        store.compact().unwrap();

        let pinned = store.snapshot();
        let pinned_gen = pinned.generation();

        write(&store, "https://x.dev/b", 2);
        store.compact().unwrap();

        assert_eq!(pinned.generation(), pinned_gen, "a pinned generation must not move");
        assert_eq!(pinned.len(), 1, "the old generation still reads its own state");
        let fresh = store.snapshot();
        assert!(fresh.generation() > pinned_gen, "the store advanced");
        assert_eq!(fresh.len(), 2);
    }

    /// The generation number increments once per compaction and is what the WAL
    /// checks itself against on replay.
    #[test]
    fn each_compaction_advances_the_generation_exactly_once() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::create(dir.path(), Config::default()).unwrap();
        write(&store, "https://x.dev/a", 1);

        let mut prev = store.snapshot().generation();
        for _ in 0..6 {
            store.compact().unwrap();
            let now = store.snapshot().generation();
            assert_eq!(now, prev + 1, "a compaction must advance the generation by one");
            prev = now;
        }
    }

    /// The ring is finite. Holding a snapshot on every slot leaves compaction
    /// nowhere to install the new generation — a documented, retryable refusal
    /// rather than a stall or a corrupted swap. The new base is already durable
    /// on disk, so the retry only has to redo the in-memory install.
    #[test]
    fn exhausting_the_generation_ring_is_a_retryable_refusal() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::create(dir.path(), Config::default()).unwrap();
        write(&store, "https://x.dev/a", 1);

        // Compaction never reuses the current slot, so with GEN_SLOTS slots it
        // can install GEN_SLOTS - 1 new generations while each previous one
        // stays pinned. Pin before every compaction so none can be recycled.
        let mut pins = Vec::new();
        for round in 0..GEN_SLOTS - 1 {
            pins.push(store.snapshot());
            store.compact().unwrap_or_else(|e| panic!("round {round} should still fit: {e}"));
        }

        // Pinning the newest generation too leaves the next install nowhere to go.
        pins.push(store.snapshot());
        assert!(
            matches!(store.compact(), Err(Error::Busy { .. })),
            "a full ring must refuse with Busy, not stall or corrupt the swap"
        );

        // Releasing the pins makes the very same call succeed.
        drop(pins);
        store.compact().expect("compaction must succeed once a slot frees up");
        assert_eq!(store.snapshot().len(), 1);
    }

    /// A read-only handle must be able to pick up another process's work without
    /// reopening, and report whether anything actually changed.
    #[test]
    fn a_read_only_handle_reloads_a_newer_generation() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::create(dir.path(), Config::default()).unwrap();
        write(&store, "https://x.dev/a", 1);
        store.compact().unwrap();

        let reader = Store::open_read_only(dir.path()).unwrap();
        assert_eq!(reader.snapshot().len(), 1);
        assert!(!reader.reload_if_stale().unwrap(), "nothing has changed yet");

        write(&store, "https://x.dev/b", 2);
        store.compact().unwrap();

        assert!(reader.reload_if_stale().unwrap(), "a new generation is visible");
        assert_eq!(reader.snapshot().len(), 2);
    }
}
