// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
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
use crate::mmap::MappedFile;
use crate::overlay::Arena;
use crate::snapshot::Snapshot;
use crate::ttl::TtlConfig;
use crate::writer::{WriteState, Writer};
use std::fs;
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
    /// What a reader saw at its last load: the file stamp for cheap
    /// staleness checks and how far into the log it has consumed. The mutex
    /// also makes reloads on one handle one-at-a-time (the overlay arena is
    /// single-producer); snapshots never take it.
    reload: Mutex<Reload>,
}

#[derive(Clone, Copy, Debug, Default)]
struct Reload {
    stamp: Option<Stamp>,
    /// Byte offset just past the last intact log frame consumed.
    wal_pos: u64,
}

/// The cross-process writer lock, held for the store's lifetime.
#[derive(Debug)]
struct LockFile {
    _file: fs::File,
}

impl LockFile {
    fn acquire(path: &Path) -> Result<Self> {
        match crate::fsio::try_lock(path)? {
            Some(file) => Ok(Self { _file: file }),
            None => Err(Error::Locked),
        }
    }
}

/// Re-reads tolerated when a reader catches a compaction between its two
/// renames (new log, old base): the base rename always lands first.
const RACE_RETRIES: usize = 3;

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

/// Write `bytes` to `path` atomically (see [`crate::fsio::write_atomic`]).
use crate::fsio::now_ms;

pub(crate) fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    crate::fsio::write_atomic(path, &[bytes]).map_err(Into::into)
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
        if built.log == LogState::Newer {
            return Err(newer_image());
        }
        Ok(Self::assemble(PathBuf::new(), Config::default(), true, None, None, built))
    }

    /// Keep an image store current. Pass the new base when the generation
    /// changed (a compaction), else `None` and the current log image: frames
    /// not yet visible are published. Returns `true` when anything new
    /// became visible. Works on file-backed read-only stores too.
    pub fn reload_image(&self, base: Option<bytes::Bytes>, wal: &[u8]) -> Result<bool> {
        if !self.read_only {
            return Ok(false);
        }
        let _one_at_a_time = self.reload.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        match base {
            Some(b) => {
                self.install_or_apply(MappedFile::from_bytes(b), wal)?.ok_or_else(newer_image)
            }
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
        let reload = Reload { stamp, wal_pos: built.good_len as u64 };
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
                generation,
            )),
            compact_inline: AtomicBool::new(true),
            due: AtomicBool::new(false),
            reload: Mutex::new(reload),
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
    /// When only the log grew, only the appended bytes are read. Reloads on
    /// one handle run one at a time; snapshots are never blocked. On a
    /// writable handle this is a no-op (`Ok(false)`): its own view is always
    /// current.
    pub fn reload_if_stale(&self) -> Result<bool> {
        if self.is_image() || !self.read_only {
            return Ok(false); // images: see reload_image; writers: always current
        }
        let now = Stamp::read(&self.dir);
        let mut st = self.reload.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if let (Some(n), Some(l)) = (now, st.stamp) {
            if n == l {
                return Ok(false);
            }
            // Same base and a log that only grew: read just the new bytes.
            if n.base == l.base && n.wal_len >= st.wal_pos && st.wal_pos >= wal::HEADER_LEN as u64 {
                let (changed, pos) = self.reload_wal_tail(st.wal_pos)?;
                *st = Reload { stamp: now, wal_pos: pos };
                return Ok(changed);
            }
        }
        let (changed, pos) = self.reload_full()?;
        *st = Reload { stamp: now, wal_pos: pos };
        Ok(changed)
    }

    /// Same base file: read the log from `pos` (a frame boundary already
    /// consumed) and publish the new frames. Returns whether anything became
    /// visible and the new consumed offset.
    fn reload_wal_tail(&self, pos: u64) -> Result<(bool, u64)> {
        use std::io::{Read as _, Seek as _, SeekFrom};
        let mut tail = Vec::new();
        match fs::File::open(wal_path(&self.dir)) {
            Ok(mut f) => {
                f.seek(SeekFrom::Start(pos))?;
                f.read_to_end(&mut tail)?;
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok((false, pos)),
            Err(e) => return Err(e.into()),
        }
        let (ops, used) = wal::replay_frames(&tail)?;
        Ok((self.publish_new(ops)?, pos + used as u64))
    }

    /// Publish `ops` (in sequence order) that the served generation has not
    /// seen: sequence above both the base's folded ops and the overlay's last.
    fn publish_new(&self, ops: Vec<(u64, wal::Op)>) -> Result<bool> {
        let cur = self.current.load(Ordering::Acquire);
        let guard = self.gens[cur].inner.try_read().map_err(|_| Error::busy("slot busy"))?;
        let Some(g) = guard.as_ref() else { return Ok(false) };
        let seen = g.dir.header.wal_applied_seq.max(g.arena.last_seq());
        let mut changed = false;
        for (seq, op) in ops.into_iter().filter(|&(s, _)| s > seen) {
            if !g.arena.publish(seq, op) {
                return Err(Error::busy("overlay arena full"));
            }
            changed = true;
        }
        Ok(changed)
    }

    /// Publish the frames of a log image that the current generation has not
    /// seen. `None` when the image belongs to another generation.
    fn apply_wal_bytes(&self, raw: &[u8]) -> Result<Option<bool>> {
        if raw.is_empty() {
            return Ok(Some(false));
        }
        let replayed = wal::replay(raw)?;
        if replayed.base_generation != self.generation() {
            return Ok(None);
        }
        self.publish_new(replayed.ops).map(Some)
    }

    /// Map the base and read the log, retrying across a compaction caught
    /// between its renames. Returns whether anything changed and the log
    /// offset consumed.
    fn reload_full(&self) -> Result<(bool, u64)> {
        for _ in 0..RACE_RETRIES {
            let map = MappedFile::open(&base_path(&self.dir))?;
            let raw = match fs::read(wal_path(&self.dir)) {
                Ok(raw) => raw,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
                Err(e) => return Err(e.into()),
            };
            if let Some(changed) = self.install_or_apply(map, &raw)? {
                return Ok((changed, wal::committed_len(&raw) as u64));
            }
        }
        Err(Error::busy("compaction in progress; retry"))
    }

    /// Same generation as served: publish new frames. Otherwise rebuild the
    /// generation from `map` + `raw` and install it into a free ring slot.
    /// `None` when the log is newer than `map` (a compaction between the
    /// reads): the caller re-reads.
    fn install_or_apply(&self, map: MappedFile, raw: &[u8]) -> Result<Option<bool>> {
        let cur_gen = self.generation();
        let new_gen = base::Header::parse(map.bytes())?.generation;
        if new_gen == cur_gen {
            // Generations are never reused, so the same number is the same
            // base: only the log can have grown.
            return match self.apply_wal_bytes(raw)? {
                Some(changed) => Ok(Some(changed)),
                // Another generation's log: newer means a compaction landed
                // between the reads (re-read); older is already folded in.
                None if wal::replay(raw)?.base_generation > new_gen => Ok(None),
                None => Ok(Some(false)),
            };
        }
        let built = build_generation(map, raw)?;
        if built.log == LogState::Newer {
            return Ok(None);
        }
        let cur = self.current.load(Ordering::Acquire);
        for (i, slot) in self.gens.iter().enumerate() {
            if i == cur {
                continue;
            }
            if let Ok(mut w) = slot.inner.try_write() {
                *w = Some(built.inner);
                drop(w);
                slot.seq.store(new_gen, Ordering::Release);
                self.current.store(i, Ordering::Release);
                return Ok(Some(true));
            }
        }
        Err(Error::busy("all generation slots pinned by snapshots"))
    }
}

fn newer_image() -> Error {
    Error::busy("log image is newer than its base; fetch the base again")
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
    /// A log of a later generation than the base: a compaction happened
    /// between reading the two (readers re-read; a writer never sees this).
    Newer,
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
        return Ok(Built {
            inner,
            next_seq: header.wal_applied_seq + 1,
            replayed: 0,
            good_len: 0,
            log: LogState::Newer,
        });
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
    let wal_file = wal_path(dir);
    let mut attempt = 0;
    let mut built = loop {
        let map = MappedFile::open(&base_path(dir))?;
        let raw = match fs::read(&wal_file) {
            Ok(raw) => raw,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound && read_only => Vec::new(),
            Err(e) => return Err(e.into()),
        };
        let built = build_generation(map, &raw)?;
        if built.log != LogState::Newer {
            break built;
        }
        // A writer holds the lock, so nothing can compact under it: a newer
        // log is real damage. A reader caught a compaction mid-way: re-read.
        attempt += 1;
        if !read_only {
            return Err(Error::corrupt("wal is newer than base"));
        }
        if attempt == RACE_RETRIES {
            return Err(Error::busy("compaction in progress; retry"));
        }
    };
    if !read_only {
        match built.log {
            // An empty log (external truncation) gets its header back, or
            // the next commit would append frames to a headerless file.
            LogState::Stale | LogState::Empty => {
                let generation = built.inner.dir.header.generation;
                atomic_write(&wal_file, &wal::encode_header(generation, now_ms()))?;
                built.good_len = wal::HEADER_LEN;
            }
            LogState::Current { torn: true } => {
                let f = fs::OpenOptions::new().write(true).open(&wal_file)?;
                f.set_len(built.good_len as u64)?;
                f.sync_all()?;
            }
            LogState::Current { torn: false } | LogState::Newer => {}
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
