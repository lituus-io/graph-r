// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! The store: a directory holding `graph.base` (immutable snapshot),
//! `graph.wal` (append-only op log) and `graph.lock` (commit exclusion).
//!
//! # Concurrency model
//!
//! - **Any number of writers.** Any number of read-write handles may be open
//!   on one directory, in one process or many; opening takes no lock. What is
//!   exclusive is the *commit*: [`Writer::commit`], [`Store::transact`] and
//!   compaction take an exclusive `flock` on `graph.lock` only around the
//!   append (and its fsync) and release it straight after. A busy lock is
//!   retried with exponential backoff (1 ms doubling to 50 ms) for up to
//!   [`Config::lock_wait`], then refused with [`Error::Locked`]. `flock` is
//!   per open file description, so two handles in one process exclude each
//!   other exactly like two processes; within one handle a mutex serializes
//!   [`Writer`]s across threads.
//! - **Catch-up under the lock.** Holding the lock, a committer first
//!   compares the files' stamp (two `stat`s) with what its handle last saw.
//!   A new base (another handle compacted) is installed into a free
//!   generation slot; a grown log has just its appended frames read and
//!   published; the sequence counter and log offset advance to the file's.
//!   Only then are the new frames encoded and appended, so sequence numbers
//!   stay strictly increasing across every writer. A torn tail left by a
//!   writer that crashed mid-append is truncated here, by the next committer
//!   (and opportunistically at open when the lock happens to be free).
//! - **Serializable transactions.** [`Store::transact`] holds the lock from
//!   before its caught-up read view is taken until its single append lands,
//!   so a read-modify-write is serializable across processes. One
//!   transaction is one append and one fsync however many ops it carries.
//! - **Readers never lock.** [`Store::open_read_only`] takes no lock and
//!   never writes; [`Store::reload_if_stale`] picks up other handles' work on
//!   any handle (a writable one also catches up at every commit).
//! - A fixed ring of generation slots each holds an mmapped base plus its
//!   committed-op overlay behind an `RwLock`. Readers only ever `try_read` —
//!   the sole moment a `try_read` can fail is the microseconds in which a
//!   handle installs a *different* generation into a free slot, so readers
//!   spin-retry instead of blocking. Snapshots are `RwLockReadGuard`-backed
//!   borrow guards: a pinned generation cannot be recycled under them, and
//!   nothing is reference-counted.
//! - Compaction writes a new base + fresh WAL under the lock, installs it
//!   into a free slot, and swaps the current index; existing snapshots keep
//!   reading the old generation untouched until they drop. Other handles
//!   install the new base at their next catch-up.
//!
//! A transaction is atomic with respect to other writers and to a failed
//! append (the log is cut back to where it was). The log format has no batch
//! marker, so a power loss in the middle of a multi-op append can still leave
//! a prefix of that transaction's frames — the same guarantee a multi-op
//! [`Writer::commit`] has always had.

use crate::compact;
use crate::error::{Error, Result};
use crate::format::{base, wal};
use crate::mmap::MappedFile;
use crate::overlay::Arena;
use crate::snapshot::Snapshot;
use crate::ttl::TtlConfig;
use crate::writer::{Txn, WriteState, Writer};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard, RwLock};
use std::time::{Duration, Instant};

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
    /// How long a commit waits for another writer's commit lock before
    /// giving up with [`Error::Locked`].
    pub lock_wait: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            durability: Durability::Always,
            compact_after_ops: 4096,
            compact_after_wal_bytes: 4 << 20,
            ttl: TtlConfig::default(),
            lock_wait: Duration::from_secs(10),
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
    pub(crate) current: AtomicUsize,
    pub(crate) gens: [GenSlot; GEN_SLOTS],
    /// Per-handle write state, including what the handle last saw of the
    /// files. Also makes reloads on one handle one-at-a-time (the overlay
    /// arena is single-producer); snapshots never take it.
    pub(crate) write: Mutex<WriteState>,
    /// Compact inside `commit` past the thresholds (the 0.2 behaviour).
    pub(crate) compact_inline: AtomicBool,
    /// Thresholds reached while inline compaction is off.
    pub(crate) due: AtomicBool,
}

/// What a handle saw at its last load or catch-up.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Seen {
    /// The file stamp, for cheap staleness checks; `None` forces a full
    /// reload.
    pub(crate) stamp: Option<Stamp>,
    /// Byte offset just past the last intact log frame consumed.
    pub(crate) wal_pos: u64,
    /// The log's header names the base's generation (false for a missing,
    /// empty or older log, which a committer rewrites).
    pub(crate) log_current: bool,
    /// Generation of the base file on disk (the served one, unless every
    /// ring slot was pinned when it appeared).
    pub(crate) base_gen: u64,
}

impl Seen {
    /// Account for `n` bytes this handle appended itself.
    pub(crate) fn appended(&mut self, n: u64) {
        self.wal_pos += n;
        if let Some(w) = self.stamp.as_mut().and_then(|s| s.wal.as_mut()) {
            w.1 += n;
        }
    }
}

/// The commit lock, held until dropped. Owned, so a caller (a language
/// binding, say) can keep it across several calls and then commit under it
/// with [`Store::transact_with`].
#[derive(Debug)]
pub struct CommitLock {
    _held: Held,
    dir: PathBuf,
}

/// A held `flock` on `graph.lock`; dropping the file releases it.
#[derive(Debug)]
pub(crate) struct Held(pub(crate) fs::File);

/// Open `dir`'s lock file. The path is fixed (`dir/graph.lock`), and a lock
/// file that is not a regular file — a symlink planted to make the store
/// create or lock a file elsewhere — is refused.
fn open_lock_file(dir: &Path) -> Result<fs::File> {
    let path = lock_path(dir);
    match fs::symlink_metadata(&path) {
        Ok(md) if !md.file_type().is_file() => {
            return Err(Error::corrupt(format!("{LOCK_FILE} is not a regular file")));
        }
        _ => {}
    }
    Ok(fs::OpenOptions::new().create(true).truncate(false).write(true).open(path)?)
}

/// Lock `file`, retrying with exponential backoff (1 ms doubling to 50 ms)
/// for up to `wait`; `Error::Locked` past it.
fn lock_within(file: &fs::File, wait: Duration) -> Result<()> {
    let mut start = None;
    let mut nap = Duration::from_millis(1);
    loop {
        #[cfg(test)]
        probe::bump(&probe::LOCKS, 1);
        match file.try_lock() {
            Ok(()) => return Ok(()),
            Err(fs::TryLockError::WouldBlock) => {}
            Err(fs::TryLockError::Error(e)) => return Err(e.into()),
        }
        let left = wait.saturating_sub(start.get_or_insert_with(Instant::now).elapsed());
        if left.is_zero() {
            return Err(Error::Locked);
        }
        std::thread::sleep(nap.min(left));
        nap = (nap * 2).min(Duration::from_millis(50));
    }
}

/// Take `dir`'s commit lock on a fresh descriptor.
fn lock_dir(dir: &Path, wait: Duration) -> Result<Held> {
    let file = open_lock_file(dir)?;
    lock_within(&file, wait)?;
    Ok(Held(file))
}

/// Counters for the counted (never clocked) performance tests: per thread,
/// so parallel tests do not see each other's work.
#[cfg(test)]
pub(crate) mod probe {
    use std::cell::Cell;
    use std::thread::LocalKey;
    thread_local! {
        /// Lock attempts (`try_lock` calls).
        pub(crate) static LOCKS: Cell<u64> = const { Cell::new(0) };
        /// Log fsyncs by commits.
        pub(crate) static FSYNCS: Cell<u64> = const { Cell::new(0) };
        /// Log bytes read by tail catch-ups.
        pub(crate) static TAIL_BYTES: Cell<u64> = const { Cell::new(0) };
    }
    pub(crate) fn bump(c: &'static LocalKey<Cell<u64>>, n: u64) {
        c.with(|x| x.set(x.get() + n));
    }
    pub(crate) fn take(c: &'static LocalKey<Cell<u64>>) -> u64 {
        c.with(|x| x.replace(0))
    }
}

/// Re-reads tolerated when a reader catches a compaction between its two
/// renames (new log, old base): the base rename always lands first.
const RACE_RETRIES: usize = 3;

/// The base file's identity plus the log's identity and length. Equal stamps
/// mean nothing a reader could observe has changed: the base is only ever
/// replaced by rename (new inode, or new creation time where there are no
/// inodes), and the log only grows in place — it is replaced (by
/// compaction, or a committer's repair) or cut back (a torn tail), never
/// rewritten in place to the same length.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Stamp {
    base: FileId,
    /// The log's identity and length; `None` when it does not exist.
    wal: Option<(Ident, u64)>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct FileId {
    len: u64,
    modified: Option<std::time::SystemTime>,
    ident: Ident,
}

/// Which file a path names, independent of its contents.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Ident {
    #[cfg(unix)]
    ino: (u64, u64),
    #[cfg(not(unix))]
    created: Option<std::time::SystemTime>,
}

impl Ident {
    fn of(md: &fs::Metadata) -> Self {
        #[cfg(unix)]
        use std::os::unix::fs::MetadataExt as _;
        Self {
            #[cfg(unix)]
            ino: (md.dev(), md.ino()),
            #[cfg(not(unix))]
            created: md.created().ok(),
        }
    }
}

impl FileId {
    fn of(md: &fs::Metadata) -> Self {
        Self { len: md.len(), modified: md.modified().ok(), ident: Ident::of(md) }
    }
}

impl Stamp {
    /// Two `stat` calls; `None` when either file cannot be examined (the
    /// caller then falls back to a full load, which reports the real error).
    pub(crate) fn read(dir: &Path) -> Option<Self> {
        let base = FileId::of(&fs::metadata(base_path(dir)).ok()?);
        let wal = match fs::metadata(wal_path(dir)) {
            Ok(md) => Some((Ident::of(&md), md.len())),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(_) => return None,
        };
        Some(Self { base, wal })
    }
}

/// How a catch-up brought a handle up to the files.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Caught {
    /// The stamp was unchanged: nothing read.
    Nothing,
    /// Same base and log file: only the appended bytes were read.
    Tail,
    /// The base or the log file was replaced: both re-read.
    Full,
}

/// File name of the immutable base snapshot inside a store directory.
pub const BASE_FILE: &str = "graph.base";
/// File name of the append-only op log inside a store directory.
pub const WAL_FILE: &str = "graph.wal";
/// File name of the commit lock inside a store directory.
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

/// Lay down the files of a fresh store in `dir`, under its commit lock.
/// Never replaces a file that exists: the log first (so a base on disk
/// always has its log beside it), then the base, which is what marks the
/// store as created.
fn lay_down(dir: &Path) -> Result<()> {
    if !wal_path(dir).exists() {
        atomic_write(&wal_path(dir), &wal::encode_header(1, now_ms()))?;
    }
    atomic_write(&base_path(dir), &compact::render_empty_base(1))
}

impl Store {
    /// Create a fresh store in `dir` (created if absent; must not already
    /// contain a store) and open it read-write. Creation runs under the
    /// commit lock, so a racing creator either finds the store already made
    /// (and is refused) or waits for this one — it never lays fresh files
    /// over a store another process has committed to.
    pub fn create(dir: impl AsRef<Path>, cfg: Config) -> Result<Self> {
        let dir = dir.as_ref();
        fs::create_dir_all(dir)?;
        {
            let _held = lock_dir(dir, cfg.lock_wait)?;
            if base_path(dir).exists() {
                return Err(Error::format("store already exists; use open"));
            }
            lay_down(dir)?;
        }
        Self::open_with(dir, cfg, false)
    }

    /// Open the store in `dir` read-write, creating it first when absent.
    /// Safe when many processes race to create the same store: creation
    /// happens once, under the commit lock (waiting up to
    /// [`Config::lock_wait`] for a creator in progress), and everyone opens
    /// the result. An existing store is opened without taking any lock.
    pub fn open_or_create(dir: impl AsRef<Path>, cfg: Config) -> Result<Self> {
        let dir = dir.as_ref();
        if !base_path(dir).exists() {
            fs::create_dir_all(dir)?;
            let _held = lock_dir(dir, cfg.lock_wait)?;
            if !base_path(dir).exists() {
                lay_down(dir)?;
            }
        }
        Self::open_with(dir, cfg, false)
    }

    /// Open an existing store read-write. Takes no lock: any number of
    /// read-write handles may be open at once (see the module docs). A torn
    /// log tail is repaired by the next commit, or right here when no other
    /// writer holds the commit lock.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        Self::open_with(dir.as_ref(), Config::default(), false)
    }

    /// Open with an explicit configuration.
    pub fn open_cfg(dir: impl AsRef<Path>, cfg: Config) -> Result<Self> {
        Self::open_with(dir.as_ref(), cfg, false)
    }

    /// Open read-only: no lock taken, no repair performed; replay simply
    /// stops at a torn tail. Use [`Store::reload_if_stale`] to observe
    /// other handles' commits and compactions.
    pub fn open_read_only(dir: impl AsRef<Path>) -> Result<Self> {
        Self::open_with(dir.as_ref(), Config::default(), true)
    }

    fn open_with(dir: &Path, cfg: Config, read_only: bool) -> Result<Self> {
        let stamp = Stamp::read(dir);
        let built = match load_generation(dir, false) {
            // A log newer than its base, on every re-read. Without the lock
            // that may be a compaction in flight; a writable opener checks
            // under the lock, where it can only be damage.
            Err(Error::Busy { .. }) if !read_only => match lock_now(dir)? {
                Some(_held) => load_generation(dir, true)?,
                None => return Err(Error::busy("compaction in progress; retry")),
            },
            other => other?,
        };
        let repair = !read_only && built.log != LogState::Current { torn: false };
        let store = Self::assemble(dir.to_path_buf(), cfg, read_only, stamp, built);
        if repair {
            store.repair_now()?;
        }
        Ok(store)
    }

    /// Repair the log (torn tail, missing or stale header) if the commit lock
    /// is free right now; otherwise leave it to whoever holds it or commits
    /// next.
    fn repair_now(&self) -> Result<()> {
        let Some(held) = lock_now(&self.dir)? else { return Ok(()) };
        let mut state = self.write_state()?;
        let r = self.sync(&mut state, true).map(|_| ());
        drop(held);
        r
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
        Ok(Self::assemble(PathBuf::new(), Config::default(), true, None, built))
    }

    /// Keep an image store current. Pass the new base when the generation
    /// changed (a compaction), else `None` and the current log image: frames
    /// not yet visible are published. Returns `true` when anything new
    /// became visible. Works on file-backed read-only stores too.
    pub fn reload_image(&self, base: Option<bytes::Bytes>, wal: &[u8]) -> Result<bool> {
        if !self.read_only {
            return Ok(false);
        }
        let _one_at_a_time = self.write.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
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
        stamp: Option<Stamp>,
        built: Built,
    ) -> Self {
        let generation = built.inner.dir.header.generation;
        let seen = Seen {
            stamp,
            wal_pos: built.good_len as u64,
            log_current: matches!(built.log, LogState::Current { .. }),
            base_gen: generation,
        };
        let store = Self {
            dir,
            cfg,
            read_only,
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
                seen,
            )),
            compact_inline: AtomicBool::new(true),
            due: AtomicBool::new(false),
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
    /// when a catch-up installs a newer one).
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
    /// steady state; spins only across a slot-install window.
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

    fn write_state(&self) -> Result<MutexGuard<'_, WriteState>> {
        self.write.lock().map_err(|_| Error::corrupt("writer mutex poisoned"))
    }

    /// This handle's writer. `Error::ReadOnly` on read-only stores.
    ///
    /// Blocks while another thread holds a [`Writer`] on *this* handle.
    /// Writers on other handles — in this process or another — stage freely;
    /// only their commits take turns, on the commit lock.
    pub fn writer(&self) -> Result<Writer<'_>> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }
        Ok(Writer::new(self, self.write_state()?))
    }

    /// Run `f` as one serializable transaction and commit what it stages.
    ///
    /// The commit lock is taken (waiting up to [`Config::lock_wait`]) and
    /// this handle caught up *before* `f` runs, and held until its single
    /// append (one fsync) lands: [`Txn::snapshot`] is the latest committed
    /// state of every writer, and no other writer can commit between that
    /// read and this transaction's write. `Err` from `f` aborts with nothing
    /// written. The view shows committed state only — ops staged by `f` are
    /// not visible in it.
    pub fn transact<F, R>(&self, f: F) -> Result<R>
    where
        F: for<'t> FnOnce(&mut Txn<'t>) -> Result<R>,
    {
        crate::writer::transact(self, None, self.cfg.lock_wait, f)
    }

    /// [`Store::transact`] that never waits: `Error::Locked` at once when
    /// another writer holds the commit lock (`f` does not run), so an async
    /// caller can retry on its own schedule instead of parking a thread.
    pub fn try_transact<F, R>(&self, f: F) -> Result<R>
    where
        F: for<'t> FnOnce(&mut Txn<'t>) -> Result<R>,
    {
        crate::writer::transact(self, None, Duration::ZERO, f)
    }

    /// Take the commit lock now and catch this handle up, for a transaction
    /// that spans several calls (a language binding's `with` block). Commit
    /// under it with [`Store::transact_with`]; drop it to release. While it
    /// is held, a commit on this same handle from another thread waits for it
    /// like any other writer would.
    pub fn commit_lock(&self) -> Result<CommitLock> {
        self.commit_lock_within(self.cfg.lock_wait)
    }

    /// [`Store::commit_lock`] that never waits: `Error::Locked` at once when
    /// another writer holds it.
    pub fn try_commit_lock(&self) -> Result<CommitLock> {
        self.commit_lock_within(Duration::ZERO)
    }

    fn commit_lock_within(&self, wait: Duration) -> Result<CommitLock> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }
        let mut state = self.write_state()?;
        let held = lock_dir(&self.dir, wait)?;
        self.sync(&mut state, true)?;
        Ok(CommitLock { _held: held, dir: self.dir.clone() })
    }

    /// [`Store::transact`] under a lock already taken with
    /// [`Store::commit_lock`] on this store (refused for another store's).
    pub fn transact_with<F, R>(&self, lock: &CommitLock, f: F) -> Result<R>
    where
        F: for<'t> FnOnce(&mut Txn<'t>) -> Result<R>,
    {
        if lock.dir != self.dir {
            return Err(Error::format("the commit lock belongs to another store"));
        }
        crate::writer::transact(self, Some(lock), Duration::ZERO, f)
    }

    /// Fold the WAL into a new base generation now. Also triggered
    /// automatically by [`Writer::commit`] past the configured thresholds.
    /// Runs under the commit lock, on a caught-up handle.
    pub fn compact(&self) -> Result<compact::CompactStats> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }
        let mut state = self.write_state()?;
        let held = self.acquire(&mut state, self.cfg.lock_wait)?;
        let r = self.sync(&mut state, true).and_then(|_| compact::compact_locked(self, &mut state));
        state.release(held);
        r
    }

    /// Pick up commits and compactions made through other handles (other
    /// processes, or other handles in this one). Returns `true` when
    /// anything new became visible.
    ///
    /// Cheap when nothing changed: two `stat` calls, no mapping, no reads.
    /// When only the log grew, only the appended bytes are read. Reloads on
    /// one handle run one at a time; snapshots are never blocked. A writable
    /// handle whose [`Writer`] is held by another thread returns `Ok(false)`
    /// rather than wait (it catches up at that writer's commit anyway).
    pub fn reload_if_stale(&self) -> Result<bool> {
        if self.is_image() {
            return Ok(false); // images: see reload_image
        }
        let mut state = if self.read_only {
            self.write.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
        } else {
            match self.write.try_lock() {
                Ok(g) => g,
                Err(std::sync::TryLockError::WouldBlock) => return Ok(false),
                Err(std::sync::TryLockError::Poisoned(p)) => p.into_inner(),
            }
        };
        self.sync(&mut state, false)
    }

    /// Take the commit lock with this handle's cached descriptor, waiting up
    /// to `wait`.
    pub(crate) fn acquire(&self, state: &mut WriteState, wait: Duration) -> Result<Held> {
        let file = match state.lock_file.take() {
            Some(f) => f,
            None => open_lock_file(&self.dir)?,
        };
        match lock_within(&file, wait) {
            Ok(()) => Ok(Held(file)),
            Err(e) => {
                state.lock_file = Some(file);
                Err(e)
            }
        }
    }

    /// Bring this handle up to the files: a new base installed, appended
    /// frames published, the write counters advanced to the file's. With
    /// `repair` — only ever under the commit lock, where nobody can be
    /// mid-append — a torn tail is truncated and a missing or stale log
    /// header rewritten. Returns whether anything new became visible.
    pub(crate) fn sync(&self, state: &mut WriteState, repair: bool) -> Result<bool> {
        let (changed, caught) = self.catch_up(&mut state.seen, repair)?;
        if caught == Caught::Nothing && !repair {
            return Ok(false);
        }
        if caught == Caught::Full {
            state.drop_wal_handle(); // the log may be another file now
        }
        if repair {
            self.repair(state)?;
        }
        let cur = self.current.load(Ordering::Acquire);
        let guard = self.gens[cur].inner.try_read().map_err(|_| Error::busy("slot busy"))?;
        let g = guard.as_ref().ok_or_else(|| Error::corrupt("current slot empty"))?;
        let next_seq = g.dir.header.wal_applied_seq.max(g.arena.last_seq()) + 1;
        state.adopt(next_seq, g.arena.len(), state.seen.base_gen);
        if caught == Caught::Full {
            let over = state.ops_since_compact >= self.cfg.compact_after_ops
                || state.wal_bytes >= self.cfg.compact_after_wal_bytes;
            self.due.store(over && !self.compact_inline.load(Ordering::Relaxed), Ordering::Release);
        }
        Ok(changed)
    }

    /// Cut a torn tail back to the last intact frame, or give a missing,
    /// empty or older log a header for the served generation. Under the
    /// commit lock only.
    fn repair(&self, state: &mut WriteState) -> Result<()> {
        let seen = &mut state.seen;
        let file = wal_path(&self.dir);
        if !seen.log_current {
            atomic_write(&file, &wal::encode_header(seen.base_gen, now_ms()))?;
            *seen = Seen {
                stamp: Stamp::read(&self.dir),
                wal_pos: wal::HEADER_LEN as u64,
                log_current: true,
                base_gen: seen.base_gen,
            };
            state.drop_wal_handle();
            return Ok(());
        }
        if let Some((ident, len)) = seen.stamp.and_then(|s| s.wal) {
            if len > seen.wal_pos {
                let f = fs::OpenOptions::new().write(true).open(&file)?;
                f.set_len(seen.wal_pos)?;
                f.sync_all()?;
                if let Some(s) = seen.stamp.as_mut() {
                    s.wal = Some((ident, seen.wal_pos));
                }
            }
        }
        Ok(())
    }

    /// Compare the files with what `seen` recorded and read only what
    /// changed. Under the commit lock (`locked`) a log newer than its base
    /// is damage; without it, a compaction caught mid-way (re-read, then
    /// `Busy`).
    fn catch_up(&self, seen: &mut Seen, locked: bool) -> Result<(bool, Caught)> {
        let now = Stamp::read(&self.dir);
        if let (Some(n), Some(l)) = (now, seen.stamp) {
            if n == l {
                return Ok((false, Caught::Nothing));
            }
            // Same base and log file, and a log that only grew: read just
            // the new bytes.
            if let (Some((ni, nlen)), Some((li, _))) = (n.wal, l.wal) {
                if n.base == l.base
                    && ni == li
                    && nlen >= seen.wal_pos
                    && seen.wal_pos >= wal::HEADER_LEN as u64
                {
                    let (changed, pos) = self.reload_wal_tail(seen.wal_pos)?;
                    *seen = Seen { stamp: now, wal_pos: pos, ..*seen };
                    return Ok((changed, Caught::Tail));
                }
            }
        }
        let (changed, pos, log_current, base_gen) = self.reload_full(locked)?;
        *seen = Seen { stamp: now, wal_pos: pos, log_current, base_gen };
        Ok((changed, Caught::Full))
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
        #[cfg(test)]
        probe::bump(&probe::TAIL_BYTES, tail.len() as u64);
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
    /// between its renames (under the commit lock there is none: a newer log
    /// is damage). Returns whether anything changed, the log offset consumed,
    /// whether the log belongs to the base and the base's generation.
    fn reload_full(&self, locked: bool) -> Result<(bool, u64, bool, u64)> {
        for _ in 0..RACE_RETRIES {
            let map = MappedFile::open(&base_path(&self.dir))?;
            let base_gen = base::Header::parse(map.bytes())?.generation;
            let raw = match fs::read(wal_path(&self.dir)) {
                Ok(raw) => raw,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
                Err(e) => return Err(e.into()),
            };
            if let Some(changed) = self.install_or_apply(map, &raw)? {
                let current = wal::parse_header(&raw).ok() == Some(base_gen);
                let pos = if current { wal::committed_len(&raw) } else { wal::HEADER_LEN };
                return Ok((changed, pos as u64, current, base_gen));
            }
            if locked {
                return Err(Error::corrupt("wal is newer than base"));
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
        let folded = built.inner.dir.header.wal_applied_seq;
        match self.install(built.inner, new_gen) {
            Ok(()) => Ok(Some(true)),
            // Every other slot is pinned by a snapshot. If the served
            // generation already holds every op the new base folded (this
            // handle's own compaction, typically), keep serving it and
            // publish the new base's log on top: the same state, one
            // generation behind on disk. Otherwise there is no way to serve
            // it yet.
            Err(busy) => {
                if folded > self.served_seq()? {
                    return Err(busy);
                }
                match built.log {
                    LogState::Current { .. } => self.publish_new(wal::replay(raw)?.ops).map(Some),
                    _ => Ok(Some(false)), // nothing past what the base folded
                }
            }
        }
    }

    /// The last sequence number the served generation holds.
    fn served_seq(&self) -> Result<u64> {
        let cur = self.current.load(Ordering::Acquire);
        let guard = self.gens[cur].inner.try_read().map_err(|_| Error::busy("slot busy"))?;
        Ok(guard.as_ref().map_or(0, |g| g.dir.header.wal_applied_seq.max(g.arena.last_seq())))
    }

    /// Install a loaded generation into a free ring slot and serve it. The
    /// one install path: catch-ups and compaction both come through here.
    pub(crate) fn install(&self, inner: GenInner, generation: u64) -> Result<()> {
        let cur = self.current.load(Ordering::Acquire);
        for (i, slot) in self.gens.iter().enumerate() {
            if i == cur {
                continue;
            }
            if let Ok(mut w) = slot.inner.try_write() {
                *w = Some(inner);
                drop(w);
                slot.seq.store(generation, Ordering::Release);
                self.current.store(i, Ordering::Release);
                return Ok(());
            }
        }
        Err(Error::busy("all generation slots pinned by snapshots"))
    }
}

/// Take `dir`'s commit lock if it is free this instant (no waiting).
fn lock_now(dir: &Path) -> Result<Option<Held>> {
    match lock_dir(dir, Duration::ZERO) {
        Ok(h) => Ok(Some(h)),
        Err(Error::Locked) => Ok(None),
        Err(e) => Err(e),
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
    /// between reading the two (readers re-read; under the lock, damage).
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

/// Load `graph.base` + replay `graph.wal` for `dir`. Pure reading: repair is
/// a committer's job, under the commit lock. A log newer than its base is
/// re-read (a compaction caught between its renames) and then `Busy` —
/// unless the caller holds the lock (`locked`), where it is damage.
fn load_generation(dir: &Path, locked: bool) -> Result<Built> {
    let wal_file = wal_path(dir);
    for _ in 0..RACE_RETRIES {
        let map = MappedFile::open(&base_path(dir))?;
        let raw = match fs::read(&wal_file) {
            Ok(raw) => raw,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(e.into()),
        };
        let built = build_generation(map, &raw)?;
        if built.log != LogState::Newer {
            return Ok(built);
        }
        if locked {
            return Err(Error::corrupt("wal is newer than base"));
        }
    }
    Err(Error::busy("compaction in progress; retry"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::writer::DocRecord;
    use crate::UrlKey;
    use std::time::{Duration, Instant};

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

    // ---- several writers ---------------------------------------------------

    fn quick() -> Config {
        Config { lock_wait: Duration::from_millis(30), ..Config::default() }
    }

    fn urls(store: &Store) -> Vec<String> {
        let mut v = Vec::new();
        store.snapshot().for_each_live(|n| v.push(n.url.to_owned()));
        v.sort();
        v
    }

    /// A second handle's commits land in the log; this handle's next commit
    /// catches up first — reads just those frames, takes the next sequence
    /// number after them, and serves both.
    #[test]
    fn a_commit_catches_up_with_a_grown_log_first() {
        let dir = tempfile::tempdir().unwrap();
        let a = Store::create(dir.path(), Config::default()).unwrap();
        let b = Store::open(dir.path()).unwrap();
        write(&a, "https://x.dev/a", 1);
        write(&b, "https://x.dev/b", 2);
        write(&a, "https://x.dev/c", 3);
        assert_eq!(urls(&a), ["https://x.dev/a", "https://x.dev/b", "https://x.dev/c"]);
        let raw = fs::read(wal_path(dir.path())).unwrap();
        let seqs: Vec<u64> = wal::replay(&raw).unwrap().ops.iter().map(|o| o.0).collect();
        assert_eq!(seqs, [1, 2, 3], "one sequence across both writers");
        assert!(b.reload_if_stale().unwrap(), "a writable handle reloads too");
        assert_eq!(urls(&b), urls(&a));
    }

    /// Another handle compacts: this handle's next commit installs the new
    /// base before appending to the new log, never to the replaced one.
    #[test]
    fn a_commit_catches_up_after_another_handle_compacted() {
        let dir = tempfile::tempdir().unwrap();
        let a = Store::create(dir.path(), Config::default()).unwrap();
        let b = Store::open(dir.path()).unwrap();
        write(&a, "https://x.dev/a", 1); // a's log handle is now open
        write(&b, "https://x.dev/b", 2);
        b.compact().unwrap();
        let generation = b.generation();
        write(&a, "https://x.dev/c", 3);
        assert_eq!(a.generation(), generation, "the compacted base was installed");
        drop((a, b));
        let fresh = Store::open_read_only(dir.path()).unwrap();
        assert_eq!(urls(&fresh), ["https://x.dev/a", "https://x.dev/b", "https://x.dev/c"]);
    }

    /// Past `lock_wait` a commit is a typed `Locked`, with its ops kept.
    #[test]
    fn a_commit_past_lock_wait_is_locked_and_keeps_its_ops() {
        let dir = tempfile::tempdir().unwrap();
        let a = Store::create(dir.path(), Config::default()).unwrap();
        let b = Store::open_cfg(dir.path(), quick()).unwrap();
        let held = a.commit_lock().unwrap();
        let mut w = b.writer().unwrap();
        w.upsert_node(&doc("https://x.dev/a", 1)).unwrap();
        let started = Instant::now();
        assert!(matches!(w.commit(), Err(Error::Locked)));
        assert!(started.elapsed() >= Duration::from_millis(30), "it waited lock_wait");
        let third = Store::open_cfg(dir.path(), quick()).unwrap();
        assert!(matches!(third.compact(), Err(Error::Locked)), "compaction takes the same lock");
        // The non-waiting form refuses at once and never runs the closure.
        let patient = Store::open(dir.path()).unwrap(); // a 10 s lock_wait
        let started = Instant::now();
        let ran = std::cell::Cell::new(false);
        let refused = patient.try_transact(|_| {
            ran.set(true);
            Ok(())
        });
        assert!(matches!(refused, Err(Error::Locked)) && !ran.get());
        assert!(started.elapsed() < Duration::from_secs(5), "try_transact waited");
        drop(held);
        w.commit().unwrap();
        drop(w);
        assert_eq!(b.len(), 1);
    }

    /// `Err` from the closure aborts: nothing appended, nothing visible, and
    /// the handle's next transaction starts clean.
    #[test]
    fn an_aborted_transaction_writes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::create(dir.path(), Config::default()).unwrap();
        let before = fs::read(wal_path(dir.path())).unwrap();
        let r: Result<()> = store.transact(|tx| {
            tx.upsert_node(&doc("https://x.dev/a", 1))?;
            Err(Error::format("caller changed its mind"))
        });
        assert!(r.is_err());
        assert_eq!(fs::read(wal_path(dir.path())).unwrap(), before, "nothing appended");
        assert!(store.is_empty());
        store
            .transact(|tx| tx.upsert_node(&doc("https://x.dev/b", 2)))
            .expect("the next transaction commits");
        assert_eq!(urls(&store), ["https://x.dev/b"], "the aborted op never resurfaces");
    }

    /// The read-modify-write a transaction exists for: allocate the next id
    /// from what is committed. Interleaved over two handles, no id repeats.
    #[test]
    fn transactions_allocate_ids_without_collisions() {
        let dir = tempfile::tempdir().unwrap();
        let a = Store::create(dir.path(), Config::default()).unwrap();
        let b = Store::open(dir.path()).unwrap();
        let next = |store: &Store| {
            store
                .transact(|tx| {
                    let id = tx.snapshot().len();
                    let url = format!("https://x.dev/id/{id}");
                    tx.upsert_node(&doc(&url, id as u64))?;
                    Ok(id)
                })
                .unwrap()
        };
        let ids: Vec<usize> = (0..10).map(|i| next(if i % 2 == 0 { &a } else { &b })).collect();
        assert_eq!(ids, (0..10).collect::<Vec<_>>());
    }

    /// Counted, not clocked: an open takes no lock at all, and a transaction
    /// of many ops is one lock and one fsync.
    #[test]
    fn open_takes_no_lock_and_a_transaction_is_one_lock_one_fsync() {
        let dir = tempfile::tempdir().unwrap();
        drop(Store::create(dir.path(), Config::default()).unwrap());
        probe::take(&probe::LOCKS);
        let store = Store::open(dir.path()).unwrap();
        let other = Store::open(dir.path()).unwrap();
        assert_eq!(probe::take(&probe::LOCKS), 0, "open made a lock call");
        // And nothing is held: the lock is free for anyone.
        drop(lock_dir(dir.path(), Duration::ZERO).expect("the lock is free after open"));
        probe::take(&probe::LOCKS);
        probe::take(&probe::FSYNCS);
        store
            .transact(|tx| {
                for i in 0..100 {
                    tx.upsert_node(&doc(&format!("https://x.dev/{i}"), i))?;
                }
                Ok(())
            })
            .unwrap();
        assert_eq!(probe::take(&probe::LOCKS), 1, "one lock per transaction");
        assert_eq!(probe::take(&probe::FSYNCS), 1, "one fsync per transaction");
        assert_eq!(other.len(), 0);
        assert!(other.reload_if_stale().unwrap());
        assert_eq!(other.len(), 100);
    }

    /// Counted: a catch-up reads exactly the bytes appended since the
    /// handle last looked, not the log.
    #[test]
    fn a_catch_up_reads_only_the_appended_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let a = Store::create(dir.path(), Config::default()).unwrap();
        let b = Store::open(dir.path()).unwrap();
        for i in 0..50 {
            write(&a, &format!("https://x.dev/{i}"), i);
        }
        assert!(b.reload_if_stale().unwrap());
        let len = || fs::metadata(wal_path(dir.path())).unwrap().len();
        let before = len();
        write(&a, "https://x.dev/new", 99);
        let appended = len() - before;
        probe::take(&probe::TAIL_BYTES);
        write(&b, "https://x.dev/mine", 100);
        assert_eq!(probe::take(&probe::TAIL_BYTES), appended, "read only a's append");
        assert_eq!(b.len(), 52);
    }

    /// A writer that dies mid-append leaves a torn tail. Whoever commits next
    /// cuts it off under the lock before appending — its frames are never
    /// written after garbage, where replay would never reach them.
    #[test]
    fn the_next_committer_repairs_a_torn_tail() {
        use std::io::Write as _;
        let dir = tempfile::tempdir().unwrap();
        let a = Store::create(dir.path(), Config::default()).unwrap();
        let b = Store::open(dir.path()).unwrap();
        write(&a, "https://x.dev/a", 1);
        let intact = fs::metadata(wal_path(dir.path())).unwrap().len();
        // The crashed writer's half frame.
        let mut f = fs::OpenOptions::new().append(true).open(wal_path(dir.path())).unwrap();
        f.write_all(&[0x40, 0, 0, 0, 1, 2, 3]).unwrap();
        drop(f);
        write(&b, "https://x.dev/b", 2);
        let raw = fs::read(wal_path(dir.path())).unwrap();
        assert_eq!(wal::committed_len(&raw), raw.len(), "no garbage left in the log");
        assert!(raw.len() as u64 > intact);
        drop((a, b));
        let fresh = Store::open_read_only(dir.path()).unwrap();
        assert_eq!(urls(&fresh), ["https://x.dev/a", "https://x.dev/b"]);
    }
}
