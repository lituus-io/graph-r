// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! The store: a directory holding `graph.base` (immutable snapshot),
//! `graph.wal` (append-only op log), `graph.lock` (commit exclusion) and
//! `graph.wait` (the queue of writers waiting for it).
//!
//! # Concurrency model
//!
//! - **Any number of writers.** Any number of read-write handles may be open
//!   on one directory, in one process or many; opening takes no lock. What is
//!   exclusive is the *commit*: [`Writer::commit`], [`Store::transact`] and
//!   compaction take an exclusive `flock` on `graph.lock` only around the
//!   append (and its fsync) and release it straight after. A busy lock is
//!   retried with exponential backoff (250 µs doubling to 2 ms) for up to
//!   [`Config::lock_wait`], then refused with [`Error::Locked`]. Waiting is
//!   fair: a waiter holds a shared lock on `graph.wait`, and a writer that
//!   finds anyone waiting queues behind them instead of taking the commit
//!   lock first, so one writer committing back to back cannot starve the
//!   rest. `flock` is
//!   per open file description, so two handles in one process exclude each
//!   other exactly like two processes; within one handle a mutex serializes
//!   [`Writer`]s across threads.
//! - **Catch-up under the lock.** Holding the lock, a committer first
//!   compares the files' stamp (two `stat`s) with what its handle last saw.
//!   A new base (another handle compacted) is installed into a free
//!   generation slot; a grown log has just its appended records read and
//!   published, whole records only; the sequence counter and log offset
//!   advance to the file's. Only then is the new record sealed and
//!   appended, so sequence numbers stay strictly increasing across every
//!   writer. A torn record left by a writer that crashed mid-append is
//!   truncated here, by the next committer (and opportunistically at open
//!   when the lock happens to be free).
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
//! A transaction is atomic everywhere: it is one checksummed log record, so
//! a crash or power loss mid-append leaves a torn record that replay ignores
//! and the next committer cuts off — the transaction is absent whole, and
//! every earlier one present. In memory, a commit, a catch-up and a replay
//! each publish whole records with one frontier move, so no snapshot (and
//! no other process reading the log) ever sees part of a transaction.

use crate::compact;
use crate::error::{Error, Result};
use crate::format::{base, wal};
use crate::mmap::MappedFile;
use crate::overlay::{publish_records, LogArena};
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

/// One loaded generation: the mmapped base and its overlay of log records.
#[derive(Debug)]
pub(crate) struct GenInner {
    map: MappedFile,
    pub(crate) dir: base::BaseDir,
    pub(crate) log: LogArena,
}

impl GenInner {
    pub(crate) fn bytes(&self) -> &[u8] {
        self.map.bytes()
    }

    pub(crate) fn from_parts(map: MappedFile, dir: base::BaseDir) -> Self {
        Self { map, dir, log: LogArena::new() }
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
    /// `graph.base` and `graph.wal` inside `dir`, joined once: a commit's
    /// catch-up and append then build no path.
    pub(crate) base_file: PathBuf,
    pub(crate) wal_file: PathBuf,
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

/// Open one of `dir`'s lock files (`name` is [`LOCK_FILE`] or
/// [`WAIT_FILE`]). The path is fixed, and a lock file that is not a regular
/// file — a symlink planted to make the store create or lock a file
/// elsewhere — is refused.
fn open_guarded(dir: &Path, name: &str) -> Result<fs::File> {
    let path = dir.join(name);
    match fs::symlink_metadata(&path) {
        Ok(md) if !md.file_type().is_file() => {
            return Err(Error::corrupt(format!("{name} is not a regular file")));
        }
        _ => {}
    }
    Ok(fs::OpenOptions::new().create(true).truncate(false).write(true).open(path)?)
}

/// The pause before the first retry of a held commit lock, and the longest
/// pause between retries. A waiter retries at least every [`NAP_MAX`], which
/// is also how long a writer queued behind waiters stands back.
const NAP_MIN: Duration = Duration::from_micros(250);
const NAP_MAX: Duration = Duration::from_millis(2);

/// One attempt on the commit lock.
fn try_commit_lock(lock: &fs::File) -> Result<bool> {
    #[cfg(test)]
    probe::bump(&probe::LOCKS, 1);
    match lock.try_lock() {
        Ok(()) => Ok(true),
        Err(fs::TryLockError::WouldBlock) => Ok(false),
        Err(fs::TryLockError::Error(e)) => Err(e.into()),
    }
}

/// Take the commit lock on `lock`, waiting up to `wait`; `Error::Locked`
/// past it.
///
/// Fair to waiters. A writer that has to wait announces itself with a shared
/// lock on `turnstile` (`graph.wait`) for as long as it waits. A writer that
/// finds anyone announced does not take the commit lock ahead of them, even
/// when it is free: it announces itself too and stands back one full pause,
/// in which every earlier waiter retries. Without this a writer committing
/// back to back holds the lock through each fsync and frees it for
/// microseconds, a gap a polling waiter almost never lands in, and the
/// waiter times out. A no-wait call (`wait` zero) never jumps the queue
/// either. Uncontended, this is one probe of `turnstile` and one attempt on
/// the commit lock; nothing blocks, and nothing spins.
fn lock_within(lock: &fs::File, turnstile: &fs::File, wait: Duration) -> Result<()> {
    #[cfg(test)]
    probe::bump(&probe::TURNSTILE, 1);
    let queued = match turnstile.try_lock() {
        Ok(()) => {
            turnstile.unlock()?;
            if try_commit_lock(lock)? {
                return Ok(());
            }
            false
        }
        Err(fs::TryLockError::WouldBlock) => true,
        Err(fs::TryLockError::Error(e)) => return Err(e.into()),
    };
    if wait.is_zero() {
        return Err(Error::Locked);
    }
    let start = Instant::now();
    let left = || wait.saturating_sub(start.elapsed());
    // Announce. Only another writer's probe above holds `turnstile`
    // exclusively, for two calls, so this lands within microseconds.
    loop {
        match turnstile.try_lock_shared() {
            Ok(()) => break,
            Err(fs::TryLockError::WouldBlock) => {}
            Err(fs::TryLockError::Error(e)) => return Err(e.into()),
        }
        if left().is_zero() {
            return Err(Error::Locked);
        }
        std::thread::sleep(NAP_MIN.min(left()));
    }
    let mut nap = if queued { NAP_MAX } else { NAP_MIN };
    let won = loop {
        let l = left();
        if l.is_zero() {
            break Err(Error::Locked);
        }
        std::thread::sleep(nap.min(l));
        nap = (nap * 2).min(NAP_MAX);
        match try_commit_lock(lock) {
            Ok(false) => {}
            done => break done.map(|_| ()),
        }
    };
    let _ = turnstile.unlock();
    won
}

/// Take `dir`'s commit lock on fresh descriptors.
fn lock_dir(dir: &Path, wait: Duration) -> Result<Held> {
    let file = open_guarded(dir, LOCK_FILE)?;
    lock_within(&file, &open_guarded(dir, WAIT_FILE)?, wait)?;
    Ok(Held(file))
}

/// Counters for the counted (never clocked) performance tests: per thread,
/// so parallel tests do not see each other's work.
#[cfg(test)]
pub(crate) mod probe {
    use std::cell::Cell;
    use std::thread::LocalKey;
    thread_local! {
        /// Commit-lock attempts (`try_lock` calls).
        pub(crate) static LOCKS: Cell<u64> = const { Cell::new(0) };
        /// Lock acquisitions begun (one probe of `graph.wait` each).
        pub(crate) static TURNSTILE: Cell<u64> = const { Cell::new(0) };
        /// Log fsyncs by commits.
        pub(crate) static FSYNCS: Cell<u64> = const { Cell::new(0) };
        /// Log writes by commits.
        pub(crate) static WRITES: Cell<u64> = const { Cell::new(0) };
        /// Log bytes read by tail catch-ups.
        pub(crate) static TAIL_BYTES: Cell<u64> = const { Cell::new(0) };
        /// Make every overlay reservation fail, as a full arena would.
        pub(crate) static FULL: Cell<u64> = const { Cell::new(0) };
        /// Fail the next appends after their bytes are written and synced.
        pub(crate) static FAIL_APPEND: Cell<u64> = const { Cell::new(0) };
        /// … and leave those bytes in the log (the cut-back fails too), as
        /// a failing disk can.
        pub(crate) static STUCK: Cell<u64> = const { Cell::new(0) };
    }
    pub(crate) fn full() -> bool {
        FULL.with(Cell::get) != 0
    }
    pub(crate) fn stuck() -> bool {
        STUCK.with(Cell::get) != 0
    }
    /// Consume one injected append failure, if any is armed.
    pub(crate) fn fail_append() -> bool {
        FAIL_APPEND.with(|x| x.get().checked_sub(1).map(|n| x.set(n)).is_some())
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
        Self::of(&base_path(dir), &wal_path(dir))
    }

    /// [`Stamp::read`] on paths already joined.
    fn of(base: &Path, wal: &Path) -> Option<Self> {
        let base = FileId::of(&fs::metadata(base).ok()?);
        let wal = match fs::metadata(wal) {
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
/// File name of the commit lock's wait queue inside a store directory: each
/// writer waiting for the commit lock holds a shared lock on it.
pub const WAIT_FILE: &str = "graph.wait";

/// Paths inside a store directory.
pub(crate) fn base_path(dir: &Path) -> PathBuf {
    dir.join(BASE_FILE)
}
pub(crate) fn wal_path(dir: &Path) -> PathBuf {
    dir.join(WAL_FILE)
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
        let built = build_generation(MappedFile::from_bytes(base), wal.to_vec())?;
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
            Some(b) => self
                .install_or_apply(MappedFile::from_bytes(b), wal.to_vec())?
                .map(|a| a.changed)
                .ok_or_else(newer_image),
            None => match self.apply_log(wal)? {
                Some((changed, _)) => Ok(changed),
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
            base_file: base_path(&dir),
            wal_file: wal_path(&dir),
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
            None => open_guarded(&self.dir, LOCK_FILE)?,
        };
        if state.wait_file.is_none() {
            match open_guarded(&self.dir, WAIT_FILE) {
                Ok(t) => state.wait_file = Some(t),
                Err(e) => {
                    state.lock_file = Some(file);
                    return Err(e);
                }
            }
        }
        let turnstile = state.wait_file.as_ref().expect("opened above");
        match lock_within(&file, turnstile, wait) {
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
        let next_seq = g.dir.header.wal_applied_seq.max(g.log.last_seq()) + 1;
        state.adopt(next_seq, g.log.len(), state.seen.base_gen);
        if caught == Caught::Full {
            let over = state.ops_since_compact >= self.cfg.compact_after_ops
                || state.wal_bytes >= self.cfg.compact_after_wal_bytes;
            self.due.store(over && !self.compact_inline.load(Ordering::Relaxed), Ordering::Release);
        }
        Ok(changed)
    }

    /// Cut a torn tail back to the last whole record, or give a missing,
    /// empty or older log a header for the served generation. Under the
    /// commit lock only. A log of another version, with a bad header or with
    /// a corrupt record never gets here: reading it failed, and nothing was
    /// written.
    fn repair(&self, state: &mut WriteState) -> Result<()> {
        let seen = &mut state.seen;
        let file = &self.wal_file;
        if !seen.log_current {
            atomic_write(file, &wal::encode_header(seen.base_gen, now_ms()))?;
            *seen = Seen {
                stamp: Stamp::of(&self.base_file, file),
                wal_pos: wal::HEADER_LEN as u64,
                log_current: true,
                base_gen: seen.base_gen,
            };
            state.drop_wal_handle();
            return Ok(());
        }
        if let Some((ident, len)) = seen.stamp.and_then(|s| s.wal) {
            if len > seen.wal_pos {
                let f = fs::OpenOptions::new().write(true).open(file)?;
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
        let now = Stamp::of(&self.base_file, &self.wal_file);
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

    /// Same base file: read the log from `pos` (a record boundary already
    /// consumed) and publish the whole records found there; a partial one
    /// (another writer mid-append) is left for the next look. Returns
    /// whether anything became visible and the new consumed offset, always
    /// a record boundary.
    fn reload_wal_tail(&self, pos: u64) -> Result<(bool, u64)> {
        use std::io::{Read as _, Seek as _, SeekFrom};
        let mut tail = Vec::new();
        match fs::File::open(&self.wal_file) {
            Ok(mut f) => {
                f.seek(SeekFrom::Start(pos))?;
                f.read_to_end(&mut tail)?;
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok((false, pos)),
            Err(e) => return Err(e.into()),
        }
        #[cfg(test)]
        probe::bump(&probe::TAIL_BYTES, tail.len() as u64);
        let scanned = wal::scan(&tail, 0, None);
        if let Some(e) = scanned.corrupt {
            return Err(e);
        }
        tail.truncate(scanned.end);
        Ok((self.publish_new(tail, 0)?, pos + scanned.end as u64))
    }

    /// Publish the ops of `seg[from..]` (whole records, validated) that the
    /// served generation has not seen — sequence above both the base's
    /// folded ops and the overlay's last — moving `seg` in as one segment.
    fn publish_new(&self, seg: Vec<u8>, from: usize) -> Result<bool> {
        let cur = self.current.load(Ordering::Acquire);
        let guard = self.gens[cur].inner.try_read().map_err(|_| Error::busy("slot busy"))?;
        let Some(g) = guard.as_ref() else { return Ok(false) };
        publish_above(&g.log, g.dir.header.wal_applied_seq.max(g.log.last_seq()), seg, from)
    }

    /// Publish the records of a log image that the current generation has
    /// not seen, copying only those (the image is borrowed). `None` when the
    /// image belongs to another generation; otherwise whether anything
    /// became visible and the image's committed length.
    fn apply_log(&self, raw: &[u8]) -> Result<Option<(bool, usize)>> {
        if raw.is_empty() {
            return Ok(Some((false, wal::HEADER_LEN)));
        }
        if wal::parse_header(raw)? != self.generation() {
            return Ok(None);
        }
        let replayed = wal::replay(raw)?;
        let served = self.served_seq()?;
        let start = replayed.records().find(|r| r.last_seq() > served).map(|r| r.offset());
        let changed = match start {
            Some(at) => self.publish_new(raw[at..replayed.good_len].to_vec(), 0)?,
            None => false,
        };
        Ok(Some((changed, replayed.good_len)))
    }

    /// Map the base and read the log, retrying across a compaction caught
    /// between its renames (under the commit lock there is none: a newer log
    /// is damage). Returns whether anything changed, the log offset consumed,
    /// whether the log belongs to the base and the base's generation.
    fn reload_full(&self, locked: bool) -> Result<(bool, u64, bool, u64)> {
        for _ in 0..RACE_RETRIES {
            let map = MappedFile::open(&self.base_file)?;
            let base_gen = base::Header::parse(map.bytes())?.generation;
            let raw = match fs::read(&self.wal_file) {
                Ok(raw) => raw,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
                Err(e) => return Err(e.into()),
            };
            if let Some(a) = self.install_or_apply(map, raw)? {
                return Ok((a.changed, a.pos as u64, a.current, base_gen));
            }
            if locked {
                return Err(Error::corrupt("wal is newer than base"));
            }
        }
        Err(Error::busy("compaction in progress; retry"))
    }

    /// Same generation as served: publish new records. Otherwise rebuild
    /// the generation from `map` + `raw` and install it into a free ring
    /// slot. `None` when the log is newer than `map` (a compaction between
    /// the reads): the caller re-reads. A log that is unreadable — another
    /// version, a bad header, a corrupt record — is an error here, before
    /// anything is decided about repairing it.
    fn install_or_apply(&self, map: MappedFile, raw: Vec<u8>) -> Result<Option<Applied>> {
        let cur_gen = self.generation();
        let new_gen = base::Header::parse(map.bytes())?.generation;
        if new_gen == cur_gen {
            // Generations are never reused, so the same number is the same
            // base: only the log can have grown.
            return Ok(match self.apply_log(&raw)? {
                Some((changed, pos)) => Some(Applied { changed, pos, current: !raw.is_empty() }),
                // Another generation's log: newer means a compaction landed
                // between the reads (re-read); older is already folded in.
                None if wal::parse_header(&raw)? > new_gen => None,
                None => Some(Applied { changed: false, pos: wal::HEADER_LEN, current: false }),
            });
        }
        let built = build_generation(map, raw)?;
        let pos = built.good_len;
        let current = matches!(built.log, LogState::Current { .. });
        if built.log == LogState::Newer {
            return Ok(None);
        }
        let folded = built.inner.dir.header.wal_applied_seq;
        match self.install(built.inner, new_gen) {
            None => Ok(Some(Applied { changed: true, pos, current })),
            // Every other slot is pinned by a snapshot. If the served
            // generation already holds every op the new base folded (this
            // handle's own compaction, typically), keep serving it and
            // publish the new base's log on top: the same state, one
            // generation behind on disk. Otherwise there is no way to serve
            // it yet.
            Some(mut inner) => {
                if folded > self.served_seq()? {
                    return Err(Error::busy("all generation slots pinned by snapshots"));
                }
                let changed = match inner.log.take_first_segment() {
                    Some(seg) => self.publish_new(seg, wal::HEADER_LEN)?,
                    None => false,
                };
                Ok(Some(Applied { changed, pos, current }))
            }
        }
    }

    /// The last sequence number the served generation holds.
    fn served_seq(&self) -> Result<u64> {
        let cur = self.current.load(Ordering::Acquire);
        let guard = self.gens[cur].inner.try_read().map_err(|_| Error::busy("slot busy"))?;
        Ok(guard.as_ref().map_or(0, |g| g.dir.header.wal_applied_seq.max(g.log.last_seq())))
    }

    /// Install a loaded generation into a free ring slot and serve it. The
    /// one install path: catch-ups and compaction both come through here.
    /// `Some` (the generation, handed back) when every other slot is pinned
    /// by a snapshot.
    #[must_use]
    pub(crate) fn install(&self, inner: GenInner, generation: u64) -> Option<GenInner> {
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
                return None;
            }
        }
        Some(inner)
    }
}

/// Publish the ops of `seg[from..]` (whole records, validated) with
/// sequence numbers above `served` into `log`, moving `seg` in as one
/// segment. Whether anything became visible.
fn publish_above(log: &LogArena, served: u64, seg: Vec<u8>, from: usize) -> Result<bool> {
    let new: usize = wal::Records::trusted(&seg, from, seg.len())
        .map(|r| match r.last_seq().checked_sub(served) {
            Some(n) if n > 0 => n.min(u64::from(r.op_count())) as usize,
            _ => 0,
        })
        .sum();
    if new == 0 {
        return Ok(false);
    }
    if !log.reserve(new, seg.len()) {
        return Err(Error::busy("overlay arena full"));
    }
    if new == wal::Records::trusted(&seg, from, seg.len()).map(|r| r.op_count() as usize).sum() {
        publish_records(log, seg, from);
    } else {
        log.publish(seg, move |s| {
            wal::Records::trusted(s, from, s.len())
                .flat_map(|r| r.ops())
                .filter(move |&(seq, _, _)| seq > served)
                .map(|(seq, off, _)| (seq, off))
        });
    }
    Ok(true)
}

/// What a full re-read made of the log.
struct Applied {
    /// Anything new became visible.
    changed: bool,
    /// The log offset consumed (a record boundary).
    pos: usize,
    /// The log's header names the base's generation.
    current: bool,
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

/// Validate `map` as a base and replay `raw` against it, moving the log's
/// committed records into the new generation's overlay as one segment. Pure:
/// shared by the file-backed and in-memory (image) paths. A log of another
/// version, with a bad header or holding a corrupt record is an error, so
/// nothing downstream ever repairs (rewrites) it.
fn build_generation(map: MappedFile, mut raw: Vec<u8>) -> Result<Built> {
    let bdir = base::BaseDir::parse(map.bytes())?;
    let header = bdir.header;
    let inner = GenInner { map, dir: bdir, log: LogArena::new() };
    let folded = Built {
        inner,
        next_seq: header.wal_applied_seq + 1,
        replayed: 0,
        good_len: 0,
        log: LogState::Empty,
    };
    if raw.is_empty() {
        return Ok(folded);
    }
    let generation = wal::parse_header(&raw)?;
    if generation > header.generation {
        return Ok(Built { log: LogState::Newer, ..folded });
    }
    if generation < header.generation {
        return Ok(Built { good_len: wal::HEADER_LEN, log: LogState::Stale, ..folded });
    }
    let scanned = wal::scan(&raw, wal::HEADER_LEN, None);
    if let Some(e) = scanned.corrupt {
        return Err(e);
    }
    let good_len = scanned.end;
    let torn = good_len < raw.len();
    let applied = header.wal_applied_seq;
    let next_seq = scanned.last_seq.map_or(applied, |l| l.max(applied)) + 1;
    raw.truncate(good_len);
    publish_above(&folded.inner.log, applied, raw, wal::HEADER_LEN)
        .map_err(|_| Error::busy("overlay arena full during replay"))?;
    let replayed = folded.inner.log.len();
    Ok(Built { next_seq, replayed, good_len, log: LogState::Current { torn }, ..folded })
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
        let built = build_generation(map, raw)?;
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

    /// Every op's sequence number in a log image, in log order.
    fn seqs(raw: &[u8]) -> Vec<u64> {
        wal::replay(raw).unwrap().records().flat_map(|r| r.first_seq()..=r.last_seq()).collect()
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
        let seqs = seqs(&raw);
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
        probe::take(&probe::TURNSTILE);
        store
            .transact(|tx| {
                for i in 0..100 {
                    tx.upsert_node(&doc(&format!("https://x.dev/{i}"), i))?;
                }
                Ok(())
            })
            .unwrap();
        assert_eq!(probe::take(&probe::LOCKS), 1, "one lock per transaction");
        assert_eq!(probe::take(&probe::TURNSTILE), 1, "one wait-queue probe per transaction");
        assert_eq!(probe::take(&probe::FSYNCS), 1, "one fsync per transaction");
        assert_eq!(other.len(), 0);
        assert!(other.reload_if_stale().unwrap());
        assert_eq!(other.len(), 100);
    }

    /// A writer never takes the commit lock ahead of an announced waiter,
    /// even a free one: a no-wait call is refused, and a waiting call stands
    /// back one pause before its single attempt. With the queue empty, a
    /// no-wait call goes straight through.
    #[test]
    fn a_writer_queues_behind_an_announced_waiter() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::create(dir.path(), Config::default()).unwrap();
        let waiter = open_guarded(dir.path(), WAIT_FILE).unwrap();
        waiter.try_lock_shared().unwrap();
        let r = store.try_transact(|tx| tx.upsert_node(&doc("https://x.dev/a", 1)));
        assert!(matches!(r, Err(Error::Locked)), "jumped the queue: {r:?}");
        probe::take(&probe::LOCKS);
        store.transact(|tx| tx.upsert_node(&doc("https://x.dev/a", 1))).unwrap();
        assert_eq!(probe::take(&probe::LOCKS), 1, "tried the lock before standing back");
        waiter.unlock().unwrap();
        store.try_transact(|tx| tx.upsert_node(&doc("https://x.dev/b", 2))).unwrap();
        assert_eq!(store.len(), 2);
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

    // ---- a commit that cannot complete ------------------------------------

    fn wal_len(dir: &Path) -> u64 {
        fs::metadata(wal_path(dir)).unwrap().len()
    }

    /// The overlay is checked for room before anything is written: a full
    /// one refuses with `Busy`, the log untouched and the ops still staged.
    /// (Room checked after a durable append is how 0.5.1 could write a
    /// commit it never accounted for, and reuse its sequence numbers.)
    #[test]
    fn a_full_overlay_refuses_before_writing_and_keeps_the_ops() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::create(dir.path(), Config::default()).unwrap();
        write(&store, "https://x.dev/a", 1);
        let before = wal_len(dir.path());
        let mut w = store.writer().unwrap();
        w.upsert_node(&doc("https://x.dev/b", 2)).unwrap();
        probe::FULL.with(|f| f.set(1));
        let r = w.commit();
        probe::FULL.with(|f| f.set(0));
        assert!(matches!(r, Err(Error::Busy { .. })), "{r:?}");
        assert_eq!(wal_len(dir.path()), before, "a refused commit wrote to the log");
        w.commit().expect("the staged op commits once there is room");
        drop(w);
        assert_eq!(urls(&store), ["https://x.dev/a", "https://x.dev/b"]);
    }

    /// A failed append is cut back off the log and keeps the writer's ops
    /// staged: a retry commits them (0.5.1 dropped them).
    #[test]
    fn a_failed_append_keeps_the_ops() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::create(dir.path(), Config::default()).unwrap();
        write(&store, "https://x.dev/a", 1);
        let before = wal_len(dir.path());
        let mut w = store.writer().unwrap();
        w.upsert_node(&doc("https://x.dev/b", 2)).unwrap();
        probe::FAIL_APPEND.with(|f| f.set(1));
        assert!(matches!(w.commit(), Err(Error::Io { .. })));
        assert_eq!(wal_len(dir.path()), before, "the failed append was cut back");
        w.commit().expect("the kept op commits on retry");
        drop(w);
        assert_eq!(urls(&store), ["https://x.dev/a", "https://x.dev/b"]);
    }

    /// The worst case: the append fails after its bytes reached the log and
    /// they cannot be cut back off. The next commit re-reads the log rather
    /// than trust the handle's bookkeeping, so no sequence number is ever
    /// written twice and the store reopens. (A durable append the handle did
    /// not account for is how 0.5.1 reused sequences and bricked a store.)
    #[test]
    fn a_failed_append_that_stays_in_the_log_never_reuses_a_sequence() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::create(dir.path(), Config::default()).unwrap();
        write(&store, "https://x.dev/a", 1);
        let mut w = store.writer().unwrap();
        w.upsert_node(&doc("https://x.dev/b", 2)).unwrap();
        probe::FAIL_APPEND.with(|f| f.set(1));
        probe::STUCK.with(|f| f.set(1));
        let failed = w.commit();
        probe::STUCK.with(|f| f.set(0));
        assert!(matches!(failed, Err(Error::Io { .. })));
        w.commit().expect("the kept op commits on retry");
        w.upsert_node(&doc("https://x.dev/c", 3)).unwrap();
        w.commit().unwrap();
        drop(w);
        drop(store);
        let raw = fs::read(wal_path(dir.path())).unwrap();
        wal::replay(&raw).expect("no sequence regression");
        let seqs = seqs(&raw);
        assert!(seqs.windows(2).all(|p| p[0] < p[1]), "sequences {seqs:?}");
        let reopened = Store::open(dir.path()).expect("the store reopens");
        assert_eq!(urls(&reopened), ["https://x.dev/a", "https://x.dev/b", "https://x.dev/c"]);
    }

    /// A transaction is all or nothing for its caller too: when its append
    /// fails, its ops are dropped (a retry runs the closure again), so they
    /// never ride along with the next transaction.
    #[test]
    fn a_failed_transaction_leaves_nothing_of_its_own_staged() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::create(dir.path(), Config::default()).unwrap();
        probe::FAIL_APPEND.with(|f| f.set(1));
        let r = store.transact(|tx| tx.upsert_node(&doc("https://x.dev/a", 1)));
        assert!(matches!(r, Err(Error::Io { .. })), "{r:?}");
        store.transact(|tx| tx.upsert_node(&doc("https://x.dev/b", 2))).unwrap();
        assert_eq!(urls(&store), ["https://x.dev/b"]);
    }

    /// A commit is durable and visible before any inline compaction runs, so
    /// a compaction that cannot complete (here: every ring slot pinned) must
    /// not fail it. It leaves the compaction due instead.
    #[test]
    fn an_inline_compaction_that_cannot_install_leaves_the_commit_ok_and_due() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = Config { compact_after_ops: 1, ..Config::default() };
        let store = Store::create(dir.path(), cfg).unwrap();
        let mut pins = vec![store.snapshot()];
        for (i, url) in ["https://x.dev/a", "https://x.dev/b", "https://x.dev/c"].iter().enumerate()
        {
            write(&store, url, i as u64);
            pins.push(store.snapshot());
        }
        let mut w = store.writer().unwrap();
        w.upsert_node(&doc("https://x.dev/d", 4)).unwrap();
        w.commit().expect("the commit itself succeeded");
        drop(w);
        assert!(store.compaction_due(), "the compaction is left due");
        assert_eq!(store.len(), 4, "and the commit is visible");
        drop(pins);
        store.compact().unwrap();
        assert!(!store.compaction_due());
        assert_eq!(store.len(), 4);
    }
}
