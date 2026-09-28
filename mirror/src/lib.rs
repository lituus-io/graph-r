// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>
//
//! # graph-r-mirror
//!
//! Keep a [graph-r](graph_r) store directory in sync with an object store —
//! Google Cloud Storage, a local directory, or memory — through
//! [objstore-r](objstore_r)'s [`ObjectStore`] trait.
//!
//! graph-r stays exactly as it is: synchronous, file-based, mmapped. The
//! mirror moves *its files*, byte for byte, so a store can live on tmpfs on a
//! stateless instance and be durable in a bucket:
//!
//! ```text
//! {prefix}HEAD                                  manifest, compare-and-swap only
//! {prefix}base/{generation:016x}.base           graph.base, create-only
//! {prefix}wal/{generation:016x}/{a:016x}-{b:016x}.seg   graph.wal bytes [a, b), create-only
//! ```
//!
//! - [`Mirror::pull_into`] rebuilds `graph.base` and `graph.wal` in a
//!   directory (every byte checksummed, the base validated end to end); open
//!   it with the unchanged [`graph_r::Store::open`].
//! - [`Mirror::publish`] ships what the store committed since the last
//!   publish: only the new WAL frames while the generation is unchanged, the
//!   new base after a compaction. Long segment runs are coalesced. Uploads
//!   are create-only, so a retried publish reuses what already landed.
//! - [`Mirror::refresh_into`] keeps a read-only replica current: one
//!   metadata request when nothing changed, appended frames when the log
//!   grew, a new base after a compaction; then
//!   [`graph_r::Store::reload_if_stale`].
//! - **One writer, enforced remotely.** `HEAD` is only ever written with a
//!   generation precondition, so of two instances publishing from the same
//!   state exactly one wins and the other gets [`Error::Fenced`]. An optional
//!   lease ([`Mirror::acquire_lease`]) says *who* should be writing.
//!
//! Futures are unboxed (`impl Future` over borrowed state); no `Arc`, no
//! `dyn`. The internal `Mutex` is never held across an `.await`.

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations, rust_2018_idioms, unreachable_pub)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(
    clippy::module_name_repetitions,
    clippy::missing_errors_doc,
    clippy::cast_possible_truncation
)]

mod fsio;
mod manifest;

pub use manifest::{Lease, Segment};
pub use objstore_r::{self, ObjectStore};

use graph_r::format::{base, wal};
use graph_r::store::{now_ms, BASE_FILE, WAL_FILE};
use graph_r::Store;
use manifest::Manifest;
use objstore_r::{Bytes, Cond};
use std::future::Future;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::{Mutex, MutexGuard, PoisonError};
use xxhash_rust::xxh3::xxh3_64;

/// Coalesce the log into one segment once a generation has this many.
pub const COALESCE_SEGMENTS: usize = 32;

#[doc(hidden)]
pub use manifest::fuzz as fuzz_manifest;

/// Result alias.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// What can go wrong.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The local store failed.
    #[error(transparent)]
    Graph(#[from] graph_r::Error),
    /// The object store failed.
    #[error("object store: {0}")]
    Store(#[from] objstore_r::Error),
    /// Another writer published first, or the local store diverges from the
    /// published one. Stop writing; reopen read-only or pull again.
    #[error("fenced: {reason}")]
    Fenced {
        /// Why.
        reason: &'static str,
    },
    /// Someone else holds an unexpired writer lease.
    #[error("lease held by {holder} until {expires_ms}")]
    Leased {
        /// The holder.
        holder: String,
        /// When it lapses, Unix ms.
        expires_ms: u64,
    },
    /// Published data failed verification.
    #[error("corrupt remote store: {message}")]
    Corrupt {
        /// What was wrong.
        message: String,
    },
    /// Local I/O.
    #[error("i/o: {0}")]
    Io(#[from] std::io::Error),
}

fn corrupt(message: impl Into<String>) -> Error {
    Error::Corrupt { message: message.into() }
}

/// What [`Mirror::pull_into`] rebuilt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Pulled {
    /// The graph-r generation.
    pub generation: u64,
    /// Bytes written (base + log).
    pub bytes: u64,
}

/// What [`Mirror::publish`] did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Published {
    /// The published generation.
    pub generation: u64,
    /// Published log length.
    pub wal_len: u64,
    /// Bytes uploaded by this call (0: nothing new).
    pub uploaded: u64,
    /// A new base was uploaded (the store compacted since the last publish).
    pub new_base: bool,
}

/// The published state as last read or written.
#[derive(Clone, Debug)]
struct Remote {
    /// Object generation of `HEAD` (the CAS token).
    head_gen: u64,
    manifest: Manifest,
    /// The local directory is known to hold exactly the published bytes
    /// (after a pull, a refresh or our own publish).
    local_verified: bool,
}

#[derive(Debug)]
struct Holder {
    name: String,
    ttl_ms: u64,
}

#[derive(Debug, Default)]
struct State {
    remote: Option<Remote>,
    holder: Option<Holder>,
}

/// The local cut a publish ships, taken under the store's writer lock.
enum Cut {
    Nothing { generation: u64, wal_len: u64 },
    Tail { generation: u64, start: u64, frames: Vec<u8> },
    Coalesce { generation: u64, wal: Vec<u8> },
    Full { generation: u64, base: Vec<u8>, wal: Vec<u8> },
}

/// A store directory ⇄ object store link. See the crate docs.
#[derive(Debug)]
pub struct Mirror<'o, O: ObjectStore> {
    objects: &'o O,
    prefix: String,
    state: Mutex<State>,
}

impl<'o, O: ObjectStore> Mirror<'o, O> {
    /// Mirror under `prefix` (empty, or a key prefix such as `"kb/runbooks"`;
    /// a trailing `/` is implied).
    pub fn new(objects: &'o O, prefix: &str) -> Result<Self> {
        let prefix = prefix.trim_end_matches('/');
        let prefix = if prefix.is_empty() { String::new() } else { format!("{prefix}/") };
        if !prefix.is_empty() {
            objstore_r::Key::parse(&format!("{prefix}HEAD"))?;
        }
        Ok(Self { objects, prefix, state: Mutex::new(State::default()) })
    }

    /// The normalized prefix (empty or ending in `/`).
    #[must_use]
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// The lease recorded in the last `HEAD` this mirror saw.
    #[must_use]
    pub fn lease(&self) -> Option<Lease> {
        self.lock().remote.as_ref().and_then(|r| r.manifest.lease.clone())
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn head_key(&self) -> String {
        format!("{}HEAD", self.prefix)
    }
    fn base_key(&self, generation: u64) -> String {
        format!("{}base/{generation:016x}.base", self.prefix)
    }
    fn gen_dir(&self, generation: u64) -> String {
        format!("{}wal/{generation:016x}/", self.prefix)
    }
    fn seg_key(&self, generation: u64, s: &Segment) -> String {
        format!("{}{:016x}-{:016x}.seg", self.gen_dir(generation), s.start, s.end())
    }

    /// Read `HEAD`; keeps the verified flag when it is unchanged.
    async fn fetch_remote(&self) -> Result<Option<Remote>> {
        let Some(obj) = self.objects.get(&self.head_key()).await? else {
            self.lock().remote = None;
            return Ok(None);
        };
        let manifest = Manifest::decode(&obj.bytes)?;
        let mut st = self.lock();
        let local_verified =
            st.remote.as_ref().is_some_and(|r| r.local_verified && r.head_gen == obj.generation);
        let remote = Remote { head_gen: obj.generation, manifest, local_verified };
        st.remote = Some(remote.clone());
        Ok(Some(remote))
    }

    fn remember(&self, remote: Option<Remote>) {
        self.lock().remote = remote;
    }

    /// Write `next` as `HEAD` if it still is `prev`. A lost response (our
    /// bytes landed but the reply did not) is recognised by reading back.
    async fn cas_head(&self, prev: Option<&Remote>, next: &Manifest) -> Result<u64> {
        let key = self.head_key();
        let body = Bytes::from(next.encode());
        let cond = prev.map_or(Cond::CreateOnly, |r| Cond::IfGeneration(r.head_gen));
        match self.objects.put(&key, body.clone(), cond).await {
            Ok(g) => Ok(g),
            Err(objstore_r::Error::Conflict | objstore_r::Error::PreconditionFailed) => {
                match self.objects.get(&key).await? {
                    Some(o) if o.bytes == body => Ok(o.generation),
                    _ => {
                        self.remember(None);
                        Err(Error::Fenced { reason: "another writer published first" })
                    }
                }
            }
            Err(e) => {
                // Outcome unknown: re-read HEAD before the next attempt.
                self.remember(None);
                Err(e.into())
            }
        }
    }

    /// Upload an immutable object; an existing one with the same checksum
    /// (a retried publish) is accepted, a different one fences.
    async fn put_immutable(&self, key: &str, body: Bytes, sum: u64) -> Result<()> {
        match self.objects.put(key, body, Cond::CreateOnly).await {
            Ok(_) => Ok(()),
            Err(objstore_r::Error::Conflict) => match self.objects.get(key).await? {
                Some(o) if xxh3_64(&o.bytes) == sum => Ok(()),
                _ => Err(Error::Fenced { reason: "an immutable object already holds other bytes" }),
            },
            Err(e) => Err(e.into()),
        }
    }

    async fn fetch_verified(&self, key: &str, len: u64, sum: u64) -> Result<Bytes> {
        let obj =
            self.objects.get(key).await?.ok_or_else(|| corrupt(format!("{key} is missing")))?;
        if obj.bytes.len() as u64 != len || xxh3_64(&obj.bytes) != sum {
            return Err(corrupt(format!("{key} does not match the manifest")));
        }
        Ok(obj.bytes)
    }

    /// Download and verify everything `m` names.
    async fn fetch_all(&self, m: &Manifest) -> Result<(Bytes, Vec<Bytes>)> {
        let base =
            self.fetch_verified(&self.base_key(m.generation), m.base_len, m.base_sum).await?;
        let dir = base::BaseDir::parse(&base).map_err(|e| corrupt(format!("base: {e}")))?;
        if dir.header.generation != m.generation {
            return Err(corrupt("base generation differs from the manifest"));
        }
        let mut segs = Vec::with_capacity(m.segments.len());
        for s in &m.segments {
            let b = self.fetch_verified(&self.seg_key(m.generation, s), s.len, s.sum).await?;
            check_framing(&b, s.start, m.generation)?;
            segs.push(b);
        }
        Ok((base, segs))
    }

    /// Rebuild `graph.base` and `graph.wal` in `dir` from the published
    /// store, then open it with [`graph_r::Store::open`]. `None` (and `dir`
    /// untouched) when nothing has been published. Refused with
    /// `graph_r::Error::Locked` while a writable store has `dir` open.
    pub fn pull_into<'a>(
        &'a self,
        dir: &'a Path,
    ) -> impl Future<Output = Result<Option<Pulled>>> + 'a {
        async move {
            let Some(remote) = self.fetch_remote().await? else { return Ok(None) };
            if remote.manifest.generation == 0 {
                return Ok(None);
            }
            let _lock = fsio::lock_dir(dir)?;
            let (base, segs) = self.fetch_all(&remote.manifest).await?;
            write_store(dir, &base, &segs)?;
            let bytes = base.len() as u64 + remote.manifest.wal_len();
            let generation = remote.manifest.generation;
            self.remember(Some(Remote { local_verified: true, ..remote }));
            Ok(Some(Pulled { generation, bytes }))
        }
    }

    /// Keep a read-only replica current: `store` must have been opened with
    /// [`graph_r::Store::open_read_only`] on a directory this mirror pulled.
    /// Returns `true` when anything new became visible.
    pub fn refresh_into<'a>(&'a self, store: &'a Store) -> impl Future<Output = Result<bool>> + 'a {
        async move {
            let cached = self.lock().remote.clone();
            let meta = self.objects.head(&self.head_key()).await?;
            match (&meta, &cached) {
                (None, _) => return Ok(false),
                (Some(m), Some(c)) if m.generation == c.head_gen && c.local_verified => {
                    return Ok(false)
                }
                _ => {}
            }
            let Some(remote) = self.fetch_remote().await? else { return Ok(false) };
            let m = &remote.manifest;
            if m.generation == 0 {
                return Ok(false);
            }
            let dir = store.dir();
            let _lock = fsio::lock_dir(dir)?;
            let wal_path = dir.join(WAL_FILE);
            let local_len = std::fs::metadata(&wal_path).map(|md| md.len()).ok();
            let appendable = cached.as_ref().filter(|c| {
                c.local_verified
                    && c.manifest.generation == m.generation
                    && c.manifest.base_sum == m.base_sum
                    && m.wal_len() >= c.manifest.wal_len()
                    && local_len == Some(c.manifest.wal_len())
            });
            if let Some(c) = appendable {
                let have = c.manifest.wal_len();
                let mut tail = Vec::new();
                for s in m.segments.iter().filter(|s| s.end() > have) {
                    let b =
                        self.fetch_verified(&self.seg_key(m.generation, s), s.len, s.sum).await?;
                    check_framing(&b, s.start, m.generation)?;
                    let skip = usize::try_from(have.saturating_sub(s.start)).unwrap_or(usize::MAX);
                    tail.push(b.slice(skip.min(b.len())..));
                }
                let parts: Vec<&[u8]> = tail.iter().map(AsRef::as_ref).collect();
                fsio::append(&wal_path, &parts)?;
            } else {
                let (base, segs) = self.fetch_all(m).await?;
                write_store(dir, &base, &segs)?;
            }
            self.remember(Some(Remote { local_verified: true, ..remote }));
            store.reload_if_stale()?;
            Ok(true)
        }
    }

    /// Ship what `store` committed since the last publish. See the crate
    /// docs; [`Error::Fenced`] means another writer owns the published store.
    pub fn publish<'a>(&'a self, store: &'a Store) -> impl Future<Output = Result<Published>> + 'a {
        async move {
            let cached = self.lock().remote.clone();
            let remote = match cached {
                Some(r) => Some(r),
                None => self.fetch_remote().await?,
            };
            let lease = self.lease_for_write(remote.as_ref())?;
            let cut = local_cut(store, remote.as_ref())?;
            let prev_segments =
                remote.as_ref().map(|r| r.manifest.segments.clone()).unwrap_or_default();
            let prev_gen = remote.as_ref().map_or(0, |r| r.manifest.generation);
            let mut next = remote.as_ref().map(|r| r.manifest.clone()).unwrap_or_default();
            next.lease = lease;
            let (uploaded, new_base, stale): (u64, bool, Vec<String>) = match cut {
                Cut::Nothing { generation, wal_len } => {
                    if let Some(r) = remote {
                        self.remember(Some(Remote { local_verified: true, ..r }));
                    }
                    return Ok(Published { generation, wal_len, uploaded: 0, new_base: false });
                }
                Cut::Tail { generation, start, frames } => {
                    let s = Segment { start, len: frames.len() as u64, sum: xxh3_64(&frames) };
                    self.put_immutable(&self.seg_key(generation, &s), Bytes::from(frames), s.sum)
                        .await?;
                    next.segments.push(s);
                    (s.len, false, Vec::new())
                }
                Cut::Coalesce { generation, wal } => {
                    let s = Segment { start: 0, len: wal.len() as u64, sum: xxh3_64(&wal) };
                    self.put_immutable(&self.seg_key(generation, &s), Bytes::from(wal), s.sum)
                        .await?;
                    let stale = prev_segments.iter().map(|p| self.seg_key(generation, p)).collect();
                    next.segments = vec![s];
                    (s.len, false, stale)
                }
                Cut::Full { generation, base, wal } => {
                    let base_sum = xxh3_64(&base);
                    let base_len = base.len() as u64;
                    self.put_immutable(&self.base_key(generation), Bytes::from(base), base_sum)
                        .await?;
                    let s = Segment { start: 0, len: wal.len() as u64, sum: xxh3_64(&wal) };
                    self.put_immutable(&self.seg_key(generation, &s), Bytes::from(wal), s.sum)
                        .await?;
                    next.generation = generation;
                    next.base_len = base_len;
                    next.base_sum = base_sum;
                    next.segments = vec![s];
                    let stale = if prev_gen != 0 && prev_gen != generation {
                        vec![self.base_key(prev_gen), self.gen_dir(prev_gen)]
                    } else {
                        Vec::new()
                    };
                    (base_len + s.len, true, stale)
                }
            };
            let head_gen = self.cas_head(remote.as_ref(), &next).await?;
            let published = Published {
                generation: next.generation,
                wal_len: next.wal_len(),
                uploaded,
                new_base,
            };
            self.remember(Some(Remote { head_gen, manifest: next, local_verified: true }));
            self.cleanup(&stale).await;
            Ok(published)
        }
    }

    /// Best-effort removal of objects no manifest names any more (a bucket
    /// lifecycle rule is the backstop). Keys ending in `/` are directories.
    async fn cleanup(&self, stale: &[String]) {
        for key in stale {
            if key.ends_with('/') {
                if let Ok(list) = self.objects.list(key).await {
                    for m in list {
                        let _ = self.objects.delete(&m.key).await;
                    }
                }
            } else {
                let _ = self.objects.delete(key).await;
            }
        }
    }

    /// The lease to write with the next manifest, or why writing is refused.
    fn lease_for_write(&self, remote: Option<&Remote>) -> Result<Option<Lease>> {
        let st = self.lock();
        let current = remote.and_then(|r| r.manifest.lease.clone());
        let now = now_ms();
        match (&st.holder, current) {
            (Some(h), Some(l)) if l.holder != h.name && l.expires_ms > now => {
                Err(Error::Leased { holder: l.holder, expires_ms: l.expires_ms })
            }
            (Some(h), _) => {
                Ok(Some(Lease { holder: h.name.clone(), expires_ms: now.saturating_add(h.ttl_ms) }))
            }
            (None, Some(l)) if l.expires_ms > now => {
                Err(Error::Leased { holder: l.holder, expires_ms: l.expires_ms })
            }
            (None, other) => Ok(other.filter(|l| l.expires_ms > now)),
        }
    }

    /// Become (or stay) the designated writer for `ttl_ms`. Every
    /// [`Mirror::publish`] renews it. Refused with [`Error::Leased`] while
    /// someone else's lease is unexpired. Clocks of competing instances are
    /// assumed to agree to well within `ttl_ms`.
    pub fn acquire_lease<'a>(
        &'a self,
        holder: &'a str,
        ttl_ms: u64,
    ) -> impl Future<Output = Result<Lease>> + 'a {
        async move {
            if holder.is_empty() || holder.len() > manifest::MAX_HOLDER {
                return Err(corrupt("lease holder must be 1..=256 bytes"));
            }
            let remote = self.fetch_remote().await?;
            let now = now_ms();
            if let Some(l) = remote.as_ref().and_then(|r| r.manifest.lease.as_ref()) {
                if l.holder != holder && l.expires_ms > now {
                    return Err(Error::Leased {
                        holder: l.holder.clone(),
                        expires_ms: l.expires_ms,
                    });
                }
            }
            let mut next = remote.as_ref().map(|r| r.manifest.clone()).unwrap_or_default();
            let lease = Lease { holder: holder.to_owned(), expires_ms: now.saturating_add(ttl_ms) };
            next.lease = Some(lease.clone());
            let head_gen = self.cas_head(remote.as_ref(), &next).await?;
            let verified = remote.as_ref().is_some_and(|r| r.local_verified);
            let mut st = self.lock();
            st.remote = Some(Remote { head_gen, manifest: next, local_verified: verified });
            st.holder = Some(Holder { name: holder.to_owned(), ttl_ms });
            Ok(lease)
        }
    }

    /// Give the lease up (no-op unless this mirror holds it).
    pub fn release_lease(&self) -> impl Future<Output = Result<()>> + '_ {
        async move {
            let Some(name) = self.lock().holder.take().map(|h| h.name) else { return Ok(()) };
            let Some(remote) = self.fetch_remote().await? else { return Ok(()) };
            if remote.manifest.lease.as_ref().is_none_or(|l| l.holder != name) {
                return Ok(());
            }
            let mut next = remote.manifest.clone();
            next.lease = None;
            let head_gen = self.cas_head(Some(&remote), &next).await?;
            self.remember(Some(Remote {
                head_gen,
                manifest: next,
                local_verified: remote.local_verified,
            }));
            Ok(())
        }
    }
}

/// Segment bytes must be whole committed frames (and the first must carry a
/// header for `generation`).
fn check_framing(b: &[u8], start: u64, generation: u64) -> Result<()> {
    let ok = if start == 0 {
        wal::parse_header(b).ok() == Some(generation) && wal::committed_len(b) == b.len()
    } else {
        wal::committed_frames_len(b) == b.len()
    };
    if ok {
        Ok(())
    } else {
        Err(corrupt("segment is not a run of committed frames"))
    }
}

/// Base first, then the log — graph-r's own compaction order, so a reader in
/// between sees the new base with a stale (ignored) log, never the reverse.
fn write_store(dir: &Path, base: &Bytes, segs: &[Bytes]) -> Result<()> {
    fsio::write_atomic(&dir.join(BASE_FILE), &[base])?;
    let parts: Vec<&[u8]> = segs.iter().map(AsRef::as_ref).collect();
    fsio::write_atomic(&dir.join(WAL_FILE), &parts)
}

/// Take a consistent local cut under the store's writer lock (no commit or
/// compaction can interleave). Only file reads happen here.
fn local_cut(store: &Store, remote: Option<&Remote>) -> Result<Cut> {
    let _writer = store.writer()?;
    let dir = store.dir();
    let wal_path = dir.join(WAL_FILE);
    let mut file = std::fs::File::open(&wal_path)?;
    let mut header = [0u8; wal::HEADER_LEN];
    file.read_exact(&mut header)?;
    let generation = wal::parse_header(&header)?;
    let published = remote.filter(|r| r.manifest.generation == generation);
    if let Some(r) = remote {
        if r.manifest.generation > generation {
            return Err(Error::Fenced {
                reason: "the published store is newer; pull before writing",
            });
        }
    }
    let Some(r) = published else {
        let base = std::fs::read(dir.join(BASE_FILE))?;
        let bdir = base::BaseDir::parse(&base)?;
        if bdir.header.generation != generation {
            return Err(graph_r::Error::corrupt("base and log generations differ").into());
        }
        let mut wal = Vec::new();
        file.seek(SeekFrom::Start(0))?;
        file.read_to_end(&mut wal)?;
        wal.truncate(wal::committed_len(&wal));
        return Ok(Cut::Full { generation, base, wal });
    };
    let have = r.manifest.wal_len();
    let file_len = file.metadata()?.len();
    if file_len < have {
        return Err(Error::Fenced {
            reason: "the local log is shorter than the published one; pull first",
        });
    }
    let coalesce = r.manifest.segments.len() >= COALESCE_SEGMENTS;
    if r.local_verified && !coalesce {
        let mut frames = Vec::with_capacity(usize::try_from(file_len - have).unwrap_or(0));
        file.seek(SeekFrom::Start(have))?;
        file.read_to_end(&mut frames)?;
        frames.truncate(wal::committed_frames_len(&frames));
        return Ok(if frames.is_empty() {
            Cut::Nothing { generation, wal_len: have }
        } else {
            Cut::Tail { generation, start: have, frames }
        });
    }
    let mut wal = Vec::with_capacity(usize::try_from(file_len).unwrap_or(0));
    file.seek(SeekFrom::Start(0))?;
    file.read_to_end(&mut wal)?;
    if !r.local_verified {
        // First publish from a directory this mirror did not produce: the
        // published prefix must be exactly our prefix.
        for s in &r.manifest.segments {
            let (a, b) = (s.start as usize, s.end() as usize);
            if wal.get(a..b).is_none_or(|bytes| xxh3_64(bytes) != s.sum) {
                return Err(Error::Fenced {
                    reason: "the local log diverges from the published one",
                });
            }
        }
    }
    let committed = wal::HEADER_LEN + wal::committed_frames_len(&wal[wal::HEADER_LEN..]);
    wal.truncate(committed);
    if committed as u64 == have {
        return Ok(Cut::Nothing { generation, wal_len: have });
    }
    if coalesce {
        return Ok(Cut::Coalesce { generation, wal });
    }
    let frames = wal.split_off(have as usize);
    Ok(Cut::Tail { generation, start: have, frames })
}
