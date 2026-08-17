// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>
//
//! Error taxonomy. Decoders return [`Error::Format`] rather than panicking on
//! any malformed input — the property the fuzz targets enforce — and every
//! concurrency- or lock-related refusal has its own variant so callers can
//! distinguish "retry later" from "corrupt file".

/// Crate-wide result alias.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// All failure modes surfaced by graph-r.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// A file or byte stream failed structural validation.
    #[error("format error: {message}")]
    Format {
        /// What was malformed and where.
        message: String,
    },

    /// Another process holds the writer lock for this store directory.
    #[error("store is locked by another writer")]
    Locked,

    /// A bounded wait for internal capacity expired (e.g. every generation
    /// slot is pinned by long-lived snapshots during compaction).
    #[error("store is busy: {message}")]
    Busy {
        /// Which capacity was exhausted.
        message: String,
    },

    /// The on-disk state is inconsistent in a way replay cannot repair.
    #[error("store is corrupt: {message}")]
    Corrupt {
        /// What invariant was violated.
        message: String,
    },

    /// A mutating operation was attempted on a read-only store handle.
    #[error("store was opened read-only")]
    ReadOnly,

    /// An underlying I/O failure.
    #[error("io error: {source}")]
    Io {
        /// The originating I/O error.
        #[from]
        source: std::io::Error,
    },
}

impl Error {
    /// A structural-validation failure.
    #[must_use]
    pub fn format(message: impl Into<String>) -> Self {
        Self::Format { message: message.into() }
    }

    /// A capacity-exhaustion refusal.
    #[must_use]
    pub fn busy(message: impl Into<String>) -> Self {
        Self::Busy { message: message.into() }
    }

    /// An unrepairable inconsistency.
    #[must_use]
    pub fn corrupt(message: impl Into<String>) -> Self {
        Self::Corrupt { message: message.into() }
    }
}
