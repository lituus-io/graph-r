// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! Error taxonomy. Decoders return [`Error::Format`] (or [`Error::Version`]
//! for a well-formed file of another format version) rather than panicking on
//! any malformed input — the property the fuzz targets enforce — and every
//! concurrency- or lock-related refusal has its own variant so callers can
//! distinguish "retry later" from "corrupt file".

/// Crate-wide result alias.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// All failure modes surfaced by graph-r.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// A file or byte stream failed structural validation.
    Format {
        /// What was malformed and where.
        message: String,
    },

    /// Another process holds the writer lock for this store directory.
    Locked,

    /// A bounded wait for internal capacity expired (e.g. every generation
    /// slot is pinned by long-lived snapshots during compaction).
    Busy {
        /// Which capacity was exhausted.
        message: String,
    },

    /// The on-disk state is inconsistent in a way replay cannot repair.
    Corrupt {
        /// What invariant was violated.
        message: String,
    },

    /// A mutating operation was attempted on a read-only store handle.
    ReadOnly,

    /// An underlying I/O failure.
    Io {
        /// The originating I/O error.
        source: std::io::Error,
    },

    /// A file written in a format version this build does not read (an
    /// older store to rebuild, or a newer one from a later release). Its own
    /// variant so a caller can tell "rebuild this" from "this is damaged"
    /// without reading the message.
    Version {
        /// The version the file declares.
        found: u16,
        /// The one version this build reads and writes.
        supported: u16,
    },
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Format { message } => write!(f, "format error: {message}"),
            // Keeps the `format error: unsupported version N` prefix earlier
            // releases printed, for callers that still match on the text.
            Self::Version { found, supported } => {
                write!(
                    f,
                    "format error: unsupported version {found} (this build reads {supported})"
                )
            }
            Self::Locked => f.write_str("store is locked by another writer"),
            Self::Busy { message } => write!(f, "store is busy: {message}"),
            Self::Corrupt { message } => write!(f, "store is corrupt: {message}"),
            Self::ReadOnly => f.write_str("store was opened read-only"),
            Self::Io { source } => write!(f, "io error: {source}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source } => Some(source),
            _ => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(source: std::io::Error) -> Self {
        Self::Io { source }
    }
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
