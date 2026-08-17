// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>
//
//! Identity types. The durable foreign key everywhere is [`UrlKey`] — the
//! 64-bit xxh3 of a canonical URL, byte-compatible with link-r's key of the
//! same name — because link-r's internal document ids are unstable across
//! removals. [`NodeId`]s here are dense per-snapshot indices assigned at
//! compaction; they are stable *within* one base generation and never
//! persisted across it, exactly link-r's `DocId` philosophy.

use xxhash_rust::xxh3::xxh3_64;

/// Durable identity of a document node: xxh3 of its canonical URL.
///
/// When the `bridge` feature is enabled this is produced from link-r's
/// canonicalization; standalone callers must pass an already-canonical URL
/// string to [`UrlKey::of`] (graph-r deliberately does not re-implement URL
/// canonicalization — one recipe, one owner).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct UrlKey(pub u64);

impl UrlKey {
    /// Key a canonical URL string.
    #[must_use]
    pub fn of(canonical_url: &str) -> Self {
        Self(xxh3_64(canonical_url.as_bytes()))
    }

    /// The raw 64-bit key.
    #[must_use]
    pub fn raw(self) -> u64 {
        self.0
    }
}

/// Durable identity of a sub-document segment: xxh3 of the owning key plus
/// the segment's heading path, so a re-crawl that keeps a heading keeps the
/// segment's identity (and any enrichment attached to it).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SegKey(pub u64);

impl SegKey {
    /// Derive the segment key for `heading_path` under `owner`.
    #[must_use]
    pub fn derive(owner: UrlKey, heading_path: &str) -> Self {
        let mut buf = Vec::with_capacity(8 + 1 + heading_path.len());
        buf.extend_from_slice(&owner.0.to_le_bytes());
        buf.push(0);
        buf.extend_from_slice(heading_path.as_bytes());
        Self(xxh3_64(&buf))
    }
}

/// Dense per-snapshot node index (position in the base `Nodes` section).
/// Invalidated by compaction; cross-snapshot APIs use [`UrlKey`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(pub u32);

/// Typed edges. The discriminants are burned into the on-disk format; new
/// kinds are additive (the enum is non-exhaustive and unknown values decode
/// to [`EdgeType::Unknown`] rather than failing the file).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum EdgeType {
    /// An outbound hyperlink observed in the crawled document (crawl tier).
    Link,
    /// A semantic-similarity edge proposed from dense vectors or enrichment.
    Related,
    /// A redirect observed at fetch time.
    Redirect,
    /// Forward-compatibility catch-all for tags this build does not know.
    Unknown(u8),
}

impl EdgeType {
    /// On-disk tag.
    #[must_use]
    pub fn as_tag(self) -> u8 {
        match self {
            Self::Link => 0,
            Self::Related => 2,
            Self::Redirect => 3,
            Self::Unknown(t) => t,
        }
    }

    /// Decode an on-disk tag (never fails; unknown tags are preserved).
    #[must_use]
    pub fn from_tag(tag: u8) -> Self {
        match tag {
            0 => Self::Link,
            2 => Self::Related,
            3 => Self::Redirect,
            t => Self::Unknown(t),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_key_matches_xxh3_of_string() {
        let k = UrlKey::of("https://example.com/a");
        assert_eq!(k.raw(), xxh3_64(b"https://example.com/a"));
    }

    #[test]
    fn seg_key_is_stable_and_distinct() {
        let owner = UrlKey::of("https://example.com/a");
        let a = SegKey::derive(owner, "Install > Linux");
        let b = SegKey::derive(owner, "Install > Linux");
        let c = SegKey::derive(owner, "Install > macOS");
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert_ne!(a, SegKey::derive(UrlKey::of("https://example.com/b"), "Install > Linux"));
    }

    #[test]
    fn edge_type_tags_round_trip() {
        for t in [EdgeType::Link, EdgeType::Related, EdgeType::Redirect, EdgeType::Unknown(9)] {
            assert_eq!(EdgeType::from_tag(t.as_tag()), t);
        }
    }
}
