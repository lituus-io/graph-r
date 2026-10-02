// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>
//
//! The crate's single hash family (xxh3), exposed so dependants key and
//! fingerprint with exactly the function behind [`crate::UrlKey`], section
//! checksums and file trailers — one family across graph-r, link-r and their
//! users, no second hashing crate. Thin wrappers: the underlying crate is an
//! implementation detail and never appears in this API.

/// xxh3-64 of `bytes` (what [`crate::UrlKey::of`] applies to a URL).
#[must_use]
#[inline]
pub fn xxh3_64(bytes: &[u8]) -> u64 {
    xxhash_rust::xxh3::xxh3_64(bytes)
}

/// xxh3-64 of `bytes` with a seed (independent hash functions for sketches).
#[must_use]
#[inline]
pub fn xxh3_64_seeded(bytes: &[u8], seed: u64) -> u64 {
    xxhash_rust::xxh3::xxh3_64_with_seed(bytes, seed)
}

/// xxh3-128 of `bytes` (fingerprints where 64-bit collisions matter).
#[must_use]
#[inline]
pub fn xxh3_128(bytes: &[u8]) -> u128 {
    xxhash_rust::xxh3::xxh3_128(bytes)
}

/// Streaming xxh3: feed pieces with [`Hasher::update`]; the digests equal
/// the one-shot functions over the concatenation.
#[derive(Clone)]
pub struct Hasher(xxhash_rust::xxh3::Xxh3);

impl std::fmt::Debug for Hasher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Hasher")
    }
}

impl Default for Hasher {
    fn default() -> Self {
        Self::new()
    }
}

impl Hasher {
    /// An unseeded hasher.
    #[must_use]
    pub fn new() -> Self {
        Self(xxhash_rust::xxh3::Xxh3::new())
    }

    /// A seeded hasher (matches [`xxh3_64_seeded`]).
    #[must_use]
    pub fn with_seed(seed: u64) -> Self {
        Self(xxhash_rust::xxh3::Xxh3::with_seed(seed))
    }

    /// Feed bytes.
    #[inline]
    pub fn update(&mut self, bytes: &[u8]) {
        self.0.update(bytes);
    }

    /// 64-bit digest of everything fed so far (the hasher stays usable).
    #[must_use]
    pub fn digest64(&self) -> u64 {
        self.0.digest()
    }

    /// 128-bit digest of everything fed so far.
    #[must_use]
    pub fn digest128(&self) -> u128 {
        self.0.digest128()
    }

    /// Forget everything fed, keeping the seed.
    pub fn reset(&mut self) {
        self.0.reset();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_shot_matches_url_key() {
        assert_eq!(xxh3_64(b"https://x.dev/a"), crate::UrlKey::of("https://x.dev/a").raw());
    }

    #[test]
    fn streaming_equals_one_shot() {
        let mut h = Hasher::new();
        h.update(b"hello ");
        h.update(b"world");
        assert_eq!(h.digest64(), xxh3_64(b"hello world"));
        assert_eq!(h.digest128(), xxh3_128(b"hello world"));
        h.reset();
        assert_eq!(h.digest64(), xxh3_64(b""));
    }

    #[test]
    fn seeds_give_independent_functions() {
        assert_ne!(xxh3_64_seeded(b"v", 1), xxh3_64_seeded(b"v", 2));
        let mut h = Hasher::with_seed(7);
        h.update(b"v");
        assert_eq!(h.digest64(), xxh3_64_seeded(b"v", 7));
    }
}
