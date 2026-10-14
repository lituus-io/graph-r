// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! The text rules every index shares: tokenization, case folding, char
//! n-grams, stopwords and the token estimate behind budgets. One
//! implementation for graph-r's lexicon, link-r's BM25 and embedder, and
//! their users — a query term and an indexed term always agree.
//! Allocation-free apart from the case-folding of non-lowercase tokens.

use std::borrow::Cow;

/// Tokens: maximal runs of Unicode alphanumerics, borrowed from the input.
/// Not case-folded (see [`normalize`] and [`for_each_term`]).
pub fn tokens(text: &str) -> impl Iterator<Item = &str> {
    text.split(|c: char| !c.is_alphanumeric()).filter(|t| !t.is_empty())
}

/// A token's canonical lowercase form; borrowed when it already is one.
#[must_use]
pub fn normalize(token: &str) -> Cow<'_, str> {
    if token.chars().all(|c| !c.is_uppercase()) {
        Cow::Borrowed(token)
    } else if token.is_ascii() {
        Cow::Owned(token.to_ascii_lowercase())
    } else {
        Cow::Owned(token.to_lowercase())
    }
}

/// Run `f` on every case-folded token of `text`, through one reused buffer.
pub fn for_each_term(text: &str, mut f: impl FnMut(&str)) {
    let mut buf = String::new();
    for t in tokens(text) {
        match normalize(t) {
            Cow::Borrowed(t) => f(t),
            Cow::Owned(o) => {
                buf.clear();
                buf.push_str(&o);
                f(&buf);
            }
        }
    }
}

/// Run `f` on each `n`-character window of `token` (on char boundaries); a
/// token of at most `n` characters is emitted whole. No allocation.
pub fn for_each_char_ngram(token: &str, n: usize, mut f: impl FnMut(&str)) {
    if n == 0 || token.is_empty() {
        return;
    }
    let mut ends = token.char_indices().map(|(i, _)| i).chain(std::iter::once(token.len()));
    let mut starts = token.char_indices().map(|(i, _)| i);
    // `end` runs `n` characters ahead of `start`.
    let Some(mut end) = ends.nth(n) else {
        f(token);
        return;
    };
    for start in starts.by_ref() {
        f(&token[start..end]);
        match ends.next() {
            Some(e) => end = e,
            None => break,
        }
    }
}

/// A compact English stopword list, sorted for binary search.
static STOPWORDS: &[&str] = &[
    "a", "about", "above", "after", "again", "all", "am", "an", "and", "any", "are", "as", "at",
    "be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "can",
    "did", "do", "does", "doing", "down", "during", "each", "few", "for", "from", "further", "had",
    "has", "have", "having", "he", "her", "here", "hers", "him", "his", "how", "i", "if", "in",
    "into", "is", "it", "its", "itself", "just", "me", "more", "most", "my", "no", "nor", "not",
    "of", "off", "on", "once", "only", "or", "other", "our", "out", "over", "own", "same", "she",
    "should", "so", "some", "such", "than", "that", "the", "their", "them", "then", "there",
    "these", "they", "this", "those", "through", "to", "too", "under", "until", "up", "very",
    "was", "we", "were", "what", "when", "where", "which", "while", "who", "whom", "why", "will",
    "with", "you", "your",
];

/// Whether a case-folded token is a common English stopword.
#[must_use]
pub fn is_stopword(term: &str) -> bool {
    STOPWORDS.binary_search(&term).is_ok()
}

/// Model tokens in `bytes` of text: the 4-bytes-per-token rule every budget
/// (graph-r query, prompt assembly, request sizing) uses.
#[must_use]
#[inline]
pub fn estimate_tokens(bytes: usize) -> u64 {
    (bytes as u64).div_ceil(4)
}

/// The text bytes a budget of `tokens` admits (the inverse of
/// [`estimate_tokens`]).
#[must_use]
#[inline]
pub fn bytes_for_tokens(tokens: u64) -> usize {
    usize::try_from(tokens.saturating_mul(4)).unwrap_or(usize::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_are_zero_copy_unicode_runs() {
        assert_eq!(
            tokens("Hello, world!  foo_bar.baz").collect::<Vec<_>>(),
            ["Hello", "world", "foo", "bar", "baz"]
        );
        assert_eq!(tokens("café—über").collect::<Vec<_>>(), ["café", "über"]);
    }

    #[test]
    fn terms_fold_case_and_borrow_when_they_can() {
        assert!(matches!(normalize("already"), Cow::Borrowed(_)));
        assert_eq!(normalize("ÜBER"), "über");
        let mut t = Vec::new();
        for_each_term("Install FooBar v2.1 (Löwe)", |x| t.push(x.to_owned()));
        assert_eq!(t, ["install", "foobar", "v2", "1", "löwe"]);
    }

    #[test]
    fn char_ngrams_window_on_char_boundaries() {
        let grams = |s: &str, n| {
            let mut v = Vec::new();
            for_each_char_ngram(s, n, |g| v.push(g.to_owned()));
            v
        };
        assert_eq!(grams("hello", 3), ["hel", "ell", "llo"]);
        assert_eq!(grams("hi", 3), ["hi"]);
        assert_eq!(grams("abc", 3), ["abc"]);
        assert_eq!(grams("café", 2), ["ca", "af", "fé"]);
        assert!(grams("", 2).is_empty());
    }

    #[test]
    fn stopwords_and_estimate() {
        assert!(is_stopword("the") && !is_stopword("bigquery"));
        assert!(STOPWORDS.windows(2).all(|w| w[0] < w[1]));
        assert_eq!((estimate_tokens(0), estimate_tokens(1), estimate_tokens(8)), (0, 1, 2));
        assert_eq!(estimate_tokens(bytes_for_tokens(250)), 250);
    }
}
