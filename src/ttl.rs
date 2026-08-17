// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>
//
//! Adaptive freshness. Every document node carries a revalidation interval
//! that grows geometrically while the source is observed unchanged and is cut
//! sharply when it changes — integer math with explicit clamps, so replay
//! and tests are bit-deterministic. Important nodes (percentile `PageRank`)
//! are checked more often via a permille bias on the effective interval.

/// The outcome of one revalidation observation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// Source unchanged (304, or identical content hash).
    Unchanged,
    /// Source content changed.
    Changed,
    /// Transient failure (network, 5xx); retry soon, do not penalize.
    Error,
    /// Source is gone (404/403/401); the node is tombstoned.
    Gone,
}

impl Outcome {
    /// On-disk tag.
    #[must_use]
    pub fn as_tag(self) -> u8 {
        match self {
            Self::Unchanged => 0,
            Self::Changed => 1,
            Self::Error => 2,
            Self::Gone => 3,
        }
    }

    /// Decode an on-disk tag (unknown tags degrade to `Error`, the neutral
    /// outcome, rather than failing replay).
    #[must_use]
    pub fn from_tag(tag: u8) -> Self {
        match tag {
            0 => Self::Unchanged,
            1 => Self::Changed,
            3 => Self::Gone,
            _ => Self::Error,
        }
    }
}

/// Adaptive-TTL policy. All fields are settable; defaults suit documentation
/// corpora (hours-scale freshness, month-scale ceiling).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TtlConfig {
    /// Interval assigned to a newly ingested node, seconds.
    pub base_s: u32,
    /// Floor, seconds.
    pub min_s: u32,
    /// Ceiling, seconds.
    pub max_s: u32,
    /// Growth ratio numerator (applied per consecutive `Unchanged`).
    pub grow_num: u32,
    /// Growth ratio denominator.
    pub grow_den: u32,
    /// Cut ratio numerator (applied on `Changed`).
    pub cut_num: u32,
    /// Cut ratio denominator.
    pub cut_den: u32,
    /// How strongly importance shortens the effective interval, in permille:
    /// a top-ranked node's interval is scaled by `(1000 - bias) / 1000`.
    pub importance_bias_permille: u32,
}

impl Default for TtlConfig {
    fn default() -> Self {
        Self {
            base_s: 21_600,      // 6 h
            min_s: 900,          // 15 min
            max_s: 2_592_000,    // 30 d
            grow_num: 3,
            grow_den: 2,
            cut_num: 1,
            cut_den: 4,
            importance_bias_permille: 500,
        }
    }
}

impl TtlConfig {
    /// The next stored interval after observing `outcome` with the current
    /// `interval_s`. `Gone` keeps the interval (the node is tombstoned and
    /// leaves the schedule); `Error` schedules a floor-interval retry.
    #[must_use]
    pub fn next_interval(&self, interval_s: u32, outcome: Outcome) -> u32 {
        let clamp = |v: u64| -> u32 {
            v.clamp(u64::from(self.min_s), u64::from(self.max_s)) as u32
        };
        match outcome {
            Outcome::Unchanged => {
                clamp(u64::from(interval_s) * u64::from(self.grow_num) / u64::from(self.grow_den.max(1)))
            }
            Outcome::Changed => {
                clamp(u64::from(interval_s) * u64::from(self.cut_num) / u64::from(self.cut_den.max(1)))
            }
            Outcome::Error => self.min_s,
            Outcome::Gone => interval_s,
        }
    }

    /// When the node is next due, given its rank percentile in permille.
    /// Higher rank ⇒ shorter effective interval, down to `(1000-bias)/1000`.
    #[must_use]
    pub fn next_check_at_ms(&self, fetched_at_ms: u64, interval_s: u32, rank_permille: u16) -> u64 {
        let bias = u64::from(self.importance_bias_permille.min(1000));
        let mult = 1000 - bias * u64::from(rank_permille.min(1000)) / 1000;
        fetched_at_ms.saturating_add(u64::from(interval_s) * 1000 * mult / 1000)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grows_on_unchanged_until_ceiling() {
        let cfg = TtlConfig::default();
        let mut iv = cfg.base_s;
        let mut prev = 0;
        for _ in 0..64 {
            iv = cfg.next_interval(iv, Outcome::Unchanged);
            assert!(iv >= prev, "monotone growth");
            assert!(iv <= cfg.max_s);
            prev = iv;
        }
        assert_eq!(iv, cfg.max_s, "parks at the ceiling");
    }

    #[test]
    fn changed_cuts_and_error_floors() {
        let cfg = TtlConfig::default();
        let grown = cfg.next_interval(cfg.max_s, Outcome::Unchanged);
        let cut = cfg.next_interval(grown, Outcome::Changed);
        assert!(cut < grown);
        assert!(cut >= cfg.min_s);
        assert_eq!(cfg.next_interval(grown, Outcome::Error), cfg.min_s);
        assert_eq!(cfg.next_interval(grown, Outcome::Gone), grown);
    }

    #[test]
    fn importance_shortens_the_effective_interval() {
        let cfg = TtlConfig::default();
        let nobody = cfg.next_check_at_ms(0, 1000, 0);
        let star = cfg.next_check_at_ms(0, 1000, 1000);
        assert_eq!(nobody, 1_000_000);
        assert_eq!(star, 500_000, "top rank halves the interval at bias 500");
        assert!(cfg.next_check_at_ms(0, 1000, 500) < nobody);
    }

    #[test]
    fn outcome_tags_round_trip() {
        for o in [Outcome::Unchanged, Outcome::Changed, Outcome::Error, Outcome::Gone] {
            assert_eq!(Outcome::from_tag(o.as_tag()), o);
        }
        assert_eq!(Outcome::from_tag(200), Outcome::Error);
    }
}
