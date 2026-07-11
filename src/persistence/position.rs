//! The [`Position`] abstraction: a generic ordering key for durable persistence.
//!
//! A [`Position`] is whatever a collector uses to order and resume its stream —
//! a block number, a queue offset, a `(time, seen-set)` frontier. Generalising
//! the persistence layer over `Position` lets any totally-ordered or
//! frontier-ordered source reuse the resume / backfill / gap-free machinery that
//! block sources already have. The built-in [`BlockPosition`] keeps the EVM path
//! behaviourally identical: it is the default position type, so the common block
//! case stays a one-liner. The reference [`TimeFrontier`] is the shipped worked
//! example of the hardest common non-block case, a `(time, hash-set)` frontier.

use anyhow::{Context, Result};

/// How a source treats re-observing a position that is already covered by the
/// stored watermark.
///
/// The policy lives on the [`Position`] type so a single writer body serves both
/// halt-on-reorg (blocks) and dedupe (frontiers) without per-source branching.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reobservation {
    /// Re-observing a covered position is a deep reorg: halt persistence.
    Halt,
    /// Re-observing a covered position is expected overlap: skip and suppress.
    Dedupe,
}

/// A generic ordering key for durable persistence.
///
/// Implement `Position` for a custom ordering key to gain the same resume,
/// backfill, and gap-free persistence guarantees that block-based sources
/// already have.
///
/// # Trait laws
///
/// Implementations must satisfy these laws; the built-in positions are
/// unit-tested against them:
///
/// - **Round-trip**: `decode(&p.encode()) == p` — a position survives a restart
///   through its persisted column value.
/// - **Monotone advance**: `advance(prev, next).sort_key() >= prev.sort_key()`
///   for any `prev` — the watermark never regresses.
/// - **Contains implies identity**: if `w.contains(&pos)` then
///   `advance(Some(w), pos) == w` — re-folding a covered position is a no-op.
/// - **Sort-key order is stream order**: `sort_key` projects the position onto
///   the total order in which the source delivers events.
/// - **Late events are skipped**: an event whose position is below the current
///   frontier boundary is deliberately skipped; completeness and finality remain
///   the consumer's responsibility.
/// - **The collector's tip is a coverage boundary**: the tip a
///   [`PersistableCollector`](super::PersistableCollector) reports must already
///   be fully readable via `query_range` — new events may only appear above it;
///   a later live delivery at or below it is treated as a re-observation
///   (overlap or reorg), never as new coverage. A source whose recent range is
///   still settling must report a lagged tip. This is the collector-side law
///   the replay/backfill/live split leans on; see
///   [`PersistableCollector::tip`](super::PersistableCollector::tip).
pub trait Position: Clone + std::fmt::Debug + Send + Sync + 'static {
    /// The re-observation policy for this position type — [`Reobservation::Halt`]
    /// for non-overlapping sources (blocks), [`Reobservation::Dedupe`] for
    /// overlapping ones (frontiers).
    const REOBSERVATION: Reobservation;

    /// The totally-ordered scalar projection of this position, stored per row and
    /// driving grouping, confirmation depth, backfill windows, and replay bounds.
    /// Sort-key order equals stream order.
    fn sort_key(&self) -> u64;

    /// The minimal position at `key`, used to build backfill window bounds.
    fn from_sort_key(key: u64) -> Self;

    /// Fold the previously-stored position with a newly-processed one to define
    /// how the watermark moves forward. `prev` is `None` before anything is
    /// stored. Must be monotone in [`sort_key`](Position::sort_key).
    fn advance(prev: Option<Self>, next: Self) -> Self;

    /// Whether an event at `pos` is already covered by this watermark — the
    /// dedupe / re-observation test.
    fn contains(&self, pos: &Self) -> bool;

    /// The sort key that backfill resumes from: `last + 1` for non-overlapping
    /// sources (blocks), the boundary itself for overlapping re-read (frontiers).
    fn resume_key(&self) -> u64;

    /// Encode the position to its persisted (TEXT) column value.
    fn encode(&self) -> String;

    /// Decode a persisted column value back into a position, failing loudly on
    /// malformed input rather than silently re-syncing from genesis.
    fn decode(encoded: &str) -> Result<Self>;
}

/// The built-in default position: an EVM block number.
///
/// `BlockPosition` keeps the common block case a one-liner and the EVM path
/// unchanged. Its `advance` is `max`, its encoding is the decimal integer, and
/// its re-observation policy is [`Reobservation::Halt`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlockPosition(pub u64);

impl From<u64> for BlockPosition {
    fn from(block: u64) -> Self {
        BlockPosition(block)
    }
}

impl From<BlockPosition> for u64 {
    fn from(position: BlockPosition) -> Self {
        position.0
    }
}

impl Position for BlockPosition {
    const REOBSERVATION: Reobservation = Reobservation::Halt;

    fn sort_key(&self) -> u64 {
        self.0
    }

    fn from_sort_key(key: u64) -> Self {
        BlockPosition(key)
    }

    fn advance(prev: Option<Self>, next: Self) -> Self {
        match prev {
            Some(prev) => BlockPosition(prev.0.max(next.0)),
            None => next,
        }
    }

    fn contains(&self, pos: &Self) -> bool {
        pos.0 <= self.0
    }

    fn resume_key(&self) -> u64 {
        self.0.saturating_add(1)
    }

    fn encode(&self) -> String {
        // Decimal integer text so the progress migration can CAST(last_block AS
        // TEXT).
        self.0.to_string()
    }

    fn decode(encoded: &str) -> Result<Self> {
        let block = encoded
            .parse::<u64>()
            .with_context(|| format!("decoding BlockPosition from {encoded:?}"))?;
        Ok(BlockPosition(block))
    }
}

/// The reference `(time, hash-set)` frontier position.
///
/// `TimeFrontier` is the framework-shipped worked example of the hardest common
/// non-block case: a source ordered by a millisecond timestamp where several
/// events can share one instant, so a bare scalar cannot express "everything up
/// to and including instant *t*, but only these identities *at* *t*". The
/// `time_ms` field is the boundary instant (the [`sort_key`](Position::sort_key)),
/// and `seen` holds the identities already observed *at that boundary instant*.
///
/// # Frontier laws
///
/// - **Advance moves time forward and unions at the max instant** — a strictly
///   later `next` wins and *drops the stale seen-set*, an equal instant unions
///   the two seen-sets, and an earlier `next` is a no-op.
///   Because a later instant discards the previous seen-set, the encoded set is
///   **bounded by per-instant event volume, not by history** — advancing past an
///   instant garbage-collects every identity below the new boundary.
/// - **Containment** is boundary-inclusive: `contains(&pos)` is true when `pos` is
///   at a strictly earlier instant, or at the same instant with every identity
///   already in `seen` (a superset check).
/// - **Overlapping resume**: [`resume_key`](Position::resume_key) is the boundary
///   instant *itself* (not `+ 1`), so a restart re-reads the boundary instant and
///   the writer dedupes the re-observed identities against `seen`.
/// - **Round-trip**: `encode` / `decode` go through `serde_json`, so a populated
///   seen-set survives a restart — `decode(&p.encode()) == p`.
///
/// # Non-goals
///
/// A late event arriving *below* the frontier boundary instant is deliberately
/// skipped ([`Reobservation::Dedupe`]); the frontier does not solve completeness,
/// finality, or reconciliation — those remain the consumer's responsibility.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TimeFrontier {
    /// The boundary instant in milliseconds — the position's sort key.
    pub time_ms: u64,
    /// Identities observed at the boundary instant `time_ms`. Bounded by
    /// per-instant volume: [`advance`](Position::advance) drops every identity
    /// below the boundary when time moves forward.
    pub seen: std::collections::BTreeSet<String>,
}

impl Position for TimeFrontier {
    // Overlapping re-read is expected, not a reorg: re-observed identities at the
    // boundary instant are deduped against `seen` rather than halting.
    const REOBSERVATION: Reobservation = Reobservation::Dedupe;

    fn sort_key(&self) -> u64 {
        self.time_ms
    }

    fn from_sort_key(key: u64) -> Self {
        // The minimal frontier at `key`: the instant with no identities seen yet.
        TimeFrontier {
            time_ms: key,
            seen: std::collections::BTreeSet::new(),
        }
    }

    fn advance(prev: Option<Self>, next: Self) -> Self {
        // See the "Frontier laws" on the type doc.
        let Some(prev) = prev else {
            return next;
        };
        match next.time_ms.cmp(&prev.time_ms) {
            std::cmp::Ordering::Greater => next,
            std::cmp::Ordering::Less => prev,
            std::cmp::Ordering::Equal => {
                let mut seen = prev.seen;
                seen.extend(next.seen);
                TimeFrontier {
                    time_ms: prev.time_ms,
                    seen,
                }
            }
        }
    }

    fn contains(&self, pos: &Self) -> bool {
        // Covered when strictly below the boundary, or at the boundary with every
        // identity already seen (a same-instant subset).
        pos.time_ms < self.time_ms
            || (pos.time_ms == self.time_ms && self.seen.is_superset(&pos.seen))
    }

    fn resume_key(&self) -> u64 {
        // The boundary instant itself: backfill re-reads the boundary so the
        // writer can dedupe same-instant identities (overlapping re-read).
        self.time_ms
    }

    fn encode(&self) -> String {
        // Compact JSON, e.g. {"time_ms":2000,"seen":["0xc1"]}. `to_string` only
        // fails for a map with non-string keys or a `Serialize` impl that
        // errors — neither applies to `{u64, BTreeSet<String>}` — so the
        // fallback below is unreachable in practice; it exists so encoding
        // itself can never panic.
        serde_json::to_string(self)
            .unwrap_or_else(|_| format!("{{\"time_ms\":{},\"seen\":[]}}", self.time_ms))
    }

    fn decode(encoded: &str) -> Result<Self> {
        serde_json::from_str(encoded)
            .with_context(|| format!("decoding TimeFrontier from {encoded:?}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // advance returns the greater of prev and next.
    #[test]
    fn advance_is_max() {
        // prev > next: max must keep prev (rules out a "take next" model).
        assert_eq!(
            BlockPosition::advance(Some(BlockPosition(5)), BlockPosition(3)),
            BlockPosition(5)
        );
        // prev < next: max must take next (rules out an "always prev" model).
        assert_eq!(
            BlockPosition::advance(Some(BlockPosition(3)), BlockPosition(5)),
            BlockPosition(5)
        );
    }

    // advance(None, next) == next.
    #[test]
    fn advance_from_none_returns_next() {
        assert_eq!(
            BlockPosition::advance(None, BlockPosition(7)),
            BlockPosition(7)
        );
    }

    // The `u64` ⇄ `BlockPosition` conversions are the ergonomic sugar callers
    // use at the boundary; they round-trip the block number unchanged.
    #[test]
    fn block_position_round_trips_through_u64() {
        assert_eq!(BlockPosition::from(7u64), BlockPosition(7));
        assert_eq!(u64::from(BlockPosition(7)), 7u64);
    }

    // decode∘encode == id, and encode is the decimal string.
    #[test]
    fn encode_decode_round_trips() {
        let position = BlockPosition(12345);
        assert_eq!(position.encode(), "12345");
        assert_eq!(BlockPosition::decode(&position.encode()).unwrap(), position);
        // Boundary values round-trip too.
        assert_eq!(
            BlockPosition::decode(&BlockPosition(0).encode()).unwrap(),
            BlockPosition(0)
        );
        assert_eq!(
            BlockPosition::decode(&BlockPosition(u64::MAX).encode()).unwrap(),
            BlockPosition(u64::MAX)
        );
    }

    // contains = pos.0 <= self.0; resume_key = self.0.saturating_add(1).
    #[test]
    fn contains_and_resume_key_for_blocks() {
        let watermark = BlockPosition(10);
        assert!(watermark.contains(&BlockPosition(5)), "below is covered");
        assert!(watermark.contains(&BlockPosition(10)), "equal is covered");
        assert!(
            !watermark.contains(&BlockPosition(11)),
            "above is not covered"
        );
        assert_eq!(watermark.resume_key(), 11);
        // resume_key saturates instead of overflowing.
        assert_eq!(BlockPosition(u64::MAX).resume_key(), u64::MAX);
    }

    // A non-numeric column value fails loudly (anyhow Err), never a
    // panic and never a silent genesis re-sync.
    #[test]
    fn decode_rejects_non_numeric() {
        assert!(BlockPosition::decode("not-a-number").is_err());
    }

    // ---- TimeFrontier ----

    /// Build a `TimeFrontier` from an instant and a slice of identity strings.
    fn frontier(time_ms: u64, hashes: &[&str]) -> TimeFrontier {
        TimeFrontier {
            time_ms,
            seen: hashes.iter().map(|h| h.to_string()).collect(),
        }
    }

    // A strictly later instant wins and drops the stale seen-set.
    // A "union across all history" model would
    // keep 0xa1/0xa2, and a "keep prev" model would keep instant 1000 — both
    // ruled out by asserting exactly (2000, {0xc1}).
    #[test]
    fn frontier_advance_later_instant_drops_stale() {
        let advanced = TimeFrontier::advance(
            Some(frontier(1000, &["0xa1", "0xa2"])),
            frontier(2000, &["0xc1"]),
        );
        assert_eq!(advanced, frontier(2000, &["0xc1"]));
        // The stale identities below the new boundary are gone (bounded set).
        assert!(!advanced.seen.contains("0xa1"));
        assert!(!advanced.seen.contains("0xa2"));
    }

    // An equal instant unions the seen-sets.
    // A "later/next wins dropping seen" model would give {0xa2} and
    // "keep prev" would give {0xa1}; the union asserts both survive.
    #[test]
    fn frontier_advance_equal_instant_unions() {
        let advanced =
            TimeFrontier::advance(Some(frontier(1000, &["0xa1"])), frontier(1000, &["0xa2"]));
        assert_eq!(advanced, frontier(1000, &["0xa1", "0xa2"]));
    }

    // An earlier instant is a no-op (monotone).
    // A "next wins" model would regress the watermark to
    // (1000, {0xa1}); the assertion pins it at the earlier prev value.
    #[test]
    fn frontier_advance_earlier_is_noop() {
        let prev = frontier(2000, &["0xc1"]);
        let advanced = TimeFrontier::advance(Some(prev.clone()), frontier(1000, &["0xa1"]));
        assert_eq!(advanced, prev);
        // The watermark never regresses in sort key.
        assert!(advanced.sort_key() >= 2000);
    }

    // advance(None, next) seeds the frontier with next (first flush).
    #[test]
    fn frontier_advance_from_none_returns_next() {
        assert_eq!(
            TimeFrontier::advance(None, frontier(1500, &["0xb1"])),
            frontier(1500, &["0xb1"])
        );
    }

    // decode∘encode == id for a rich, multi-element seen-set. This is the
    // round-trip that a resumed subscribe relies on; a lossy encoding of the set would fail this.
    #[test]
    fn frontier_round_trips_multi_element_seen_set() {
        let position = frontier(2500, &["0xc1", "0xc2", "0xd1"]);
        let encoded = position.encode();
        // Sanity: the encoding is JSON carrying both fields.
        assert!(encoded.contains("time_ms"));
        assert!(encoded.contains("seen"));
        assert_eq!(TimeFrontier::decode(&encoded).unwrap(), position);
        // The empty-seen-set / from_sort_key shape round-trips too.
        let minimal = TimeFrontier::from_sort_key(0);
        assert_eq!(TimeFrontier::decode(&minimal.encode()).unwrap(), minimal);
    }

    // contains: earlier instants and same-instant subsets are covered; a
    // genuinely-new same-instant identity and a later instant are not.
    #[test]
    fn frontier_contains_covers_earlier_and_same_instant_subsets() {
        let watermark = frontier(2000, &["0xc1", "0xc2"]);
        // Strictly earlier instant is covered regardless of its identities.
        assert!(watermark.contains(&frontier(1000, &["0xa1"])));
        // Same instant, subset of seen -> covered.
        assert!(watermark.contains(&frontier(2000, &["0xc1"])));
        assert!(watermark.contains(&frontier(2000, &["0xc1", "0xc2"])));
        // Same instant, a genuinely-new identity -> NOT covered.
        assert!(!watermark.contains(&frontier(2000, &["0xc3"])));
        // A later instant is never covered.
        assert!(!watermark.contains(&frontier(2500, &[])));
    }

    // Trait-law idempotence the writer relies on: advance(Some(w), pos) == w
    // whenever w.contains(&pos). Covers both the earlier-instant and the
    // same-instant-subset containment cases.
    #[test]
    fn frontier_advance_is_identity_when_contained() {
        let watermark = frontier(2000, &["0xc1", "0xc2"]);
        for covered in [
            frontier(1000, &["0xa1"]),         // strictly earlier
            frontier(2000, &["0xc1"]),         // same instant, subset
            frontier(2000, &["0xc1", "0xc2"]), // same instant, equal set
        ] {
            assert!(watermark.contains(&covered));
            assert_eq!(
                TimeFrontier::advance(Some(watermark.clone()), covered),
                watermark
            );
        }
    }

    // resume_key is the boundary instant itself (overlapping re-read), NOT +1.
    #[test]
    fn frontier_resume_key_is_boundary_instant() {
        assert_eq!(frontier(2000, &["0xc1"]).resume_key(), 2000);
    }

    // REOBSERVATION policy is Dedupe for frontiers (blocks Halt).
    #[test]
    fn frontier_reobservation_is_dedupe() {
        assert_eq!(TimeFrontier::REOBSERVATION, Reobservation::Dedupe);
    }

    // Invalid JSON fails loudly (anyhow Err) — the same loud-failure
    // contract a resumed subscribe relies on.
    #[test]
    fn frontier_decode_rejects_invalid_json() {
        assert!(TimeFrontier::decode("not-json").is_err());
        assert!(TimeFrontier::decode("{\"time_ms\":\"oops\"}").is_err());
    }

    // Illustrates the seen-set is bounded by per-instant volume, not history:
    // folding a long run of increasing instants keeps only the last instant's
    // identities.
    #[test]
    fn frontier_seen_set_bounded_by_instant_not_history() {
        let mut watermark: Option<TimeFrontier> = None;
        for (t, h) in [(1000, "0xa1"), (2000, "0xb1"), (3000, "0xc1")] {
            watermark = Some(TimeFrontier::advance(watermark, frontier(t, &[h])));
        }
        let watermark = watermark.unwrap();
        assert_eq!(watermark, frontier(3000, &["0xc1"]));
        assert_eq!(watermark.seen.len(), 1);
    }
}
