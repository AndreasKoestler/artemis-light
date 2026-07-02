//! [`ConfirmationWindow`]: persists the live tail, lagging the live edge by a
//! confirmation depth so a shallow reorg is corrected before any orphaned row
//! is written.

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::sync::Arc;

use serde::Serialize;

use super::GapFreeWriter;
use crate::persistence::{Position, Record, Reobservation, Row, Store};

/// One buffered position-group: the [`Position`] to write it at and its rows.
struct Pending<P> {
    position: P,
    rows: Vec<Row>,
}

/// Buffers the most recent `depth` position-groups of a live tail and writes a
/// group only once it is buried `depth` deep (`head >= sort_key + depth`).
/// Unlike [`BlockWriter`](super::BlockWriter), a backwards position within the
/// unflushed window is treated as a reorg re-emission and corrected in place
/// rather than halting: the node has rewound to a forked position whose row was
/// never written, so dropping the old fork's buffered groups and rewinding
/// `head` lets the canonical chain re-fill the window. Only a write at or below
/// the already-flushed watermark (a reorg *deeper* than `depth`, which would
/// need a delete to undo) or an unencodable event halts.
///
/// The finalized watermark is seeded from the stored position at subscribe, so a
/// live re-observation at or below the resumed watermark is recognised as a deep
/// reorg. The unflushed window is deliberately never drained at stream end:
/// there is no "stream end" on a live tail, and on a restart the Backfill segment
/// re-fetches the whole window (`[resume_key ..= tip]`) from the canonical chain.
pub(super) struct ConfirmationWindow<'a, S, P, E> {
    core: GapFreeWriter<'a, S, E>,
    /// Groups buried this many sort keys deep are mature and get written.
    depth: u64,
    /// Buffered groups keyed by sort key, lowest first — the order they must be
    /// flushed in to keep the stored watermark a gap-free prefix.
    pending: BTreeMap<u64, Pending<P>>,
    /// Highest sort key seen so far; maturity is measured against it.
    head: Option<u64>,
    /// Highest sort key already written — the finalized watermark's sort key,
    /// seeded from the stored position. A re-emission at or below it is a reorg
    /// deeper than `depth` (Halt path).
    flushed: Option<u64>,
    /// The finalized watermark as a full [`Position`], seeded from the stored
    /// position and advanced as groups flush. Consulted only on the dedupe path
    /// ([`Reobservation::Dedupe`]); dead on the Halt (block) path, which uses
    /// [`flushed`](Self::flushed).
    watermark: Option<P>,
}

impl<'a, S, P, E> ConfirmationWindow<'a, S, P, E>
where
    S: Store<P>,
    P: Position,
    E: Serialize,
{
    pub(super) fn new(store: &'a S, record: Arc<Record<E>>, depth: u64, seed: Option<P>) -> Self {
        Self {
            core: GapFreeWriter::new(store, record),
            depth,
            pending: BTreeMap::new(),
            head: None,
            // Seed the finalized watermark with the stored position's sort key so
            // a resumed live tail treats a re-observation at/below it as a deep
            // reorg [position-trait.PARITY.2].
            flushed: seed.as_ref().map(|p| p.sort_key()),
            watermark: seed,
        }
    }

    /// Buffer one event's row, correcting an in-window reorg, then flush every
    /// group that has matured to `depth` confirmations. Returns whether the
    /// event should be delivered downstream: `false` only for a
    /// [`Reobservation::Dedupe`] position already covered by the watermark or the
    /// pending slot's fold at its own sort key (a suppressed re-observation),
    /// `true` otherwise. No per-event work once unhealthy — but the event still
    /// flows (`true`).
    pub(super) async fn record(&mut self, position: P, event: &E) -> bool {
        if !self.core.healthy() {
            return true;
        }
        let key = position.sort_key();

        // Dedupe (Reobservation::Dedupe only): an event already covered by the
        // finalized watermark or the pending slot's fold at its own sort key is
        // an expected re-read across an overlapping backfill — encode no row and
        // suppress it downstream [position-trait.DEDUP.1, position-trait.DEDUP.2].
        if P::REOBSERVATION == Reobservation::Dedupe && self.covered(&position) {
            return false;
        }

        // A finalized position being rewritten is a reorg deeper than `depth`:
        // unfixable without a delete, so halt (the stored watermark stays; a
        // restart re-syncs). Halt sources only — a Dedupe re-observation was
        // already suppressed above, where `contains` (not the sort-key `<=`) is
        // the covered test.
        if P::REOBSERVATION == Reobservation::Halt
            && let Some(f) = self.flushed
            && key <= f
        {
            // Read `depth` into a local first: `fail` takes `&mut self.core`,
            // so the format args may not also borrow `self` immutably.
            let depth = self.depth;
            self.core.fail(format_args!(
                "position {position:?} re-observed at/below the watermark \
                 (reorg deeper than confirmation depth {depth})"
            ));
            self.pending.clear();
            return true;
        }

        let Some(row) = self.core.encode(event) else {
            // As in BlockWriter: an unencodable event must not be skipped, or
            // progress advances past a hole replay would expose.
            self.pending.clear();
            return true;
        };

        // Shallow reorg: the chain forked above `key`. Drop the old fork's
        // buffered groups (the node re-emits the canonical ones) and rewind the
        // head so those groups must re-confirm. Groups strictly below `key` are
        // untouched — they belong to the shared prefix. Halt-policy (block)
        // sources only (design §4 record step 5): a Dedupe (frontier) feed is
        // append-only, so a backwards position is jittered late arrival, not a
        // reorg. It falls through to a plain insert into its own slot below —
        // never a rewind that would drop a higher already-buffered slot and
        // silently lose it.
        if P::REOBSERVATION == Reobservation::Halt
            && let Some(h) = self.head
            && key < h
        {
            self.pending.retain(|&b, _| b < key);
            self.head = Some(key);
        }

        // Fold this event into its sort-key group: a union of same-instant
        // identities for a frontier, the same block for `BlockPosition`.
        match self.pending.entry(key) {
            Entry::Vacant(slot) => {
                slot.insert(Pending {
                    position,
                    rows: vec![row],
                });
            }
            Entry::Occupied(mut slot) => {
                let pending = slot.get_mut();
                pending.position = P::advance(Some(pending.position.clone()), position);
                pending.rows.push(row);
            }
        }
        self.head = Some(self.head.map_or(key, |h| h.max(key)));

        self.flush_matured().await;
        true
    }

    /// Whether `pos` is already covered by the finalized watermark or by the
    /// pending (buffered-but-unflushed) group at `pos`'s own sort key — the
    /// dedupe test for [`Reobservation::Dedupe`] positions (design §4 record
    /// step 5: "the pending slot's fold at `pos.sort_key()`").
    ///
    /// The pending-fold check is scoped to the *same* sort-key slot on purpose.
    /// Consulting every pending slot would let a slot at a higher sort key (a
    /// later instant `T3`) report a genuinely-new lower event (`T2 < T3`) as
    /// "contained" — a frontier's `contains` is true for any earlier instant —
    /// and silently suppress it, permanently losing it once the later slot
    /// flushes. Append-only feeds only jitter; they do not reorg, so a
    /// lower-but-uncovered position is a new event for its own slot, never a
    /// re-observation of a higher one.
    fn covered(&self, pos: &P) -> bool {
        self.watermark.as_ref().is_some_and(|w| w.contains(pos))
            || self
                .pending
                .get(&pos.sort_key())
                .is_some_and(|p| p.position.contains(pos))
    }

    /// Flush every buffered group now buried `depth` deep, lowest first.
    async fn flush_matured(&mut self) {
        let Some(head) = self.head else { return };
        // Collect the matured sort keys first to avoid borrowing `pending`
        // across the await inside the flush loop.
        let matured: Vec<u64> = self
            .pending
            .keys()
            .copied()
            .filter(|&b| head >= b + self.depth)
            .collect();
        for b in matured {
            let Some(pending) = self.pending.remove(&b) else {
                continue;
            };
            let Pending { position, rows } = pending;
            // A failed write means a later group must not advance the stored
            // watermark past the gap; drop the rest of the window and stop. The
            // shared core has already gone unhealthy and logged.
            if !self.core.flush(position.clone(), rows).await {
                self.pending.clear();
                return;
            }
            self.flushed = Some(b);
            // Fold the flushed group into the finalized watermark so a later
            // re-observation of a covered identity still dedupes.
            self.watermark = Some(P::advance(self.watermark.take(), position));
        }
    }

    /// Sort keys currently buffered, lowest first.
    #[cfg(test)]
    fn buffered_blocks(&self) -> Vec<u64> {
        self.pending.keys().copied().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_support::{
        BadPing, Ping, RecordingStore, TestFrontier, bad_ping, ping, record,
    };
    use super::*;
    use crate::persistence::BlockPosition;

    fn window<S, E>(store: &S, depth: u64) -> ConfirmationWindow<'_, S, BlockPosition, E>
    where
        S: Store<BlockPosition>,
        E: alloy::sol_types::SolEvent + Serialize,
    {
        ConfirmationWindow::new(store, record(), depth, None)
    }

    /// At depth `n` a block is written only once a block `n` higher arrives
    /// (`head >= block + n`). The window holds the most recent `n` blocks
    /// unwritten so a shallow reorg can still rewrite them.
    #[tokio::test]
    async fn windowed_writer_flushes_only_blocks_buried_depth_deep() {
        let store = RecordingStore::default();
        let mut w = window::<_, Ping>(&store, 2);

        w.record(BlockPosition(1), &ping(1)).await; // head 1: nothing matured
        w.record(BlockPosition(2), &ping(2)).await; // head 2: block 1 needs head>=3
        assert_eq!(store.written(), Vec::<u64>::new());

        w.record(BlockPosition(3), &ping(3)).await; // head 3: block 1 matures (1+2<=3)
        assert_eq!(store.written(), vec![1]);

        w.record(BlockPosition(4), &ping(4)).await; // head 4: block 2 matures
        assert_eq!(store.written(), vec![1, 2]);
        assert_eq!(w.buffered_blocks(), vec![3, 4]);
    }

    /// Depth 1 must reproduce the single-block flush semantics exactly: a block
    /// is written as soon as the next block arrives, leaving the open block
    /// buffered — the behaviour [`BlockWriter`](super::super::BlockWriter) gives
    /// a backfill range today.
    #[tokio::test]
    async fn depth_one_matches_single_block_behaviour() {
        let store = RecordingStore::default();
        let mut w = window::<_, Ping>(&store, 1);

        w.record(BlockPosition(1), &ping(1)).await;
        w.record(BlockPosition(2), &ping(2)).await; // block 1 matures (1+1<=2)
        w.record(BlockPosition(3), &ping(3)).await; // block 2 matures
        assert_eq!(store.written(), vec![1, 2]);
        assert_eq!(w.buffered_blocks(), vec![3], "block 3 stays open");
    }

    /// A shallow reorg — a block re-emitted while still inside the unflushed
    /// window — must be corrected in the buffer: the old fork's higher blocks
    /// are dropped, `head` rewinds, and only the canonical row is ever written.
    /// This is the whole point of the lag, so it must hold before any flush.
    #[tokio::test]
    async fn in_window_reorg_replaces_buffered_rows_before_flush() {
        let store = RecordingStore::default();
        let mut w = window::<_, Ping>(&store, 2);

        // Original fork: blocks 5 and 6 buffered (neither matured yet at depth 2).
        w.record(BlockPosition(5), &ping(50)).await;
        w.record(BlockPosition(6), &ping(60)).await;
        assert_eq!(store.written(), Vec::<u64>::new());

        // Reorg: block 5 re-emitted. Block 6's buffered rows are dropped; head
        // rewinds to 5.
        w.record(BlockPosition(5), &ping(51)).await;
        assert_eq!(
            w.buffered_blocks(),
            vec![5],
            "the old fork's block 6 is gone"
        );

        // Re-advance: the canonical 6, then 7 matures block 5.
        w.record(BlockPosition(6), &ping(61)).await;
        w.record(BlockPosition(7), &ping(70)).await; // head 7: block 5 matures (5+2<=7)
        assert_eq!(
            store.written(),
            vec![5],
            "block 5 written once, after correction"
        );
    }

    /// A reorg deeper than the confirmation depth re-emits an already-flushed
    /// block. That row is finalized — undoing it would need a delete the writer
    /// doesn't do — so persistence halts rather than writing the orphaned and
    /// canonical versions over each other. The stored watermark stays put for a
    /// restart to re-sync from.
    #[tokio::test]
    async fn deep_reorg_past_the_watermark_halts() {
        let store = RecordingStore::default();
        let mut w = window::<_, Ping>(&store, 1);

        w.record(BlockPosition(5), &ping(1)).await;
        w.record(BlockPosition(6), &ping(2)).await; // block 5 flushes (watermark = 5)
        assert_eq!(store.written(), vec![5]);

        // Block 5 re-emitted after being finalized: deeper than depth -> halt.
        w.record(BlockPosition(5), &ping(3)).await;
        // No further writes; later events are ignored.
        w.record(BlockPosition(7), &ping(4)).await;
        assert_eq!(
            store.written(),
            vec![5],
            "nothing written after a deep reorg"
        );
    }

    /// As with [`BlockWriter`](super::super::BlockWriter), an unencodable event
    /// halts the windowed writer rather than being skipped: progress must not
    /// advance past a position whose row was never written, or a restart's replay
    /// exposes the hole.
    #[tokio::test]
    async fn windowed_writer_halts_on_unencodable_event() {
        let store = RecordingStore::default();
        let mut w = window::<_, BadPing>(&store, 2);

        w.record(BlockPosition(1), &bad_ping(1)).await;
        w.record(BlockPosition(2), &bad_ping(0)).await; // unencodable -> halt
        w.record(BlockPosition(3), &bad_ping(3)).await;
        w.record(BlockPosition(4), &bad_ping(4)).await; // would otherwise mature block 1/2
        assert_eq!(store.written(), Vec::<u64>::new());
    }

    /// DEDUP.1/.2 on the live tail: an overlapping re-read is suppressed while a
    /// new same-instant identity is retained; the matured group flushes once
    /// carrying only the new identity. Also exercises depth-based maturity for a
    /// Dedupe (frontier) position. Discriminating: without the dedupe skip the
    /// re-read would either halt (old sort-key `<=` check) or add a second row.
    #[tokio::test]
    async fn dedupe_skip_suppresses_and_matures_in_window() {
        let store = RecordingStore::<TestFrontier>::default();
        // A prior run finalized (2000, {0xc1}).
        let mut w = ConfirmationWindow::<_, TestFrontier, Ping>::new(
            &store,
            record(),
            1,
            Some(TestFrontier::at(2000, 0xc1)),
        );

        // Overlapping re-read of 0xc1: covered by the seed -> suppressed.
        assert!(
            !w.record(TestFrontier::at(2000, 0xc1), &ping(1)).await,
            "the overlapping re-read is deduped against the seed"
        );
        // A new identity at the same instant: delivered, buffered at t=2000.
        assert!(w.record(TestFrontier::at(2000, 0xc2), &ping(2)).await);
        // A later instant matures the 2000 group at depth 1.
        assert!(w.record(TestFrontier::at(2500, 0xd1), &ping(3)).await);

        assert_eq!(
            store.written(),
            vec![2000],
            "the 2000 group matured and flushed once"
        );
        assert_eq!(store.total_rows(), 1, "0xc1 not re-stored");
        assert_eq!(
            store.positions()[0],
            TestFrontier::at(2000, 0xc2),
            "the flushed group carries only the new identity"
        );
    }

    /// Live-tail jitter on a Dedupe (frontier) source: a genuinely-new event
    /// arriving below the head of the confirmation window — while a *higher*
    /// slot is still pending-and-unflushed — must be retained and stored in its
    /// own slot, never suppressed as a re-observation of the higher slot, and
    /// must not drop that higher slot. A same-instant re-observation already
    /// folded into its own slot IS still deduped.
    ///
    /// Discriminating on both halves of the fix:
    ///   * against the old `pending.values().any(...)` covered check, the new
    ///     event at t=2 is reported "contained" by the pending t=3 slot
    ///     (`contains` is true for any earlier instant) and wrongly suppressed —
    ///     the first `assert!(record(...))` here fails;
    ///   * against an *ungated* shallow-reorg rewind, the arrival of t=2 drops
    ///     the pending t=3 slot and rewinds head, so only t=2 is ever
    ///     written — the `written() == [2, 3]` assertion here fails.
    /// Both halves are required to eliminate the silent data loss.
    #[tokio::test]
    async fn dedupe_retains_jittered_new_event_below_pending_head() {
        let store = RecordingStore::<TestFrontier>::default();
        // depth 2 keeps low sort keys pending: a slot matures only at
        // head >= slot + 2, and the instants here are one sort-key apart.
        let mut w = ConfirmationWindow::<_, TestFrontier, Ping>::new(&store, record(), 2, None);

        // A slot at t=3 is buffered and unflushed (head 3, nothing matured yet).
        assert!(w.record(TestFrontier::at(3, 0xd1), &ping(1)).await);
        assert_eq!(w.buffered_blocks(), vec![3]);

        // A jittered NEW identity arrives below the head, at t=2. It shares no
        // sort-key slot with the pending t=3 group, so it is genuinely new and
        // must be delivered (not suppressed). It is a plain insert into its own
        // slot; the pending t=3 slot survives (append-only feeds do not reorg).
        assert!(
            w.record(TestFrontier::at(2, 0xc2), &ping(2)).await,
            "a jittered new event below the head is not a re-observation"
        );
        assert_eq!(
            w.buffered_blocks(),
            vec![2, 3],
            "the new t=2 slot is inserted and the pending t=3 slot is kept"
        );

        // A genuine same-instant re-observation of 0xc2 at t=2, already folded
        // into the t=2 slot, IS deduped by the scoped pending-slot check.
        assert!(
            !w.record(TestFrontier::at(2, 0xc2), &ping(3)).await,
            "the same-instant duplicate in the t=2 slot is deduped"
        );
        assert_eq!(
            w.buffered_blocks(),
            vec![2, 3],
            "the duplicate added nothing"
        );

        // A later instant matures both buffered slots (head 4: 2+2<=4, 3+2>4?
        // no — 3 matures at head>=5). Push to t=5 so both flush.
        assert!(w.record(TestFrontier::at(5, 0xe1), &ping(4)).await);

        assert_eq!(
            store.written(),
            vec![2, 3],
            "both the jittered t=2 event and the pending t=3 event are stored — no data loss"
        );
        assert_eq!(
            store.total_rows(),
            2,
            "one row each for 0xc2 and 0xd1; the duplicate stored nothing"
        );
        assert_eq!(
            store.positions(),
            vec![TestFrontier::at(2, 0xc2), TestFrontier::at(3, 0xd1)],
            "each slot flushes carrying its own identity"
        );
    }
}
