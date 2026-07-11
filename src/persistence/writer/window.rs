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
    /// Rows this group held when the live tail first re-emitted its position
    /// (Halt path only) — the still-unconfirmed backfill copy. A live row equal
    /// to one of these is the same event re-delivered across the backfill/live
    /// boundary: it is kept as the group's canonical content but suppressed
    /// downstream (the backfill already delivered it); a row matching nothing
    /// here is genuinely new (a reorg's changed event) and flows through.
    expected: Vec<Row>,
    /// Whether this group was buffered from the backfill tail (see
    /// [`ConfirmationWindow::record_seeded`]) and has not yet been touched by a
    /// live re-emission. A seeded group flushes as-is when it matures; the
    /// live tail's first arrival at its key converts its rows into
    /// [`expected`](Self::expected) and rebuilds the group from the live feed.
    seeded: bool,
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
            // reorg.
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
        // suppress it downstream.
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

        // Shallow reorg: the chain forked above `key`. Rewind the old fork's
        // buffered groups (the node re-emits the canonical ones) and the head,
        // so those groups must re-confirm. Groups strictly below `key` are
        // untouched — they belong to the shared prefix. Halt-policy (block)
        // sources only: a Dedupe (frontier) feed is
        // append-only, so a backwards position is jittered late arrival, not a
        // reorg. It falls through to a plain insert into its own slot below —
        // never a rewind that would drop a higher already-buffered slot and
        // silently lose it.
        if P::REOBSERVATION == Reobservation::Halt
            && let Some(h) = self.head
            && key < h
        {
            self.rewind(key);
            self.head = Some(key);
        }

        // Fold this event into its sort-key group: a union of same-instant
        // identities for a frontier, the same block for `BlockPosition`.
        let delivered = match self.pending.entry(key) {
            Entry::Vacant(slot) => {
                slot.insert(Pending {
                    position,
                    rows: vec![row],
                    expected: Vec::new(),
                    seeded: false,
                });
                true
            }
            Entry::Occupied(mut slot) => {
                let pending = slot.get_mut();
                if P::REOBSERVATION == Reobservation::Halt && pending.seeded {
                    // First live contact with a backfill-seeded group at its
                    // own key: the position is being re-emitted (boundary
                    // overlap, or a reorg at the head), not extended. Rebuild
                    // the group from the live feed, keeping the backfill copy
                    // only as the downstream-dedupe set.
                    pending.expected = std::mem::take(&mut pending.rows);
                    pending.seeded = false;
                }
                pending.position = P::advance(Some(pending.position.clone()), position);
                // A row identical to a still-unconfirmed backfill row is that
                // event re-delivered across the boundary: keep it as canonical
                // content, but the backfill segment already delivered it.
                if let Some(i) = pending.expected.iter().position(|r| *r == row) {
                    pending.expected.swap_remove(i);
                    pending.rows.push(row);
                    false
                } else {
                    pending.rows.push(row);
                    true
                }
            }
        };
        self.head = Some(self.head.map_or(key, |h| h.max(key)));

        self.flush_matured().await;
        delivered
    }

    /// Buffer one backfill-tail event — the last `depth` positions of the gap
    /// query, which must stay pending (not final) so the live tail can still
    /// correct an in-window reorg of them. Groups recorded here are marked
    /// seeded; the live tail's [`record`](Self::record) treats its first
    /// arrival at a seeded key as a re-emission of that position rather than
    /// an extension of it. Returns whether the event should be delivered
    /// downstream, exactly as `record` does.
    ///
    /// The seed phase is one `query_range` snapshot, so there is no reorg
    /// re-emission to correct: a backwards position here is an unordered
    /// source, and — as in [`BlockWriter`](super::BlockWriter) — the buffered
    /// groups' completeness can no longer be trusted, so the writer halts.
    pub(super) async fn record_seeded(&mut self, position: P, event: &E) -> bool {
        if !self.core.healthy() {
            return true;
        }
        let key = position.sort_key();

        if P::REOBSERVATION == Reobservation::Dedupe && self.covered(&position) {
            return false;
        }
        if P::REOBSERVATION == Reobservation::Halt
            && let Some(h) = self.head
            && key < h
        {
            self.core.fail(format_args!(
                "position {position:?} arrived after sort key {h} in the \
                 backfill tail (unordered source)"
            ));
            self.pending.clear();
            return true;
        }
        let Some(row) = self.core.encode(event) else {
            self.pending.clear();
            return true;
        };
        match self.pending.entry(key) {
            Entry::Vacant(slot) => {
                slot.insert(Pending {
                    position,
                    rows: vec![row],
                    expected: Vec::new(),
                    seeded: true,
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

    /// Halt this window's persistence for `reason` and drop the buffer — used
    /// by the writer pipeline when the settled prefix below the window halted,
    /// so flushing anything above it would advance the stored watermark over
    /// the gap.
    pub(super) fn halt(&mut self, reason: std::fmt::Arguments<'_>) {
        self.core.fail(reason);
        self.pending.clear();
    }

    /// Fold the settled (backfill) writer's finalized watermark into this
    /// window's. The window is seeded with only the *sort key* of the settled
    /// boundary ([`Position::from_sort_key`], an empty identity set for a
    /// frontier), so without this fold a live re-delivery of an identity the
    /// settled writer stored *at* the boundary instant would not be covered and
    /// would be stored a second time.
    pub(super) fn absorb(&mut self, settled: Option<P>) {
        if let Some(position) = settled {
            self.flushed = Some(
                self.flushed
                    .map_or(position.sort_key(), |f| f.max(position.sort_key())),
            );
            self.watermark = Some(P::advance(self.watermark.take(), position));
        }
    }

    /// Apply a reorg *retraction* at `position` — the node re-sent a log with
    /// `removed: true`, signalling that events at this position no longer
    /// happened. Only meaningful for [`Reobservation::Halt`] sources (a Dedupe
    /// feed is append-only and never retracts; a stray retraction is ignored).
    ///
    /// A retraction at or below the finalized watermark is a reorg deeper than
    /// the confirmation depth — unfixable without a delete, so halt. Inside the
    /// window it rewinds exactly like a backwards re-emission: live-origin
    /// groups at or above the key are dropped (the node re-emits the canonical
    /// replacements, which may share the retracted key — the same-height reorg
    /// a re-emission alone cannot reveal), seeded groups convert their rows
    /// into the downstream-dedupe set, and `head` rewinds below the key so the
    /// canonical re-fill must re-confirm.
    pub(super) fn retract(&mut self, position: P) {
        if !self.core.healthy() {
            return;
        }
        if P::REOBSERVATION == Reobservation::Dedupe {
            tracing::warn!(
                "ignoring retraction at {position:?}: append-only (Dedupe) \
                 sources do not reorg"
            );
            return;
        }
        let key = position.sort_key();
        if let Some(f) = self.flushed
            && key <= f
        {
            let depth = self.depth;
            self.core.fail(format_args!(
                "position {position:?} retracted at/below the watermark \
                 (reorg deeper than confirmation depth {depth})"
            ));
            self.pending.clear();
            return;
        }
        self.rewind(key);
        self.head = self.head.map(|h| h.min(key.saturating_sub(1)));
    }

    /// Rewind the window for a re-emission at `key`: a live-origin group at or
    /// above it is dropped — the node re-emits the canonical replacements —
    /// while a backfill-seeded group converts its rows into the `expected`
    /// dedupe set and stays in place, so (a) an identical re-delivery across
    /// the backfill/live boundary is suppressed downstream and (b) a canonical
    /// position that re-emits nothing still flushes (empty) instead of leaving
    /// a hole below the advancing watermark.
    fn rewind(&mut self, key: u64) {
        let mut dropped = Vec::new();
        for (&b, pending) in self.pending.range_mut(key..) {
            if pending.seeded {
                pending.expected = std::mem::take(&mut pending.rows);
                pending.seeded = false;
            } else {
                dropped.push(b);
            }
        }
        for b in dropped {
            self.pending.remove(&b);
        }
    }

    /// Whether `pos` is already covered by the finalized watermark or by the
    /// pending (buffered-but-unflushed) group at `pos`'s own sort key — the
    /// dedupe test for [`Reobservation::Dedupe`] positions.
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
        // Saturating: at u64::MAX a group can never be buried deeper, so it
        // matures at once instead of overflowing the add.
        let matured: Vec<u64> = self
            .pending
            .keys()
            .copied()
            .filter(|&b| head >= b.saturating_add(self.depth))
            .collect();
        for b in matured {
            let Some(pending) = self.pending.remove(&b) else {
                continue;
            };
            let Pending { position, rows, .. } = pending;
            // A failed write means a later group must not advance the stored
            // watermark past the gap; drop the rest of the window and stop. The
            // shared core has already gone unhealthy and logged.
            if !self.core.flush(position.clone(), rows).await {
                self.pending.clear();
                return;
            }
            // Max, not overwrite: a Dedupe position whose `contains` is scoped
            // to its own identity can flush a jittered lower slot after a
            // higher one, and the finalized watermark must never regress.
            self.flushed = Some(self.flushed.map_or(b, |f| f.max(b)));
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

    /// The finalized watermark's sort key.
    #[cfg(test)]
    fn flushed_key(&self) -> Option<u64> {
        self.flushed
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

    /// Backfill-tail seeds stay pending (never flushed by the seed phase) until
    /// buried `depth` deep, and a second event at a seeded key folds into the
    /// same buffered group rather than opening a new one.
    #[tokio::test]
    async fn seeded_groups_accumulate_and_stay_pending_until_buried() {
        let store = RecordingStore::default();
        let mut w = window::<_, Ping>(&store, 2);

        assert!(w.record_seeded(BlockPosition(1), &ping(1)).await);
        assert!(w.record_seeded(BlockPosition(1), &ping(1)).await); // occupied: folds in
        assert!(w.record_seeded(BlockPosition(2), &ping(2)).await);

        // At depth 2, head 2 buries nothing (block 1 needs head >= 3).
        assert_eq!(store.written(), Vec::<u64>::new());
        assert_eq!(w.buffered_blocks(), vec![1, 2]);
    }

    /// An unencodable event in the seed phase can't be trusted to sit in a
    /// half-built buffer, so — as in the live path — the window drops its buffer.
    #[tokio::test]
    async fn seeding_an_unencodable_event_drops_the_buffer() {
        let store = RecordingStore::default();
        let mut w = window::<_, BadPing>(&store, 2);

        assert!(w.record_seeded(BlockPosition(1), &bad_ping(1)).await);
        assert!(w.record_seeded(BlockPosition(2), &bad_ping(0)).await); // unencodable
        assert!(w.buffered_blocks().is_empty(), "the buffer is dropped");
    }

    /// The seed phase is one snapshot, so a backwards sort key is an unordered
    /// source, not a reorg re-emission (a `Halt` position): the window halts and
    /// clears rather than silently trusting the buffered groups.
    #[tokio::test]
    async fn seeding_a_backwards_position_halts_as_an_unordered_source() {
        let store = RecordingStore::default();
        let mut w = window::<_, Ping>(&store, 2);

        assert!(w.record_seeded(BlockPosition(5), &ping(5)).await);
        assert!(w.record_seeded(BlockPosition(4), &ping(4)).await); // 4 < head 5 -> halt
        assert!(w.buffered_blocks().is_empty(), "halt clears the buffer");
    }

    /// `halt` is the writer pipeline's signal that the settled prefix below the
    /// window failed: it marks the window unhealthy and drops the buffer so
    /// nothing above the gap can advance the stored watermark.
    #[tokio::test]
    async fn halt_marks_unhealthy_and_clears_the_buffer() {
        let store = RecordingStore::default();
        let mut w = window::<_, Ping>(&store, 2);

        w.record(BlockPosition(1), &ping(1)).await;
        assert_eq!(w.buffered_blocks(), vec![1]);

        w.halt(format_args!("settled prefix below the window halted"));
        assert!(w.buffered_blocks().is_empty());
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

    /// A same-height reorg — block `N` replaced by `N′` before `N + 1` arrives —
    /// re-emits at `key == head`, which a re-emission alone cannot distinguish
    /// from another event in the same block. The node's `removed`-log
    /// *retraction* is the only signal, and it must drop the orphaned buffered
    /// rows so only the replacement's rows are ever written.
    #[tokio::test]
    async fn retraction_drops_orphaned_rows_of_a_same_height_reorg() {
        let store = RecordingStore::default();
        let mut w = window::<_, Ping>(&store, 2);

        // Block 6 with the soon-to-be-orphaned event A.
        w.record(BlockPosition(6), &ping(60)).await;
        // The reorg: the node retracts block 6's logs...
        w.retract(BlockPosition(6));
        assert_eq!(w.buffered_blocks(), Vec::<u64>::new(), "orphan dropped");
        // ...and re-emits the replacement 6′ with A′, then the chain advances.
        w.record(BlockPosition(6), &ping(61)).await;
        w.record(BlockPosition(7), &ping(70)).await;
        w.record(BlockPosition(8), &ping(80)).await; // block 6 matures (6+2<=8)

        assert_eq!(store.written(), vec![6], "block 6 written once");
        assert_eq!(
            store.total_rows(),
            1,
            "only the replacement row is stored; the orphan is gone"
        );
    }

    /// A retraction at or below the finalized watermark is a reorg deeper than
    /// the confirmation depth: the orphaned row is already final, so the writer
    /// halts rather than leaving it silently wrong.
    #[tokio::test]
    async fn retraction_at_or_below_the_watermark_halts() {
        let store = RecordingStore::default();
        let mut w = window::<_, Ping>(&store, 1);

        w.record(BlockPosition(5), &ping(1)).await;
        w.record(BlockPosition(6), &ping(2)).await; // block 5 flushes (watermark 5)
        assert_eq!(store.written(), vec![5]);

        w.retract(BlockPosition(5));
        w.record(BlockPosition(7), &ping(3)).await; // would mature block 6
        assert_eq!(store.written(), vec![5], "nothing written after the halt");
    }

    /// Absorbing the settled (backfill) writer's watermark closes the boundary
    /// leak: an identity the settled writer stored *at* the cut instant must
    /// dedupe when the live tail re-delivers it, not store a second row.
    #[tokio::test]
    async fn absorbed_settled_watermark_dedupes_boundary_instant_redelivery() {
        let store = RecordingStore::<TestFrontier>::default();
        // Window seeded as `persist_and_emit` does: the cut's sort key with an
        // EMPTY identity set.
        let mut w = ConfirmationWindow::<_, TestFrontier, Ping>::new(
            &store,
            record(),
            1,
            Some(TestFrontier::from_sort_key(4999)),
        );
        // The settled writer stored identity 0xc1 at the cut instant 4999.
        w.absorb(Some(TestFrontier::at(4999, 0xc1)));

        // The live tail re-delivers 0xc1@4999 (the pre-tip subscription race):
        // covered by the absorbed watermark -> suppressed, no row.
        assert!(
            !w.record(TestFrontier::at(4999, 0xc1), &ping(1)).await,
            "the boundary-instant re-delivery is deduped"
        );
        // A genuinely new identity at the boundary instant still flows.
        assert!(w.record(TestFrontier::at(4999, 0xc2), &ping(2)).await);
        w.record(TestFrontier::at(6000, 0xd1), &ping(3)).await; // matures 4999

        assert_eq!(store.written(), vec![4999]);
        assert_eq!(store.total_rows(), 1, "0xc1 never re-stored");
        assert_eq!(store.positions()[0], TestFrontier::at(4999, 0xc2));
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

    /// The maturity check `head >= key + depth` must saturate: near `u64::MAX`
    /// the unchecked add panics in debug builds and wraps in release. At
    /// saturation a group can never be buried deeper, so it flushes at once.
    #[tokio::test]
    async fn maturity_arithmetic_saturates_at_u64_max() {
        let store = RecordingStore::default();
        let mut w = window::<_, Ping>(&store, 2);

        w.record(BlockPosition(u64::MAX), &ping(1)).await;
        assert_eq!(store.written(), vec![u64::MAX]);
    }

    /// On the live tail an overlapping re-read is suppressed while a
    /// new same-instant identity is retained; the matured group flushes once
    /// carrying only the new identity. Also exercises depth-based maturity for a
    /// Dedupe (frontier) position. Without the dedupe skip the
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

    /// A Dedupe position whose `contains` covers only its own identity — unlike
    /// a time frontier it does NOT cover earlier slots, so a jittered lower
    /// slot can still reach the buffer after a higher one has flushed.
    #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    struct SlotId {
        slot: u64,
        id: u64,
    }

    impl Position for SlotId {
        const REOBSERVATION: crate::persistence::Reobservation =
            crate::persistence::Reobservation::Dedupe;

        fn sort_key(&self) -> u64 {
            self.slot
        }
        fn from_sort_key(key: u64) -> Self {
            SlotId { slot: key, id: 0 }
        }
        fn advance(prev: Option<Self>, next: Self) -> Self {
            match prev {
                Some(prev) if prev.slot >= next.slot => prev,
                _ => next,
            }
        }
        fn contains(&self, pos: &Self) -> bool {
            self == pos
        }
        fn resume_key(&self) -> u64 {
            self.slot
        }
        fn encode(&self) -> String {
            serde_json::to_string(self).expect("SlotId serialises")
        }
        fn decode(encoded: &str) -> anyhow::Result<Self> {
            Ok(serde_json::from_str(encoded)?)
        }
    }

    /// Flushing a jittered lower slot after a higher one (possible on a Dedupe
    /// position whose `contains` is scoped to its own identity) must not walk
    /// the finalized watermark's sort key backwards.
    #[tokio::test]
    async fn flushed_watermark_never_regresses_on_jittered_dedupe_flush() {
        let store = RecordingStore::<SlotId>::default();
        let mut w = ConfirmationWindow::<_, SlotId, Ping>::new(&store, record(), 1, None);

        w.record(SlotId { slot: 5, id: 1 }, &ping(1)).await;
        w.record(SlotId { slot: 7, id: 2 }, &ping(2)).await; // slot 5 matures
        assert_eq!(w.flushed_key(), Some(5));

        // A jittered, genuinely-new lower slot: stored (Dedupe never halts),
        // but its flush must not regress the watermark below 5.
        w.record(SlotId { slot: 3, id: 3 }, &ping(3)).await; // matures at head 7
        assert_eq!(store.written(), vec![5, 3], "the late slot is stored");
        assert_eq!(
            w.flushed_key(),
            Some(5),
            "the finalized watermark must never regress"
        );
    }

    /// Live-tail jitter on a Dedupe (frontier) source: a genuinely-new event
    /// arriving below the head of the confirmation window — while a *higher*
    /// slot is still pending-and-unflushed — must be retained and stored in its
    /// own slot, never suppressed as a re-observation of the higher slot, and
    /// must not drop that higher slot. A same-instant re-observation already
    /// folded into its own slot IS still deduped.
    ///
    /// Both halves of the fix are exercised:
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
