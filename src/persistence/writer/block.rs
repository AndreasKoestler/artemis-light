//! [`BlockWriter`]: persists the finite backfill range, one transaction per
//! complete position-group.

use std::sync::Arc;

use serde::Serialize;

use super::GapFreeWriter;
use crate::persistence::{Position, Record, Reobservation, Row, Store};

/// Buffers one position-group's rows at a time and writes each complete group to
/// the store in a single transaction. A group is complete once a higher sort key
/// is observed; the trailing group is written only by [`finish`].
///
/// A backwards position halts the writer: the open group's completeness can no
/// longer be trusted, so flushing it would advance the stored watermark past the
/// late position's rows and leave a permanent hole behind the gap-free watermark.
/// (The live tail tolerates such re-emissions as shallow reorgs — that is
/// [`ConfirmationWindow`](super::ConfirmationWindow)'s job, not this one's.)
///
/// Once the shared [`GapFreeWriter`] goes unhealthy the writer does no per-event
/// work at all — deriving and buffering rows for positions that will never be
/// written would grow the buffer without bound on a live tail. The event stream
/// itself keeps flowing either way; a restart re-fetches everything after the
/// last good position.
///
/// [`finish`]: BlockWriter::finish
pub(super) struct BlockWriter<'a, S, P, E> {
    core: GapFreeWriter<'a, S, E>,
    buffer: Vec<Row>,
    /// The open group's position, folded across all same-sort-key events via
    /// [`Position::advance`] (a union for a frontier; the same block for
    /// `BlockPosition`).
    current: Option<P>,
    /// The finalized watermark: seeded from the stored position at subscribe and
    /// advanced as groups flush. Consulted only on the dedupe path
    /// ([`Reobservation::Dedupe`]); dead on the Halt (block) path.
    watermark: Option<P>,
}

impl<'a, S, P, E> BlockWriter<'a, S, P, E>
where
    S: Store<P>,
    P: Position,
    E: Serialize,
{
    pub(super) fn new(store: &'a S, record: Arc<Record<E>>, seed: Option<P>) -> Self {
        Self {
            core: GapFreeWriter::new(store, record),
            buffer: Vec::new(),
            current: None,
            watermark: seed,
        }
    }

    /// Buffer one event's row, first flushing the previous group if `position`
    /// has advanced past it. Returns whether the event should be delivered
    /// downstream: `false` only for a [`Reobservation::Dedupe`] position already
    /// covered by the watermark (a suppressed re-observation), `true` otherwise.
    /// No per-event work once unhealthy — but the event still flows (`true`),
    /// because a halt freezes persistence, not the event stream.
    pub(super) async fn record(&mut self, position: P, event: &E) -> bool {
        if !self.core.healthy() {
            return true;
        }
        // Dedupe (Reobservation::Dedupe only): an event already covered by the
        // finalized watermark or the open group's fold is a re-observation
        // across an overlapping backfill — encode no row, touch no buffer, and
        // suppress it downstream. The Halt (block) path never dedupes, so its
        // buffering and emission stay bit-identical.
        if P::REOBSERVATION == Reobservation::Dedupe && self.covered(&position) {
            return false;
        }
        let Some(row) = self.core.encode(event) else {
            // The event can never be written, so its position — and everything
            // after it — must not be either; drop the open group's buffer too.
            self.buffer.clear();
            return true;
        };
        if let Some(cur) = self.current.clone() {
            let cur_key = cur.sort_key();
            let pos_key = position.sort_key();
            // A backwards position means the open group's completeness can no
            // longer be trusted: flushing it would advance the stored watermark
            // past the late position's rows, leaving a permanent hole behind the
            // gap-free watermark. On a Dedupe source this is an out-of-order
            // genuinely-new event (a covered re-observation was suppressed
            // above): the single-group writer cannot reorder, so it halts too —
            // the watermark stays put and a restart's backfill re-fetches it.
            if pos_key < cur_key {
                self.core.fail(format_args!(
                    "position {position:?} arrived after {cur:?} (reorg or \
                     unordered source)"
                ));
                self.buffer.clear();
                return true;
            }
            // The position advanced: the previous group is complete. A failed
            // flush leaves this event's group unwritable without a gap, so stop
            // (the buffer was already taken, so nothing is left to drop).
            if pos_key > cur_key {
                self.current = None;
                if !self
                    .core
                    .flush(cur.clone(), std::mem::take(&mut self.buffer))
                    .await
                {
                    return true;
                }
                // Fold the just-flushed group into the finalized watermark so a
                // later re-observation of a covered identity still dedupes.
                self.watermark = Some(P::advance(self.watermark.take(), cur));
            }
        }
        // Fold this event's position into the open group (a union at the same
        // instant for a frontier; the same block for `BlockPosition`).
        self.current = Some(P::advance(self.current.take(), position));
        self.buffer.push(row);
        true
    }

    /// Whether `pos` is already covered by the finalized watermark or the open
    /// group's fold — the dedupe test for [`Reobservation::Dedupe`] positions.
    ///
    /// The open-group check is scoped to the group's own sort key, as in
    /// [`ConfirmationWindow`](super::ConfirmationWindow): a frontier's
    /// `contains` is true for ANY strictly-earlier instant, and `query_range`
    /// promises no sort-key ordering, so an unscoped check would report an
    /// out-of-order genuinely-new event as a re-observation and silently lose
    /// it. Only the watermark — actually-stored history — covers earlier keys;
    /// an uncovered event below the open group falls through to the
    /// backwards-position halt.
    fn covered(&self, pos: &P) -> bool {
        self.watermark.as_ref().is_some_and(|w| w.contains(pos))
            || self
                .current
                .as_ref()
                .is_some_and(|c| c.sort_key() == pos.sort_key() && c.contains(pos))
    }

    /// Whether the writer may still persist (see [`GapFreeWriter::healthy`]).
    pub(super) fn healthy(&self) -> bool {
        self.core.healthy()
    }

    /// Flush the trailing group. Only correct when the source delivered the
    /// group completely (a finite backfill range, not a live tail).
    pub(super) async fn finish(&mut self) {
        if self.core.healthy()
            && let Some(cur) = self.current.take()
            && self
                .core
                .flush(cur.clone(), std::mem::take(&mut self.buffer))
                .await
        {
            // Fold the trailing group too, so the watermark handed to the
            // confirmation window covers everything this writer stored.
            self.watermark = Some(P::advance(self.watermark.take(), cur));
        }
    }

    /// The finalized watermark after [`finish`](Self::finish): everything this
    /// writer stored, folded over the seed. The window above the settled range
    /// absorbs it so a live re-delivery of a boundary-instant identity dedupes
    /// instead of storing a second row.
    pub(super) fn watermark(&self) -> Option<P> {
        self.watermark.clone()
    }

    /// Rows currently buffered for the open group.
    #[cfg(test)]
    fn buffered(&self) -> usize {
        self.buffer.len()
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_support::{
        BadPing, FailingStore, Ping, RecordingStore, TestFrontier, bad_ping, ping, record,
    };
    use super::*;
    use crate::persistence::BlockPosition;

    fn writer<S, E>(store: &S) -> BlockWriter<'_, S, BlockPosition, E>
    where
        S: Store<BlockPosition>,
        E: alloy::sol_types::SolEvent + Serialize,
    {
        BlockWriter::new(store, record(), None)
    }

    /// Once a write fails, persistence is halted for the rest of the stream —
    /// and a halted writer must stop doing per-event work entirely. Deriving
    /// and buffering rows for positions that will never be written would grow the
    /// buffer without bound on a live tail: one transient write failure would
    /// become a slow-motion OOM.
    #[tokio::test]
    async fn writer_stops_buffering_once_unhealthy() {
        let store = FailingStore;
        let mut writer = writer::<_, Ping>(&store);

        // Block 1 buffers normally.
        writer.record(BlockPosition(1), &ping(1)).await;
        assert_eq!(writer.buffered(), 1);

        // Block 2 arrives: block 1 is complete, its flush fails, and the
        // writer goes unhealthy. Block 2's row must not be buffered either —
        // its block can never be written without leaving a gap.
        writer.record(BlockPosition(2), &ping(2)).await;
        assert_eq!(writer.buffered(), 0, "failed flush must clear the buffer");

        // A long tail of further events must not accumulate anything.
        for block in 3..100 {
            writer.record(BlockPosition(block), &ping(block)).await;
        }
        assert_eq!(
            writer.buffered(),
            0,
            "an unhealthy writer must not accumulate rows"
        );
    }

    /// A live stream can deliver a lower position after a higher one (a reorg
    /// re-emission, or a misbehaving source). Flushing on *any* position change
    /// would write the higher group — advancing `_artemis_progress` past the
    /// lower position whose rows were never written, so a crash between the two
    /// transactions leaves a permanent hole behind a "gap-free" watermark. A
    /// backwards position must instead halt the writer before anything is
    /// written: the open group's completeness can no longer be trusted.
    #[tokio::test]
    async fn writer_halts_on_non_monotone_blocks_without_writing() {
        let store = RecordingStore::default();
        let mut writer = writer::<_, Ping>(&store);

        writer.record(BlockPosition(5), &ping(1)).await;
        writer.record(BlockPosition(4), &ping(2)).await; // position went backwards
        writer.record(BlockPosition(5), &ping(3)).await; // the reorg's second half
        writer.finish().await;

        assert_eq!(
            store.written(),
            Vec::<u64>::new(),
            "no group may be written once ordering is violated"
        );
        assert_eq!(writer.buffered(), 0);
    }

    /// An event that cannot be encoded into a row must halt persistence, not
    /// be skipped: progress would otherwise advance past its position, and replay
    /// would hand strategies exactly the "quietly truncated history" the read
    /// side refuses to produce (see `replay_stored`). Strategies that ran live
    /// saw the event; strategies after a restart must not silently lose it.
    #[tokio::test]
    async fn writer_halts_on_unencodable_event_instead_of_leaving_a_hole() {
        let store = RecordingStore::default();
        let mut writer = writer::<_, BadPing>(&store);

        writer.record(BlockPosition(1), &bad_ping(1)).await;
        writer.record(BlockPosition(2), &bad_ping(0)).await; // zero serialises unencodably
        writer.record(BlockPosition(3), &bad_ping(3)).await; // would previously flush past block 2
        writer.finish().await;

        assert_eq!(
            store.written(),
            Vec::<u64>::new(),
            "nothing may be written once an event cannot be persisted"
        );
        assert_eq!(writer.buffered(), 0);
    }

    /// A frontier (Dedupe) `BlockWriter` over the recording store.
    fn frontier_writer(
        store: &RecordingStore<TestFrontier>,
        seed: Option<TestFrontier>,
    ) -> BlockWriter<'_, RecordingStore<TestFrontier>, TestFrontier, Ping> {
        BlockWriter::new(store, record(), seed)
    }

    /// A re-observed identity already covered by the open group's
    /// fold is suppressed downstream (`record` returns false) and stored zero
    /// additional times — exactly-once persisted effect over an at-least-once
    /// re-read. Without the dedupe skip, `total_rows` would be 2.
    #[tokio::test]
    async fn dedupe_skip_stores_one_row_and_suppresses_downstream() {
        let store = RecordingStore::<TestFrontier>::default();
        let mut writer = frontier_writer(&store, None);

        // First sighting of 0xc1 at t=2000: delivered and buffered.
        assert!(
            writer.record(TestFrontier::at(2000, 0xc1), &ping(1)).await,
            "first sighting is delivered"
        );
        // Re-observation of 0xc1 at the same instant: covered by the open
        // group's fold -> suppressed, no second row.
        assert!(
            !writer.record(TestFrontier::at(2000, 0xc1), &ping(1)).await,
            "the re-observation is suppressed downstream"
        );
        writer.finish().await;

        assert_eq!(store.written(), vec![2000], "one group written");
        assert_eq!(
            store.total_rows(),
            1,
            "0xc1 stored exactly once despite the re-observation"
        );
    }

    /// The backfill watermark seeded from the stored position
    /// dedupes an overlapping re-read while still storing a genuinely new
    /// identity at the same instant — the real resume/overlap scenario.
    #[tokio::test]
    async fn seeded_watermark_dedupes_overlapping_reread() {
        let store = RecordingStore::<TestFrontier>::default();
        // A prior run stored up to (2000, {0xc1}).
        let mut writer = frontier_writer(&store, Some(TestFrontier::at(2000, 0xc1)));

        // Overlapping re-read of 0xc1: covered by the seed -> suppressed.
        assert!(
            !writer.record(TestFrontier::at(2000, 0xc1), &ping(1)).await,
            "the overlapping re-read is deduped against the seed"
        );
        // A new identity at the same instant: not covered -> delivered.
        assert!(writer.record(TestFrontier::at(2000, 0xc2), &ping(2)).await);
        // A later instant: delivered, completing and flushing the 2000 group.
        assert!(writer.record(TestFrontier::at(2500, 0xd1), &ping(3)).await);
        writer.finish().await;

        assert_eq!(
            store.written(),
            vec![2000, 2500],
            "two groups written, none for the re-read"
        );
        assert_eq!(store.total_rows(), 2, "0xc1 never re-stored");
        assert_eq!(
            store.positions()[0],
            TestFrontier::at(2000, 0xc2),
            "the 2000 group carries only the new identity"
        );
    }

    /// `query_range` promises no sort-key ordering, and a frontier's `contains`
    /// is true for ANY strictly-earlier instant — so an out-of-order,
    /// genuinely-new event during backfill must not be swallowed by the open
    /// group's coverage. The covered test is scoped to the open group's own
    /// sort key; below it, the single-group writer cannot reorder, so it halts
    /// (nothing flushes past the unwritten event, and the unadvanced watermark
    /// lets a restart re-fetch it) while the event still flows downstream.
    #[tokio::test]
    async fn out_of_order_new_event_is_not_silently_suppressed() {
        let store = RecordingStore::<TestFrontier>::default();
        let mut writer = frontier_writer(&store, None);

        assert!(writer.record(TestFrontier::at(2000, 0xc1), &ping(1)).await);
        // Genuinely new, merely out of order: it was never stored and is not in
        // the open group's slot, so it must not be suppressed as a
        // re-observation.
        assert!(
            writer.record(TestFrontier::at(1500, 0xb1), &ping(2)).await,
            "an uncovered out-of-order event must not be suppressed"
        );
        writer.finish().await;

        assert_eq!(
            store.written(),
            Vec::<u64>::new(),
            "no group may flush past the unordered event's unwritten position"
        );
    }

    /// The open group folds (unions) every event sharing one sort key into a
    /// single group whose position is the union of their identities.
    /// An overwrite or first-wins fold would leave a singleton.
    #[tokio::test]
    async fn same_key_group_fold_unions_events() {
        let store = RecordingStore::<TestFrontier>::default();
        let mut writer = frontier_writer(&store, None);

        assert!(writer.record(TestFrontier::at(1000, 1), &ping(1)).await);
        assert!(writer.record(TestFrontier::at(1000, 2), &ping(2)).await);
        writer.finish().await;

        let positions = store.positions();
        assert_eq!(
            positions.len(),
            1,
            "same-instant events fold into one group"
        );
        assert_eq!(
            positions[0],
            TestFrontier {
                time: 1000,
                seen: [1, 2].into_iter().collect()
            },
            "the group's position is the union of both identities"
        );
        assert_eq!(
            store.total_rows(),
            2,
            "both rows persisted in the one group"
        );
    }
}
