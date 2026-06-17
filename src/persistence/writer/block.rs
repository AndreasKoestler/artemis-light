//! [`BlockWriter`]: persists the finite backfill range, one transaction per
//! complete block.

use std::sync::Arc;

use serde::Serialize;

use super::GapFreeWriter;
use crate::persistence::{Record, Row, Store};

/// Buffers one block's rows at a time and writes each complete block to the
/// store in a single transaction. A block is complete once a higher block
/// number is observed; the trailing block is written only by [`finish`].
///
/// A backwards block halts the writer: the open block's completeness can no
/// longer be trusted, so flushing it would advance the stored height past the
/// late block's rows and leave a permanent hole behind the gap-free watermark.
/// (The live tail tolerates such re-emissions as shallow reorgs — that is
/// [`ConfirmationWindow`](super::ConfirmationWindow)'s job, not this one's.)
///
/// Once the shared [`GapFreeWriter`] goes unhealthy the writer does no per-event
/// work at all — deriving and buffering rows for blocks that will never be
/// written would grow the buffer without bound on a live tail. The event stream
/// itself keeps flowing either way; a restart re-fetches everything after the
/// last good block.
///
/// [`finish`]: BlockWriter::finish
pub(super) struct BlockWriter<'a, S, E> {
    core: GapFreeWriter<'a, S, E>,
    buffer: Vec<Row>,
    current: Option<u64>,
}

impl<'a, S: Store, E: Serialize> BlockWriter<'a, S, E> {
    pub(super) fn new(store: &'a S, record: Arc<Record<E>>) -> Self {
        Self {
            core: GapFreeWriter::new(store, record),
            buffer: Vec::new(),
            current: None,
        }
    }

    /// Buffer one event's row, first flushing the previous block if `block`
    /// has advanced past it. No-op once unhealthy.
    pub(super) async fn record(&mut self, block: u64, event: &E) {
        if !self.core.healthy() {
            return;
        }
        let Some(row) = self.core.encode(event) else {
            // The event can never be written, so its block — and everything
            // after it — must not be either; drop the open block's buffer too.
            self.buffer.clear();
            return;
        };
        if let Some(cur) = self.current {
            // A backwards block means the open block's completeness can no
            // longer be trusted: flushing it would advance the stored height
            // past the late block's rows, leaving a permanent hole behind the
            // gap-free watermark.
            if block < cur {
                self.core.fail(format_args!(
                    "block {block} arrived after block {cur} (reorg or \
                     unordered source)"
                ));
                self.buffer.clear();
                return;
            }
            // The block advanced: the previous block is complete. A failed
            // flush leaves this event's block unwritable without a gap, so stop
            // (the buffer was already taken, so nothing is left to drop).
            if block > cur && !self.core.flush(cur, std::mem::take(&mut self.buffer)).await {
                return;
            }
        }
        self.current = Some(block);
        self.buffer.push(row);
    }

    /// Flush the trailing block. Only correct when the source delivered the
    /// block completely (a finite backfill range, not a live tail).
    pub(super) async fn finish(&mut self) {
        if self.core.healthy()
            && let Some(cur) = self.current
        {
            self.core.flush(cur, std::mem::take(&mut self.buffer)).await;
        }
    }

    /// Rows currently buffered for the open block.
    #[cfg(test)]
    fn buffered(&self) -> usize {
        self.buffer.len()
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_support::{
        BadPing, FailingStore, Ping, RecordingStore, bad_ping, ping, record,
    };
    use super::*;

    fn writer<S, E>(store: &S) -> BlockWriter<'_, S, E>
    where
        S: Store,
        E: alloy::sol_types::SolEvent + Serialize,
    {
        BlockWriter::new(store, record())
    }

    /// Once a write fails, persistence is halted for the rest of the stream —
    /// and a halted writer must stop doing per-event work entirely. Deriving
    /// and buffering rows for blocks that will never be written would grow the
    /// buffer without bound on a live tail: one transient write failure would
    /// become a slow-motion OOM.
    #[tokio::test]
    async fn writer_stops_buffering_once_unhealthy() {
        let store = FailingStore;
        let mut writer = writer::<_, Ping>(&store);

        // Block 1 buffers normally.
        writer.record(1, &ping(1)).await;
        assert_eq!(writer.buffered(), 1);

        // Block 2 arrives: block 1 is complete, its flush fails, and the
        // writer goes unhealthy. Block 2's row must not be buffered either —
        // its block can never be written without leaving a gap.
        writer.record(2, &ping(2)).await;
        assert_eq!(writer.buffered(), 0, "failed flush must clear the buffer");

        // A long tail of further events must not accumulate anything.
        for block in 3..100 {
            writer.record(block, &ping(block)).await;
        }
        assert_eq!(
            writer.buffered(),
            0,
            "an unhealthy writer must not accumulate rows"
        );
    }

    /// A live stream can deliver a lower block after a higher one (a reorg
    /// re-emission, or a misbehaving source). Flushing on *any* block change
    /// would write the higher block — advancing `_artemis_progress` past the
    /// lower block whose rows were never written, so a crash between the two
    /// transactions leaves a permanent hole behind a "gap-free" watermark. A
    /// backwards block must instead halt the writer before anything is
    /// written: the open block's completeness can no longer be trusted.
    #[tokio::test]
    async fn writer_halts_on_non_monotone_blocks_without_writing() {
        let store = RecordingStore::default();
        let mut writer = writer::<_, Ping>(&store);

        writer.record(5, &ping(1)).await;
        writer.record(4, &ping(2)).await; // block went backwards
        writer.record(5, &ping(3)).await; // the reorg's second half
        writer.finish().await;

        assert_eq!(
            store.written(),
            Vec::<u64>::new(),
            "no block may be written once ordering is violated"
        );
        assert_eq!(writer.buffered(), 0);
    }

    /// An event that cannot be encoded into a row must halt persistence, not
    /// be skipped: progress would otherwise advance past its block, and replay
    /// would hand strategies exactly the "quietly truncated history" the read
    /// side refuses to produce (see `replay_stored`). Strategies that ran live
    /// saw the event; strategies after a restart must not silently lose it.
    #[tokio::test]
    async fn writer_halts_on_unencodable_event_instead_of_leaving_a_hole() {
        let store = RecordingStore::default();
        let mut writer = writer::<_, BadPing>(&store);

        writer.record(1, &bad_ping(1)).await;
        writer.record(2, &bad_ping(0)).await; // zero serialises unencodably
        writer.record(3, &bad_ping(3)).await; // would previously flush past block 2
        writer.finish().await;

        assert_eq!(
            store.written(),
            Vec::<u64>::new(),
            "nothing may be written once an event cannot be persisted"
        );
        assert_eq!(writer.buffered(), 0);
    }
}
