//! [`ConfirmationWindow`]: persists the live tail, lagging the live edge by a
//! confirmation depth so a shallow reorg is corrected before any orphaned row
//! is written.

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::Serialize;

use super::GapFreeWriter;
use crate::persistence::{Record, Store};

/// Buffers the most recent `depth` blocks of a live tail and writes a block
/// only once it is buried `depth` blocks deep (`head >= block + depth`). Unlike
/// [`BlockWriter`](super::BlockWriter), a backwards block within the unflushed
/// window is treated as a reorg re-emission and corrected in place rather than
/// halting: the node has rewound to a forked block whose row was never written,
/// so dropping the old fork's buffered blocks and rewinding `head` lets the
/// canonical chain re-fill the window. Only a write at or below the
/// already-flushed watermark (a reorg *deeper* than `depth`, which would need a
/// delete to undo) or an unencodable event halts.
///
/// The unflushed window is deliberately never drained at stream end: there is
/// no "stream end" on a live tail, and on a restart the Backfill segment
/// re-fetches the whole window (`[last+1 ..= tip]`) from the canonical chain.
pub(super) struct ConfirmationWindow<'a, S, E> {
    core: GapFreeWriter<'a, S, E>,
    /// Blocks buried this many deep are mature and get written.
    depth: u64,
    /// Buffered rows keyed by block number, lowest first — the order they must
    /// be flushed in to keep the stored height a gap-free prefix.
    pending: BTreeMap<u64, Vec<crate::persistence::Row>>,
    /// Highest block number seen so far; maturity is measured against it.
    head: Option<u64>,
    /// Highest block already written — the finalized watermark. A re-emission
    /// at or below it is a reorg deeper than `depth`.
    flushed: Option<u64>,
}

impl<'a, S: Store, E: Serialize> ConfirmationWindow<'a, S, E> {
    pub(super) fn new(store: &'a S, record: Arc<Record<E>>, depth: u64) -> Self {
        Self {
            core: GapFreeWriter::new(store, record),
            depth,
            pending: BTreeMap::new(),
            head: None,
            flushed: None,
        }
    }

    /// Buffer one event's row, correcting an in-window reorg, then flush every
    /// block that has matured to `depth` confirmations. No-op once unhealthy.
    pub(super) async fn record(&mut self, block: u64, event: &E) {
        if !self.core.healthy() {
            return;
        }

        // A finalized block being rewritten is a reorg deeper than `depth`:
        // unfixable without a delete, so halt (the stored height stays; a
        // restart re-syncs).
        if let Some(f) = self.flushed
            && block <= f
        {
            // Read `depth` into a local first: `fail` takes `&mut self.core`,
            // so the format args may not also borrow `self` immutably.
            let depth = self.depth;
            self.core.fail(format_args!(
                "block {block} rewritten at/below the finalized watermark {f} \
                 (reorg deeper than confirmation depth {depth})"
            ));
            self.pending.clear();
            return;
        }

        let Some(row) = self.core.encode(event) else {
            // As in BlockWriter: an unencodable event must not be skipped, or
            // progress advances past a hole replay would expose.
            self.pending.clear();
            return;
        };

        // Shallow reorg: the chain forked above `block`. Drop the old fork's
        // buffered blocks (the node re-emits the canonical ones) and rewind the
        // head so those blocks must re-confirm. Blocks strictly below `block`
        // are untouched — they belong to the shared prefix.
        if let Some(h) = self.head
            && block < h
        {
            self.pending.retain(|&b, _| b < block);
            self.head = Some(block);
        }

        self.pending.entry(block).or_default().push(row);
        self.head = Some(self.head.map_or(block, |h| h.max(block)));

        self.flush_matured().await;
    }

    /// Flush every buffered block now buried `depth` deep, lowest first.
    async fn flush_matured(&mut self) {
        let Some(head) = self.head else { return };
        // Collect the matured block numbers first to avoid borrowing `pending`
        // across the await inside the flush loop.
        let matured: Vec<u64> = self
            .pending
            .keys()
            .copied()
            .filter(|&b| head >= b + self.depth)
            .collect();
        for b in matured {
            let rows = self.pending.remove(&b).unwrap_or_default();
            // A failed write means a later block must not advance the stored
            // height past the gap; drop the rest of the window and stop. The
            // shared core has already gone unhealthy and logged.
            if !self.core.flush(b, rows).await {
                self.pending.clear();
                return;
            }
            self.flushed = Some(b);
        }
    }

    /// Block numbers currently buffered, lowest first.
    #[cfg(test)]
    fn buffered_blocks(&self) -> Vec<u64> {
        self.pending.keys().copied().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_support::{BadPing, Ping, RecordingStore, bad_ping, ping, record};
    use super::*;

    fn window<S, E>(store: &S, depth: u64) -> ConfirmationWindow<'_, S, E>
    where
        S: Store,
        E: alloy::sol_types::SolEvent + Serialize,
    {
        ConfirmationWindow::new(store, record(), depth)
    }

    /// At depth `n` a block is written only once a block `n` higher arrives
    /// (`head >= block + n`). The window holds the most recent `n` blocks
    /// unwritten so a shallow reorg can still rewrite them.
    #[tokio::test]
    async fn windowed_writer_flushes_only_blocks_buried_depth_deep() {
        let store = RecordingStore::default();
        let mut w = window::<_, Ping>(&store, 2);

        w.record(1, &ping(1)).await; // head 1: nothing matured
        w.record(2, &ping(2)).await; // head 2: block 1 needs head>=3
        assert_eq!(store.written(), Vec::<u64>::new());

        w.record(3, &ping(3)).await; // head 3: block 1 matures (1+2<=3)
        assert_eq!(store.written(), vec![1]);

        w.record(4, &ping(4)).await; // head 4: block 2 matures
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

        w.record(1, &ping(1)).await;
        w.record(2, &ping(2)).await; // block 1 matures (1+1<=2)
        w.record(3, &ping(3)).await; // block 2 matures
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
        w.record(5, &ping(50)).await;
        w.record(6, &ping(60)).await;
        assert_eq!(store.written(), Vec::<u64>::new());

        // Reorg: block 5 re-emitted. Block 6's buffered rows are dropped; head
        // rewinds to 5.
        w.record(5, &ping(51)).await;
        assert_eq!(
            w.buffered_blocks(),
            vec![5],
            "the old fork's block 6 is gone"
        );

        // Re-advance: the canonical 6, then 7 matures block 5.
        w.record(6, &ping(61)).await;
        w.record(7, &ping(70)).await; // head 7: block 5 matures (5+2<=7)
        assert_eq!(
            store.written(),
            vec![5],
            "block 5 written once, after correction"
        );
    }

    /// A reorg deeper than the confirmation depth re-emits an already-flushed
    /// block. That row is finalized — undoing it would need a delete the writer
    /// doesn't do — so persistence halts rather than writing the orphaned and
    /// canonical versions over each other. The stored height stays put for a
    /// restart to re-sync from.
    #[tokio::test]
    async fn deep_reorg_past_the_watermark_halts() {
        let store = RecordingStore::default();
        let mut w = window::<_, Ping>(&store, 1);

        w.record(5, &ping(1)).await;
        w.record(6, &ping(2)).await; // block 5 flushes (watermark = 5)
        assert_eq!(store.written(), vec![5]);

        // Block 5 re-emitted after being finalized: deeper than depth -> halt.
        w.record(5, &ping(3)).await;
        // No further writes; later events are ignored.
        w.record(7, &ping(4)).await;
        assert_eq!(
            store.written(),
            vec![5],
            "nothing written after a deep reorg"
        );
    }

    /// As with [`BlockWriter`](super::super::BlockWriter), an unencodable event
    /// halts the windowed writer rather than being skipped: progress must not
    /// advance past a block whose row was never written, or a restart's replay
    /// exposes the hole.
    #[tokio::test]
    async fn windowed_writer_halts_on_unencodable_event() {
        let store = RecordingStore::default();
        let mut w = window::<_, BadPing>(&store, 2);

        w.record(1, &bad_ping(1)).await;
        w.record(2, &bad_ping(0)).await; // unencodable -> halt
        w.record(3, &bad_ping(3)).await;
        w.record(4, &bad_ping(4)).await; // would otherwise mature block 1/2
        assert_eq!(store.written(), Vec::<u64>::new());
    }
}
