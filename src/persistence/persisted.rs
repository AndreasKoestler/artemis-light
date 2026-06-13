//! [`Persisted`]: a [`Collector`] wrapper that records every event it sees and,
//! on subscribe, replays stored history before following the chain tip.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use alloy::sol_types::SolEvent;
use anyhow::Result;
use async_trait::async_trait;
use futures::StreamExt;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio_util::sync::CancellationToken;

use super::record::Record;
use super::schema::{Row, SqlValue, TableSchema};
use super::store::Store;
use crate::types::{Collector, CollectorStream};

/// A collector that is aware of block numbers and can replay historical block
/// ranges — the capability [`Persisted`] needs to record events and fill the
/// gap between the last stored block and the chain tip.
///
/// Implemented by collectors that wrap a queryable source (e.g. an
/// `EventCollector` over alloy's `Event`).
#[async_trait]
pub trait PersistableCollector<E>: Send + Sync {
    /// Live, block-numbered events from the chain tip onward.
    async fn subscribe_indexed(&self) -> Result<CollectorStream<'_, (u64, E)>>;

    /// Historical block-numbered events for the inclusive range `from..=to`.
    async fn query_range(&self, from: u64, to: u64) -> Result<CollectorStream<'_, (u64, E)>>;

    /// The current chain tip (latest block number).
    async fn tip(&self) -> Result<u64>;
}

/// Extension method that wraps a [`PersistableCollector`] with a [`Store`].
pub trait PersistExt<E>: PersistableCollector<E> + Sized {
    /// Record every event into `store`, replaying stored history on subscribe.
    fn with_persistence<S: Store>(self, store: S) -> Persisted<Self, S> {
        Persisted::new(self, store)
    }
}

impl<E, C: PersistableCollector<E> + Sized> PersistExt<E> for C {}

/// The default upper bound on blocks per backfill `query_range` call. Sized to
/// fit within common provider `eth_getLogs` range caps.
const DEFAULT_BACKFILL_CHUNK_SIZE: u64 = 10_000;

/// A [`PersistableCollector`] paired with a [`Store`].
pub struct Persisted<C, S> {
    collector: C,
    store: S,
    /// The declared schema for this collector's event type, replacing the
    /// best-guess schema derived from the event signature. A `Persisted` wraps
    /// exactly one event type, so the override is a plain field here — the
    /// Store never needs to know which event type a row came from.
    schema: Option<TableSchema>,
    /// The lowest block the backfill segment may start from. With an empty
    /// store this is where the very first sync begins (instead of genesis);
    /// the backfill never reaches below it.
    start_block: u64,
    /// Upper bound on blocks per backfill `query_range` call; the gap is
    /// sliced into windows of this size, queried one at a time.
    backfill_chunk_size: u64,
    /// How many blocks deep a block must be buried before the live tail writes
    /// it (default 1). The most recent `confirmation_depth` blocks are buffered
    /// unwritten so an in-window reorg can be corrected before any orphaned row
    /// reaches the store; see [`with_confirmation_depth`].
    ///
    /// [`with_confirmation_depth`]: Persisted::with_confirmation_depth
    confirmation_depth: u64,
    /// Whether stored history has already been replayed to a subscriber. The
    /// engine re-subscribes after a stream ends, and replaying the full archive
    /// on every reconnect would re-deliver the entire history to strategies —
    /// so the replay segment runs only on the first subscribe; thereafter the
    /// backfill segment alone covers the gap since the last stored block.
    replayed: AtomicBool,
}

impl<C, S> Persisted<C, S> {
    /// Pair `collector` with `store`. Prefer [`PersistExt::with_persistence`].
    pub fn new(collector: C, store: S) -> Self {
        Self {
            collector,
            store,
            schema: None,
            start_block: 0,
            backfill_chunk_size: DEFAULT_BACKFILL_CHUNK_SIZE,
            confirmation_depth: 1,
            replayed: AtomicBool::new(false),
        }
    }

    /// Persist events under `schema` instead of the best-guess schema derived
    /// from the event signature: rows go to `schema`'s table with its listed
    /// columns (event fields it does not list are dropped; the lossless
    /// payload column is always appended).
    ///
    /// # Panics
    /// Panics when the schema names a column the persistence layer adds
    /// implicitly (`block_number`, `_payload`) or the store's internal
    /// progress table — misconfigurations that would otherwise halt
    /// persistence with an opaque SQL error on the first write.
    pub fn with_schema(mut self, schema: TableSchema) -> Self {
        if let Err(reason) = schema.ensure_no_reserved_names() {
            panic!("invalid schema override: {reason}");
        }
        self.schema = Some(schema);
        self
    }

    /// Never backfill below `block`. With an empty store, the very first sync
    /// starts here instead of at genesis — a strategy that only cares about
    /// recent history shouldn't have to fetch (or be able to fetch) the whole
    /// chain. Stored history beyond this block wins: the backfill resumes from
    /// the last stored block as usual.
    pub fn with_start_block(mut self, block: u64) -> Self {
        self.start_block = block;
        self
    }

    /// Slice the backfill into `query_range` windows of at most `blocks`
    /// blocks (default 10,000), queried one at a
    /// time, so no single RPC call exceeds provider range caps or buffers an
    /// unbounded result.
    ///
    /// # Panics
    /// Panics if `blocks` is zero.
    pub fn with_backfill_chunk_size(mut self, blocks: u64) -> Self {
        assert!(blocks >= 1, "backfill chunk size must be at least 1 block");
        self.backfill_chunk_size = blocks;
        self
    }

    /// Persist a block only once it is `depth` blocks deep (default 1). Events
    /// are still delivered downstream live and immediately; only the Store
    /// write lags. A reorg shallower than `depth` is corrected in the buffer
    /// before any orphaned row is written; a reorg deeper than `depth` halts
    /// persistence (a restart re-syncs). Choose `depth` above the deepest reorg
    /// you expect.
    ///
    /// # Panics
    /// Panics if `depth` is zero.
    pub fn with_confirmation_depth(mut self, depth: u64) -> Self {
        assert!(depth >= 1, "confirmation depth must be at least 1 block");
        self.confirmation_depth = depth;
        self
    }
}

/// The three segments of a [`Persisted`] subscription, in delivery order.
///
/// Construction forces an editor to account for every segment; the order in
/// which they reach the subscriber is fixed in exactly one place,
/// [`Segments::into_stream`]. The boundary arithmetic that keeps the segments
/// disjoint lives at the construction site in [`Persisted::subscribe`].
struct Segments<'a, E> {
    /// Stored history reconstructed from the database. Empty on every
    /// subscribe after the first (see the replay-once flag on [`Persisted`]).
    replay: CollectorStream<'a, E>,
    /// The RPC gap `[last+1 ..= tip]`: complete blocks, so the trailing block
    /// is flushed too.
    backfill: CollectorStream<'a, E>,
    /// The unbounded live tail, strictly above the backfill cut (`> tip`); its
    /// final in-progress block is never flushed.
    live: CollectorStream<'a, E>,
}

impl<'a, E: Send + 'a> Segments<'a, E> {
    /// Deliver replay, then backfill, then live. Replay and backfill must
    /// precede the live tail so strategies see history in block order, and the
    /// live tail must come last because it never ends.
    fn into_stream(self) -> CollectorStream<'a, E> {
        Box::pin(self.replay.chain(self.backfill).chain(self.live))
    }
}

#[async_trait]
impl<C, S, E> Collector<E> for Persisted<C, S>
where
    C: PersistableCollector<E>,
    S: Store,
    E: Serialize + DeserializeOwned + SolEvent + Send + Sync + 'static,
{
    async fn subscribe(&self) -> Result<CollectorStream<'_, E>> {
        // Subscribe to the live tip first so events between the tip query and
        // the live subscription are buffered by the source rather than lost.
        let live_source = self.collector.subscribe_indexed().await?;
        let tip = self.collector.tip().await?;

        // The Record fixes the table name and owns the event <-> row mapping
        // for the whole subscription; the override was already validated in
        // `with_schema`, so this only fails on a library bug. Shared (Arc)
        // between the backfill and live writers, so a schema frozen during
        // backfill is reused by the live tail.
        let record = Arc::new(Record::<E>::new(self.schema.clone())?);
        let last = self.store.last_block(record.table()).await?;

        // 1. Replay stored history, reconstructed from the database — but only
        //    on the first subscribe. On a reconnect the engine subscribes again;
        //    re-emitting the whole archive would re-deliver every historical
        //    event to strategies, so subsequent subscribes skip replay and let
        //    the backfill segment cover only the gap since the last stored block.
        let first_subscribe = !self.replayed.load(Ordering::SeqCst);
        let replay: CollectorStream<'_, E> = if first_subscribe {
            let inner = replay_stored(&self.store, &record, last).await?;
            // Flip the replay-once flag when the archive is first *consumed*,
            // not merely when `subscribe` succeeds. The engine retries
            // `subscribe` on error, but it also discards the returned stream
            // when a *sibling* fails the composite subscribe — e.g. this
            // `Persisted` chained or merged with another collector, where the
            // other source's subscribe errors after this one already succeeded.
            // In that case the stream is dropped without ever being polled, so
            // flipping the flag eagerly here would make the retry skip the DB
            // replay while backfill covers only blocks after `last` — stranding
            // the stored history. A zero-item stream that sets the flag on its
            // first poll, chained ahead of the real replay, ties the flip to
            // actual consumption.
            let replayed = &self.replayed;
            let mark = futures::stream::poll_fn(move |_| {
                replayed.store(true, Ordering::SeqCst);
                std::task::Poll::Ready(None::<E>)
            });
            Box::pin(mark.chain(inner)) as CollectorStream<'_, E>
        } else {
            Box::pin(futures::stream::empty()) as CollectorStream<'_, E>
        };

        // 2. Backfill the RPC gap `[last+1 ..= tip]`, never reaching below the
        //    configured start block. These are complete blocks, so the trailing
        //    block is flushed too (`flush_final = true`). When the stored
        //    height has already reached the tip (a restart within one block
        //    interval, or a node whose tip lags the store) there is no gap, and
        //    querying the inverted range `[tip+1 ..= tip]` would be rejected by
        //    real providers — failing every resubscribe until the Reconnect
        //    Policy escalates to Fatal. Skip the query instead.
        //
        //    The gap is sliced into bounded chunks queried one at a time; a
        //    chunk that fails after the first cancels `poison`, which ends the
        //    live tail too (see below).
        let poison = CancellationToken::new();
        let backfill_from = last.map(|l| l + 1).unwrap_or(0).max(self.start_block);
        let backfill_source: CollectorStream<'_, (u64, E)> = if backfill_from > tip {
            Box::pin(futures::stream::empty())
        } else {
            chunked_query(
                &self.collector,
                backfill_from,
                tip,
                self.backfill_chunk_size,
                poison.clone(),
            )
            .await?
        };
        let backfill = persist_and_emit(backfill_source, &self.store, record.clone(), true);

        // 3. Live tail, strictly above the backfill cut so the two segments are
        //    disjoint. A live subscription streams from "now", whose lower edge
        //    is fuzzy around the head (it may re-deliver blocks `<= tip`), so we
        //    enforce the `> tip` boundary rather than assume it. Live events end
        //    an "open" block only when a higher block arrives, so the final
        //    in-progress block is left unflushed (`flush_final = false`) and
        //    re-fetched on restart.
        //
        //    The disjoint split assumes the backfill query reliably covers block
        //    `tip`. If an RPC node's `eth_getLogs` lags behind its reported tip,
        //    a log at exactly `tip` could be missing from the backfill yet
        //    dropped here by the `> tip` filter — present in neither segment. A
        //    fully robust fix needs per-log identity (block + log index) to
        //    overlap the segments and de-duplicate; `(u64, E)` does not carry
        //    that today, so it is deferred rather than reversing the deliberate
        //    no-duplicate guarantee. See PR #18 / the boundary de-dup test.
        //    The live tail ends when `poison` is cancelled by a failed backfill
        //    chunk. If it kept going instead, blocks above the tip would be
        //    persisted while the failed chunk's blocks are missing — advancing
        //    the stored height over a permanent gap. Ending the whole stream
        //    hands the failure to the Reconnect Policy: the resubscribe
        //    backfills again from the last stored block.
        let live_source = Box::pin(
            live_source
                .filter(move |(block, _)| {
                    let above_cut = *block > tip;
                    async move { above_cut }
                })
                .take_until(poison.cancelled_owned()),
        ) as CollectorStream<'_, (u64, E)>;
        let live = persist_and_emit(live_source, &self.store, record, false);

        Ok(Segments {
            replay,
            backfill,
            live,
        }
        .into_stream())
    }
}

/// Replay stored events up to and including `last`, reconstructed from each
/// row's payload column. Returns an empty stream when nothing is stored.
async fn replay_stored<'a, E, S>(
    store: &'a S,
    record: &Record<E>,
    last: Option<u64>,
) -> Result<CollectorStream<'a, E>>
where
    E: DeserializeOwned + Send + 'a,
    S: Store + 'a,
{
    let Some(to) = last else {
        return Ok(Box::pin(futures::stream::empty()));
    };

    let rows = store.replay(&record.payload_schema(), to).await?;
    // A stored row that cannot be reconstructed is a hard error, not a row to
    // skip: replay feeds strategies the historical view they reason over, and
    // `_artemis_progress` already counts these blocks as processed. Silently
    // omitting them would hand strategies a quietly truncated history, so we
    // fail the subscribe (the engine retries, surfacing the problem) instead.
    let mut events = Vec::with_capacity(rows.len());
    for Row(cols) in rows {
        match cols.into_iter().next() {
            Some(SqlValue::Text(payload)) => events.push(record.decode(&payload)?),
            other => anyhow::bail!("unexpected payload column on replay: {other:?}"),
        }
    }
    Ok(Box::pin(futures::stream::iter(events)))
}

/// Query the inclusive range `[from ..= to]` in block-aligned windows of at
/// most `chunk` blocks, one `query_range` call at a time, flattened into a
/// single stream.
///
/// The first window is queried eagerly so a backfill that can't start at all
/// fails the subscribe (feeding the Reconnect Policy's counter). Later windows
/// are queried lazily as the stream is consumed; one of them failing cannot
/// fail the already-returned subscribe, so it instead logs, cancels `poison`,
/// and ends the stream — every block delivered up to that point is complete,
/// because windows are block-aligned.
async fn chunked_query<'a, C, E>(
    collector: &'a C,
    from: u64,
    to: u64,
    chunk: u64,
    poison: CancellationToken,
) -> Result<CollectorStream<'a, (u64, E)>>
where
    C: PersistableCollector<E> + ?Sized,
    E: Send + 'a,
{
    /// Last block of the window starting at `from`: `from + chunk - 1`,
    /// saturating, and never beyond `to`.
    fn window_end(from: u64, to: u64, chunk: u64) -> u64 {
        from.saturating_add(chunk - 1).min(to)
    }

    let first_to = window_end(from, to, chunk);
    let mut first = collector.query_range(from, first_to).await?;

    let stream = async_stream::stream! {
        while let Some(item) = first.next().await {
            yield item;
        }
        let mut next_from = first_to.saturating_add(1);
        // `saturating_add` can only stall at u64::MAX, where `window_end`
        // already returned `to` and the loop is done.
        while next_from <= to {
            let next_to = window_end(next_from, to, chunk);
            match collector.query_range(next_from, next_to).await {
                Ok(mut window) => {
                    while let Some(item) = window.next().await {
                        yield item;
                    }
                }
                Err(e) => {
                    tracing::error!(
                        "backfill chunk [{next_from}, {next_to}] failed; \
                         ending stream for resubscribe: {e}"
                    );
                    poison.cancel();
                    return;
                }
            }
            next_from = next_to.saturating_add(1);
        }
    };
    Ok(Box::pin(stream))
}

/// Wrap a stream of `(block, event)` so that each event is buffered and written
/// to `store` one transaction per complete block, while the plain events flow
/// downstream unchanged.
///
/// A block is "complete" once a higher block number is observed. The trailing
/// block is flushed at stream end only when `flush_final` is set (true for a
/// finite backfill range, false for a live tail).
fn persist_and_emit<'a, E, S>(
    mut source: CollectorStream<'a, (u64, E)>,
    store: &'a S,
    record: Arc<Record<E>>,
    flush_final: bool,
) -> CollectorStream<'a, E>
where
    E: Serialize + Send + Sync + 'static,
    S: Store + 'a,
{
    let stream = async_stream::stream! {
        let mut writer = BlockWriter::new(store, record);

        while let Some((block, event)) = source.next().await {
            writer.record(block, &event).await;
            yield event;
        }

        if flush_final {
            writer.finish().await;
        }
    };

    Box::pin(stream)
}

/// Buffers one block's rows at a time and writes each complete block to the
/// store in a single transaction. A block is complete once a higher block
/// number is observed; the trailing block is written only by [`finish`].
///
/// Once a write fails the writer goes unhealthy and stays that way: writing
/// any later block would leave a hole behind `last_block`'s gap-free prefix.
/// An unhealthy writer does no per-event work at all — deriving and buffering
/// rows for blocks that will never be written would grow the buffer without
/// bound on a live tail. The event stream itself keeps flowing either way; a
/// restart re-fetches everything after the last good block.
///
/// [`finish`]: BlockWriter::finish
struct BlockWriter<'a, S, E> {
    store: &'a S,
    record: Arc<Record<E>>,
    buffer: Vec<Row>,
    current: Option<u64>,
    healthy: bool,
}

impl<'a, S: Store, E: Serialize> BlockWriter<'a, S, E> {
    fn new(store: &'a S, record: Arc<Record<E>>) -> Self {
        Self {
            store,
            record,
            buffer: Vec::new(),
            current: None,
            healthy: true,
        }
    }

    /// Buffer one event's row, first flushing the previous block if `block`
    /// has advanced past it. No-op once unhealthy.
    async fn record(&mut self, block: u64, event: &E) {
        if !self.healthy {
            return;
        }
        match self.record.encode(event) {
            Ok(row) => {
                if let Some(cur) = self.current {
                    // A backwards block means the open block's completeness
                    // can no longer be trusted: flushing it would advance the
                    // stored height past the late block's rows, leaving a
                    // permanent hole behind the gap-free watermark.
                    if block < cur {
                        self.halt(format_args!(
                            "block {block} arrived after block {cur} (reorg or \
                             unordered source)"
                        ));
                        return;
                    }
                    // The block advanced: the previous block is complete. The
                    // schema is always present here — `current` is only set
                    // after a successful encode, which froze it.
                    if block > cur {
                        let schema = self.record.schema().expect("frozen by first encode");
                        self.healthy =
                            flush(self.store, &schema, cur, std::mem::take(&mut self.buffer)).await;
                        if !self.healthy {
                            // This event's block can never be written without
                            // leaving a gap, so don't buffer its row either.
                            return;
                        }
                    }
                }
                self.current = Some(block);
                self.buffer.push(row);
            }
            // An event that can't be encoded into a row can never be written,
            // so its block — and everything after it — must not be either:
            // progress advancing past it would hand the next restart exactly
            // the quietly truncated history `replay_stored` refuses to emit.
            // The event stream itself keeps flowing.
            Err(e) => self.halt(format_args!("failed to encode row: {e}")),
        }
    }

    /// Stop persisting for the rest of the stream and discard the open block's
    /// buffer (its completeness is no longer trustworthy). The stored height
    /// stays at the last fully written block; a restart re-fetches from there.
    fn halt(&mut self, reason: std::fmt::Arguments<'_>) {
        self.healthy = false;
        self.buffer.clear();
        tracing::error!(
            "halting persistence ({reason}); events keep flowing, and a \
             restart will re-sync from the last stored block"
        );
    }

    /// Flush the trailing block. Only correct when the source delivered the
    /// block completely (a finite backfill range, not a live tail).
    async fn finish(&mut self) {
        if self.healthy
            && let Some(cur) = self.current
        {
            let schema = self.record.schema().expect("frozen by first encode");
            flush(self.store, &schema, cur, std::mem::take(&mut self.buffer)).await;
        }
    }

    /// Rows currently buffered for the open block.
    #[cfg(test)]
    fn buffered(&self) -> usize {
        self.buffer.len()
    }
}

/// Buffers the most recent `depth` blocks of a live tail and writes a block
/// only once it is buried `depth` blocks deep (`head >= block + depth`). Unlike
/// [`BlockWriter`], a backwards block within the unflushed window is treated as
/// a reorg re-emission and corrected in place rather than halting: the node has
/// rewound to a forked block whose row was never written, so dropping the old
/// fork's buffered blocks and rewinding `head` lets the canonical chain re-fill
/// the window. Only a write at or below the already-flushed watermark (a reorg
/// *deeper* than `depth`, which would need a delete to undo) or an unencodable
/// event halts.
///
/// The unflushed window is deliberately never drained at stream end: there is
/// no "stream end" on a live tail, and on a restart the Backfill segment
/// re-fetches the whole window (`[last+1 ..= tip]`) from the canonical chain.
struct ConfirmationWindow<'a, S, E> {
    store: &'a S,
    record: Arc<Record<E>>,
    /// Blocks buried this many deep are mature and get written.
    depth: u64,
    /// Buffered rows keyed by block number, lowest first — the order they must
    /// be flushed in to keep the stored height a gap-free prefix.
    pending: BTreeMap<u64, Vec<Row>>,
    /// Highest block number seen so far; maturity is measured against it.
    head: Option<u64>,
    /// Highest block already written — the finalized watermark. A re-emission
    /// at or below it is a reorg deeper than `depth`.
    flushed: Option<u64>,
    healthy: bool,
}

impl<'a, S: Store, E: Serialize> ConfirmationWindow<'a, S, E> {
    fn new(store: &'a S, record: Arc<Record<E>>, depth: u64) -> Self {
        Self {
            store,
            record,
            depth,
            pending: BTreeMap::new(),
            head: None,
            flushed: None,
            healthy: true,
        }
    }

    /// Buffer one event's row, correcting an in-window reorg, then flush every
    /// block that has matured to `depth` confirmations. No-op once unhealthy.
    async fn record(&mut self, block: u64, event: &E) {
        if !self.healthy {
            return;
        }

        // A finalized block being rewritten is a reorg deeper than `depth`:
        // unfixable without a delete, so halt (the stored height stays; a
        // restart re-syncs).
        if let Some(f) = self.flushed
            && block <= f
        {
            // Read `depth` into a local first: `halt` takes `&mut self`, so the
            // format args may not also borrow `self` immutably.
            let depth = self.depth;
            self.halt(format_args!(
                "block {block} rewritten at/below the finalized watermark {f} \
                 (reorg deeper than confirmation depth {depth})"
            ));
            return;
        }

        let row = match self.record.encode(event) {
            Ok(row) => row,
            // As in BlockWriter: an unencodable event must not be skipped, or
            // progress advances past a hole replay would expose.
            Err(e) => {
                self.halt(format_args!("failed to encode row: {e}"));
                return;
            }
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
            let schema = self.record.schema().expect("frozen by first encode");
            if !flush(self.store, &schema, b, rows).await {
                // A failed write means a later block must not advance the
                // stored height past the gap; drop the rest of the window and
                // stop, exactly as BlockWriter does on a flush failure.
                self.healthy = false;
                self.pending.clear();
                return;
            }
            self.flushed = Some(b);
        }
    }

    /// Stop persisting for the rest of the stream and discard the buffered
    /// window (its blocks will be re-fetched by a restart's backfill). The
    /// stored height stays at the last fully written block.
    fn halt(&mut self, reason: std::fmt::Arguments<'_>) {
        self.healthy = false;
        self.pending.clear();
        tracing::error!(
            "halting persistence ({reason}); events keep flowing, and a \
             restart will re-sync from the last stored block"
        );
    }

    /// Block numbers currently buffered, lowest first.
    #[cfg(test)]
    fn buffered_blocks(&self) -> Vec<u64> {
        self.pending.keys().copied().collect()
    }
}

/// Persist one block's buffered rows. Returns `false` (and logs) on failure so
/// the caller can stop advancing the stored block height rather than leave a
/// gap; never tears down the event stream.
async fn flush<S: Store>(store: &S, schema: &TableSchema, block: u64, rows: Vec<Row>) -> bool {
    match store.write_block(schema, block, rows).await {
        Ok(()) => true,
        Err(e) => {
            tracing::error!("failed to persist block {block}; halting persistence: {e}");
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;

    alloy::sol! {
        #[derive(serde::Serialize)]
        event Ping(uint256 value);
    }

    fn ping(value: u64) -> Ping {
        Ping {
            value: alloy::primitives::U256::from(value),
        }
    }

    alloy::sol! {
        // No serde derive: `Serialize` is implemented by hand below to produce
        // a non-object JSON value — which `Record::encode` rejects — for the
        // zero value only, so one writer can see good and bad events of the
        // same type.
        event BadPing(uint256 value);
    }

    impl serde::Serialize for BadPing {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            if self.value.is_zero() {
                serializer.serialize_str("not a JSON object")
            } else {
                use serde::ser::SerializeMap;
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("value", &self.value.to_string())?;
                map.end()
            }
        }
    }

    fn bad_ping(value: u64) -> BadPing {
        BadPing {
            value: alloy::primitives::U256::from(value),
        }
    }

    /// A writer over a fresh inferred [`Record`] for `E`.
    fn writer<S, E>(store: &S) -> BlockWriter<'_, S, E>
    where
        S: Store,
        E: alloy::sol_types::SolEvent + Serialize,
    {
        BlockWriter::new(store, Arc::new(Record::new(None).unwrap()))
    }

    /// A windowed writer over a fresh inferred [`Record`] for `E`.
    fn window<S, E>(store: &S, depth: u64) -> ConfirmationWindow<'_, S, E>
    where
        S: Store,
        E: alloy::sol_types::SolEvent + Serialize,
    {
        ConfirmationWindow::new(store, Arc::new(Record::new(None).unwrap()), depth)
    }

    /// A store whose every write fails.
    struct FailingStore;

    #[async_trait]
    impl Store for FailingStore {
        async fn write_block(
            &self,
            _schema: &TableSchema,
            block: u64,
            _rows: Vec<Row>,
        ) -> Result<()> {
            anyhow::bail!("simulated write failure at block {block}")
        }
        async fn last_block(&self, _table: &str) -> Result<Option<u64>> {
            Ok(None)
        }
        async fn replay(&self, _schema: &TableSchema, _to: u64) -> Result<Vec<Row>> {
            Ok(vec![])
        }
    }

    /// A store that records which blocks were written and always succeeds.
    #[derive(Default)]
    struct RecordingStore {
        written: std::sync::Mutex<Vec<u64>>,
    }

    impl RecordingStore {
        fn written(&self) -> Vec<u64> {
            self.written.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl Store for RecordingStore {
        async fn write_block(
            &self,
            _schema: &TableSchema,
            block: u64,
            _rows: Vec<Row>,
        ) -> Result<()> {
            self.written.lock().unwrap().push(block);
            Ok(())
        }
        async fn last_block(&self, _table: &str) -> Result<Option<u64>> {
            Ok(None)
        }
        async fn replay(&self, _schema: &TableSchema, _to: u64) -> Result<Vec<Row>> {
            Ok(vec![])
        }
    }

    /// A zero confirmation depth is nonsensical (a block can never be buried
    /// zero deep before it is written — that is the same block) and a builder
    /// that silently accepted it would defeat the reorg protection it exists
    /// for. Reject it at construction, as the other knobs reject their invalid
    /// values.
    #[test]
    #[should_panic(expected = "confirmation depth must be at least 1")]
    fn zero_confirmation_depth_panics() {
        let store = RecordingStore::default();
        // The collector type is irrelevant to the panic — neither `new` nor
        // `with_confirmation_depth` requires the `PersistableCollector` bound,
        // so the unit `()` collector compiles and keeps the test focused on
        // the assertion alone.
        let _ = Persisted::new((), store).with_confirmation_depth(0);
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
    /// buffered — the behaviour [`BlockWriter`] gives a live tail today.
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
}
