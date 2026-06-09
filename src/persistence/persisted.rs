//! [`Persisted`]: a [`Collector`] wrapper that records every event it sees and,
//! on subscribe, replays stored history before following the chain tip.

use std::any::TypeId;
use std::sync::atomic::{AtomicBool, Ordering};

use alloy::sol_types::SolEvent;
use anyhow::Result;
use async_trait::async_trait;
use futures::StreamExt;
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::record::{PAYLOAD_COLUMN, derive_record_with, from_payload, table_name};
use super::schema::{Row, SqlType, SqlValue, TableSchema};
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

/// A [`PersistableCollector`] paired with a [`Store`].
pub struct Persisted<C, S> {
    collector: C,
    store: S,
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
            replayed: AtomicBool::new(false),
        }
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

        // The table name follows any registered override.
        let table = resolved_table::<E, S>(&self.store);
        let last = self.store.last_block(&table).await?;

        // 1. Replay stored history, reconstructed from the database — but only
        //    on the first subscribe. On a reconnect the engine subscribes again;
        //    re-emitting the whole archive would re-deliver every historical
        //    event to strategies, so subsequent subscribes skip replay and let
        //    the backfill segment cover only the gap since the last stored block.
        let first_subscribe = !self.replayed.load(Ordering::SeqCst);
        let replay: CollectorStream<'_, E> = if first_subscribe {
            replay_stored::<E, S>(&self.store, table, last).await?
        } else {
            Box::pin(futures::stream::empty()) as CollectorStream<'_, E>
        };

        // 2. Backfill the RPC gap `[last+1 ..= tip]`. These are complete blocks,
        //    so the trailing block is flushed too (`flush_final = true`).
        let backfill_from = last.map(|l| l + 1).unwrap_or(0);
        let backfill_source = self.collector.query_range(backfill_from, tip).await?;
        let backfill = persist_and_emit(backfill_source, &self.store, true);

        // Only now — after every fallible setup step has succeeded — mark the
        // replay segment as consumed. The engine retries `subscribe` when it
        // returns an error, so flipping this flag earlier (e.g. right after
        // `replay_stored`) would make a retry skip the DB replay while backfill
        // covers only blocks after `last`, stranding the stored history.
        if first_subscribe {
            self.replayed.store(true, Ordering::SeqCst);
        }

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
        let live_source = Box::pin(live_source.filter(move |(block, _)| {
            let above_cut = *block > tip;
            async move { above_cut }
        })) as CollectorStream<'_, (u64, E)>;
        let live = persist_and_emit(live_source, &self.store, false);

        Ok(Segments {
            replay,
            backfill,
            live,
        }
        .into_stream())
    }
}

/// The resolved table name for event type `E`: an override's table if one is
/// registered on `store`, otherwise the best-guess name from the signature.
fn resolved_table<E, S>(store: &S) -> String
where
    E: SolEvent + 'static,
    S: Store,
{
    store
        .schema_override(TypeId::of::<E>())
        .map(|schema| schema.table)
        .unwrap_or_else(table_name::<E>)
}

/// Replay stored events up to and including `last`, reconstructed from each
/// row's payload column. Returns an empty stream when nothing is stored.
async fn replay_stored<'a, E, S>(
    store: &'a S,
    table: String,
    last: Option<u64>,
) -> Result<CollectorStream<'a, E>>
where
    E: DeserializeOwned + SolEvent + Send + 'a,
    S: Store + 'a,
{
    let Some(to) = last else {
        return Ok(Box::pin(futures::stream::empty()));
    };

    let payload_schema = TableSchema::new(table).col(PAYLOAD_COLUMN, SqlType::Text);
    let rows = store.replay(&payload_schema, to).await?;
    // A stored row that cannot be reconstructed is a hard error, not a row to
    // skip: replay feeds strategies the historical view they reason over, and
    // `_artemis_progress` already counts these blocks as processed. Silently
    // omitting them would hand strategies a quietly truncated history, so we
    // fail the subscribe (the engine retries, surfacing the problem) instead.
    let mut events = Vec::with_capacity(rows.len());
    for Row(cols) in rows {
        match cols.into_iter().next() {
            Some(SqlValue::Text(payload)) => events.push(from_payload::<E>(&payload)?),
            other => anyhow::bail!("unexpected payload column on replay: {other:?}"),
        }
    }
    Ok(Box::pin(futures::stream::iter(events)))
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
    flush_final: bool,
) -> CollectorStream<'a, E>
where
    E: Serialize + SolEvent + Send + 'static,
    S: Store + 'a,
{
    let stream = async_stream::stream! {
        let override_ = store.schema_override(TypeId::of::<E>());
        let mut buffer: Vec<Row> = Vec::new();
        let mut current: Option<u64> = None;
        let mut schema: Option<TableSchema> = None;
        // Once a write fails we stop persisting so `last_block` keeps pointing
        // at a contiguous, gap-free prefix. The event stream itself keeps
        // flowing; a restart re-fetches everything after the last good block.
        let mut healthy = true;

        while let Some((block, event)) = source.next().await {
            match derive_record_with(&event, override_.as_ref()) {
                Ok((row_schema, row)) => {
                    // The block advanced: the previous block is complete.
                    if healthy
                        && let (Some(cur), Some(sch)) = (current, &schema)
                        && block != cur
                    {
                        healthy = flush(store, sch, cur, std::mem::take(&mut buffer)).await;
                    }
                    current = Some(block);
                    schema = Some(row_schema);
                    buffer.push(row);
                }
                // A derive failure must not break the event stream the rest of
                // the pipeline depends on; log and keep forwarding.
                Err(e) => tracing::error!("failed to derive row for persistence: {e}"),
            }
            yield event;
        }

        if healthy
            && flush_final
            && let (Some(cur), Some(sch)) = (current, &schema)
        {
            flush(store, sch, cur, std::mem::take(&mut buffer)).await;
        }
    };

    Box::pin(stream)
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
