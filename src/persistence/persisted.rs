//! [`Persisted`]: a [`Collector`] wrapper that records every event it sees and,
//! on subscribe, replays stored history before following the chain tip.

use std::num::NonZeroU64;
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
use super::writer::{persist_and_emit, persist_and_emit_windowed};
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

    /// The standard restart-resilient configuration in one call: persist into
    /// `store`, begin the very first backfill at `start_block` (stored history
    /// beyond it still wins; see [`Persisted::with_start_block`]), and buffer
    /// `confirmation_depth` blocks before a row reaches the store (see
    /// [`Persisted::with_confirmation_depth`]). Equivalent to chaining
    /// [`with_persistence`](Self::with_persistence), `with_start_block`, and
    /// `with_confirmation_depth`.
    fn persisted<S: Store>(
        self,
        store: S,
        start_block: u64,
        confirmation_depth: NonZeroU64,
    ) -> Persisted<Self, S> {
        self.with_persistence(store)
            .with_start_block(start_block)
            .with_confirmation_depth(confirmation_depth)
    }
}

impl<E, C: PersistableCollector<E> + Sized> PersistExt<E> for C {}

/// Build a restart-resilient persisted [`EventCollector`] from an alloy contract
/// event filter. Clones the provider into the filter, wraps it in an
/// [`EventCollector`], and configures it via [`PersistExt::persisted`] so it
/// replays stored history from `store` and backfills the `[start_block..tip]`
/// gap before following the chain tip — buffering `confirmation_depth` blocks
/// before a row is persisted.
///
/// This is the canonical way an indexer turns a single-address contract event
/// filter into a persisted collector; without it each call site re-spells the
/// `EventCollector::new(filter.with_cloned_provider()).persisted(..)` chain.
///
/// ```ignore
/// let transfers = persisted_event_collector!(
///     pool.Transfer_filter(), events_store.clone(), from_block, confirmation_depth,
/// );
/// ```
///
/// [`EventCollector`]: crate::collectors::EventCollector
/// [`PersistExt::persisted`]: crate::persistence::PersistExt::persisted
#[macro_export]
macro_rules! persisted_event_collector {
    ($filter:expr, $store:expr, $start_block:expr, $confirmation_depth:expr $(,)?) => {
        $crate::persistence::PersistExt::persisted(
            $crate::collectors::EventCollector::new($filter.with_cloned_provider()),
            $store,
            $start_block,
            $confirmation_depth,
        )
    };
}

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
    /// When set, cap the backfill at this block and skip the live tail entirely.
    /// The stream ends naturally once backfill-to-`to_block` is exhausted. Used
    /// in bounded/testing contexts where only historical data is needed (e.g. a
    /// Tier-2 accounting cross-check that pins to a snapshot block).
    to_block: Option<u64>,
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
            to_block: None,
            replayed: AtomicBool::new(false),
        }
    }

    /// Cap the backfill at `block` and skip the live tail. The stream ends
    /// naturally once backfill-to-`block` is exhausted — no timeout needed.
    /// Useful for bounded/testing contexts that pin to a snapshot block.
    pub fn with_to_block(mut self, block: u64) -> Self {
        self.to_block = Some(block);
        self
    }

    /// Persist events under `schema` instead of the best-guess schema derived
    /// from the event signature: rows go to `schema`'s table with its listed
    /// columns (event fields it does not list are dropped; the lossless
    /// payload column is always appended).
    ///
    /// # Errors
    /// Errors when the schema names a column the persistence layer adds
    /// implicitly (`block_number`, `_payload`) or the store's internal
    /// progress table — misconfigurations that would otherwise halt
    /// persistence with an opaque SQL error on the first write.
    pub fn try_with_schema(mut self, schema: TableSchema) -> Result<Self> {
        schema
            .ensure_no_reserved_names()
            .map_err(|reason| anyhow::anyhow!("invalid schema override: {reason}"))?;
        self.schema = Some(schema);
        Ok(self)
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
    /// unbounded result. A [`NonZeroU64`] makes a zero-block chunk (which could
    /// never make progress) unrepresentable at the call site.
    pub fn with_backfill_chunk_size(mut self, blocks: NonZeroU64) -> Self {
        self.backfill_chunk_size = blocks.get();
        self
    }

    /// Persist a block only once it is `depth` blocks deep (default 1). Events
    /// are still delivered downstream live and immediately; only the Store
    /// write lags. A reorg shallower than `depth` is corrected in the buffer
    /// before any orphaned row is written; a reorg deeper than `depth` halts
    /// persistence (a restart re-syncs). Choose `depth` above the deepest reorg
    /// you expect. A [`NonZeroU64`] makes a zero depth (which would write the
    /// open live block before it can be confirmed) unrepresentable.
    pub fn with_confirmation_depth(mut self, depth: NonZeroU64) -> Self {
        self.confirmation_depth = depth.get();
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
        // In bounded mode (to_block set), skip the live subscription entirely —
        // the stream ends when backfill-to-to_block is exhausted.
        let live_source = if self.to_block.is_none() {
            // Subscribe to the live tip first so events between the tip query
            // and the live subscription are buffered by the source.
            Some(self.collector.subscribe_indexed().await?)
        } else {
            None
        };
        let tip = self.collector.tip().await?;
        // Cap the effective tip at to_block when in bounded mode.
        let effective_tip = self.to_block.map(|b| b.min(tip)).unwrap_or(tip);

        // The Record fixes the table name and owns the event <-> row mapping
        // for the whole subscription; the override was already validated in
        // `try_with_schema`, so this only fails on a library bug. Shared (Arc)
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

        // 2. Backfill the RPC gap `[last+1 ..= effective_tip]`, never reaching
        //    below the configured start block. These are complete blocks, so the
        //    trailing block is flushed too (`flush_final = true`). When the stored
        //    height has already reached the tip (a restart within one block
        //    interval, or a node whose tip lags the store) there is no gap, and
        //    querying the inverted range would be rejected — skip the query instead.
        //
        //    The gap is sliced into bounded chunks queried one at a time; a
        //    chunk that fails after the first cancels `poison`, which ends the
        //    live tail too (see below).
        let poison = CancellationToken::new();
        let backfill_from = last.map(|l| l + 1).unwrap_or(0).max(self.start_block);
        let backfill_source: CollectorStream<'_, (u64, E)> = if backfill_from > effective_tip {
            Box::pin(futures::stream::empty())
        } else {
            chunked_query(
                &self.collector,
                backfill_from,
                effective_tip,
                self.backfill_chunk_size,
                poison.clone(),
            )
            .await?
        };
        let backfill = persist_and_emit(backfill_source, &self.store, record.clone(), true);

        // 3. Live tail, strictly above the backfill cut so the two segments are
        //    disjoint. Skipped entirely in bounded mode (to_block set) — the
        //    stream ends naturally once backfill is exhausted. In unbounded mode,
        //    the live tail persists through a `ConfirmationWindow` instead of
        //    `BlockWriter`: a block is written only once it is `confirmation_depth`
        //    blocks deep, so the most recent depth blocks stay buffered and a
        //    shallow reorg is corrected before any orphaned row is written.
        //
        //    The live tail ends when `poison` is cancelled by a failed backfill
        //    chunk. Ending the whole stream hands the failure to the Reconnect
        //    Policy: the resubscribe backfills again from the last stored block.
        let live: CollectorStream<'_, E> = match live_source {
            None => Box::pin(futures::stream::empty()),
            Some(src) => {
                let live_filtered = Box::pin(
                    src.filter(move |(block, _)| {
                        let above_cut = *block > effective_tip;
                        async move { above_cut }
                    })
                    .take_until(poison.cancelled_owned()),
                ) as CollectorStream<'_, (u64, E)>;
                persist_and_emit_windowed(
                    live_filtered,
                    &self.store,
                    record,
                    self.confirmation_depth,
                )
            }
        };

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

// A zero confirmation depth is nonsensical (a block can never be buried zero
// deep before it is written — that is the same block) and would defeat the
// reorg protection the knob exists for. It is now unrepresentable:
// `with_confirmation_depth` takes a `NonZeroU64`, so the invalid value cannot
// reach the builder — the same way `with_backfill_chunk_size` rejects a zero
// chunk.
