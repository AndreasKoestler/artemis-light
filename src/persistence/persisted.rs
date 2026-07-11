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

use super::position::Position;
use super::record::{Record, table_name};
use super::schema::{Row, SqlValue, TableSchema};
use super::store::Store;
use super::writer::persist_and_emit;
use crate::types::{Collector, CollectorStream};

/// One item of a live position-indexed stream: a positioned event, or a
/// retraction of a previously delivered position.
///
/// A retraction is a reorg's *removal* signal — e.g. an EVM node re-sending a
/// log with `removed: true` — telling the pipeline that events at that position
/// no longer happened. Without it a same-height reorg (block `N` replaced by
/// `N′` before `N + 1` arrives) is invisible to the confirmation window: the
/// replacement's events arrive at the same sort key as the orphaned ones and
/// would be appended beside them rather than replacing them. Retractions only
/// affect persistence (the buffered window is rewound); events already
/// delivered downstream cannot be unwound.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Indexed<P, E> {
    /// An event at its position.
    Event(P, E),
    /// Everything buffered at (and above) this position no longer happened;
    /// the canonical replacements will be re-emitted.
    Retract(P),
}

/// A collector that is aware of its ordering [`Position`] and can replay
/// historical ranges — the capability [`Persisted`] needs to record events and
/// fill the gap between the last stored position and the source's tip.
///
/// Implemented by collectors that wrap a queryable source (e.g. an
/// `EventCollector` over alloy's `Event`, whose [`Pos`](Self::Pos) is the
/// built-in [`BlockPosition`](super::BlockPosition)).
#[async_trait]
pub trait PersistableCollector<E>: Send + Sync {
    /// The collector's ordering [`Position`] — the persistence key.
    type Pos: Position;

    /// Live, position-indexed events from the source's tip onward, plus reorg
    /// retractions ([`Indexed::Retract`]) where the source can observe them. A
    /// source with no retraction signal simply never yields one.
    async fn subscribe_indexed(&self) -> Result<CollectorStream<'_, Indexed<Self::Pos, E>>>;

    /// Historical position-indexed events for the inclusive range `from..=to`
    /// (a block range, or a time window up to a finality lag).
    async fn query_range(
        &self,
        from: Self::Pos,
        to: Self::Pos,
    ) -> Result<CollectorStream<'_, (Self::Pos, E)>>;

    /// The current tip [`Position`] — the boundary the subscribe-time gap fill
    /// runs up to.
    ///
    /// # Law: the tip is a coverage boundary
    ///
    /// Everything the source has emitted at sort keys at or below `tip()` must
    /// already be readable through [`query_range`](Self::query_range) at the
    /// moment the tip is observed. [`Persisted`] fills `[resume ..= tip]` from
    /// `query_range` and treats anything the live stream later delivers at or
    /// below the tip as a re-observation (boundary overlap or a reorg), never
    /// as new coverage — an event `query_range` cannot yet see below the tip is
    /// silently unreachable. A source whose recent range is still settling (an
    /// unsettled time window, a lagging index) must report a lagged tip — a
    /// finality boundary — rather than its raw head.
    async fn tip(&self) -> Result<Self::Pos>;
}

/// Extension method that wraps a [`PersistableCollector`] with a [`Store`].
pub trait PersistExt<E>: PersistableCollector<E> + Sized {
    /// Record every event into `store`, replaying stored history on subscribe.
    ///
    /// Keeps a `where E: SolEvent` clause so the table name can be captured from
    /// the event's Solidity signature now — [`subscribe`](crate::types::Collector::subscribe)
    /// has no `SolEvent` bound and reads the captured name. EVM call sites are
    /// unchanged.
    #[must_use]
    fn with_persistence<S: Store<Self::Pos>>(self, store: S) -> Persisted<Self, S>
    where
        E: SolEvent,
    {
        Persisted::new(self, store).with_table(table_name::<E>())
    }

    /// Record every event into `store` under the explicit `table` name,
    /// **without** requiring `E: SolEvent` — the entry point for a non-EVM event
    /// type whose table name cannot be derived from a Solidity signature.
    #[must_use]
    fn with_persistence_named<S: Store<Self::Pos>>(
        self,
        store: S,
        table: impl Into<String>,
    ) -> Persisted<Self, S> {
        Persisted::new(self, store).with_table(table)
    }

    /// The standard restart-resilient configuration in one call: persist into
    /// `store`, begin the very first backfill at `start_block` (stored history
    /// beyond it still wins; see [`Persisted::with_start_block`]), and buffer
    /// `confirmation_depth` blocks before a row reaches the store (see
    /// [`Persisted::with_confirmation_depth`]). Equivalent to chaining
    /// [`with_persistence`](Self::with_persistence), `with_start_block`, and
    /// `with_confirmation_depth`.
    #[must_use]
    fn persisted<S: Store<Self::Pos>>(
        self,
        store: S,
        start_block: u64,
        confirmation_depth: NonZeroU64,
    ) -> Persisted<Self, S>
    where
        E: SolEvent,
    {
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

/// The default upper bound on positions (by sort key) per backfill `query_range`
/// call. Sized to fit within common provider `eth_getLogs` range caps.
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
    /// The table name to persist under when no schema override is set: captured
    /// from the SolEvent signature by [`PersistExt::with_persistence`], or
    /// supplied directly via [`PersistExt::with_persistence_named`] /
    /// [`Persisted::with_table`]. `None` for a bare [`Persisted::new`] over a
    /// non-SolEvent type — subscribe then errors with the no-table-name literal.
    table: Option<String>,
    /// The lowest sort key the backfill segment may start from. With an empty
    /// store this is where the very first sync begins (instead of genesis);
    /// the backfill never reaches below it.
    start_block: u64,
    /// Upper bound on positions (by sort key) per backfill `query_range` call;
    /// the gap is sliced into windows of this size, queried one at a time.
    backfill_chunk_size: u64,
    /// How many positions deep a group must be buried before the live tail
    /// writes it (default 1). The most recent `confirmation_depth` positions are
    /// buffered unwritten so an in-window reorg can be corrected before any
    /// orphaned row reaches the store; see [`with_confirmation_depth`].
    ///
    /// [`with_confirmation_depth`]: Persisted::with_confirmation_depth
    confirmation_depth: u64,
    /// When set, cap the backfill at this sort key and skip the live tail
    /// entirely. The stream ends naturally once backfill-to-`to_block` is
    /// exhausted. Used in bounded/testing contexts where only historical data is
    /// needed (e.g. a Tier-2 accounting cross-check that pins to a snapshot
    /// block).
    to_block: Option<u64>,
    /// Whether stored history has already been replayed to a subscriber. The
    /// engine re-subscribes after a stream ends, and replaying the full archive
    /// on every reconnect would re-deliver the entire history to strategies —
    /// so the replay segment runs only on the first subscribe; thereafter the
    /// backfill segment alone covers the gap since the last stored position.
    replayed: AtomicBool,
}

impl<C, S> Persisted<C, S> {
    /// Pair `collector` with `store`. Prefer [`PersistExt::with_persistence`].
    pub fn new(collector: C, store: S) -> Self {
        Self {
            collector,
            store,
            schema: None,
            table: None,
            start_block: 0,
            backfill_chunk_size: DEFAULT_BACKFILL_CHUNK_SIZE,
            confirmation_depth: 1,
            to_block: None,
            replayed: AtomicBool::new(false),
        }
    }

    /// Cap the backfill at `block` (a sort key) and skip the live tail. The
    /// stream ends naturally once backfill-to-`block` is exhausted — no timeout
    /// needed. Useful for bounded/testing contexts that pin to a snapshot block.
    #[must_use]
    pub fn with_to_block(mut self, block: u64) -> Self {
        self.to_block = Some(block);
        self
    }

    /// Persist under `table` instead of a name derived from a Solidity event
    /// signature — the name source for a non-EVM event type built via a bare
    /// [`Persisted::new`]. A schema override set through
    /// [`try_with_schema`](Persisted::try_with_schema) still wins at subscribe.
    #[must_use]
    pub fn with_table(mut self, table: impl Into<String>) -> Self {
        self.table = Some(table.into());
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

    /// Never backfill below `block` (a sort key). With an empty store, the very
    /// first sync starts here instead of at genesis — a strategy that only cares
    /// about recent history shouldn't have to fetch (or be able to fetch) the
    /// whole chain. Stored history beyond this block wins: the backfill resumes
    /// from the last stored position as usual.
    #[must_use]
    pub fn with_start_block(mut self, block: u64) -> Self {
        self.start_block = block;
        self
    }

    /// Slice the backfill into `query_range` windows of at most `blocks`
    /// positions (by sort key, default 10,000), queried one at a
    /// time, so no single RPC call exceeds provider range caps or buffers an
    /// unbounded result. A [`NonZeroU64`] makes a zero-width chunk (which could
    /// never make progress) unrepresentable at the call site.
    #[must_use]
    pub fn with_backfill_chunk_size(mut self, blocks: NonZeroU64) -> Self {
        self.backfill_chunk_size = blocks.get();
        self
    }

    /// Persist a position-group only once it is `depth` positions deep (default
    /// 1). Events are still delivered downstream live and immediately; only the
    /// Store write lags. A reorg shallower than `depth` is corrected in the
    /// buffer before any orphaned row is written; a reorg deeper than `depth`
    /// halts persistence (a restart re-syncs). Choose `depth` above the deepest
    /// reorg you expect. A [`NonZeroU64`] makes a zero depth (which would write
    /// the open live position before it can be confirmed) unrepresentable.
    #[must_use]
    pub fn with_confirmation_depth(mut self, depth: NonZeroU64) -> Self {
        self.confirmation_depth = depth.get();
        self
    }
}

/// The segments of a [`Persisted`] subscription, in delivery order.
///
/// Construction forces an editor to account for every segment; the order in
/// which they reach the subscriber is fixed in exactly one place,
/// [`Segments::into_stream`]. The boundary arithmetic that keeps the segments
/// disjoint lives at the construction site in [`Persisted::subscribe`].
struct Segments<'a, E> {
    /// Stored history reconstructed from the database. Empty on every
    /// subscribe after the first (see the replay-once flag on [`Persisted`]).
    replay: CollectorStream<'a, E>,
    /// The Backfill and Live Tail segments as one stream — still delivered
    /// backfill first, live second, but through a single writer pipeline
    /// ([`persist_and_emit`]) because the confirmation window spans their
    /// boundary: the gap's last `confirmation_depth` positions stay pending in
    /// the window (only `[resume ..= tip − depth]` is settled final), so a
    /// reorg within the depth of the subscribe-time tip is corrected in the
    /// buffer instead of freezing orphaned backfill rows into the store.
    synced: CollectorStream<'a, E>,
}

impl<'a, E: Send + 'a> Segments<'a, E> {
    /// Deliver replay, then the synced gap-and-live stream. Replay must come
    /// first so strategies see history in sort-key order, and the live tail
    /// ends the chain because it never ends.
    fn into_stream(self) -> CollectorStream<'a, E> {
        Box::pin(self.replay.chain(self.synced))
    }
}

#[async_trait]
impl<C, S, E> Collector<E> for Persisted<C, S>
where
    C: PersistableCollector<E>,
    S: Store<C::Pos>,
    E: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    async fn subscribe(&self) -> Result<CollectorStream<'_, E>> {
        // Resolve the Record — the table name and the event <-> row mapping —
        // before opening any source subscription, so a missing table name fails
        // the subscribe without mutating any state. A schema override wins; else
        // the name captured by `with_persistence` / `with_persistence_named` /
        // `with_table` is used; a bare `Persisted::new` over a non-SolEvent type
        // resolves neither and errors. Shared (Arc)
        // between the backfill and live writers, so a schema frozen during
        // backfill is reused by the live tail.
        let record = Arc::new(match (self.schema.clone(), self.table.clone()) {
            (Some(schema), _) => Record::<E>::declared(schema)?,
            (None, Some(table)) => Record::<E>::inferred(table),
            (None, None) => anyhow::bail!(
                "no table name for persisted events: use with_persistence (SolEvent types), with_persistence_named, with_table, or try_with_schema"
            ),
        });

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
        let tip_key = tip.sort_key();
        let effective_tip = self.to_block.map(|b| b.min(tip_key)).unwrap_or(tip_key);

        let last = self.store.stored_position(record.table()).await?;

        // 1. Replay stored history — first subscribe only (see the `replayed`
        //    field doc). In bounded mode the archive may already be ahead of
        //    the snapshot; replay is then clamped to `to_block` so no event
        //    past the snapshot is delivered.
        let replay_up_to = match (self.to_block, &last) {
            (Some(cap), Some(l)) if l.sort_key() > cap => Some(C::Pos::from_sort_key(cap)),
            _ => last.clone(),
        };
        let first_subscribe = !self.replayed.load(Ordering::SeqCst);
        let replay: CollectorStream<'_, E> = if first_subscribe {
            let inner = replay_stored(&self.store, &record, replay_up_to).await?;
            // Flip the replay-once flag when the archive is first *consumed*,
            // not merely when `subscribe` succeeds. The engine retries
            // `subscribe` on error, but it also discards the returned stream
            // when a *sibling* fails the composite subscribe — e.g. this
            // `Persisted` chained or merged with another collector, where the
            // other source's subscribe errors after this one already succeeded.
            // In that case the stream is dropped without ever being polled, so
            // flipping the flag eagerly here would make the retry skip the DB
            // replay while backfill covers only positions after `last` —
            // stranding the stored history. A zero-item stream that sets the
            // flag on its first poll, chained ahead of the real replay, ties the
            // flip to actual consumption.
            let replayed = &self.replayed;
            let mark = futures::stream::poll_fn(move |_| {
                replayed.store(true, Ordering::SeqCst);
                std::task::Poll::Ready(None::<E>)
            });
            Box::pin(mark.chain(inner)) as CollectorStream<'_, E>
        } else {
            Box::pin(futures::stream::empty()) as CollectorStream<'_, E>
        };

        // 2. Backfill the RPC gap `[resume ..= effective_tip]`, never reaching
        //    below the configured start block. `resume` is the stored position's
        //    resume key (last+1 for blocks). When the stored height has already
        //    reached the tip (a restart within one interval, or a node whose tip
        //    lags the store) there is no gap, and querying the inverted range
        //    would be rejected — skip the query instead.
        //
        //    The gap is sliced into bounded chunks queried one at a time; a
        //    chunk that fails after the first cancels `poison`, which ends the
        //    live tail too (see below).
        let poison = CancellationToken::new();
        let backfill_from = last
            .as_ref()
            .map(|l| l.resume_key())
            .unwrap_or(0)
            .max(self.start_block);
        let backfill_source: CollectorStream<'_, (C::Pos, E)> = if backfill_from > effective_tip {
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

        // 3. Live tail. Skipped entirely in bounded mode (to_block set) — the
        //    stream ends naturally once backfill is exhausted. It ends when
        //    `poison` is cancelled by a failed backfill chunk; ending the whole
        //    stream hands the failure to the Reconnect Policy, whose resubscribe
        //    backfills again from the last stored position.
        let live_source: CollectorStream<'_, Indexed<C::Pos, E>> = match live_source {
            None => Box::pin(futures::stream::empty()),
            Some(src) => Box::pin(src.take_until(poison.cancelled_owned())),
        };

        // The gap still COVERS `[resume ..= effective_tip]` — no backfilling
        // less — but only `[.. ..= tip − depth]` is settled final: nothing there
        // can reorg without exceeding the confirmation depth. The gap's last
        // `confirmation_depth` positions stay pending in the same confirmation
        // window the live tail writes through, so a live re-emission at or
        // below the tip is deduped (boundary overlap), reorg-corrected
        // (in-window fork), or halts persistence (deeper than the depth) — the
        // exact guarantees the live tail already has — instead of being hard-
        // dropped while zero-confirmation backfill rows stay final forever. In
        // bounded mode there is no live tail to mature or correct a window, so
        // the whole snapshot range settles final exactly as before.
        //
        // `checked_sub`, not saturating: when the tip is younger than the
        // depth there is no position with `depth` confirmations yet, so
        // *nothing* may settle final — a saturated cut of 0 would write
        // position 0's rows with fewer confirmations than the caller asked
        // for. `None` routes the whole gap through the confirmation window.
        let final_cut = if self.to_block.is_some() {
            Some(effective_tip)
        } else {
            effective_tip.checked_sub(self.confirmation_depth)
        };
        let synced = persist_and_emit(
            backfill_source,
            live_source,
            &self.store,
            record,
            self.confirmation_depth,
            final_cut,
            last,
        );

        Ok(Segments { replay, synced }.into_stream())
    }
}

/// Replay stored events up to and including `last`, reconstructed from each
/// row's payload column. Returns an empty stream when nothing is stored.
async fn replay_stored<'a, E, S, P>(
    store: &'a S,
    record: &Record<E>,
    last: Option<P>,
) -> Result<CollectorStream<'a, E>>
where
    E: DeserializeOwned + Send + 'a,
    S: Store<P> + 'a,
    P: Position,
{
    let Some(up_to) = last else {
        return Ok(Box::pin(futures::stream::empty()));
    };

    let rows = store.replay(&record.payload_schema(), up_to).await?;
    // A stored row that cannot be reconstructed is a hard error, not a row to
    // skip: replay feeds strategies the historical view they reason over, and
    // `_artemis_progress` already counts these positions as processed. Silently
    // omitting them would hand strategies a quietly truncated history, so we
    // fail the subscribe (the engine retries, surfacing the problem) instead —
    // which is why decoding is eager, not deferred into the stream: once the
    // stream is returned, a bad row could only be skipped or kill a stream the
    // engine believes healthy. Peak memory stays one materialization, not two:
    // `into_iter` frees each row as its event is decoded.
    let events = rows
        .into_iter()
        .map(|Row(cols)| match cols.into_iter().next() {
            Some(SqlValue::Text(payload)) => record.decode(&payload),
            other => Err(anyhow::anyhow!(
                "unexpected payload column on replay: {other:?}"
            )),
        })
        .collect::<Result<Vec<E>>>()?;
    Ok(Box::pin(futures::stream::iter(events)))
}

/// Query the inclusive range `[from ..= to]` (sort keys) in aligned windows of
/// at most `chunk` positions, one `query_range` call at a time, flattened into a
/// single stream.
///
/// The first window is queried eagerly so a backfill that can't start at all
/// fails the subscribe (feeding the Reconnect Policy's counter). Later windows
/// are queried lazily as the stream is consumed; one of them failing cannot
/// fail the already-returned subscribe, so it instead logs, cancels `poison`,
/// and ends the stream — every position delivered up to that point is complete,
/// because windows are sort-key-aligned.
async fn chunked_query<'a, C, E>(
    collector: &'a C,
    from: u64,
    to: u64,
    chunk: u64,
    poison: CancellationToken,
) -> Result<CollectorStream<'a, (C::Pos, E)>>
where
    C: PersistableCollector<E> + ?Sized,
    E: Send + 'a,
{
    /// Last sort key of the window starting at `from`: `from + chunk - 1`,
    /// saturating, and never beyond `to`.
    fn window_end(from: u64, to: u64, chunk: u64) -> u64 {
        from.saturating_add(chunk - 1).min(to)
    }

    let first_to = window_end(from, to, chunk);
    // Eager first window: a window that can't be queried at all fails the
    // subscribe (feeding the Reconnect Policy). A provider response-size cap is
    // *not* such a failure — `query_range_split` bisects and retries it.
    let first = query_range_split(collector, from, first_to).await?;

    let stream = async_stream::stream! {
        for item in first {
            yield item;
        }
        let mut next_from = first_to.saturating_add(1);
        // `saturating_add` can only stall at u64::MAX, where `window_end`
        // already returned `to` and the loop is done.
        while next_from <= to {
            let next_to = window_end(next_from, to, chunk);
            match query_range_split(collector, next_from, next_to).await {
                Ok(window) => {
                    for item in window {
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

/// The boxed, position-indexed result future of one (possibly bisected)
/// backfill window query -- factored out to keep the recursive
/// `query_range_split` signature under the type-complexity bar.
type WindowFuture<'a, P, E> = futures::future::BoxFuture<'a, Result<Vec<(P, E)>>>;

/// Query the inclusive sort-key range `[from ..= to]`, **bisecting and
/// retrying** when the provider rejects the window for exceeding its
/// response-size / block-range cap (e.g. Alchemy answers a too-large
/// `eth_getLogs` with "up to a 2,000 block range … 10K logs"). The
/// `backfill_chunk_size` is only a starting hint: a fixed window can still
/// exceed a *log-count* cap for a high-volume event, which used to fail every
/// `subscribe` and march the collector to Fatal.
///
/// Only a size/range cap triggers a split. A single-position window that still
/// fails, or any other error (auth, revert, decode), propagates unchanged so a
/// genuinely broken query still surfaces. The sort-key bounds are lifted to the
/// collector's [`Position`] via [`Position::from_sort_key`] at each window edge.
fn query_range_split<'a, C, E>(collector: &'a C, from: u64, to: u64) -> WindowFuture<'a, C::Pos, E>
where
    C: PersistableCollector<E> + ?Sized,
    E: Send + 'a,
{
    Box::pin(async move {
        let query = collector.query_range(
            <C::Pos as Position>::from_sort_key(from),
            <C::Pos as Position>::from_sort_key(to),
        );
        match query.await {
            Ok(mut stream) => {
                let mut events = Vec::new();
                while let Some(item) = stream.next().await {
                    events.push(item);
                }
                Ok(events)
            }
            Err(e) if from < to && is_response_size_error(&e) => {
                let mid = from + (to - from) / 2;
                tracing::warn!(
                    from,
                    mid,
                    to,
                    "splitting backfill window over provider response-size cap: {e}"
                );
                let mut lo = query_range_split(collector, from, mid).await?;
                let hi = query_range_split(collector, mid + 1, to).await?;
                lo.extend(hi);
                Ok(lo)
            }
            Err(e) => Err(e),
        }
    })
}

/// Whether an `eth_getLogs` error is the provider signalling the window was too
/// large (block-range or result-size cap) — the cue to bisect and retry —
/// rather than a genuine fault (auth, revert, decode) which must propagate.
///
/// The bare "-32602" (invalid params) is matched deliberately: Alchemy answers
/// an oversized `eth_getLogs` with that code, and dropping it would revive the
/// subscribe-fails-to-Fatal crash cycle the bisection exists to prevent. The
/// trade-off: a *permanently* invalid filter also reports -32602, so it is
/// bisected all the way down to single-position windows (≈log₂(chunk) wasted
/// calls per window, on every reconnect) before the unsplittable error finally
/// propagates. Slow-but-loud on a bad filter beats Fatal on a big one.
fn is_response_size_error(e: &anyhow::Error) -> bool {
    let s = format!("{e:#}").to_lowercase();
    s.contains("block range")
        || s.contains("response size")
        || s.contains("max results")
        || s.contains("too many results")
        || s.contains("query returned more than")
        || s.contains("-32602")
        || s.contains("too big")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::BlockPosition;

    /// A non-SolEvent event type: plain serde, no Solidity signature — so no
    /// table name can be derived from `E`.
    #[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
    struct LedgerRow {
        id: u64,
    }

    /// A minimal collector for `LedgerRow` keyed by `BlockPosition`. Its streams
    /// are empty; the point is that the no-table-name error fires before they
    /// are ever used.
    struct BareCollector;

    #[async_trait]
    impl PersistableCollector<LedgerRow> for BareCollector {
        type Pos = BlockPosition;
        async fn subscribe_indexed(
            &self,
        ) -> Result<CollectorStream<'_, Indexed<BlockPosition, LedgerRow>>> {
            Ok(Box::pin(futures::stream::empty()))
        }
        async fn query_range(
            &self,
            _from: BlockPosition,
            _to: BlockPosition,
        ) -> Result<CollectorStream<'_, (BlockPosition, LedgerRow)>> {
            Ok(Box::pin(futures::stream::empty()))
        }
        async fn tip(&self) -> Result<BlockPosition> {
            Ok(BlockPosition(0))
        }
    }

    /// A no-op store.
    struct NullStore;

    #[async_trait]
    impl Store for NullStore {
        async fn write(
            &self,
            _schema: &TableSchema,
            _position: BlockPosition,
            _rows: Vec<Row>,
        ) -> Result<()> {
            Ok(())
        }
        async fn stored_position(&self, _table: &str) -> Result<Option<BlockPosition>> {
            Ok(None)
        }
        async fn replay(&self, _schema: &TableSchema, _up_to: BlockPosition) -> Result<Vec<Row>> {
            Ok(vec![])
        }
    }

    /// A bare `Persisted::new` over a non-SolEvent type resolves no table name
    /// (no schema override, no captured name, and `E` is not `SolEvent`), so
    /// subscribe errors with the documented literal and mutates no state.
    /// Had subscribe fallen back to a derived or empty name it would succeed,
    /// so the `Ok` arm panics.
    #[tokio::test]
    async fn subscribe_without_table_name_errors() {
        let persisted = Persisted::new(BareCollector, NullStore);
        // `expect_err` is unavailable: the `Ok` type (a boxed stream) is not
        // `Debug`, so match to extract the error.
        let err = match persisted.subscribe().await {
            Ok(_) => panic!("a nameless persisted subscription must fail"),
            Err(e) => e,
        };
        assert_eq!(
            err.to_string(),
            "no table name for persisted events: use with_persistence (SolEvent types), with_persistence_named, with_table, or try_with_schema"
        );
    }

    /// `with_persistence_named` supplies a table name for a non-SolEvent type,
    /// so subscribe resolves a `Record` and succeeds.
    #[tokio::test]
    async fn with_persistence_named_resolves_a_table_for_non_solevent_types() {
        // Bounded (to_block) so the empty backfill ends the stream at once.
        let persisted = BareCollector
            .with_persistence_named(NullStore, "ledger_rows")
            .with_to_block(0);
        let mut stream = persisted
            .subscribe()
            .await
            .expect("a named persisted subscription resolves a table");
        assert!(
            stream.next().await.is_none(),
            "the scripted feed is empty; the point is that subscribe succeeded"
        );
    }
}
