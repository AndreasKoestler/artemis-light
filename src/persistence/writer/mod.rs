//! Position-aligned persistence for a [`Persisted`](crate::persistence::Persisted)
//! subscription.
//!
//! Two writers turn a stream of `(position, event)` into Store writes while
//! leaving the events flowing downstream unchanged: [`BlockWriter`] for the
//! finite backfill range, [`ConfirmationWindow`] for the unbounded live tail.
//! They differ only in *when* a position-group is written and how a reorg is
//! handled; both share the same safety core, [`GapFreeWriter`], which guarantees
//! the one invariant a write must never break — the stored position is always a
//! gap-free prefix, so a failure halts persistence rather than advancing the
//! watermark past a position whose rows were never written.
//!
//! Both writers are generic over the [`Position`] type `P` (default
//! `BlockPosition` at the call sites), grouping and ordering by `P::sort_key`.

use std::sync::Arc;

use futures::StreamExt;
use serde::Serialize;

use crate::persistence::{Position, Record, Row, Store, TableSchema};
use crate::types::CollectorStream;

mod block;
mod window;

use block::BlockWriter;
use window::ConfirmationWindow;

/// The gap-free-prefix safety core shared by [`BlockWriter`] and
/// [`ConfirmationWindow`].
///
/// It owns the Store handle, the [`Record`], and a sticky health flag, and
/// performs the two fallible steps both writers make — encoding an event into a
/// row and writing a position-group's rows — going permanently unhealthy on
/// either failure. Once unhealthy it stays that way: the writers stop doing
/// per-event work, so the stored watermark freezes at the last fully written
/// position and a restart re-syncs from there. The *buffering* (which rows are
/// held for which position) and the *reorg policy* (halt vs. correct-in-place)
/// are the writers' own; the core only decides when a write is no longer safe.
struct GapFreeWriter<'a, S, E> {
    store: &'a S,
    record: Arc<Record<E>>,
    /// The write schema, captured from the [`Record`] on the first successful
    /// [`encode`](Self::encode). A declared schema is available immediately; an
    /// inferred one is frozen by that first encode. Held here so [`flush`] never
    /// has to assume the freeze happened — it writes only what was encoded.
    ///
    /// [`flush`]: Self::flush
    schema: Option<TableSchema>,
    healthy: bool,
}

impl<'a, S, E: Serialize> GapFreeWriter<'a, S, E> {
    fn new(store: &'a S, record: Arc<Record<E>>) -> Self {
        Self {
            store,
            record,
            schema: None,
            healthy: true,
        }
    }

    /// Whether the writer may still persist. Once `false`, it stays `false`.
    fn healthy(&self) -> bool {
        self.healthy
    }

    /// Encode one event into a row, or go unhealthy if it cannot be persisted.
    ///
    /// An event that can't be encoded must never be skipped: progress advancing
    /// past its position would hand the next restart exactly the quietly
    /// truncated history `replay_stored` refuses to emit. Returns `None` once the
    /// writer has halted; the caller then discards its own buffer.
    fn encode(&mut self, event: &E) -> Option<Row> {
        match self.record.encode(event) {
            Ok(row) => {
                // The encode just froze the inferred columns (declared ones
                // were always present), so the write schema is available now.
                // Capture it once; `flush` writes only groups built from rows
                // this method produced, so a row in hand means a schema in hand.
                if self.schema.is_none() {
                    self.schema = self.record.schema();
                }
                Some(row)
            }
            Err(e) => {
                self.fail(format_args!("failed to encode row: {e}"));
                None
            }
        }
    }

    /// Write one position-group's buffered rows in a single transaction. On
    /// failure the writer goes unhealthy (the caller must stop advancing the
    /// stored watermark) and returns `false`; the event stream itself keeps
    /// flowing either way. Generic over the [`Position`] type so one core serves
    /// both writers for any `P`.
    async fn flush<P: Position>(&mut self, position: P, rows: Vec<Row>) -> bool
    where
        S: Store<P>,
    {
        let Some(schema) = &self.schema else {
            // No event has been encoded, so there is nothing to persist: the
            // buffered rows `flush` writes are only ever produced by `encode`,
            // which captures the schema. An empty group is a healthy no-op.
            return true;
        };
        match self.store.write(schema, position, rows).await {
            Ok(()) => true,
            Err(e) => {
                tracing::error!("failed to persist position; halting persistence: {e}");
                self.healthy = false;
                false
            }
        }
    }

    /// Mark the writer permanently unhealthy and log why. The caller discards
    /// its own buffer (its positions will be re-fetched by a restart's backfill);
    /// the stored watermark stays at the last fully written position.
    fn fail(&mut self, reason: std::fmt::Arguments<'_>) {
        self.healthy = false;
        tracing::error!(
            "halting persistence ({reason}); events keep flowing, and a \
             restart will re-sync from the last stored position"
        );
    }
}

/// Wrap a stream of `(position, event)` so that each event is buffered and
/// written to `store` one transaction per complete position-group, while the
/// plain events flow downstream unchanged.
///
/// A group is "complete" once a higher sort key is observed. The trailing group
/// is flushed at stream end only when `flush_final` is set (true for a finite
/// backfill range, false for a live tail).
pub(super) fn persist_and_emit<'a, E, P, S>(
    mut source: CollectorStream<'a, (P, E)>,
    store: &'a S,
    record: Arc<Record<E>>,
    flush_final: bool,
    seed: Option<P>,
) -> CollectorStream<'a, E>
where
    E: Serialize + Send + Sync + 'static,
    P: Position,
    S: Store<P> + 'a,
{
    let stream = async_stream::stream! {
        let mut writer = BlockWriter::new(store, record, seed);

        while let Some((position, event)) = source.next().await {
            // `record` returns false for a Dedupe re-observation already covered
            // by the watermark: suppress it downstream too (replay delivered it
            // once). The Halt (block) path never suppresses, so EVM emission is
            // bit-identical [position-trait.DEDUP.2].
            if writer.record(position, &event).await {
                yield event;
            }
        }

        if flush_final {
            writer.finish().await;
        }
    };

    Box::pin(stream)
}

/// Like [`persist_and_emit`], but persists with a [`ConfirmationWindow`]: a
/// group is written only once it is `depth` confirmations deep, and an in-window
/// reorg is corrected before any orphaned row is written. The window's finalized
/// watermark is seeded from the stored position at subscribe so a live
/// re-observation at or below it is a deep reorg. There is no `flush_final` —
/// the live tail never ends, and the unflushed window is intentionally left for
/// a restart's backfill to re-fetch (the whole window, not just a single open
/// position).
pub(super) fn persist_and_emit_windowed<'a, E, P, S>(
    mut source: CollectorStream<'a, (P, E)>,
    store: &'a S,
    record: Arc<Record<E>>,
    depth: u64,
    seed: Option<P>,
) -> CollectorStream<'a, E>
where
    E: Serialize + Send + Sync + 'static,
    P: Position,
    S: Store<P> + 'a,
{
    let stream = async_stream::stream! {
        let mut writer = ConfirmationWindow::new(store, record, depth, seed);
        while let Some((position, event)) = source.next().await {
            // Suppress a Dedupe re-observation downstream; deliver otherwise.
            // Halt sources never suppress, so live block emission is unchanged
            // [position-trait.DEDUP.2].
            if writer.record(position, &event).await {
                yield event;
            }
        }
    };

    Box::pin(stream)
}

/// Event types and stores shared by the [`block`] and [`window`] writer tests.
#[cfg(test)]
pub(super) mod test_support {
    use anyhow::Result;
    use async_trait::async_trait;

    use crate::persistence::{
        BlockPosition, Position, Record, Reobservation, Row, Store, TableSchema,
    };

    alloy::sol! {
        #[derive(serde::Serialize)]
        event Ping(uint256 value);
    }

    pub(crate) fn ping(value: u64) -> Ping {
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

    pub(crate) fn bad_ping(value: u64) -> BadPing {
        BadPing {
            value: alloy::primitives::U256::from(value),
        }
    }

    /// A fresh inferred [`Record`] for `E`, shared by the writer constructors in
    /// the test modules.
    pub(crate) fn record<E>() -> std::sync::Arc<Record<E>>
    where
        E: alloy::sol_types::SolEvent + serde::Serialize,
    {
        std::sync::Arc::new(Record::new(None).unwrap())
    }

    /// A store whose every write fails.
    pub(crate) struct FailingStore;

    #[async_trait]
    impl Store for FailingStore {
        async fn write(
            &self,
            _schema: &TableSchema,
            position: BlockPosition,
            _rows: Vec<Row>,
        ) -> Result<()> {
            anyhow::bail!("simulated write failure at position {position:?}")
        }
        async fn stored_position(&self, _table: &str) -> Result<Option<BlockPosition>> {
            Ok(None)
        }
        async fn replay(&self, _schema: &TableSchema, _up_to: BlockPosition) -> Result<Vec<Row>> {
            Ok(vec![])
        }
    }

    /// A store that records the position and row count of every written group
    /// and always succeeds. Generic over the [`Position`] type so both the block
    /// path (`P = BlockPosition`) and a Dedupe frontier can be exercised.
    pub(crate) struct RecordingStore<P = BlockPosition> {
        written: std::sync::Mutex<Vec<(P, usize)>>,
    }

    impl<P> Default for RecordingStore<P> {
        fn default() -> Self {
            Self {
                written: std::sync::Mutex::new(Vec::new()),
            }
        }
    }

    impl<P: Position> RecordingStore<P> {
        /// Sort keys of the written groups, in write order.
        pub(crate) fn written(&self) -> Vec<u64> {
            self.written
                .lock()
                .unwrap()
                .iter()
                .map(|(p, _)| p.sort_key())
                .collect()
        }

        /// The full positions written, in write order — for asserting a group's
        /// folded (unioned) frontier.
        pub(crate) fn positions(&self) -> Vec<P> {
            self.written
                .lock()
                .unwrap()
                .iter()
                .map(|(p, _)| p.clone())
                .collect()
        }

        /// Total rows written across every group — for exactly-once dedupe
        /// assertions.
        pub(crate) fn total_rows(&self) -> usize {
            self.written.lock().unwrap().iter().map(|(_, n)| n).sum()
        }
    }

    #[async_trait]
    impl<P: Position> Store<P> for RecordingStore<P> {
        async fn write(&self, _schema: &TableSchema, position: P, rows: Vec<Row>) -> Result<()> {
            self.written.lock().unwrap().push((position, rows.len()));
            Ok(())
        }
        async fn stored_position(&self, _table: &str) -> Result<Option<P>> {
            Ok(None)
        }
        async fn replay(&self, _schema: &TableSchema, _up_to: P) -> Result<Vec<Row>> {
            Ok(vec![])
        }
    }

    /// A test-only [`Position`] with a [`Reobservation::Dedupe`] policy: a
    /// `(time, seen-set)` frontier, so the writer dedupe/suppress path can be
    /// exercised without depending on `TimeFrontier` (Phase 5). Its `advance`
    /// moves time forward (dropping the stale seen-set), unions same-instant
    /// identities, and treats an earlier instant as a no-op; `contains` covers
    /// any earlier instant and a same-instant subset.
    #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    pub(crate) struct TestFrontier {
        pub(crate) time: u64,
        pub(crate) seen: std::collections::BTreeSet<u64>,
    }

    impl TestFrontier {
        /// A single-identity frontier: identity `id` observed at instant `time`.
        pub(crate) fn at(time: u64, id: u64) -> Self {
            Self {
                time,
                seen: std::iter::once(id).collect(),
            }
        }
    }

    impl Position for TestFrontier {
        const REOBSERVATION: Reobservation = Reobservation::Dedupe;

        fn sort_key(&self) -> u64 {
            self.time
        }

        fn from_sort_key(key: u64) -> Self {
            Self {
                time: key,
                seen: std::collections::BTreeSet::new(),
            }
        }

        fn advance(prev: Option<Self>, next: Self) -> Self {
            match prev {
                None => next,
                Some(prev) => match next.time.cmp(&prev.time) {
                    std::cmp::Ordering::Greater => next,
                    std::cmp::Ordering::Less => prev,
                    std::cmp::Ordering::Equal => {
                        let mut seen = prev.seen;
                        seen.extend(next.seen);
                        Self {
                            time: prev.time,
                            seen,
                        }
                    }
                },
            }
        }

        fn contains(&self, pos: &Self) -> bool {
            pos.time < self.time || (pos.time == self.time && pos.seen.is_subset(&self.seen))
        }

        fn resume_key(&self) -> u64 {
            self.time
        }

        fn encode(&self) -> String {
            serde_json::to_string(self).expect("TestFrontier serialises")
        }

        fn decode(encoded: &str) -> Result<Self> {
            Ok(serde_json::from_str(encoded)?)
        }
    }
}
