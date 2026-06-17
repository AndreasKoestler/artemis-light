//! Block-aligned persistence for a [`Persisted`](crate::persistence::Persisted)
//! subscription.
//!
//! Two writers turn a stream of `(block, event)` into Store writes while leaving
//! the events flowing downstream unchanged: [`BlockWriter`] for the finite
//! backfill range, [`ConfirmationWindow`] for the unbounded live tail. They
//! differ only in *when* a block is written and how a reorg is handled; both
//! share the same safety core, [`GapFreeWriter`], which guarantees the one
//! invariant a write must never break — the stored block height is always a
//! gap-free prefix, so a failure halts persistence rather than advancing the
//! height past a block whose rows were never written.

use std::sync::Arc;

use futures::StreamExt;
use serde::Serialize;

use crate::persistence::{Record, Row, Store};
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
/// row and writing a block's rows — going permanently unhealthy on either
/// failure. Once unhealthy it stays that way: the writers stop doing per-event
/// work, so the stored height freezes at the last fully written block and a
/// restart re-syncs from there. The *buffering* (which rows are held for which
/// block) and the *reorg policy* (halt vs. correct-in-place) are the writers'
/// own; the core only decides when a write is no longer safe.
struct GapFreeWriter<'a, S, E> {
    store: &'a S,
    record: Arc<Record<E>>,
    healthy: bool,
}

impl<'a, S: Store, E: Serialize> GapFreeWriter<'a, S, E> {
    fn new(store: &'a S, record: Arc<Record<E>>) -> Self {
        Self {
            store,
            record,
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
    /// past its block would hand the next restart exactly the quietly truncated
    /// history `replay_stored` refuses to emit. Returns `None` once the writer
    /// has halted; the caller then discards its own buffer.
    fn encode(&mut self, event: &E) -> Option<Row> {
        match self.record.encode(event) {
            Ok(row) => Some(row),
            Err(e) => {
                self.fail(format_args!("failed to encode row: {e}"));
                None
            }
        }
    }

    /// Write one block's buffered rows in a single transaction. On failure the
    /// writer goes unhealthy (the caller must stop advancing the stored height)
    /// and returns `false`; the event stream itself keeps flowing either way.
    async fn flush(&mut self, block: u64, rows: Vec<Row>) -> bool {
        let schema = self.record.schema().expect("frozen by first encode");
        match self.store.write_block(&schema, block, rows).await {
            Ok(()) => true,
            Err(e) => {
                tracing::error!("failed to persist block {block}; halting persistence: {e}");
                self.healthy = false;
                false
            }
        }
    }

    /// Mark the writer permanently unhealthy and log why. The caller discards
    /// its own buffer (its blocks will be re-fetched by a restart's backfill);
    /// the stored height stays at the last fully written block.
    fn fail(&mut self, reason: std::fmt::Arguments<'_>) {
        self.healthy = false;
        tracing::error!(
            "halting persistence ({reason}); events keep flowing, and a \
             restart will re-sync from the last stored block"
        );
    }
}

/// Wrap a stream of `(block, event)` so that each event is buffered and written
/// to `store` one transaction per complete block, while the plain events flow
/// downstream unchanged.
///
/// A block is "complete" once a higher block number is observed. The trailing
/// block is flushed at stream end only when `flush_final` is set (true for a
/// finite backfill range, false for a live tail).
pub(super) fn persist_and_emit<'a, E, S>(
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

/// Like [`persist_and_emit`], but persists with a [`ConfirmationWindow`]: a
/// block is written only once it is `depth` confirmations deep, and an
/// in-window reorg is corrected before any orphaned row is written. There is no
/// `flush_final` — the live tail never ends, and the unflushed window is
/// intentionally left for a restart's backfill to re-fetch (the whole window,
/// not just a single open block).
pub(super) fn persist_and_emit_windowed<'a, E, S>(
    mut source: CollectorStream<'a, (u64, E)>,
    store: &'a S,
    record: Arc<Record<E>>,
    depth: u64,
) -> CollectorStream<'a, E>
where
    E: Serialize + Send + Sync + 'static,
    S: Store + 'a,
{
    let stream = async_stream::stream! {
        let mut writer = ConfirmationWindow::new(store, record, depth);
        while let Some((block, event)) = source.next().await {
            writer.record(block, &event).await;
            yield event;
        }
    };

    Box::pin(stream)
}

/// Event types and stores shared by the [`block`] and [`window`] writer tests.
#[cfg(test)]
pub(super) mod test_support {
    use anyhow::Result;
    use async_trait::async_trait;

    use crate::persistence::{Record, Row, Store, TableSchema};

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
    pub(crate) struct RecordingStore {
        written: std::sync::Mutex<Vec<u64>>,
    }

    impl RecordingStore {
        pub(crate) fn written(&self) -> Vec<u64> {
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
}
