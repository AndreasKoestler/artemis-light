//! Behaviour tests for the persistence layer, exercised through its public API.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use alloy::node_bindings::{Anvil, AnvilInstance};
use alloy::primitives::{Address, U256};
use alloy::providers::{Provider, ProviderBuilder, WsConnect};
use alloy::signers::local::PrivateKeySigner;
use alloy::sol;
use anyhow::Result;
use artemis_light::collectors::EventCollector;
use artemis_light::persistence::{
    Column, PersistExt, PersistableCollector, Row, SqlType, SqlValue, SqliteStore, Store,
    TableSchema, derive, derive_record, derive_record_with, from_payload, payload_schema,
    table_name,
};
use artemis_light::types::{Collector, CollectorStream};
use async_trait::async_trait;
use futures::StreamExt;

sol! {
    #[sol(rpc, bytecode = "6080604052348015600e575f5ffd5b5060d980601a5f395ff3fe6080604052348015600e575f5ffd5b50600436106030575f3560e01c80633fa4f2451460345780635524107714604d575b5f5ffd5b603b5f5481565b60405190815260200160405180910390f35b605c6058366004608d565b605e565b005b5f81815560405182917f012c78e2b84325878b1bd9d250d772cfe5bda7722d795f45036fa5e1e6e303fc91a250565b5f60208284031215609c575f5ffd5b503591905056fea264697066735822122050fddb04e40945ebc7c51aef06d27a86c4aa98943b773d9ffdc789caf784441064736f6c634300081e0033")]
    contract Emitter {
        uint256 public value;

        #[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
        event ValueSet(uint256 indexed value);

        function setValue(uint256 _value) external {
            value = _value;
            emit ValueSet(_value);
        }
    }
}

use Emitter::ValueSet;

sol! {
    // A two-field event used to exercise multi-column schema derivation and the
    // override field-alignment logic (rename-away, missing-field, reorder).
    #[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
    event Transfer(address indexed from, uint256 amount);
}

fn transfer_event() -> Transfer {
    Transfer {
        from: Address::ZERO,
        amount: U256::from(1000),
    }
}

/// Spawns Anvil (1s blocks) and a WS provider with a wallet signer.
async fn spawn_anvil_with_signer() -> Result<(impl Provider + Clone, AnvilInstance)> {
    let anvil = Anvil::new().block_time(1).chain_id(1337).try_spawn()?;
    let signer: PrivateKeySigner = anvil.keys()[0].clone().into();
    let ws = WsConnect::new(anvil.ws_endpoint());
    let provider = ProviderBuilder::new().wallet(signer).connect_ws(ws).await?;
    Ok((provider, anvil))
}

/// A scripted [`PersistableCollector`] used to drive `Persisted` deterministically.
#[derive(Default)]
struct FakeCollector {
    live: Vec<(u64, u64)>,
    backfill: Vec<(u64, u64)>,
    tip: u64,
    /// Number of leading `query_range` calls that should error before the rest
    /// succeed — used to simulate a transient RPC backfill failure.
    query_range_fails: AtomicUsize,
    /// 1-based index of a single `query_range` call to fail (0 = none) — used
    /// to simulate one bad chunk in the middle of a sliced backfill.
    query_range_fails_on_call: AtomicUsize,
    /// Every `(from, to)` range passed to `query_range`, for asserting how the
    /// wrapper slices the backfill.
    queried: Arc<std::sync::Mutex<Vec<(u64, u64)>>>,
}

impl FakeCollector {
    fn live(mut self, events: Vec<(u64, u64)>) -> Self {
        self.live = events;
        self
    }
    fn backfill(mut self, events: Vec<(u64, u64)>) -> Self {
        self.backfill = events;
        self
    }
    fn tip(mut self, tip: u64) -> Self {
        self.tip = tip;
        self
    }
    fn fail_query_range_times(self, n: usize) -> Self {
        self.query_range_fails.store(n, Ordering::SeqCst);
        self
    }
    fn fail_query_range_on_call(self, n: usize) -> Self {
        self.query_range_fails_on_call.store(n, Ordering::SeqCst);
        self
    }
    /// Handle onto the recorded `query_range` calls; stays usable after the
    /// collector has been consumed by `with_persistence`.
    fn queried(&self) -> Arc<std::sync::Mutex<Vec<(u64, u64)>>> {
        self.queried.clone()
    }
}

fn value_event(value: u64) -> ValueSet {
    ValueSet {
        value: U256::from(value),
    }
}

#[async_trait]
impl PersistableCollector<ValueSet> for FakeCollector {
    async fn subscribe_indexed(&self) -> Result<CollectorStream<'_, (u64, ValueSet)>> {
        let events: Vec<_> = self
            .live
            .iter()
            .map(|&(b, v)| (b, value_event(v)))
            .collect();
        Ok(Box::pin(futures::stream::iter(events)))
    }

    async fn query_range(
        &self,
        from: u64,
        to: u64,
    ) -> Result<CollectorStream<'_, (u64, ValueSet)>> {
        let call_number = {
            let mut queried = self.queried.lock().unwrap();
            queried.push((from, to));
            queried.len()
        };
        // Real RPC providers reject inverted `eth_getLogs` ranges; tolerating
        // them here would hide a wrapper that issues impossible queries.
        if from > to {
            anyhow::bail!("inverted range: from {from} > to {to}");
        }
        if self.query_range_fails_on_call.load(Ordering::SeqCst) == call_number {
            anyhow::bail!("simulated query_range failure on call {call_number}");
        }
        let remaining = self.query_range_fails.load(Ordering::SeqCst);
        if remaining > 0 {
            self.query_range_fails
                .store(remaining - 1, Ordering::SeqCst);
            anyhow::bail!("simulated query_range failure");
        }
        let events: Vec<_> = self
            .backfill
            .iter()
            .filter(|&&(b, _)| b >= from && b <= to)
            .map(|&(b, v)| (b, value_event(v)))
            .collect();
        Ok(Box::pin(futures::stream::iter(events)))
    }

    async fn tip(&self) -> Result<u64> {
        Ok(self.tip)
    }
}

/// The `value` field of each persisted row, in stored order.
async fn stored_values(store: &SqliteStore) -> Vec<String> {
    let schema = value_set_schema();
    store
        .replay(&schema, i64::MAX as u64)
        .await
        .unwrap()
        .into_iter()
        .map(|Row(mut cols)| match cols.remove(0) {
            SqlValue::Text(s) => s,
            other => panic!("unexpected value column: {other:?}"),
        })
        .collect()
}

/// A one-column `value_set` schema reused across tests.
fn value_set_schema() -> TableSchema {
    TableSchema {
        table: "value_set".into(),
        columns: vec![Column::new("value", SqlType::Text)],
    }
}

/// Slice 1: a written block can be read back via `replay`.
#[tokio::test]
async fn write_block_then_replay_reads_rows_back() {
    let store = SqliteStore::connect("sqlite::memory:").await.unwrap();
    let schema = value_set_schema();

    store
        .write_block(
            &schema,
            7,
            vec![
                Row(vec![SqlValue::Text("0x2a".into())]),
                Row(vec![SqlValue::Text("0x2b".into())]),
            ],
        )
        .await
        .unwrap();

    let rows = store.replay(&schema, 100).await.unwrap();
    assert_eq!(
        rows,
        vec![
            Row(vec![SqlValue::Text("0x2a".into())]),
            Row(vec![SqlValue::Text("0x2b".into())]),
        ]
    );
}

/// Slice 2: `last_block` reports the highest written block, `None` when empty.
#[tokio::test]
async fn last_block_tracks_highest_written_block() {
    let store = SqliteStore::connect("sqlite::memory:").await.unwrap();
    let schema = value_set_schema();

    // Nothing stored yet.
    assert_eq!(store.last_block(&schema.table).await.unwrap(), None);

    store
        .write_block(&schema, 5, vec![Row(vec![SqlValue::Text("a".into())])])
        .await
        .unwrap();
    assert_eq!(store.last_block(&schema.table).await.unwrap(), Some(5));

    store
        .write_block(&schema, 9, vec![Row(vec![SqlValue::Text("b".into())])])
        .await
        .unwrap();
    assert_eq!(store.last_block(&schema.table).await.unwrap(), Some(9));
}

/// Slice 3: a failing row in a batch rolls back the whole block, leaving prior
/// committed data and the last processed block untouched.
#[tokio::test]
async fn write_block_is_atomic_on_failure() {
    let store = SqliteStore::connect("sqlite::memory:").await.unwrap();
    let schema = value_set_schema(); // one column

    // Block 5 is written cleanly.
    store
        .write_block(&schema, 5, vec![Row(vec![SqlValue::Text("ok".into())])])
        .await
        .unwrap();

    // Block 9's second row has too few values for the schema, so its INSERT
    // fails partway through the batch.
    let result = store
        .write_block(
            &schema,
            9,
            vec![
                Row(vec![SqlValue::Text("good".into())]),
                Row(vec![]), // missing the `value` column
            ],
        )
        .await;
    assert!(result.is_err(), "malformed batch should fail");

    // Block 9 rolled back entirely: only block 5's row survives and the
    // progress marker still points at block 5.
    assert_eq!(
        store.replay(&schema, 100).await.unwrap(),
        vec![Row(vec![SqlValue::Text("ok".into())])]
    );
    assert_eq!(store.last_block(&schema.table).await.unwrap(), Some(5));
}

/// Slice 4: schema is derived from a Serialize+SolEvent type — table name from
/// the Solidity signature, columns from the event's field names.
#[tokio::test]
async fn derive_schema_uses_event_name_and_field_names() {
    let event = ValueSet {
        value: U256::from(42),
    };

    assert_eq!(table_name::<ValueSet>(), "value_set");

    let (schema, _row) = derive(&event).unwrap();
    assert_eq!(schema.table, "value_set");
    assert_eq!(schema.columns, vec![Column::new("value", SqlType::Text)]);
}

/// A multi-field event derives one column per field, named after the field and
/// ordered deterministically (by field name), with values aligned to columns.
#[test]
fn derive_maps_each_event_field_to_a_column() {
    let (schema, Row(values)) = derive(&transfer_event()).unwrap();

    assert_eq!(schema.table, "transfer");
    let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
    // Sorted by field name: `amount` before `from`.
    assert_eq!(names, vec!["amount", "from"]);
    assert_eq!(values.len(), 2);
}

/// `derive_record` appends an implicit `_payload` column holding the event's
/// full JSON, and that payload round-trips back to an equal event.
#[test]
fn derive_record_appends_payload_column_that_round_trips() {
    let event = transfer_event();
    let (schema, Row(values)) = derive_record(&event).unwrap();

    let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, vec!["amount", "from", "_payload"]);

    let SqlValue::Text(payload) = values.last().unwrap() else {
        panic!("payload column should be text");
    };
    let restored: Transfer = from_payload(payload).unwrap();
    assert_eq!(restored, event);
}

/// A schema override redirects the table, renames-away unlisted fields, fills
/// columns with no matching field with `NULL`, and still appends `_payload`.
#[test]
fn derive_record_with_override_aligns_values_by_column_name() {
    let event = transfer_event();
    let override_ = TableSchema::new("transfers_custom")
        .col("amount", SqlType::Numeric) // kept and retyped
        .col("missing", SqlType::Text); // no matching event field

    let (schema, Row(values)) = derive_record_with(&event, Some(&override_)).unwrap();

    // Table and column set follow the override, with `_payload` appended; the
    // `from` field is renamed-away because the override does not list it.
    assert_eq!(schema.table, "transfers_custom");
    let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, vec!["amount", "missing", "_payload"]);

    // `amount` is populated; `missing` has no field so it is NULL.
    assert!(matches!(values[0], SqlValue::Text(_)));
    assert_eq!(values[1], SqlValue::Null);

    // The payload is unaffected by the override and still round-trips fully.
    let SqlValue::Text(payload) = values.last().unwrap() else {
        panic!("payload column should be text");
    };
    assert_eq!(from_payload::<Transfer>(payload).unwrap(), event);
}

/// `payload_schema` describes the read-back shape — table name plus the single
/// `_payload` column — without needing an event instance.
#[test]
fn payload_schema_is_table_plus_payload_column() {
    let schema = payload_schema::<Transfer>();
    assert_eq!(schema.table, "transfer");
    assert_eq!(schema.columns, vec![Column::new("_payload", SqlType::Text)]);
}

/// A stored payload that is not valid JSON for the event type is a hard error,
/// never a silently dropped row.
#[test]
fn from_payload_errors_on_unreadable_text() {
    assert!(from_payload::<Transfer>("not a valid payload").is_err());
}

/// Slice 7: a `Persisted` collector records live events one transaction per
/// complete block, while passing the plain events downstream. The final
/// in-progress block stays unflushed (no higher block seen yet), so a restart
/// re-fetches it.
#[tokio::test]
async fn persisted_records_live_events_per_complete_block() {
    let store = Arc::new(SqliteStore::connect("sqlite::memory:").await.unwrap());

    // Two events in block 10, one in block 11 (the open tip).
    let collector = FakeCollector::default().live(vec![(10, 1), (10, 2), (11, 3)]);
    let persisted = collector.with_persistence(store.clone());

    let events: Vec<ValueSet> = persisted.subscribe().await.unwrap().collect().await;

    // Downstream sees every event, in order.
    assert_eq!(events, vec![value_event(1), value_event(2), value_event(3)]);

    // Only block 10 is complete and flushed; block 11 is still open.
    assert_eq!(store.last_block("value_set").await.unwrap(), Some(10));
    assert_eq!(
        stored_values(&store).await,
        vec!["0x1".to_string(), "0x2".to_string()]
    );
}

/// Persist one event at `block` as if a previous run had stored it.
async fn seed(store: &SqliteStore, block: u64, value: u64) {
    let (schema, row) = derive_record(&value_event(value)).unwrap();
    store.write_block(&schema, block, vec![row]).await.unwrap();
}

/// Slice 8: on subscribe, stored history is replayed first (reconstructed from
/// the database), then the live tip follows — a single chained stream.
#[tokio::test]
async fn persisted_replays_db_then_live() {
    let store = Arc::new(SqliteStore::connect("sqlite::memory:").await.unwrap());
    seed(&store, 5, 1).await;
    seed(&store, 6, 2).await;

    // Tip is the last stored block, so there is no RPC gap to backfill; the
    // live stream carries the next event.
    let collector = FakeCollector::default().live(vec![(7, 3)]).tip(6);
    let persisted = collector.with_persistence(store.clone());

    let events: Vec<ValueSet> = persisted.subscribe().await.unwrap().collect().await;
    assert_eq!(events, vec![value_event(1), value_event(2), value_event(3)]);
}

/// Slice 9: the RPC gap between the last stored block and the tip is backfilled
/// and chained as [DB replay][backfill][live]. Backfilled blocks are persisted;
/// the open live block is not.
#[tokio::test]
async fn persisted_backfills_gap_between_last_stored_and_tip() {
    let store = Arc::new(SqliteStore::connect("sqlite::memory:").await.unwrap());
    seed(&store, 5, 1).await; // last stored block = 5

    // Tip is block 8: blocks 6 and 7 must be backfilled from the RPC node,
    // then the live stream carries block 9.
    let collector = FakeCollector::default()
        .tip(8)
        .backfill(vec![(6, 2), (7, 3)])
        .live(vec![(9, 4)]);
    let persisted = collector.with_persistence(store.clone());

    let events: Vec<ValueSet> = persisted.subscribe().await.unwrap().collect().await;
    assert_eq!(
        events,
        vec![
            value_event(1),
            value_event(2),
            value_event(3),
            value_event(4)
        ]
    );

    // Backfilled blocks 6 and 7 are now stored (last complete block = 7); the
    // open live block 9 is not flushed.
    assert_eq!(store.last_block("value_set").await.unwrap(), Some(7));
    assert_eq!(
        stored_values(&store).await,
        vec!["0x1".to_string(), "0x2".to_string(), "0x3".to_string()]
    );
}

/// Restarting while the stored height already equals (or exceeds) the chain
/// tip must not issue an inverted backfill query (`from > to`). Real providers
/// reject inverted `eth_getLogs` ranges, and that error would fail every
/// resubscribe until the Reconnect Policy escalates to Fatal — a restart brick
/// whose occurrence depends on restart timing. There is no gap, so no query
/// should be issued at all.
#[tokio::test]
async fn backfill_is_skipped_when_store_is_at_the_tip() {
    let store = Arc::new(SqliteStore::connect("sqlite::memory:").await.unwrap());
    seed(&store, 6, 1).await; // last stored block = 6

    // The chain tip is *also* 6 — a restart within one block interval.
    let collector = FakeCollector::default().live(vec![(7, 2)]).tip(6);
    let queried = collector.queried();
    let persisted = collector.with_persistence(store.clone());

    let events: Vec<ValueSet> = persisted.subscribe().await.unwrap().collect().await;

    // Replay delivers the archive, live carries on; nothing was backfilled.
    assert_eq!(events, vec![value_event(1), value_event(2)]);
    assert_eq!(
        *queried.lock().unwrap(),
        Vec::<(u64, u64)>::new(),
        "no backfill query should be issued when there is no gap"
    );
}

/// The backfill must be sliced into bounded windows rather than issued as one
/// `query_range` over the whole gap. With an empty store the gap is the entire
/// chain (`[0 ..= tip]`); a single `eth_getLogs` over that is rejected by most
/// providers (range/result caps) or returns an unboundedly large payload.
#[tokio::test]
async fn backfill_is_sliced_into_bounded_chunks() {
    let store = Arc::new(SqliteStore::connect("sqlite::memory:").await.unwrap());

    let collector = FakeCollector::default()
        .tip(25)
        .backfill(vec![(5, 1), (15, 2), (25, 3)])
        .live(vec![(26, 4)]);
    let queried = collector.queried();
    let persisted = collector
        .with_persistence(store.clone())
        .with_backfill_chunk_size(10);

    let events: Vec<ValueSet> = persisted.subscribe().await.unwrap().collect().await;

    // Every backfilled event arrives, in block order, then the live tail.
    assert_eq!(
        events,
        vec![
            value_event(1),
            value_event(2),
            value_event(3),
            value_event(4)
        ]
    );
    // The gap was queried in inclusive, block-aligned windows of 10.
    assert_eq!(*queried.lock().unwrap(), vec![(0, 9), (10, 19), (20, 25)]);
    // Backfilled blocks are complete, so the trailing one is flushed too.
    assert_eq!(store.last_block("value_set").await.unwrap(), Some(25));
}

/// With an empty store, the Backfill segment must begin at the configured
/// start block instead of genesis — a strategy that only cares about recent
/// history shouldn't have to sync (or be able to fetch) the whole chain.
#[tokio::test]
async fn backfill_starts_at_the_configured_start_block() {
    let store = Arc::new(SqliteStore::connect("sqlite::memory:").await.unwrap());

    // An event below the start block must never be queried for.
    let collector = FakeCollector::default()
        .tip(125)
        .backfill(vec![(99, 1), (110, 2)]);
    let queried = collector.queried();
    let persisted = collector
        .with_persistence(store.clone())
        .with_start_block(100);

    let events: Vec<ValueSet> = persisted.subscribe().await.unwrap().collect().await;

    assert_eq!(events, vec![value_event(2)]);
    assert_eq!(*queried.lock().unwrap(), vec![(100, 125)]);
}

/// Stored history that already reaches beyond the start block wins: the
/// Backfill segment resumes from the last stored block, not from the start
/// block, so no stored range is ever re-fetched.
#[tokio::test]
async fn stored_history_beyond_the_start_block_wins() {
    let store = Arc::new(SqliteStore::connect("sqlite::memory:").await.unwrap());
    seed(&store, 110, 1).await; // last stored block = 110

    let collector = FakeCollector::default()
        .tip(125)
        .backfill(vec![(105, 9), (115, 2)]);
    let queried = collector.queried();
    let persisted = collector
        .with_persistence(store.clone())
        .with_start_block(100);

    let events: Vec<ValueSet> = persisted.subscribe().await.unwrap().collect().await;

    // Replay delivers the archive; backfill covers only `[111 ..= 125]`.
    assert_eq!(events, vec![value_event(1), value_event(2)]);
    assert_eq!(*queried.lock().unwrap(), vec![(111, 125)]);
}

/// A chunk failure in the middle of the Backfill segment must end the whole
/// subscription stream — including the live tail — not just the backfill. If
/// the live tail kept going, blocks above the tip would be persisted while the
/// failed chunk's blocks are missing, advancing the stored height over a
/// permanent gap. Ending the stream instead hands the failure to the Reconnect
/// Policy: the resubscribe backfills again from the last stored block.
#[tokio::test]
async fn mid_backfill_chunk_failure_ends_the_stream_without_corrupting_progress() {
    let store = Arc::new(SqliteStore::connect("sqlite::memory:").await.unwrap());

    let collector = FakeCollector::default()
        .tip(25)
        .backfill(vec![(5, 1), (15, 2), (25, 3)])
        .live(vec![(26, 4)])
        .fail_query_range_on_call(2); // the second chunk
    let persisted = collector
        .with_persistence(store.clone())
        .with_backfill_chunk_size(10);

    // The first chunk is queried eagerly and is fine, so subscribe succeeds.
    let stream = persisted.subscribe().await.unwrap();

    // The stream must terminate (bounded by the timeout) after delivering only
    // the first chunk — no later chunks, and crucially no live events.
    let events: Vec<ValueSet> =
        tokio::time::timeout(std::time::Duration::from_secs(5), stream.collect())
            .await
            .expect("stream must end after a failed backfill chunk");
    assert_eq!(
        events,
        vec![value_event(1)],
        "no event past the failed chunk may be delivered"
    );

    // The complete first chunk was flushed; nothing later advanced progress.
    assert_eq!(store.last_block("value_set").await.unwrap(), Some(5));
    assert_eq!(stored_values(&store).await, vec!["0x1".to_string()]);
}

/// Slice 5: a schema override declared on the Persisted Collector changes the
/// table name and column types; events persist under the overridden table.
#[tokio::test]
async fn override_schema_redirects_table_and_types() {
    let store = Arc::new(SqliteStore::connect("sqlite::memory:").await.unwrap());

    // Block 1 complete, block 2 open.
    let collector = FakeCollector::default().live(vec![(1, 7), (2, 8)]);
    let persisted = collector
        .with_persistence(store.clone())
        .with_schema(TableSchema::new("custom_values").col("value", SqlType::Numeric));
    let _events: Vec<ValueSet> = persisted.subscribe().await.unwrap().collect().await;

    // Progress and rows live under the overridden table, not the derived one.
    assert_eq!(store.last_block("custom_values").await.unwrap(), Some(1));
    assert_eq!(store.last_block("value_set").await.unwrap(), None);

    let rows = store
        .replay(
            &TableSchema::new("custom_values").col("value", SqlType::Numeric),
            i64::MAX as u64,
        )
        .await
        .unwrap();
    assert_eq!(rows, vec![Row(vec![SqlValue::Text("0x7".into())])]);
}

/// Slice 10: against a real chain, an `EventCollector` wrapped with persistence
/// forwards typed events downstream and records them with their block numbers.
#[tokio::test]
async fn event_collector_with_persistence_records_against_anvil() {
    let (provider, _anvil) = spawn_anvil_with_signer().await.unwrap();
    let provider = Arc::new(provider);
    let contract = Emitter::deploy(provider.clone()).await.unwrap();

    let store = Arc::new(SqliteStore::connect("sqlite::memory:").await.unwrap());
    let collector = EventCollector::new(contract.ValueSet_filter());
    let persisted = collector.with_persistence(store.clone());
    let mut stream = persisted.subscribe().await.unwrap();

    // Emit three events; with 1s blocks each mined tx lands in its own block.
    for v in [11u64, 22, 33] {
        contract
            .setValue(U256::from(v))
            .send()
            .await
            .unwrap()
            .watch()
            .await
            .unwrap();
    }

    // Downstream receives the typed events with the right values.
    let mut received = Vec::new();
    for _ in 0..3 {
        received.push(stream.next().await.unwrap().value);
    }
    assert_eq!(
        received,
        vec![U256::from(11), U256::from(22), U256::from(33)]
    );

    // The first two blocks are complete and persisted (block 33's is still
    // open); their block numbers were recovered from the logs.
    assert_eq!(
        stored_values(&store).await,
        vec!["0xb".to_string(), "0x16".to_string()]
    );
    assert!(store.last_block("value_set").await.unwrap().unwrap() > 0);
}

/// A stored payload that cannot be deserialized into its event type (a code or
/// schema change, or corruption) must surface as a subscribe error rather than
/// be silently dropped — strategies must never be handed a quietly truncated
/// history.
#[tokio::test]
async fn persisted_replay_fails_loudly_on_unreadable_payload() {
    let store = Arc::new(SqliteStore::connect("sqlite::memory:").await.unwrap());

    // Seed a row whose `_payload` is not valid JSON for `ValueSet`.
    let payload_schema = TableSchema::new("value_set").col("_payload", SqlType::Text);
    store
        .write_block(
            &payload_schema,
            5,
            vec![Row(vec![SqlValue::Text("not a valid payload".into())])],
        )
        .await
        .unwrap();

    let collector = FakeCollector::default().tip(5);
    let persisted = collector.with_persistence(store.clone());

    let result = persisted.subscribe().await;
    assert!(
        result.is_err(),
        "an unreadable stored payload must fail the subscribe, not be silently skipped"
    );
}

/// The engine re-subscribes after a stream ends. The full stored history must
/// be replayed only on the first subscribe; a reconnect must not re-send the
/// entire archive to strategies — the backfill segment already covers the gap.
#[tokio::test]
async fn persisted_does_not_replay_history_on_resubscribe() {
    let store = Arc::new(SqliteStore::connect("sqlite::memory:").await.unwrap());
    seed(&store, 5, 1).await;
    seed(&store, 6, 2).await;

    // Tip equals the last stored block, so there is no gap to backfill; the
    // live stream carries the next event.
    let collector = FakeCollector::default().live(vec![(7, 3)]).tip(6);
    let persisted = collector.with_persistence(store.clone());

    // First subscribe: stored history (1, 2) replayed, then live (3).
    let first: Vec<ValueSet> = persisted.subscribe().await.unwrap().collect().await;
    assert_eq!(first, vec![value_event(1), value_event(2), value_event(3)]);

    // Reconnect: stored history must NOT be replayed again — only live flows.
    let second: Vec<ValueSet> = persisted.subscribe().await.unwrap().collect().await;
    assert_eq!(second, vec![value_event(3)]);
}

/// A failed subscribe must not consume the replay-once flag. If a fallible step
/// after the DB replay (here the RPC backfill query) errors, the engine retries
/// `subscribe`; that retry must still replay the stored history rather than skip
/// it — otherwise the archive never reaches strategies and is lost for good.
#[tokio::test]
async fn failed_subscribe_does_not_consume_replay() {
    let store = Arc::new(SqliteStore::connect("sqlite::memory:").await.unwrap());
    seed(&store, 5, 1).await;
    seed(&store, 6, 2).await;

    // Tip 7 leaves a one-block gap, so a backfill query is issued; the first
    // one errors, subsequent ones succeed.
    let collector = FakeCollector::default()
        .backfill(vec![(7, 3)])
        .live(vec![(8, 4)])
        .tip(7)
        .fail_query_range_times(1);
    let persisted = collector.with_persistence(store.clone());

    // First subscribe fails because the RPC backfill query errors.
    assert!(
        persisted.subscribe().await.is_err(),
        "a failing backfill query must fail the subscribe"
    );

    // Retry: the stored history (1, 2) must still be replayed — the failed
    // attempt must not have flipped the replay-once flag.
    let events: Vec<ValueSet> = persisted.subscribe().await.unwrap().collect().await;
    assert_eq!(
        events,
        vec![
            value_event(1),
            value_event(2),
            value_event(3),
            value_event(4)
        ]
    );
}

/// A sibling collector that fails its first `subscribe` and succeeds after.
struct FailOnceCollector {
    failed: std::sync::atomic::AtomicBool,
}

#[async_trait]
impl Collector<ValueSet> for FailOnceCollector {
    async fn subscribe(&self) -> Result<CollectorStream<'_, ValueSet>> {
        if !self.failed.swap(true, Ordering::SeqCst) {
            anyhow::bail!("sibling subscribe fails the first time");
        }
        Ok(Box::pin(futures::stream::empty()))
    }
}

/// Composing a `Persisted` collector under a combinator (here `chain`) must not
/// strand the stored history. If a *sibling* source fails the composite
/// `subscribe` **after** the `Persisted` source already subscribed
/// successfully, the engine retries the whole composite — and that retry must
/// still replay the archive. The replay-once flag must therefore be consumed by
/// actually delivering the archive, not merely by a `subscribe` whose stream is
/// then dropped undrained. Regression test for the replay-strand-under-
/// composition bug.
#[tokio::test]
async fn composite_subscribe_failure_does_not_strand_replay() {
    use artemis_light::collector_ext::CollectorExt;

    let store = Arc::new(SqliteStore::connect("sqlite::memory:").await.unwrap());
    seed(&store, 5, 1).await;
    seed(&store, 6, 2).await;

    // Tip equals the last stored block, so there is no gap to backfill; live
    // carries the next event.
    let persisted = FakeCollector::default()
        .live(vec![(7, 3)])
        .tip(6)
        .with_persistence(store.clone());
    let sibling = FailOnceCollector {
        failed: std::sync::atomic::AtomicBool::new(false),
    };
    let chained = persisted.chain(sibling);

    // First subscribe fails: the `Persisted` source subscribes fine, then the
    // sibling errors and fails the whole composite. The returned `Persisted`
    // stream is dropped without ever being polled.
    assert!(
        chained.subscribe().await.is_err(),
        "a failing sibling must fail the composite subscribe"
    );

    // Retry: the stored history (1, 2) must still be replayed — the first
    // attempt's unpolled stream must not have consumed the replay-once flag.
    let events: Vec<ValueSet> = chained.subscribe().await.unwrap().collect().await;
    assert_eq!(events, vec![value_event(1), value_event(2), value_event(3)]);
}

/// A store that fails `write_block` for one specific block, delegating
/// everything else to an inner [`SqliteStore`].
struct FlakyStore {
    inner: Arc<SqliteStore>,
    fail_at: u64,
}

#[async_trait]
impl Store for FlakyStore {
    async fn write_block(&self, schema: &TableSchema, block: u64, rows: Vec<Row>) -> Result<()> {
        if block == self.fail_at {
            anyhow::bail!("simulated write failure at block {block}");
        }
        self.inner.write_block(schema, block, rows).await
    }
    async fn last_block(&self, table: &str) -> Result<Option<u64>> {
        self.inner.last_block(table).await
    }
    async fn replay(&self, schema: &TableSchema, to: u64) -> Result<Vec<Row>> {
        self.inner.replay(schema, to).await
    }
}

/// A failed block write halts persistence so the stored block height stays a
/// gap-free prefix — a later block must not advance past the failed one. The
/// event stream keeps flowing regardless.
#[tokio::test]
async fn persisted_halts_on_write_failure_to_avoid_gaps() {
    let inner = Arc::new(SqliteStore::connect("sqlite::memory:").await.unwrap());
    let store = FlakyStore {
        inner: inner.clone(),
        fail_at: 6,
    };

    // Blocks 5,6,7 complete (8 is the open tip). Block 6's write fails.
    let collector = FakeCollector::default().live(vec![(5, 1), (6, 2), (7, 3), (8, 4)]);
    let persisted = collector.with_persistence(store);

    let events: Vec<ValueSet> = persisted.subscribe().await.unwrap().collect().await;
    // Every event still reaches downstream.
    assert_eq!(
        events,
        vec![
            value_event(1),
            value_event(2),
            value_event(3),
            value_event(4)
        ]
    );

    // Only block 5 was persisted before the failure; block 7 must NOT advance
    // the height past the gap at block 6.
    assert_eq!(inner.last_block("value_set").await.unwrap(), Some(5));
    assert_eq!(stored_values(&inner).await, vec!["0x1".to_string()]);
}

/// The backfill and live segments must be disjoint at the tip: an event that
/// appears in both (because a live subscription re-delivers blocks `<= tip`)
/// is emitted once downstream and stored once.
#[tokio::test]
async fn persisted_does_not_duplicate_events_at_backfill_live_boundary() {
    let store = Arc::new(SqliteStore::connect("sqlite::memory:").await.unwrap());
    seed(&store, 5, 1).await; // last stored block = 5

    // Tip is block 7. Block 7's event is delivered by BOTH the backfill query
    // and the live subscription; block 8 is genuinely new.
    let collector = FakeCollector::default()
        .tip(7)
        .backfill(vec![(6, 2), (7, 3)])
        .live(vec![(7, 3), (8, 4)]);
    let persisted = collector.with_persistence(store.clone());

    let events: Vec<ValueSet> = persisted.subscribe().await.unwrap().collect().await;

    // Block 7 (value 3) appears exactly once — not twice.
    assert_eq!(
        events,
        vec![
            value_event(1),
            value_event(2),
            value_event(3),
            value_event(4)
        ]
    );

    // Stored once each; the open live block 8 is not flushed.
    assert_eq!(store.last_block("value_set").await.unwrap(), Some(7));
    assert_eq!(
        stored_values(&store).await,
        vec!["0x1".to_string(), "0x2".to_string(), "0x3".to_string()]
    );
}
