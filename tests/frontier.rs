//! Automated non-block end-to-end persistence test — the CI twin of
//! [`examples/hypercore_ledger_example.rs`].
//!
//! The example is the human-readable proof (four fixed stdout lines); this suite
//! is the machine-checked proof of the same flow so it cannot bit-rot. It drives
//! a custom [`TimeFrontier`] position — not a block number — through the generic
//! persistence stack to show a time-ordered source gets the same resume /
//! backfill / gap-free guarantees an EVM source already has:
//!
//! 1. **Persist** four events, two of which share one millisecond
//!    (`(1000,0xa1)` / `(1000,0xa2)`) — a bare scalar cannot keep both.
//! 2. **Restart**: drop the store-backed wrapper and rebuild a fresh one from
//!    the *stored frontier* over the same durable store.
//! 3. **Overlapping backfill**: re-read the boundary instant `2000`, which
//!    re-serves the already-stored `0xc1` alongside a genuinely NEW same-instant
//!    sibling `0xc2` and a later `0xd1`.
//! 4. **Assert** the resume point round-trips, there is no gap, and the
//!    re-observed `0xc1` dedupes to a single stored row: at-least-once delivery
//!    in, exactly-once persisted effect.
//!
//! Runs entirely on an **in-memory SQLite store** with an in-process scripted
//! feed: no Docker, no external database, no network — so it runs unattended in
//! CI. Because SQLite serves an in-memory database from a single shared
//! connection, the durable store is a single `Arc<SqliteStore>` shared across
//! both store-backed wrappers (exactly as the example does); the "restart" is
//! the wrapper being dropped and rebuilt, reading the persisted frontier back.

use std::collections::BTreeSet;
use std::sync::Arc;

use anyhow::Result;
use artemis_light::persistence::{
    BlockPosition, PAYLOAD_COLUMN, PersistExt, PersistableCollector, Position, Row, SqlType,
    SqlValue, SqliteStore, Store, TableSchema, TimeFrontier,
};
use artemis_light::types::{Collector, CollectorStream};
use async_trait::async_trait;
use futures::StreamExt;
use serde::{Deserialize, Serialize};

/// The table the ledger updates are persisted under. A non-EVM event type has no
/// Solidity signature to derive a name from, so it is supplied explicitly via
/// [`PersistExt::with_persistence_named`].
const TABLE: &str = "ledger_updates";

/// A minimal HyperCore-shaped ledger entry (`{ time, hash }`): a plain serde
/// type, deliberately *not* a `SolEvent`, so it can only be persisted through the
/// SolEvent-free `with_persistence_named` entry point.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct LedgerEvent {
    /// The event instant in milliseconds — the frontier's sort key.
    time: u64,
    /// The ledger entry's unique hash — the identity the frontier dedupes on.
    hash: String,
}

/// A scripted, in-process ledger feed keyed by a [`TimeFrontier`]. It serves a
/// list of `(time_ms, hash)` entries through the [`PersistableCollector`]
/// contract; there is no network and no external service behind it.
struct FakeFrontierCollector {
    /// The scripted entries, sorted non-decreasing by instant so the stream
    /// contract (`sort_key` order == stream order) holds.
    events: Vec<(u64, String)>,
}

impl FakeFrontierCollector {
    /// Build a feed from a `(time_ms, hash)` script, sorting by instant.
    fn new(script: Vec<(u64, &str)>) -> Self {
        let mut events: Vec<(u64, String)> = script
            .into_iter()
            .map(|(t, h)| (t, h.to_string()))
            .collect();
        events.sort_by_key(|(t, _)| *t);
        Self { events }
    }

    /// The `(position, event)` pair for one scripted entry: the position is a
    /// single-identity frontier `{ time, {hash} }`.
    fn item(time: u64, hash: &str) -> (TimeFrontier, LedgerEvent) {
        let position = TimeFrontier {
            time_ms: time,
            seen: std::iter::once(hash.to_string()).collect(),
        };
        (
            position,
            LedgerEvent {
                time,
                hash: hash.to_string(),
            },
        )
    }

    /// The current tip instant: the latest scripted instant (0 if empty).
    fn tip_ms(&self) -> u64 {
        self.events.iter().map(|(t, _)| *t).max().unwrap_or(0)
    }
}

#[async_trait]
impl PersistableCollector<LedgerEvent> for FakeFrontierCollector {
    type Pos = TimeFrontier;

    async fn subscribe_indexed(&self) -> Result<CollectorStream<'_, (TimeFrontier, LedgerEvent)>> {
        // Bounded (`with_to_block`) runs never poll the live tail; the scripted
        // entries are returned here anyway so the collector is complete on its own.
        let items: Vec<(TimeFrontier, LedgerEvent)> =
            self.events.iter().map(|(t, h)| Self::item(*t, h)).collect();
        Ok(Box::pin(futures::stream::iter(items)))
    }

    async fn query_range(
        &self,
        from: TimeFrontier,
        to: TimeFrontier,
    ) -> Result<CollectorStream<'_, (TimeFrontier, LedgerEvent)>> {
        // Inclusive `[from ..= to]` over the sort key (the instant). The restart
        // leg re-reads `[2000 ..= 2500]`, overlapping the stored boundary instant
        // 2000 by design — the writer dedupes the overlap.
        let (lo, hi) = (from.sort_key(), to.sort_key());
        let items: Vec<(TimeFrontier, LedgerEvent)> = self
            .events
            .iter()
            .filter(|(t, _)| *t >= lo && *t <= hi)
            .map(|(t, h)| Self::item(*t, h))
            .collect();
        Ok(Box::pin(futures::stream::iter(items)))
    }

    async fn tip(&self) -> Result<TimeFrontier> {
        Ok(TimeFrontier::from_sort_key(self.tip_ms()))
    }
}

/// A `(time, [hashes])` frontier — the same helper the position law tests use.
fn frontier(time_ms: u64, hashes: &[&str]) -> TimeFrontier {
    TimeFrontier {
        time_ms,
        seen: hashes.iter().map(|h| h.to_string()).collect(),
    }
}

/// The single-column payload schema the store persists each event under.
fn payload_schema() -> TableSchema {
    TableSchema::new(TABLE).col(PAYLOAD_COLUMN, SqlType::Text)
}

/// Decode the lossless JSON payload column of every stored row back into events.
fn decode_rows(rows: Vec<Row>) -> Result<Vec<LedgerEvent>> {
    let mut out = Vec::with_capacity(rows.len());
    for Row(cols) in rows {
        let Some(SqlValue::Text(payload)) = cols.into_iter().next() else {
            anyhow::bail!("unexpected replay row shape");
        };
        out.push(serde_json::from_str(&payload)?);
    }
    Ok(out)
}

/// Run one store-backed leg: wrap `feed` with the shared `store`, drain the
/// bounded subscription, and return every event delivered downstream.
async fn drain_leg(
    store: Arc<SqliteStore>,
    script: Vec<(u64, &str)>,
    to_block: u64,
) -> Result<Vec<LedgerEvent>> {
    let feed = FakeFrontierCollector::new(script);
    let persisted = feed
        .with_persistence_named(store, TABLE)
        .with_to_block(to_block);
    let mut stream = persisted.subscribe().await?;
    let mut delivered = Vec::new();
    while let Some(event) = stream.next().await {
        delivered.push(event);
    }
    Ok(delivered)
}

/// Everything the persist → restart → overlapping-backfill flow produces, so
/// each test can assert its own slice against one shared run.
struct Outcome {
    /// The frontier `stored_position` returned after run 1 (the resume point).
    resume_after_run1: TimeFrontier,
    /// Events delivered downstream on the restart leg (replay + deduped backfill).
    run2_downstream: Vec<LedgerEvent>,
    /// Every event row persisted in the store after the restart leg.
    stored_rows: Vec<LedgerEvent>,
    /// The frontier `stored_position` returned after run 2 (the advanced point).
    advanced_frontier: TimeFrontier,
}

/// Persist four events (two sharing instant 1000), rebuild the wrapper from the
/// stored frontier, then run an overlapping backfill that re-serves the boundary
/// instant. Shared by every test so the flow is exercised identically.
async fn persist_restart_overlap() -> Result<Outcome> {
    // In-memory SQLite behind an `Arc`, shared across both store-backed wrappers
    // (a single connection serves the in-memory database, so the handle IS the
    // durable store across the simulated restart).
    let store = Arc::new(SqliteStore::connect("sqlite::memory:").await?);

    // ---- Run 1: persist four events, two sharing instant 1000. ----
    let run1 = drain_leg(
        store.clone(),
        vec![
            (1000, "0xa1"),
            (1000, "0xa2"), // shares one millisecond with 0xa1
            (1500, "0xb1"),
            (2000, "0xc1"),
        ],
        2000,
    )
    .await?;
    assert_eq!(run1.len(), 4, "run 1 persists and delivers 4 events");

    // Fix the store to the frontier position for the direct read-backs.
    let store_pos: &dyn Store<TimeFrontier> = store.as_ref();
    let resume_after_run1 = store_pos
        .stored_position(TABLE)
        .await?
        .expect("run 1 stored a frontier");

    // ---- "Restart": a new wrapper over the SAME store, overlapping re-read. ----
    // Re-serves the already-stored (2000, 0xc1) plus a NEW same-instant sibling
    // (2000, 0xc2) and a later (2500, 0xd1).
    let run2_downstream = drain_leg(
        store.clone(),
        vec![
            (2000, "0xc1"), // re-observed — already covered by the stored frontier
            (2000, "0xc2"), // new same-instant sibling — must be retained
            (2500, "0xd1"),
        ],
        2500,
    )
    .await?;

    // Read the full stored table back and the advanced resume point.
    let rows = store_pos
        .replay(
            &payload_schema(),
            TimeFrontier::from_sort_key(i64::MAX as u64),
        )
        .await?;
    let stored_rows = decode_rows(rows)?;
    let advanced_frontier = store_pos
        .stored_position(TABLE)
        .await?
        .expect("run 2 advanced the frontier");

    Ok(Outcome {
        resume_after_run1,
        run2_downstream,
        stored_rows,
        advanced_frontier,
    })
}

/// The headline end-to-end: persist, restart from the stored frontier, run the
/// overlapping backfill, and assert every guarantee at once — resume round-trip,
/// same-millisecond retention, and exactly-once dedupe across two store-backed
/// wrappers.
#[tokio::test]
async fn frontier_persist_restart_overlap_dedupe_across_two_stores() {
    let outcome = persist_restart_overlap().await.unwrap();

    // The resume point round-trips through encode/decode across the restart: a
    // strictly later instant dropped the stale 1000/1500 identities.
    assert_eq!(
        outcome.resume_after_run1,
        frontier(2000, &["0xc1"]),
        "the stored resume frontier is (2000, {{0xc1}})"
    );

    // Both same-millisecond events are retained (replayed on the restart leg) —
    // a bare scalar watermark could keep at most one.
    let same_millisecond: BTreeSet<String> = outcome
        .run2_downstream
        .iter()
        .filter(|e| e.time == 1000)
        .map(|e| e.hash.clone())
        .collect();
    assert!(
        same_millisecond.contains("0xa1") && same_millisecond.contains("0xa2"),
        "both same-millisecond events retained, got {same_millisecond:?}"
    );

    // At-least-once in, exactly-once effect: the re-observed 0xc1 is delivered
    // once downstream and stored once, while the NEW same-instant 0xc2 survives.
    let downstream_c1 = outcome
        .run2_downstream
        .iter()
        .filter(|e| e.hash == "0xc1")
        .count();
    assert_eq!(downstream_c1, 1, "re-observed 0xc1 delivered exactly once");
    let stored_c1 = outcome
        .stored_rows
        .iter()
        .filter(|e| e.hash == "0xc1")
        .count();
    assert_eq!(stored_c1, 1, "0xc1 stored exactly once despite the overlap");
    assert!(
        outcome.stored_rows.iter().any(|e| e.hash == "0xc2"),
        "the new same-instant 0xc2 is retained"
    );

    // Store holds exactly the 6 distinct events; the advanced frontier folds to
    // the tip instant (2500, {0xd1}); replay(4) + deduped backfill(0xc2,0xd1)=6.
    assert_eq!(outcome.stored_rows.len(), 6, "store holds exactly 6 rows");
    assert_eq!(
        outcome.run2_downstream.len(),
        6,
        "6 events downstream on restart"
    );
    assert_eq!(outcome.advanced_frontier, frontier(2500, &["0xd1"]));
}

/// The resume point persisted in run 1 decodes byte-for-byte on the restart, and
/// the overlapping backfill leaves no gap in the axis.
#[tokio::test]
async fn frontier_resume_point_round_trips_with_no_gap() {
    let outcome = persist_restart_overlap().await.unwrap();

    // Round-trip: the frontier stored in run 1 decodes to the same value and
    // re-encodes to the same JSON — a rich (time, seen-set) position survived a
    // restart through its persisted column value.
    assert_eq!(outcome.resume_after_run1, frontier(2000, &["0xc1"]));
    assert_eq!(
        outcome.resume_after_run1.encode(),
        frontier(2000, &["0xc1"]).encode()
    );

    // No gap: every distinct identity across both runs is stored, none skipped.
    // Discriminating on 0xc2 — a NEW identity AT the resume boundary instant
    // 2000: it is present only because the backfill re-read the boundary
    // (resume_key = the boundary itself, not last+1). A "+1" resume would have
    // skipped instant 2000 entirely, losing 0xc2 and leaving a gap.
    let stored: BTreeSet<String> = outcome.stored_rows.iter().map(|e| e.hash.clone()).collect();
    let expected: BTreeSet<String> = ["0xa1", "0xa2", "0xb1", "0xc1", "0xc2", "0xd1"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    assert_eq!(stored, expected, "gap-free coverage of the whole axis");
    assert!(
        stored.contains("0xc2"),
        "boundary-instant re-read retained the new 0xc2 -> no gap"
    );

    // The advanced watermark sits at the tip instant: nothing beyond 2500 lost.
    assert_eq!(outcome.advanced_frontier, frontier(2500, &["0xd1"]));
}

/// The core dedupe promise in isolation: after an overlapping re-read the store
/// holds exactly one row for the re-observed identity.
#[tokio::test]
async fn re_observed_frontier_event_has_exactly_one_row() {
    let outcome = persist_restart_overlap().await.unwrap();

    let stored_c1 = outcome
        .stored_rows
        .iter()
        .filter(|e| e.hash == "0xc1")
        .count();
    assert_eq!(
        stored_c1, 1,
        "the re-observed 0xc1 has exactly one stored row despite the overlap"
    );
    // A second stored 0xc1 row (dedupe broken) would push the total past 6.
    assert_eq!(
        outcome.stored_rows.len(),
        6,
        "exactly the 6 distinct events are stored — nothing dropped or duplicated"
    );
}

/// A corrupt / wrong-typed stored position must fail the subscribe loudly
/// (`MalformedStoredPosition`), never silently re-sync from genesis. Seeds a
/// decimal position cell (as an old BlockPosition archive would hold) and reads
/// it back under a `TimeFrontier` subscription — the JSON decode fails verbatim.
#[tokio::test]
async fn corrupt_stored_position_surfaces_malformed_error() {
    let store = Arc::new(SqliteStore::connect("sqlite::memory:").await.unwrap());

    // A BlockPosition write stores the decimal "2000" in the progress table's
    // position column for TABLE — a wrong-typed cell for a TimeFrontier reader.
    let block_store: &dyn Store<BlockPosition> = store.as_ref();
    block_store
        .write(
            &payload_schema(),
            BlockPosition(2000),
            vec![Row(vec![SqlValue::Text(
                "{\"time\":2000,\"hash\":\"0xc1\"}".into(),
            )])],
        )
        .await
        .unwrap();

    // A TimeFrontier subscription over the same table must error on decode.
    let feed = FakeFrontierCollector::new(vec![(2500, "0xd1")]);
    let persisted = feed
        .with_persistence_named(store.clone(), TABLE)
        .with_to_block(2500);
    let err = match persisted.subscribe().await {
        Ok(_) => panic!("a corrupt stored position must fail the subscribe"),
        Err(e) => e,
    };
    let rendered = format!("{err:#}");
    assert!(
        rendered.contains("decoding TimeFrontier"),
        "surfaces the decode failure verbatim, got: {rendered}"
    );
}
