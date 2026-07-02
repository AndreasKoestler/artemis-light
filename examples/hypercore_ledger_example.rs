//! A self-contained, verified example of the generic [`Position`] persistence
//! layer keyed by a `(time_ms, seen-hashes)` frontier instead of a block number.
//!
//! It models a HyperCore-shaped `userNonFundingLedgerUpdates` feed — entries of
//! the form `{ time, hash, delta }` — and drives the full durable-persistence
//! flow end to end on an **in-memory SQLite** store with an **in-process scripted
//! feed**: no network, no external database, no live L1 connection
//! [position-trait.EXAMPLE.4].
//!
//! The scripted feed deliberately exercises the two things a bare block number
//! cannot express [position-trait.EXAMPLE.2]:
//!
//! - **Same-millisecond events** — `(1000, "0xa1")` and `(1000, "0xa2")` share
//!   one instant, so the frontier's per-instant seen-set (not a scalar) is what
//!   keeps both.
//! - **An overlapping re-read on restart** — a second wrapper over the *same*
//!   store re-serves `(2000, "0xc1")` (already stored) alongside a genuinely new
//!   same-instant sibling `(2000, "0xc2")` and a later `(2500, "0xd1")`, so the
//!   writer's dedupe path turns at-least-once delivery into exactly-once stored
//!   effect [position-trait.EXAMPLE.3].
//!
//! A verified run producing the four fixed stdout lines and exiting `0` is the
//! acceptance bar; compiling is explicitly not sufficient
//! [position-trait.EXAMPLE.6]. Any failed assertion panics with a non-zero exit.
//!
//! Note the frontier does **not** solve completeness or finality: a late event
//! arriving below the frontier boundary instant is deliberately skipped, and
//! reconciliation remains the consumer's responsibility.
//!
//! Run with:
//! ```sh
//! cargo run --example hypercore_ledger_example
//! ```

use std::collections::BTreeSet;
use std::sync::Arc;

use anyhow::Result;
use artemis_light::persistence::{
    PAYLOAD_COLUMN, PersistExt, PersistableCollector, Position, Row, SqlType, SqlValue,
    SqliteStore, Store, TableSchema, TimeFrontier,
};
use artemis_light::types::{Collector, CollectorStream};
use async_trait::async_trait;
use futures::StreamExt;
use serde::{Deserialize, Serialize};

/// The table the ledger updates are persisted under. A non-EVM event type has no
/// Solidity signature to derive a name from, so it is supplied explicitly via
/// [`PersistExt::with_persistence_named`].
const TABLE: &str = "ledger_updates";

/// One entry of the simulated feed, mirroring HyperCore's
/// `userNonFundingLedgerUpdates` shape: `{ time, hash, delta }`
/// [position-trait.EXAMPLE.1]. A plain serde type — deliberately *not* a
/// `SolEvent` — so it can only be persisted through the SolEvent-free
/// `with_persistence_named` entry point.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct LedgerUpdate {
    /// The event instant in milliseconds — the frontier's sort key.
    time: u64,
    /// The ledger entry's unique hash — the identity the frontier dedupes on.
    hash: String,
    /// The spot-transfer payload.
    delta: SpotTransferDelta,
}

/// A minimal spot-transfer delta for a [`LedgerUpdate`]; the shape mirrors a
/// HyperCore spot transfer (`{ token, amount, user, destination }`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct SpotTransferDelta {
    token: String,
    amount: String,
    user: String,
    destination: String,
}

/// A scripted, in-process ledger feed keyed by a [`TimeFrontier`]
/// [position-trait.EXAMPLE.2]. It holds a list of `(time_ms, hash)` entries and
/// serves them through the [`PersistableCollector`] contract; there is no
/// network and no external service behind it.
struct SimulatedLedgerFeed {
    /// The scripted entries, sorted non-decreasing by instant so the stream
    /// contract (`sort_key` order == stream order) holds.
    events: Vec<(u64, String)>,
}

impl SimulatedLedgerFeed {
    /// Build a feed from a `(time_ms, hash)` script, sorting by instant so the
    /// emitted positions are non-decreasing.
    fn new(script: Vec<(u64, &str)>) -> Self {
        let mut events: Vec<(u64, String)> = script
            .into_iter()
            .map(|(t, h)| (t, h.to_string()))
            .collect();
        events.sort_by_key(|(t, _)| *t);
        Self { events }
    }

    /// The current tip instant: the latest scripted instant (0 if empty).
    fn tip_ms(&self) -> u64 {
        self.events.iter().map(|(t, _)| *t).max().unwrap_or(0)
    }

    /// The `(position, event)` pair for one scripted entry: the position is a
    /// single-identity frontier `{ time, {hash} }`, the event the full
    /// [`LedgerUpdate`].
    fn item(time: u64, hash: &str) -> (TimeFrontier, LedgerUpdate) {
        let position = TimeFrontier {
            time_ms: time,
            seen: std::iter::once(hash.to_string()).collect(),
        };
        let update = LedgerUpdate {
            time,
            hash: hash.to_string(),
            delta: SpotTransferDelta {
                token: "USDC".to_string(),
                amount: "100".to_string(),
                user: "0xuser".to_string(),
                destination: "0xdest".to_string(),
            },
        };
        (position, update)
    }
}

#[async_trait]
impl PersistableCollector<LedgerUpdate> for SimulatedLedgerFeed {
    type Pos = TimeFrontier;

    async fn subscribe_indexed(&self) -> Result<CollectorStream<'_, (TimeFrontier, LedgerUpdate)>> {
        // The example runs both legs in bounded mode (`with_to_block`), which
        // never subscribes to a live tail — the whole feed is drained through
        // `query_range`. The scripted entries are returned here too so the
        // collector is a complete implementation on its own.
        let items: Vec<(TimeFrontier, LedgerUpdate)> = self
            .events
            .iter()
            .map(|(time, hash)| Self::item(*time, hash))
            .collect();
        Ok(Box::pin(futures::stream::iter(items)))
    }

    async fn query_range(
        &self,
        from: TimeFrontier,
        to: TimeFrontier,
    ) -> Result<CollectorStream<'_, (TimeFrontier, LedgerUpdate)>> {
        // Inclusive `[from ..= to]` over the sort key (the instant). The restart
        // leg re-reads `[2000 ..= 2500]`, which overlaps the stored boundary
        // instant 2000 by design — the writer dedupes the overlap.
        let (lo, hi) = (from.sort_key(), to.sort_key());
        let items: Vec<(TimeFrontier, LedgerUpdate)> = self
            .events
            .iter()
            .filter(|(time, _)| *time >= lo && *time <= hi)
            .map(|(time, hash)| Self::item(*time, hash))
            .collect();
        Ok(Box::pin(futures::stream::iter(items)))
    }

    async fn tip(&self) -> Result<TimeFrontier> {
        Ok(TimeFrontier::from_sort_key(self.tip_ms()))
    }
}

/// Render a seen-set as `a, b, c` (sorted; `BTreeSet` iterates in order).
fn render_seen(seen: &BTreeSet<String>) -> String {
    seen.iter().cloned().collect::<Vec<_>>().join(", ")
}

#[tokio::main]
async fn main() -> Result<()> {
    // An in-memory SQLite store behind an `Arc`, kept alive across the simulated
    // restart so the second wrapper opens the same database
    // [position-trait.EXAMPLE.4].
    let store = Arc::new(SqliteStore::connect("sqlite::memory:").await?);
    // A `&dyn Store<TimeFrontier>` for the direct read-backs below (the store is
    // generic over the position type; this fixes it to the frontier).
    let store_pos: &dyn Store<TimeFrontier> = store.as_ref();

    // ---- Run 1: persist four events, two of them sharing instant 1000. ----
    let run1_feed = SimulatedLedgerFeed::new(vec![
        (1000, "0xa1"),
        (1000, "0xa2"), // shares one millisecond with 0xa1
        (1500, "0xb1"),
        (2000, "0xc1"),
    ]);
    let persisted = run1_feed
        .with_persistence_named(store.clone(), TABLE)
        .with_to_block(2000);
    let mut stream = persisted.subscribe().await?;
    let mut run1_count = 0usize;
    while stream.next().await.is_some() {
        run1_count += 1;
    }
    // Drop the subscription and the wrapper, as a process shutdown would; the
    // shared store stays alive.
    drop(stream);
    drop(persisted);
    assert_eq!(run1_count, 4, "run 1 persists 4 events");

    // The stored resume frontier round-trips through the store's encode/decode:
    // it is (2000, {0xc1}) — a strictly later instant dropped the stale
    // 1000/1500 identities [position-trait.EXAMPLE.3-1].
    let resume = store_pos
        .stored_position(TABLE)
        .await?
        .expect("run 1 stored a frontier");
    assert_eq!(
        resume,
        TimeFrontier {
            time_ms: 2000,
            seen: std::iter::once("0xc1".to_string()).collect(),
        },
        "the stored resume frontier is (2000, {{0xc1}})"
    );
    println!(
        "resume frontier round-trips: time_ms={}, seen={{{}}}",
        resume.time_ms,
        render_seen(&resume.seen)
    );

    // ---- "Restart": a new wrapper over the SAME store, overlapping re-read. ----
    // The feed re-serves the already-stored (2000, 0xc1) plus a new same-instant
    // sibling (2000, 0xc2) and a later (2500, 0xd1).
    let run2_feed = SimulatedLedgerFeed::new(vec![
        (2000, "0xc1"), // re-observed — already covered by the stored frontier
        (2000, "0xc2"), // new same-instant sibling
        (2500, "0xd1"),
    ]);
    let recovered = run2_feed
        .with_persistence_named(store.clone(), TABLE)
        .with_to_block(2500);
    let mut stream = recovered.subscribe().await?;
    let mut recovered_events: Vec<LedgerUpdate> = Vec::new();
    while let Some(event) = stream.next().await {
        recovered_events.push(event);
    }
    drop(stream);
    drop(recovered);

    // Replay yields the 4 stored events (both same-millisecond ones retained);
    // the overlapping backfill re-reads [2000 ..= 2500] and the writer skips the
    // re-observed 0xc1 (contained in the stored frontier) while delivering the
    // new 0xc2 and 0xd1 — 6 events downstream, 0xc1 exactly once.
    assert_eq!(
        recovered_events.len(),
        6,
        "replay (4) + deduped backfill (0xc2, 0xd1) = 6 events downstream"
    );
    let same_millisecond: BTreeSet<String> = recovered_events
        .iter()
        .filter(|e| e.time == 1000)
        .map(|e| e.hash.clone())
        .collect();
    assert!(
        same_millisecond.contains("0xa1") && same_millisecond.contains("0xa2"),
        "both same-millisecond events are retained on replay [position-trait.EXAMPLE.3-1]"
    );
    println!(
        "same-millisecond events retained: {}",
        render_seen(&same_millisecond)
    );
    let downstream_c1 = recovered_events.iter().filter(|e| e.hash == "0xc1").count();
    assert_eq!(
        downstream_c1, 1,
        "the re-observed 0xc1 is delivered exactly once downstream (suppressed on backfill)"
    );

    // Store-level proof: the ledger_updates table holds exactly 6 rows with 0xc1
    // stored once, and the advanced frontier is (2500, {0xd1})
    // [position-trait.EXAMPLE.3-2].
    let payload_schema = TableSchema::new(TABLE).col(PAYLOAD_COLUMN, SqlType::Text);
    let rows = store_pos
        .replay(
            &payload_schema,
            TimeFrontier::from_sort_key(i64::MAX as u64),
        )
        .await?;
    assert_eq!(rows.len(), 6, "the store holds exactly 6 event rows");
    let mut stored_c1 = 0usize;
    for Row(cols) in &rows {
        let Some(SqlValue::Text(payload)) = cols.first() else {
            anyhow::bail!("unexpected replay row shape: {cols:?}");
        };
        let update: LedgerUpdate = serde_json::from_str(payload)?;
        if update.hash == "0xc1" {
            stored_c1 += 1;
        }
    }
    assert_eq!(
        stored_c1, 1,
        "0xc1 is stored exactly once despite the overlapping re-read"
    );
    println!("re-observed 0xc1 deduped: stored exactly once");

    let advanced = store_pos
        .stored_position(TABLE)
        .await?
        .expect("run 2 advanced the frontier");
    assert_eq!(
        advanced,
        TimeFrontier {
            time_ms: 2500,
            seen: std::iter::once("0xd1".to_string()).collect(),
        },
        "the advanced frontier is (2500, {{0xd1}}) [position-trait.EXAMPLE.3-2]"
    );

    println!("hypercore_ledger_example: OK");
    Ok(())
}
