# artemis-light

[![crates.io](https://img.shields.io/crates/v/artemis-light.svg)](https://crates.io/crates/artemis-light)
[![docs.rs](https://docs.rs/artemis-light/badge.svg)](https://docs.rs/artemis-light)
[![CI](https://github.com/AndreasKoestler/artemis-light/actions/workflows/test.yml/badge.svg)](https://github.com/AndreasKoestler/artemis-light/actions/workflows/test.yml)

A Rust framework for reliable, long-running **on-chain automation**:
event-driven agents that watch a chain, decide, and act. Built on
[Alloy](https://github.com/alloy-rs/alloy) and Tokio, it began as a
stripped-down, modernised fork of Paradigm's
[Artemis](https://github.com/paradigmxyz/artemis) MEV framework.

Use it for liquidation bots and keepers, indexers and event archivers,
monitoring and alerting agents, trading automation — and MEV searchers.

**[API documentation](https://andreaskoestler.github.io/artemis-light/)** (also on [docs.rs](https://docs.rs/artemis-light))

## Purpose and scope

artemis-light provides what an unattended on-chain agent needs to stay
correct and alive over a long horizon:

- **The pipeline** — Collectors → Strategies → Executors, orchestrated by an
  Engine over broadcast channels.
- **Composition** — combinators for every stage (`map`, `filter_map`, `merge`,
  `chain`, risk gates, cooldowns) so cross-cutting policy is visible at
  composition time.
- **Operational safety** — per-collector reconnect with fatal escalation,
  polling fallback for pubsub-less transports, and executor wrappers for
  retry, fallback, rate limiting, circuit breaking, and a kill switch /
  dry-run mode.
- **Durable persistence** — events recorded to SQL, replayed on restart, and
  backfilled across the gap, so a restarted agent resumes instead of
  re-syncing from genesis.

Out of scope: strategy logic itself, protocol-specific integrations, and
MEV-specific infrastructure (bundles, private relays, latency optimisation) —
the parts the fork deliberately dropped.

## Architecture

Artemis-light is an **event-processing pipeline** composed of three pluggable stages wired together by an engine:

```
Collectors ──events──▶ Strategies ──actions──▶ Executors
                          ▲                        │
                          │     Engine (broadcast)  │
                          └────────────────────────-┘
```

The **Engine** fans-out every event to every strategy via a `tokio::sync::broadcast` channel, and fans-out every action to every executor via a second broadcast channel. All stages run as independent Tokio tasks and shut down cooperatively through a `CancellationToken`.

## Components

| Layer | Type | Description |
|---|---|---|
| **Collector** | `BlockCollector` | Subscribes to new blocks via WebSocket (falls back to polling) |
| | `MempoolCollector` | Subscribes to pending transactions in the mempool |
| | `LogCollector` | Subscribes to on-chain event logs matching a filter |
| | `EventCollector` | Subscribes to an arbitrary `alloy` subscription |
| **Strategy** | `Strategy<E, A>` | User-defined: receives events, produces action streams |
| **Executor** | `MempoolExecutor` | Submits EIP-1559-priced transactions to the public mempool; optionally watches for confirmation and replaces a stuck transaction at an escalated fee |
| **Observer** | `Observer<E, A>` | Passive consumer of every event and action crossing the channels |
| **Persistence** | `Persisted<C, S>` | Wraps a position-aware collector to record events to a SQL `Store` and replay them on restart |

`MempoolExecutor` prices transactions with EIP-1559 fields from the provider's
fee estimate (with a configurable `with_priority_fee_bump`). By default it is
fire-and-forget; `with_replacement(policy)` makes it watch for confirmation and
resubmit a stuck transaction at the same nonce with escalated fees. Use
replacement *or* the `retry` wrapper, not both — `retry` resubmits on a send
error, replacement resubmits a sent-but-unmined transaction.

## Combinators

Extension traits let you compose collectors and executors without boilerplate:

```rust
use artemis_light::collector_ext::CollectorExt;

// Transform events
let collector = block_collector.map(|block| MyEvent::Block(block));

// Filter + transform events
let collector = mempool_collector.filter_map(|tx| {
    if tx.value() > threshold { Some(tx) } else { None }
});

// Merge two collectors into one stream
let collector = block_collector.merge(mempool_collector);

// Prefer a primary source, fall back to a backup if its subscribe fails
// (primary-preferring: each reconnect retries the primary first)
let collector = primary_ws_collector.fallback(backup_ws_collector);
```

Executors compose the same way. Actions that implement `Expires` carry the
freshness window their strategy priced them against; the `deadline` wrapper
drops expired actions with `Ok`, so expiry neither trips the circuit breaker
nor keeps a retry loop alive:

```rust
// Reliability-wrap an executor: innermost deadline drops stale actions
// (each retry attempt re-checks expiry; every wait above it has elapsed
// by the time the check runs)
let executor = mempool_executor
    .deadline()
    .retry(RetryPolicy::default())
    .rate_limit(5)
    .circuit_breaker(3)
    .gated(kill_switch);
```

## Observers

An **Observer** is one more subscriber on the engine's event and action
channels: it sees everything strategies and executors see while producing and
perturbing nothing. Observation is best-effort (a lagging observer skips
messages like any consumer) and infallible by design — there is no error
channel through which observing could fail the pipeline. Use it for metrics,
logging, or shadow analysis:

```rust
use artemis_light::types::Observer;

struct Telemetry;

#[async_trait::async_trait]
impl Observer<MyEvent, MyAction> for Telemetry {
    async fn observe_event(&mut self, event: MyEvent) { /* count it */ }
    async fn observe_action(&mut self, action: MyAction) { /* count it */ }
}

engine.add_observer(Box::new(Telemetry));
```

## Execution feedback

Submission is otherwise fire-and-forget: a strategy never learns whether the
action it produced was submitted or failed. `ExecutorExt::report` publishes an
`ExecutionOutcome` — the action plus an `Ok`/`Err` verdict — to a broadcast
channel after each submission, returning the inner executor's result unchanged
(it is transparent, so it composes anywhere in the reliability stack; place it
outermost for the final post-retry verdict). A `ChannelCollector` over the same
channel feeds those verdicts back as events through the normal
collector → strategy path — no back-channel in the engine:

```rust
use artemis_light::{collectors::ChannelCollector, collector_ext::CollectorExt,
    executor_ext::{ExecutionOutcome, ExecutorExt}};
use tokio::sync::broadcast;

let (outcomes, _) = broadcast::channel(256);

engine.add_executor(Box::new(
    mempool_executor.retry(policy).report(outcomes.clone()),  // outermost
));
engine.add_collector(Box::new(
    ChannelCollector::new(outcomes).map(Event::Outcome),      // back in as an event
));
```

The verdict is the executor stack's `Ok`/`Err`, not on-chain confirmation: a
layer that drops with `Ok` (`gated`, `deadline`) reports `Ok`. Knowing whether a
transaction actually mined or reverted is a separate, larger facility.

## Persistence

A long-running strategy that restarts shouldn't have to re-sync from genesis.
The `persistence` module records every event a collector sees into a SQL
[`Store`](src/persistence/store.rs) (SQLite first), and on restart replays the
stored history before catching up to — and following — the chain tip.

Wrapping is a single call on any block-aware collector (one that implements
`PersistableCollector`, e.g. `EventCollector`):

```rust
use artemis_light::{collectors::EventCollector, persistence::{PersistExt, SqliteStore}};
use std::sync::Arc;

// `sqlite::memory:` for ephemeral, or `sqlite:events.db` to survive restarts.
let store = Arc::new(SqliteStore::connect("sqlite:events.db").await?);

let collector = EventCollector::new(contract.MyEvent_filter());
let persisted = collector.with_persistence(store);

engine.add_collector(Box::new(persisted));
```

On `subscribe`, a `Persisted` collector chains three segments into one stream:

1. **Replay** — stored events, reconstructed from the database (first subscribe
   only; a reconnect does not re-replay the archive).
2. **Backfill** — the RPC gap between the last stored block and the chain tip.
3. **Live** — the tip onward, recording each completed block as it goes.

Events must be `serde::Serialize + Deserialize`. The table name and columns are
derived from the event's Solidity signature and field names; register a
`TableSchema` override on the store to rename or retype columns. A full lossless
JSON payload is stored alongside the derived columns so replay reconstructs the
exact event. Writes are one transaction per complete block, and the stored block
height only advances over a gap-free prefix.

The backfill is sliced into bounded `eth_getLogs` windows (default 10,000
blocks, `.with_backfill_chunk_size(..)`) so no single call exceeds provider
range caps, and `.with_start_block(..)` sets where the very first sync begins
instead of genesis.

By default a block is persisted once the next block arrives. Set
`.with_confirmation_depth(n)` to persist a block only once it is `n` blocks
deep: events are still delivered to strategies live and immediately, but the
write to the store lags `n` blocks, so a reorg shallower than `n` is corrected
in the buffer before any orphaned row is written. A reorg deeper than `n` halts
persistence and a restart re-syncs, so choose `n` above the deepest reorg you
expect. [`examples/confirmation_depth_example.rs`](examples/confirmation_depth_example.rs)
shows the resulting write lag (`cargo run --example confirmation_depth_example`).

See [`examples/persistence_example.rs`](examples/persistence_example.rs) for a
runnable demo (record live events, then recover them on a simulated restart):

```sh
cargo run --example persistence_example
```

### Beyond block numbers: the `Position` trait

The persistence layer is generic over a `Position` — the ordering key a source
resumes and dedupes on — not just a block number. The built-in
[`BlockPosition`](src/persistence/position.rs) is the default, so every EVM call
site above stays a one-liner. To persist a **non-block** source (a queue offset,
a `(time, seen-set)` frontier) implement `Position` for your key and you inherit
the same resume / backfill / gap-free machinery.

The shipped reference [`TimeFrontier`](src/persistence/position.rs) is a worked
`(time_ms, hash-set)` frontier: several events can share one millisecond, so a
bare scalar cannot express "everything up to instant *t*, but only these
identities *at* *t*". Its re-observation policy is *dedupe* (not halt), so an
overlapping backfill re-reads the boundary instant and the writer stores each
re-observed identity exactly once. See
[`examples/hypercore_ledger_example.rs`](examples/hypercore_ledger_example.rs)
for a self-contained, verified run of a HyperCore-shaped ledger feed:

```sh
cargo run --example hypercore_ledger_example
```

> **Scope — this does not solve completeness or finality.** A late event
> arriving *below* a frontier's boundary instant is deliberately **skipped**:
> the frontier resumes and dedupes, it does not backfill history it has already
> advanced past. Completeness, finality, and reconciliation of late or reorged
> data remain the **consumer's responsibility**.

## Migrating to position-generic persistence

`0.2.0` generalises durable persistence from a hardwired `u64` block number to
the generic `Position` trait. **The breaking surface is confined to the `Store`
and `PersistableCollector` traits** (plus the internal `Dialect` seam). If you
only use the built-ins — `EventCollector`, `SqliteStore`, `PostgresStore`,
`with_persistence` / `persisted!` over a `SolEvent` — **your code is
source-compatible and needs no changes**: `Store` and `PersistableCollector`
default their position type to `BlockPosition`, so `dyn Store` and `S: Store`
still mean `Store<BlockPosition>`, and the concrete store aliases are unchanged.

You only migrate if you wrote a **custom `Store`, `PersistableCollector`, or
`Dialect` impl**.

### `Store`: method renames and signatures

| 0.1 (block-hardwired) | 0.2 (position-generic) |
|---|---|
| `write_block(&self, schema, block: u64, rows)` | `write(&self, schema, position: P, rows)` |
| `last_block(&self, table) -> Option<u64>` | `stored_position(&self, table) -> Option<P>` |
| `replay(&self, schema, to: u64)` | `replay(&self, schema, up_to: P)` |

The monotonic-advance rule moved **out of SQL and into `Position::advance`**,
applied inside the single write transaction. Where 0.1 leaned on a SQL
`GREATEST(...)` upsert (via `Dialect::monotonic_watermark_set`), 0.2 reads the
previous position under the row lock, folds it with `P::advance(prev, next)`, and
writes the result — so the watermark rule is expressed once, for any position
type, and stays atomic with the row write.

### `PersistableCollector`: an associated position type

```diff
 #[async_trait]
 impl PersistableCollector<MyEvent> for MyCollector {
-    async fn subscribe_indexed(&self) -> Result<CollectorStream<'_, (u64, MyEvent)>> { .. }
-    async fn query_range(&self, from: u64, to: u64) -> Result<CollectorStream<'_, (u64, MyEvent)>> { .. }
-    async fn tip(&self) -> Result<u64> { .. }
+    type Pos = BlockPosition;  // or your custom Position
+    async fn subscribe_indexed(&self) -> Result<CollectorStream<'_, (Self::Pos, MyEvent)>> { .. }
+    async fn query_range(&self, from: Self::Pos, to: Self::Pos) -> Result<CollectorStream<'_, (Self::Pos, MyEvent)>> { .. }
+    async fn tip(&self) -> Result<Self::Pos> { .. }
 }
```

`tip()` now returns the current frontier / finality boundary as a `Pos`, and the
`(Pos, E)` pairs replace the old `(u64, E)`. For a block source, set
`type Pos = BlockPosition` and wrap/unwrap `u64` at the boundary
(`BlockPosition(n)` / `position.sort_key()`).

### Worked port of a custom `Store`

```rust
use anyhow::Result;
use async_trait::async_trait;
use artemis_light::persistence::{BlockPosition, Position, Row, Store, TableSchema};

#[async_trait]
impl Store for MyStore {           // `Store` == `Store<BlockPosition>` by default
    async fn write(
        &self,
        schema: &TableSchema,
        position: BlockPosition,    // was `block: u64`
        rows: Vec<Row>,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        // ... INSERT `rows` into `schema.table` ...

        // Advance the watermark IN RUST, inside this same transaction — no SQL
        // GREATEST. Read the previous position under the row lock, fold it, and
        // persist both the sort key and the encoded value.
        let prev = self.read_position(&mut tx, &schema.table).await?; // Option<BlockPosition>
        let next = BlockPosition::advance(prev, position);
        self.upsert_progress(&mut tx, &schema.table, next.sort_key(), &next.encode()).await?;

        tx.commit().await
    }

    async fn stored_position(&self, table: &str) -> Result<Option<BlockPosition>> {
        // was `last_block`; decode the stored TEXT value, or None when empty.
        let encoded: Option<String> = self.read_encoded_position(table).await?;
        encoded.map(|e| BlockPosition::decode(&e)).transpose()
    }

    async fn replay(&self, schema: &TableSchema, up_to: BlockPosition) -> Result<Vec<Row>> {
        let to = up_to.sort_key();  // was the `to: u64` argument
        // ... SELECT rows with sort_key <= `to`, ascending ...
        # Ok(vec![])
    }
}
```

Swapping `BlockPosition` for a custom `Position` (e.g. `TimeFrontier`) is the
only change needed to persist a non-block source — the body is identical because
`advance` / `encode` / `decode` / `sort_key` are all defined by the trait.

### `Dialect`: `monotonic_watermark_set` removed

Custom `Dialect` impls must **drop `monotonic_watermark_set`** — the method no
longer exists (monotonicity is now `Position::advance` in Rust, above). The
`Dialect` seam gained `progress_row_lock` and `is_undefined_column`, both with
default implementations, so no further action is required.

## Serving layer (opt-in)

Behind the **non-default `serving` cargo feature**, a `ServingLayer` exposes the
persisted tables over a read-only HTTP/JSON API — list tables, inspect a table's
schema, query its rows (paged, block-range filtered), and read per-table indexing
progress. It is purely additive: with the feature off, nothing is compiled and no
existing pipeline behavior changes.

```toml
artemis-light = { version = "0.2", features = ["serving"] }
```

```rust,ignore
use artemis_light::ServingLayer;
use tokio_util::sync::CancellationToken;

// Same database URL the SqliteStore writer uses; serves on 127.0.0.1:8080.
let shutdown = CancellationToken::new();
ServingLayer::new("sqlite:events.db", "127.0.0.1:8080".parse()?)
    .serve(shutdown) // returns when the token is cancelled
    .await?;
```

Endpoints: `GET /health`, `GET /tables`, `GET /tables/{table}/schema`,
`GET /tables/{table}/rows?from_block&to_block&limit&offset`, `GET /status`.

For a runnable end-to-end demo — index events, serve them, and walk every
endpoint with a minimal client — see
[`examples/serving_example.rs`](examples/serving_example.rs):

```sh
cargo run --example serving_example --features serving
```

The serving layer opens its **own read-only connection pool** to the same SQLite
file the writer uses (it never reuses the writer's single-connection pool); under
WAL, reads run concurrently with the live writer and observe only committed
blocks.

**Deployment notes.**
- The API has **no authentication, TLS, or rate limiting** — front it with a
  reverse proxy (or bind to localhost) if exposed beyond a trusted network.
- Row queries are not backed by a `block_number` index, so `GET …/rows` is a full
  scan + sort per request and large `offset` paging degrades linearly; size for
  operator/dashboard traffic over modest tables.
- File-backed databases only — `:memory:` is not servable (a separate pool would
  see an empty database).

## Quickstart

Add the dependency to your `Cargo.toml`:

```toml
[dependencies]
artemis-light = "0.2"
```

### Minimal example

```rust
use artemis_light::{
    collectors::BlockCollector,
    engine::Engine,
    types::Collector,
};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let provider = /* your Alloy provider */;
    let provider = Arc::new(provider);

    let mut engine = Engine::default();
    engine.add_collector(Box::new(BlockCollector::new(provider.clone())));
    engine.add_strategy(Box::new(my_strategy));
    engine.add_executor(Box::new(my_executor));

    let mut handle = engine.run().await?;

    // Run until Ctrl-C, or until a collector becomes unrecoverable. Bind the
    // outcome to the branch that actually won the `select!` — don't re-check
    // `handle.fatal.is_cancelled()` afterwards, or a Ctrl-C that races a fatal
    // cancellation gets mislabeled as a collector failure.
    let fatal = tokio::select! {
        _ = tokio::signal::ctrl_c() => false,
        _ = handle.fatal.cancelled() => {
            tracing::error!("collector unrecoverable; restarting");
            true
        }
    };
    handle.token.cancel();
    while handle.tasks.join_next().await.is_some() {}

    // The library never calls `process::exit`; the binary decides. Exiting
    // non-zero lets an orchestrator restart the process with a fresh sync.
    if fatal {
        std::process::exit(1);
    }
    Ok(())
}
```

On a persistent WebSocket disconnect (or a stream that can never be
established), each collector retries with exponential backoff up to a
configurable threshold (`Engine::with_reconnect_config`). Once exhausted, the
engine cancels every task and fires `handle.fatal` — an observe-only token that
lets the binary tell a fatal shutdown apart from a Ctrl-C one and restart,
rather than the library killing the process.

## Examples

Runnable, narrated demos of every facility — the core pipeline, collector
combinators, observers, the reconnect/fatal lifecycle, persistence, and an
end-to-end on-chain run against a local Anvil chain — live in
[`examples/`](examples/). Start with:

```sh
cargo run --example basic_example
```

and see [`examples/README.md`](examples/README.md) for the full list and a
suggested reading order.

## Testing

Run the full test suite (requires `anvil` on `$PATH` for integration tests):

```bash
cargo test --all-features
```

Run only the in-process unit tests (no external dependencies):

```bash
cargo test --lib
```

Lint checks:

```bash
cargo fmt --all -- --check
RUSTFLAGS="-Dwarnings" cargo clippy --all-features
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). For security issues, please follow
[SECURITY.md](SECURITY.md) instead of opening a public issue.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).

This project is a derivative of [Artemis](https://github.com/paradigmxyz/artemis)
by Paradigm, also licensed under Apache-2.0; see [NOTICE](NOTICE).
