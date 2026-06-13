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
| **Executor** | `MempoolExecutor` | Submits transactions to the public mempool |
| **Observer** | `Observer<E, A>` | Passive consumer of every event and action crossing the channels |
| **Persistence** | `Persisted<C, S>` | Wraps a block-aware collector to record events to a SQL `Store` and replay them on restart |

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

See [`examples/persistence_example.rs`](examples/persistence_example.rs) for a
runnable demo (record live events, then recover them on a simulated restart):

```sh
cargo run --example persistence_example
```

## Quickstart

Add the dependency to your `Cargo.toml`:

```toml
[dependencies]
artemis-light = "0.1"
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
