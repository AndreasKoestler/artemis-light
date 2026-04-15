# artemis-light

A stripped-down, modernised fork of the [Artemis](https://github.com/paradigmxyz/artemis) MEV framework, built on [Alloy](https://github.com/alloy-rs/alloy) and Tokio.

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
| **Collector** | `BlockCollector` | Subscribes to new blocks via WebSocket |
| | `MempoolCollector` | Subscribes to pending transactions in the mempool |
| | `LogCollector` | Subscribes to on-chain event logs matching a filter |
| | `EventCollector` | Subscribes to an arbitrary `alloy` subscription |
| **Strategy** | `Strategy<E, A>` | User-defined: receives events, produces action streams |
| **Executor** | `MempoolExecutor` | Submits transactions to the public mempool |

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

**Executor combinators:**

| Combinator | Description |
|---|---|
| `ExecutorFilterMap` | Filter-maps actions before forwarding to an inner executor |
| `.instrument(metrics)` | Wraps an executor (or strategy) with a `Metrics` callback |

## Quickstart

Add the dependency to your `Cargo.toml`:

```toml
[dependencies]
artemis-light = { git = "https://github.com/hypurrfi/artemis-light" }
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

    let (cancel_token, mut join_set) = engine.run().await?;

    // Run until Ctrl-C
    tokio::signal::ctrl_c().await?;
    cancel_token.cancel();
    while join_set.join_next().await.is_some() {}

    Ok(())
}
```

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
