# Examples

Runnable, narrated demos of every facility the crate provides. Each one is
self-contained and exits on its own.

Suggested reading order:

| Example | Demonstrates | Needs a node? |
|---|---|---|
| [`basic_example`](basic_example.rs) | The core pipeline: a custom `Collector`, `Strategy`, and `Executor` wired through the `Engine`, plus cooperative shutdown | No |
| [`combinators_example`](combinators_example.rs) | Composing collectors with `CollectorExt`: `map`, `filter_map`, `merge`, `chain`, and the `merge_all`/`chain_all` list forms | No |
| [`observer_example`](observer_example.rs) | A passive `Observer` watching every event and action crossing the engine's channels | No |
| [`reconnect_example`](reconnect_example.rs) | The collector reconnect lifecycle: `ReconnectConfig`, exponential backoff, recovery, and escalation to the fatal token | No |
| [`persistence_example`](persistence_example.rs) | Recording events to SQLite with `.with_persistence(store)` and replaying them after a restart | Anvil |
| [`onchain_example`](onchain_example.rs) | An end-to-end on-chain pipeline: `BlockCollector` → strategy → `MempoolExecutor` submitting real transactions | Anvil |

Run any of them with:

```sh
cargo run --example <name>
```

The two Anvil-backed examples spawn their own local chain; they only need
`anvil` on `$PATH` (it ships with [Foundry](https://getfoundry.sh)).
