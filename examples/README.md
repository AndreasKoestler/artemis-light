# Examples

Runnable, narrated demos of every facility the crate provides. Each one is
self-contained and exits on its own.

Suggested reading order:

| Example | Demonstrates | Needs a node? |
|---|---|---|
| [`basic_example`](basic_example.rs) | The core pipeline: a custom `Collector`, `Strategy`, and `Executor` wired through the `Engine`, plus cooperative shutdown | No |
| [`combinators_example`](combinators_example.rs) | Composing collectors with `CollectorExt`: `map`, `filter_map`, `merge`, `chain`, `fallback`, and the `merge_all`/`chain_all`/`fallback_all` list forms | No |
| [`adapters_example`](adapters_example.rs) | Mounting narrow strategies and executors into an umbrella-enum engine with `StrategyExt::filter_map_event`/`map_action` and `ExecutorExt::filter_map_action` | No |
| [`observer_example`](observer_example.rs) | A passive `Observer` watching every event and action crossing the engine's channels | No |
| [`reliability_example`](reliability_example.rs) | Reliability wrappers for executors (`retry`, `fallback`, `rate_limit`, `circuit_breaker`, `gated`/`dry_run`) and strategy-side risk guards (`filter_actions`, `cooldown`) | No |
| [`feedback_example`](feedback_example.rs) | Execution feedback: `ExecutorExt::report` publishes each action's verdict, a `ChannelCollector` feeds it back as an event, and the strategy stops re-submitting once a trade is confirmed | No |
| [`liquidation_bot_example`](liquidation_bot_example.rs) | The same combinators in their production seats: a risk-gated, cooled-down liquidation strategy feeding a `retry` → `fallback` → `rate_limit` → `circuit_breaker` → `gated` submission stack, per-route policies under an umbrella `Action`, and a dry-run shadow executor | No |
| [`reconnect_example`](reconnect_example.rs) | The collector reconnect lifecycle: `ReconnectConfig`, exponential backoff, recovery, and escalation to the fatal token | No |
| [`persistence_example`](persistence_example.rs) | Recording events to SQLite with `.with_persistence(store)` and replaying them after a restart | Anvil |
| [`confirmation_depth_example`](confirmation_depth_example.rs) | Lagging the store behind the live edge with `.with_confirmation_depth(n)` so a shallow reorg is absorbed before any row is written; events still arrive live | Anvil |
| [`onchain_example`](onchain_example.rs) | An end-to-end on-chain pipeline: `BlockCollector` → strategy → `MempoolExecutor` submitting real transactions | Anvil |
| [`serving_example`](serving_example.rs) | Indexing events into a file-backed SQLite store, standing up the read-only `ServingLayer` over it, and navigating the HTTP/JSON API (health, status, tables, schema, paged rows) with a tiny client | Anvil |
| [`injected_pool_example`](injected_pool_example.rs) | Bring-your-own PostgreSQL pool: building a `PostgresStore` from a caller-owned `sqlx::PgPool` with `with_pool`, persisting events, rebuilding a second store from the **same** pool to replay history, then proving the injected pool is still open after every store handle is dropped | Anvil + Postgres |

Run any of them with:

```sh
cargo run --example <name>
```

The Anvil-backed examples spawn their own local chain; they only need
`anvil` on `$PATH` (it ships with [Foundry](https://getfoundry.sh)).

`serving_example` additionally needs the opt-in `serving` feature:

```sh
cargo run --example serving_example --features serving
```

`injected_pool_example` needs the opt-in `postgres` feature and a real
PostgreSQL database — it reads its connection string from `DATABASE_URL` and
never provisions a database itself. The quickest local Postgres is one line of
Docker:

```sh
docker run --rm -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres:16
```

Then, with `anvil` on `$PATH`, point `DATABASE_URL` at it and run:

```sh
DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
  cargo run --example injected_pool_example --features postgres
```

Run without `DATABASE_URL` set and the example exits `1` with:

```
Error: DATABASE_URL must be set to a PostgreSQL URL, e.g. postgres://postgres:postgres@localhost:5432/postgres
```

The example drops its own `value_set` and `_artemis_progress` tables at startup
(over the caller's pool) so re-runs are deterministic; point it only at a demo
database.

**Verified run.** This example has been executed end-to-end against a real
PostgreSQL (not merely compiled). Environment: macOS, `postgres:16` in Docker
via the one-liner above, Foundry `anvil` on `$PATH`, crate built with
`--features postgres`, on 2026-07-01. With `DATABASE_URL` of the shape
`postgres://postgres:postgres@localhost:<port>/postgres` pointing at the
container, `cargo run --example injected_pool_example --features postgres` exited
`0` and printed exactly:

```
First run — persisting 3 events through the injected pool:
  [live] ValueSet(10)
  [live] ValueSet(20)
  [live] ValueSet(30)
Highest persisted block: 3
Restart — a new store over the same injected pool recovers history:
  [recovered] ValueSet(10)
  [recovered] ValueSet(20)
  [recovered] ValueSet(30)
Store dropped — injected pool still usable: SELECT 1 succeeded
Done!
```

The stored resume point advances (`Highest persisted block: 3`), the second
store recovers all three events from the **same** pool, and the injected pool
still answers `SELECT 1` after every store handle is dropped. At rest the
database holds two `value_set` rows (blocks 2 and 3, values 10 and 20) plus the
`_artemis_progress` watermark `value_set → 3`; the third event occupies the
still-open latest block and is recovered on the restart leg by chain backfill
(the pipeline flushes a block only once a higher block is observed). A second
run reproduces byte-identical output thanks to the startup table reset.
