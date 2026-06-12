# Collector Polling Fallback

**Date:** 2026-06-11
**Status:** Approved

## Problem

A collector built over a plain-HTTP provider has no pubsub: every
`eth_subscribe`-backed call fails, so `subscribe()` errors and the Reconnect
Policy retries a call that can never succeed. Today only `BlockCollector`
copes — it carries an inline fallback to `watch_blocks` polling. The other
three collectors (`MempoolCollector`, `LogCollector`, `EventCollector`) fail
hard, even though alloy ships a polling counterpart for every subscription
(`watch_pending_transactions`, `watch_logs`, `Event::watch`).

All four collectors should downgrade to polling when subscriptions aren't
supported, log a warning when they do, and share one abstraction for the
downgrade instead of four inline copies.

## Decisions

- **Crate-private helper, not a public combinator.** The fallback is built-in
  default behavior of the four collectors; nothing about it is composable by
  users, so it adds no public API surface.
- **Stateless across reconnects.** Every `subscribe()` attempt tries the
  subscription first, warns on failure, then polls. If a WS endpoint
  recovers, the next reconnect upgrades back automatically. The cost is one
  failed RPC and one warning line per reconnect on HTTP-only providers.
- **Poll cadence inherits from the provider.** Alloy's `watch_*` pollers use
  the provider client's configured poll interval; the collectors expose no
  interval knob of their own.

## Design

### The helper

A new crate-private module `src/collectors/fallback.rs`:

```rust
pub(crate) async fn subscribe_or_poll<'a, E>(
    what: &str,
    subscribe: impl Future<Output = Result<CollectorStream<'a, E>>> + Send,
    poll: impl Future<Output = Result<CollectorStream<'a, E>>> + Send,
) -> Result<CollectorStream<'a, E>>
```

Await `subscribe`; on `Ok`, return its stream and never touch `poll`. On
`Err(e)`, emit `warn!("Error subscribing to {what} ({e}); polling instead")`
and await `poll`, returning its result as-is.

### Per-collector wiring

Each collector builds two futures producing streams of the same item type and
hands them to the helper. Post-stream processing shared by both paths is
written once and applied after the helper returns.

- **BlockCollector** — refactor only. Primary: `subscribe_blocks` mapped to
  `NewBlock`. Poll: the existing `watch_blocks` + `get_block_by_hash` logic,
  including its skip-with-warn handling for missing or failed block fetches.
  Behavior is unchanged; the inline `match` moves into the helper's shape.
- **MempoolCollector** — primary: `subscribe_pending_transactions` as a
  stream of hashes. Poll: `watch_pending_transactions`, whose `Vec<TxHash>`
  batches are flattened into the same hash stream. The hash → full
  transaction lookup pipeline (per-call timeout, `buffer_unordered`,
  drop-with-warn on failure) is applied once, after the helper.
- **LogCollector** — primary: `subscribe_logs`. Poll: `watch_logs(&filter)`
  with batches flattened.
- **EventCollector** — primary: `event.subscribe()`. Poll: `event.watch()`
  (alloy's `EventPoller`). Both paths yield decoded `(E, Log)` results, so
  the existing `indexed_event` reorg/decode filtering applies to both. The
  fallback covers **both** `Collector::subscribe` and
  `PersistableCollector::subscribe_indexed`; `query_range` and `tip` already
  work over HTTP and are untouched. The exact `EventPoller` stream item shape
  is verified at implementation time; any mismatch with the subscription
  stream is adapted inside the collector, not the helper.

### Error handling

If polling also fails, that error propagates out of `subscribe()` — exactly
what the Reconnect Policy counts — so backoff and Fatal semantics are
unchanged. The downgrade warning fires once per subscribe attempt: once per
reconnect on HTTP-only providers, an accepted cost of statelessness.

### Documentation

Add a `CONTEXT.md` language entry:

> **Polling Fallback**: The collector-side downgrade from a pubsub
> subscription to filter polling when `subscribe()` fails, logged as a
> warning and re-attempted fresh on every reconnect. While polling, event
> latency is the provider's poll interval rather than push-on-arrival.
> Distinct from the executor-side **Fallback** wrapper.
> _Avoid_: failover, degraded mode

## Testing

- **Helper unit tests** (in `fallback.rs`): primary succeeds → poll arm never
  runs; primary fails → poll stream is returned; both fail → poll's error
  propagates.
- **Integration tests** (`tests/main.rs`, existing Anvil harness): one test
  per collector over `anvil.endpoint()` (HTTP) asserting events are delivered
  via the polling path. Existing WS tests continue to cover the subscription
  path.

## Out of scope

- A public `or_else` collector combinator (no in-repo consumer; YAGNI).
- Per-collector poll-interval configuration.
- Remembering the downgrade across reconnects.
