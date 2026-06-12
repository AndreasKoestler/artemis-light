# Collector Polling Fallback Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** All four collectors downgrade from pubsub subscriptions to filter polling when `subscribe()` fails, with a warning, via one shared crate-private helper.

**Architecture:** A crate-private `subscribe_or_poll` helper in `src/collectors/fallback.rs` awaits a subscription future and, on error, warns and awaits a polling future. Each collector supplies both futures as private methods producing streams of the same item type; shared post-processing (tx lookup, event decode) is applied once after the helper. Stateless: every reconnect re-attempts the subscription first.

**Tech Stack:** Rust, alloy 1.0.8 (`watch_blocks` / `watch_pending_transactions` / `watch_logs` / `Event::watch`), tokio, futures, anyhow, tracing. Spec: `docs/superpowers/specs/2026-06-11-collector-polling-fallback-design.md`.

## Discovery

**Similar implementations:**
- `BlockCollector` already has the exact fallback inline (`src/collectors/block_collector.rs:38-78`): match on `subscribe_blocks()`, `warn!` on error, then `watch_blocks()` polling. This is the pattern being extracted.
- Executor-side combinators each live in their own file under `src/executor_ext/` (e.g. `fallback.rs`, `retry.rs`) — one responsibility per file.

**File conventions:**
- Collectors live in `src/collectors/<name>_collector.rs`, registered in `src/collectors/mod.rs` as `mod x;` + `pub use x::*;`. A crate-private helper gets `mod fallback;` with **no** `pub use`.
- `CollectorStream<'a, E> = Pin<Box<dyn Stream<Item = E> + Send + 'a>>` from `crate::types`.

**Testing patterns:**
- Unit tests co-located in `#[cfg(test)] mod tests` (see `src/collectors/event_collector.rs:104-133`), with doc comments explaining *why* the behavior matters.
- Integration tests in `tests/main.rs` against Anvil (`alloy::node_bindings`), 1s block time. An HTTP-fallback test already exists: `test_block_collector_polls_when_subscriptions_are_unavailable` builds `ProviderBuilder::new().connect_http(anvil.endpoint().parse().unwrap())` and asserts delivery within a 15s timeout.
- The `Emitter` test contract (already in `tests/main.rs`) provides logs/events; `spawn_anvil_with_signer()` is the wallet-provider template.

**Integration points:**
- The Collector Driver calls `subscribe()` on every reconnect; a `subscribe()` error feeds the Reconnect Policy's failure counter. The helper changes nothing about this contract — a failed poll still propagates as a `subscribe()` error.
- `EventCollector` implements both `Collector::subscribe` and `PersistableCollector::subscribe_indexed`; both must downgrade.

**Verified alloy 1.0.8 API facts (from registry sources):**
- `watch_blocks()` / `watch_pending_transactions()` → `TransportResult<FilterPollerBuilder<B256>>`; `watch_logs(&Filter)` → `TransportResult<FilterPollerBuilder<Log>>`. `FilterPollerBuilder<R>::into_stream()` yields **`Vec<R>` batches** (no Result wrapper).
- `Event::watch()` → `TransportResult<EventPoller<E>>`; `EventPoller::into_stream()` and `EventSubscription::into_stream()` (from `Event::subscribe()`) both yield **`alloy::sol_types::Result<(E, Log)>`** — identical item types, so decode/reorg filtering is shared verbatim.
- Anvil's `127.0.0.1` URL is detected as local, so the HTTP poll interval defaults to 1s — 15s test timeouts are ample.

**Project conventions:** `/Users/andreas/CLAUDE.md` asks for the pre-commit hook before declaring completion — **no pre-commit hook exists in this repo** (`.git/hooks/pre-commit` absent), so run `cargo fmt` + targeted tests before each commit instead. Conventional-commit style messages (`feat:`, `docs:`, `refactor:`, `test:`).

**Context loaded:** `CONTEXT.md` (domain language: Reconnect Policy, Collector Driver, Fallback — the new term must not collide with the executor-side **Fallback**); no `.superpowers/context/` root exists — ad-hoc discovery.

## File Structure

- **Create** `src/collectors/fallback.rs` — `pub(crate) async fn subscribe_or_poll` + unit tests. One responsibility: the downgrade decision and its warning.
- **Modify** `src/collectors/mod.rs` — register `mod fallback;` (crate-private).
- **Modify** `src/collectors/block_collector.rs` — refactor inline fallback onto the helper (behavior-preserving).
- **Modify** `src/collectors/mempool_collector.rs` — split hash sources into two private methods; shared lookup pipeline applied after the helper.
- **Modify** `src/collectors/log_collector.rs` — two private stream methods + helper.
- **Modify** `src/collectors/event_collector.rs` — private `raw_stream()` (subscription-or-poll) shared by `subscribe` and `subscribe_indexed`.
- **Modify** `tests/main.rs` — `spawn_anvil_http()` / `spawn_anvil_http_with_signer()` helpers; refactor the existing block HTTP test onto the helper; three new HTTP fallback tests (mempool, log, event).
- **Modify** `CONTEXT.md` — **Polling Fallback** language entry + one Relationships bullet.

---

### Task 1: `subscribe_or_poll` helper

**Files:**
- Create: `src/collectors/fallback.rs`
- Modify: `src/collectors/mod.rs`

- [ ] **Step 1: Write the helper module with failing tests and a `todo!()` body**

Create `src/collectors/fallback.rs`:

```rust
//! The subscribe-or-poll downgrade shared by every collector.
//!
//! Pubsub subscriptions need a WebSocket (or IPC) transport; over plain HTTP
//! every `eth_subscribe` fails. Rather than letting the Reconnect Policy
//! retry a call that can never succeed, a collector hands this helper two
//! futures — its pubsub subscription and its filter-polling counterpart —
//! and the helper downgrades to polling, with a warning, when the
//! subscription cannot be established.

use crate::types::CollectorStream;
use anyhow::Result;
use std::future::Future;
use tracing::warn;

/// Await `subscribe`; on error, warn (naming `what` and the error) and await
/// `poll` instead. A poll failure propagates as the `subscribe()` error the
/// Reconnect Policy counts.
///
/// Stateless by design: every call — one per reconnect — re-attempts the
/// subscription first, so a recovered pubsub endpoint upgrades back
/// automatically. The cost is one failed RPC and one warning line per
/// reconnect on HTTP-only providers.
pub(crate) async fn subscribe_or_poll<'a, E>(
    what: &str,
    subscribe: impl Future<Output = Result<CollectorStream<'a, E>>> + Send,
    poll: impl Future<Output = Result<CollectorStream<'a, E>>> + Send,
) -> Result<CollectorStream<'a, E>> {
    let _ = (what, subscribe, poll);
    todo!()
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn stream_of(items: Vec<u32>) -> Result<CollectorStream<'static, u32>> {
        Ok(Box::pin(tokio_stream::iter(items)))
    }

    // A typed failing arm: `subscribe_or_poll` takes `impl Trait` arguments,
    // so its type parameters cannot be supplied via turbofish (E0632) and a
    // bare `async { Err(..) }` block would leave `E` uninferable.
    async fn failing(msg: &'static str) -> Result<CollectorStream<'static, u32>> {
        Err(anyhow::anyhow!(msg))
    }

    /// A working subscription must be used as-is; the poll arm must not even
    /// start, since building it costs RPC calls (filter creation).
    #[tokio::test]
    async fn working_subscription_skips_polling() {
        let polled = Arc::new(AtomicBool::new(false));
        let flag = polled.clone();
        let poll = async move {
            flag.store(true, Ordering::SeqCst);
            stream_of(vec![9])
        };

        let stream = subscribe_or_poll("test", async { stream_of(vec![1, 2]) }, poll)
            .await
            .unwrap();
        assert_eq!(stream.collect::<Vec<_>>().await, vec![1, 2]);
        assert!(!polled.load(Ordering::SeqCst), "poll arm must not run");
    }

    /// A failed subscription (e.g. no pubsub on an HTTP transport) downgrades
    /// to the polling stream instead of erroring out.
    #[tokio::test]
    async fn failed_subscription_downgrades_to_polling() {
        let stream = subscribe_or_poll("test", failing("no pubsub"), async {
            stream_of(vec![7])
        })
        .await
        .unwrap();
        assert_eq!(stream.collect::<Vec<_>>().await, vec![7]);
    }

    /// When polling fails too, its error must propagate out of `subscribe()`
    /// so the Reconnect Policy counts the failure and drives the retry.
    #[tokio::test]
    async fn poll_failure_propagates_to_reconnect_policy() {
        let err = subscribe_or_poll("test", failing("no pubsub"), failing("filters unsupported"))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("filters unsupported"));
    }
}
```

Register the module in `src/collectors/mod.rs` — after the `event_collector` line, add:

```rust
/// Crate-private subscribe-or-poll downgrade shared by the collectors above.
mod fallback;
```

(No `pub use` — the helper is not public API.)

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test --lib collectors::fallback`
Expected: 3 tests FAIL (panic at `not yet implemented` from `todo!()`). If they fail to *compile* instead, fix the test code first — the red state must be the `todo!()` panic.

- [ ] **Step 3: Implement the helper**

Replace the function body (`let _ = ...; todo!()`) with:

```rust
    match subscribe.await {
        Ok(stream) => Ok(stream),
        Err(e) => {
            warn!("Error subscribing to {what} ({e}); polling instead");
            poll.await
        }
    }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test --lib collectors::fallback`
Expected: `test result: ok. 3 passed`

- [ ] **Step 5: Format and commit**

```bash
cargo fmt
git add src/collectors/fallback.rs src/collectors/mod.rs
git commit -m "feat: add subscribe_or_poll downgrade helper for collectors"
```

---

### Task 2: Refactor `BlockCollector` onto the helper

Behavior-preserving refactor — the inline fallback at `src/collectors/block_collector.rs:38-78` becomes two private methods plus a helper call. The existing WS and HTTP integration tests are the safety net; no new tests.

**Files:**
- Modify: `src/collectors/block_collector.rs`
- Test (existing): `tests/main.rs` — `test_block_collector_sends_blocks`, `test_block_collector_polls_when_subscriptions_are_unavailable`

- [ ] **Step 1: Run the existing block tests to confirm green before refactoring**

Run: `cargo test --test main test_block_collector`
Expected: `test result: ok. 2 passed` (slow — each spawns Anvil with 1s blocks)

- [ ] **Step 2: Refactor**

Replace the whole `Collector` impl (everything from `/// Implementation of the [Collector]...` to the end of the file) with:

```rust
/// Implementation of the [Collector](Collector) trait for the [BlockCollector](BlockCollector).
#[async_trait]
impl<M> Collector<NewBlock> for BlockCollector<M>
where
    M: Provider,
{
    async fn subscribe(&self) -> Result<CollectorStream<'_, NewBlock>> {
        subscribe_or_poll("blocks", self.subscription_stream(), self.polling_stream()).await
    }
}

impl<M> BlockCollector<M>
where
    M: Provider,
{
    /// New-block headers over pubsub. Fails on transports without pubsub
    /// (most commonly plain HTTP), which is the cue to poll instead.
    async fn subscription_stream(&self) -> Result<CollectorStream<'_, NewBlock>> {
        let subscription = self.provider.subscribe_blocks().await?;
        let stream = subscription.into_stream().map(|header| NewBlock {
            hash: header.hash,
            number: header.number,
        });
        Ok(Box::pin(stream))
    }

    /// Poll block *hashes* and fetch each header on demand. A `NewBlock`
    /// needs only the header, so polling full blocks would download every
    /// transaction body just to throw it away.
    async fn polling_stream(&self) -> Result<CollectorStream<'_, NewBlock>> {
        let mut hashes = self.provider.watch_blocks().await?.into_stream();
        let provider = self.provider.clone();
        let stream = async_stream::stream! {
            while let Some(batch) = hashes.next().await {
                for hash in batch {
                    match provider.get_block_by_hash(hash).await {
                        Ok(Some(block)) => {
                            yield NewBlock {
                                hash: block.header.hash,
                                number: block.header.number,
                            };
                        }
                        Ok(None) => {
                            warn!("Polled block {hash} not found; skipping")
                        }
                        Err(e) => {
                            warn!("Error fetching polled block {hash}; skipping: {e}")
                        }
                    }
                }
            }
        };
        Ok(Box::pin(stream))
    }
}
```

Update the imports at the top of the file: the first line becomes

```rust
use crate::collectors::fallback::subscribe_or_poll;
use crate::types::{Collector, CollectorStream};
```

(The `tracing::warn` import stays — the polling arm still uses it; the per-subscribe-failure warning now lives in the helper.)

- [ ] **Step 3: Run the block tests to verify still green**

Run: `cargo test --test main test_block_collector`
Expected: `test result: ok. 2 passed`

- [ ] **Step 4: Format and commit**

```bash
cargo fmt
git add src/collectors/block_collector.rs
git commit -m "refactor: move BlockCollector polling fallback onto subscribe_or_poll"
```

---

### Task 3: `MempoolCollector` fallback

**Files:**
- Modify: `src/collectors/mempool_collector.rs`
- Modify: `tests/main.rs` (add `spawn_anvil_http`, refactor existing block HTTP test onto it, add the mempool fallback test)

- [ ] **Step 1: Write the failing integration test**

In `tests/main.rs`, add next to `spawn_anvil_with_signer` (around line 56):

```rust
/// Spawns Anvil and instantiates an HTTP-only provider (no pubsub), to
/// exercise the collectors' polling fallback.
pub async fn spawn_anvil_http() -> Result<(impl Provider + Clone, AnvilInstance)> {
    let anvil = Anvil::new().block_time(1).chain_id(1337).try_spawn()?;
    let provider = ProviderBuilder::new().connect_http(anvil.endpoint().parse()?);
    Ok((provider, anvil))
}
```

Refactor `test_block_collector_polls_when_subscriptions_are_unavailable` to use it — its first three statements (the inline `Anvil::new()...` / `ProviderBuilder...` block) become:

```rust
    let (provider, _anvil) = spawn_anvil_http().await.unwrap();
    let provider = Arc::new(provider);
```

(If the `Anvil` import at the top of the file then goes unused, it won't — `spawn_anvil_http` still uses it.)

Then add after `test_mempool_collector_sends_txs`:

```rust
/// Over plain HTTP there is no pubsub, so `subscribe_pending_transactions`
/// fails and the collector must fall back to polling the pending-tx filter —
/// and still deliver the full transaction.
#[tokio::test]
async fn test_mempool_collector_polls_when_subscriptions_are_unavailable() {
    let (provider, _anvil) = spawn_anvil_http().await.unwrap();
    let provider = Arc::new(provider);
    let mempool_collector = MempoolCollector::new(provider.clone());
    let mempool_stream = mempool_collector.subscribe().await.unwrap();

    let account = provider.get_accounts().await.unwrap()[0];
    let tx = TransactionRequest::default()
        .with_to(account)
        .with_from(account)
        .with_value(U256::from(42))
        .with_gas_price(100_000_000_000_000_000u128);
    let pending_tx = provider.send_transaction(tx).await.unwrap();
    let tx_hash = *pending_tx.tx_hash();

    let tx = tokio::time::timeout(
        std::time::Duration::from_secs(15),
        mempool_stream.into_future(),
    )
    .await
    .expect("polling fallback should deliver the pending tx within 15s")
    .0
    .unwrap();
    assert_eq!(tx.inner.hash(), &tx_hash);
}
```

- [ ] **Step 2: Run the new test to verify it fails**

Run: `cargo test --test main test_mempool_collector_polls`
Expected: FAIL — panics at `mempool_collector.subscribe().await.unwrap()` with a transport error (HTTP has no `eth_subscribe`).

- [ ] **Step 3: Implement the fallback**

In `src/collectors/mempool_collector.rs`, replace the `Collector` impl (the whole block from `/// Implementation of the [Collector]...` to end of file) with:

```rust
/// Implementation of the [Collector](Collector) trait for the [MempoolCollector](MempoolCollector).
#[async_trait]
impl<M> Collector<Transaction> for MempoolCollector<M>
where
    M: Provider,
{
    async fn subscribe(&self) -> Result<CollectorStream<'_, Transaction>> {
        let hashes = subscribe_or_poll(
            "pending transactions",
            self.subscription_hashes(),
            self.polling_hashes(),
        )
        .await?;

        // Both sources yield bare hashes; the full-transaction lookup is
        // shared and applied once, after the source is chosen.
        let provider = self.provider.clone();
        let rpc_timeout = self.rpc_timeout;
        let stream = hashes
            .map(move |tx_hash| {
                let provider = provider.clone();
                async move {
                    match tokio::time::timeout(
                        rpc_timeout,
                        provider.get_transaction_by_hash(tx_hash),
                    )
                    .await
                    {
                        Ok(Ok(tx)) => tx,
                        Ok(Err(e)) => {
                            tracing::warn!(
                                "Failed to get transaction by hash {:?}: {}",
                                tx_hash,
                                e
                            );
                            None
                        }
                        Err(_) => {
                            tracing::warn!("Timeout getting transaction by hash {:?}", tx_hash);
                            None
                        }
                    }
                }
            })
            .buffer_unordered(self.max_concurrent_lookups)
            .filter_map(|tx| async { tx });

        Ok(Box::pin(stream))
    }
}

impl<M> MempoolCollector<M>
where
    M: Provider,
{
    /// Pending-tx hashes over pubsub. Fails on transports without pubsub.
    async fn subscription_hashes(&self) -> Result<CollectorStream<'_, TxHash>> {
        let stream = self
            .provider
            .subscribe_pending_transactions()
            .await?
            .into_stream();
        Ok(Box::pin(stream))
    }

    /// Pending-tx hashes via a polled filter; the poller yields batches,
    /// flattened here to match the subscription's shape.
    async fn polling_hashes(&self) -> Result<CollectorStream<'_, TxHash>> {
        let poller = self.provider.watch_pending_transactions().await?;
        Ok(Box::pin(poller.into_stream().flat_map(futures::stream::iter)))
    }
}
```

Update imports at the top of the file:

```rust
use alloy::{primitives::TxHash, providers::Provider, rpc::types::Transaction};
```

and add below the existing `use crate::types::...` line:

```rust
use crate::collectors::fallback::subscribe_or_poll;
```

- [ ] **Step 4: Run the mempool tests (both transports) to verify they pass**

Run: `cargo test --test main test_mempool_collector`
Expected: `test result: ok. 2 passed`

- [ ] **Step 5: Format and commit**

```bash
cargo fmt
git add src/collectors/mempool_collector.rs tests/main.rs
git commit -m "feat: MempoolCollector falls back to filter polling without pubsub"
```

---

### Task 4: `LogCollector` fallback

**Files:**
- Modify: `src/collectors/log_collector.rs`
- Modify: `tests/main.rs` (add `spawn_anvil_http_with_signer`, add the log fallback test)

- [ ] **Step 1: Write the failing integration test**

In `tests/main.rs`, add next to `spawn_anvil_http`:

```rust
/// Spawns Anvil and instantiates an HTTP-only provider with a wallet signer,
/// for fallback tests that deploy contracts.
pub async fn spawn_anvil_http_with_signer() -> Result<(impl Provider + Clone, AnvilInstance)> {
    let anvil = Anvil::new().block_time(1).chain_id(1337).try_spawn()?;
    let signer: PrivateKeySigner = anvil.keys()[0].clone().into();
    let provider = ProviderBuilder::new()
        .wallet(signer)
        .connect_http(anvil.endpoint().parse()?);
    Ok((provider, anvil))
}
```

Then add after `test_log_collector_receives_logs`:

```rust
/// Over plain HTTP there is no pubsub, so `subscribe_logs` fails and the
/// collector must fall back to a polled log filter — and still deliver logs.
#[tokio::test]
async fn test_log_collector_polls_when_subscriptions_are_unavailable() {
    let (provider, _anvil) = spawn_anvil_http_with_signer().await.unwrap();
    let provider = Arc::new(provider);

    let contract = Emitter::deploy(provider.clone()).await.unwrap();
    let contract_addr = *contract.address();

    // Subscribe before emitting: the polled filter only reports logs that
    // arrive after its creation.
    let filter = Filter::new().address(contract_addr);
    let log_collector = LogCollector::new(provider.clone(), filter);
    let log_stream = log_collector.subscribe().await.unwrap();

    contract
        .setValue(U256::from(42))
        .send()
        .await
        .unwrap()
        .watch()
        .await
        .unwrap();

    let log = tokio::time::timeout(
        std::time::Duration::from_secs(15),
        log_stream.into_future(),
    )
    .await
    .expect("polling fallback should deliver the log within 15s")
    .0
    .unwrap();
    assert_eq!(log.address(), contract_addr);
}
```

- [ ] **Step 2: Run the new test to verify it fails**

Run: `cargo test --test main test_log_collector_polls`
Expected: FAIL — panics at `log_collector.subscribe().await.unwrap()` with a transport error.

- [ ] **Step 3: Implement the fallback**

In `src/collectors/log_collector.rs`, replace the `Collector` impl (from `/// Implementation of the [Collector]...` to end of file) with:

```rust
/// Implementation of the [Collector](Collector) trait for the [LogCollector](LogCollector).
#[async_trait]
impl<M> Collector<Log> for LogCollector<M>
where
    M: Provider,
{
    async fn subscribe(&self) -> Result<CollectorStream<'_, Log>> {
        subscribe_or_poll("logs", self.subscription_stream(), self.polling_stream()).await
    }
}

impl<M> LogCollector<M>
where
    M: Provider,
{
    /// Matching logs over pubsub. Fails on transports without pubsub.
    async fn subscription_stream(&self) -> Result<CollectorStream<'_, Log>> {
        let stream = self.provider.subscribe_logs(&self.filter).await?;
        Ok(Box::pin(stream.into_stream()))
    }

    /// Matching logs via a polled filter; the poller yields batches,
    /// flattened here to match the subscription's shape.
    async fn polling_stream(&self) -> Result<CollectorStream<'_, Log>> {
        let poller = self.provider.watch_logs(&self.filter).await?;
        Ok(Box::pin(poller.into_stream().flat_map(futures::stream::iter)))
    }
}
```

Update the imports at the top of the file:

```rust
use crate::collectors::fallback::subscribe_or_poll;
use crate::types::{Collector, CollectorStream};
use alloy::{
    providers::Provider,
    rpc::types::{Filter, Log},
};
use anyhow::Result;
use async_trait::async_trait;
use futures::StreamExt;
use std::sync::Arc;
```

(`futures::StreamExt` is new — needed for `flat_map`.)

- [ ] **Step 4: Run the log tests (both transports) to verify they pass**

Run: `cargo test --test main test_log_collector`
Expected: `test result: ok. 2 passed`

- [ ] **Step 5: Format and commit**

```bash
cargo fmt
git add src/collectors/log_collector.rs tests/main.rs
git commit -m "feat: LogCollector falls back to filter polling without pubsub"
```

---

### Task 5: `EventCollector` fallback (subscribe + subscribe_indexed)

Both `EventPoller::into_stream()` and `EventSubscription::into_stream()` yield `alloy::sol_types::Result<(E, Log)>`, so one private `raw_stream()` feeds both trait methods and the existing `indexed_event` filtering applies unchanged to either source.

**Files:**
- Modify: `src/collectors/event_collector.rs`
- Modify: `tests/main.rs` (add the event fallback test)

- [ ] **Step 1: Write the failing integration test**

In `tests/main.rs`, add after `test_event_collector_receives_events`:

```rust
/// Over plain HTTP there is no pubsub, so the event subscription fails and
/// the collector must fall back to a polled log filter — and still deliver
/// decoded events.
#[tokio::test]
async fn test_event_collector_polls_when_subscriptions_are_unavailable() {
    let (provider, _anvil) = spawn_anvil_http_with_signer().await.unwrap();
    let provider = Arc::new(provider);

    let contract = Emitter::deploy(provider.clone()).await.unwrap();

    // Subscribe before emitting: the polled filter only reports logs that
    // arrive after its creation.
    let event_filter = contract.ValueSet_filter();
    let event_collector = EventCollector::new(event_filter);
    let event_stream = event_collector.subscribe().await.unwrap();

    contract
        .setValue(U256::from(42))
        .send()
        .await
        .unwrap()
        .watch()
        .await
        .unwrap();

    let ev = tokio::time::timeout(
        std::time::Duration::from_secs(15),
        event_stream.into_future(),
    )
    .await
    .expect("polling fallback should deliver the event within 15s")
    .0
    .unwrap();
    assert_eq!(ev.value, U256::from(42));
}
```

- [ ] **Step 2: Run the new test to verify it fails**

Run: `cargo test --test main test_event_collector_polls`
Expected: FAIL — panics at `event_collector.subscribe().await.unwrap()` with a transport error.

- [ ] **Step 3: Implement the fallback**

In `src/collectors/event_collector.rs`:

Add to the imports:

```rust
use crate::collectors::fallback::subscribe_or_poll;
use alloy::rpc::types::Log;
```

Add below the `indexed_event` function (before the `Collector` impl):

```rust
/// The raw decoded `(event, log)` stream, before reorg/index filtering.
/// Subscription and poller deliberately share this item type.
type RawEventStream<'a, E> = CollectorStream<'a, alloy::sol_types::Result<(E, Log)>>;

impl<P, E> EventCollector<P, E>
where
    P: Provider,
    E: SolEvent + Send + Sync,
{
    /// The `(event, log)` source shared by `subscribe` and
    /// `subscribe_indexed`: pubsub when available, filter polling otherwise.
    async fn raw_stream(&self) -> Result<RawEventStream<'_, E>> {
        subscribe_or_poll(
            "contract events",
            self.subscription_stream(),
            self.polling_stream(),
        )
        .await
    }

    /// Decoded events over pubsub. Fails on transports without pubsub.
    async fn subscription_stream(&self) -> Result<RawEventStream<'_, E>> {
        Ok(Box::pin(self.event.subscribe().await?.into_stream()))
    }

    /// Decoded events via a polled log filter.
    async fn polling_stream(&self) -> Result<RawEventStream<'_, E>> {
        Ok(Box::pin(self.event.watch().await?.into_stream()))
    }
}
```

In `Collector::subscribe`, replace the first line

```rust
        let stream = self.event.subscribe().await?.into_stream();
```

with

```rust
        let stream = self.raw_stream().await?;
```

In `PersistableCollector::subscribe_indexed`, make the same one-line replacement (`self.event.subscribe().await?.into_stream()` → `self.raw_stream().await?`). The `filter_map` bodies in both methods stay exactly as they are. `query_range` and `tip` are untouched.

- [ ] **Step 4: Run the event collector tests to verify they pass**

Run: `cargo test --test main test_event_collector && cargo test --lib event_collector`
Expected: integration `2 passed`; lib unit tests `2 passed`.

- [ ] **Step 5: Format and commit**

```bash
cargo fmt
git add src/collectors/event_collector.rs tests/main.rs
git commit -m "feat: EventCollector falls back to filter polling without pubsub"
```

---

### Task 6: CONTEXT.md term + full verification

**Files:**
- Modify: `CONTEXT.md`

- [ ] **Step 1: Add the language entry**

In `CONTEXT.md`, directly after the **Fallback** entry (the paragraph ending `_Avoid_: failover, backup executor`), add:

```markdown
**Polling Fallback**:
The collector-side downgrade from a pubsub subscription to filter polling when the subscription cannot be established (most commonly a transport without pubsub, e.g. plain HTTP). The downgrade is logged as a warning and is stateless: every subscribe attempt — one per reconnect — tries the subscription first, so a recovered pubsub endpoint upgrades back automatically. A failed poll propagates as an ordinary subscribe failure to the **Reconnect Policy**. Distinct from **Fallback**, the executor-side wrapper.
_Avoid_: failover, degraded mode
```

In the `## Relationships` section, add this bullet:

```markdown
- Every built-in **Collector**'s subscribe carries a **Polling Fallback**; a failed poll feeds the **Reconnect Policy**'s counter like any subscribe failure.
```

- [ ] **Step 2: Run the full verification suite**

```bash
cargo fmt --check && cargo clippy --all-targets && cargo test
```

Expected: fmt clean; clippy introduces no new warnings; full test suite passes (lib unit tests + `tests/main.rs` + `tests/persistence.rs` — slow, spawns many Anvil instances).

- [ ] **Step 3: Commit**

```bash
git add CONTEXT.md
git commit -m "docs: add Polling Fallback to the domain language"
```
