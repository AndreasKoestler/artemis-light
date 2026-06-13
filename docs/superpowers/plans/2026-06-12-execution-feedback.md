# Execution Feedback Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Report each action's submission verdict back to strategies by closing the loop through a collector — no engine or trait changes.

**Architecture:** Three composable pieces sharing one `tokio::sync::broadcast` channel: an `ExecutionOutcome<A>` value, a transparent `Report<A>` executor wrapper that publishes outcomes and returns the inner verdict unchanged, and a `ChannelCollector<T>` that turns the broadcast sender back into an event stream. The verdict re-enters the pipeline through the normal collector → strategy path, preserving the one-way topology.

**Tech Stack:** Rust (edition 2024), tokio broadcast channels, `tokio_stream::wrappers::BroadcastStream` (the `sync` feature is already enabled), async-trait, anyhow, tracing.

**Spec:** `docs/superpowers/specs/2026-06-12-execution-feedback-design.md`

## Discovery

**Similar implementations:** `src/executor_ext/gated.rs` is the wrapper template (boxed inner executor, `Debug` bound, tracing). `src/collectors/block_collector.rs` shows a `Collector` returning `Box::pin(stream)`; `src/collectors/event_collector.rs` shows `filter_map` over a fallible stream (the lag-skipping shape `ChannelCollector` needs). The engine's `src/engine/channel.rs` converts a broadcast receiver to a stream — confirms `BroadcastStream` is the idiom.
**File conventions:** One file per combinator/collector; `mod` + `pub use` in the parent (`src/executor_ext.rs`, `src/collectors/mod.rs`), both alphabetical. Extension methods carry rustdoc on placement.
**Testing patterns:** `#[cfg(test)] mod test`/`mod tests` in-file; sentence-style names; purpose-built doubles (`RecordingExecutor`, `FailingExecutor`); `#[tokio::test]` (no paused time needed here — no delays). Collector tests construct the collector directly and drive `subscribe()`.
**Integration points:** `ExecutorExt` and `CollectorExt` are blanket-implemented, so `report` and `.map()` chain freely. Examples are registered in both `Cargo.toml` (`[[example]]`) and `examples/README.md`. `collectors` are re-exported via `pub use ...::*` in `src/collectors/mod.rs`.
**Project conventions:** Verification gate is `cargo fmt --all -- --check`, `RUSTFLAGS="-Dwarnings" cargo clippy --all-features`, `cargo test --lib`. `lib.rs` has `#![warn(unused_crate_dependencies)]` and `#![deny(unused_must_use, ...)]`. Commit messages are plain imperative sentences. No pre-commit hook present.
**Context loaded:** none — ad-hoc discovery (engine.rs, collectors, executor_ext, CONTEXT.md, README.md).

## File Structure

- Create: `src/executor_ext/report.rs` — `ExecutionOutcome<A>`, `Report<A>`, unit tests
- Modify: `src/executor_ext.rs` — `mod report;` + `pub use report::*;` + `ExecutorExt::report()`
- Create: `src/collectors/channel_collector.rs` — `ChannelCollector<T>`, unit tests
- Modify: `src/collectors/mod.rs` — `mod channel_collector;` + `pub use channel_collector::*;`
- Create: `examples/feedback_example.rs` — closed-loop demo (no RPC)
- Modify: `Cargo.toml` — `[[example]]` registration
- Modify: `examples/README.md` — table row
- Modify: `README.md` — "Execution feedback" subsection
- Modify: `CONTEXT.md` — three terms + a relationships line

---

### Task 1: `ExecutionOutcome` and the `Report` wrapper

**Files:**
- Create: `src/executor_ext/report.rs`
- Modify: `src/executor_ext.rs`

- [ ] **Step 1: Write the failing tests**

Create `src/executor_ext/report.rs` with the types and the full test module. `execute` is stubbed with `todo!()` so tests compile and fail:

```rust
use crate::types::Executor;
use anyhow::Result;
use async_trait::async_trait;
use std::fmt::Debug;
use tokio::sync::broadcast::Sender;

/// The verdict the executor stack reached for one action, fed back into the
/// pipeline as an event. `result` is `Ok(())` when the stack accepted the
/// action and `Err(message)` when it failed; the error is stringified because
/// [`anyhow::Error`] is not `Clone` and the outcome rides a broadcast channel.
///
/// `Ok(())` means the stack *accepted* the action — submitted, or deliberately
/// dropped by a [`gated`](super::ExecutorExt::gated) /
/// [`deadline`](super::ExecutorExt::deadline) layer that returns `Ok`. It is
/// not a claim that the transaction landed on chain.
#[derive(Clone, Debug)]
pub struct ExecutionOutcome<A> {
    pub action: A,
    pub result: Result<(), String>,
}

/// `Report` wraps an [`Executor`] and publishes an [`ExecutionOutcome`] for
/// every action after submitting it, then returns the inner executor's result
/// unchanged. It is transparent — it never alters control flow — so it
/// composes anywhere in the reliability stack; place it outermost to report
/// the stack's final post-retry/post-fallback verdict. Reporting is
/// best-effort: a dropped receiver is logged and ignored, never failing the
/// submission.
pub struct Report<A> {
    executor: Box<dyn Executor<A>>,
    outcomes: Sender<ExecutionOutcome<A>>,
}

impl<A> Report<A> {
    /// Creates a new `Report` that publishes each action's verdict to
    /// `outcomes`.
    pub fn new(executor: Box<dyn Executor<A>>, outcomes: Sender<ExecutionOutcome<A>>) -> Self {
        Self { executor, outcomes }
    }
}

#[async_trait]
impl<A> Executor<A> for Report<A>
where
    A: Clone + Debug + Send + Sync + 'static,
{
    async fn execute(&mut self, action: A) -> Result<()> {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::executor_ext::{ExecutorExt, RetryPolicy};
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicU32, Ordering},
    };
    use tokio::sync::broadcast;

    /// Records every action it executes; always succeeds.
    struct RecordingExecutor {
        received: Arc<Mutex<Vec<u32>>>,
    }

    fn recording() -> (RecordingExecutor, Arc<Mutex<Vec<u32>>>) {
        let received = Arc::new(Mutex::new(Vec::new()));
        (
            RecordingExecutor {
                received: Arc::clone(&received),
            },
            received,
        )
    }

    #[async_trait]
    impl Executor<u32> for RecordingExecutor {
        async fn execute(&mut self, action: u32) -> Result<()> {
            self.received.lock().unwrap().push(action);
            Ok(())
        }
    }

    /// Fails its first `failures` executions, then succeeds.
    struct FlakyExecutor {
        failures: u32,
        attempts: Arc<AtomicU32>,
    }

    fn flaky(failures: u32) -> FlakyExecutor {
        FlakyExecutor {
            failures,
            attempts: Arc::new(AtomicU32::new(0)),
        }
    }

    #[async_trait]
    impl Executor<u32> for FlakyExecutor {
        async fn execute(&mut self, _action: u32) -> Result<()> {
            let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
            if attempt < self.failures {
                anyhow::bail!("transient failure {attempt}")
            }
            Ok(())
        }
    }

    /// Always fails with a fixed message.
    struct FailingExecutor;

    #[async_trait]
    impl Executor<u32> for FailingExecutor {
        async fn execute(&mut self, _action: u32) -> Result<()> {
            anyhow::bail!("submission rejected")
        }
    }

    #[tokio::test]
    async fn a_success_forwards_the_action_and_reports_ok() {
        let (executor, received) = recording();
        let (tx, mut rx) = broadcast::channel(8);
        let mut reporting = executor.report(tx);

        reporting.execute(7).await.unwrap();

        assert_eq!(*received.lock().unwrap(), vec![7], "the action reaches the inner executor");
        let outcome = rx.try_recv().expect("an outcome was published");
        assert_eq!(outcome.action, 7);
        assert!(outcome.result.is_ok());
    }

    #[tokio::test]
    async fn a_failure_returns_the_inner_error_and_reports_it() {
        let (tx, mut rx) = broadcast::channel(8);
        let mut reporting = FailingExecutor.report(tx);

        let err = reporting.execute(9).await.expect_err("the inner verdict propagates");
        assert_eq!(err.to_string(), "submission rejected");

        let outcome = rx.try_recv().expect("an outcome was published");
        assert_eq!(outcome.action, 9);
        assert_eq!(outcome.result.unwrap_err(), "submission rejected");
    }

    #[tokio::test]
    async fn reporting_is_best_effort_with_no_receiver() {
        let (tx, rx) = broadcast::channel(8);
        drop(rx); // no live receiver
        let (executor, received) = recording();
        let mut reporting = executor.report(tx);

        reporting
            .execute(7)
            .await
            .expect("a missing receiver must not fail the submission");
        assert_eq!(*received.lock().unwrap(), vec![7]);
    }

    #[tokio::test]
    async fn outermost_report_under_retry_reports_one_final_ok() {
        let (tx, mut rx) = broadcast::channel(8);
        // One transient failure, absorbed by retry; report is outermost.
        let mut stack = flaky(1)
            .retry(RetryPolicy {
                max_retries: 3,
                base_delay: std::time::Duration::from_millis(0),
            })
            .report(tx);

        stack.execute(5).await.unwrap();

        let outcome = rx.try_recv().expect("exactly one final outcome");
        assert_eq!(outcome.action, 5);
        assert!(outcome.result.is_ok(), "the final post-retry verdict is Ok");
        assert!(rx.try_recv().is_err(), "retry's internal failure is not reported");
    }
}
```

- [ ] **Step 2: Wire the module and extension method; run tests to verify they fail**

In `src/executor_ext.rs`, add the module and re-export (alphabetical — `report` goes after `rate_limit`):

```rust
mod rate_limit;
mod report;
mod retry;
```

```rust
pub use rate_limit::*;
pub use report::*;
pub use retry::*;
```

Add the extension method to the `ExecutorExt` trait (after `dry_run`):

```rust
    /// Publish each action's verdict to `outcomes` after submitting it, then
    /// return the inner executor's result unchanged. Transparent — it never
    /// alters control flow — so it composes anywhere; place it outermost to
    /// report the stack's final post-retry/post-fallback verdict. Pair it with
    /// a [`ChannelCollector`](crate::collectors::ChannelCollector) over the
    /// same channel to feed verdicts back to strategies as events. Reporting
    /// is best-effort: a dropped receiver is logged and ignored.
    fn report(
        self,
        outcomes: tokio::sync::broadcast::Sender<ExecutionOutcome<A>>,
    ) -> Report<A>
    where
        A: Clone,
    {
        Report::new(Box::new(self), outcomes)
    }
```

Run: `cargo test --lib executor_ext::report`
Expected: 4 tests FAIL (panic at `todo!()`).

- [ ] **Step 3: Implement `execute`**

Replace the `todo!()` body in `src/executor_ext/report.rs`:

```rust
    async fn execute(&mut self, action: A) -> Result<()> {
        let result = self.executor.execute(action.clone()).await;
        let outcome = ExecutionOutcome {
            action,
            result: result.as_ref().map(|_| ()).map_err(|e| format!("{e:#}")),
        };
        // Synchronous, non-blocking: a full channel drops the oldest outcome,
        // and no live receiver is a best-effort miss — never fail the
        // submission over reporting.
        if let Err(e) = self.outcomes.send(outcome) {
            tracing::debug!("no execution-outcome receiver; dropping verdict: {e}");
        }
        result
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib executor_ext::report`
Expected: 4 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/executor_ext/report.rs src/executor_ext.rs
git commit -m "Add ExecutionOutcome and the Report executor wrapper

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: `ChannelCollector`

**Files:**
- Create: `src/collectors/channel_collector.rs`
- Modify: `src/collectors/mod.rs`

- [ ] **Step 1: Write the failing tests**

Create `src/collectors/channel_collector.rs` with the struct and tests; `subscribe` stubbed with `todo!()`:

```rust
use crate::types::{Collector, CollectorStream};
use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::broadcast::Sender;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::BroadcastStream;

/// A [`Collector`] over an in-process [`broadcast`](tokio::sync::broadcast)
/// channel: each event sent to the channel becomes a collected event. It holds
/// the `Sender` (not a `Receiver`) so every `subscribe` mints a fresh receiver
/// — surviving the reconnect driver's re-subscription, where a single
/// `Receiver` could not. The seam through which execution feedback (an
/// [`ExecutionOutcome`](crate::executor_ext::ExecutionOutcome)) — or any
/// in-process source — re-enters the pipeline as events.
pub struct ChannelCollector<T> {
    sender: Sender<T>,
}

impl<T> ChannelCollector<T> {
    /// Creates a collector that emits every item sent to `sender`'s channel.
    pub fn new(sender: Sender<T>) -> Self {
        Self { sender }
    }
}

#[async_trait]
impl<T> Collector<T> for ChannelCollector<T>
where
    T: Clone + Send + Sync + 'static,
{
    async fn subscribe(&self) -> Result<CollectorStream<'_, T>> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::broadcast;

    #[tokio::test]
    async fn delivers_items_sent_after_subscribe() {
        let (tx, _rx) = broadcast::channel(8);
        let collector = ChannelCollector::new(tx.clone());
        let mut stream = collector.subscribe().await.unwrap();

        tx.send(1u32).unwrap();
        tx.send(2u32).unwrap();

        assert_eq!(stream.next().await, Some(1));
        assert_eq!(stream.next().await, Some(2));
    }

    #[tokio::test]
    async fn a_second_subscribe_works_where_a_receiver_could_not() {
        let (tx, _rx) = broadcast::channel(8);
        let collector = ChannelCollector::new(tx.clone());

        // First subscription, then dropped — as a lost stream would be.
        let first = collector.subscribe().await.unwrap();
        drop(first);

        // The reconnect driver re-subscribes; the new stream sees later items.
        let mut second = collector.subscribe().await.unwrap();
        tx.send(42u32).unwrap();
        assert_eq!(second.next().await, Some(42));
    }
}
```

- [ ] **Step 2: Wire the module; run tests to verify they fail**

In `src/collectors/mod.rs`, add (alphabetical — `channel_collector` before `event_collector`; with a doc line matching the file's style):

```rust
/// This collector turns an in-process broadcast channel into events.
mod channel_collector;
```

```rust
pub use block_collector::*;
pub use channel_collector::*;
pub use event_collector::*;
```

Run: `cargo test --lib collectors::channel_collector`
Expected: 2 tests FAIL (panic at `todo!()`).

- [ ] **Step 3: Implement `subscribe`**

Replace the `todo!()` body:

```rust
    async fn subscribe(&self) -> Result<CollectorStream<'_, T>> {
        let stream = BroadcastStream::new(self.sender.subscribe()).filter_map(|item| match item {
            Ok(item) => Some(item),
            Err(e) => {
                tracing::warn!("channel collector lagged: {e}");
                None
            }
        });
        Ok(Box::pin(stream))
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib collectors::channel_collector`
Expected: 2 tests PASS.

Note: `BroadcastStream` requires `T: Clone + Send + 'static`, satisfied by the impl bound. The `sync` feature on `tokio-stream` (already enabled in `Cargo.toml`) provides it.

- [ ] **Step 5: Commit**

```bash
git add src/collectors/channel_collector.rs src/collectors/mod.rs
git commit -m "Add ChannelCollector: an in-process broadcast source as a collector

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: End-to-end loop test

**Files:**
- Modify: `src/executor_ext/report.rs` (test module only)

- [ ] **Step 1: Write the loop test**

This proves the pieces compose: an outcome published by `report` is observed on a `ChannelCollector` stream over the same channel. Append to the `mod test` in `src/executor_ext/report.rs`:

```rust
    #[tokio::test]
    async fn report_and_channel_collector_close_the_loop() {
        use crate::collectors::ChannelCollector;
        use crate::types::Collector;
        use tokio_stream::StreamExt;

        let (tx, _rx) = broadcast::channel(8);

        // Collector side, subscribed before the submission so it sees it.
        let collector = ChannelCollector::new(tx.clone());
        let mut events = collector.subscribe().await.unwrap();

        // Executor side.
        let (executor, _received) = recording();
        let mut reporting = executor.report(tx);
        reporting.execute(123).await.unwrap();

        let outcome = events.next().await.expect("the verdict re-entered as an event");
        assert_eq!(outcome.action, 123);
        assert!(outcome.result.is_ok());
    }
```

- [ ] **Step 2: Run the test**

Run: `cargo test --lib executor_ext::report::test::report_and_channel_collector_close_the_loop`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add src/executor_ext/report.rs
git commit -m "Test the report -> channel collector feedback loop end to end

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: `feedback_example`

**Files:**
- Create: `examples/feedback_example.rs`
- Modify: `Cargo.toml`
- Modify: `examples/README.md`

- [ ] **Step 1: Write the example**

Create `examples/feedback_example.rs`. A strategy submits a trade per tick and stops re-submitting an id once it sees a successful outcome for it — the double-fire problem solved by feedback, not a cooldown:

```rust
//! Execution feedback: a strategy reacts to its own submissions. The executor
//! publishes an `ExecutionOutcome` per action via `ExecutorExt::report`; a
//! `ChannelCollector` over the same broadcast channel feeds those verdicts
//! back as events, and the strategy stops re-submitting a trade once it sees a
//! successful outcome for it — closing the loop without a blind cooldown. No
//! external node required.
//!
//! Run with:
//! ```sh
//! cargo run --example feedback_example
//! ```

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use artemis_light::{
    collectors::ChannelCollector,
    collector_ext::CollectorExt,
    engine::Engine,
    executor_ext::{ExecutionOutcome, ExecutorExt},
    types::{ActionStream, Collector, CollectorStream, Executor, Strategy},
};
use async_trait::async_trait;
use tokio::sync::broadcast;

/// A trade the strategy wants submitted.
#[derive(Clone, Debug)]
struct Trade {
    id: u64,
}

/// The umbrella event type: clock ticks plus fed-back submission verdicts.
#[derive(Clone, Debug)]
enum Event {
    Tick(u64),
    Outcome(ExecutionOutcome<Trade>),
}

/// Emits `count` sequential ticks on a fixed interval.
struct TickCollector {
    interval: Duration,
    count: u64,
}

#[async_trait]
impl Collector<u64> for TickCollector {
    async fn subscribe(&self) -> Result<CollectorStream<'_, u64>> {
        let interval = self.interval;
        let count = self.count;
        let stream = futures::stream::unfold(0u64, move |n| async move {
            if n >= count {
                return None;
            }
            tokio::time::sleep(interval).await;
            Some((n, n + 1))
        });
        Ok(Box::pin(stream))
    }
}

/// Re-submits trade id 1 on every tick until it sees a successful outcome for
/// it, then goes quiet — driven entirely by feedback.
struct PersistentStrategy {
    confirmed: HashSet<u64>,
}

#[async_trait]
impl Strategy<Event, Trade> for PersistentStrategy {
    async fn sync_state(&mut self) -> Result<()> {
        Ok(())
    }

    async fn process_event(&mut self, event: Event) -> Result<ActionStream<'_, Trade>> {
        match event {
            Event::Tick(_) if !self.confirmed.contains(&1) => {
                println!("[strategy] trade 1 unconfirmed; submitting");
                Ok(Box::pin(futures::stream::iter(vec![Trade { id: 1 }])))
            }
            Event::Tick(_) => {
                println!("[strategy] trade 1 confirmed; nothing to do");
                Ok(Box::pin(futures::stream::empty()))
            }
            Event::Outcome(o) => {
                if o.result.is_ok() {
                    println!("[strategy] outcome: trade {} confirmed", o.action.id);
                    self.confirmed.insert(o.action.id);
                } else {
                    println!("[strategy] outcome: trade {} failed; will retry", o.action.id);
                }
                Ok(Box::pin(futures::stream::empty()))
            }
        }
    }
}

/// Fails the first submission of each id, succeeds thereafter.
struct FlakyExecutor {
    seen: HashSet<u64>,
}

#[async_trait]
impl Executor<Trade> for FlakyExecutor {
    async fn execute(&mut self, trade: Trade) -> Result<()> {
        if self.seen.insert(trade.id) {
            anyhow::bail!("first submission of trade {} fails", trade.id)
        }
        println!("[executor] submitted trade {}", trade.id);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let (outcomes, _) = broadcast::channel::<ExecutionOutcome<Trade>>(64);

    let mut engine = Engine::<Event, Trade>::default();

    // Ticks, widened into the umbrella event type.
    engine.add_collector(Box::new(
        TickCollector {
            interval: Duration::from_millis(200),
            count: 5,
        }
        .map(Event::Tick),
    ));

    // Feedback: verdicts re-enter as events.
    engine.add_collector(Box::new(
        ChannelCollector::new(outcomes.clone()).map(Event::Outcome),
    ));

    engine.add_strategy(Box::new(PersistentStrategy {
        confirmed: HashSet::new(),
    }));

    engine.add_executor(Box::new(
        FlakyExecutor { seen: HashSet::new() }.report(outcomes),
    ));

    let mut handle = engine.run().await?;
    // Let the loop run a few ticks, then shut down cooperatively.
    tokio::time::sleep(Duration::from_millis(1200)).await;
    handle.token.cancel();
    while handle.tasks.join_next().await.is_some() {}

    println!("\nDone! Trade 1 failed once, then confirmed; the strategy went quiet.");
    Ok(())
}
```

- [ ] **Step 2: Register the example in `Cargo.toml`**

After the `onchain_example` block (Cargo.toml line 73), add:

```toml
[[example]]
name = "feedback_example"
path = "examples/feedback_example.rs"
```

- [ ] **Step 3: Run the example**

Run: `cargo run --example feedback_example`
Expected: output shows trade 1 submitted, the first attempt failing (`[strategy] outcome: trade 1 failed; will retry`), a later tick re-submitting, then `[executor] submitted trade 1`, `[strategy] outcome: trade 1 confirmed`, and subsequent ticks printing "nothing to do". Process exits on its own.

- [ ] **Step 4: Add the README table row**

In `examples/README.md`, add a row after the `reliability_example` row (line 14):

```markdown
| [`feedback_example`](feedback_example.rs) | Execution feedback: `ExecutorExt::report` publishes each action's verdict, a `ChannelCollector` feeds it back as an event, and the strategy stops re-submitting once a trade is confirmed | No |
```

- [ ] **Step 5: Commit**

```bash
git add examples/feedback_example.rs Cargo.toml examples/README.md
git commit -m "Add feedback_example: a strategy reacting to its own submissions

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 5: README and CONTEXT.md

**Files:**
- Modify: `README.md`
- Modify: `CONTEXT.md`

- [ ] **Step 1: Add the README "Execution feedback" subsection**

Insert after the "Observers" section in `README.md` (before "## Persistence"):

````markdown
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
````

- [ ] **Step 2: Add the CONTEXT.md terms**

Insert after the **Cooldown** entry (after CONTEXT.md line 70, before "## Relationships"):

```markdown
**Execution Outcome**:
The verdict the Executor stack reached for one action — the action plus `Ok(())` or `Err(message)` — fed back into the pipeline as an event. `Ok` means the stack *accepted* the action (submitted, or deliberately dropped by a **Gated**/**Deadline** layer), not that the transaction landed on chain. The error is stringified because it rides a broadcast channel.
_Avoid_: receipt, confirmation, result

**Report**:
The transparent Executor wrapper that publishes an **Execution Outcome** per action and then returns the inner executor's verdict unchanged, so it never alters control flow and composes anywhere in the reliability stack. Outermost, it reports the stack's final post-retry/post-fallback verdict.
_Avoid_: callback, hook, notify

**Channel Collector**:
A Collector over an in-process broadcast channel: it holds the Sender and mints a fresh receiver on every subscribe, so it survives the Collector Driver's re-subscription where a single receiver could not. The seam through which an **Execution Outcome** — or any in-process source — re-enters the pipeline as events.
_Avoid_: feedback channel, back-channel
```

- [ ] **Step 3: Add the relationships line**

In the "## Relationships" section of CONTEXT.md, add after the reliability-wrappers bullet (CONTEXT.md line 104):

```markdown
- A **Report** and a **Channel Collector** sharing one broadcast channel close the execution-feedback loop *without* a back-channel in the **Engine**: the Report publishes each verdict, the Channel Collector re-enters it through the normal Collector → Strategy path, and a Strategy reacts. The one-way topology is preserved — the loop is explicit caller wiring, not a hidden feedback edge.
```

- [ ] **Step 4: Commit**

```bash
git add README.md CONTEXT.md
git commit -m "Document execution feedback in README and CONTEXT

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 6: Full verification

- [ ] **Step 1: Format, lint, test**

```bash
cargo fmt --all -- --check
RUSTFLAGS="-Dwarnings" cargo clippy --all-features
cargo test --lib
```

Expected: all clean, all tests pass. `#![warn(unused_crate_dependencies)]` is satisfied (no new deps). If `fmt` complains, run `cargo fmt --all` and fold the change into the relevant commit.

- [ ] **Step 2: Build the example under the lint gate**

```bash
RUSTFLAGS="-Dwarnings" cargo clippy --example feedback_example
```

Expected: clean.

- [ ] **Step 3: Docs build sanity**

```bash
cargo doc --no-deps
```

Expected: no rustdoc warnings about the new intra-doc links (`ExecutionOutcome`, `ChannelCollector`, `ExecutorExt::report`, `gated`, `deadline`).

Note: the `gated`/`deadline` intra-doc links in `ExecutionOutcome`'s rustdoc resolve only if those methods exist. `gated` exists today; `deadline` ships on the `feat/action-deadline` branch. On this branch (`feat/execution-feedback`, cut from `master`), change the `deadline` reference in the `ExecutionOutcome` doc comment to plain text (no intra-doc link) to avoid a broken-link warning, or link only `gated`. Resolve the cross-reference when both features land on `master`.
