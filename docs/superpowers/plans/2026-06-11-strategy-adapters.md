# Strategy and Executor Adapter Combinators Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `StrategyExt::filter_map_event` / `map_action` and `ExecutorExt::filter_map_action` so strategies and executors written against narrow event/action types can be mounted into an engine using umbrella enums.

**Architecture:** Two new extension traits (`strategy_ext`, `executor_ext`) mirroring `collector_ext` exactly: blanket-implemented trait in `src/<name>_ext.rs`, one wrapper struct per combinator in `src/<name>_ext/<combinator>.rs`, each wrapper holding a boxed inner trait object plus a closure. Spec: `docs/superpowers/specs/2026-06-11-strategy-adapters-design.md`.

**Tech Stack:** Rust, `async_trait`, `anyhow`, `futures` (all already dependencies — no `Cargo.toml` dependency changes; note `lib.rs` has `#![warn(unused_crate_dependencies)]`).

## Discovery

**Similar implementations:** `src/collector_ext.rs` (extension trait + blanket impl `impl<T: Collector<E> + 'static, E> CollectorExt<E> for T {}`) with wrappers in `src/collector_ext/{map,filter_map,merge,chain}.rs`. `Map`/`FilterMap` hold `Box<dyn Collector<E>>` + closure; `Map`'s closure needs `Clone` because it's captured inside the returned stream.
**File conventions:** Module file `src/foo_ext.rs` declares `mod <combinator>;` + `pub use <combinator>::*;`; one struct per file with a `new(Box<dyn ...>, f)` constructor. `lib.rs` declares each module with a one-line doc comment.
**Testing patterns:** `#[cfg(test)] mod test` at the bottom of the `_ext.rs` file; small purpose-built doubles (`TestCollector`, `FailingCollector`, `CountingCollector` with `Arc<AtomicUsize>`); `#[tokio::test]`; descriptive sentence-style test names (`chain_fails_subscribe_when_any_source_fails`).
**Integration points:** `lib.rs` `pub mod` declarations; examples registered as `[[example]]` blocks in `Cargo.toml` AND a row in `examples/README.md`'s table; examples model on `examples/basic_example.rs` (self-contained, oneshot-based shutdown, no node needed).
**Project conventions:** CI runs `cargo fmt --all -- --check`, `cargo clippy --all-features`, `cargo test --all-features`, `cargo doc --no-deps`. Run all four before declaring any task complete. Strategy trait: `sync_state(&mut self) -> Result<()>`, `process_event(&mut self, E) -> Result<ActionStream<'_, A>>`; Executor trait: `execute(&mut self, A) -> Result<()>` (`src/types.rs`).
**Context loaded:** none — ad-hoc discovery.

## File Structure

```
src/
  strategy_ext.rs                    (new: StrategyExt trait + tests)
  strategy_ext/
    filter_map_event.rs              (new: FilterMapEvent wrapper)
    map_action.rs                    (new: MapAction wrapper)
  executor_ext.rs                    (new: ExecutorExt trait + tests)
  executor_ext/
    filter_map_action.rs             (new: FilterMapAction wrapper)
  lib.rs                             (modify: declare the two new modules)
examples/
  adapters_example.rs                (new: umbrella-enum pattern end-to-end)
  README.md                          (modify: add table row)
Cargo.toml                           (modify: add [[example]] block)
```

---

### Task 1: `StrategyExt::filter_map_event`

**Files:**
- Create: `src/strategy_ext.rs`
- Create: `src/strategy_ext/filter_map_event.rs`
- Modify: `src/lib.rs` (add module declaration after the `collector_ext` block at the end)

- [x] **Step 1: Create the module skeleton and wire it into lib.rs**

Create `src/strategy_ext.rs`:

```rust
use crate::types::Strategy;

mod filter_map_event;

pub use filter_map_event::*;

/// Extension trait that provides adapter combinators for types implementing
/// [`Strategy`].
///
/// The engine broadcasts one event type to every strategy, so multi-source
/// engines use umbrella enums. These adapters mount a strategy written
/// against its own narrow types into such an engine — the consumer-side dual
/// of [`CollectorExt::map`](crate::collector_ext::CollectorExt::map), which
/// widens narrow *sources* into the umbrella type.
pub trait StrategyExt<E, A>: Strategy<E, A> + Send + Sync + Sized + 'static {
    /// Mount this strategy into an engine with a wider event type `E2`: `f`
    /// projects each engine event down to this strategy's event type,
    /// returning `None` for events this strategy doesn't consume. A `None`
    /// event yields an empty action stream — it is not an error.
    fn filter_map_event<F, E2>(self, f: F) -> FilterMapEvent<E, A, F>
    where
        F: Fn(E2) -> Option<E> + Send + Sync + 'static,
    {
        FilterMapEvent::new(Box::new(self), f)
    }
}

impl<T: Strategy<E, A> + 'static, E, A> StrategyExt<E, A> for T {}
```

Add to the end of `src/lib.rs` (after the `pub mod collector_ext;` block):

```rust
/// This module contains syntax extensions for the `Strategy` trait.
pub mod strategy_ext;
```

- [x] **Step 2: Write the failing tests**

Append to `src/strategy_ext.rs`:

```rust
#[cfg(test)]
mod test {
    use super::StrategyExt;
    use crate::types::{ActionStream, Strategy};
    use anyhow::Result;
    use async_trait::async_trait;
    use futures::{StreamExt, stream};
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    /// The umbrella event type a multi-source engine would broadcast.
    #[derive(Clone, Debug)]
    enum Event {
        Num(u32),
        Text(String),
    }

    /// A narrow strategy: emits `n * 10` as its single action for event `n`.
    struct TimesTenStrategy;

    #[async_trait]
    impl Strategy<u32, u32> for TimesTenStrategy {
        async fn sync_state(&mut self) -> Result<()> {
            Ok(())
        }

        async fn process_event(&mut self, event: u32) -> Result<ActionStream<'_, u32>> {
            Ok(Box::pin(stream::iter(vec![event * 10])))
        }
    }

    /// Flags when `sync_state` reaches it, for proving delegation through
    /// wrappers.
    struct SyncProbe {
        synced: Arc<AtomicBool>,
    }

    #[async_trait]
    impl Strategy<u32, u32> for SyncProbe {
        async fn sync_state(&mut self) -> Result<()> {
            self.synced.store(true, Ordering::SeqCst);
            Ok(())
        }

        async fn process_event(&mut self, _event: u32) -> Result<ActionStream<'_, u32>> {
            Ok(Box::pin(stream::empty()))
        }
    }

    /// A strategy whose every method fails, for proving errors pass through
    /// wrappers unchanged.
    struct FailingStrategy;

    #[async_trait]
    impl Strategy<u32, u32> for FailingStrategy {
        async fn sync_state(&mut self) -> Result<()> {
            anyhow::bail!("sync failed")
        }

        async fn process_event(&mut self, _event: u32) -> Result<ActionStream<'_, u32>> {
            anyhow::bail!("process failed")
        }
    }

    #[tokio::test]
    async fn filter_map_event_routes_matching_events_to_the_inner_strategy() {
        let mut strategy = TimesTenStrategy.filter_map_event(|e: Event| match e {
            Event::Num(n) => Some(n),
            _ => None,
        });
        let actions = strategy
            .process_event(Event::Num(3))
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        assert_eq!(actions, vec![30]);
    }

    #[tokio::test]
    async fn filter_map_event_yields_an_empty_stream_not_an_error_for_unmatched_events() {
        let mut strategy = TimesTenStrategy.filter_map_event(|e: Event| match e {
            Event::Num(n) => Some(n),
            _ => None,
        });
        let actions = strategy
            .process_event(Event::Text("not for me".into()))
            .await
            .expect("an unmatched event is normal broadcast traffic, not an error")
            .collect::<Vec<_>>()
            .await;
        assert!(actions.is_empty());
    }

    #[tokio::test]
    async fn filter_map_event_delegates_sync_state_to_the_inner_strategy() {
        let synced = Arc::new(AtomicBool::new(false));
        let mut strategy = SyncProbe {
            synced: Arc::clone(&synced),
        }
        .filter_map_event(|e: Event| match e {
            Event::Num(n) => Some(n),
            _ => None,
        });
        strategy.sync_state().await.unwrap();
        assert!(synced.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn filter_map_event_propagates_inner_errors_unchanged() {
        let mut strategy = FailingStrategy.filter_map_event(|e: Event| match e {
            Event::Num(n) => Some(n),
            _ => None,
        });
        assert!(strategy.sync_state().await.is_err());
        assert!(strategy.process_event(Event::Num(1)).await.is_err());
    }
}
```

- [x] **Step 3: Run the tests to verify they fail to compile**

Run: `cargo test --all-features strategy_ext`
Expected: compile error — `filter_map_event.rs` module file does not exist yet (`file not found for module 'filter_map_event'`).

- [x] **Step 4: Implement `FilterMapEvent`**

Create `src/strategy_ext/filter_map_event.rs`:

```rust
use crate::types::{ActionStream, Strategy};
use anyhow::Result;
use async_trait::async_trait;

/// `FilterMapEvent` is a wrapper around a [`Strategy`] that projects a wider
/// event type `E2` down to the strategy's own event type `E`: events mapped
/// to `None` yield an empty action stream instead of reaching the strategy.
pub struct FilterMapEvent<E, A, F> {
    strategy: Box<dyn Strategy<E, A>>,
    f: F,
}

impl<E, A, F> FilterMapEvent<E, A, F> {
    /// Creates a new `FilterMapEvent` wrapping `strategy` with the projection
    /// function `f`.
    pub fn new(strategy: Box<dyn Strategy<E, A>>, f: F) -> Self {
        Self { strategy, f }
    }
}

#[async_trait]
impl<E, E2, A, F> Strategy<E2, A> for FilterMapEvent<E, A, F>
where
    E: Send + Sync + 'static,
    E2: Send + Sync + 'static,
    A: Send + Sync + 'static,
    F: Fn(E2) -> Option<E> + Send + Sync + 'static,
{
    async fn sync_state(&mut self) -> Result<()> {
        self.strategy.sync_state().await
    }

    async fn process_event(&mut self, event: E2) -> Result<ActionStream<'_, A>> {
        match (self.f)(event) {
            Some(event) => self.strategy.process_event(event).await,
            None => Ok(Box::pin(futures::stream::empty())),
        }
    }
}
```

- [x] **Step 5: Run the tests to verify they pass**

Run: `cargo test --all-features strategy_ext`
Expected: 4 tests pass.

- [x] **Step 6: Format, lint, commit**

```bash
cargo fmt --all
cargo clippy --all-features
git add src/strategy_ext.rs src/strategy_ext/filter_map_event.rs src/lib.rs
git commit -m "feat: add StrategyExt::filter_map_event adapter"
```

---

### Task 2: `StrategyExt::map_action`

**Files:**
- Create: `src/strategy_ext/map_action.rs`
- Modify: `src/strategy_ext.rs` (module decl, trait method, tests)

- [x] **Step 1: Write the failing tests**

Append to the `test` module in `src/strategy_ext.rs`. The umbrella action enum and `PairStrategy` double also serve the chained end-to-end test:

```rust
    /// The umbrella action type a multi-strategy engine would broadcast.
    #[derive(Clone, Debug, PartialEq)]
    enum Action {
        Submit(u32),
    }

    /// A narrow strategy emitting two actions per event: `n` and `n + 1`.
    struct PairStrategy;

    #[async_trait]
    impl Strategy<u32, u32> for PairStrategy {
        async fn sync_state(&mut self) -> Result<()> {
            Ok(())
        }

        async fn process_event(&mut self, event: u32) -> Result<ActionStream<'_, u32>> {
            Ok(Box::pin(stream::iter(vec![event, event + 1])))
        }
    }

    #[tokio::test]
    async fn map_action_transforms_every_action_preserving_order() {
        let mut strategy = PairStrategy.map_action(Action::Submit);
        let actions = strategy
            .process_event(7)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        assert_eq!(actions, vec![Action::Submit(7), Action::Submit(8)]);
    }

    #[tokio::test]
    async fn map_action_delegates_sync_state_and_propagates_errors() {
        let synced = Arc::new(AtomicBool::new(false));
        let mut probe = SyncProbe {
            synced: Arc::clone(&synced),
        }
        .map_action(Action::Submit);
        probe.sync_state().await.unwrap();
        assert!(synced.load(Ordering::SeqCst));

        let mut failing = FailingStrategy.map_action(Action::Submit);
        assert!(failing.sync_state().await.is_err());
        assert!(failing.process_event(1).await.is_err());
    }

    #[tokio::test]
    async fn filter_map_event_and_map_action_compose_end_to_end() {
        let mut strategy = PairStrategy
            .filter_map_event(|e: Event| match e {
                Event::Num(n) => Some(n),
                _ => None,
            })
            .map_action(Action::Submit);

        let actions = strategy
            .process_event(Event::Num(1))
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        assert_eq!(actions, vec![Action::Submit(1), Action::Submit(2)]);

        let skipped = strategy
            .process_event(Event::Text("not for me".into()))
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        assert!(skipped.is_empty());
    }
```

Update the test module's import line to include the new wrapper re-exports (no change needed — `super::StrategyExt` already brings the methods in; the wrappers are used only through inference).

- [x] **Step 2: Run the tests to verify they fail to compile**

Run: `cargo test --all-features strategy_ext`
Expected: compile error — no method named `map_action`.

- [x] **Step 3: Implement `MapAction` and the trait method**

Create `src/strategy_ext/map_action.rs`:

```rust
use crate::types::{ActionStream, Strategy};
use anyhow::Result;
use async_trait::async_trait;
use futures::StreamExt;

/// `MapAction` is a wrapper around a [`Strategy`] that lifts its actions into
/// a wider type `A2` — typically an umbrella-enum constructor.
pub struct MapAction<E, A, F> {
    strategy: Box<dyn Strategy<E, A>>,
    f: F,
}

impl<E, A, F> MapAction<E, A, F> {
    /// Creates a new `MapAction` wrapping `strategy` with the lifting
    /// function `f`.
    pub fn new(strategy: Box<dyn Strategy<E, A>>, f: F) -> Self {
        Self { strategy, f }
    }
}

#[async_trait]
impl<E, A, A2, F> Strategy<E, A2> for MapAction<E, A, F>
where
    E: Send + Sync + 'static,
    A: Send + Sync + 'static,
    A2: Send + Sync + 'static,
    F: Fn(A) -> A2 + Send + Sync + Clone + 'static,
{
    async fn sync_state(&mut self) -> Result<()> {
        self.strategy.sync_state().await
    }

    async fn process_event(&mut self, event: E) -> Result<ActionStream<'_, A2>> {
        // Cloned, not borrowed: the returned stream already holds the mutable
        // borrow of the inner strategy for its full lifetime.
        let f = self.f.clone();
        let stream = self.strategy.process_event(event).await?;
        Ok(Box::pin(stream.map(f)))
    }
}
```

In `src/strategy_ext.rs`, add the module declaration and re-export next to the existing ones:

```rust
mod filter_map_event;
mod map_action;

pub use filter_map_event::*;
pub use map_action::*;
```

And add the method to the `StrategyExt` trait body:

```rust
    /// Lift this strategy's actions into a wider action type `A2` — typically
    /// an umbrella-enum constructor: `.map_action(Action::Submit)`.
    fn map_action<F, A2>(self, f: F) -> MapAction<E, A, F>
    where
        F: Fn(A) -> A2 + Send + Sync + Clone + 'static,
    {
        MapAction::new(Box::new(self), f)
    }
```

- [x] **Step 4: Run the tests to verify they pass**

Run: `cargo test --all-features strategy_ext`
Expected: 7 tests pass.

- [x] **Step 5: Format, lint, commit**

```bash
cargo fmt --all
cargo clippy --all-features
git add src/strategy_ext.rs src/strategy_ext/map_action.rs
git commit -m "feat: add StrategyExt::map_action adapter"
```

---

### Task 3: `ExecutorExt::filter_map_action`

**Files:**
- Create: `src/executor_ext.rs`
- Create: `src/executor_ext/filter_map_action.rs`
- Modify: `src/lib.rs` (add module declaration after `strategy_ext`)

- [x] **Step 1: Create the module skeleton, wire lib.rs, write the failing tests**

Create `src/executor_ext.rs`:

```rust
use crate::types::Executor;

mod filter_map_action;

pub use filter_map_action::*;

/// Extension trait that provides adapter combinators for types implementing
/// [`Executor`].
///
/// The engine broadcasts every action to every executor, so an executor
/// written against its own narrow action type uses
/// [`filter_map_action`](ExecutorExt::filter_map_action) to route only the
/// actions it handles — the action-channel counterpart of
/// [`StrategyExt::filter_map_event`](crate::strategy_ext::StrategyExt::filter_map_event).
pub trait ExecutorExt<A>: Executor<A> + Send + Sync + Sized + 'static {
    /// Route only matching actions to this executor: `f` projects each engine
    /// action down to this executor's action type. A `None` action is skipped
    /// with `Ok(())` — the inner executor never sees it.
    fn filter_map_action<F, A2>(self, f: F) -> FilterMapAction<A, F>
    where
        F: Fn(A2) -> Option<A> + Send + Sync + 'static,
    {
        FilterMapAction::new(Box::new(self), f)
    }
}

impl<T: Executor<A> + 'static, A> ExecutorExt<A> for T {}

#[cfg(test)]
mod test {
    use super::ExecutorExt;
    use crate::types::Executor;
    use anyhow::Result;
    use async_trait::async_trait;
    use std::sync::{Arc, Mutex};

    /// The umbrella action type a multi-executor engine would broadcast.
    #[derive(Clone, Debug)]
    enum Action {
        Submit(u32),
        Log(String),
    }

    /// Records every action it executes, for proving routing and skipping.
    struct RecordingExecutor {
        received: Arc<Mutex<Vec<u32>>>,
    }

    #[async_trait]
    impl Executor<u32> for RecordingExecutor {
        async fn execute(&mut self, action: u32) -> Result<()> {
            self.received.lock().unwrap().push(action);
            Ok(())
        }
    }

    /// An executor whose execute always fails, for proving error passthrough.
    struct FailingExecutor;

    #[async_trait]
    impl Executor<u32> for FailingExecutor {
        async fn execute(&mut self, _action: u32) -> Result<()> {
            anyhow::bail!("execute failed")
        }
    }

    #[tokio::test]
    async fn filter_map_action_routes_matching_and_skips_unmatched_actions() {
        let received = Arc::new(Mutex::new(Vec::new()));
        let mut executor = RecordingExecutor {
            received: Arc::clone(&received),
        }
        .filter_map_action(|a: Action| match a {
            Action::Submit(n) => Some(n),
            _ => None,
        });

        executor.execute(Action::Submit(7)).await.unwrap();
        executor
            .execute(Action::Log("not for me".into()))
            .await
            .expect("a skipped action is Ok, not an error");

        assert_eq!(
            *received.lock().unwrap(),
            vec![7],
            "the skipped action must never reach the inner executor"
        );
    }

    #[tokio::test]
    async fn filter_map_action_propagates_inner_errors_unchanged() {
        let mut executor = FailingExecutor.filter_map_action(|a: Action| match a {
            Action::Submit(n) => Some(n),
            _ => None,
        });
        assert!(executor.execute(Action::Submit(1)).await.is_err());
        // A skipped action still succeeds even on a failing inner executor.
        assert!(executor.execute(Action::Log("skip".into())).await.is_ok());
    }
}
```

Add to `src/lib.rs` after the `strategy_ext` declaration:

```rust
/// This module contains syntax extensions for the `Executor` trait.
pub mod executor_ext;
```

- [x] **Step 2: Run the tests to verify they fail to compile**

Run: `cargo test --all-features executor_ext`
Expected: compile error — `filter_map_action.rs` module file does not exist yet.

- [x] **Step 3: Implement `FilterMapAction`**

Create `src/executor_ext/filter_map_action.rs`:

```rust
use crate::types::Executor;
use anyhow::Result;
use async_trait::async_trait;

/// `FilterMapAction` is a wrapper around an [`Executor`] that routes only
/// matching actions to it: actions mapped to `None` are skipped with
/// `Ok(())` and never reach the inner executor.
pub struct FilterMapAction<A, F> {
    executor: Box<dyn Executor<A>>,
    f: F,
}

impl<A, F> FilterMapAction<A, F> {
    /// Creates a new `FilterMapAction` wrapping `executor` with the routing
    /// function `f`.
    pub fn new(executor: Box<dyn Executor<A>>, f: F) -> Self {
        Self { executor, f }
    }
}

#[async_trait]
impl<A, A2, F> Executor<A2> for FilterMapAction<A, F>
where
    A: Send + Sync + 'static,
    A2: Send + Sync + 'static,
    F: Fn(A2) -> Option<A> + Send + Sync + 'static,
{
    async fn execute(&mut self, action: A2) -> Result<()> {
        match (self.f)(action) {
            Some(action) => self.executor.execute(action).await,
            None => Ok(()),
        }
    }
}
```

- [x] **Step 4: Run the tests to verify they pass**

Run: `cargo test --all-features executor_ext`
Expected: 2 tests pass.

- [x] **Step 5: Format, lint, commit**

```bash
cargo fmt --all
cargo clippy --all-features
git add src/executor_ext.rs src/executor_ext/filter_map_action.rs src/lib.rs
git commit -m "feat: add ExecutorExt::filter_map_action adapter"
```

---

### Task 4: `adapters_example.rs`

**Files:**
- Create: `examples/adapters_example.rs`
- Modify: `Cargo.toml` (add `[[example]]` block after the `combinators_example` block)
- Modify: `examples/README.md` (add table row after `combinators_example`)

- [x] **Step 1: Write the example**

Create `examples/adapters_example.rs`:

```rust
//! Composing narrow strategies and executors into one engine with the
//! adapter combinators: collectors are widened into an umbrella `Event` enum
//! with `CollectorExt::map`, strategies are mounted with
//! `StrategyExt::filter_map_event` + `map_action`, and executors are routed
//! with `ExecutorExt::filter_map_action`. No external node required.
//!
//! Run with:
//! ```sh
//! cargo run --example adapters_example
//! ```

use std::time::Duration;

use anyhow::Result;
use artemis_light::{
    collector_ext::CollectorExt,
    engine::Engine,
    executor_ext::ExecutorExt,
    strategy_ext::StrategyExt,
    types::{ActionStream, Collector, CollectorStream, Executor, Strategy},
};
use async_trait::async_trait;

/// The engine-wide umbrella event: every collector's output, widened.
#[derive(Clone, Debug)]
enum Event {
    Tick(u64),
    Price(f64),
}

/// The engine-wide umbrella action: every strategy's output, widened.
#[derive(Clone, Debug)]
enum Action {
    Submit(u64),
    Log(String),
}

/// Emits `count` sequential `u64` ticks on a fixed interval. Knows nothing
/// about the umbrella `Event` type.
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

/// Emits a fixed series of `f64` prices on an interval. Also narrow.
struct PriceCollector {
    interval: Duration,
    prices: Vec<f64>,
}

#[async_trait]
impl Collector<f64> for PriceCollector {
    async fn subscribe(&self) -> Result<CollectorStream<'_, f64>> {
        let interval = self.interval;
        let prices = self.prices.clone();
        let stream = futures::stream::unfold(prices.into_iter(), move |mut it| async move {
            let price = it.next()?;
            tokio::time::sleep(interval).await;
            Some((price, it))
        });
        Ok(Box::pin(stream))
    }
}

/// A narrow strategy over `u64` ticks: submits every even tick. Written and
/// testable without any knowledge of `Event` or `Action`.
struct EvenTickStrategy;

#[async_trait]
impl Strategy<u64, u64> for EvenTickStrategy {
    async fn sync_state(&mut self) -> Result<()> {
        Ok(())
    }

    async fn process_event(&mut self, tick: u64) -> Result<ActionStream<'_, u64>> {
        let actions = if tick % 2 == 0 { vec![tick] } else { vec![] };
        Ok(Box::pin(futures::stream::iter(actions)))
    }
}

/// A narrow strategy over `f64` prices: logs an alert above a threshold.
struct PriceAlertStrategy {
    threshold: f64,
}

#[async_trait]
impl Strategy<f64, String> for PriceAlertStrategy {
    async fn sync_state(&mut self) -> Result<()> {
        Ok(())
    }

    async fn process_event(&mut self, price: f64) -> Result<ActionStream<'_, String>> {
        let actions = if price > self.threshold {
            vec![format!("price {price} above threshold {}", self.threshold)]
        } else {
            vec![]
        };
        Ok(Box::pin(futures::stream::iter(actions)))
    }
}

/// A narrow executor for `u64` submissions; signals `done` after the expected
/// number of actions so the example can exit.
struct SubmitExecutor {
    remaining: u64,
    done: Option<tokio::sync::oneshot::Sender<()>>,
}

#[async_trait]
impl Executor<u64> for SubmitExecutor {
    async fn execute(&mut self, tick: u64) -> Result<()> {
        println!("[submit] tick {tick}");
        self.remaining = self.remaining.saturating_sub(1);
        if self.remaining == 0
            && let Some(done) = self.done.take()
        {
            let _ = done.send(());
        }
        Ok(())
    }
}

/// A narrow executor for log lines.
struct LogExecutor;

#[async_trait]
impl Executor<String> for LogExecutor {
    async fn execute(&mut self, line: String) -> Result<()> {
        println!("[log] {line}");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    const TICKS: u64 = 6;
    const EVEN_TICKS: u64 = 3; // ticks 0, 2, 4

    let (done_tx, done_rx) = tokio::sync::oneshot::channel();

    let mut engine = Engine::<Event, Action>::default();

    // Producer side: widen each narrow collector into the umbrella Event.
    engine.add_collector(Box::new(
        TickCollector {
            interval: Duration::from_millis(200),
            count: TICKS,
        }
        .map(Event::Tick),
    ));
    engine.add_collector(Box::new(
        PriceCollector {
            interval: Duration::from_millis(300),
            prices: vec![0.9, 1.4, 2.3],
        }
        .map(Event::Price),
    ));

    // Consumer side: project the umbrella Event down to each narrow
    // strategy, and lift each strategy's actions into the umbrella Action.
    engine.add_strategy(Box::new(
        EvenTickStrategy
            .filter_map_event(|e: Event| match e {
                Event::Tick(t) => Some(t),
                _ => None,
            })
            .map_action(Action::Submit),
    ));
    engine.add_strategy(Box::new(
        PriceAlertStrategy { threshold: 1.0 }
            .filter_map_event(|e: Event| match e {
                Event::Price(p) => Some(p),
                _ => None,
            })
            .map_action(Action::Log),
    ));

    // Route only matching umbrella Actions to each narrow executor.
    engine.add_executor(Box::new(
        SubmitExecutor {
            remaining: EVEN_TICKS,
            done: Some(done_tx),
        }
        .filter_map_action(|a: Action| match a {
            Action::Submit(t) => Some(t),
            _ => None,
        }),
    ));
    engine.add_executor(Box::new(LogExecutor.filter_map_action(
        |a: Action| match a {
            Action::Log(line) => Some(line),
            _ => None,
        },
    )));

    println!("Starting engine — two narrow strategies in one umbrella engine...\n");
    let mut handle = engine.run().await?;

    let _ = done_rx.await;
    handle.token.cancel();
    while handle.tasks.join_next().await.is_some() {}

    println!("\nDone!");
    Ok(())
}
```

- [x] **Step 2: Register the example**

Add to `Cargo.toml` after the `combinators_example` block:

```toml
[[example]]
name = "adapters_example"
path = "examples/adapters_example.rs"
```

Add to the table in `examples/README.md`, after the `combinators_example` row:

```markdown
| [`adapters_example`](adapters_example.rs) | Mounting narrow strategies and executors into an umbrella-enum engine with `StrategyExt::filter_map_event`/`map_action` and `ExecutorExt::filter_map_action` | No |
```

- [x] **Step 3: Run the example to verify it works**

Run: `cargo run --example adapters_example`
Expected: interleaved `[submit] tick 0/2/4` and `[log] price 1.4 ...` / `[log] price 2.3 ...` lines (order varies), ending with `Done!` and a clean exit.

- [x] **Step 4: Format, lint, commit**

```bash
cargo fmt --all
cargo clippy --all-features
git add examples/adapters_example.rs examples/README.md Cargo.toml
git commit -m "docs: add adapters_example demonstrating the umbrella-enum pattern"
```

---

### Task 5: Final verification

**Files:** none (verification only; fixes — if any — amend the relevant commit or land as a follow-up commit).

- [ ] **Step 1: Run the full CI gate locally**

```bash
cargo fmt --all -- --check
cargo clippy --all-features
cargo test --all-features
cargo doc --no-deps
```

Expected: all four succeed with no warnings. `cargo doc` matters here: the rustdoc cross-links (`CollectorExt::map`, `StrategyExt::filter_map_event`) must resolve — `docs.yml` CI builds docs and the crate denies rustdoc warnings.

- [ ] **Step 2: Confirm the spec is fully covered**

Check each spec requirement (docs/superpowers/specs/2026-06-11-strategy-adapters-design.md) against the code: three combinators with the spec'd signatures and semantics, empty-stream/Ok(()) filtering, sync_state delegation, error passthrough, chaining, example, README registration, lib.rs exports, rustdoc cross-links.
