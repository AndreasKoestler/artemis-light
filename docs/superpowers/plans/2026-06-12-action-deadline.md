# Action Deadline Executor Wrapper Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** An `Expires` trait plus a `Deadline` executor wrapper that drops actions whose freshness window has passed, instead of submitting them.

**Architecture:** One new combinator in the existing `executor_ext` family. The deadline travels with each action (strategies stamp it at pricing time); the wrapper checks it at every `execute` call and drops expired actions with `Ok(())`, the same drop shape as `Gated`. Innermost placement in the reliability stack means every queueing/waiting layer has elapsed before the check runs, and each `retry` attempt re-checks.

**Tech Stack:** Rust (edition 2024), tokio (paused-clock tests via `tokio::time::Instant`), async-trait, anyhow, tracing.

**Spec:** `docs/superpowers/specs/2026-06-12-action-deadline-design.md`

## Discovery

**Similar implementations:** `src/executor_ext/gated.rs` is the exact precedent — boxed inner executor, drop-with-`Ok(())` + tracing log, `Debug` bound on `A` for `?action` logging. `src/executor_ext/retry.rs` shows the paused-time test idiom.
**File conventions:** One file per combinator under `src/executor_ext/`, `mod` + `pub use` re-export in `src/executor_ext.rs` (alphabetical), extension method on `ExecutorExt` with rustdoc explaining placement in the stack.
**Testing patterns:** Unit tests in a `#[cfg(test)] mod test` inside the combinator file; sentence-style test names (`a_closed_gate_drops_actions_with_ok`); purpose-built test doubles (`RecordingExecutor`, `FlakyExecutor`); `#[tokio::test(start_paused = true)]` + `tokio::time::Instant` for anything time-dependent — no real sleeps.
**Integration points:** `ExecutorExt` is blanket-implemented for all `Executor<A> + 'static`, so wrappers chain freely. `CircuitBreaker` exposes `handle()` / `is_open()` / `reset()` (used in the breaker-interaction test). `examples/reliability_example.rs` is the showcase for the executor stack.
**Project conventions:** No pre-commit hook present; the verification gate is `cargo fmt --all -- --check`, `RUSTFLAGS="-Dwarnings" cargo clippy --all-features`, `cargo test --lib`. Commit messages are plain imperative sentences (no `feat:` prefixes). CONTEXT.md records every domain term; README "Combinators" section documents the extension traits.
**Context loaded:** none — ad-hoc discovery (CONTEXT.md, README.md, executor_ext sources, reliability example).

## File Structure

- Create: `src/executor_ext/deadline.rs` — `Expires` trait, `Deadline<A>` wrapper, unit tests (mirrors `gated.rs`)
- Modify: `src/executor_ext.rs` — `mod deadline;` + `pub use deadline::*;` + `ExecutorExt::deadline()` method
- Modify: `examples/reliability_example.rs` — Scene 5 demonstrating a deadline drop
- Modify: `README.md` — executor stack snippet in the Combinators section
- Modify: `CONTEXT.md` — **Deadline** term + relationships line

---

### Task 1: `Expires` trait, `Deadline` wrapper, core semantics

**Files:**
- Create: `src/executor_ext/deadline.rs`
- Modify: `src/executor_ext.rs`

- [ ] **Step 1: Write the failing tests**

Create `src/executor_ext/deadline.rs` with the trait/struct stubs and the full test module. The struct exists but `execute` is not yet implemented (use `todo!()`), so tests compile and fail:

```rust
use crate::types::Executor;
use anyhow::Result;
use async_trait::async_trait;
use std::fmt::Debug;
use tokio::time::Instant;

/// An action that knows when it goes stale. The strategy that priced the
/// opportunity stamps the deadline at creation; [`Deadline`] enforces it at
/// submission time. The clock is [`tokio::time::Instant`], so the paused-time
/// test harness controls "now" completely.
pub trait Expires {
    /// The instant after which this action must not be submitted.
    fn expires_at(&self) -> Instant;
}

/// `Deadline` is a wrapper around an [`Executor`] that drops expired actions
/// instead of submitting them. The check runs at every `execute` call, so
/// every delay layer *outside* the wrapper — channel backlog, a rate
/// limiter's wait, each retry backoff — has already elapsed by the time it
/// runs; place it innermost. An expired action is logged and dropped with
/// `Ok(())`, the same drop shape as [`Gated`](super::Gated): expiry is normal
/// operation, not a fault, so it neither trips a
/// [`CircuitBreaker`](super::CircuitBreaker) nor keeps a
/// [`Retry`](super::Retry) loop alive.
pub struct Deadline<A> {
    executor: Box<dyn Executor<A>>,
}

impl<A> Deadline<A> {
    /// Creates a new `Deadline` around `executor`.
    pub fn new(executor: Box<dyn Executor<A>>) -> Self {
        Self { executor }
    }
}

#[async_trait]
impl<A> Executor<A> for Deadline<A>
where
    A: Expires + Debug + Send + Sync + 'static,
{
    async fn execute(&mut self, action: A) -> Result<()> {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::executor_ext::ExecutorExt;
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicU32, Ordering},
    };
    use std::time::Duration;

    /// An action stamped with the freshness window it was priced against.
    #[derive(Clone, Debug)]
    struct TimedAction {
        id: u32,
        expires_at: Instant,
    }

    impl Expires for TimedAction {
        fn expires_at(&self) -> Instant {
            self.expires_at
        }
    }

    /// Live for a minute — far beyond anything a test advances past
    /// unintentionally.
    fn live(id: u32) -> TimedAction {
        TimedAction {
            id,
            expires_at: Instant::now() + Duration::from_secs(60),
        }
    }

    /// Expired on arrival: the check is `now >= expires_at`.
    fn expired(id: u32) -> TimedAction {
        TimedAction {
            id,
            expires_at: Instant::now(),
        }
    }

    /// Records the id of every action it executes.
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
    impl Executor<TimedAction> for RecordingExecutor {
        async fn execute(&mut self, action: TimedAction) -> Result<()> {
            self.received.lock().unwrap().push(action.id);
            Ok(())
        }
    }

    /// Fails every execution, counting attempts.
    struct FailingExecutor {
        attempts: Arc<AtomicU32>,
    }

    fn failing() -> (FailingExecutor, Arc<AtomicU32>) {
        let attempts = Arc::new(AtomicU32::new(0));
        (
            FailingExecutor {
                attempts: Arc::clone(&attempts),
            },
            attempts,
        )
    }

    #[async_trait]
    impl Executor<TimedAction> for FailingExecutor {
        async fn execute(&mut self, _action: TimedAction) -> Result<()> {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            anyhow::bail!("submission failed")
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_live_action_passes_through() {
        let (executor, received) = recording();
        executor.deadline().execute(live(7)).await.unwrap();
        assert_eq!(*received.lock().unwrap(), vec![7]);
    }

    #[tokio::test(start_paused = true)]
    async fn an_expired_action_is_dropped_with_ok() {
        let (executor, received) = recording();
        executor
            .deadline()
            .execute(expired(7))
            .await
            .expect("an expired action is dropped, not an error");
        assert!(received.lock().unwrap().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn an_action_expires_when_time_advances_past_its_deadline() {
        let (executor, received) = recording();
        let mut deadline = executor.deadline();
        let action = TimedAction {
            id: 7,
            expires_at: Instant::now() + Duration::from_secs(1),
        };

        deadline.execute(action.clone()).await.unwrap();
        tokio::time::advance(Duration::from_secs(2)).await;
        deadline.execute(action).await.unwrap();

        assert_eq!(
            *received.lock().unwrap(),
            vec![7],
            "only the pre-expiry submission reaches the inner executor"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn inner_errors_propagate_for_live_actions() {
        let (executor, attempts) = failing();
        let err = executor
            .deadline()
            .execute(live(7))
            .await
            .expect_err("a live action's failure is the inner executor's verdict");
        assert_eq!(err.to_string(), "submission failed");
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }
}
```

- [ ] **Step 2: Wire the module and extension method, run tests to verify they fail**

In `src/executor_ext.rs`, add the module declaration and re-export (alphabetical, after `circuit_breaker`):

```rust
mod circuit_breaker;
mod deadline;
mod fallback;
```

```rust
pub use circuit_breaker::*;
pub use deadline::*;
pub use fallback::*;
```

Add the extension method to the `ExecutorExt` trait, after `dry_run`:

```rust
    /// Drop actions whose deadline has passed instead of submitting them:
    /// the deadline travels with each action via [`Expires`], stamped by the
    /// strategy that priced it. Place it innermost in the reliability stack —
    /// the check runs at every `execute`, so every queueing or waiting layer
    /// outside has already elapsed, and each [`retry`](ExecutorExt::retry)
    /// attempt re-checks. An expired action is logged and dropped with
    /// `Ok(())`: invisible to `retry` and
    /// [`circuit_breaker`](ExecutorExt::circuit_breaker), because expiry is
    /// normal operation, not a fault.
    fn deadline(self) -> Deadline<A>
    where
        A: Expires,
    {
        Deadline::new(Box::new(self))
    }
```

Run: `cargo test --lib executor_ext::deadline -- --nocapture`
Expected: 4 tests FAIL (panic at `todo!()`).

- [ ] **Step 3: Implement `execute`**

Replace the `todo!()` body in `src/executor_ext/deadline.rs`:

```rust
    async fn execute(&mut self, action: A) -> Result<()> {
        if Instant::now() >= action.expires_at() {
            tracing::warn!(?action, "action expired; dropping without submission");
            return Ok(());
        }
        self.executor.execute(action).await
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib executor_ext::deadline`
Expected: 4 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/executor_ext/deadline.rs src/executor_ext.rs
git commit -m "Add Expires trait and Deadline executor wrapper

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: Composition semantics — `retry` and `circuit_breaker` interplay

**Files:**
- Modify: `src/executor_ext/deadline.rs` (test module only)

- [ ] **Step 1: Write the failing-or-passing composition tests**

These tests pin the spec's composition claims. Append to the `mod test` in `src/executor_ext/deadline.rs`:

```rust
    #[tokio::test(start_paused = true)]
    async fn an_action_expiring_mid_backoff_stops_the_retry_loop() {
        use crate::executor_ext::RetryPolicy;

        let (executor, attempts) = failing();
        let mut stack = executor.deadline().retry(RetryPolicy {
            max_retries: 3,
            base_delay: Duration::from_secs(1),
        });
        let action = TimedAction {
            id: 7,
            expires_at: Instant::now() + Duration::from_millis(1500),
        };

        stack
            .execute(action)
            .await
            .expect("expiry mid-backoff resolves Ok, not exhausted-retries Err");

        // Attempts at t=0 and t=1s are live and fail; the t=3s attempt finds
        // the action expired, returns Ok, and the retry loop stops — the
        // inner executor never sees a third attempt.
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn an_expired_drop_does_not_count_against_the_circuit_breaker() {
        let (executor, attempts) = failing();
        let breaker = executor.deadline().circuit_breaker(1);
        let operator = breaker.handle();
        let mut breaker = breaker;

        breaker.execute(expired(1)).await.unwrap();
        breaker.execute(expired(2)).await.unwrap();

        assert!(
            !operator.is_open(),
            "expired drops are Ok and must not trip the breaker"
        );
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            0,
            "expired actions never reach the failing inner executor"
        );
    }
```

- [ ] **Step 2: Run tests**

Run: `cargo test --lib executor_ext::deadline`
Expected: 6 tests PASS (the implementation from Task 1 already satisfies these; if either fails, the implementation — not the test — is wrong).

- [ ] **Step 3: Commit**

```bash
git add src/executor_ext/deadline.rs
git commit -m "Pin Deadline composition semantics with retry and circuit breaker

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: Deadline scene in the reliability example

**Files:**
- Modify: `examples/reliability_example.rs`

- [ ] **Step 1: Add the dated-trade types and Scene 5**

Update the import to include `Expires` (line 20):

```rust
    executor_ext::{ExecutorExt, Expires, RetryPolicy},
```

Update the module doc comment's wrapper list (line 2-3) to mention `deadline`:

```rust
//! The reliability layer for executors and the risk guards for strategies:
//! `ExecutorExt::retry`, `fallback`, `rate_limit`, `circuit_breaker`,
//! `deadline`, and `gated`/`dry_run` wrap a submission sink the way
//! `reconnect` guards a collector; `StrategyExt::filter_actions` and
//! `cooldown` keep the risk policy visible at composition time. No external
//! node required.
```

Add the types after `PrintingRpc` (after line 167):

```rust
/// A trade stamped with the freshness window it was priced against.
#[derive(Clone, Debug)]
struct DatedTrade {
    id: u64,
    expires_at: Instant,
}

impl Expires for DatedTrade {
    fn expires_at(&self) -> Instant {
        self.expires_at
    }
}

/// A sink that prints what it submits, for the deadline scene.
struct DatedRpc;

#[async_trait]
impl Executor<DatedTrade> for DatedRpc {
    async fn execute(&mut self, trade: DatedTrade) -> Result<()> {
        println!("[dated]    submitted trade {}", trade.id);
        Ok(())
    }
}
```

Add Scene 5 in `main`, after the rate-limit scene (before `println!("\nDone!")`):

```rust
    // ── Scene 5: the deadline drops stale actions ──────────────────────────
    // The strategy stamps the freshness window when it prices the trade; the
    // wrapper checks it at submission. Trade 401's window has already passed
    // (the check is `now >= expires_at`), so it is dropped — logged, Ok —
    // exactly like a gated-off action.
    println!("\n── deadline ──\n");
    let mut dated = DatedRpc.deadline();
    let now = Instant::now();
    dated
        .execute(DatedTrade {
            id: 400,
            expires_at: now + Duration::from_secs(1),
        })
        .await?;
    dated
        .execute(DatedTrade {
            id: 401,
            expires_at: now,
        })
        .await?;
    println!("trade 401 expired before submission: dropped (logged, Ok)");
```

- [ ] **Step 2: Run the example**

Run: `cargo run --example reliability_example`
Expected: existing scenes unchanged; Scene 5 prints `[dated]    submitted trade 400` then `trade 401 expired before submission: dropped (logged, Ok)`. Trade 401 must NOT print a `[dated]` submission line.

- [ ] **Step 3: Commit**

```bash
git add examples/reliability_example.rs
git commit -m "Demonstrate the deadline wrapper in the reliability example

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: README and CONTEXT.md

**Files:**
- Modify: `README.md` (Combinators section, after the collector snippet at line 54)
- Modify: `CONTEXT.md` (term after **Gated**, relationships line)

- [ ] **Step 1: Extend the README Combinators section**

Append to the code block ending at README line 54 (after the `merge` example), inside the same section:

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

with this prose immediately above the snippet:

```markdown
Executors compose the same way. Actions that implement `Expires` carry the
freshness window their strategy priced them against; the `deadline` wrapper
drops expired actions with `Ok`, so expiry neither trips the circuit breaker
nor keeps a retry loop alive:
```

- [ ] **Step 2: Add the CONTEXT.md term and relationship**

Insert after the **Gated** entry (after CONTEXT.md line 62):

```markdown
**Deadline**:
An Executor wrapper that drops actions whose freshness window has passed instead of submitting them. The deadline travels with the action (the `Expires` trait), stamped by the Strategy that priced it; the check runs at every execute, so inside a **Retry** each attempt re-checks and an action that expires mid-backoff stops the loop. An expired drop is `Ok` — invisible to **Retry** and **Circuit Breaker** — because expiry is normal operation, not a fault.
_Avoid_: TTL, expiry, timeout (the MempoolExecutor's `rpc_timeout` is a different thing)
```

Update the relationships line (CONTEXT.md line 104) to include Deadline and its placement:

```markdown
- The reliability wrappers (**Deadline**, **Retry**, **Fallback**, **Rate Limit**, **Circuit Breaker**, **Gated**) nest around one **Executor** and compose in any order, but order is meaningful: `retry` inside `fallback` retries the primary before failing over; `gated` outermost means a kill switch drops actions before any other layer sees them; `deadline` belongs innermost, so every queueing and waiting layer above it has already elapsed by the time the expiry check runs.
```

- [ ] **Step 3: Commit**

```bash
git add README.md CONTEXT.md
git commit -m "Document the Deadline wrapper in README and CONTEXT

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 5: Full verification

- [ ] **Step 1: Format, lint, test**

```bash
cargo fmt --all -- --check
RUSTFLAGS="-Dwarnings" cargo clippy --all-features
cargo test --lib
```

Expected: all clean, all tests pass. If `fmt` complains, run `cargo fmt --all` and amend the offending commit's files into a fixup commit.

- [ ] **Step 2: Run the full suite if anvil is available**

```bash
command -v anvil >/dev/null && cargo test --all-features || cargo test --lib
```

Expected: PASS. (Integration tests need `anvil`; skip gracefully if absent.)

- [ ] **Step 3: Docs build sanity**

```bash
cargo doc --no-deps
```

Expected: no rustdoc warnings about the new intra-doc links (`Expires`, `Gated`, `CircuitBreaker`, `Retry`).
