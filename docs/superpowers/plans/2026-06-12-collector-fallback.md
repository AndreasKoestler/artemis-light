# Collector-Side Fallback Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A `Fallback` collector combinator (plus `fallback_all` and `CollectorExt::fallback`) that subscribes a primary source, falling back to a backup on subscribe failure — primary-preferring and stateless.

**Architecture:** Mirrors `Merge`/`merge_all`/`CollectorExt::merge` exactly, but subscribes sources in order until one succeeds instead of subscribing all. One shared lifecycle; mid-stream failover rides the existing Reconnect Policy re-subscribe.

**Tech Stack:** Rust (edition 2024), async-trait, anyhow, tracing, futures.

**Spec:** `docs/superpowers/specs/2026-06-12-collector-fallback-design.md`

## Discovery

**Similar implementations:** `src/collector_ext/merge.rs` is the structural template (`Merge<C1, C2>` + `MergeAll<E>` + `merge_all`); `src/collector_ext/chain.rs` mirrors it. The executor `src/executor_ext/fallback.rs` is the naming/semantics sibling (primary → secondary on error, logs the primary's error).
**File conventions:** One file per combinator under `src/collector_ext/`, `mod` + `pub use` in `src/collector_ext.rs` (alphabetical), plus a method on the `CollectorExt` trait. Doc comments carry the full contract (eager vs lazy subscribe, lifecycle).
**Testing patterns:** Tests live in the `collector_ext.rs` `#[cfg(test)] mod test`, reusing `TestCollector`, `FailingCollector` (a `Collector<i32>` whose subscribe bails), `CountingCollector` (counts subscribes), and `PendingCollector`. Sentence-style test names. `merge_all_fails_subscribe_when_any_source_fails` is the closest existing shape.
**Integration points:** `CollectorExt` is blanket-implemented for all `Collector<E> + 'static`, so `fallback` chains with `map`/`merge`/etc. Examples register collector combinators in `examples/combinators_example.rs` and its `examples/README.md` row.
**Project conventions:** Verification gate: `cargo fmt --all -- --check`, `RUSTFLAGS="-Dwarnings" cargo clippy --all-features`, `cargo test --lib`. Commit messages are plain imperative sentences. No new dependencies.
**Context loaded:** none — ad-hoc discovery (merge.rs, chain.rs, executor_ext/fallback.rs, collector_ext.rs, combinators_example.rs).

## File Structure

- Create: `src/collector_ext/fallback.rs` — `Fallback<C1, C2>`, `FallbackAll<E>`, `fallback_all`
- Modify: `src/collector_ext.rs` — `mod fallback;` + `pub use fallback::*;` + `CollectorExt::fallback()` + tests
- Modify: `examples/combinators_example.rs` — a fallback scene
- Modify: `examples/README.md` — update the combinators row
- Modify: `README.md` — Combinators section snippet
- Modify: `CONTEXT.md` — extend the **Fallback** term + a relationship line

---

### Task 1: `Fallback` and `fallback_all`

**Files:**
- Create: `src/collector_ext/fallback.rs`
- Modify: `src/collector_ext.rs`

- [ ] **Step 1: Write the combinator (no tests yet — tests live in collector_ext.rs)**

Create `src/collector_ext/fallback.rs`, modelled on `merge.rs`:

```rust
use crate::types::{Collector, CollectorStream};
use anyhow::Result;
use async_trait::async_trait;

/// Subscribes a primary [Collector], falling back to a secondary if the
/// primary's subscribe fails.
///
/// Tries `this` first; on `Ok` it returns that stream and **never subscribes
/// `other`** (no wasted connection — unlike [`Merge`](super::Merge), which
/// subscribes every source). On `this`'s subscribe error it logs and tries
/// `other`. The combinator is stateless and primary-preferring: every
/// (re)subscribe tries `this` first, so a recovered primary is picked back up
/// automatically. Mid-stream failover happens through the Reconnect Policy's
/// re-subscribe, not state here. To the Engine the composite is one Collector —
/// one Collector Driver, one Reconnect Policy, one lifecycle.
pub struct Fallback<C1, C2> {
    this: C1,
    other: C2,
}

impl<C1, C2> Fallback<C1, C2> {
    /// Creates a new `Fallback` preferring `this`, falling back to `other`.
    pub fn new(this: C1, other: C2) -> Self {
        Self { this, other }
    }
}

#[async_trait]
impl<C1, C2, E> Collector<E> for Fallback<C1, C2>
where
    C1: Collector<E> + Send + Sync + 'static,
    C2: Collector<E> + Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    async fn subscribe(&self) -> Result<CollectorStream<'_, E>> {
        match self.this.subscribe().await {
            Ok(stream) => Ok(stream),
            Err(e) => {
                tracing::warn!("primary collector subscribe failed; falling back: {e:#}");
                self.other.subscribe().await
            }
        }
    }
}

/// [`Fallback`] over a runtime-sized, ordered set of sources; see [`fallback_all`].
pub struct FallbackAll<E> {
    sources: Vec<Box<dyn Collector<E>>>,
}

/// Tries each source in registration order, falling back to the next on a
/// subscribe error, with the same contract as [`Fallback`]: the first source
/// to subscribe wins and later sources are never subscribed; if every source
/// fails, the whole subscribe fails (feeding the Reconnect Policy). The
/// sources share one lifecycle (one Collector Driver, one Reconnect Policy).
pub fn fallback_all<E>(sources: Vec<Box<dyn Collector<E>>>) -> FallbackAll<E> {
    FallbackAll { sources }
}

#[async_trait]
impl<E: Send + Sync + 'static> Collector<E> for FallbackAll<E> {
    async fn subscribe(&self) -> Result<CollectorStream<'_, E>> {
        let mut last_err =
            anyhow::anyhow!("fallback_all has no sources; nothing to subscribe to");
        for source in &self.sources {
            match source.subscribe().await {
                Ok(stream) => return Ok(stream),
                Err(e) => {
                    tracing::warn!("collector subscribe failed; trying next fallback: {e:#}");
                    last_err = e;
                }
            }
        }
        Err(last_err)
    }
}
```

- [ ] **Step 2: Wire the module and trait method**

In `src/collector_ext.rs`, add the module and re-export (alphabetical, after `chain`):

```rust
mod chain;
mod fallback;
mod filter_map;
```

```rust
pub use chain::*;
pub use fallback::*;
pub use filter_map::*;
```

Add the method to the `CollectorExt` trait (after `chain`):

```rust
    /// Subscribe to this collector; if its subscribe fails, fall back to
    /// `other`. Prefers this collector on every (re)subscribe, and subscribes
    /// `other` only when this one fails. See [`Fallback`] for the full contract.
    fn fallback<C>(self, other: C) -> Fallback<Self, C>
    where
        C: Collector<E> + Send + Sync + 'static,
    {
        Fallback::new(self, other)
    }
```

- [ ] **Step 3: Build**

Run: `cargo build`
Expected: compiles cleanly.

- [ ] **Step 4: Commit**

```bash
git add src/collector_ext/fallback.rs src/collector_ext.rs
git commit -m "Add Fallback collector combinator and fallback_all

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: Tests

**Files:**
- Modify: `src/collector_ext.rs` (test module)

- [ ] **Step 1: Write the tests**

Append to the `#[cfg(test)] mod test` in `src/collector_ext.rs`. Import
`fallback_all` in the `use super::{...}` line (alongside `chain_all`,
`merge_all`). These reuse the existing `TestCollector`, `FailingCollector`, and
`CountingCollector` doubles:

```rust
    #[tokio::test]
    async fn fallback_uses_primary_and_never_subscribes_the_backup() {
        let subscribes = Arc::new(AtomicUsize::new(0));
        let composite = TestCollector::new(vec![1, 2, 3]).fallback(CountingCollector {
            subscribes: Arc::clone(&subscribes),
        });
        let stream = composite.subscribe().await.unwrap();
        assert_eq!(stream.collect::<Vec<_>>().await, vec![1, 2, 3]);
        assert_eq!(
            subscribes.load(Ordering::SeqCst),
            0,
            "a healthy primary must not subscribe the backup"
        );
    }

    #[tokio::test]
    async fn fallback_uses_backup_when_primary_subscribe_fails() {
        let composite = FailingCollector.fallback(TestCollector::new(vec![7, 8, 9]));
        let stream = composite.subscribe().await.unwrap();
        assert_eq!(stream.collect::<Vec<_>>().await, vec![7, 8, 9]);
    }

    #[tokio::test]
    async fn fallback_fails_subscribe_only_when_all_sources_fail() {
        let composite = FailingCollector.fallback(FailingCollector);
        assert!(composite.subscribe().await.is_err());
    }

    #[tokio::test]
    async fn fallback_all_returns_the_first_succeeding_source_in_order() {
        let subscribes = Arc::new(AtomicUsize::new(0));
        let sources: Vec<Box<dyn Collector<i32>>> = vec![
            Box::new(FailingCollector),
            Box::new(TestCollector::new(vec![1, 2])),
            Box::new(CountingCollector {
                subscribes: Arc::clone(&subscribes),
            }),
        ];
        let composite = fallback_all(sources);
        let stream = composite.subscribe().await.unwrap();
        assert_eq!(stream.collect::<Vec<_>>().await, vec![1, 2]);
        assert_eq!(
            subscribes.load(Ordering::SeqCst),
            0,
            "sources after the first success are never subscribed"
        );
    }

    #[tokio::test]
    async fn fallback_all_fails_when_every_source_fails() {
        let sources: Vec<Box<dyn Collector<i32>>> =
            vec![Box::new(FailingCollector), Box::new(FailingCollector)];
        assert!(fallback_all(sources).subscribe().await.is_err());
    }

    #[tokio::test]
    async fn fallback_all_with_no_sources_fails_subscribe() {
        let sources: Vec<Box<dyn Collector<i32>>> = vec![];
        assert!(fallback_all(sources).subscribe().await.is_err());
    }
```

- [ ] **Step 2: Run the tests**

Run: `cargo test --lib collector_ext::test::fallback`
Expected: 6 tests PASS.

- [ ] **Step 3: Commit**

```bash
git add src/collector_ext.rs
git commit -m "Test the Fallback collector combinator

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: Example

**Files:**
- Modify: `examples/combinators_example.rs`
- Modify: `examples/README.md`

- [ ] **Step 1: Add a fallback scene**

Update the module doc comment (after the `chain` bullet, line 8) to add:

```rust
//! - `fallback`: prefer a primary source, falling back to a backup if the
//!   primary's subscribe fails
```

Add `fallback_all` to the import if used; at minimum the `.fallback(..)` method
is available via `CollectorExt`. Add a `FailingCollector` local double near
`VecCollector` (the example has no failing double yet):

```rust
/// A collector whose subscribe always fails — a stand-in for a primary
/// endpoint that is currently down.
struct DownCollector;

#[async_trait]
impl Collector<String> for DownCollector {
    async fn subscribe(&self) -> Result<CollectorStream<'_, String>> {
        anyhow::bail!("primary endpoint is down")
    }
}
```

In `main`, add a fallback scene (adapt the event type to the example's existing
one; the snippet below uses `String` events — match whatever the example's
collectors already emit, or add a small dedicated section):

```rust
    // fallback: the primary endpoint is down, so the composite transparently
    // delivers the backup's events. A healthy primary would be used instead,
    // and the backup never subscribed.
    let resilient = DownCollector.fallback(VecCollector::new(vec![
        "backup-event-1".to_string(),
        "backup-event-2".to_string(),
    ]));
    let events: Vec<String> = resilient.subscribe().await?.collect().await;
    println!("fallback delivered from backup: {events:?}");
```

> Adjust the event type so it matches the collectors already in the example. If
> the example's combinators operate on a unified enum, make `DownCollector` and
> the backup produce that enum instead of `String`, and print accordingly.

- [ ] **Step 2: Run the example**

Run: `cargo run --example combinators_example`
Expected: existing output unchanged; the new line prints
`fallback delivered from backup: ["backup-event-1", "backup-event-2"]`.

- [ ] **Step 3: Update the examples README row**

In `examples/README.md`, update the `combinators_example` row (line 11) to
list `fallback`:

```markdown
| [`combinators_example`](combinators_example.rs) | Composing collectors with `CollectorExt`: `map`, `filter_map`, `merge`, `chain`, `fallback`, and the `merge_all`/`chain_all`/`fallback_all` list forms | No |
```

- [ ] **Step 4: Commit**

```bash
git add examples/combinators_example.rs examples/README.md
git commit -m "Demonstrate the fallback collector in the combinators example

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: README and CONTEXT.md

**Files:**
- Modify: `README.md`
- Modify: `CONTEXT.md`

- [ ] **Step 1: README Combinators snippet**

In the README "## Combinators" code block (ending line 54), append:

```rust
// Prefer a primary source, fall back to a backup if its subscribe fails
// (primary-preferring: each reconnect retries the primary first)
let collector = primary_ws_collector.fallback(backup_ws_collector);
```

- [ ] **Step 2: Extend the CONTEXT.md Fallback term**

The existing **Fallback** entry (CONTEXT.md line ~48) describes the executor
wrapper. Generalise it to cover both sides, the way **Retry** documents its two
meanings. Replace the entry with:

```markdown
**Fallback**:
Tries a primary and re-routes to a secondary on failure. Two duals:
- **Executor Fallback**: tries a primary Executor and re-submits the action to a secondary on a *submit* error — primary RPC → backup RPC, or private relay → public mempool. The primary's error is logged; only the fallback's verdict is returned.
- **Collector Fallback**: subscribes a primary Collector, falling back to a secondary on a *subscribe* error — primary WS → backup WS. Stateless and primary-preferring: every re-subscribe tries the primary first, so a recovered primary is picked back up automatically; the backup is subscribed only when the primary fails (unlike **Merge**, which subscribes every source). One shared lifecycle (one Collector Driver, one Reconnect Policy), like **Merge** and **Chain**.
_Avoid_: failover, backup executor/collector
```

- [ ] **Step 3: Add a relationship line**

In the "## Relationships" section of CONTEXT.md, after the Merge/Chain
lifecycle bullet (around line 98), add:

```markdown
- A **Collector Fallback** composite is one **Collector** to the **Engine**: mid-stream failover happens when the live stream ends and the **Collector Driver** re-subscribes — the combinator holds no health state, it just prefers the primary on every subscribe. Register sources separately if each should reconnect independently.
```

- [ ] **Step 4: Commit**

```bash
git add README.md CONTEXT.md
git commit -m "Document the collector Fallback in README and CONTEXT

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

Expected: clean; the 6 new fallback tests pass alongside the existing combinator tests.

- [ ] **Step 2: Build the example under the lint gate**

```bash
RUSTFLAGS="-Dwarnings" cargo clippy --example combinators_example
```

Expected: clean.

- [ ] **Step 3: Docs build**

```bash
cargo doc --no-deps
```

Expected: no rustdoc warnings on the new `Fallback`/`fallback_all`/`CollectorExt::fallback` intra-doc links (`Merge`, `Chain`).
