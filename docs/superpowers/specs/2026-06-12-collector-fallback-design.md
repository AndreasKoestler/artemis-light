# Collector-Side Fallback

**Date:** 2026-06-12
**Status:** Approved

## Problem

The executor side has `fallback` (primary RPC → backup RPC), but the ingest
side has no equivalent. A collector's only resilience is the Reconnect Policy,
which retries the *same* endpoint with backoff until it escalates to Fatal. A
bot whose primary WebSocket provider goes down has no way to keep ingesting
from a backup endpoint — it just backs off against the dead primary until it
gives up. Provider outages are at least as common on the ingest side as on the
submission side.

## Design

A `Fallback<C1, C2>` collector combinator, a `fallback_all` list form, and a
`CollectorExt::fallback` method — mirroring the `Merge` / `merge_all` /
`CollectorExt::merge` trio in structure, naming, and the shared-lifecycle
contract.

```rust
// src/collector_ext.rs
pub trait CollectorExt<E>: Collector<E> + Send + Sync + Sized + 'static {
    /// Subscribe to this collector; if its subscribe fails, fall back to
    /// `other`. Prefers this collector on every (re)subscribe. See [`Fallback`]
    /// for the full contract.
    fn fallback<C>(self, other: C) -> Fallback<Self, C>
    where
        C: Collector<E> + Send + Sync + 'static;
}
```

### Subscribe semantics

`Fallback::subscribe` tries its sources in order:

1. `this.subscribe()` — on `Ok`, return that stream and **do not subscribe the
   backup** (no wasted connection; the key difference from `Merge`, which
   subscribes every source).
2. On `this`'s `Err`, log it at `warn` (mirroring how the executor `fallback`
   logs the primary's error) and try `other.subscribe()`.
3. Return the first source that succeeds; if *all* fail, return the last error.

A failed composite subscribe is indistinguishable, to the Engine, from any
single collector's failed subscribe: it feeds the Reconnect Policy's counter
exactly as today.

### Stateless, primary-preferring

The combinator holds no health state. Every `subscribe` call tries the primary
first. This is correct because of how the Engine already works:

- **Within one subscribe**, a primary outage fails over to the backup
  immediately — no reconnect-backoff cycle is spent.
- **Mid-stream**, when the primary's live stream dies, the composite stream
  ends; the Collector Driver re-subscribes the composite; `subscribe` tries the
  primary first again. A recovered primary is picked straight back up; a still-
  down primary yields the backup again.

No sticky-to-backup state is needed, and none is added — it would require
interior mutability on `&self` and a recovery policy, fighting the stateless
re-subscribe model the Engine already relies on.

### One shared lifecycle

Like `Merge` and `Chain`, the composite is one Collector to the Engine: one
Collector Driver, one Reconnect Policy, one lifecycle. The Reconnect Policy
counts a composite-subscribe failure (all sources down) and escalates to Fatal
as for any collector. Register the sources as separate collectors instead if
each should reconnect — and go Fatal — independently; but internal failover is
the entire point of `Fallback`, so a shared lifecycle is the right default.

### Structs

Mirroring `merge.rs`:

```rust
// src/collector_ext/fallback.rs
pub struct Fallback<C1, C2> { this: C1, other: C2 }

impl<C1, C2> Fallback<C1, C2> {
    pub fn new(this: C1, other: C2) -> Self { Self { this, other } }
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

/// `Fallback` over a runtime-sized, ordered set of sources.
pub fn fallback_all<E>(sources: Vec<Box<dyn Collector<E>>>) -> FallbackAll<E>;
```

`FallbackAll::subscribe` tries each source in registration order, logging and
skipping the ones whose subscribe errors, returning the first success or the
last error if all fail. An empty source list is a configuration error and
fails the subscribe with a clear message (rather than producing a silent
never-yielding stream).

## Testing

Unit tests in the `collector_ext.rs` test module, reusing the existing
`TestCollector` / `FailingCollector` / `CountingCollector` doubles:

- **Primary success returns the primary's events**, and the backup is never
  subscribed: a `CountingCollector` backup shows `0` subscribes after the
  composite subscribe succeeds via a `TestCollector` primary.
- **Primary subscribe-failure yields the backup's stream**:
  `FailingCollector.fallback(TestCollector::new(vec![..]))` delivers the
  backup's events.
- **Subscribe fails only when all sources fail**:
  `FailingCollector.fallback(FailingCollector)` returns `Err` (feeding the
  Reconnect Policy).
- **`fallback_all` returns the first succeeding source in order**, skipping
  earlier failures: `[Failing, Test(vec![1,2]), Test(vec![3])]` yields `1,2`
  (the second source — the third is never reached).
- **`fallback_all` fails only when every source fails**, and an empty list
  fails the subscribe with the configuration-error message.

## Example

`examples/combinators_example.rs` gains a `fallback` scene: a primary collector
whose subscribe fails and a healthy backup, showing the composite delivering
the backup's events. Its `examples/README.md` row is updated to list
`fallback`/`fallback_all` alongside `merge`/`chain`.

## Docs

- README "Combinators" section: a `fallback` snippet next to `merge`/`map`,
  noting primary-preferring failover and the shared lifecycle.
- CONTEXT.md: extend the existing **Fallback** term to cover the collector-side
  dual, the way **Retry** documents its collector-vs-executor meanings:
  - Executor **Fallback** (existing): primary executor → secondary on a submit
    error.
  - Collector **Fallback** (new): subscribe a primary collector, falling back
    to a secondary on a *subscribe* error; stateless and primary-preferring, so
    each re-subscribe retries the primary first; the backup is subscribed only
    when needed (unlike **Merge**, which subscribes all sources). One shared
    lifecycle (one Collector Driver, one Reconnect Policy), like **Merge** and
    **Chain**.
  - Add a relationship line: a **Fallback** composite is one Collector to the
    Engine; mid-stream failover happens through the normal Reconnect Policy
    re-subscribe, not through state inside the combinator.

## Out of scope

- **Sticky-to-backup / per-source health tracking**: rejected in favour of the
  stateless primary-preferring model that fits the Engine's re-subscribe loop.
- **Per-source independent reconnect within one composite**: register the
  sources as separate collectors for that; `Fallback` is deliberately one
  lifecycle.
- **Mid-stream switchover without ending the stream** (hot-swapping the live
  source under the subscriber): the Reconnect Policy's re-subscribe already
  provides failover at stream boundaries; an in-stream swap would need source
  identity and buffering machinery not justified here.
