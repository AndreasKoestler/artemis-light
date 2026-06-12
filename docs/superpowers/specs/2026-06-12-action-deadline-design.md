# Action Deadline Executor Wrapper

**Date:** 2026-06-12
**Status:** Approved

## Problem

The reliability wrappers deliberately introduce delay and queuing: `rate_limit`
blocks an over-cap action, `retry` backs off between attempts, and the
broadcast action channel buffers behind a slow executor. An MEV action priced
against block N is stale — possibly toxic — a few blocks later, and nothing in
the pipeline drops it. The opportunity expires; the submission does not.

Only the strategy knows when an action goes stale: it priced the opportunity,
so it knows the freshness window. The library cannot infer a deadline after
the fact — by the time an executor wrapper first sees the action, an unknown
amount of channel queueing has already elapsed.

## Design

A new `Expires` trait that actions implement, and a `Deadline` executor
wrapper that drops expired actions instead of submitting them.

```rust
// src/executor_ext/deadline.rs
pub trait Expires {
    /// The instant after which this action must not be submitted.
    fn expires_at(&self) -> tokio::time::Instant;
}

// src/executor_ext.rs
pub trait ExecutorExt<A>: Executor<A> + Send + Sync + Sized + 'static {
    /// Drop actions whose deadline has passed instead of submitting them.
    /// Available only where `A: Expires`; the deadline travels with each
    /// action, stamped by the strategy that priced it.
    fn deadline(self) -> Deadline<A>
    where
        A: Expires;
}
```

The strategy stamps `expires_at` when it prices the action. The clock is
`tokio::time::Instant`, not `std::time::Instant`, so the paused-clock test
harness (`#[tokio::test(start_paused = true)]` + `tokio::time::advance`)
controls "now" completely — the same idiom the `retry` and `rate_limit` tests
already use. No real time passes in any test.

### Semantics

- `now < expires_at`: delegate to the inner executor untouched.
- `now >= expires_at`: log at `warn` and drop with `Ok(())` — the same drop
  shape as `Gated`. Expiry is normal operation, not a fault: returning `Err`
  would count against a `circuit_breaker` and make `retry` re-attempt an
  action that can only get staler.
- The check runs at every `execute` call, so inside a `retry` it re-runs per
  attempt: an action that expires mid-backoff returns `Ok` on the next
  attempt and the retry loop stops cleanly.
- No configuration on the wrapper itself — the deadline travels with each
  action.

### Composition: innermost placement

The check runs when `execute` is called, so every delay layer *outside* the
wrapper has already elapsed by the time it runs. `deadline` therefore belongs
innermost — applied first, just outside the raw executor:

```rust
mempool_executor
    .deadline()        // checked last, after all queueing/waiting above
    .retry(policy)     // each backoff attempt re-checks via the inner deadline
    .rate_limit(5)     // post-wait expiry is caught underneath
    .circuit_breaker(3)
    .gated(flag)
```

Innermost placement means the stamp-to-check span covers everything: the
broadcast-channel backlog (the stamp was made at strategy time), the rate
limiter's wait, and each retry backoff. This guidance goes in the rustdoc,
README, and CONTEXT.md's relationships section, mirroring the existing
"order is meaningful" notes for the reliability stack.

### Wrapper struct

One file per combinator, matching the existing family:

| Struct | File | Implements | Core of impl |
|---|---|---|---|
| `Deadline<A>` | `src/executor_ext/deadline.rs` | `Executor<A>` where `A: Expires` | expired → `warn` + `Ok(())`; else delegate |

Holds only `Box<dyn Executor<A>>`. `Expires` lives in the same file and is
re-exported from `executor_ext`.

## Testing

Unit tests in `src/executor_ext/deadline.rs` with paused tokio time:

- A not-yet-expired action passes through to the inner executor.
- An expired action returns `Ok(())` and never reaches the inner executor
  (counting executor proves it).
- Inner errors propagate unchanged for live actions.
- `deadline().retry(..)`: an action that expires mid-backoff stops the retry
  loop — the inner executor sees exactly the pre-expiry attempts.
- `deadline().circuit_breaker(..)`: an expired drop does not increment the
  breaker's failure count.

## Docs and example

- README combinator section gains `deadline` alongside the reliability
  wrappers, with the innermost-placement rationale.
- CONTEXT.md gains a **Deadline** term (avoid: TTL, expiry, timeout — the
  `MempoolExecutor`'s `rpc_timeout` is a different thing) and a relationship
  line: innermost in the reliability stack; expiry drops are `Ok`, invisible
  to `retry` and `circuit_breaker`.
- `examples/reliability_example.rs` gains a deadline layer in its stack.

## Out of scope

- Block-height deadlines (`valid_until_block`): would require a current-block
  source in the wrapper — I/O the executor-wrapper family deliberately
  avoids. A strategy can convert blocks to a wall-clock window itself.
- A wrapper-stamped TTL variant (no trait): rejected — the clock would start
  at executor receipt, missing the channel backlog that precedes it.
- Stamping helpers on the strategy side: the strategy constructs its own
  action type; it can set the field directly.
