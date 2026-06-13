# Execution Feedback

**Date:** 2026-06-12
**Status:** Approved

## Problem

Submission is fire-and-forget. `MempoolExecutor` discards the pending
transaction, and a strategy never learns whether the action it produced was
submitted, failed, or dropped by a wrapper. For a liquidation bot, not knowing
your liquidation went through means double-firing the next time the same
opportunity appears; `cooldown` is a blunt time-based workaround for a question
that has a real answer.

The engine's topology is deliberately one-way: collectors → strategies →
executors over broadcast channels, and CONTEXT.md records that executors are
sinks and observers "produce nothing." Execution feedback must not smuggle a
hidden back-channel into that topology. Instead it closes the loop the same way
any fact enters the pipeline — as an event from a collector — with the wiring
explicit at the call site.

## Scope

This design reports the **submission verdict**: whether the executor stack
returned `Ok` or `Err` for an action, after all reliability layers
(`retry`, `fallback`, …) have run. It is generic over any executor and action
type and adds no provider dependency. On-chain confirmation (did the tx mine,
revert, or get evicted) is explicitly out of scope — see below.

## Design

Three composable pieces. None changes the engine, the `Executor` trait, or the
`Strategy` trait.

### 1. `ExecutionOutcome<A>` (in `executor_ext`)

```rust
/// The verdict the executor stack reached for one action, fed back into the
/// pipeline as an event. `result` is `Ok(())` when the stack accepted the
/// action and `Err(message)` when it failed; the error is stringified because
/// `anyhow::Error` is not `Clone` and the outcome rides a broadcast channel.
#[derive(Clone, Debug)]
pub struct ExecutionOutcome<A> {
    pub action: A,
    pub result: Result<(), String>,
}
```

`Ok(())` means "the stack accepted the action" — submitted, or deliberately
dropped by a `gated`/`deadline` layer that returns `Ok`. It is **not** a claim
that the transaction landed on chain. This is the inherent ceiling of a
verdict-level signal and is documented as such.

### 2. `Report<A>` executor wrapper + `ExecutorExt::report`

```rust
// src/executor_ext/report.rs
pub struct Report<A> {
    executor: Box<dyn Executor<A>>,
    outcomes: tokio::sync::broadcast::Sender<ExecutionOutcome<A>>,
}

// src/executor_ext.rs
pub trait ExecutorExt<A>: Executor<A> + Send + Sync + Sized + 'static {
    /// Publish the verdict for each action to `outcomes` after submitting it,
    /// then return the inner executor's result unchanged. Transparent: it
    /// never alters control flow, so it composes anywhere in the reliability
    /// stack. Place it outermost to report the stack's final
    /// post-retry/post-fallback verdict. Reporting is best-effort — a dropped
    /// receiver is logged and ignored, never failing the submission.
    fn report(self, outcomes: broadcast::Sender<ExecutionOutcome<A>>) -> Report<A>
    where
        A: Clone;
}
```

`execute` semantics:

1. Run `self.executor.execute(action.clone())`.
2. Build `ExecutionOutcome { action, result: res.as_ref().map(|_| ()).map_err(|e| format!("{e:#}")) }`.
3. `outcomes.send(outcome)` — synchronous and non-blocking; a send error
   (no live receivers) is logged at `debug` and dropped.
4. Return the original `res` unchanged.

`broadcast::Sender::send` does not await and applies no backpressure to the
executor: a full channel drops the oldest queued outcome, exactly the
lagging-consumer semantics every other channel in the engine has. The action
is cloned (the engine already requires `A: Clone`) so the outcome can carry it
while the original goes to the inner executor.

### 3. `ChannelCollector<T>` (in `collectors`)

```rust
// src/collectors/channel_collector.rs
pub struct ChannelCollector<T> {
    sender: tokio::sync::broadcast::Sender<T>,
}

impl<T: Clone + Send + 'static> ChannelCollector<T> {
    pub fn new(sender: broadcast::Sender<T>) -> Self { Self { sender } }
}

#[async_trait]
impl<T: Clone + Send + Sync + 'static> Collector<T> for ChannelCollector<T> {
    async fn subscribe(&self) -> Result<CollectorStream<'_, T>> {
        let rx = self.sender.subscribe();
        // BroadcastStream yields Err(Lagged(n)) on overrun; log and skip,
        // the same best-effort semantics as every other consumer.
        let stream = BroadcastStream::new(rx).filter_map(|item| match item {
            Ok(item) => Some(item),
            Err(e) => {
                tracing::warn!("channel collector lagged: {e}");
                None
            }
        });
        Ok(Box::pin(stream))
    }
}
```

Holding the `Sender` (not a `Receiver`) is what makes this a `Collector`:
`subscribe(&self)` may be called more than once (the reconnect driver
re-subscribes after a lost stream), and each call mints a fresh receiver via
`sender.subscribe()`. A `Receiver` could only be handed out once. This mirrors
the engine's own broadcast idiom and makes `ChannelCollector` independently
useful: any in-process broadcast source becomes an event stream.

`ChannelCollector` is deliberately **not** `PersistableCollector`: a feedback
stream has no block number and no historical range to query.

### Closing the loop

```rust
use tokio::sync::broadcast;

let (outcomes, _) = broadcast::channel(256);

engine.add_executor(Box::new(
    mempool_executor
        .retry(RetryPolicy::default())
        .report(outcomes.clone()),          // outermost: the final verdict
));

engine.add_collector(Box::new(
    ChannelCollector::new(outcomes).map(Event::Outcome),  // widen into E
));
```

The existing `CollectorExt::map` widens `ExecutionOutcome<A>` into the engine's
umbrella event type `E`. The verdict re-enters through the normal
collector → strategy path; the strategy matches `Event::Outcome(o)` and reacts.
As long as a `Report` (or the caller) holds a `Sender` clone, the channel stays
open; when every sender drops, the `ChannelCollector` stream ends like any
collector whose source closed, feeding the reconnect policy.

## Testing

Unit tests, paused time not required (no delays involved):

`src/executor_ext/report.rs`:
- A successful submission forwards the action to the inner executor and the
  returned verdict is `Ok`; an `ExecutionOutcome` with `result: Ok(())`
  carrying that action is received on the channel.
- A failing submission returns the inner `Err` unchanged, and the emitted
  outcome's `result` is `Err` with the inner error's message.
- With no live receiver, `execute` still returns the inner verdict (best-effort
  reporting never fails the submission).
- `report` composed under `retry`: a transient failure that `retry` absorbs
  reports a single final `Ok` outcome when `report` is outermost.

`src/collectors/channel_collector.rs`:
- Items sent before and after `subscribe` are observed on the stream.
- A second `subscribe()` (simulating reconnect) yields items sent after it —
  proving re-subscription works where a `Receiver` could not.

End-to-end (unit level, no engine): `inner.report(tx)` → `ChannelCollector::new(tx)`
→ subscribe → one `execute` → the outcome appears on the collector stream.

## Example

`examples/feedback_example.rs`, self-contained (no RPC), registered in
`Cargo.toml` and `examples/README.md`:

- `enum Event { Tick(u64), Outcome(ExecutionOutcome<Trade>) }`.
- A strategy submits a `Trade` on each tick and **stops re-submitting a trade
  id once it sees a successful `Outcome` for it** — the double-fire problem the
  feature exists to solve, solved by feedback rather than a blind cooldown.
- An executor that fails a trade's first attempt and succeeds on the next, so
  the loop visibly drives the strategy's state.
- Wires `report(outcomes)` on the executor and
  `ChannelCollector::new(outcomes).map(Event::Outcome)` as a second collector.

## Docs

- README: new "Execution feedback" subsection after "Observers", showing the
  closed loop and stating the verdict-vs-on-chain ceiling.
- CONTEXT.md: three terms —
  - **Execution Outcome**: the verdict (`Ok`/`Err` message + the action) fed
    back as an event; `Ok` means the stack accepted the action, not that it
    landed on chain. _Avoid_: receipt, confirmation, result.
  - **Report**: the transparent Executor wrapper that publishes an Execution
    Outcome per action and returns the inner verdict unchanged; outermost
    reports the stack's final verdict. _Avoid_: callback, hook, notify.
  - **Channel Collector**: a Collector over an in-process broadcast Sender,
    minting a fresh receiver per subscribe so it survives reconnect; the seam
    through which feedback (or any in-process source) re-enters as events.
    _Avoid_: feedback channel, back-channel.
  - Relationships: a **Report** and a **Channel Collector** sharing one
    broadcast channel close the execution-feedback loop *without* a back-channel
    in the engine — the loop is explicit caller wiring through the normal
    collector → strategy path, preserving the one-way topology.

## Out of scope

- **On-chain confirmation** (mined / reverted / evicted): requires surfacing tx
  hashes from the executor and a receipt-polling component with provider I/O — a
  separate, larger design that can build on this verdict mechanism later.
- Typed error payloads: the outcome stringifies the error. A strategy needs the
  action identity and success/failure; structured error matching is YAGNI here.
- Engine-level feedback wiring (auto-routing outcomes back as events): rejected
  to keep the topology one-way and the loop explicit at the call site.
