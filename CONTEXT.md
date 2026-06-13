# artemis-light

Domain language for a framework for reliable, long-running on-chain automation: event-driven agents that watch a chain, decide, and act through an event-processing pipeline (Collectors → Strategies → Executors, wired by the Engine). Records the terms a maintainer must share to talk about the pipeline precisely.

## Language

**Collector**:
A source of events that turns an external stream (new blocks, pending txs, logs) into a stream of internal events.
_Avoid_: listener, watcher, source

**Strategy**:
The decision logic — consumes events, produces a stream of actions.

**Executor**:
The sink that carries out actions in an external domain (submitting a tx, posting an order).

**Engine**:
The orchestrator that spawns every Collector, Strategy, and Executor as a task and fans events/actions between them over broadcast channels.

**Observer**:
A passive consumer of the pipeline: one more subscriber on the Engine's event and action channels, seeing everything Strategies and Executors see while producing and perturbing nothing. Observation is best-effort (a lagging Observer skips messages like any consumer) and infallible by design — there is no error channel through which observing could fail the pipeline. Events and actions each arrive in channel order; no ordering holds between the two.
_Avoid_: metrics, instrument, monitor, telemetry hook

**Reconnect Policy**:
The per-Collector state machine that decides, after a stream is lost or never established, whether to **Retry** (after a backoff delay) or declare the Collector **Fatal** (unrecoverable). It owns the consecutive-failure counter and the backoff curve; it performs no I/O and keeps no clock — the **Collector Driver** supplies timing and cancellation.
_Avoid_: retry handler, supervisor, backoff helper

**Collector Driver**:
The loop that runs one Collector's full lifecycle: subscribe to the event stream, pump its events into the event channel, and on a lost or failed stream consult the Collector's **Reconnect Policy** — sleeping for a **Retry** or, on a **Fatal** verdict, cancelling the fatal token and then the root token. It supplies the I/O the policy refuses to: the actual `sleep`, the actual stream subscription, the actual send. The Engine spawns one Driver per Collector and otherwise stays out of reconnection.
_Avoid_: supervisor, collector task, reconnect loop

**Fatal**:
The Reconnect Policy's verdict that a Collector cannot recover. The Engine responds by cancelling a dedicated, observe-only **fatal token** (the reason) and then the root token (tearing down every task), so the binary can tell a fatal shutdown apart from a caller-initiated one and restart the process with a fresh sync — never by killing the host process itself.
_Avoid_: crash, panic, die

**Merge**:
A combinator that interleaves two or more Collectors into one composite Collector. Events arrive in whichever order the sources produce them. All sources subscribe eagerly when the composite subscribes; any creation failure fails the composite's subscribe, so the failure feeds the Reconnect Policy's counter instead of vanishing.
_Avoid_: combine, join, fan-in (the Engine's channel-level fan-in is a different thing)

**Chain**:
A combinator that delivers two or more Collectors' streams strictly in sequence — the next source's events are held back until the previous source's stream ends. Sources still subscribe eagerly at the composite's subscribe, so a later live source buffers at its source rather than missing events while earlier segments drain (the same head-buffering rationale as the Persisted Collector's subscribe). Any creation failure fails the whole subscribe.
_Avoid_: concat, append

**Retry**:
An Executor wrapper that re-submits a failed action with exponential backoff, per its **Retry Policy** (max retries + base delay), returning the last error once retries are exhausted. The execution-side counterpart of the **Reconnect Policy** — distinct from that policy's *Retry* decision, which concerns Collector streams.
_Avoid_: resubmit loop, retry handler

**Fallback**:
Tries a primary and re-routes to a secondary on failure. Two duals:
- **Executor Fallback**: tries a primary Executor and re-submits the action to a secondary on a *submit* error — primary RPC → backup RPC, or private relay → public mempool. The primary's error is logged; only the fallback's verdict is returned.
- **Collector Fallback**: subscribes a primary Collector, falling back to a secondary on a *subscribe* error — primary WS → backup WS. Stateless and primary-preferring: every re-subscribe tries the primary first, so a recovered primary is picked back up automatically; the backup is subscribed only when the primary fails (unlike **Merge**, which subscribes every source). One shared lifecycle (one Collector Driver, one Reconnect Policy), like **Merge** and **Chain**.
_Avoid_: failover, backup executor/collector

**Polling Fallback**:
The collector-side downgrade from a pubsub subscription to filter polling when the subscription cannot be established (most commonly a transport without pubsub, e.g. plain HTTP). The downgrade is logged as a warning and is stateless: every subscribe attempt — one per reconnect — tries the subscription first, so a recovered pubsub endpoint upgrades back automatically. A failed poll propagates as an ordinary subscribe failure to the **Reconnect Policy**. While polling, event latency is the provider's poll interval rather than push-on-arrival. Distinct from **Fallback**, the executor-side wrapper.
_Avoid_: failover, degraded mode

**Rate Limit**:
An Executor wrapper that caps submissions per sliding one-second window, to respect provider limits. An over-cap action waits (backpressure on the action channel) — it is never dropped. Every attempt counts against the window, including failed ones: a failed submission still spent provider quota.
_Avoid_: throttle, debounce

**EIP-1559 Pricing**:
The Executor prices `max_fee_per_gas` and `max_priority_fee_per_gas` from the provider's fee estimate, scaling the suggested priority fee by a configurable bump. With a `GasBidInfo`, the opportunity's break-even (`total_profit / gas_usage`) caps `max_fee_per_gas` rather than the legacy single gas price; the priority fee is clamped to stay at or below that cap.
_Avoid_: gas price, legacy pricing

**Replacement**:
The opt-in loop that resubmits an unconfirmed transaction at the same nonce with escalated fees, up to `max_replacements`, after each **Confirmation Timeout**. Distinct from the Executor **Retry** wrapper: Retry resubmits on a *send* error; Replacement resubmits a *sent-but-unmined* transaction. Use one or the other, not both.
_Avoid_: resend, speed-up, retry (that is the wrapper)

**Confirmation Timeout**:
How long the Executor waits for a transaction to mine before escalating its fees and replacing it. Distinct from the action-side **Deadline** (which drops stale actions before submission) and from `rpc_timeout` (which bounds a single RPC call).
_Avoid_: deadline, rpc_timeout

**Circuit Breaker**:
An Executor wrapper that stops submitting after N consecutive failures: an open circuit fails fast without reaching the inner executor, until an operator resets it through its **Handle** (taken before the engine consumes the executor). For a bot that signs transactions, failing closed is a safety feature, not just resilience. A success closes the counter; only an explicit reset closes an open circuit.
_Avoid_: fuse, trip switch

**Gated**:
An Executor wrapper guarded by a kill switch the caller keeps (an `AtomicBool`): flag on, actions execute; flag off, actions are logged and dropped with `Ok`. Flipping the flag at runtime is the emergency stop. **Dry Run** is a Gated whose flag is permanently off — paper-trading mode.
_Avoid_: toggle, feature flag

**Deadline**:
An Executor wrapper that drops actions whose freshness window has passed instead of submitting them. The deadline travels with the action (the `Expires` trait), stamped by the Strategy that priced it; the check runs at every execute, so inside a **Retry** each attempt re-checks and an action that expires mid-backoff stops the loop. An expired drop is `Ok` — invisible to **Retry** and **Circuit Breaker** — because expiry is normal operation, not a fault.
_Avoid_: TTL, expiry, timeout (the MempoolExecutor's `rpc_timeout` is a different thing)

**Risk Gate**:
A Strategy wrapper (`filter_actions`) that drops every action failing a predicate — minimum profit, maximum notional, allowlisted targets. As a combinator, the risk policy is visible at composition time rather than buried inside strategy logic.
_Avoid_: action filter, sanity check

**Cooldown**:
A Strategy wrapper that suppresses a strategy's actions for a period after it fires. A cooling strategy still sees every event — only its actions are dropped — so its internal state stays current; an actionless event does not start the cooldown, and a multi-action batch passes whole before the cooldown engages.
_Avoid_: debounce, rate limit (that is the Executor-side wrapper)

**Execution Outcome**:
The verdict the Executor stack reached for one action — the action plus `Ok(())` or `Err(message)` — fed back into the pipeline as an event. `Ok` means the stack *accepted* the action (submitted, or deliberately dropped by a **Gated**/**Deadline** layer), not that the transaction landed on chain. The error is stringified because it rides a broadcast channel.
_Avoid_: receipt, confirmation, result

**Report**:
The transparent Executor wrapper that publishes an **Execution Outcome** per action and then returns the inner executor's verdict unchanged, so it never alters control flow and composes anywhere in the reliability stack. Outermost, it reports the stack's final post-retry/post-fallback verdict.
_Avoid_: callback, hook, notify

**Channel Collector**:
A Collector over an in-process broadcast channel: it holds the Sender and mints a fresh receiver on every subscribe, so it survives the Collector Driver's re-subscription where a single receiver could not. The seam through which an **Execution Outcome** — or any in-process source — re-enters the pipeline as events.
_Avoid_: feedback channel, back-channel

**Persisted Collector**:
A Collector wrapper that records every event it sees into a Store and, on subscribe, delivers three **Segments** in fixed order: **Replay**, then **Backfill**, then the **Live Tail**.
_Avoid_: indexer, archiver, recorder

**Record**:
The mapping between one event type and its SQL rows. It owns the table name, the column schema — declared via an override (validated at construction, where a bad override panics) or frozen from the first encoded event — the encode-to-row and decode-from-payload directions, and the reserved-name invariant. The Store sees only the schemas and rows a Record produces.
_Avoid_: codec, row mapper, serializer

**Segment**:
One of the three ordered parts of a Persisted Collector's subscription. The order — Replay → Backfill → Live Tail — is fixed; the Segments are disjoint, split at the chain tip observed during subscribe.

**Replay**:
The Segment that reconstructs stored history from the Store. Runs only on the **first** subscribe (replay-once): on a reconnect the Engine subscribes again, and re-emitting the archive would re-deliver every historical event to Strategies.
_Avoid_: re-emit, history dump

**Backfill**:
The Segment that fetches the gap between the last stored block and the chain tip (`[last+1 ..= tip]`, never below the configured start block) from the source, sliced into bounded block-aligned chunks queried one at a time. These are complete blocks, so all of them — including the trailing one — are persisted. When there is no gap (stored height at or past the tip) no query is issued. A chunk that fails mid-backfill ends the whole subscription — live tail included — so the stored height cannot advance over the hole; the Reconnect Policy drives the resubscribe, which backfills again from the last stored block.
_Avoid_: catch-up, gap fill

**Live Tail**:
The unbounded Segment following the chain tip, strictly above the Backfill's cut (`> tip`). Its final in-progress block is never flushed to the Store; a restart re-fetches it via Backfill.
_Avoid_: live stream, subscription

## Relationships

- An **Engine** spawns one **Collector Driver** per **Collector**; each Driver owns one **Reconnect Policy** instance.
- A **Merge** or **Chain** composite is one **Collector** to the **Engine**: its sources share one **Collector Driver** and one **Reconnect Policy** (one lifecycle). Register sources as separate Collectors instead when each should reconnect — and go **Fatal** — independently.
- A **Collector Fallback** composite is one **Collector** to the **Engine**: mid-stream failover happens when the live stream ends and the **Collector Driver** re-subscribes — the combinator holds no health state, it just prefers the primary on every subscribe. Register sources separately if each should reconnect independently.
- A **Reconnect Policy** counts consecutive stream failures and resets that count only when its **Collector Driver** reports a delivered event.
- A **Persisted Collector** pairs one **Collector** (block-aware) with one Store; its subscription is the chain Replay → Backfill → Live Tail.
- A **Persisted Collector** constructs one **Record** per subscription; every row written to or replayed from the Store passes through it.
- A **Fatal** verdict cancels the observe-only fatal token, then the root token shared by all **Collector**, **Strategy**, and **Executor** tasks; the binary observes the fatal token and decides to exit.
- An **Engine** spawns one task per **Observer**, subscribed to both channels; an Observer has no feedback path into the pipeline.
- The reliability wrappers (**Deadline**, **Retry**, **Fallback**, **Rate Limit**, **Circuit Breaker**, **Gated**) nest around one **Executor** and compose in any order, but order is meaningful: `retry` inside `fallback` retries the primary before failing over; `gated` outermost means a kill switch drops actions before any other layer sees them; `deadline` belongs innermost, so every queueing and waiting layer above it has already elapsed by the time the expiry check runs.
- A **Risk Gate** and a **Cooldown** wrap one **Strategy**; the Cooldown counts only actions that survive the layers inside it as firing.
- Every built-in **Collector**'s subscribe carries a **Polling Fallback**; a failed poll feeds the **Reconnect Policy**'s counter like any subscribe failure.
- A **Report** and a **Channel Collector** sharing one broadcast channel close the execution-feedback loop *without* a back-channel in the **Engine**: the Report publishes each verdict, the Channel Collector re-enters it through the normal Collector → Strategy path, and a Strategy reacts. The one-way topology is preserved — the loop is explicit caller wiring, not a hidden feedback edge.

## Example dialogue

> **Dev:** "When the WebSocket drops, who decides whether to reconnect?"
> **Maintainer:** "The Collector's **Reconnect Policy**. It returns **Retry** with a backoff until the failure count crosses the threshold, then it returns **Fatal**."
> **Dev:** "And **Fatal** kills the process?"
> **Maintainer:** "The library never calls `exit`. **Fatal** cancels the observe-only fatal token and then the root token; the binary sees the fatal token, tells it apart from a Ctrl-C, and restarts so the orchestrator re-syncs from clean state."

## Flagged ambiguities

- "retry" was used for both a single backoff-and-reconnect step and the whole give-up-or-keep-trying policy — resolved: a single step is a **Retry** decision; the state machine that emits those decisions is the **Reconnect Policy**.
- A persistent stream-*creation* failure and a persistent stream-*end* are the same concept to the Reconnect Policy: both feed one counter and both can reach **Fatal**. The earlier code treated creation-failure as a quiet task exit — that asymmetry was an accidental gap, not a decision.
- "Retry" now names two things: the Reconnect Policy's per-step *decision* on the Collector side, and the Executor wrapper on the execution side. Context disambiguates — streams reconnect, submissions retry — and both share the same backoff curve idiom.
