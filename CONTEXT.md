# artemis-light

Domain language for the event-processing pipeline (Collectors → Strategies → Executors, wired by the Engine). Records the terms a maintainer must share to talk about the pipeline precisely.

## Language

**Collector**:
A source of events that turns an external stream (new blocks, pending txs, logs) into a stream of internal events.
_Avoid_: listener, watcher, source

**Strategy**:
The opportunity logic — consumes events, produces a stream of actions.

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

**Persisted Collector**:
A Collector wrapper that records every event it sees into a Store and, on subscribe, delivers three **Segments** in fixed order: **Replay**, then **Backfill**, then the **Live Tail**.
_Avoid_: indexer, archiver, recorder

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
- A **Reconnect Policy** counts consecutive stream failures and resets that count only when its **Collector Driver** reports a delivered event.
- A **Persisted Collector** pairs one **Collector** (block-aware) with one Store; its subscription is the chain Replay → Backfill → Live Tail.
- A **Fatal** verdict cancels the observe-only fatal token, then the root token shared by all **Collector**, **Strategy**, and **Executor** tasks; the binary observes the fatal token and decides to exit.
- An **Engine** spawns one task per **Observer**, subscribed to both channels; an Observer has no feedback path into the pipeline.

## Example dialogue

> **Dev:** "When the WebSocket drops, who decides whether to reconnect?"
> **Maintainer:** "The Collector's **Reconnect Policy**. It returns **Retry** with a backoff until the failure count crosses the threshold, then it returns **Fatal**."
> **Dev:** "And **Fatal** kills the process?"
> **Maintainer:** "The library never calls `exit`. **Fatal** cancels the observe-only fatal token and then the root token; the binary sees the fatal token, tells it apart from a Ctrl-C, and restarts so the orchestrator re-syncs from clean state."

## Flagged ambiguities

- "retry" was used for both a single backoff-and-reconnect step and the whole give-up-or-keep-trying policy — resolved: a single step is a **Retry** decision; the state machine that emits those decisions is the **Reconnect Policy**.
- A persistent stream-*creation* failure and a persistent stream-*end* are the same concept to the Reconnect Policy: both feed one counter and both can reach **Fatal**. The earlier code treated creation-failure as a quiet task exit — that asymmetry was an accidental gap, not a decision.
