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

**Reconnect Policy**:
The per-Collector state machine that decides, after a stream is lost or never established, whether to **Retry** (after a backoff delay) or declare the Collector **Fatal** (unrecoverable). It owns the consecutive-failure counter and the backoff curve; it performs no I/O and keeps no clock — the driver supplies timing and cancellation.
_Avoid_: retry handler, supervisor, backoff helper

**Fatal**:
The Reconnect Policy's verdict that a Collector cannot recover. The Engine responds by cancelling a dedicated, observe-only **fatal token** (the reason) and then the root token (tearing down every task), so the binary can tell a fatal shutdown apart from a caller-initiated one and restart the process with a fresh sync — never by killing the host process itself.
_Avoid_: crash, panic, die

## Relationships

- An **Engine** drives many **Collectors**; each **Collector** task owns one **Reconnect Policy** instance.
- A **Reconnect Policy** counts consecutive stream failures and resets that count only when its Collector delivers a real event.
- A **Fatal** verdict cancels the observe-only fatal token, then the root token shared by all **Collector**, **Strategy**, and **Executor** tasks; the binary observes the fatal token and decides to exit.

## Example dialogue

> **Dev:** "When the WebSocket drops, who decides whether to reconnect?"
> **Maintainer:** "The Collector's **Reconnect Policy**. It returns **Retry** with a backoff until the failure count crosses the threshold, then it returns **Fatal**."
> **Dev:** "And **Fatal** kills the process?"
> **Maintainer:** "The library never calls `exit`. **Fatal** cancels the observe-only fatal token and then the root token; the binary sees the fatal token, tells it apart from a Ctrl-C, and restarts so the orchestrator re-syncs from clean state."

## Flagged ambiguities

- "retry" was used for both a single backoff-and-reconnect step and the whole give-up-or-keep-trying policy — resolved: a single step is a **Retry** decision; the state machine that emits those decisions is the **Reconnect Policy**.
- A persistent stream-*creation* failure and a persistent stream-*end* are the same concept to the Reconnect Policy: both feed one counter and both can reach **Fatal**. The earlier code treated creation-failure as a quiet task exit — that asymmetry was an accidental gap, not a decision.
