# Strategy and Executor Adapter Combinators

**Date:** 2026-06-11
**Status:** Approved

## Problem

The engine broadcasts one event type `E` to every strategy and one action type
`A` to every executor. Multi-source bots therefore define umbrella enums
(`enum Event { Block(..), Tx(..) }`), and today every strategy and executor
must be written *against* those umbrella types: each `process_event` starts
with a `match event { Event::Block(b) => .., _ => return empty }` preamble,
and each `execute` ignores the variants it doesn't handle. Strategies and
executors written against their own narrow types — the reusable, testable
form — cannot be added to the engine at all.

`CollectorExt::map` already solves the producer side (widening narrow sources
into the umbrella type). This design adds the consumer-side duals.

## Design

Two new extension traits, blanket-implemented for all implementors, mirroring
`CollectorExt` exactly (module layout, boxed-inner wrapper structs, naming):

```rust
// src/strategy_ext.rs
pub trait StrategyExt<E, A>: Strategy<E, A> + Send + Sync + Sized + 'static {
    /// Mount this strategy into an engine with a wider event type `E2`:
    /// `f` projects each engine event down to this strategy's event type,
    /// returning `None` for events this strategy doesn't consume.
    fn filter_map_event<F, E2>(self, f: F) -> FilterMapEvent<E, A, F>
    where
        F: Fn(E2) -> Option<E> + Send + Sync + 'static;

    /// Lift this strategy's actions into a wider action type `A2`,
    /// e.g. an umbrella-enum constructor: `.map_action(Action::Submit)`.
    fn map_action<F, A2>(self, f: F) -> MapAction<E, A, F>
    where
        F: Fn(A) -> A2 + Send + Sync + Clone + 'static;
}

// src/executor_ext.rs
pub trait ExecutorExt<A>: Executor<A> + Send + Sync + Sized + 'static {
    /// Route only matching actions to this executor: `f` projects each
    /// engine action down to this executor's action type; `None` is skipped.
    fn filter_map_action<F, A2>(self, f: F) -> FilterMapAction<A, F>
    where
        F: Fn(A2) -> Option<A> + Send + Sync + 'static;
}
```

Composition site:

```rust
engine.add_strategy(Box::new(
    BlockStrategy::new(cfg)
        .filter_map_event(|e| match e { Event::Block(b) => Some(b), _ => None })
        .map_action(Action::Submit),
));
engine.add_executor(Box::new(
    MempoolExecutor::new(provider)
        .filter_map_action(|a| match a { Action::Submit(tx) => Some(tx), _ => None }),
));
```

### Direction of the closures

These adapters are consumer-side, so the closures run *engine type → narrow
type* (`Fn(E2) -> Option<E>`) — the reverse of collector `map`. The event and
executor-action projections are partial (`Option`) because on a broadcast
channel most events/actions are "not for me"; hence `filter_map_*` naming,
consistent with `CollectorExt::filter_map`. `map_action` is the covariant
output side and total — typically an enum constructor. A total `map_event` is
deliberately omitted (YAGNI; can be added later without breaking anything).

### Semantics

- A filtered-out event is **not an error**: `FilterMapEvent::process_event`
  returns `Ok` with an empty `ActionStream`.
- A filtered-out action is `Ok(())`; the inner executor never sees it.
- `sync_state` delegates to the inner strategy untouched (both wrappers).
- Errors from the inner strategy/executor propagate unchanged. The adapters
  add no retry, logging, or error mapping — the engine already logs failures.
- Wrappers implement `Strategy`/`Executor`, so they pick the extension traits
  back up and chain freely.

### Wrapper structs

One file per combinator, holding `Box<dyn Strategy<E, A>>` /
`Box<dyn Executor<A>>` plus the closure, exactly like `collector_ext::Map`:

| Struct | File | Implements | Core of impl |
|---|---|---|---|
| `FilterMapEvent<E, A, F>` | `src/strategy_ext/filter_map_event.rs` | `Strategy<E2, A>` | `f(event)` → `Some(e)`: delegate; `None`: `Ok(empty stream)` |
| `MapAction<E, A, F>` | `src/strategy_ext/map_action.rs` | `Strategy<E, A2>` | await inner stream, wrap in `StreamExt::map(f.clone())` |
| `FilterMapAction<A, F>` | `src/executor_ext/filter_map_action.rs` | `Executor<A2>` | `f(action)` → `Some(a)`: delegate; `None`: `Ok(())` |

`MapAction` is the only wrapper that captures its closure inside a returned
stream (which mutably borrows the inner strategy for `'_`), so it alone needs
`F: Clone` — the same reason `CollectorExt::map` requires it. The other two
call their closure before delegating.

In each `impl`, the outer type (`E2`/`A2`) appears only in the closure's
signature, so inference resolves it at the `add_strategy`/`add_executor` call
site with no turbofish — the same shape as `Map<E, F>`.

## Testing

Unit tests in the `strategy_ext.rs` / `executor_ext.rs` test modules, using
small purpose-built test doubles in `collector_ext`'s style:

- `filter_map_event` routes matching events to the inner strategy; a
  non-matching event yields an empty action stream, not an error.
- `map_action` transforms every action in the stream, preserving order.
- `sync_state` reaches the inner strategy through both strategy wrappers.
- Inner errors (`sync_state`, `process_event`, `execute`) propagate unchanged
  through every wrapper.
- `filter_map_action` executes matching actions and returns `Ok` for skipped
  ones; a counting executor proves a skipped action never reaches the inner
  executor.
- A chained `filter_map_event(...).map_action(...)` composes end-to-end.

## Example

`examples/adapters_example.rs`: the umbrella-enum pattern end-to-end —
`enum Event { Tick(u64), Price(f64) }` / `enum Action { Log(String),
Submit(u64) }`, two narrow collectors widened with the existing
`.map(Event::Tick)`, two narrow strategies mounted with
`.filter_map_event(...).map_action(...)`, two executors routed with
`.filter_map_action(...)`, all in one `Engine<Event, Action>`. Self-contained
like `basic_example.rs` (no RPC required), registered in
`examples/README.md`.

## Wiring and docs

- `lib.rs` exports `strategy_ext` and `executor_ext` alongside
  `collector_ext`.
- Rustdoc on both traits explains the umbrella-enum pattern and cross-links
  the collector duals (`CollectorExt::map` / `filter_map`).

## Out of scope

- A total `map_event` (add later if needed).
- Reliability wrappers (`retry`, `fallback`, `rate_limit`, `circuit_breaker`,
  `gated`) — a separate design.
- Strategy-to-strategy piping (`and_then`): deliberately rejected; the
  broadcast topology plus `&mut self`/`sync_state` make ordering and state
  ownership ambiguous.
