# Confirmation-Depth Lag on Persistence

**Date:** 2026-06-12
**Status:** Approved

## Problem

The persisted live tail flushes a block as soon as one higher block arrives —
effectively a 1-confirmation commit (`BlockWriter`, the `block > cur` branch in
`src/persistence/persisted.rs`). A reorg deeper than that rewrites blocks the
store has already committed, and because retraction (`removed: true`) logs are
dropped upstream (`EventCollector`), nothing un-writes the orphaned rows:
replay then reconstructs events that no longer happened on-chain. The store can
silently diverge from the canonical chain at the head.

The existing defenses are real but partial: the writer *halts* on a backwards
block (refusing to leave a hole) and never flushes its open block. Halting
turns every ordinary shallow reorg into a stalled persistence stream that only
a restart clears.

## Scope

Add a configurable confirmation-depth lag: persist a block only once it is `n`
blocks deep, while still delivering events downstream live and immediately. A
shallow reorg (depth `< n`) is *corrected* in the buffer before anything is
written; a reorg deeper than `n` still halts (the operator chooses `n` above
the deepest reorg they expect — the standard indexer contract). Active
retraction handling — propagating `removed` logs and deleting stored rows — is
explicitly out of scope (it needs per-log identity the indexed stream does not
carry; see the deferral already noted in `persisted.rs`).

## Design

### The knob

```rust
impl<C, S> Persisted<C, S> {
    /// Persist a block only once it is `depth` blocks deep (default 1). Events
    /// are still delivered downstream live and immediately; only the write to
    /// the Store lags. A reorg shallower than `depth` is corrected in the
    /// buffer before any orphaned row is written; a reorg deeper than `depth`
    /// halts persistence (a restart re-syncs). Choose `depth` above the deepest
    /// reorg you expect.
    ///
    /// # Panics
    /// Panics if `depth` is zero.
    pub fn with_confirmation_depth(mut self, depth: u64) -> Self {
        assert!(depth >= 1, "confirmation depth must be at least 1 block");
        self.confirmation_depth = depth;
        self
    }
}
```

Default `confirmation_depth = 1` reproduces today's behaviour exactly (flush a
block when the next block arrives; never flush the open block).

### What changes, and what does not

- **Backfill is unchanged.** It reads a settled range via `query_range`
  (`eth_getLogs`), an ordered source where a backwards block is a genuine fault,
  not a reorg. It keeps using `BlockWriter` (single-block buffer, halt on
  backwards, `flush_final = true`). All existing backfill tests stand.
- **The live tail gets a new windowed writer**, `ConfirmationWindow`, applying
  the depth lag and absorbing in-window reorgs. The live source filter
  (`> tip`), the `poison` cancellation, and the `persist_and_emit` event
  passthrough are unchanged — only the writer behind the live segment changes.

### `ConfirmationWindow`

State:

```rust
struct ConfirmationWindow<'a, S, E> {
    store: &'a S,
    record: Arc<Record<E>>,
    depth: u64,
    /// Unflushed blocks' rows, keyed by block number (ascending).
    pending: BTreeMap<u64, Vec<Row>>,
    /// Highest block seen on the live stream — the confirmation head.
    head: Option<u64>,
    /// Highest block already flushed (the finalized watermark).
    flushed: Option<u64>,
    healthy: bool,
}
```

`record(block, event)` (no-op once unhealthy):

1. Encode the event to a row; an encode error halts (as today — the same
   "quietly truncated history" argument).
2. **Deep reorg:** if `flushed` is `Some(f)` and `block <= f`, a finalized block
   is being rewritten — unfixable without a delete → **halt**. (Today's
   backwards-halt safety, now measured against the finalized watermark.)
3. **Shallow reorg (in-window correction):** if `head` is `Some(h)` and
   `block < h`, the chain forked above `block`. Drop every `pending` entry with
   key `>= block` (the old fork's blocks; the node re-emits the canonical ones),
   and set `head = block`. No halt — this is the expected case the feature
   exists to handle.
4. Append the row to `pending[block]`; advance `head` to `max(head, block)`.
5. **Flush matured blocks:** while the lowest `pending` block `b` satisfies
   `head >= b + depth`, flush `b` (one block per transaction, via the existing
   `flush` helper), set `flushed = b`, and remove it from `pending`. A flush
   failure halts (as today).

There is no `finish()` for the live tail: the stream never ends, and the
unflushed window (the most recent `< depth`-deep blocks) is intentionally left
for a restart's backfill to re-fetch — the same contract the open block has
today.

Buffer bound: `pending` holds at most `depth` blocks (plus the transient extra
during a reorg rebuild), so the window is bounded by the configured depth, not
by stream length.

### Why `head`-based confirmation is correct

`head >= b + depth` means `depth` distinct higher blocks have been observed
building on top of `b` — exactly "n confirmations." A reorg lowers `head`
(step 3), which correctly *un-confirms* the blocks above the fork: they must be
re-observed to mature again. No wall clock and no separate tip query is needed;
the live stream's own progression is the signal.

## Testing

### Unit tests (new `ConfirmationWindow`, in `persisted.rs`)

Using the existing `RecordingStore` / `FailingStore` doubles:

- **Flush lags by depth:** at `depth = 2`, feed blocks 1,2,3,4; assert blocks
  flush only as they bury 2 deep (block 1 flushes when 3 arrives, block 2 when
  4 arrives), with 3 and 4 still buffered.
- **`depth = 1` reproduces single-block behaviour:** feed 1,2,3; blocks 1 and 2
  flush, 3 stays open.
- **In-window reorg replaces buffered rows before flush:** at `depth = 2`, feed
  block 5 (event A), block 6 (event A'), then block 5 again (event B — the
  reorg). Assert the eventual stored rows for block 5 are B's, never A's, and
  block 6's original rows were dropped (re-emitted afresh).
- **Deep reorg past the watermark halts:** at `depth = 1`, let block 5 flush
  (watermark = 5), then deliver block 5 again; assert the writer halts and no
  further block is written.
- **Unencodable event halts** (port the existing `BadPing` test to the windowed
  writer).
- **Reorg does not write the orphaned block:** at `depth = 2`, feed 5,6, then
  re-emit 5 (corrected) — assert block 5 is written only after it matures with
  the corrected rows, and the pre-reorg block 6 rows are absent.

### Integration test (`tests/persistence.rs`)

Extend `FakeCollector` to emit a live sequence containing a reorg re-emission
(a block number that goes backwards within the window). With
`with_confirmation_depth(2)`:

- A live stream `(10,A),(11,B),(10,A2)` — block 10 re-emitted before it matured
  — ends with the store holding block 10's *corrected* row `A2` (once it
  matures), and the original `A`/`B` never persisted as canonical. Assert
  `last_block` and `stored_values` reflect the corrected chain.
- A regression assertion that the default (no `with_confirmation_depth`) still
  flushes block 10 of `(10,1),(10,2),(11,3)` and leaves 11 open — the existing
  `persisted_records_live_events_per_complete_block` expectation, now also
  covered explicitly at depth 1.

## Docs

- README "Persistence" section: a paragraph on `with_confirmation_depth(n)` —
  events deliver live immediately, persistence lags `n` blocks so reorgs
  shallower than `n` never reach the store; deeper reorgs halt and a restart
  re-syncs; default 1 is today's behaviour.
- CONTEXT.md:
  - **Confirmation Depth**: the number of blocks a block must be buried under
    before the Persisted Collector writes it. The live tail buffers the most
    recent Confirmation-Depth blocks; a reorg shallower than the depth is
    corrected in the buffer before any orphaned row is written, while a reorg
    deeper than it halts persistence (a restart re-syncs). _Avoid_: finality,
    confirmations count, lag.
  - Update **Live Tail**: persistence now lags the live edge by the Confirmation
    Depth — the unflushed window (not just the single open block) is what a
    restart re-fetches via Backfill.
  - Update the **Backfill** entry's reorg note and the **Persisted Collector**
    relationship to mention that shallow reorgs are absorbed by the live tail's
    window rather than halting.

## Out of scope

- **Active retraction handling** (`removed: true` → delete/supersede stored
  rows): needs per-log identity (block + log index) the `(u64, E)` indexed
  stream does not carry, plus a Store delete operation and replay changes — a
  separate, larger design.
- **Applying the depth lag to backfill.** Backfill reads a settled range and
  flushes it whole; the near-tip exposure is bounded and self-heals on the next
  resubscribe's re-query. Reworking backfill to share the live window is not
  justified by the reorg risk on an `eth_getLogs` range.
- **A wall-clock or finalized-checkpoint finality signal** (e.g. tracking the
  beacon-chain finalized block): the block-count proxy is the simple, provider-
  agnostic choice; a checkpoint source can layer on later.
