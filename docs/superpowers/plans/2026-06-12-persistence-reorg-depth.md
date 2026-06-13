# Confirmation-Depth Lag on Persistence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist a block only once it is `n` confirmations deep, correcting shallow reorgs in the buffer before any orphaned row is written, while still delivering events live immediately.

**Architecture:** A new `ConfirmationWindow` writer behind the live-tail segment buffers the most recent `n` blocks and flushes a block when `head >= block + n`; the backfill writer (`BlockWriter`) is untouched. A `with_confirmation_depth(n)` knob (default 1 = today's behavior) threads the depth into the live segment.

**Tech Stack:** Rust (edition 2024), `std::collections::BTreeMap`, async-stream, tokio, anyhow, tracing; existing `Store`/`Record`/`Row` types; anvil only for the broader persistence integration suite (the new tests use in-memory SQLite + fakes).

**Spec:** `docs/superpowers/specs/2026-06-12-persistence-reorg-depth-design.md`

## Discovery

**Similar implementations:** `src/persistence/persisted.rs` — `BlockWriter` (the single-block writer being generalized for the live tail), `persist_and_emit` (the wrapper that records while passing events through), and `flush` (the per-block store write). The `subscribe` method builds the three segments; the live segment is constructed at lines ~259-267.
**File conventions:** Persistence internals live in `src/persistence/persisted.rs`; the public knob is a `with_*` builder on `Persisted`. Doc comments carry the reasoning (this file is heavily commented — match that density).
**Testing patterns:** Unit tests in the file's `#[cfg(test)] mod tests` use `RecordingStore` (records written block numbers) and `FailingStore`; events are `Ping`/`BadPing` sol types with helpers `ping(n)` / `bad_ping(n)`. Integration tests in `tests/persistence.rs` use `SqliteStore::connect("sqlite::memory:")`, a `FakeCollector` with `.live(..)` / `.backfill(..)` / `.tip(..)`, and helpers `value_event`, `stored_values`, `seed`.
**Integration points:** `Persisted::subscribe` wires the live segment via `persist_and_emit(live_source, &self.store, record, false)`. The new writer plugs in there. `with_confirmation_depth` joins `with_schema` / `with_start_block` / `with_backfill_chunk_size` as a builder.
**Project conventions:** Builders that reject invalid values `panic!`/`assert!` (see `with_backfill_chunk_size`). Verification gate: `cargo fmt --all -- --check`, `RUSTFLAGS="-Dwarnings" cargo clippy --all-features`, `cargo test --lib` (+ `cargo test --all-features` with anvil). Commit messages are plain imperative sentences.
**Context loaded:** none — ad-hoc discovery (persisted.rs, store.rs, tests/persistence.rs).

## File Structure

- Modify: `src/persistence/persisted.rs` — add `confirmation_depth` field + `with_confirmation_depth`; add `ConfirmationWindow` + `persist_and_emit_windowed`; switch the live segment to it; unit tests
- Modify: `tests/persistence.rs` — extend `FakeCollector` usage with a reorg sequence; depth-2 correction test + explicit depth-1 regression
- Modify: `README.md` — Persistence section paragraph
- Modify: `CONTEXT.md` — **Confirmation Depth** term; update **Live Tail**, **Backfill**, **Persisted Collector**

---

### Task 1: The `with_confirmation_depth` knob

**Files:**
- Modify: `src/persistence/persisted.rs`

- [ ] **Step 1: Add the field and builder with a failing test**

Add a `confirmation_depth: u64` field to `Persisted` (after `backfill_chunk_size`), default it to `1` in `new`, and add the builder after `with_backfill_chunk_size`:

```rust
    /// Persist a block only once it is `depth` blocks deep (default 1). Events
    /// are still delivered downstream live and immediately; only the Store
    /// write lags. A reorg shallower than `depth` is corrected in the buffer
    /// before any orphaned row is written; a reorg deeper than `depth` halts
    /// persistence (a restart re-syncs). Choose `depth` above the deepest reorg
    /// you expect.
    ///
    /// # Panics
    /// Panics if `depth` is zero.
    pub fn with_confirmation_depth(mut self, depth: u64) -> Self {
        assert!(depth >= 1, "confirmation depth must be at least 1 block");
        self.confirmation_depth = depth;
        self
    }
```

Add a unit test in `mod tests`:

```rust
    #[test]
    #[should_panic(expected = "confirmation depth must be at least 1")]
    fn zero_confirmation_depth_panics() {
        let store = RecordingStore::default();
        // `Persisted::new` then the builder; the collector type is irrelevant
        // to the panic, so reuse a minimal fake. If no in-file fake exists,
        // assert via a direct `Persisted` construction over `FailingStore`.
        let _ = Persisted::new((), store).with_confirmation_depth(0);
    }
```

> Note: `Persisted::new` is generic over `C`; `()` does not implement
> `PersistableCollector`, but `with_confirmation_depth` and `new` do not
> require that bound, so this compiles. If the bound is in fact required at
> construction, use the integration test in Task 4 to cover the panic instead
> and drop this unit test.

- [ ] **Step 2: Run the test**

Run: `cargo test --lib persistence::persisted::tests::zero_confirmation_depth_panics`
Expected: PASS (the assert fires). If the `()` collector does not compile, move this assertion to `tests/persistence.rs` over a `FakeCollector` and rerun there.

- [ ] **Step 3: Commit**

```bash
git add src/persistence/persisted.rs
git commit -m "Add with_confirmation_depth knob to Persisted

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: `ConfirmationWindow` writer — flush-at-depth

**Files:**
- Modify: `src/persistence/persisted.rs`

- [ ] **Step 1: Write the windowed writer with failing tests**

Add `use std::collections::BTreeMap;` to the imports. Add the struct and impl
(near `BlockWriter`), with the flush logic. Reuse the existing free `flush`
helper.

```rust
/// Buffers the most recent `depth` blocks of a live tail and writes a block
/// only once it is buried `depth` blocks deep (`head >= block + depth`). Unlike
/// [`BlockWriter`], a backwards block within the unflushed window is treated as
/// a reorg re-emission and corrected in place rather than halting; only a write
/// at or below the already-flushed watermark (a reorg deeper than `depth`) or
/// an unencodable event halts.
struct ConfirmationWindow<'a, S, E> {
    store: &'a S,
    record: Arc<Record<E>>,
    depth: u64,
    pending: BTreeMap<u64, Vec<Row>>,
    head: Option<u64>,
    flushed: Option<u64>,
    healthy: bool,
}

impl<'a, S: Store, E: Serialize> ConfirmationWindow<'a, S, E> {
    fn new(store: &'a S, record: Arc<Record<E>>, depth: u64) -> Self {
        Self {
            store,
            record,
            depth,
            pending: BTreeMap::new(),
            head: None,
            flushed: None,
            healthy: true,
        }
    }

    /// Buffer one event's row, correcting an in-window reorg, then flush every
    /// block that has matured to `depth` confirmations. No-op once unhealthy.
    async fn record(&mut self, block: u64, event: &E) {
        if !self.healthy {
            return;
        }

        // A finalized block being rewritten is a reorg deeper than `depth`:
        // unfixable without a delete, so halt (the stored height stays; a
        // restart re-syncs).
        if let Some(f) = self.flushed
            && block <= f
        {
            self.halt(format_args!(
                "block {block} rewritten at/below the finalized watermark {f} \
                 (reorg deeper than confirmation depth {})",
                self.depth
            ));
            return;
        }

        let row = match self.record.encode(event) {
            Ok(row) => row,
            // As in BlockWriter: an unencodable event must not be skipped, or
            // progress advances past a hole replay would expose.
            Err(e) => {
                self.halt(format_args!("failed to encode row: {e}"));
                return;
            }
        };

        // Shallow reorg: the chain forked above `block`. Drop the old fork's
        // buffered blocks (the node re-emits the canonical ones) and rewind the
        // head so those blocks must re-confirm.
        if let Some(h) = self.head
            && block < h
        {
            self.pending.retain(|&b, _| b < block);
            self.head = Some(block);
        }

        self.pending.entry(block).or_default().push(row);
        self.head = Some(self.head.map_or(block, |h| h.max(block)));

        self.flush_matured().await;
    }

    /// Flush every buffered block now buried `depth` deep, lowest first.
    async fn flush_matured(&mut self) {
        let Some(head) = self.head else { return };
        // Collect the matured block numbers first to avoid borrowing `pending`
        // across the await.
        let matured: Vec<u64> = self
            .pending
            .keys()
            .copied()
            .filter(|&b| head >= b + self.depth)
            .collect();
        for b in matured {
            let rows = self.pending.remove(&b).unwrap_or_default();
            let schema = self.record.schema().expect("frozen by first encode");
            if !flush(self.store, &schema, b, rows).await {
                self.healthy = false;
                self.pending.clear();
                return;
            }
            self.flushed = Some(b);
        }
    }

    fn halt(&mut self, reason: std::fmt::Arguments<'_>) {
        self.healthy = false;
        self.pending.clear();
        tracing::error!(
            "halting persistence ({reason}); events keep flowing, and a \
             restart will re-sync from the last stored block"
        );
    }

    #[cfg(test)]
    fn buffered_blocks(&self) -> Vec<u64> {
        self.pending.keys().copied().collect()
    }
}
```

Add unit tests in `mod tests`:

```rust
    /// A windowed writer over a fresh inferred Record for `E`.
    fn window<S, E>(store: &S, depth: u64) -> ConfirmationWindow<'_, S, E>
    where
        S: Store,
        E: alloy::sol_types::SolEvent + Serialize,
    {
        ConfirmationWindow::new(store, Arc::new(Record::new(None).unwrap()), depth)
    }

    #[tokio::test]
    async fn windowed_writer_flushes_only_blocks_buried_depth_deep() {
        let store = RecordingStore::default();
        let mut w = window::<_, Ping>(&store, 2);

        w.record(1, &ping(1)).await; // head 1: nothing matured
        w.record(2, &ping(2)).await; // head 2: 1 needs head>=3
        assert_eq!(store.written(), Vec::<u64>::new());

        w.record(3, &ping(3)).await; // head 3: block 1 matures (1+2<=3)
        assert_eq!(store.written(), vec![1]);

        w.record(4, &ping(4)).await; // head 4: block 2 matures
        assert_eq!(store.written(), vec![1, 2]);
        assert_eq!(w.buffered_blocks(), vec![3, 4]);
    }

    #[tokio::test]
    async fn depth_one_matches_single_block_behaviour() {
        let store = RecordingStore::default();
        let mut w = window::<_, Ping>(&store, 1);

        w.record(1, &ping(1)).await;
        w.record(2, &ping(2)).await; // block 1 matures (1+1<=2)
        w.record(3, &ping(3)).await; // block 2 matures
        assert_eq!(store.written(), vec![1, 2]);
        assert_eq!(w.buffered_blocks(), vec![3], "block 3 stays open");
    }
```

- [ ] **Step 2: Run the tests to verify they pass**

Run: `cargo test --lib persistence::persisted::tests::windowed`
plus `cargo test --lib persistence::persisted::tests::depth_one`
Expected: both new tests PASS.

- [ ] **Step 3: Commit**

```bash
git add src/persistence/persisted.rs
git commit -m "Add ConfirmationWindow writer with depth-based flush

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: Reorg correction and deep-reorg halt

**Files:**
- Modify: `src/persistence/persisted.rs` (test module + verify behavior)

- [ ] **Step 1: Write the reorg tests**

Append to `mod tests`. These exercise the reorg branches already implemented in
Task 2:

```rust
    #[tokio::test]
    async fn in_window_reorg_replaces_buffered_rows_before_flush() {
        let store = RecordingStore::default();
        let mut w = window::<_, Ping>(&store, 2);

        // Original fork: blocks 5 and 6 buffered (neither matured yet at depth 2).
        w.record(5, &ping(50)).await;
        w.record(6, &ping(60)).await;
        assert_eq!(store.written(), Vec::<u64>::new());

        // Reorg: block 5 re-emitted. Block 6's buffered rows are dropped; head
        // rewinds to 5.
        w.record(5, &ping(51)).await;
        assert_eq!(w.buffered_blocks(), vec![5], "the old fork's block 6 is gone");

        // Re-advance: the canonical 6, then 7 matures block 5.
        w.record(6, &ping(61)).await;
        w.record(7, &ping(70)).await; // head 7: block 5 matures (5+2<=7)
        assert_eq!(store.written(), vec![5], "block 5 written once, after correction");
    }

    #[tokio::test]
    async fn deep_reorg_past_the_watermark_halts() {
        let store = RecordingStore::default();
        let mut w = window::<_, Ping>(&store, 1);

        w.record(5, &ping(1)).await;
        w.record(6, &ping(2)).await; // block 5 flushes (watermark = 5)
        assert_eq!(store.written(), vec![5]);

        // Block 5 re-emitted after being finalized: deeper than depth -> halt.
        w.record(5, &ping(3)).await;
        // No further writes; later events are ignored.
        w.record(7, &ping(4)).await;
        assert_eq!(store.written(), vec![5], "nothing written after a deep reorg");
    }

    #[tokio::test]
    async fn windowed_writer_halts_on_unencodable_event() {
        let store = RecordingStore::default();
        let mut w = window::<_, BadPing>(&store, 2);

        w.record(1, &bad_ping(1)).await;
        w.record(2, &bad_ping(0)).await; // unencodable -> halt
        w.record(3, &bad_ping(3)).await;
        w.record(4, &bad_ping(4)).await; // would otherwise mature block 1/2
        assert_eq!(store.written(), Vec::<u64>::new());
    }
```

- [ ] **Step 2: Run the tests**

Run: `cargo test --lib persistence::persisted::tests`
Expected: all windowed-writer tests PASS. If `in_window_reorg_*` fails, the
reorg branch in `record` (drop `pending >= block`, rewind `head`) is wrong —
fix the implementation, not the test.

- [ ] **Step 3: Commit**

```bash
git add src/persistence/persisted.rs
git commit -m "Test ConfirmationWindow reorg correction and deep-reorg halt

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: Wire the windowed writer into the live segment

**Files:**
- Modify: `src/persistence/persisted.rs`

- [ ] **Step 1: Add `persist_and_emit_windowed`**

Add a sibling to `persist_and_emit` that drives the `ConfirmationWindow`. It
has no `flush_final` (the live tail never ends; the window is re-fetched on
restart):

```rust
/// Like [`persist_and_emit`], but persists with a [`ConfirmationWindow`]: a
/// block is written only once it is `depth` confirmations deep, and an
/// in-window reorg is corrected before any orphaned row is written. The
/// unflushed window is intentionally left for a restart's backfill to
/// re-fetch.
fn persist_and_emit_windowed<'a, E, S>(
    mut source: CollectorStream<'a, (u64, E)>,
    store: &'a S,
    record: Arc<Record<E>>,
    depth: u64,
) -> CollectorStream<'a, E>
where
    E: Serialize + Send + Sync + 'static,
    S: Store + 'a,
{
    let stream = async_stream::stream! {
        let mut writer = ConfirmationWindow::new(store, record, depth);
        while let Some((block, event)) = source.next().await {
            writer.record(block, &event).await;
            yield event;
        }
    };
    Box::pin(stream)
}
```

- [ ] **Step 2: Switch the live segment to it**

In `subscribe`, replace the live segment's writer call (currently
`let live = persist_and_emit(live_source, &self.store, record, false);`) with:

```rust
        let live =
            persist_and_emit_windowed(live_source, &self.store, record, self.confirmation_depth);
```

Leave the backfill segment's `persist_and_emit(backfill_source, &self.store, record.clone(), true)` unchanged.

- [ ] **Step 3: Verify existing persistence tests still pass**

Run: `cargo test --lib persistence`
Run: `cargo test --test persistence` (requires the in-memory SQLite tests; no anvil needed for the live-tail ones, but the suite also has anvil tests — run `cargo test --test persistence` and expect the non-anvil cases to pass; if anvil is absent, those specific cases are skipped/fail to spawn — note which).

Expected: `persisted_records_live_events_per_complete_block` (live 10,10,11 → flush 10, leave 11) still passes at default depth 1; backfill tests unchanged.

- [ ] **Step 4: Commit**

```bash
git add src/persistence/persisted.rs
git commit -m "Persist the live tail through the ConfirmationWindow

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 5: Integration test for a live reorg at depth 2

**Files:**
- Modify: `tests/persistence.rs`

- [ ] **Step 1: Add the depth-2 correction test**

Use the existing `FakeCollector`/`SqliteStore`/`value_event`/`stored_values`
helpers. A live sequence re-emits block 10 before it matures at depth 2:

```rust
/// At confirmation depth 2, a block re-emitted before it matures (a shallow
/// reorg) is corrected in the buffer: the store ends with the canonical row,
/// never the orphaned one.
#[tokio::test]
async fn confirmation_depth_corrects_a_shallow_reorg() {
    let store = Arc::new(SqliteStore::connect("sqlite::memory:").await.unwrap());

    // Live: block 10 (value 1), block 11 (value 2), then block 10 re-emitted
    // (value 3 — the reorg), then 11,12,13 advance so the corrected 10 and 11
    // mature at depth 2.
    let collector = FakeCollector::default()
        .live(vec![(10, 1), (11, 2), (10, 3), (11, 4), (12, 5), (13, 6)])
        .tip(9); // live filter is > tip, so all of the above pass

    let persisted = collector
        .with_persistence(store.clone())
        .with_confirmation_depth(2);

    let _events: Vec<ValueSet> = persisted.subscribe().await.unwrap().collect().await;

    // Block 10 matures once head reaches 12 (10+2). Its stored value is the
    // corrected 3, not the orphaned 1. Block 11's stored value is 4, not 2.
    assert_eq!(store.last_block("value_set").await.unwrap(), Some(11));
    assert_eq!(
        stored_values(&store).await,
        vec!["0x3".to_string(), "0x4".to_string()],
        "the store holds the corrected chain, never the orphaned rows"
    );
}
```

> Verify the exact `stored_values` formatting against the existing tests
> (`"0x1"` style) and adjust the expected strings to the encoding the helper
> produces. The block/value pairs may need tuning so blocks 10 and 11 mature
> (head must reach `block + 2`) while 12 and 13 stay buffered.

- [ ] **Step 2: Add an explicit depth-1 regression**

```rust
/// The default (no confirmation-depth override) is depth 1: a block flushes
/// when the next block arrives, and the open block stays unflushed.
#[tokio::test]
async fn default_confirmation_depth_is_one() {
    let store = Arc::new(SqliteStore::connect("sqlite::memory:").await.unwrap());
    let collector = FakeCollector::default().live(vec![(10, 1), (10, 2), (11, 3)]);
    let persisted = collector.with_persistence(store.clone());

    let _events: Vec<ValueSet> = persisted.subscribe().await.unwrap().collect().await;

    assert_eq!(store.last_block("value_set").await.unwrap(), Some(10));
    assert_eq!(
        stored_values(&store).await,
        vec!["0x1".to_string(), "0x2".to_string()]
    );
}
```

- [ ] **Step 3: Run the integration tests**

Run: `cargo test --test persistence confirmation_depth_corrects_a_shallow_reorg default_confirmation_depth_is_one`
Expected: both PASS. Tune the block/value sequence in step 1 if maturation timing is off (the assertion comments explain the `head >= block + depth` rule).

- [ ] **Step 4: Commit**

```bash
git add tests/persistence.rs
git commit -m "Integration-test confirmation-depth reorg correction

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 6: Docs

**Files:**
- Modify: `README.md`
- Modify: `CONTEXT.md`

- [ ] **Step 1: README Persistence paragraph**

In the "## Persistence" section of `README.md`, after the backfill paragraph
(around README line 119), add:

```markdown
By default a block is persisted once the next block arrives. Set
`.with_confirmation_depth(n)` to persist a block only once it is `n` blocks
deep: events are still delivered to strategies live and immediately, but the
write to the store lags `n` blocks, so a reorg shallower than `n` is corrected
in the buffer before any orphaned row is written. A reorg deeper than `n` halts
persistence and a restart re-syncs, so choose `n` above the deepest reorg you
expect.
```

- [ ] **Step 2: CONTEXT.md term and updates**

Add after the **Live Tail** entry (CONTEXT.md line ~93):

```markdown
**Confirmation Depth**:
The number of blocks a block must be buried under before the Persisted Collector writes it (default 1). The Live Tail buffers the most recent Confirmation-Depth blocks; a reorg shallower than the depth is corrected in the buffer before any orphaned row is written, while a reorg deeper than it halts persistence and a restart re-syncs. Events are still delivered live and immediately — only the Store write lags.
_Avoid_: finality, confirmations count, lag
```

Update the **Live Tail** entry to note the lag:

```markdown
**Live Tail**:
The unbounded Segment following the chain tip, strictly above the Backfill's cut (`> tip`). Persistence lags the live edge by the **Confirmation Depth**: the most recent depth blocks are buffered unwritten, and a restart re-fetches that whole window (not just a single open block) via Backfill.
_Avoid_: live stream, subscription
```

Update the **Backfill** entry's reorg sentence and the **Persisted Collector**
relationship to state that shallow reorgs are absorbed by the live tail's
confirmation window rather than halting persistence (deeper-than-depth reorgs
still halt).

- [ ] **Step 3: Commit**

```bash
git add README.md CONTEXT.md
git commit -m "Document confirmation-depth lag in README and CONTEXT

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 7: Full verification

- [ ] **Step 1: Format, lint, unit tests**

```bash
cargo fmt --all -- --check
RUSTFLAGS="-Dwarnings" cargo clippy --all-features
cargo test --lib
```

Expected: clean; all persistence unit tests (existing + new windowed-writer) pass.

- [ ] **Step 2: Integration suite**

```bash
command -v anvil >/dev/null && cargo test --all-features || cargo test --test persistence
```

Expected: the new SQLite-backed tests and the existing persistence tests pass; the anvil-backed cases pass when `anvil` is present.

- [ ] **Step 3: Docs build**

```bash
cargo doc --no-deps
```

Expected: no rustdoc warnings on the new `with_confirmation_depth` doc or the `ConfirmationWindow` references.
