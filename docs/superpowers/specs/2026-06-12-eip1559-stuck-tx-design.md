# EIP-1559 Pricing and Stuck-Transaction Replacement

**Date:** 2026-06-12
**Status:** Approved

## Problem

`MempoolExecutor` prices transactions with the legacy `set_gas_price`
(`src/executors/mempool_executor.rs:107`) — at odds with the crate's
"modernised" framing and with every post-London chain. It is also
fire-and-forget: it discards the pending transaction, so a submission that
sticks in the mempool (priced below the moving base fee) is never noticed, let
alone replaced. An MEV bot whose transaction is stuck misses the opportunity
and has no recourse.

Two changes: price with EIP-1559 fields unconditionally, and add an opt-in
loop that watches for confirmation and resubmits a stuck transaction at an
escalated fee (the standard "speed-up", same nonce).

## Design

The behaviour that can be tested without a node — all the fee arithmetic —
lives in pure functions. The executor's `execute` is then a thin I/O shell
around them, exercised by anvil integration tests.

### Shared fee type

```rust
/// An EIP-1559 fee pair. Invariant: `max_priority_fee_per_gas <= max_fee_per_gas`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fees {
    pub max_fee_per_gas: u128,
    pub max_priority_fee_per_gas: u128,
}

/// A provider's EIP-1559 fee suggestion (the shape of
/// `Provider::estimate_eip1559_fees`).
#[derive(Debug, Clone, Copy)]
pub struct FeeEstimate {
    pub max_fee_per_gas: u128,
    pub max_priority_fee_per_gas: u128,
}
```

### Part A — EIP-1559 pricing (unconditional)

Replace the legacy `gas_price` path. The pure pricing function:

```rust
/// Price a transaction's EIP-1559 fees.
///
/// - `bump_percent` scales the provider's suggested priority fee (100 = as-is).
/// - With `bid`, the break-even gas price (`total_profit / gas_usage`) caps
///   `max_fee_per_gas`, scaled by `bid_percentage`; the priority fee is then
///   clamped to at most that cap. Without `bid`, the priority fee rides on top
///   of the provider's base-fee headroom.
///
/// Errors: `bid_percentage > 100` (the bid would cost more than the
/// opportunity's profit) and `gas_usage == 0` with a bid (cannot derive a
/// break-even). Mirrors the guards the legacy executor already had.
fn price_1559(
    est: FeeEstimate,
    gas_usage: u64,
    bump_percent: u64,
    bid: Option<&GasBidInfo>,
) -> anyhow::Result<Fees>;
```

Algorithm:

```
base_headroom    = est.max_fee_per_gas - est.max_priority_fee_per_gas
bumped_priority  = est.max_priority_fee_per_gas * bump_percent / 100

if let Some(bid) = bid:
    if bid.bid_percentage > 100  -> Err(over-100% bid)
    if gas_usage == 0            -> Err(zero gas)
    breakeven = bid.total_profit / gas_usage as u128
    max_fee   = breakeven * bid.bid_percentage as u128 / 100
    priority  = min(bumped_priority, max_fee)        // preserve the invariant
else:
    priority  = bumped_priority
    max_fee   = base_headroom + bumped_priority
Fees { max_fee_per_gas: max_fee, max_priority_fee_per_gas: priority }
```

`base_headroom` uses saturating subtraction (a malformed estimate where
priority exceeds max_fee yields 0 headroom rather than underflowing).

When a bid cap falls below the bumped priority, the priority is clamped down:
the bid is too low to be competitive, but the executor honours the profit
ceiling rather than overspending. This is intended and documented.

`GasBidInfo` is unchanged in shape (`total_profit`, `bid_percentage`); its
*meaning* shifts from "caps the legacy gas price" to "caps `max_fee_per_gas`".

### Part B — stuck-transaction replacement (opt-in)

```rust
/// When and how to replace a transaction that has not confirmed.
#[derive(Debug, Clone, Copy)]
pub struct ReplacementPolicy {
    /// How long to wait for a mined transaction before replacing it.
    pub confirmation_timeout: Duration,
    /// How many escalated resubmissions after the original (0 = watch only,
    /// never replace).
    pub max_replacements: u32,
    /// Fee multiplier per replacement, as a percentage. Must be >= 110 — a
    /// node rejects a replacement that does not raise both fee fields by ~10%.
    pub escalation_percent: u64,
}
```

The pure escalation function:

```rust
/// Escalate both fee fields for a replacement transaction. The caller's
/// `escalation_percent` (validated >= 110 at construction) guarantees the
/// node's minimum ~10% bump on both fields.
fn escalate(fees: Fees, escalation_percent: u64) -> Fees;
```

`escalate` multiplies both fields by `escalation_percent / 100` with saturating
arithmetic; because `escalation_percent >= 110`, both fields rise by at least
10%, and the `priority <= max_fee` invariant is preserved (both scale by the
same factor).

`MempoolExecutor` gains:

```rust
pub fn with_priority_fee_bump(self, percent: u64) -> Self;     // default 100
pub fn with_replacement(self, policy: ReplacementPolicy) -> Self;  // default None
```

`with_replacement` panics if `escalation_percent < 110`, the same fail-fast
shape as `rate_limit(0)`.

`execute` behaviour:

- **No replacement policy (default):** estimate fees, `price_1559`, set
  `max_fee`/`max_priority`/`gas_limit`, send, return — fire-and-forget, exactly
  today's latency profile but 1559-priced. Nonce is left to the provider's
  filler.
- **With a replacement policy:** the path requires `tx.from` to be set (so the
  nonce can be pinned and reused across replacements); a missing `from` is an
  error. Then:

```
from   = tx.from  (else Err: "replacement requires tx.from")
nonce  = provider.get_transaction_count(from)        // pending count
fees   = price_1559(estimate, gas_usage, bump, bid)
set nonce, gas_limit, fees; pending = send(tx)
for n in 0..=max_replacements:
    match pending.watch(confirmation_timeout):
        confirmed            -> return Ok(())
        timed out:
            if n == max_replacements -> return Err("unconfirmed after N replacements")
            fees = escalate(fees, escalation_percent)
            rebuild tx (same nonce, same gas_limit, new fees); pending = send(tx)
```

Every RPC call keeps the existing `rpc_timeout` wrapper.

### Composition note

With a replacement policy, `execute` blocks until the transaction confirms or
the replacements are exhausted. Use **replacement or `retry`, not both**: both
resubmit, but for different reasons — `retry` re-runs on a *send* error
(transient RPC), replacement resubmits a *successfully sent but unmined*
transaction at a higher fee. A `rate_limit` wrapper still counts one
submission per `execute` regardless of internal replacements. This is
documented on `with_replacement` and in CONTEXT.md.

## Testing

### Pure unit tests (no provider) — the backbone

In `src/executors/mempool_executor.rs`:

`price_1559`:
- No-bid: priority = `est.priority * bump/100`; max_fee = headroom + priority.
- No-bid, `bump_percent = 100`: fees equal the estimate (headroom + priority).
- Bid: `max_fee = breakeven * bid_percentage / 100`, where
  `breakeven = total_profit / gas_usage`.
- Bid with a low cap clamps priority to `max_fee` (invariant holds).
- `bid_percentage > 100` → `Err`.
- `gas_usage == 0` with a bid → `Err`.
- A malformed estimate (priority > max_fee) yields 0 headroom, no panic.

`escalate`:
- Both fields rise by exactly `escalation_percent`.
- At the minimum `escalation_percent = 110`, both fields rise ≥10%.
- The `priority <= max_fee` invariant survives escalation.
- Large fees use saturating multiplication (no overflow panic).

### Integration tests (anvil) — the I/O shell

In `tests/main.rs`, using `spawn_anvil_with_signer`:

- **1559 submission mines:** build a transfer, `execute` (no replacement),
  assert the transaction mines and its receipt carries 1559 fields
  (`effective_gas_price` present; the request used `max_fee_per_gas`).
- **Replacement speeds up a stuck tx (deterministic via manual mining):**
  disable auto-mining (`anvil_setAutomine(false)`), submit an underpriced
  transfer through an executor with a short `confirmation_timeout` and
  `max_replacements >= 1`; the timeout fires, the executor resubmits at the
  same nonce with escalated fees; then mine one block (`evm_mine`) and assert
  the mined transaction is the *replacement* (its `max_fee_per_gas` is the
  escalated value, and the original hash is absent). The exact anvil control
  RPCs and alloy pending-transaction timeout API are verified by the
  implementer against alloy 1.0.

## Docs

- README: the Components table's `MempoolExecutor` row notes EIP-1559 pricing
  and optional stuck-tx replacement; a short note on the replacement-vs-retry
  choice.
- CONTEXT.md:
  - **EIP-1559 Pricing**: the executor prices `max_fee_per_gas` /
    `max_priority_fee_per_gas` from the provider's fee estimate, with a
    configurable priority-fee bump; the `GasBidInfo` break-even caps `max_fee`.
    _Avoid_: gas price, legacy pricing.
  - **Replacement**: the opt-in loop that resubmits an unconfirmed transaction
    at the same nonce with escalated fees, up to `max_replacements`, after each
    `confirmation_timeout`. Distinct from the Executor **Retry** wrapper, which
    resubmits on a *send* error; replacement resubmits a *sent-but-unmined* tx.
    _Avoid_: resend, retry (that is the wrapper), speed-up.
  - **Confirmation Timeout**: how long the executor waits for a transaction to
    mine before escalating and replacing it. _Avoid_: deadline (that is the
    action-side wrapper), rpc_timeout (that bounds a single RPC call).
  - Update **GasBidInfo**'s description: the break-even now caps
    `max_fee_per_gas`, not the legacy gas price.
- `examples/onchain_example.rs`: the `gas_bid_info` comment is updated to
  1559 terms; no behavioural change to the example (default fire-and-forget).

## Out of scope

- A caller-supplied fee strategy (trait/closure for bespoke bidding): the
  provider estimate + bump covers the common case; a strategy hook can layer on
  later.
- Explicit nonce management across *concurrent* submissions (a nonce pool /
  reservation): the replacement path pins a single nonce per `execute`; a bot
  submitting many concurrent transactions through one executor is a separate
  concern.
- On-chain *outcome* classification (reverted vs. succeeded): replacement keys
  only on confirmation (mined), not on the receipt's status. Pairs with the
  separate execution-feedback / confirmation-watching work.
- Blob (EIP-4844) fees.
