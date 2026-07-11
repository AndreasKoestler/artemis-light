use std::sync::Arc;
use std::time::Duration;

use crate::types::Executor;
use anyhow::{Context, Result};
use async_trait::async_trait;

use super::pricing::{FeeEstimate, Fees, GasBidInfo, escalate, price_1559};

use alloy::{
    network::TransactionBuilder,
    primitives::TxHash,
    providers::{PendingTransactionBuilder, PendingTransactionError, Provider, WatchTxError},
    rpc::types::eth::TransactionRequest,
};

const DEFAULT_RPC_TIMEOUT: Duration = Duration::from_secs(10);

/// Gas-limit headroom over the `eth_estimateGas` result, as a divisor
/// (estimate/5 = +20%): state can drift between estimation and inclusion, and
/// a limit pinned to the bare estimate turns that drift into an out-of-gas
/// revert that burns the whole limit.
const GAS_LIMIT_HEADROOM_DIVISOR: u64 = 5;

/// The gas limit to submit for an `eth_estimateGas` result: the estimate plus
/// 20% headroom (see [`GAS_LIMIT_HEADROOM_DIVISOR`]), saturating.
fn gas_limit_with_headroom(estimate: u64) -> u64 {
    estimate.saturating_add(estimate / GAS_LIMIT_HEADROOM_DIVISOR)
}

/// A validated fee multiplier per replacement, as a percentage. Constructed
/// only through [`EscalationPercent::new`], which rejects anything below 110 —
/// a node rejects a replacement that does not raise both fee fields by ~10%, so
/// a smaller bump could never land. Carrying the bound in the type makes an
/// invalid [`ReplacementPolicy`] unrepresentable.
#[derive(Debug, Clone, Copy)]
pub struct EscalationPercent(u64);

impl EscalationPercent {
    /// The smallest bump a node accepts as a replacement (~10% over the
    /// original, rounded up).
    pub const MIN: u64 = 110;

    /// A fee multiplier of `percent`, or an error if it is below
    /// [`MIN`](Self::MIN).
    pub fn new(percent: u64) -> Result<Self> {
        if percent < Self::MIN {
            anyhow::bail!(
                "escalation_percent must be >= {} to clear the node's minimum \
                 replacement bump; got {percent}",
                Self::MIN
            );
        }
        Ok(Self(percent))
    }

    /// The percentage as a plain integer.
    pub fn get(self) -> u64 {
        self.0
    }
}

/// When and how to replace a transaction that has not confirmed.
#[derive(Debug, Clone, Copy)]
pub struct ReplacementPolicy {
    /// How long to wait for a mined transaction before replacing it.
    pub confirmation_timeout: Duration,
    /// How many escalated resubmissions after the original (0 = watch only).
    pub max_replacements: u32,
    /// Fee multiplier per replacement; see [`EscalationPercent`].
    pub escalation_percent: EscalationPercent,
}

/// How one confirmation watch ended: mined, genuinely unmined at the
/// confirmation timeout, or a transport failure — which says nothing about the
/// transaction either way.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WatchOutcome {
    Confirmed,
    TimedOut,
    TransportError,
}

impl WatchOutcome {
    fn classify(result: &Result<TxHash, PendingTransactionError>) -> Self {
        match result {
            Ok(_) => Self::Confirmed,
            Err(PendingTransactionError::TxWatcher(WatchTxError::Timeout)) => Self::TimedOut,
            Err(_) => Self::TransportError,
        }
    }
}

/// The replacement loop's next move after a watch ended, decided by
/// [`ReplacementSchedule::next_step`] — pure, so the timeout-vs-transport
/// distinction is testable without a chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NextStep {
    /// The transaction mined.
    Confirmed,
    /// Escalate to these fees and resend at the same nonce.
    Replace(Fees),
    /// Watch the same submission again without burning a replacement: a
    /// transport error says nothing about whether the transaction mined.
    Rewatch,
    /// Budget exhausted — check every sent hash's receipt, then give up.
    GiveUp,
}

/// The escalate-or-give-up half of the replacement loop, factored out of the
/// I/O so the fee schedule and the give-up boundary are testable without a
/// chain — the execution-side counterpart of the collector-side
/// [`ReconnectPolicy`](crate::engine::reconnect::ReconnectPolicy): this owns the
/// fee schedule and the replacement counter; [`send_with_replacement`] supplies
/// the actual send and confirmation watch.
///
/// [`send_with_replacement`]: MempoolExecutor::send_with_replacement
struct ReplacementSchedule {
    escalation_percent: u64,
    max_replacements: u32,
    /// The fees the *next* submission would use; advanced by [`escalate`].
    ///
    /// [`escalate`]: ReplacementSchedule::escalate
    fees: Fees,
    /// Replacements issued so far (0 = only the original has been sent).
    issued: u32,
    /// Consecutive watch transport failures since the last watch that reached
    /// the chain; reset by a confirmed or timed-out watch.
    transport_failures: u32,
}

impl ReplacementSchedule {
    /// Consecutive watch transport failures tolerated (each answered with a
    /// re-watch) before the loop gives up rather than spinning forever.
    const MAX_TRANSPORT_FAILURES: u32 = 3;

    fn new(policy: ReplacementPolicy, initial: Fees) -> Self {
        Self {
            escalation_percent: policy.escalation_percent.get(),
            max_replacements: policy.max_replacements,
            fees: initial,
            issued: 0,
            transport_failures: 0,
        }
    }

    /// The loop's next move after a watch ended. A confirmation timeout is the
    /// chain saying "still unmined", so it burns a replacement; a transport
    /// error says nothing about the transaction, so the same submission is
    /// watched again without burning one, up to
    /// [`MAX_TRANSPORT_FAILURES`](Self::MAX_TRANSPORT_FAILURES) in a row.
    fn next_step(&mut self, outcome: WatchOutcome) -> NextStep {
        match outcome {
            WatchOutcome::Confirmed => NextStep::Confirmed,
            WatchOutcome::TimedOut => {
                // The watch reached the chain: the transport recovered.
                self.transport_failures = 0;
                match self.escalate() {
                    Some(fees) => NextStep::Replace(fees),
                    None => NextStep::GiveUp,
                }
            }
            WatchOutcome::TransportError => {
                self.transport_failures += 1;
                if self.transport_failures > Self::MAX_TRANSPORT_FAILURES {
                    NextStep::GiveUp
                } else {
                    NextStep::Rewatch
                }
            }
        }
    }

    /// After a submission failed to confirm, escalate to the next replacement's
    /// fees, or return `None` once `max_replacements` escalations have been
    /// issued — the signal to give up. The `priority <= max_fee` invariant is
    /// preserved by the [`escalate`](crate::executors::escalate) fee math.
    fn escalate(&mut self) -> Option<Fees> {
        if self.issued >= self.max_replacements {
            return None;
        }
        self.issued += 1;
        self.fees = escalate(self.fees, self.escalation_percent);
        Some(self.fees)
    }

    /// Replacements issued so far.
    fn issued(&self) -> u32 {
        self.issued
    }
}

/// An executor that sends transactions to the mempool.
pub struct MempoolExecutor<M> {
    client: Arc<M>,
    rpc_timeout: Duration,
    /// Percentage applied to the provider's suggested priority fee (100 = as-is).
    priority_fee_bump_percent: u64,
    /// When set, watch for confirmation and replace a stuck transaction.
    replacement: Option<ReplacementPolicy>,
}

impl<M: Provider> MempoolExecutor<M> {
    /// Creates a new `MempoolExecutor` with default settings.
    pub fn new(client: Arc<M>) -> Self {
        Self {
            client,
            rpc_timeout: DEFAULT_RPC_TIMEOUT,
            priority_fee_bump_percent: 100,
            replacement: None,
        }
    }

    /// Sets the timeout for individual RPC calls.
    pub fn with_rpc_timeout(mut self, timeout: Duration) -> Self {
        self.rpc_timeout = timeout;
        self
    }

    /// Scale the provider's suggested priority fee by `percent` (100 = as-is).
    pub fn with_priority_fee_bump(mut self, percent: u64) -> Self {
        self.priority_fee_bump_percent = percent;
        self
    }

    /// Watch each submission for confirmation and replace it at an escalated
    /// fee if it stays unmined. Requires `tx.from` to be set on each action so
    /// the nonce can be pinned across replacements. Use this *or* the
    /// [`retry`](crate::executor_ext::ExecutorExt::retry) wrapper, not both:
    /// `retry` resubmits on a send error, replacement resubmits a sent-but-
    /// unmined transaction.
    ///
    /// The policy's [`EscalationPercent`] already guarantees each replacement
    /// raises both fee fields enough to clear the node's minimum bump.
    ///
    /// The watch loop runs inside `execute`, so one stuck action serialises
    /// this executor's task for up to `(max_replacements + 1) ×
    /// confirmation_timeout`; actions broadcast meanwhile sit in the bounded
    /// action channel and can be dropped if it wraps — size the engine's
    /// `action_channel_capacity` (and this timeout) accordingly.
    pub fn with_replacement(mut self, policy: ReplacementPolicy) -> Self {
        self.replacement = Some(policy);
        self
    }

    /// Fire-and-forget: submit the (already 1559-priced) transaction once and
    /// return without watching for confirmation.
    async fn send_and_forget(&self, tx: TransactionRequest) -> Result<()> {
        let _pending = tokio::time::timeout(self.rpc_timeout, self.client.send_transaction(tx))
            .await
            .context("Timeout sending transaction")?
            .context("Error sending transaction")?;
        Ok(())
    }

    /// Pin the nonce, submit, and watch for confirmation; on each confirmation
    /// timeout escalate the fee per the [`ReplacementSchedule`] and resend at
    /// the same nonce, until the transaction confirms or the schedule is
    /// exhausted. `initial_fees` are the priced fees already set on `tx` — the
    /// schedule escalates from there.
    ///
    /// Every sent hash is remembered, and every failure verdict — an exhausted
    /// schedule or a rejected replacement — first sweeps their receipts via
    /// [`any_mined`](Self::any_mined): a submission that mined behind the
    /// executor's back (e.g. just after the timeout, making the replacement
    /// fail "nonce too low") is a success, and reporting it as a failure would
    /// feed the circuit breaker and could re-fire the trade.
    async fn send_with_replacement(
        &self,
        mut tx: TransactionRequest,
        initial_fees: Fees,
        policy: ReplacementPolicy,
    ) -> Result<()> {
        // Pin the nonce so each resend replaces the prior rather than queuing.
        let from = tx
            .from
            .context("replacement requires `tx.from` to pin the nonce")?;
        let nonce = tokio::time::timeout(
            self.rpc_timeout,
            self.client.get_transaction_count(from).pending(),
        )
        .await
        .context("Timeout fetching nonce")?
        .context("Error fetching nonce")?;
        tx.set_nonce(nonce);

        let mut pending =
            tokio::time::timeout(self.rpc_timeout, self.client.send_transaction(tx.clone()))
                .await
                .context("Timeout sending transaction")?
                .context("Error sending transaction")?;
        let mut sent = vec![*pending.tx_hash()];

        let mut schedule = ReplacementSchedule::new(policy, initial_fees);
        loop {
            let watched = pending
                .with_timeout(Some(policy.confirmation_timeout))
                .watch()
                .await;
            let outcome = WatchOutcome::classify(&watched);
            // `NextStep::Rewatch` only follows `WatchOutcome::TransportError`,
            // which `classify` only returns for `watched: Err(_)` — so
            // `watched.err()` below is always `Some` on that branch. Matched
            // explicitly rather than `expect`-ed so a future change to that
            // invariant surfaces as a returned error, not a panic.
            let transport_err = watched.err();
            match schedule.next_step(outcome) {
                NextStep::Confirmed => return Ok(()),
                NextStep::GiveUp => {
                    if self.any_mined(&sent).await {
                        return Ok(());
                    }
                    return Err(anyhow::anyhow!(
                        "transaction unconfirmed after {} replacement(s)",
                        schedule.issued()
                    ));
                }
                NextStep::Rewatch => {
                    let Some(err) = transport_err else {
                        return Err(anyhow::anyhow!(
                            "rewatch requested without a transport error"
                        ));
                    };
                    let Some(&hash) = sent.last() else {
                        return Err(anyhow::anyhow!("no transaction hash recorded to re-watch"));
                    };
                    // `watch` consumed the builder (alloy's
                    // `PendingTransactionBuilder` is not `Clone`); mint a fresh
                    // watcher on the same hash rather than resending.
                    tracing::warn!(%hash, "confirmation watch failed ({err:#}); re-watching");
                    pending = PendingTransactionBuilder::new(self.client.root().clone(), hash);
                }
                NextStep::Replace(next) => {
                    tracing::warn!(
                        replacement = schedule.issued(),
                        "transaction unconfirmed; replacing at escalated fee"
                    );
                    tx.set_max_fee_per_gas(next.max_fee_per_gas());
                    tx.set_max_priority_fee_per_gas(next.max_priority_fee_per_gas());
                    let resent = tokio::time::timeout(
                        self.rpc_timeout,
                        self.client.send_transaction(tx.clone()),
                    )
                    .await
                    .context("Timeout sending replacement")
                    .and_then(|sent| sent.context("Error sending replacement"));
                    pending = match resent {
                        Ok(pending) => pending,
                        // A rejected replacement — "nonce too low" — is the
                        // shape of an earlier submission having just mined.
                        Err(e) => {
                            if self.any_mined(&sent).await {
                                return Ok(());
                            }
                            return Err(e);
                        }
                    };
                    sent.push(*pending.tx_hash());
                }
            }
        }
    }

    /// Whether any of the given transaction hashes has a mined receipt — the
    /// last check behind every failure verdict, so a transaction that actually
    /// moved funds is never reported as a failure. Best-effort: a lookup error
    /// or timeout counts as unmined and the sweep moves on.
    async fn any_mined(&self, hashes: &[TxHash]) -> bool {
        for &hash in hashes {
            match tokio::time::timeout(self.rpc_timeout, self.client.get_transaction_receipt(hash))
                .await
            {
                Ok(Ok(Some(_))) => {
                    tracing::info!(%hash, "a previously sent transaction mined");
                    return true;
                }
                Ok(Ok(None)) => {}
                Ok(Err(e)) => tracing::warn!(%hash, "receipt lookup failed: {e:#}"),
                Err(_) => tracing::warn!(%hash, "receipt lookup timed out"),
            }
        }
        false
    }
}

#[derive(Debug, Clone)]
pub struct SubmitTxToMempool {
    pub tx: TransactionRequest,
    pub gas_bid_info: Option<GasBidInfo>,
}

#[async_trait]
impl<M> Executor<SubmitTxToMempool> for MempoolExecutor<M>
where
    M: Provider,
{
    /// Send a transaction to the mempool.
    async fn execute(&mut self, mut action: SubmitTxToMempool) -> Result<()> {
        // Refuse an over-100% bid before spending any RPC on it: it would
        // price gas above the opportunity's total profit, making the
        // transaction itself the loss.
        if let Some(bid) = &action.gas_bid_info
            && bid.bid_percentage > 100
        {
            return Err(anyhow::anyhow!(
                "bid_percentage {} exceeds 100: the gas bid would cost more \
                 than the opportunity's total profit",
                bid.bid_percentage
            ));
        }

        let gas_usage = tokio::time::timeout(
            self.rpc_timeout,
            self.client.estimate_gas(action.tx.clone()),
        )
        .await
        .context("Timeout estimating gas usage")?
        .context("Error estimating gas usage")?;

        let estimate = {
            let est = tokio::time::timeout(self.rpc_timeout, self.client.estimate_eip1559_fees())
                .await
                .context("Timeout estimating EIP-1559 fees")?
                .context("Error estimating EIP-1559 fees")?;
            FeeEstimate {
                max_fee_per_gas: est.max_fee_per_gas,
                max_priority_fee_per_gas: est.max_priority_fee_per_gas,
            }
        };

        let fees = price_1559(
            estimate,
            gas_usage,
            self.priority_fee_bump_percent,
            action.gas_bid_info.as_ref(),
        )?;

        // The estimate priced the bid; set the gas limit too (with headroom),
        // so the provider's filler doesn't estimate a second time (an extra
        // RPC per action, and a limit that could diverge from the one priced).
        action.tx.set_gas_limit(gas_limit_with_headroom(gas_usage));
        action.tx.set_max_fee_per_gas(fees.max_fee_per_gas());
        action
            .tx
            .set_max_priority_fee_per_gas(fees.max_priority_fee_per_gas());

        // Fire-and-forget unless a replacement policy is configured; the
        // confirmation-watch-and-escalate loop lives in `send_with_replacement`.
        match self.replacement {
            None => self.send_and_forget(action.tx).await,
            Some(policy) => self.send_with_replacement(action.tx, fees, policy).await,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn fees(max_fee: u128, priority: u128) -> Fees {
        Fees::clamped(max_fee, priority)
    }

    fn policy(max_replacements: u32, escalation_percent: u64) -> ReplacementPolicy {
        ReplacementPolicy {
            confirmation_timeout: Duration::from_millis(1),
            max_replacements,
            escalation_percent: EscalationPercent::new(escalation_percent).unwrap(),
        }
    }

    /// The gas limit carries 20% headroom over the estimate, so state drift
    /// between estimation and inclusion doesn't turn into an out-of-gas revert.
    #[test]
    fn gas_limit_carries_twenty_percent_headroom_over_the_estimate() {
        assert_eq!(gas_limit_with_headroom(100_000), 120_000);
    }

    #[test]
    fn gas_limit_headroom_saturates_instead_of_overflowing() {
        assert_eq!(gas_limit_with_headroom(u64::MAX), u64::MAX);
    }

    /// `max_replacements = 0` means watch-only: the first unconfirmed result is
    /// the give-up signal, with no escalated resend. Exercises the give-up
    /// boundary that previously needed a live chain to reach.
    #[test]
    fn schedule_gives_up_immediately_when_no_replacements_allowed() {
        let mut schedule = ReplacementSchedule::new(policy(0, 125), fees(200, 20));
        assert_eq!(schedule.escalate(), None);
        assert_eq!(schedule.issued(), 0);
    }

    /// Each replacement escalates the previous fees, compounding, and the
    /// schedule gives up after exactly `max_replacements` escalations.
    #[test]
    fn schedule_escalates_each_replacement_then_gives_up() {
        let mut schedule = ReplacementSchedule::new(policy(2, 125), fees(200, 20));

        // First replacement: 200 -> 250, 20 -> 25.
        assert_eq!(schedule.escalate(), Some(fees(250, 25)));
        // Second replacement compounds: 250 -> 312, 25 -> 31.
        assert_eq!(schedule.escalate(), Some(fees(312, 31)));
        // Budget exhausted: give up, and the count stays at the cap.
        assert_eq!(schedule.escalate(), None);
        assert_eq!(schedule.issued(), 2);
    }

    /// Whatever the fee path, the escalated fees never violate the EIP-1559
    /// invariant — the schedule delegates to
    /// [`escalate`](crate::executors::escalate), which builds through
    /// [`Fees::clamped`](crate::executors::Fees::clamped).
    #[test]
    fn schedule_preserves_the_eip1559_invariant_across_replacements() {
        let mut schedule = ReplacementSchedule::new(policy(3, 130), fees(200, 200));
        while let Some(f) = schedule.escalate() {
            assert!(f.max_priority_fee_per_gas() <= f.max_fee_per_gas());
        }
    }

    #[test]
    fn a_confirmed_watch_ends_the_loop() {
        let mut schedule = ReplacementSchedule::new(policy(0, 125), fees(200, 20));
        assert_eq!(
            schedule.next_step(WatchOutcome::Confirmed),
            NextStep::Confirmed
        );
    }

    /// A confirmation timeout is the chain saying "still unmined": it burns a
    /// replacement, and an exhausted budget is the give-up signal.
    #[test]
    fn a_confirmation_timeout_burns_a_replacement_step() {
        let mut schedule = ReplacementSchedule::new(policy(1, 125), fees(200, 20));
        assert_eq!(
            schedule.next_step(WatchOutcome::TimedOut),
            NextStep::Replace(fees(250, 25))
        );
        assert_eq!(schedule.next_step(WatchOutcome::TimedOut), NextStep::GiveUp);
    }

    /// A transport error says nothing about the transaction: the loop watches
    /// the same submission again without spending a fee escalation on it.
    #[test]
    fn a_transport_error_rewatches_without_burning_a_replacement() {
        let mut schedule = ReplacementSchedule::new(policy(1, 125), fees(200, 20));
        assert_eq!(
            schedule.next_step(WatchOutcome::TransportError),
            NextStep::Rewatch
        );
        // The replacement budget is untouched: the next timeout still escalates.
        assert_eq!(
            schedule.next_step(WatchOutcome::TimedOut),
            NextStep::Replace(fees(250, 25))
        );
    }

    /// Transport errors don't burn replacements, so on their own they must
    /// still reach a give-up boundary rather than re-watching forever.
    #[test]
    fn persistent_transport_errors_give_up_instead_of_looping_forever() {
        let mut schedule = ReplacementSchedule::new(policy(5, 125), fees(200, 20));
        for _ in 0..ReplacementSchedule::MAX_TRANSPORT_FAILURES {
            assert_eq!(
                schedule.next_step(WatchOutcome::TransportError),
                NextStep::Rewatch
            );
        }
        assert_eq!(
            schedule.next_step(WatchOutcome::TransportError),
            NextStep::GiveUp
        );
    }

    /// A watch that reaches the chain (even one reporting "unmined") proves the
    /// transport recovered, so the consecutive-failure count starts over.
    #[test]
    fn a_watch_that_reaches_the_chain_resets_the_transport_failure_count() {
        let mut schedule = ReplacementSchedule::new(policy(5, 125), fees(200, 20));
        for _ in 0..ReplacementSchedule::MAX_TRANSPORT_FAILURES {
            assert_eq!(
                schedule.next_step(WatchOutcome::TransportError),
                NextStep::Rewatch
            );
        }
        assert!(matches!(
            schedule.next_step(WatchOutcome::TimedOut),
            NextStep::Replace(_)
        ));
        assert_eq!(
            schedule.next_step(WatchOutcome::TransportError),
            NextStep::Rewatch
        );
    }

    /// The receipt sweep behind every failure verdict, against a mocked
    /// provider: doubles at the `Provider` seam, mirroring the `Executor`-seam
    /// doubles in `executor_ext::test_support`.
    mod receipt_sweep {
        use super::*;
        use alloy::consensus::{Receipt, ReceiptEnvelope, ReceiptWithBloom};
        use alloy::primitives::TxHash;
        use alloy::providers::{Provider, ProviderBuilder, mock::Asserter};
        use alloy::rpc::types::eth::TransactionReceipt;

        fn mocked_executor(asserter: &Asserter) -> MempoolExecutor<impl Provider> {
            let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
            MempoolExecutor::new(Arc::new(provider))
        }

        fn mined_receipt(hash: TxHash) -> TransactionReceipt {
            TransactionReceipt {
                inner: ReceiptEnvelope::Eip1559(ReceiptWithBloom {
                    receipt: Receipt {
                        status: true.into(),
                        cumulative_gas_used: 21_000,
                        logs: vec![],
                    },
                    logs_bloom: Default::default(),
                }),
                transaction_hash: hash,
                transaction_index: Some(0),
                block_hash: Some(Default::default()),
                block_number: Some(1),
                gas_used: 21_000,
                effective_gas_price: 1,
                blob_gas_used: None,
                blob_gas_price: None,
                from: Default::default(),
                to: None,
                contract_address: None,
            }
        }

        /// The original mined behind the executor's back while a replacement
        /// was being watched: the sweep over *all* sent hashes finds it.
        #[tokio::test]
        async fn any_mined_finds_a_receipt_behind_an_unmined_hash() {
            let asserter = Asserter::new();
            let executor = mocked_executor(&asserter);
            let (original, replacement) = (TxHash::with_last_byte(1), TxHash::with_last_byte(2));
            asserter.push_success(&serde_json::Value::Null); // original: pending?
            asserter.push_success(&mined_receipt(replacement)); // replacement: mined
            assert!(executor.any_mined(&[original, replacement]).await);
        }

        #[tokio::test]
        async fn any_mined_is_false_when_no_sent_hash_has_a_receipt() {
            let asserter = Asserter::new();
            let executor = mocked_executor(&asserter);
            asserter.push_success(&serde_json::Value::Null);
            asserter.push_success(&serde_json::Value::Null);
            assert!(
                !executor
                    .any_mined(&[TxHash::with_last_byte(1), TxHash::with_last_byte(2)])
                    .await
            );
        }

        /// A lookup error on one hash must not abort the sweep — the next
        /// hash's receipt still turns the verdict into a success.
        #[tokio::test]
        async fn any_mined_keeps_sweeping_past_a_lookup_error() {
            let asserter = Asserter::new();
            let executor = mocked_executor(&asserter);
            let mined = TxHash::with_last_byte(2);
            asserter.push_failure_msg("receipt lookup failed");
            asserter.push_success(&mined_receipt(mined));
            assert!(
                executor
                    .any_mined(&[TxHash::with_last_byte(1), mined])
                    .await
            );
        }

        /// The tuning setters mutate their fields and hand the executor back,
        /// so a full configuration is one chained expression.
        #[test]
        fn builder_setters_apply() {
            let asserter = Asserter::new();
            let executor = mocked_executor(&asserter)
                .with_rpc_timeout(Duration::from_secs(2))
                .with_priority_fee_bump(150);
            assert_eq!(executor.rpc_timeout, Duration::from_secs(2));
            assert_eq!(executor.priority_fee_bump_percent, 150);
        }

        /// Replacement pins the nonce off `tx.from`; without it there is nothing
        /// to pin, so the submit fails before any RPC is spent.
        #[tokio::test]
        async fn send_with_replacement_requires_from_to_pin_the_nonce() {
            let asserter = Asserter::new();
            let executor = mocked_executor(&asserter);
            let err = executor
                .send_with_replacement(TransactionRequest::default(), fees(200, 20), policy(1, 125))
                .await
                .unwrap_err();
            assert!(
                err.to_string().contains("tx.from"),
                "expected a `tx.from` diagnostic, got: {err}"
            );
        }
    }
}
