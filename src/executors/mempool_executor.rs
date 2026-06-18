use std::sync::Arc;
use std::time::Duration;

use crate::types::Executor;
use anyhow::{Context, Result};
use async_trait::async_trait;

use super::pricing::{FeeEstimate, Fees, GasBidInfo, escalate, price_1559};

use alloy::{
    network::TransactionBuilder, providers::Provider, rpc::types::eth::TransactionRequest,
};

const DEFAULT_RPC_TIMEOUT: Duration = Duration::from_secs(10);

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
}

impl ReplacementSchedule {
    fn new(policy: ReplacementPolicy, initial: Fees) -> Self {
        Self {
            escalation_percent: policy.escalation_percent.get(),
            max_replacements: policy.max_replacements,
            fees: initial,
            issued: 0,
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
    /// Timeout for individual RPC calls.
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

    /// Pin the nonce, submit, and watch for confirmation; on each timeout
    /// escalate the fee per the [`ReplacementSchedule`] and resend at the same
    /// nonce, until the transaction confirms or the schedule is exhausted.
    /// `initial_fees` are the priced fees already set on `tx` — the schedule
    /// escalates from there.
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

        let mut schedule = ReplacementSchedule::new(policy, initial_fees);
        loop {
            // `watch` consumes the builder (alloy 1.0 `PendingTransactionBuilder`
            // is not `Clone`), so on timeout we resend to obtain a fresh one.
            match pending
                .with_timeout(Some(policy.confirmation_timeout))
                .watch()
                .await
            {
                Ok(_hash) => return Ok(()),
                Err(e) => {
                    let Some(next) = schedule.escalate() else {
                        return Err(anyhow::anyhow!(
                            "transaction unconfirmed after {} replacement(s)",
                            policy.max_replacements
                        ));
                    };
                    tracing::warn!(
                        replacement = schedule.issued(),
                        "transaction unconfirmed ({e:#}); replacing at escalated fee"
                    );
                    tx.set_max_fee_per_gas(next.max_fee_per_gas());
                    tx.set_max_priority_fee_per_gas(next.max_priority_fee_per_gas());
                    pending = tokio::time::timeout(
                        self.rpc_timeout,
                        self.client.send_transaction(tx.clone()),
                    )
                    .await
                    .context("Timeout sending replacement")?
                    .context("Error sending replacement")?;
                }
            }
        }
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

        // The estimate priced the bid; set the gas limit too, so the provider's
        // filler doesn't estimate a second time (an extra RPC per action, and a
        // limit that could diverge from the one priced).
        action.tx.set_gas_limit(gas_usage);
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
        Fees::new(max_fee, priority)
    }

    fn policy(max_replacements: u32, escalation_percent: u64) -> ReplacementPolicy {
        ReplacementPolicy {
            confirmation_timeout: Duration::from_millis(1),
            max_replacements,
            escalation_percent: EscalationPercent::new(escalation_percent).unwrap(),
        }
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
    /// [`Fees::new`](crate::executors::Fees::new).
    #[test]
    fn schedule_preserves_the_eip1559_invariant_across_replacements() {
        let mut schedule = ReplacementSchedule::new(policy(3, 130), fees(200, 200));
        while let Some(f) = schedule.escalate() {
            assert!(f.max_priority_fee_per_gas() <= f.max_fee_per_gas());
        }
    }
}
