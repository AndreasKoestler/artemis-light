use std::ops::{Div, Mul};
use std::sync::Arc;
use std::time::Duration;

use crate::types::Executor;
use anyhow::{Context, Result};
use async_trait::async_trait;

use alloy::{
    network::TransactionBuilder, providers::Provider, rpc::types::eth::TransactionRequest,
};

const DEFAULT_RPC_TIMEOUT: Duration = Duration::from_secs(10);

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

/// Price a transaction's EIP-1559 fees. See the module/spec for the algorithm.
// `allow(dead_code)`: wired into `execute` in a later step; the pricing
// function and its tests land first.
#[allow(dead_code)]
fn price_1559(
    est: FeeEstimate,
    gas_usage: u64,
    bump_percent: u64,
    bid: Option<&GasBidInfo>,
) -> anyhow::Result<Fees> {
    let base_headroom = est
        .max_fee_per_gas
        .saturating_sub(est.max_priority_fee_per_gas);
    let bumped_priority = est.max_priority_fee_per_gas * bump_percent as u128 / 100;

    let (max_fee_per_gas, max_priority_fee_per_gas) = match bid {
        Some(bid) => {
            if bid.bid_percentage > 100 {
                return Err(anyhow::anyhow!(
                    "bid_percentage {} exceeds 100: the gas bid would cost more \
                     than the opportunity's total profit",
                    bid.bid_percentage
                ));
            }
            if gas_usage == 0 {
                return Err(anyhow::anyhow!(
                    "Gas estimation returned 0, cannot calculate bid price"
                ));
            }
            let breakeven = bid.total_profit / gas_usage as u128;
            let max_fee = breakeven * bid.bid_percentage as u128 / 100;
            // Preserve the EIP-1559 invariant: priority can't exceed max_fee.
            (max_fee, bumped_priority.min(max_fee))
        }
        None => (base_headroom + bumped_priority, bumped_priority),
    };

    Ok(Fees {
        max_fee_per_gas,
        max_priority_fee_per_gas,
    })
}

/// An executor that sends transactions to the mempool.
pub struct MempoolExecutor<M> {
    client: Arc<M>,
    /// Timeout for individual RPC calls.
    rpc_timeout: Duration,
}

impl<M: Provider> MempoolExecutor<M> {
    /// Creates a new `MempoolExecutor` with default settings.
    pub fn new(client: Arc<M>) -> Self {
        Self {
            client,
            rpc_timeout: DEFAULT_RPC_TIMEOUT,
        }
    }

    /// Sets the timeout for individual RPC calls.
    pub fn with_rpc_timeout(mut self, timeout: Duration) -> Self {
        self.rpc_timeout = timeout;
        self
    }
}

/// Information about the gas bid for a transaction.
#[derive(Debug, Clone)]
pub struct GasBidInfo {
    /// Total profit expected from opportunity
    pub total_profit: u128,

    /// Percentage of bid profit to use for gas, at most 100: bidding the whole
    /// profit (100) breaks even; anything above it makes the transaction
    /// itself the loss, so the executor refuses such actions.
    pub bid_percentage: u64,
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
        if let Some(gas_bid_info) = &action.gas_bid_info
            && gas_bid_info.bid_percentage > 100
        {
            return Err(anyhow::anyhow!(
                "bid_percentage {} exceeds 100: the gas bid would cost more \
                 than the opportunity's total profit",
                gas_bid_info.bid_percentage
            ));
        }

        let gas_usage = tokio::time::timeout(
            self.rpc_timeout,
            self.client.estimate_gas(action.tx.clone()),
        )
        .await
        .context("Timeout estimating gas usage")?
        .context("Error estimating gas usage")?;

        let bid_gas_price;
        if let Some(gas_bid_info) = action.gas_bid_info {
            if gas_usage == 0 {
                return Err(anyhow::anyhow!(
                    "Gas estimation returned 0, cannot calculate bid price"
                ));
            }
            // gas price at which we'd break even, meaning 100% of profit goes to validator
            let breakeven_gas_price = gas_bid_info.total_profit / gas_usage as u128;
            // gas price corresponding to bid percentage
            bid_gas_price = breakeven_gas_price
                .mul(gas_bid_info.bid_percentage as u128)
                .div(100);
        } else {
            bid_gas_price = tokio::time::timeout(self.rpc_timeout, self.client.get_gas_price())
                .await
                .context("Timeout getting gas price")?
                .context("Error getting gas price")?;
        }
        // The estimate priced the bid; set it as the limit too, so the
        // provider's filler doesn't estimate a second time (an extra RPC per
        // action, and a limit that could diverge from the one priced).
        action.tx.set_gas_limit(gas_usage);
        action.tx.set_gas_price(bid_gas_price);
        let _pending_tx =
            tokio::time::timeout(self.rpc_timeout, self.client.send_transaction(action.tx))
                .await
                .context("Timeout sending transaction")?
                .context("Error sending transaction")?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn est(max_fee: u128, priority: u128) -> FeeEstimate {
        FeeEstimate {
            max_fee_per_gas: max_fee,
            max_priority_fee_per_gas: priority,
        }
    }

    #[test]
    fn no_bid_rides_priority_on_base_headroom() {
        // headroom = 100 - 10 = 90; bump 100% leaves priority = 10.
        let fees = price_1559(est(100, 10), 21_000, 100, None).unwrap();
        assert_eq!(fees.max_priority_fee_per_gas, 10);
        assert_eq!(fees.max_fee_per_gas, 90 + 10);
    }

    #[test]
    fn no_bid_applies_the_priority_bump() {
        // bump 150% on priority 10 -> 15; headroom 90 unchanged.
        let fees = price_1559(est(100, 10), 21_000, 150, None).unwrap();
        assert_eq!(fees.max_priority_fee_per_gas, 15);
        assert_eq!(fees.max_fee_per_gas, 90 + 15);
    }

    #[test]
    fn bid_caps_max_fee_at_the_breakeven_share() {
        // total_profit 2_100_000, gas 21_000 -> breakeven 100/gas unit;
        // bid 50% -> max_fee 50.
        let bid = GasBidInfo {
            total_profit: 2_100_000,
            bid_percentage: 50,
        };
        let fees = price_1559(est(1_000, 10), 21_000, 100, Some(&bid)).unwrap();
        assert_eq!(fees.max_fee_per_gas, 50);
        // priority (bumped 10) fits under the cap.
        assert_eq!(fees.max_priority_fee_per_gas, 10);
    }

    #[test]
    fn a_low_bid_cap_clamps_priority_to_max_fee() {
        // breakeven 100/unit, bid 5% -> max_fee 5; priority would be 10, clamps to 5.
        let bid = GasBidInfo {
            total_profit: 2_100_000,
            bid_percentage: 5,
        };
        let fees = price_1559(est(1_000, 10), 21_000, 100, Some(&bid)).unwrap();
        assert_eq!(fees.max_fee_per_gas, 5);
        assert_eq!(fees.max_priority_fee_per_gas, 5);
        assert!(fees.max_priority_fee_per_gas <= fees.max_fee_per_gas);
    }

    #[test]
    fn over_100_percent_bid_is_rejected() {
        let bid = GasBidInfo {
            total_profit: 100,
            bid_percentage: 101,
        };
        assert!(price_1559(est(100, 10), 21_000, 100, Some(&bid)).is_err());
    }

    #[test]
    fn zero_gas_with_a_bid_is_rejected() {
        let bid = GasBidInfo {
            total_profit: 100,
            bid_percentage: 50,
        };
        assert!(price_1559(est(100, 10), 0, 100, Some(&bid)).is_err());
    }

    #[test]
    fn a_malformed_estimate_yields_zero_headroom_without_panic() {
        // priority > max_fee: saturating headroom = 0; priority bump = 100.
        // max_fee = headroom (0) + bumped priority (100) = 100.
        let fees = price_1559(est(10, 100), 21_000, 100, None).unwrap();
        assert_eq!(fees.max_fee_per_gas, 100);
        assert_eq!(fees.max_priority_fee_per_gas, 100);
    }
}
