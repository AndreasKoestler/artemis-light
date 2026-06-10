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
