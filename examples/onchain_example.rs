//! An end-to-end on-chain pipeline: `BlockCollector` events drive a strategy
//! whose actions a `MempoolExecutor` submits as real transactions.
//!
//! ```text
//! BlockCollector ──NewBlock──▶ TransferStrategy ──SubmitTxToMempool──▶ MempoolExecutor
//! ```
//!
//! The strategy sends a fixed-value transfer on each of the first few blocks;
//! the executor estimates gas, prices the transaction, and submits it to the
//! mempool. An observer counts submissions so the example knows when to stop,
//! and the recipient's balance proves the transfers landed.
//!
//! Requires `anvil` on `$PATH` (ships with Foundry). Run with:
//! ```sh
//! cargo run --example onchain_example
//! ```

use std::sync::Arc;
use std::time::Duration;

use alloy::network::TransactionBuilder;
use alloy::node_bindings::Anvil;
use alloy::primitives::{Address, U256};
use alloy::providers::{Provider, ProviderBuilder, WsConnect};
use alloy::rpc::types::TransactionRequest;
use alloy::signers::local::PrivateKeySigner;
use anyhow::Result;
use artemis_light::{
    collectors::{BlockCollector, NewBlock},
    engine::Engine,
    executors::{MempoolExecutor, SubmitTxToMempool},
    types::{ActionStream, Observer, Strategy},
};
use async_trait::async_trait;

const TRANSFERS: u64 = 3;
const TRANSFER_WEI: u64 = 1_000_000_000_000_000; // 0.001 ETH

/// On each new block, produce one transfer action — until the budget runs out.
struct TransferStrategy {
    to: Address,
    remaining: u64,
}

#[async_trait]
impl Strategy<NewBlock, SubmitTxToMempool> for TransferStrategy {
    /// Nothing to sync here; a real strategy would fetch its initial on-chain
    /// state (reserves, positions, ...) before processing live events.
    async fn sync_state(&mut self) -> Result<()> {
        Ok(())
    }

    async fn process_event(
        &mut self,
        event: NewBlock,
    ) -> Result<ActionStream<'_, SubmitTxToMempool>> {
        if self.remaining == 0 {
            return Ok(Box::pin(futures::stream::empty()));
        }
        self.remaining -= 1;
        println!(
            "[strategy] block #{}: submitting transfer ({} left)",
            event.number, self.remaining
        );

        let tx = TransactionRequest::default()
            .with_to(self.to)
            .with_value(U256::from(TRANSFER_WEI));
        // `gas_bid_info: None` bids the current network gas price. For an MEV
        // opportunity with a known profit, `Some(GasBidInfo { total_profit,
        // bid_percentage })` spends that share of the profit on gas instead.
        let action = SubmitTxToMempool {
            tx,
            gas_bid_info: None,
        };
        Ok(Box::pin(futures::stream::iter(vec![action])))
    }
}

/// Counts actions crossing the action channel and signals `done` once every
/// budgeted transfer has been handed to the executor.
struct SubmissionCounter {
    seen: u64,
    done: Option<tokio::sync::oneshot::Sender<()>>,
}

#[async_trait]
impl Observer<NewBlock, SubmitTxToMempool> for SubmissionCounter {
    async fn observe_action(&mut self, _action: SubmitTxToMempool) {
        self.seen += 1;
        if self.seen == TRANSFERS
            && let Some(done) = self.done.take()
        {
            let _ = done.send(());
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // A local chain mining one block per second. The provider signs with the
    // first Anvil key, so the executor's transactions are sent from it.
    let anvil = Anvil::new().block_time(1).try_spawn()?;
    let signer: PrivateKeySigner = anvil.keys()[0].clone().into();
    let recipient = anvil.addresses()[1];
    let ws = WsConnect::new(anvil.ws_endpoint());
    let provider = Arc::new(ProviderBuilder::new().wallet(signer).connect_ws(ws).await?);

    let balance_before = provider.get_balance(recipient).await?;

    let (done_tx, done_rx) = tokio::sync::oneshot::channel();
    let mut engine = Engine::<NewBlock, SubmitTxToMempool>::default();
    engine.add_collector(Box::new(BlockCollector::new(provider.clone())));
    engine.add_strategy(Box::new(TransferStrategy {
        to: recipient,
        remaining: TRANSFERS,
    }));
    engine.add_executor(Box::new(MempoolExecutor::new(provider.clone())));
    engine.add_observer(Box::new(SubmissionCounter {
        seen: 0,
        done: Some(done_tx),
    }));

    println!("Starting engine — {TRANSFERS} transfers of {TRANSFER_WEI} wei on new blocks...\n");
    let mut handle = engine.run().await?;

    // Run until the observer has seen every submission, then shut down
    // cooperatively. A long-running binary would select against Ctrl-C and
    // `handle.fatal` here instead (see the README's minimal example).
    let _ = done_rx.await;
    handle.token.cancel();
    while handle.tasks.join_next().await.is_some() {}

    // The executor submits to the mempool without waiting for inclusion, so
    // poll until the last transfer is mined.
    println!("\nWaiting for the transfers to be mined...");
    let expected = balance_before + U256::from(TRANSFER_WEI) * U256::from(TRANSFERS);
    loop {
        let balance = provider.get_balance(recipient).await?;
        if balance >= expected {
            println!(
                "Recipient balance grew by {} wei across {TRANSFERS} transfers",
                balance - balance_before
            );
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    println!("\nDone!");
    Ok(())
}
