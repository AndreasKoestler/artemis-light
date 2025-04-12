use alloy::{
    eips::{BlockId, BlockNumberOrTag},
    network::TransactionBuilder,
    primitives::U256,
    providers::{Provider, ProviderBuilder, WsConnect},
};
use artemis_light::{
    collectors::{block_collector::BlockCollector, mempool_collector::MempoolCollector},
    executors::mempool_executor::{MempoolExecutor, SubmitTxToMempool},
    types::{Collector, Executor},
};

use alloy::rpc::types::eth::TransactionRequest;
use anyhow::Result;
use futures::StreamExt;
use std::sync::Arc;

use alloy::node_bindings::{Anvil, AnvilInstance};

/// Spawns Anvil and instantiates an Http provider.
pub async fn spawn_anvil() -> Result<(impl Provider, AnvilInstance)> {
    let anvil = Anvil::new().block_time(1).chain_id(1337).try_spawn()?;
    let rpc_url = anvil.ws_endpoint();
    println!("RPC URL: {}", rpc_url);
    let ws = WsConnect::new(&rpc_url);
    let provider = ProviderBuilder::new().connect_ws(ws).await?;
    Ok((provider, anvil))
}

/// Test that block collector correctly emits blocks.
#[tokio::test]
async fn test_block_collector_sends_blocks() {
    let (provider, _anvil) = spawn_anvil().await.unwrap();
    let provider = Arc::new(provider);
    let block_collector = BlockCollector::new(provider.clone());

    let block_stream = block_collector.get_event_stream().await.unwrap();
    let block_a = block_stream.into_future().await.0.unwrap();
    let block_b = provider
        .get_block(BlockId::Number(BlockNumberOrTag::Latest))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(block_a.hash, block_b.header.hash);
}

/// Test that mempool collector correctly emits blocks.
#[tokio::test]
async fn test_mempool_collector_sends_txs() {
    let (provider, _anvil) = spawn_anvil().await.expect("Failed to spawn anvil");
    let provider = Arc::new(provider);
    let mempool_collector = MempoolCollector::new(provider.clone());
    let mempool_stream = mempool_collector
        .get_event_stream()
        .await
        .expect("Failed to get mempool stream");

    let account = provider
        .get_accounts()
        .await
        .expect("Failed to get accounts")[0];
    let value: u64 = 42;
    let gas_price = 100_000_000_000_000_000u128;
    let tx = TransactionRequest::default()
        .with_to(account)
        .with_from(account)
        .with_value(U256::from(value))
        .with_gas_price(gas_price);

    let pending_tx = provider.send_transaction(tx).await.unwrap();
    let tx_receipt = pending_tx.get_receipt().await.unwrap();

    let tx = mempool_stream.into_future().await.0.unwrap();
    assert_eq!(&tx_receipt.transaction_hash, tx.inner.hash());
}

/// Test that the mempool executor correctly sends txs
#[tokio::test]
async fn test_mempool_executor_sends_tx_simple() {
    let (provider, _anvil) = spawn_anvil().await.unwrap();
    let provider = Arc::new(provider);
    let mempool_executor = MempoolExecutor::new(provider.clone());

    let account = provider.get_accounts().await.unwrap()[0];
    let value: u64 = 42;
    let gas_price = 100_000_000_000_000_000u128;
    let tx = TransactionRequest::default()
        .with_to(account)
        .with_from(account)
        .with_value(U256::from(value))
        .with_gas_price(gas_price);

    let action = SubmitTxToMempool {
        tx,
        gas_bid_info: None,
    };
    mempool_executor.execute(action).await.unwrap();
    //Sleep to seconds so that the tx has time to be mined
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let tx = provider.get_transaction_count(account).await.unwrap();
    assert_eq!(tx, 1);
}
