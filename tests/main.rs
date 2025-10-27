use alloy::{
    eips::{BlockId, BlockNumberOrTag},
    network::TransactionBuilder,
    primitives::U256,
    providers::{Provider, ProviderBuilder, WsConnect},
};
use artemis_light::{
    collectors::{BlockCollector, MempoolCollector},
    executors::{MempoolExecutor, SubmitTxToMempool},
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
    println!("RPC URL: {rpc_url}");
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
    let mut mempool_executor = MempoolExecutor::new(provider.clone());

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
        pending_tx_sender: None,
    };
    mempool_executor.execute(action).await.unwrap();
    //Sleep to seconds so that the tx has time to be mined
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let tx = provider.get_transaction_count(account).await.unwrap();
    assert_eq!(tx, 1);
}

/// Test that demonstrates a complete flow with collector, strategy, and executor
#[tokio::test]
async fn test_complete_flow() {
    use artemis_light::types::{Collector, CollectorStream, Executor, Strategy};
    use async_trait::async_trait;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::oneshot;

    // Define our event type (just a number)
    #[derive(Debug, Clone)]
    struct NumberEvent(u32);

    // Define our action type (contains number and oneshot channel)
    #[derive(Debug)]
    struct NumberAction {
        number: u32,
        response_tx: oneshot::Sender<bool>,
    }

    // Simple collector that emits numbers 1 to 10
    struct SimpleNumberCollector;

    impl SimpleNumberCollector {
        fn new() -> Self {
            Self
        }
    }

    #[async_trait]
    impl Collector<NumberEvent> for SimpleNumberCollector {
        async fn get_event_stream(&self) -> Result<CollectorStream<'_, NumberEvent>> {
            let stream = tokio_stream::StreamExt::map(tokio_stream::iter(1..=10), NumberEvent);
            Ok(Box::pin(stream))
        }
    }

    // Strategy with atomic counters
    struct NumberStrategy {
        pending_txs: Arc<AtomicUsize>,
        successful_tx: Arc<AtomicUsize>,
        failed_tx: Arc<AtomicUsize>,
    }

    impl NumberStrategy {
        fn new(
            pending_txs: Arc<AtomicUsize>,
            successful_tx: Arc<AtomicUsize>,
            failed_tx: Arc<AtomicUsize>,
        ) -> Self {
            Self {
                pending_txs,
                successful_tx,
                failed_tx,
            }
        }
    }

    #[async_trait]
    impl Strategy<NumberEvent, NumberAction> for NumberStrategy {
        async fn sync_state(&mut self) -> Result<()> {
            Ok(())
        }

        async fn process_event(&mut self, event: NumberEvent) -> Vec<NumberAction> {
            let number = event.0;
            let (tx, rx) = oneshot::channel();

            // Spawn a thread that listens on the oneshot receiver
            let pending_txs = Arc::clone(&self.pending_txs);
            let successful_tx = Arc::clone(&self.successful_tx);
            let failed_tx = Arc::clone(&self.failed_tx);

            std::thread::spawn(move || {
                // Increment pending counter
                pending_txs.fetch_add(1, Ordering::Relaxed);

                // Wait for the response on the oneshot channel
                let rt = tokio::runtime::Runtime::new().unwrap();
                rt.block_on(async {
                    match rx.await {
                        Ok(success) => {
                            // Decrease pending count
                            pending_txs.fetch_sub(1, Ordering::Relaxed);

                            // Increase appropriate counter based on whether number was even or odd
                            if success {
                                successful_tx.fetch_add(1, Ordering::Relaxed);
                            } else {
                                failed_tx.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        Err(_) => {
                            pending_txs.fetch_sub(1, Ordering::Relaxed);
                        }
                    }
                });
            });

            vec![NumberAction {
                number,
                response_tx: tx,
            }]
        }
    }

    // Executor that responds with success if number is even
    struct NumberExecutor;

    impl NumberExecutor {
        fn new() -> Self {
            Self
        }
    }

    #[async_trait]
    impl Executor<NumberAction> for NumberExecutor {
        async fn execute(&mut self, action: NumberAction) -> Result<()> {
            // Send response: true if number is even, false if odd
            let success = action.number % 2 == 0;
            action.response_tx.send(success).unwrap();
            Ok(())
        }
    }

    // Run the test
    let pending_txs = Arc::new(AtomicUsize::new(0));
    let successful_tx = Arc::new(AtomicUsize::new(0));
    let failed_tx = Arc::new(AtomicUsize::new(0));

    let collector = SimpleNumberCollector::new();
    let mut strategy = NumberStrategy::new(
        Arc::clone(&pending_txs),
        Arc::clone(&successful_tx),
        Arc::clone(&failed_tx),
    );
    let mut executor = NumberExecutor::new();

    // Create collector stream and execute flow
    let mut event_stream = collector.get_event_stream().await.unwrap();

    while let Some(event) = event_stream.next().await {
        let actions = strategy.process_event(event).await;
        for action in actions {
            executor.execute(action).await.unwrap();
        }
    }

    // Give threads time to process
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Verify results
    // Numbers 1-10: even numbers are 2,4,6,8,10 (5 numbers) -> successful
    // Numbers 1-10: odd numbers are 1,3,5,7,9 (5 numbers) -> failed
    assert_eq!(pending_txs.load(Ordering::Relaxed), 0);
    assert_eq!(successful_tx.load(Ordering::Relaxed), 5);
    assert_eq!(failed_tx.load(Ordering::Relaxed), 5);
}
