use alloy::{
    eips::{BlockId, BlockNumberOrTag},
    network::TransactionBuilder,
    primitives::U256,
    providers::{Provider, ProviderBuilder, WsConnect},
    rpc::types::Filter,
    signers::local::PrivateKeySigner,
    sol,
};
use artemis_light::{
    collectors::{BlockCollector, EventCollector, LogCollector, MempoolCollector},
    executors::{MempoolExecutor, SubmitTxToMempool},
    types::{ActionStream, Collector, Executor},
};

use alloy::rpc::types::eth::TransactionRequest;
use anyhow::Result;
use futures::StreamExt;
use std::sync::Arc;

use alloy::node_bindings::{Anvil, AnvilInstance};

sol! {
    #[sol(rpc, bytecode = "6080604052348015600e575f5ffd5b5060d980601a5f395ff3fe6080604052348015600e575f5ffd5b50600436106030575f3560e01c80633fa4f2451460345780635524107714604d575b5f5ffd5b603b5f5481565b60405190815260200160405180910390f35b605c6058366004608d565b605e565b005b5f81815560405182917f012c78e2b84325878b1bd9d250d772cfe5bda7722d795f45036fa5e1e6e303fc91a250565b5f60208284031215609c575f5ffd5b503591905056fea264697066735822122050fddb04e40945ebc7c51aef06d27a86c4aa98943b773d9ffdc789caf784441064736f6c634300081e0033")]
    contract Emitter {
        uint256 public value;

        event ValueSet(uint256 indexed value);

        function setValue(uint256 _value) external {
            value = _value;
            emit ValueSet(_value);
        }
    }
}

/// Spawns Anvil and instantiates a WS provider (no wallet).
pub async fn spawn_anvil() -> Result<(impl Provider, AnvilInstance)> {
    let anvil = Anvil::new().block_time(1).chain_id(1337).try_spawn()?;
    let rpc_url = anvil.ws_endpoint();
    println!("RPC URL: {rpc_url}");
    let ws = WsConnect::new(&rpc_url);
    let provider = ProviderBuilder::new().connect_ws(ws).await?;
    Ok((provider, anvil))
}

/// Spawns Anvil and instantiates a WS provider with a wallet signer.
pub async fn spawn_anvil_with_signer() -> Result<(impl Provider + Clone, AnvilInstance)> {
    let anvil = Anvil::new().block_time(1).chain_id(1337).try_spawn()?;
    let rpc_url = anvil.ws_endpoint();
    println!("RPC URL: {rpc_url}");
    let signer: PrivateKeySigner = anvil.keys()[0].clone().into();
    let ws = WsConnect::new(&rpc_url);
    let provider = ProviderBuilder::new().wallet(signer).connect_ws(ws).await?;
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
    };
    mempool_executor.execute(action).await.unwrap();
    //Sleep to seconds so that the tx has time to be mined
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let tx = provider.get_transaction_count(account).await.unwrap();
    assert_eq!(tx, 1);
}

/// Test that LogCollector receives logs emitted by a contract.
#[tokio::test]
async fn test_log_collector_receives_logs() {
    let (provider, _anvil) = spawn_anvil_with_signer().await.unwrap();
    let provider = Arc::new(provider);

    // Deploy the Emitter contract
    let contract = Emitter::deploy(provider.clone()).await.unwrap();
    let contract_addr = *contract.address();

    // Create a log collector filtered to this contract's address
    let filter = Filter::new().address(contract_addr);
    let log_collector = LogCollector::new(provider.clone(), filter);
    let log_stream = log_collector.get_event_stream().await.unwrap();

    // Call setValue to emit the ValueSet event
    contract
        .setValue(U256::from(42))
        .send()
        .await
        .unwrap()
        .watch()
        .await
        .unwrap();

    // Verify the log matches
    let log = log_stream.into_future().await.0.unwrap();
    assert_eq!(log.address(), contract_addr);
}

/// Test that EventCollector receives typed events from a contract.
#[tokio::test]
async fn test_event_collector_receives_events() {
    let (provider, _anvil) = spawn_anvil_with_signer().await.unwrap();
    let provider = Arc::new(provider);

    // Deploy the Emitter contract
    let contract = Emitter::deploy(provider.clone()).await.unwrap();

    // Create an event collector for ValueSet events
    let event_filter = contract.ValueSet_filter();
    let event_collector = EventCollector::new(event_filter);
    let event_stream = event_collector.get_event_stream().await.unwrap();

    // Call setValue to emit the ValueSet event
    contract
        .setValue(U256::from(42))
        .send()
        .await
        .unwrap()
        .watch()
        .await
        .unwrap();

    // Verify the decoded event value
    let ev = event_stream.into_future().await.0.unwrap();
    assert_eq!(ev.value, U256::from(42));
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

        async fn process_event(
            &mut self,
            event: NumberEvent,
        ) -> anyhow::Result<ActionStream<'_, NumberAction>> {
            let number = event.0;
            let (tx, rx) = oneshot::channel();

            // Spawn a task that listens on the oneshot receiver
            let pending_txs = Arc::clone(&self.pending_txs);
            let successful_tx = Arc::clone(&self.successful_tx);
            let failed_tx = Arc::clone(&self.failed_tx);

            tokio::spawn(async move {
                // Increment pending counter
                pending_txs.fetch_add(1, Ordering::Relaxed);

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

            Ok(Box::pin(futures::stream::iter(vec![NumberAction {
                number,
                response_tx: tx,
            }])))
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
        let mut actions = strategy.process_event(event).await.unwrap();
        while let Some(action) = actions.next().await {
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

// ---------------------------------------------------------------------------
// In-process engine tests (no Anvil required)
// ---------------------------------------------------------------------------

mod engine_tests {
    use anyhow::Result;
    use artemis_light::engine::Engine;
    use artemis_light::types::{ActionStream, Collector, CollectorStream, Executor, Strategy};
    use async_trait::async_trait;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // -- Shared mock types --------------------------------------------------

    /// A collector that emits a fixed list of u32 events.
    struct FixedCollector {
        items: Vec<u32>,
    }

    impl FixedCollector {
        fn new(items: Vec<u32>) -> Self {
            Self { items }
        }
    }

    #[async_trait]
    impl Collector<u32> for FixedCollector {
        async fn get_event_stream(&self) -> Result<CollectorStream<'_, u32>> {
            Ok(Box::pin(futures::stream::iter(self.items.clone())))
        }
    }

    /// A collector that emits items forever (one per 10 ms).
    struct InfiniteCollector;

    #[async_trait]
    impl Collector<u32> for InfiniteCollector {
        async fn get_event_stream(&self) -> Result<CollectorStream<'_, u32>> {
            let stream = futures::stream::unfold(0u32, |n| async move {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                Some((n, n + 1))
            });
            Ok(Box::pin(stream))
        }
    }

    /// A collector whose `get_event_stream` always fails.
    struct FailingCollector;

    #[async_trait]
    impl Collector<u32> for FailingCollector {
        async fn get_event_stream(&self) -> Result<CollectorStream<'_, u32>> {
            Err(anyhow::anyhow!("collector failure"))
        }
    }

    /// Strategy that echoes every event as an action.
    struct EchoStrategy;

    #[async_trait]
    impl Strategy<u32, u32> for EchoStrategy {
        async fn sync_state(&mut self) -> Result<()> {
            Ok(())
        }
        async fn process_event(&mut self, event: u32) -> Result<ActionStream<'_, u32>> {
            Ok(Box::pin(futures::stream::iter(vec![event])))
        }
    }

    /// Executor that increments a shared counter for every action it receives.
    struct CountingExecutor {
        count: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Executor<u32> for CountingExecutor {
        async fn execute(&mut self, _action: u32) -> Result<()> {
            self.count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    // -- Tests --------------------------------------------------------------

    /// 3 events flow through collector -> strategy -> executor; verify count.
    #[tokio::test]
    async fn test_engine_event_flow() {
        let count = Arc::new(AtomicUsize::new(0));

        let mut engine = Engine::<u32, u32>::default();
        engine.add_collector(Box::new(FixedCollector::new(vec![1, 2, 3])));
        engine.add_strategy(Box::new(EchoStrategy));
        engine.add_executor(Box::new(CountingExecutor {
            count: count.clone(),
        }));

        let (token, mut set) = engine.run().await.unwrap();

        // The finite collector will complete; give it a moment to drain.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        token.cancel();

        while set.join_next().await.is_some() {}

        assert_eq!(count.load(Ordering::SeqCst), 3);
    }

    /// Infinite collector + cancel token; verify all tasks exit within 2 s.
    #[tokio::test]
    async fn test_engine_shutdown() {
        let count = Arc::new(AtomicUsize::new(0));

        let mut engine = Engine::<u32, u32>::default();
        engine.add_collector(Box::new(InfiniteCollector));
        engine.add_strategy(Box::new(EchoStrategy));
        engine.add_executor(Box::new(CountingExecutor {
            count: count.clone(),
        }));

        let (token, mut set) = engine.run().await.unwrap();

        // Let it run briefly so some events flow.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(
            count.load(Ordering::SeqCst) > 0,
            "expected some events to flow"
        );

        token.cancel();

        let deadline = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while set.join_next().await.is_some() {}
        })
        .await;

        assert!(
            deadline.is_ok(),
            "engine tasks did not shut down within 2 s"
        );
    }

    /// Tiny channel capacity with a fast collector and slow strategy; verify
    /// the engine doesn't crash and events still flow despite backpressure.
    #[tokio::test]
    async fn test_engine_backpressure() {
        /// A collector that quickly emits many events.
        struct BurstCollector {
            count: u32,
        }

        #[async_trait]
        impl Collector<u32> for BurstCollector {
            async fn get_event_stream(&self) -> Result<CollectorStream<'_, u32>> {
                Ok(Box::pin(futures::stream::iter(0..self.count)))
            }
        }

        /// A strategy that sleeps briefly before echoing each event.
        struct SlowStrategy;

        #[async_trait]
        impl Strategy<u32, u32> for SlowStrategy {
            async fn sync_state(&mut self) -> Result<()> {
                Ok(())
            }
            async fn process_event(&mut self, event: u32) -> Result<ActionStream<'_, u32>> {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                Ok(Box::pin(futures::stream::iter(vec![event])))
            }
        }

        let count = Arc::new(AtomicUsize::new(0));

        let mut engine = Engine::<u32, u32>::default()
            .with_event_channel_capacity(2)
            .with_action_channel_capacity(2);
        engine.add_collector(Box::new(BurstCollector { count: 50 }));
        engine.add_strategy(Box::new(SlowStrategy));
        engine.add_executor(Box::new(CountingExecutor {
            count: count.clone(),
        }));

        let (token, mut set) = engine.run().await.unwrap();

        // Give enough time for events to drain through the slow strategy.
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        token.cancel();

        while set.join_next().await.is_some() {}

        let executed = count.load(Ordering::SeqCst);
        assert!(
            executed > 0,
            "expected some events to flow despite backpressure, got 0"
        );
    }

    /// One failing + one good collector; verify good events still flow.
    #[tokio::test]
    async fn test_engine_collector_failure() {
        let count = Arc::new(AtomicUsize::new(0));

        let mut engine = Engine::<u32, u32>::default();
        engine.add_collector(Box::new(FailingCollector));
        engine.add_collector(Box::new(FixedCollector::new(vec![10, 20])));
        engine.add_strategy(Box::new(EchoStrategy));
        engine.add_executor(Box::new(CountingExecutor {
            count: count.clone(),
        }));

        let (token, mut set) = engine.run().await.unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        token.cancel();

        while set.join_next().await.is_some() {}

        assert_eq!(
            count.load(Ordering::SeqCst),
            2,
            "good collector's events should still flow"
        );
    }
}
