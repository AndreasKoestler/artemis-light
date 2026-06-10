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
    executors::{GasBidInfo, MempoolExecutor, SubmitTxToMempool},
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

    let block_stream = block_collector.subscribe().await.unwrap();
    let block_a = block_stream.into_future().await.0.unwrap();
    let block_b = provider
        .get_block(BlockId::Number(BlockNumberOrTag::Latest))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(block_a.hash, block_b.header.hash);
}

/// Over plain HTTP there is no pubsub, so `subscribe_blocks` fails and the
/// collector must fall back to polling — and still deliver new blocks.
#[tokio::test]
async fn test_block_collector_polls_when_subscriptions_are_unavailable() {
    let anvil = Anvil::new()
        .block_time(1)
        .chain_id(1337)
        .try_spawn()
        .unwrap();
    let provider = ProviderBuilder::new().connect_http(anvil.endpoint().parse().unwrap());
    let provider = Arc::new(provider);
    let block_collector = BlockCollector::new(provider.clone());

    let block_stream = block_collector.subscribe().await.unwrap();
    let block = tokio::time::timeout(
        std::time::Duration::from_secs(15),
        block_stream.into_future(),
    )
    .await
    .expect("polling fallback should deliver a block within 15s")
    .0
    .unwrap();

    let chain_block = provider
        .get_block(BlockId::Number(BlockNumberOrTag::Number(block.number)))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(block.hash, chain_block.header.hash);
}

/// Test that mempool collector correctly emits blocks.
#[tokio::test]
async fn test_mempool_collector_sends_txs() {
    let (provider, _anvil) = spawn_anvil().await.expect("Failed to spawn anvil");
    let provider = Arc::new(provider);
    let mempool_collector = MempoolCollector::new(provider.clone());
    let mempool_stream = mempool_collector
        .subscribe()
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

/// A bid percentage above 100 would spend more than the opportunity's total
/// profit on gas — the transaction itself becomes the loss. The executor must
/// refuse the action rather than overbid.
#[tokio::test]
async fn test_mempool_executor_rejects_bid_percentage_above_100() {
    let (provider, _anvil) = spawn_anvil().await.unwrap();
    let provider = Arc::new(provider);
    let mut mempool_executor = MempoolExecutor::new(provider.clone());

    let account = provider.get_accounts().await.unwrap()[0];
    let tx = TransactionRequest::default()
        .with_to(account)
        .with_from(account)
        .with_value(U256::from(42));

    let action = SubmitTxToMempool {
        tx,
        gas_bid_info: Some(GasBidInfo {
            total_profit: 1_000_000_000_000,
            bid_percentage: 101,
        }),
    };
    let err = mempool_executor.execute(action).await.unwrap_err();
    assert!(
        err.to_string().contains("bid_percentage"),
        "error should name the invalid field, got: {err}"
    );

    // Nothing was sent.
    assert_eq!(provider.get_transaction_count(account).await.unwrap(), 0);
}

/// With a gas bid, the executor prices the tx at
/// `total_profit / gas_usage * bid_percentage / 100` and sets the gas limit
/// from its own estimate (so the provider's filler doesn't re-estimate).
#[tokio::test]
async fn test_mempool_executor_prices_tx_from_gas_bid() {
    use alloy::consensus::Transaction as _;

    let (provider, _anvil) = spawn_anvil().await.unwrap();
    let provider = Arc::new(provider);
    let mut mempool_executor = MempoolExecutor::new(provider.clone());

    let account = provider.get_accounts().await.unwrap()[0];
    let tx = TransactionRequest::default()
        .with_to(account)
        .with_from(account)
        .with_value(U256::from(42));

    // A plain transfer estimates at 21_000 gas; a profit of 21_000 * 4 gwei
    // makes the breakeven price 4 gwei, so a 50% bid prices at 2 gwei —
    // comfortably above anvil's default 1 gwei base fee.
    const GWEI: u128 = 1_000_000_000;
    let action = SubmitTxToMempool {
        tx,
        gas_bid_info: Some(GasBidInfo {
            total_profit: 21_000 * 4 * GWEI,
            bid_percentage: 50,
        }),
    };
    mempool_executor.execute(action).await.unwrap();

    // Wait for the 1s-block chain to mine it, then find the (only) tx by
    // scanning blocks from the tip down.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let tip = provider.get_block_number().await.unwrap();
    let mut sent = None;
    for n in (0..=tip).rev() {
        let block = provider
            .get_block(BlockId::Number(BlockNumberOrTag::Number(n)))
            .await
            .unwrap()
            .unwrap();
        if let Some(hash) = block.transactions.into_hashes().hashes().next() {
            sent = provider.get_transaction_by_hash(hash).await.unwrap();
            break;
        }
    }
    let sent = sent.expect("the bid-priced transaction should have been mined");

    assert_eq!(
        sent.gas_price(),
        Some(2 * GWEI),
        "tx must be priced at breakeven * bid_percentage"
    );
    assert_eq!(
        sent.gas_limit(),
        21_000,
        "the executor's estimate must be set as the gas limit"
    );
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
    let log_stream = log_collector.subscribe().await.unwrap();

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
    let event_stream = event_collector.subscribe().await.unwrap();

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
        async fn subscribe(&self) -> Result<CollectorStream<'_, NumberEvent>> {
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
            let success = action.number.is_multiple_of(2);
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
    let mut event_stream = collector.subscribe().await.unwrap();

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
    use artemis_light::types::{
        ActionStream, Collector, CollectorStream, Executor, Observer, Strategy,
    };
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
        async fn subscribe(&self) -> Result<CollectorStream<'_, u32>> {
            Ok(Box::pin(futures::stream::iter(self.items.clone())))
        }
    }

    /// A collector that emits items forever (one per 10 ms).
    struct InfiniteCollector;

    #[async_trait]
    impl Collector<u32> for InfiniteCollector {
        async fn subscribe(&self) -> Result<CollectorStream<'_, u32>> {
            let stream = futures::stream::unfold(0u32, |n| async move {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                Some((n, n + 1))
            });
            Ok(Box::pin(stream))
        }
    }

    /// A collector whose `subscribe` always fails.
    struct FailingCollector;

    #[async_trait]
    impl Collector<u32> for FailingCollector {
        async fn subscribe(&self) -> Result<CollectorStream<'_, u32>> {
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

    /// Observer that records every event and action it sees.
    struct RecordingObserver {
        events: Arc<std::sync::Mutex<Vec<u32>>>,
        actions: Arc<std::sync::Mutex<Vec<u32>>>,
    }

    #[async_trait]
    impl Observer<u32, u32> for RecordingObserver {
        async fn observe_event(&mut self, event: u32) {
            self.events.lock().unwrap().push(event);
        }
        async fn observe_action(&mut self, action: u32) {
            self.actions.lock().unwrap().push(action);
        }
    }

    // -- Tests --------------------------------------------------------------

    /// An Observer sees every event fanned to strategies and every action
    /// fanned to executors, without perturbing either: the executor still
    /// receives all actions.
    #[tokio::test]
    async fn test_engine_observer_sees_events_and_actions() {
        let count = Arc::new(AtomicUsize::new(0));
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        let actions = Arc::new(std::sync::Mutex::new(Vec::new()));

        let mut engine = Engine::<u32, u32>::default();
        engine.add_collector(Box::new(FixedCollector::new(vec![1, 2, 3])));
        engine.add_strategy(Box::new(EchoStrategy));
        engine.add_executor(Box::new(CountingExecutor {
            count: count.clone(),
        }));
        engine.add_observer(Box::new(RecordingObserver {
            events: events.clone(),
            actions: actions.clone(),
        }));

        let mut handle = engine.run().await.unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        handle.token.cancel();
        while handle.tasks.join_next().await.is_some() {}

        assert_eq!(*events.lock().unwrap(), vec![1, 2, 3]);
        assert_eq!(*actions.lock().unwrap(), vec![1, 2, 3]);
        assert_eq!(
            count.load(Ordering::SeqCst),
            3,
            "observation must not perturb the pipeline"
        );
    }

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

        let mut handle = engine.run().await.unwrap();

        // The finite collector will complete; give it a moment to drain.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        handle.token.cancel();

        while handle.tasks.join_next().await.is_some() {}

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

        let mut handle = engine.run().await.unwrap();

        // Let it run briefly so some events flow.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(
            count.load(Ordering::SeqCst) > 0,
            "expected some events to flow"
        );

        handle.token.cancel();

        let deadline = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while handle.tasks.join_next().await.is_some() {}
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
            async fn subscribe(&self) -> Result<CollectorStream<'_, u32>> {
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

        let mut handle = engine.run().await.unwrap();

        // Give enough time for events to drain through the slow strategy.
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        handle.token.cancel();

        while handle.tasks.join_next().await.is_some() {}

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

        let mut handle = engine.run().await.unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        handle.token.cancel();

        while handle.tasks.join_next().await.is_some() {}

        assert_eq!(
            count.load(Ordering::SeqCst),
            2,
            "good collector's events should still flow"
        );
    }

    /// A collector whose stream always ends immediately + a low failure
    /// threshold; verify the engine surfaces a fatal cause and cancels its own
    /// token — exercising the C1 escalation *without the test process dying*.
    #[tokio::test]
    async fn test_engine_collector_fatal_escalation() {
        use artemis_light::engine::reconnect::ReconnectConfig;
        use std::time::Duration;

        /// Always yields an empty stream — i.e. the subscription ends at once.
        struct EndingCollector;

        #[async_trait]
        impl Collector<u32> for EndingCollector {
            async fn subscribe(&self) -> Result<CollectorStream<'_, u32>> {
                Ok(Box::pin(futures::stream::empty::<u32>()))
            }
        }

        let count = Arc::new(AtomicUsize::new(0));

        let mut engine = Engine::<u32, u32>::default().with_reconnect_config(ReconnectConfig {
            max_failures: 2,
            base_delay: Duration::from_millis(10),
        });
        engine.add_collector(Box::new(EndingCollector));
        engine.add_strategy(Box::new(EchoStrategy));
        engine.add_executor(Box::new(CountingExecutor {
            count: count.clone(),
        }));

        let mut handle = engine.run().await.unwrap();

        // The fatal token fires after the threshold of consecutive stream-ends.
        tokio::time::timeout(Duration::from_secs(1), handle.fatal.cancelled())
            .await
            .expect("fatal signal did not fire within 1 s");

        // The engine also cancelled its root token; the process is still alive.
        assert!(
            handle.token.is_cancelled(),
            "root token should be cancelled on fatal escalation"
        );

        while handle.tasks.join_next().await.is_some() {}
    }

    /// Every strategy must see every event, including ones a collector emits
    /// while *another* strategy is still syncing. A strategy's broadcast
    /// receiver must therefore exist before any collector can emit — not be
    /// created lazily as each strategy's turn comes up in the sync loop, which
    /// would deterministically lose to the second strategy every event
    /// broadcast during the first strategy's sync. Regression test for the
    /// subscribe-after-send startup race.
    #[tokio::test]
    async fn test_engine_strategies_dont_miss_events_emitted_during_sync() {
        use std::time::Duration;
        use tokio::sync::{Notify, mpsc};

        /// Emits `[1, 2, 3]` only after `gate` fires, then signals `emitted`.
        /// Gating emission on a signal removes any race between collector start
        /// and receiver creation, isolating the ordering bug under test.
        struct GatedCollector {
            gate: Arc<Notify>,
            emitted: Arc<Notify>,
        }

        #[async_trait]
        impl Collector<u32> for GatedCollector {
            async fn subscribe(&self) -> Result<CollectorStream<'_, u32>> {
                let gate = self.gate.clone();
                let emitted = self.emitted.clone();
                let stream = async_stream::stream! {
                    gate.notified().await;
                    for i in [1u32, 2, 3] {
                        yield i;
                    }
                    // All three events have now been broadcast; release the
                    // strategy whose sync opened the gate.
                    emitted.notify_one();
                };
                Ok(Box::pin(stream))
            }
        }

        /// Opens the gate during sync, then waits until the collector reports it
        /// has broadcast every event — so all three are on the channel before
        /// this strategy's sync returns and the *next* strategy is registered.
        struct GateOpeningStrategy {
            gate: Arc<Notify>,
            emitted: Arc<Notify>,
            seen: mpsc::UnboundedSender<u32>,
        }

        #[async_trait]
        impl Strategy<u32, u32> for GateOpeningStrategy {
            async fn sync_state(&mut self) -> Result<()> {
                self.gate.notify_one();
                self.emitted.notified().await;
                Ok(())
            }
            async fn process_event(&mut self, event: u32) -> Result<ActionStream<'_, u32>> {
                let _ = self.seen.send(event);
                Ok(Box::pin(futures::stream::empty()))
            }
        }

        /// Syncs instantly. Under the buggy ordering its receiver is created
        /// only after the gate-opening strategy's sync completes — by which time
        /// every event has already been broadcast and dropped.
        struct RecordingStrategy {
            seen: mpsc::UnboundedSender<u32>,
        }

        #[async_trait]
        impl Strategy<u32, u32> for RecordingStrategy {
            async fn sync_state(&mut self) -> Result<()> {
                Ok(())
            }
            async fn process_event(&mut self, event: u32) -> Result<ActionStream<'_, u32>> {
                let _ = self.seen.send(event);
                Ok(Box::pin(futures::stream::empty()))
            }
        }

        let gate = Arc::new(Notify::new());
        let emitted = Arc::new(Notify::new());
        let (tx_a, mut rx_a) = mpsc::unbounded_channel();
        let (tx_b, mut rx_b) = mpsc::unbounded_channel();

        let mut engine = Engine::<u32, u32>::default();
        engine.add_collector(Box::new(GatedCollector {
            gate: gate.clone(),
            emitted: emitted.clone(),
        }));
        engine.add_strategy(Box::new(GateOpeningStrategy {
            gate,
            emitted,
            seen: tx_a,
        }));
        engine.add_strategy(Box::new(RecordingStrategy { seen: tx_b }));

        let mut handle = engine.run().await.unwrap();

        // Collect what each strategy received, bounded so the buggy case (which
        // delivers nothing to the second strategy) fails fast instead of hanging.
        async fn collect_three(rx: &mut mpsc::UnboundedReceiver<u32>) -> Vec<u32> {
            let mut got = Vec::new();
            for _ in 0..3 {
                match tokio::time::timeout(Duration::from_secs(2), rx.recv()).await {
                    Ok(Some(v)) => got.push(v),
                    _ => break,
                }
            }
            got
        }

        let a = collect_three(&mut rx_a).await;
        let b = collect_three(&mut rx_b).await;

        handle.token.cancel();
        while handle.tasks.join_next().await.is_some() {}

        assert_eq!(
            a,
            vec![1, 2, 3],
            "gate-opening strategy must see all events"
        );
        assert_eq!(
            b,
            vec![1, 2, 3],
            "a strategy registered after another's sync must not miss events \
             broadcast during that sync"
        );
    }

    /// A strategy whose `sync_state` errors must surface that error from
    /// `Engine::run` — it is the only fallible user hook in the run loop, and
    /// a regression that swallowed it would start the pipeline on unsynced
    /// state silently.
    #[tokio::test]
    async fn test_engine_run_surfaces_strategy_sync_error() {
        struct FailingSyncStrategy;

        #[async_trait]
        impl Strategy<u32, u32> for FailingSyncStrategy {
            async fn sync_state(&mut self) -> Result<()> {
                Err(anyhow::anyhow!("sync exploded"))
            }
            async fn process_event(&mut self, event: u32) -> Result<ActionStream<'_, u32>> {
                Ok(Box::pin(futures::stream::iter(vec![event])))
            }
        }

        let mut engine = Engine::<u32, u32>::default();
        engine.add_collector(Box::new(FixedCollector::new(vec![1])));
        engine.add_strategy(Box::new(FailingSyncStrategy));

        let err = engine
            .run()
            .await
            .err()
            .expect("a failing sync_state must fail run");
        assert!(
            err.to_string().contains("sync exploded"),
            "the strategy's own error must surface, got: {err}"
        );
    }

    /// A collector that escalates to `Fatal` *while* a strategy is still
    /// syncing. The root-token cancellation must not be reported as a generic
    /// `Err` — `run` must hand back an `EngineHandle` with `fatal` set so the
    /// caller still observes the fatal cause and follows the documented exit
    /// path. Regression test for the fatal-during-sync handle loss.
    #[tokio::test]
    async fn test_engine_fatal_during_sync_returns_handle() {
        use artemis_light::engine::reconnect::ReconnectConfig;
        use std::time::Duration;

        /// Always yields an empty stream — the subscription ends at once.
        struct EndingCollector;

        #[async_trait]
        impl Collector<u32> for EndingCollector {
            async fn subscribe(&self) -> Result<CollectorStream<'_, u32>> {
                Ok(Box::pin(futures::stream::empty::<u32>()))
            }
        }

        /// Strategy whose sync outlives the collector's escalation window.
        struct SlowSyncStrategy;

        #[async_trait]
        impl Strategy<u32, u32> for SlowSyncStrategy {
            async fn sync_state(&mut self) -> Result<()> {
                tokio::time::sleep(Duration::from_secs(1)).await;
                Ok(())
            }
            async fn process_event(&mut self, event: u32) -> Result<ActionStream<'_, u32>> {
                Ok(Box::pin(futures::stream::iter(vec![event])))
            }
        }

        let mut engine = Engine::<u32, u32>::default().with_reconnect_config(ReconnectConfig {
            max_failures: 2,
            base_delay: Duration::from_millis(10),
        });
        engine.add_collector(Box::new(EndingCollector));
        engine.add_strategy(Box::new(SlowSyncStrategy));

        let mut handle = tokio::time::timeout(Duration::from_secs(1), engine.run())
            .await
            .expect("run did not return within 1 s")
            .expect("fatal during sync should return Ok(handle), not Err");

        assert!(
            handle.fatal.is_cancelled(),
            "fatal cause should be observable on the returned handle"
        );
        assert!(
            handle.token.is_cancelled(),
            "root token should be cancelled on fatal escalation"
        );

        while handle.tasks.join_next().await.is_some() {}
    }
}
