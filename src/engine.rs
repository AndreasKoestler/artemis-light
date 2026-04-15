use std::marker::PhantomData;
use std::time::Duration;

use tokio::sync::broadcast::{self, Sender};
use tokio::task::JoinSet;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::types::{Collector, Executor, Strategy};

/// The main engine of Artemis. This struct is responsible for orchestrating the
/// data flow between collectors, strategies, and executors.
pub struct Engine<E, A> {
    /// The set of collectors that the engine will use to collect events.
    collectors: Vec<Box<dyn Collector<E>>>,

    /// The set of strategies that the engine will use to process events.
    strategies: Vec<Box<dyn Strategy<E, A>>>,

    /// The set of executors that the engine will use to execute actions.
    executors: Vec<Box<dyn Executor<A>>>,

    /// The capacity of the event channel.
    event_channel_capacity: usize,

    /// The capacity of the action channel.
    action_channel_capacity: usize,

    _a: PhantomData<A>,
}

impl<E, A> Default for Engine<E, A> {
    fn default() -> Self {
        Self {
            collectors: vec![],
            strategies: vec![],
            executors: vec![],
            event_channel_capacity: 512,
            action_channel_capacity: 512,
            _a: PhantomData,
        }
    }
}

impl<E, A> Engine<E, A> {
    pub fn new(
        collectors: Vec<Box<dyn Collector<E>>>,
        strategies: Vec<Box<dyn Strategy<E, A>>>,
        executors: Vec<Box<dyn Executor<A>>>,
        event_channel_capacity: usize,
        action_channel_capacity: usize,
    ) -> Self {
        Self {
            collectors,
            strategies,
            executors,
            event_channel_capacity,
            action_channel_capacity,
            _a: PhantomData,
        }
    }

    pub fn with_event_channel_capacity(mut self, capacity: usize) -> Self {
        self.event_channel_capacity = capacity;
        self
    }

    pub fn with_action_channel_capacity(mut self, capacity: usize) -> Self {
        self.action_channel_capacity = capacity;
        self
    }
}

impl<E, A> Engine<E, A>
where
    E: Send + Clone + std::fmt::Debug + 'static,
    A: Send + Clone + std::fmt::Debug + 'static,
{
    /// Adds a collector to be used by the engine.
    pub fn add_collector(&mut self, collector: Box<dyn Collector<E>>) {
        self.collectors.push(collector);
    }

    /// Adds a strategy to be used by the engine.
    pub fn add_strategy(&mut self, strategy: Box<dyn Strategy<E, A>>) {
        self.strategies.push(strategy);
    }

    /// Adds an executor to be used by the engine.
    pub fn add_executor(&mut self, executor: Box<dyn Executor<A>>) {
        self.executors.push(executor);
    }

    /// The core run loop of the engine. This function will spawn a task for
    /// each collector, strategy, and executor. It will then orchestrate the
    /// data flow between them.
    ///
    /// Collectors are started **before** strategies sync so that live events
    /// buffer in the broadcast channel while historical sync runs. This
    /// eliminates the gap between HTTP replay and WS subscription.
    ///
    /// Returns a [`CancellationToken`] that can be used to shut down the engine,
    /// and a [`JoinSet`] that can be used to await task completion.
    pub async fn run(self) -> Result<(CancellationToken, JoinSet<()>), Box<dyn std::error::Error>> {
        let (event_sender, _): (Sender<E>, _) = broadcast::channel(self.event_channel_capacity);
        let (action_sender, _): (Sender<A>, _) = broadcast::channel(self.action_channel_capacity);

        let token = CancellationToken::new();
        let mut set = JoinSet::new();

        // Spawn executors first (they subscribe before any events flow).
        for mut executor in self.executors {
            let mut receiver = action_sender.subscribe();
            let child = token.child_token();
            set.spawn(async move {
                info!("starting executor... ");
                loop {
                    tokio::select! {
                        _ = child.cancelled() => {
                            info!("executor shutting down");
                            break;
                        }
                        result = receiver.recv() => {
                            match result {
                                Ok(action) => {
                                    if let Err(e) = executor.execute(action).await {
                                        error!("error executing action: {}", e);
                                    }
                                }
                                Err(broadcast::error::RecvError::Lagged(n)) => {
                                    warn!("executor receiver lagged, skipped {n} messages");
                                }
                                Err(broadcast::error::RecvError::Closed) => {
                                    info!("action channel closed, executor shutting down");
                                    break;
                                }
                            }
                        }
                    }
                }
            });
        }

        // Spawn collectors so WS subscriptions are active during strategy sync.
        for collector in self.collectors {
            let event_sender = event_sender.clone();
            let child = token.child_token();
            set.spawn(async move {
                info!("starting collector...");
                let mut retries = 0u32;
                loop {
                    let mut event_stream = match collector.get_event_stream().await {
                        Ok(s) => s,
                        Err(e) => {
                            retries += 1;
                            error!("collector stream creation failed (attempt {retries}): {e}");
                            if retries >= 3 {
                                error!("collector failed {retries} times, giving up");
                                return;
                            }
                            tokio::select! {
                                _ = child.cancelled() => return,
                                _ = tokio::time::sleep(Duration::from_secs(2u64.pow(retries))) => {}
                            }
                            continue;
                        }
                    };
                    let mut received_events = false;
                    loop {
                        tokio::select! {
                            _ = child.cancelled() => {
                                info!("collector shutting down");
                                return;
                            }
                            event = event_stream.next() => {
                                match event {
                                    Some(event) => {
                                        received_events = true;
                                        if let Err(e) = event_sender.send(event) {
                                            error!("error sending event: {e}");
                                        }
                                    }
                                    None => break,
                                }
                            }
                        }
                    }
                    // Stream ended (WS disconnected)
                    if received_events {
                        retries = 0;
                    }
                    retries += 1;
                    warn!("collector stream ended (attempt {retries}), retrying...");
                    if retries >= 3 {
                        error!("collector stream ended {retries} times, exiting process");
                        std::process::exit(1);
                    }
                    tokio::select! {
                        _ = child.cancelled() => return,
                        _ = tokio::time::sleep(Duration::from_secs(2u64.pow(retries))) => {}
                    }
                }
            });
        }

        // Subscribe each strategy to the event channel before syncing so that
        // events produced by collectors during sync are buffered in the receiver.
        // Cancellation is respected during sync via the token.
        for mut strategy in self.strategies {
            let mut event_receiver = event_sender.subscribe();
            let action_sender = action_sender.clone();
            let child = token.child_token();

            info!("syncing strategy state...");
            tokio::select! {
                _ = token.cancelled() => {
                    return Err("engine cancelled during strategy sync".into());
                }
                result = strategy.sync_state() => {
                    result?;
                }
            }

            set.spawn(async move {
                info!("starting strategy... ");
                loop {
                    tokio::select! {
                        _ = child.cancelled() => {
                            info!("strategy shutting down");
                            break;
                        }
                        result = event_receiver.recv() => {
                            match result {
                                Ok(event) => {
                                    match strategy.process_event(event).await {
                                        Ok(mut action_stream) => {
                                            loop {
                                                tokio::select! {
                                                    _ = child.cancelled() => {
                                                        info!("strategy shutting down while draining action stream");
                                                        return;
                                                    }
                                                    action = action_stream.next() => {
                                                        match action {
                                                            Some(action) => {
                                                                if let Err(e) = action_sender.send(action) {
                                                                    error!("error sending action: {}", e);
                                                                }
                                                            }
                                                            None => break,
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        Err(e) => error!("error processing event: {}", e),
                                    }
                                }
                                Err(broadcast::error::RecvError::Lagged(n)) => {
                                    warn!("strategy receiver lagged, skipped {n} messages");
                                }
                                Err(broadcast::error::RecvError::Closed) => {
                                    info!("event channel closed, strategy shutting down");
                                    break;
                                }
                            }
                        }
                    }
                }
            });
        }

        Ok((token, set))
    }
}
