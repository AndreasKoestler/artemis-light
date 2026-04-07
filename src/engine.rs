use std::marker::PhantomData;
use std::time::Duration;

use tokio::sync::broadcast::{self, Sender};
use tokio::task::JoinSet;
use tokio_stream::StreamExt;
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

    /// The core run loop of the engine. This function will spawn a thread for
    /// each collector, strategy, and executor. It will then orchestrate the
    /// data flow between them.
    pub async fn run(self) -> Result<JoinSet<()>, Box<dyn std::error::Error>> {
        let (event_sender, _): (Sender<E>, _) = broadcast::channel(self.event_channel_capacity);
        let (action_sender, _): (Sender<A>, _) = broadcast::channel(self.action_channel_capacity);

        let mut set = JoinSet::new();

        // Spawn executors in separate threads.
        for mut executor in self.executors {
            let mut receiver = action_sender.subscribe();
            set.spawn(async move {
                info!("starting executor... ");
                loop {
                    match receiver.recv().await {
                        Ok(action) => match executor.execute(action).await {
                            Ok(_) => {}
                            Err(e) => error!("error executing action: {}", e),
                        },
                        Err(e) => error!("error receiving action: {}", e),
                    }
                }
            });
        }

        // Spawn strategies in separate threads.
        for mut strategy in self.strategies {
            let mut event_receiver = event_sender.subscribe();
            let action_sender = action_sender.clone();
            strategy.sync_state().await?;

            set.spawn(async move {
                info!("starting strategy... ");
                loop {
                    match event_receiver.recv().await {
                        Ok(event) => {
                            if let Ok(mut action_stream) = strategy.process_event(event).await {
                                while let Some(action) = action_stream.next().await {
                                    match action_sender.send(action) {
                                        Ok(_) => {}
                                        Err(e) => error!("error sending action: {}", e),
                                    }
                                }
                            }
                        }
                        Err(e) => error!("error receiving event: {}", e),
                    }
                }
            });
        }

        // Spawn collectors in separate threads.
        for collector in self.collectors {
            let event_sender = event_sender.clone();
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
                                error!("collector failed {retries} times, exiting process");
                                std::process::exit(1);
                            }
                            tokio::time::sleep(Duration::from_secs(2u64.pow(retries))).await;
                            continue;
                        }
                    };
                    let mut received_events = false;
                    while let Some(event) = event_stream.next().await {
                        received_events = true;
                        match event_sender.send(event) {
                            Ok(_) => {}
                            Err(e) => error!("error sending event: {e}"),
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
                    tokio::time::sleep(Duration::from_secs(2u64.pow(retries))).await;
                }
            });
        }

        Ok(set)
    }
}
