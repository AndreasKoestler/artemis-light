mod channel;
mod driver;
pub mod reconnect;

use std::marker::PhantomData;

use futures::StreamExt;
use tokio::sync::broadcast::{self, Sender};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::types::{Collector, Executor, Observer, Strategy};
use reconnect::ReconnectConfig;

/// Handle returned by [`Engine::run`]. Bundles the cooperative-shutdown token,
/// the set of running tasks, and an observe-only token that fires if a
/// collector becomes unrecoverable.
pub struct EngineHandle {
    /// Cancel this to shut the engine down cooperatively.
    pub token: CancellationToken,
    /// The spawned collector/strategy/executor tasks.
    pub tasks: JoinSet<()>,
    /// Observe-only. The engine cancels this — and then `token` — if a collector
    /// exhausts its [`ReconnectPolicy`], so the binary can tell a fatal shutdown
    /// apart from a caller-initiated one. The library never calls
    /// `process::exit`; the binary observes this and decides whether to restart.
    /// Do not cancel it yourself.
    pub fatal: CancellationToken,
}

/// The main engine of Artemis. This struct is responsible for orchestrating the
/// data flow between collectors, strategies, and executors.
pub struct Engine<E, A> {
    /// The set of collectors that the engine will use to collect events.
    collectors: Vec<Box<dyn Collector<E>>>,

    /// The set of strategies that the engine will use to process events.
    strategies: Vec<Box<dyn Strategy<E, A>>>,

    /// The set of executors that the engine will use to execute actions.
    executors: Vec<Box<dyn Executor<A>>>,

    /// Passive observers of every event and action crossing the channels.
    observers: Vec<Box<dyn Observer<E, A>>>,

    /// The capacity of the event channel.
    event_channel_capacity: usize,

    /// The capacity of the action channel.
    action_channel_capacity: usize,

    /// How collectors reconnect after a lost or failed stream.
    reconnect_config: ReconnectConfig,

    _a: PhantomData<A>,
}

impl<E, A> Default for Engine<E, A> {
    fn default() -> Self {
        Self {
            collectors: vec![],
            strategies: vec![],
            executors: vec![],
            observers: vec![],
            event_channel_capacity: 512,
            action_channel_capacity: 512,
            reconnect_config: ReconnectConfig::default(),
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
            observers: vec![],
            event_channel_capacity,
            action_channel_capacity,
            reconnect_config: ReconnectConfig::default(),
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

    /// Sets the [`ReconnectConfig`] applied to every collector.
    pub fn with_reconnect_config(mut self, config: ReconnectConfig) -> Self {
        self.reconnect_config = config;
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

    /// Adds a passive observer of every event and action.
    pub fn add_observer(&mut self, observer: Box<dyn Observer<E, A>>) {
        self.observers.push(observer);
    }

    /// The core run loop of the engine. This function will spawn a task for
    /// each collector, strategy, and executor. It will then orchestrate the
    /// data flow between them.
    ///
    /// Collectors are started **before** strategies sync so that live events
    /// buffer in the broadcast channel while historical sync runs. This
    /// eliminates the gap between HTTP replay and WS subscription.
    ///
    /// Returns an [`EngineHandle`] carrying the shutdown token, the running
    /// tasks, and a one-shot that fires if a collector becomes unrecoverable.
    ///
    /// Errors are [`anyhow::Error`], matching every other fallible API in the
    /// crate, so `engine.run().await?` composes in an `anyhow` (or plain
    /// `Box<dyn Error>`) `main` without a conversion shim.
    pub async fn run(self) -> anyhow::Result<EngineHandle> {
        let (event_sender, _): (Sender<E>, _) = broadcast::channel(self.event_channel_capacity);
        let (action_sender, _): (Sender<A>, _) = broadcast::channel(self.action_channel_capacity);

        let token = CancellationToken::new();
        let mut set = JoinSet::new();

        let reconnect_config = self.reconnect_config;
        // Independent of `token` so a caller-initiated shutdown isn't mistaken
        // for a fatal one. Clone + idempotent, so every collector shares it
        // directly — first to escalate wins, the rest are no-ops.
        let fatal = CancellationToken::new();

        // Spawn executors first (they subscribe before any events flow). The
        // action channel — with lag logged, closure and cancellation folded in —
        // is presented as a plain stream, so the loop is just the per-action work.
        for mut executor in self.executors {
            let mut actions = Box::pin(channel::into_stream(
                action_sender.subscribe(),
                token.child_token(),
                "executor",
            ));
            set.spawn(async move {
                info!("starting executor... ");
                while let Some(action) = actions.next().await {
                    if let Err(e) = executor.execute(action).await {
                        error!("error executing action: {e}");
                    }
                }
            });
        }

        // Spawn observers next, before any events flow, so their subscriptions
        // see everything. An observer is a passive consumer of both channels:
        // same lag/shutdown semantics as strategies and executors, no feedback
        // path into the pipeline.
        for mut observer in self.observers {
            let mut events = Box::pin(
                channel::into_stream(event_sender.subscribe(), token.child_token(), "observer")
                    .fuse(),
            );
            let mut actions = Box::pin(
                channel::into_stream(action_sender.subscribe(), token.child_token(), "observer")
                    .fuse(),
            );
            set.spawn(async move {
                info!("starting observer... ");
                loop {
                    tokio::select! {
                        Some(event) = events.next() => observer.observe_event(event).await,
                        Some(action) = actions.next() => observer.observe_action(action).await,
                        else => break,
                    }
                }
            });
        }

        // Subscribe every strategy's broadcast receiver *before* spawning any
        // collector. A tokio broadcast channel only retains messages for
        // receivers that already exist; a receiver created lazily in the sync
        // loop below would miss every event broadcast before its turn came up —
        // a deterministic loss for all but the first strategy, since each
        // strategy syncs (and collectors emit) before the next strategy's
        // receiver exists. Creating them all up front means events broadcast
        // during any strategy's sync are buffered for every strategy.
        let strategies: Vec<_> = self
            .strategies
            .into_iter()
            .map(|strategy| (strategy, event_sender.subscribe()))
            .collect();

        // Spawn collectors so WS subscriptions are active during strategy sync.
        //
        // Each collector is handed to a [`Collector Driver`](driver), which owns
        // its full lifecycle — subscribe, pump events, and on a lost or failed
        // stream consult the per-collector `ReconnectPolicy` to retry-after-
        // backoff or escalate. A `Fatal` verdict cancels the `fatal` token (the
        // reason) and the *root* token (tearing down every task); the library
        // never calls `process::exit`.
        for collector in self.collectors {
            let tokens = driver::CollectorTokens {
                child: token.child_token(),
                fatal: fatal.clone(),
                root: token.clone(),
            };
            set.spawn(driver::run(
                collector,
                reconnect_config,
                event_sender.clone(),
                tokens,
            ));
        }

        // Sync each strategy and spawn its task. The receiver was subscribed
        // above, before any collector could emit, so events produced during
        // sync are buffered for it. Cancellation is respected during sync via
        // the token.
        for (mut strategy, event_receiver) in strategies {
            let action_sender = action_sender.clone();
            let child = token.child_token();

            info!("syncing strategy state...");
            tokio::select! {
                _ = token.cancelled() => {
                    // A collector may have escalated to `Fatal` during sync,
                    // cancelling the root token. Hand back the handle so the
                    // caller still observes `fatal` (already cancelled) and
                    // follows the documented exit path. Only a caller-initiated
                    // cancellation (fatal unset) is a plain error.
                    if fatal.is_cancelled() {
                        return Ok(EngineHandle {
                            token,
                            tasks: set,
                            fatal,
                        });
                    }
                    return Err(anyhow::anyhow!("engine cancelled during strategy sync"));
                }
                result = strategy.sync_state() => {
                    result?;
                }
            }

            set.spawn(async move {
                info!("starting strategy... ");
                let mut events = Box::pin(channel::into_stream(
                    event_receiver,
                    child.clone(),
                    "strategy",
                ));
                while let Some(event) = events.next().await {
                    match strategy.process_event(event).await {
                        Ok(action_stream) => {
                            // Drain the actions, but stop mid-stream on shutdown
                            // rather than finishing a long stream first. Pinned on
                            // the stack so the per-event drain costs no allocation.
                            let mut actions = std::pin::pin!(
                                action_stream.take_until(child.clone().cancelled_owned())
                            );
                            while let Some(action) = actions.next().await {
                                if let Err(e) = action_sender.send(action) {
                                    error!("error sending action: {e}");
                                }
                            }
                        }
                        Err(e) => error!("error processing event: {e}"),
                    }
                }
            });
        }

        Ok(EngineHandle {
            token,
            tasks: set,
            fatal,
        })
    }
}
