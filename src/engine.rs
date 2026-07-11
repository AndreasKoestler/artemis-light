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
    /// exhausts its [`ReconnectPolicy`](reconnect::ReconnectPolicy), so the
    /// binary can tell a fatal shutdown
    /// apart from a caller-initiated one. The library never calls
    /// `process::exit`; the binary observes this and decides whether to restart.
    /// Do not cancel it yourself.
    pub fatal: CancellationToken,
}

/// The main engine of Artemis. This struct is responsible for orchestrating the
/// data flow between collectors, strategies, and executors.
pub struct Engine<E, A> {
    collectors: Vec<Box<dyn Collector<E>>>,
    strategies: Vec<Box<dyn Strategy<E, A>>>,
    executors: Vec<Box<dyn Executor<A>>>,
    /// Passive observers of every event and action crossing the channels.
    observers: Vec<Box<dyn Observer<E, A>>>,
    event_channel_capacity: usize,
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
    #[must_use]
    pub fn with_event_channel_capacity(mut self, capacity: usize) -> Self {
        self.event_channel_capacity = capacity;
        self
    }

    #[must_use]
    pub fn with_action_channel_capacity(mut self, capacity: usize) -> Self {
        self.action_channel_capacity = capacity;
        self
    }

    /// Sets the [`ReconnectConfig`] applied to every collector.
    #[must_use]
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
    /// Strategies sync and spawn **before** any collector task starts, so
    /// every strategy is already draining the event channel when the first
    /// event is broadcast — including a Persisted collector's replay, which
    /// runs only on its first subscribe and would otherwise overflow the
    /// bounded ring while a strategy was still syncing. The ring is still
    /// bounded (`event_channel_capacity`): a strategy that processes slower
    /// than collectors produce for a full ring's worth of events lags and
    /// skips (warn-logged in the channel adapter). A `sync_state` that reads
    /// external state observes it strictly before collectors subscribe; a
    /// Persisted collector's backfill covers that gap, a plain live collector
    /// starts at its subscription and delivers nothing from before it.
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
        // Independent of `token` so a caller-initiated shutdown isn't mistaken
        // for a fatal one. Clone + idempotent, so every collector shares it
        // directly — first to escalate wins, the rest are no-ops.
        let fatal = CancellationToken::new();
        let mut set = JoinSet::new();

        // Executors and observers subscribe before any events flow, so their
        // receivers see everything from the first message on.
        spawn_executors(&mut set, self.executors, &action_sender, &token);
        spawn_observers(
            &mut set,
            self.observers,
            &event_sender,
            &action_sender,
            &token,
        );

        // Subscribe every strategy's broadcast receiver up front. A tokio
        // broadcast channel only retains messages for receivers that already
        // exist, so each receiver is pinned to the start of the stream here —
        // before anything could broadcast — rather than to its turn in the
        // sync loop below.
        let strategies: Vec<_> = self
            .strategies
            .into_iter()
            .map(|strategy| (strategy, event_sender.subscribe()))
            .collect();

        // Sync each strategy and spawn its task before any collector exists,
        // so every strategy is draining by the time the first event is
        // broadcast. A cancellation during sync or a strategy's own sync error
        // surfaces as `Err`.
        sync_and_spawn_strategies(&mut set, strategies, &action_sender, &token).await?;

        // Collectors spawn last: nothing they emit — replay, backfill, or live
        // tail — precedes a draining consumer. Each collector is handed to a
        // [Collector Driver](driver) that owns its full lifecycle — subscribe,
        // pump events, and on a lost or failed stream consult the per-collector
        // `ReconnectPolicy` to retry-after-backoff or escalate to `Fatal`.
        spawn_collectors(
            &mut set,
            self.collectors,
            self.reconnect_config,
            &event_sender,
            &token,
            &fatal,
        );

        Ok(EngineHandle {
            token,
            tasks: set,
            fatal,
        })
    }
}

/// Spawns one task per executor. Each subscribes its own action receiver and
/// runs [`executor_task`].
fn spawn_executors<A>(
    set: &mut JoinSet<()>,
    executors: Vec<Box<dyn Executor<A>>>,
    action_sender: &Sender<A>,
    token: &CancellationToken,
) where
    A: Clone + Send + 'static,
{
    for executor in executors {
        set.spawn(executor_task(
            executor,
            action_sender.subscribe(),
            token.child_token(),
        ));
    }
}

/// Drains the action channel, executing each action. A failed `execute` is
/// logged and skipped — one bad action must not tear the executor down — and
/// the loop ends on a closed channel or cooperative shutdown.
async fn executor_task<A>(
    mut executor: Box<dyn Executor<A>>,
    actions: broadcast::Receiver<A>,
    cancel: CancellationToken,
) where
    A: Clone + Send + 'static,
{
    info!("starting executor... ");
    let mut actions = Box::pin(channel::into_stream(actions, cancel, "executor"));
    while let Some(action) = actions.next().await {
        if let Err(e) = executor.execute(action).await {
            error!("error executing action: {e}");
        }
    }
}

/// Spawns one task per observer, each subscribing to both channels before any
/// events flow so its subscriptions see everything.
fn spawn_observers<E, A>(
    set: &mut JoinSet<()>,
    observers: Vec<Box<dyn Observer<E, A>>>,
    event_sender: &Sender<E>,
    action_sender: &Sender<A>,
    token: &CancellationToken,
) where
    E: Clone + Send + 'static,
    A: Clone + Send + 'static,
{
    for observer in observers {
        set.spawn(observer_task(
            observer,
            event_sender.subscribe(),
            action_sender.subscribe(),
            token.child_token(),
            token.child_token(),
        ));
    }
}

/// Observes events and actions as they arrive, in channel order, until both
/// streams end or shutdown fires. Observation is infallible and best-effort: a
/// lagging observer skips messages like any consumer (handled in
/// [`channel::into_stream`]) and has no feedback path into the pipeline.
async fn observer_task<E, A>(
    mut observer: Box<dyn Observer<E, A>>,
    events: broadcast::Receiver<E>,
    actions: broadcast::Receiver<A>,
    events_cancel: CancellationToken,
    actions_cancel: CancellationToken,
) where
    E: Clone + Send + 'static,
    A: Clone + Send + 'static,
{
    info!("starting observer... ");
    let mut events = Box::pin(channel::into_stream(events, events_cancel, "observer").fuse());
    let mut actions = Box::pin(channel::into_stream(actions, actions_cancel, "observer").fuse());
    loop {
        tokio::select! {
            Some(event) = events.next() => observer.observe_event(event).await,
            Some(action) = actions.next() => observer.observe_action(action).await,
            else => break,
        }
    }
}

/// Spawns one [Collector Driver](driver) per collector. A `Fatal` verdict from a
/// driver cancels `fatal` (the reason) and then the root `token` (tearing down
/// every task); the library never calls `process::exit`.
fn spawn_collectors<E>(
    set: &mut JoinSet<()>,
    collectors: Vec<Box<dyn Collector<E>>>,
    reconnect_config: ReconnectConfig,
    event_sender: &Sender<E>,
    token: &CancellationToken,
    fatal: &CancellationToken,
) where
    E: Clone + Send + 'static,
{
    for collector in collectors {
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
}

/// A strategy paired with the event receiver subscribed for it up front — see
/// the subscribe-then-sync-then-collectors ordering in [`Engine::run`].
type StrategyWithReceiver<E, A> = (Box<dyn Strategy<E, A>>, broadcast::Receiver<E>);

/// Syncs each strategy in turn, then spawns its [`strategy_task`]. Runs before
/// any collector is spawned, so no event can be broadcast while a strategy is
/// still syncing.
///
/// Returns `Err` on a strategy's own `sync_state` failure or a cancellation
/// during sync.
async fn sync_and_spawn_strategies<E, A>(
    set: &mut JoinSet<()>,
    strategies: Vec<StrategyWithReceiver<E, A>>,
    action_sender: &Sender<A>,
    token: &CancellationToken,
) -> anyhow::Result<()>
where
    E: Clone + Send + 'static,
    A: Clone + Send + 'static,
{
    for (mut strategy, events) in strategies {
        info!("syncing strategy state...");
        tokio::select! {
            _ = token.cancelled() => {
                return Err(anyhow::anyhow!("engine cancelled during strategy sync"));
            }
            result = strategy.sync_state() => {
                result?;
            }
        }

        set.spawn(strategy_task(
            strategy,
            events,
            action_sender.clone(),
            token.child_token(),
        ));
    }
    Ok(())
}

/// Drains events to a strategy, forwarding each produced action to the action
/// channel. A failed `process_event` is logged and skipped; a failed `send`
/// (no executors listening) is logged. Each event's action stream stops
/// mid-drain on shutdown rather than finishing a long stream first.
async fn strategy_task<E, A>(
    mut strategy: Box<dyn Strategy<E, A>>,
    events: broadcast::Receiver<E>,
    action_sender: Sender<A>,
    cancel: CancellationToken,
) where
    E: Clone + Send + 'static,
    A: Clone + Send + 'static,
{
    info!("starting strategy... ");
    let mut events = Box::pin(channel::into_stream(events, cancel.clone(), "strategy"));
    while let Some(event) = events.next().await {
        match strategy.process_event(event).await {
            Ok(action_stream) => {
                // Pinned on the stack so the per-event drain costs no allocation.
                let mut actions =
                    std::pin::pin!(action_stream.take_until(cancel.clone().cancelled_owned()));
                while let Some(action) = actions.next().await {
                    if let Err(e) = action_sender.send(action) {
                        error!("error sending action: {e}");
                    }
                }
            }
            Err(e) => error!("error processing event: {e}"),
        }
    }
}
