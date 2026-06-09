use alloy::rpc::types::Transaction;
use anyhow::Result;
use async_trait::async_trait;
use futures::Stream;
use std::pin::Pin;

use crate::collectors::NewBlock;

use crate::executors::SubmitTxToMempool;

/// A stream of events emitted by a [Collector].
pub type CollectorStream<'a, E> = Pin<Box<dyn Stream<Item = E> + Send + 'a>>;
/// A stream of actions produced by a [Strategy].
pub type ActionStream<'a, A> = Pin<Box<dyn Stream<Item = A> + Send + 'a>>;

/// Collector trait, which defines a source of events.
#[async_trait]
pub trait Collector<E>: Send + Sync {
    /// Returns the core event stream for the collector.
    async fn subscribe(&self) -> Result<CollectorStream<'_, E>>;

    /// Deprecated alias for [`subscribe`](Collector::subscribe).
    #[deprecated(since = "0.1.0", note = "Use `subscribe` instead")]
    async fn get_event_stream(&self) -> Result<CollectorStream<'_, E>> {
        self.subscribe().await
    }
}

/// Strategy trait, which defines the core logic for each opportunity.
#[async_trait]
pub trait Strategy<E, A>: Send + Sync {
    /// Sync the initial state of the strategy if needed, usually by fetching
    /// onchain data.
    async fn sync_state(&mut self) -> Result<()>;

    /// Process an event, and return an action if needed.
    async fn process_event(&mut self, event: E) -> Result<ActionStream<'_, A>>;
}

/// Executor trait, responsible for executing actions returned by strategies.
#[async_trait]
pub trait Executor<A>: Send + Sync {
    /// Execute an action.
    async fn execute(&mut self, action: A) -> Result<()>;
}

/// A wrapper around an [Executor] that filter-maps incoming actions,
/// silently dropping actions that map to `None`.
pub struct ExecutorFilterMap<E, F> {
    executor: E,
    f: F,
}

impl<E, F> ExecutorFilterMap<E, F> {
    /// Creates a new `ExecutorFilterMap` wrapping `executor` with the filter-map function `f`.
    pub fn new(executor: E, f: F) -> Self {
        Self { executor, f }
    }
}

#[async_trait]
impl<A1, A2, E, F> Executor<A1> for ExecutorFilterMap<E, F>
where
    E: Executor<A2> + Send + Sync + 'static,
    F: Fn(A1) -> Option<A2> + Send + Sync + Clone + 'static,
    A1: Send + Sync + 'static,
    A2: Send + Sync + 'static,
{
    async fn execute(&mut self, action: A1) -> Result<()> {
        let action = (self.f)(action);
        match action {
            Some(action) => self.executor.execute(action).await,
            None => Ok(()),
        }
    }
}

/// Convenience enum containing all the events that can be emitted by collectors.
pub enum Events {
    NewBlock(NewBlock),
    Transaction(Box<Transaction>),
}

/// Convenience enum containing all the actions that can be executed by executors.
pub enum Actions {
    //    FlashbotsBundle(FlashbotsBundle),
    SubmitTxToMempool(SubmitTxToMempool),
}

/// A passive consumer of the pipeline: sees every event fanned to
/// [Strategies](Strategy) and every action fanned to [Executors](Executor),
/// and can produce or perturb neither.
///
/// An Observer is just another subscriber on the Engine's broadcast channels,
/// so observation is best-effort with the same semantics as any consumer: a
/// lagging Observer skips messages (logged) rather than slowing the pipeline.
/// Events and actions each arrive in channel order, but no ordering is
/// guaranteed *between* the two. Both methods default to ignoring their input,
/// so an adapter overrides only the side it cares about; neither returns a
/// `Result` — there is deliberately no error channel through which observation
/// could fail the pipeline.
#[async_trait]
pub trait Observer<E: Send + 'static, A: Send + 'static>: Send + Sync {
    /// Called with every event fanned to Strategies. Default: ignore.
    async fn observe_event(&mut self, event: E) {
        let _ = event;
    }

    /// Called with every action fanned to Executors. Default: ignore.
    async fn observe_action(&mut self, action: A) {
        let _ = action;
    }
}
