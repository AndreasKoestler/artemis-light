use alloy::rpc::types::Transaction;
use anyhow::Result;
use async_trait::async_trait;
use jsonrpsee::core::DeserializeOwned;
use std::pin::Pin;
use tokio_stream::Stream;
use tokio_stream::StreamExt;
use tokio_stream::StreamMap;

use crate::collectors::block_collector::NewBlock;

use crate::executors::mempool_executor::SubmitTxToMempool;

/// A stream of events emitted by a [Collector].
pub type CollectorStream<'a, E> = Pin<Box<dyn Stream<Item = E> + Send + 'a>>;

/// Collector trait, which defines a source of events.
#[async_trait]
pub trait Collector<E>: Send + Sync {
    /// Returns the core event stream for the collector.
    async fn get_event_stream(&self) -> Result<CollectorStream<'_, E>>;
}

/// Strategy trait, which defines the core logic for each opportunity.
#[async_trait]
pub trait Strategy<E, A>: Send + Sync {
    /// Sync the initial state of the strategy if needed, usually by fetching
    /// onchain data.
    async fn sync_state(&mut self) -> Result<()>;

    /// Process an event, and return an action if needed.
    async fn process_event(&mut self, event: E) -> Vec<A>;
}

/// Executor trait, responsible for executing actions returned by strategies.
#[async_trait]
pub trait Executor<A>: Send + Sync {
    /// Execute an action.
    async fn execute(&self, action: A) -> Result<()>;
}

/// CollectorMap is a wrapper around a [Collector] that maps outgoing
/// events to a different type.
pub struct CollectorMap<E, F> {
    collector: Box<dyn Collector<E>>,
    f: F,
}
impl<E, F> CollectorMap<E, F> {
    pub fn new(collector: Box<dyn Collector<E>>, f: F) -> Self {
        Self { collector, f }
    }
}

#[async_trait]
impl<E1, E2, F> Collector<E2> for CollectorMap<E1, F>
where
    E1: Send + Sync + DeserializeOwned + 'static,
    E2: Send + Sync + DeserializeOwned + 'static,
    F: Fn(E1) -> E2 + Send + Sync + Clone + 'static,
{
    async fn get_event_stream(&self) -> Result<CollectorStream<'_, E2>> {
        let stream = self.collector.get_event_stream().await?;
        let f = self.f.clone();
        let stream = stream.map(f);
        Ok(Box::pin(stream))
    }
}
/// FilterCollectorMap is a wrapper around a [Collector] that maps outgoing
/// events to a different type.
pub struct FilterCollectorMap<E, F> {
    collector: Box<dyn Collector<E>>,
    f: F,
}
impl<E, F> FilterCollectorMap<E, F> {
    pub fn new(collector: Box<dyn Collector<E>>, f: F) -> Self {
        Self { collector, f }
    }
}

#[async_trait]
impl<E1, E2, F> Collector<E2> for FilterCollectorMap<E1, F>
where
    E1: Send + Sync + DeserializeOwned + 'static,
    E2: Send + Sync + DeserializeOwned + 'static,
    F: Fn(E1) -> Option<E2> + Send + Sync + Clone + 'static,
{
    async fn get_event_stream(&self) -> Result<CollectorStream<'_, E2>> {
        let stream = self.collector.get_event_stream().await?;
        let f = self.f.clone();
        let stream = stream.filter_map(f);
        Ok(Box::pin(stream))
    }
}

pub struct CollectorMerge<C1, C2> {
    this: C1,
    other: C2,
}

impl<C1, C2> CollectorMerge<C1, C2> {
    pub fn new(this: C1, other: C2) -> Self {
        Self { this, other }
    }
}

#[async_trait]
impl<C1, C2, E> Collector<E> for CollectorMerge<C1, C2>
where
    C1: Collector<E> + Send + Sync + 'static,
    C2: Collector<E> + Send + Sync + 'static,
    E: Send + Sync + DeserializeOwned + 'static,
{
    async fn get_event_stream(&self) -> Result<CollectorStream<'_, E>> {
        let this_stream = self.this.get_event_stream().await?;
        let other_stream = self.other.get_event_stream().await?;

        let merged = Box::pin(this_stream.merge(other_stream)) as CollectorStream<'_, E>;
        Ok(merged)
    }
}

#[async_trait]
impl<E: 'static, C: Collector<E>> Collector<E> for Vec<Box<C>> {
    async fn get_event_stream(&self) -> Result<CollectorStream<'_, E>> {
        let mut smap = StreamMap::new();
        for (id, collector) in self.iter().enumerate() {
            let stream = collector.get_event_stream().await?;
            smap.insert(id, stream);
        }
        let stream = Box::pin(smap.map(|(_, v)| v)) as CollectorStream<'_, E>;
        Ok(stream)
    }
}

/// ExecutorMap is a wrapper around an [Executor] that maps incoming
/// actions to a different type.
pub struct ExecutorMap<A, F> {
    executor: Box<dyn Executor<A>>,
    f: F,
}

impl<A, F> ExecutorMap<A, F> {
    pub fn new(executor: Box<dyn Executor<A>>, f: F) -> Self {
        Self { executor, f }
    }
}

#[async_trait]
impl<A1, A2, F> Executor<A1> for ExecutorMap<A2, F>
where
    A1: Send + Sync + 'static,
    A2: Send + Sync + 'static,
    F: Fn(A1) -> Option<A2> + Send + Sync + Clone + 'static,
{
    async fn execute(&self, action: A1) -> Result<()> {
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
