use crate::collectors::fallback::subscribe_or_poll;
use crate::types::{Collector, CollectorStream};
use alloy::{
    providers::Provider,
    rpc::types::{Filter, Log},
};
use anyhow::Result;
use async_trait::async_trait;
use futures::StreamExt;
use std::sync::Arc;

/// A collector that listens for new blockchain event logs based on a [Filter],
/// and generates a stream of [events](Log).
pub struct LogCollector<M> {
    provider: Arc<M>,
    filter: Filter,
}

impl<M> LogCollector<M> {
    pub fn new(provider: Arc<M>, filter: Filter) -> Self {
        Self { provider, filter }
    }
}

/// Implementation of the [Collector](Collector) trait for the [LogCollector](LogCollector).
#[async_trait]
impl<M> Collector<Log> for LogCollector<M>
where
    M: Provider,
{
    async fn subscribe(&self) -> Result<CollectorStream<'_, Log>> {
        subscribe_or_poll("logs", self.subscription_stream(), self.polling_stream()).await
    }
}

impl<M> LogCollector<M>
where
    M: Provider,
{
    /// Matching logs over pubsub. Fails on transports without pubsub.
    async fn subscription_stream(&self) -> Result<CollectorStream<'_, Log>> {
        let stream = self.provider.subscribe_logs(&self.filter).await?;
        Ok(Box::pin(stream.into_stream()))
    }

    /// Matching logs via a polled filter; the poller yields batches,
    /// flattened here to match the subscription's shape.
    async fn polling_stream(&self) -> Result<CollectorStream<'_, Log>> {
        let poller = self.provider.watch_logs(&self.filter).await?;
        Ok(Box::pin(
            poller.into_stream().flat_map(futures::stream::iter),
        ))
    }
}
