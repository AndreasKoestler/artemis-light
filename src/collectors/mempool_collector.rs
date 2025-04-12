use async_trait::async_trait;

use alloy::{providers::Provider, rpc::types::Transaction};
use std::sync::Arc;

use crate::types::{Collector, CollectorStream};
use anyhow::Result;
use futures::StreamExt;

/// A collector that listens for new transactions in the mempool, and generates a stream of
/// [events](Transaction) which contain the transaction.
pub struct MempoolCollector<M> {
    provider: Arc<M>,
}

impl<M> MempoolCollector<M> {
    pub fn new(provider: Arc<M>) -> Self {
        Self { provider }
    }
}

/// Implementation of the [Collector](Collector) trait for the [MempoolCollector](MempoolCollector).
#[async_trait]
impl<M> Collector<Transaction> for MempoolCollector<M>
where
    M: Provider,
{
    async fn get_event_stream(&self) -> Result<CollectorStream<'_, Transaction>> {
        let stream = self
            .provider
            .subscribe_pending_transactions()
            .await?
            .into_stream();

        let stream = stream.filter_map(move |tx_hash| async move {
            if let Ok(tx) = self.provider.get_transaction_by_hash(tx_hash).await {
                tx
            } else {
                tracing::warn!("Failed to get transaction by hash: {:?}", tx_hash);
                None
            }
        });

        Ok(Box::pin(stream))
    }
}
