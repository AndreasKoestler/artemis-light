use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use alloy::{primitives::TxHash, providers::Provider, rpc::types::Transaction};
use anyhow::Result;
use async_trait::async_trait;
use futures::StreamExt;

use crate::collectors::fallback::subscribe_or_poll;
use crate::types::{Collector, CollectorStream};

const DEFAULT_RPC_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_MAX_CONCURRENT_LOOKUPS: usize = 16;

/// A collector that listens for new transactions in the mempool, and generates a stream of
/// [events](Transaction) which contain the transaction.
pub struct MempoolCollector<M> {
    provider: Arc<M>,
    /// Maximum number of concurrent transaction lookups.
    max_concurrent_lookups: usize,
    /// Timeout for individual RPC calls.
    rpc_timeout: Duration,
}

impl<M> MempoolCollector<M> {
    /// Creates a new `MempoolCollector` with default settings.
    pub fn new(provider: Arc<M>) -> Self {
        Self {
            provider,
            max_concurrent_lookups: DEFAULT_MAX_CONCURRENT_LOOKUPS,
            rpc_timeout: DEFAULT_RPC_TIMEOUT,
        }
    }

    /// Sets the maximum number of concurrent transaction lookups. A
    /// [`NonZeroUsize`] makes a zero cap (which could never look anything up)
    /// unrepresentable at the call site.
    pub fn with_max_concurrent_lookups(mut self, max: NonZeroUsize) -> Self {
        self.max_concurrent_lookups = max.get();
        self
    }

    /// Sets the timeout for individual RPC calls.
    pub fn with_rpc_timeout(mut self, timeout: Duration) -> Self {
        self.rpc_timeout = timeout;
        self
    }
}

/// Implementation of the [Collector](Collector) trait for the [MempoolCollector](MempoolCollector).
#[async_trait]
impl<M> Collector<Transaction> for MempoolCollector<M>
where
    M: Provider,
{
    async fn subscribe(&self) -> Result<CollectorStream<'_, Transaction>> {
        let hashes = subscribe_or_poll(
            "pending transactions",
            self.subscription_hashes(),
            self.polling_hashes(),
        )
        .await?;

        // Both sources yield bare hashes; the full-transaction lookup is
        // shared and applied once, after the source is chosen.
        let provider = self.provider.clone();
        let rpc_timeout = self.rpc_timeout;
        let stream = hashes
            .map(move |tx_hash| {
                let provider = provider.clone();
                async move {
                    match tokio::time::timeout(
                        rpc_timeout,
                        provider.get_transaction_by_hash(tx_hash),
                    )
                    .await
                    {
                        Ok(Ok(tx)) => tx,
                        Ok(Err(e)) => {
                            tracing::warn!(
                                "Failed to get transaction by hash {:?}: {}",
                                tx_hash,
                                e
                            );
                            None
                        }
                        Err(_) => {
                            tracing::warn!("Timeout getting transaction by hash {:?}", tx_hash);
                            None
                        }
                    }
                }
            })
            .buffer_unordered(self.max_concurrent_lookups)
            .filter_map(|tx| async { tx });

        Ok(Box::pin(stream))
    }
}

impl<M> MempoolCollector<M>
where
    M: Provider,
{
    /// Pending-tx hashes over pubsub. Fails on transports without pubsub.
    async fn subscription_hashes(&self) -> Result<CollectorStream<'_, TxHash>> {
        let stream = self
            .provider
            .subscribe_pending_transactions()
            .await?
            .into_stream();
        Ok(Box::pin(stream))
    }

    /// Pending-tx hashes via a polled filter; the poller yields batches,
    /// flattened here to match the subscription's shape.
    async fn polling_hashes(&self) -> Result<CollectorStream<'_, TxHash>> {
        let poller = self.provider.watch_pending_transactions().await?;
        Ok(Box::pin(
            poller.into_stream().flat_map(futures::stream::iter),
        ))
    }
}
