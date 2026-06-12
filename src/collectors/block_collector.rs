use crate::collectors::fallback::subscribe_or_poll;
use crate::types::{Collector, CollectorStream};
use alloy::primitives::BlockHash;
use alloy::providers::Provider;
use anyhow::Result;
use async_trait::async_trait;
use tracing::warn;

use std::sync::Arc;
use tokio_stream::StreamExt;

/// A collector that listens for new blocks, and generates a stream of
/// [events](NewBlock) which contain the block number and hash.
///
pub struct BlockCollector<M> {
    provider: Arc<M>,
}

/// A new block event, containing the block number and hash.
#[derive(Debug, Clone)]
pub struct NewBlock {
    pub hash: BlockHash,
    pub number: u64,
}

impl<M> BlockCollector<M> {
    pub fn new(provider: Arc<M>) -> Self {
        Self { provider }
    }
}

/// Implementation of the [Collector](Collector) trait for the [BlockCollector](BlockCollector).
#[async_trait]
impl<M> Collector<NewBlock> for BlockCollector<M>
where
    M: Provider,
{
    async fn subscribe(&self) -> Result<CollectorStream<'_, NewBlock>> {
        subscribe_or_poll("blocks", self.subscription_stream(), self.polling_stream()).await
    }
}

impl<M> BlockCollector<M>
where
    M: Provider,
{
    /// New-block headers over pubsub. Fails on transports without pubsub
    /// (most commonly plain HTTP), which is the cue to poll instead.
    async fn subscription_stream(&self) -> Result<CollectorStream<'_, NewBlock>> {
        let subscription = self.provider.subscribe_blocks().await?;
        let stream = subscription.into_stream().map(|header| NewBlock {
            hash: header.hash,
            number: header.number,
        });
        Ok(Box::pin(stream))
    }

    /// Poll block *hashes* and fetch each header on demand. A `NewBlock`
    /// needs only the header, so polling full blocks would download every
    /// transaction body just to throw it away.
    async fn polling_stream(&self) -> Result<CollectorStream<'_, NewBlock>> {
        let mut hashes = self.provider.watch_blocks().await?.into_stream();
        let provider = self.provider.clone();
        let stream = async_stream::stream! {
            while let Some(batch) = hashes.next().await {
                for hash in batch {
                    match provider.get_block_by_hash(hash).await {
                        Ok(Some(block)) => {
                            yield NewBlock {
                                hash: block.header.hash,
                                number: block.header.number,
                            };
                        }
                        Ok(None) => {
                            warn!("Polled block {hash} not found; skipping")
                        }
                        Err(e) => {
                            warn!("Error fetching polled block {hash}; skipping: {e}")
                        }
                    }
                }
            }
        };
        Ok(Box::pin(stream))
    }
}
