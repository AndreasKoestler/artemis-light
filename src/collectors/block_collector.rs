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
        if let Ok(subscription) = self.provider.subscribe_blocks().await {
            let stream = subscription.into_stream().map(|header| NewBlock {
                hash: header.hash,
                number: header.number,
            });
            Ok(Box::pin(stream) as CollectorStream<'_, NewBlock>)
        } else {
            warn!("Error subscribing to blocks; polling instead");
            let stream = self
                .provider
                .watch_full_blocks()
                .await?
                .into_stream()
                .filter_map(|block| match block {
                    Ok(block) => Some(block.header),
                    Err(e) => {
                        warn!("Error polling full block; skipping: {e}");
                        None
                    }
                })
                .map(|header| NewBlock {
                    hash: header.hash,
                    number: header.number,
                });
            Ok(Box::pin(stream) as CollectorStream<'_, NewBlock>)
        }
    }
}
