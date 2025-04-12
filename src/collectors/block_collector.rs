use crate::types::{Collector, CollectorStream};
use alloy::primitives::{BlockHash, U64};
use alloy::providers::Provider;
use anyhow::Result;
use async_trait::async_trait;
use serde::Deserialize;

use std::sync::Arc;
use tokio_stream::StreamExt;

/// A collector that listens for new blocks, and generates a stream of
/// [events](NewBlock) which contain the block number and hash.
///
pub struct BlockCollector<M> {
    provider: Arc<M>,
}

/// A new block event, containing the block number and hash.
#[derive(Debug, Clone, Deserialize)]
pub struct NewBlock {
    pub hash: BlockHash,
    pub number: U64,
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
    async fn get_event_stream(&self) -> Result<CollectorStream<'_, NewBlock>> {
        let stream = self.provider.subscribe_blocks().await?;
        let stream = stream.into_stream().map(|block| NewBlock {
            hash: block.hash,
            number: U64::from(block.number),
        });
        Ok(Box::pin(stream))
    }
}
