use crate::collectors::fallback::subscribe_or_poll;
use crate::types::{Collector, CollectorStream};
use alloy::primitives::BlockHash;
use alloy::providers::Provider;
use anyhow::Result;
use async_trait::async_trait;
use tracing::warn;

use futures::StreamExt;
use std::sync::Arc;

/// Total header-fetch attempts per polled hash before it is dropped.
const MAX_FETCH_ATTEMPTS: usize = 3;

/// Concurrent header fetches per poll tick.
const FETCH_CONCURRENCY: usize = 4;

/// Bounded retry bookkeeping for the polling path: hashes whose header fetch
/// failed are retried on later poll ticks until they succeed or exhaust
/// [`MAX_FETCH_ATTEMPTS`], so a transient HTTP hiccup doesn't silently drop a
/// block from per-block accounting.
struct FetchRetryQueue {
    max_attempts: usize,
    /// `(hash, failures so far)`, in first-failed order.
    pending: Vec<(BlockHash, usize)>,
}

impl FetchRetryQueue {
    fn new(max_attempts: usize) -> Self {
        Self {
            max_attempts,
            pending: Vec::new(),
        }
    }

    /// Hashes due for another attempt this tick, in first-failed order.
    fn due(&self) -> Vec<BlockHash> {
        self.pending.iter().map(|(hash, _)| *hash).collect()
    }

    /// Note a failed fetch. Returns `false` once the hash has exhausted its
    /// attempts and is dropped — the caller should warn loudly and move on.
    fn record_failure(&mut self, hash: BlockHash) -> bool {
        let failures = match self.pending.iter_mut().find(|(h, _)| *h == hash) {
            Some((_, failures)) => {
                *failures += 1;
                *failures
            }
            None => {
                self.pending.push((hash, 1));
                1
            }
        };
        if failures >= self.max_attempts {
            self.pending.retain(|(h, _)| *h != hash);
            return false;
        }
        true
    }

    /// A later attempt succeeded; stop retrying the hash.
    fn resolve(&mut self, hash: &BlockHash) {
        self.pending.retain(|(h, _)| h != hash);
    }
}

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
            // Failed fetches retry in-stream rather than ending the stream:
            // the stream carries bare `NewBlock`s (no error channel), and a
            // stream end would make the Reconnect Policy tear down and
            // rebuild the whole block filter over one flaky header fetch.
            let mut retries = FetchRetryQueue::new(MAX_FETCH_ATTEMPTS);
            while let Some(batch) = hashes.next().await {
                let mut to_fetch = retries.due();
                to_fetch.extend(batch);
                // Fetch concurrently with output order preserved, so a slow
                // provider doesn't fall behind the poll cadence.
                let results = futures::stream::iter(to_fetch)
                    .map(|hash| {
                        let provider = provider.clone();
                        async move { (hash, provider.get_block_by_hash(hash).await) }
                    })
                    .buffered(FETCH_CONCURRENCY)
                    .collect::<Vec<_>>()
                    .await;
                for (hash, result) in results {
                    let reason = match result {
                        Ok(Some(block)) => {
                            retries.resolve(&hash);
                            yield NewBlock {
                                hash: block.header.hash,
                                number: block.header.number,
                            };
                            continue;
                        }
                        Ok(None) => "not found".to_string(),
                        Err(e) => format!("fetch failed: {e}"),
                    };
                    if retries.record_failure(hash) {
                        warn!("Polled block {hash} {reason}; retrying next poll tick");
                    } else {
                        warn!(
                            "Polled block {hash} {reason}; giving up after \
                             {MAX_FETCH_ATTEMPTS} attempts"
                        );
                    }
                }
            }
        };
        Ok(Box::pin(stream))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash(n: u8) -> BlockHash {
        BlockHash::with_last_byte(n)
    }

    #[test]
    fn failed_hash_is_due_for_retry_on_the_next_tick() {
        let mut retries = FetchRetryQueue::new(3);
        assert!(
            retries.record_failure(hash(1)),
            "first failure must requeue"
        );
        assert_eq!(retries.due(), vec![hash(1)]);
    }

    #[test]
    fn resolved_hash_is_no_longer_due() {
        let mut retries = FetchRetryQueue::new(3);
        retries.record_failure(hash(1));
        retries.resolve(&hash(1));
        assert!(retries.due().is_empty());
    }

    #[test]
    fn hash_is_dropped_after_max_attempts() {
        let mut retries = FetchRetryQueue::new(3);
        assert!(retries.record_failure(hash(1)));
        assert!(retries.record_failure(hash(1)));
        assert!(
            !retries.record_failure(hash(1)),
            "the exhausting failure must report the drop"
        );
        assert!(retries.due().is_empty(), "an exhausted hash is not retried");
    }

    #[test]
    fn due_preserves_first_failed_order() {
        let mut retries = FetchRetryQueue::new(3);
        retries.record_failure(hash(2));
        retries.record_failure(hash(1));
        retries.record_failure(hash(2));
        assert_eq!(retries.due(), vec![hash(2), hash(1)]);
    }
}
