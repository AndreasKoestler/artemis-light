use crate::types::{ActionStream, Strategy};
use anyhow::Result;
use async_trait::async_trait;
use futures::StreamExt;
use futures::future::ready;

/// `FilterActions` is a wrapper around a [`Strategy`] that drops every action
/// failing `predicate` — a risk gate on the output side: minimum profit,
/// maximum notional, allowlisted targets. Keeping it as a combinator makes
/// the risk policy visible at composition time rather than buried inside
/// strategy logic.
pub struct FilterActions<E, A, P> {
    strategy: Box<dyn Strategy<E, A>>,
    predicate: P,
}

impl<E, A, P> FilterActions<E, A, P> {
    /// Creates a new `FilterActions` wrapping `strategy` with `predicate`.
    pub fn new(strategy: Box<dyn Strategy<E, A>>, predicate: P) -> Self {
        Self {
            strategy,
            predicate,
        }
    }
}

#[async_trait]
impl<E, A, P> Strategy<E, A> for FilterActions<E, A, P>
where
    E: Send + Sync + 'static,
    A: Send + Sync + 'static,
    P: Fn(&A) -> bool + Send + Sync + 'static,
{
    async fn sync_state(&mut self) -> Result<()> {
        self.strategy.sync_state().await
    }

    async fn process_event(&mut self, event: E) -> Result<ActionStream<'_, A>> {
        // `&mut self.strategy` and `&self.predicate` are disjoint field
        // borrows, so the returned stream can hold both for the same
        // lifetime — no need to clone the closure.
        let predicate = &self.predicate;
        let stream = self.strategy.process_event(event).await?;
        Ok(Box::pin(stream.filter(move |a| ready(predicate(a)))))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::strategy_ext::StrategyExt;
    use futures::{StreamExt, stream};
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    /// Emits its event and the two following numbers as actions.
    struct TripleStrategy;

    #[async_trait]
    impl Strategy<u32, u32> for TripleStrategy {
        async fn sync_state(&mut self) -> Result<()> {
            Ok(())
        }

        async fn process_event(&mut self, event: u32) -> Result<ActionStream<'_, u32>> {
            Ok(Box::pin(stream::iter(vec![event, event + 1, event + 2])))
        }
    }

    #[tokio::test]
    async fn actions_failing_the_predicate_are_dropped() {
        let mut strategy = TripleStrategy.filter_actions(|a: &u32| a.is_multiple_of(2));
        let actions = strategy
            .process_event(1)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        assert_eq!(actions, vec![2], "1 and 3 fail the risk gate");
    }

    #[tokio::test]
    async fn a_batch_failing_entirely_yields_an_empty_stream_not_an_error() {
        let mut strategy = TripleStrategy.filter_actions(|_: &u32| false);
        let actions = strategy
            .process_event(1)
            .await
            .expect("a fully-filtered batch is not an error")
            .collect::<Vec<_>>()
            .await;
        assert!(actions.is_empty());
    }

    /// Flags when `sync_state` reaches it.
    struct SyncProbe {
        synced: Arc<AtomicBool>,
    }

    #[async_trait]
    impl Strategy<u32, u32> for SyncProbe {
        async fn sync_state(&mut self) -> Result<()> {
            self.synced.store(true, Ordering::SeqCst);
            Ok(())
        }

        async fn process_event(&mut self, _event: u32) -> Result<ActionStream<'_, u32>> {
            Ok(Box::pin(stream::empty()))
        }
    }

    #[tokio::test]
    async fn sync_state_is_delegated_to_the_inner_strategy() {
        let synced = Arc::new(AtomicBool::new(false));
        let mut strategy = SyncProbe {
            synced: Arc::clone(&synced),
        }
        .filter_actions(|_: &u32| true);
        strategy.sync_state().await.unwrap();
        assert!(synced.load(Ordering::SeqCst));
    }

    /// A strategy whose every method fails.
    struct FailingStrategy;

    #[async_trait]
    impl Strategy<u32, u32> for FailingStrategy {
        async fn sync_state(&mut self) -> Result<()> {
            anyhow::bail!("sync failed")
        }

        async fn process_event(&mut self, _event: u32) -> Result<ActionStream<'_, u32>> {
            anyhow::bail!("process failed")
        }
    }

    #[tokio::test]
    async fn inner_errors_propagate_unchanged() {
        let mut strategy = FailingStrategy.filter_actions(|_: &u32| true);
        assert!(strategy.sync_state().await.is_err());
        assert!(strategy.process_event(1).await.is_err());
    }
}
