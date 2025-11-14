use async_trait::async_trait;
use futures::StreamExt;

use crate::types::{ActionStream, Metrics, Strategy};

pub struct StrategyInstrument<S, M> {
    strategy: S,
    metrics: M,
}

impl<S, M> StrategyInstrument<S, M> {
    pub fn new(strategy: S, metrics: M) -> Self {
        Self { strategy, metrics }
    }
}

#[async_trait]
impl<E, A, S, M> Strategy<E, A> for StrategyInstrument<S, M>
where
    S: Strategy<E, A> + 'static,
    M: Metrics<A> + Send + Sync + 'static,
    A: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    async fn process_event(&mut self, event: E) -> anyhow::Result<ActionStream<'_, A>> {
        let res = self.strategy.process_event(event).await?;
        let res = res.then(|action| async {
            let _ = self.metrics.collect_metrics(&action).await;
            action
        });
        Ok(Box::pin(res))
    }
    async fn sync_state(&mut self) -> anyhow::Result<()> {
        self.strategy.sync_state().await
    }
}
