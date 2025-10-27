use async_trait::async_trait;

use crate::types::{Metrics, Strategy};

pub struct StrategyInstrument<S, M> {
    strategy: S,
    metrics: M,
    ignore_errors: bool,
}

impl<S, M> StrategyInstrument<S, M> {
    pub fn new(strategy: S, metrics: M, ignore_errors: bool) -> Self {
        Self {
            strategy,
            metrics,
            ignore_errors,
        }
    }
}

#[async_trait]
impl<E, A, S, M> Strategy<E, A> for StrategyInstrument<S, M>
where
    S: Strategy<E, A> + 'static,
    M: Metrics<S> + Send + Sync + 'static,
    A: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    async fn process_event(&mut self, event: E) -> Vec<A> {
        let res = self.strategy.process_event(event).await;
        let _ = self.metrics.collect_metrics(&self.strategy).await;
        res
    }
    async fn sync_state(&mut self) -> anyhow::Result<()> {
        self.strategy.sync_state().await?;
        match self.metrics.collect_metrics(&self.strategy).await {
            Err(_) if self.ignore_errors => Ok(()),
            res => res,
        }
    }
}
