use async_trait::async_trait;

use crate::types::{Executor, Metrics};

pub struct ExecutorInstrument<E, M> {
    executor: E,
    metrics: M,
}

impl<E, M> ExecutorInstrument<E, M> {
    pub fn new(executor: E, metrics: M) -> Self {
        Self { executor, metrics }
    }
}

#[async_trait]
impl<A, E, M> Executor<A> for ExecutorInstrument<E, M>
where
    E: Executor<A> + 'static,
    M: Metrics<E> + Send + Sync + 'static,
    A: Send + Sync + 'static,
{
    async fn execute(&mut self, action: A) -> anyhow::Result<()> {
        self.executor.execute(action).await?;
        self.metrics.collect_metrics(&self.executor).await?;
        Ok(())
    }
}
