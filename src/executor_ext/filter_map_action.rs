use crate::types::Executor;
use anyhow::Result;
use async_trait::async_trait;

/// `FilterMapAction` is a wrapper around an [`Executor`] that routes only
/// matching actions to it: actions mapped to `None` are skipped with
/// `Ok(())` and never reach the inner executor.
pub struct FilterMapAction<A, F> {
    executor: Box<dyn Executor<A>>,
    f: F,
}

impl<A, F> FilterMapAction<A, F> {
    /// Creates a new `FilterMapAction` wrapping `executor` with the routing
    /// function `f`.
    pub fn new(executor: Box<dyn Executor<A>>, f: F) -> Self {
        Self { executor, f }
    }
}

#[async_trait]
impl<A, A2, F> Executor<A2> for FilterMapAction<A, F>
where
    A: Send + Sync + 'static,
    A2: Send + Sync + 'static,
    F: Fn(A2) -> Option<A> + Send + Sync + 'static,
{
    async fn execute(&mut self, action: A2) -> Result<()> {
        match (self.f)(action) {
            Some(action) => self.executor.execute(action).await,
            None => Ok(()),
        }
    }
}
