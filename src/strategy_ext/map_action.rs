use crate::types::{ActionStream, Strategy};
use anyhow::Result;
use async_trait::async_trait;
use futures::StreamExt;

/// `MapAction` is a wrapper around a [`Strategy`] that lifts its actions into
/// a wider type `A2` — typically an umbrella-enum constructor.
pub struct MapAction<E, A, F> {
    strategy: Box<dyn Strategy<E, A>>,
    f: F,
}

impl<E, A, F> MapAction<E, A, F> {
    /// Creates a new `MapAction` wrapping `strategy` with the lifting
    /// function `f`.
    pub fn new(strategy: Box<dyn Strategy<E, A>>, f: F) -> Self {
        Self { strategy, f }
    }
}

#[async_trait]
impl<E, A, A2, F> Strategy<E, A2> for MapAction<E, A, F>
where
    E: Send + Sync + 'static,
    A: Send + Sync + 'static,
    A2: Send + Sync + 'static,
    F: Fn(A) -> A2 + Send + Sync + 'static,
{
    async fn sync_state(&mut self) -> Result<()> {
        self.strategy.sync_state().await
    }

    async fn process_event(&mut self, event: E) -> Result<ActionStream<'_, A2>> {
        // `&mut self.strategy` and `&self.f` are disjoint field borrows, so
        // the returned stream can hold both for the same lifetime — no need
        // to clone the closure.
        let f = &self.f;
        let stream = self.strategy.process_event(event).await?;
        Ok(Box::pin(stream.map(f)))
    }
}
