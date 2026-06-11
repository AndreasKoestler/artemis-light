use crate::types::{ActionStream, Strategy};
use anyhow::Result;
use async_trait::async_trait;

/// `FilterMapEvent` is a wrapper around a [`Strategy`] that projects a wider
/// event type `E2` down to the strategy's own event type `E`: events mapped
/// to `None` yield an empty action stream instead of reaching the strategy.
pub struct FilterMapEvent<E, A, F> {
    strategy: Box<dyn Strategy<E, A>>,
    f: F,
}

impl<E, A, F> FilterMapEvent<E, A, F> {
    /// Creates a new `FilterMapEvent` wrapping `strategy` with the projection
    /// function `f`.
    pub fn new(strategy: Box<dyn Strategy<E, A>>, f: F) -> Self {
        Self { strategy, f }
    }
}

#[async_trait]
impl<E, E2, A, F> Strategy<E2, A> for FilterMapEvent<E, A, F>
where
    E: Send + Sync + 'static,
    E2: Send + Sync + 'static,
    A: Send + Sync + 'static,
    F: Fn(E2) -> Option<E> + Send + Sync + 'static,
{
    async fn sync_state(&mut self) -> Result<()> {
        self.strategy.sync_state().await
    }

    async fn process_event(&mut self, event: E2) -> Result<ActionStream<'_, A>> {
        match (self.f)(event) {
            Some(event) => self.strategy.process_event(event).await,
            None => Ok(Box::pin(futures::stream::empty())),
        }
    }
}
