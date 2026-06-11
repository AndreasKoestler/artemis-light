use crate::types::Executor;
use anyhow::Result;
use async_trait::async_trait;

/// `Fallback` is a wrapper around a primary [`Executor`] that, when the
/// primary fails, re-submits the action to a secondary executor: primary RPC
/// → backup RPC, or private relay → public mempool. The primary's error is
/// logged; only the fallback's verdict is returned.
pub struct Fallback<A> {
    primary: Box<dyn Executor<A>>,
    fallback: Box<dyn Executor<A>>,
}

impl<A> Fallback<A> {
    /// Creates a new `Fallback` trying `primary` first and `fallback` on
    /// error.
    pub fn new(primary: Box<dyn Executor<A>>, fallback: Box<dyn Executor<A>>) -> Self {
        Self { primary, fallback }
    }
}

#[async_trait]
impl<A> Executor<A> for Fallback<A>
where
    A: Clone + Send + Sync + 'static,
{
    async fn execute(&mut self, action: A) -> Result<()> {
        let Err(error) = self.primary.execute(action.clone()).await else {
            return Ok(());
        };
        tracing::warn!("primary executor failed; trying fallback: {error:#}");
        self.fallback.execute(action).await
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::executor_ext::ExecutorExt;
    use std::sync::{Arc, Mutex};

    /// Records every action it executes.
    struct RecordingExecutor {
        received: Arc<Mutex<Vec<u32>>>,
    }

    fn recording() -> (RecordingExecutor, Arc<Mutex<Vec<u32>>>) {
        let received = Arc::new(Mutex::new(Vec::new()));
        (
            RecordingExecutor {
                received: Arc::clone(&received),
            },
            received,
        )
    }

    #[async_trait]
    impl Executor<u32> for RecordingExecutor {
        async fn execute(&mut self, action: u32) -> Result<()> {
            self.received.lock().unwrap().push(action);
            Ok(())
        }
    }

    /// An executor whose execute always fails.
    struct FailingExecutor(&'static str);

    #[async_trait]
    impl Executor<u32> for FailingExecutor {
        async fn execute(&mut self, _action: u32) -> Result<()> {
            anyhow::bail!(self.0)
        }
    }

    #[tokio::test]
    async fn primary_success_never_reaches_the_fallback() {
        let (primary, primary_received) = recording();
        let (fallback, fallback_received) = recording();
        primary.fallback(fallback).execute(7).await.unwrap();
        assert_eq!(*primary_received.lock().unwrap(), vec![7]);
        assert!(fallback_received.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn primary_failure_routes_the_action_to_the_fallback() {
        let (fallback, fallback_received) = recording();
        FailingExecutor("primary down")
            .fallback(fallback)
            .execute(7)
            .await
            .unwrap();
        assert_eq!(*fallback_received.lock().unwrap(), vec![7]);
    }

    #[tokio::test]
    async fn both_failing_returns_the_fallback_error() {
        let err = FailingExecutor("primary down")
            .fallback(FailingExecutor("fallback down"))
            .execute(7)
            .await
            .expect_err("both executors fail");
        assert_eq!(err.to_string(), "fallback down");
    }
}
