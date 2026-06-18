use crate::backoff::Backoff;
use crate::types::Executor;
use anyhow::Result;
use async_trait::async_trait;
use std::time::Duration;

/// Configuration for [`Retry`]: how many times to re-submit and how fast the
/// backoff grows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Number of *re*-submissions after the initial attempt. `0` means the
    /// wrapper is a no-op passthrough.
    pub max_retries: u32,
    /// Base unit for the exponential backoff. The delay before the Nth retry
    /// is `base_delay * 2^(N-1)`: `base_delay`, then doubled each retry.
    pub base_delay: Duration,
}

impl Default for RetryPolicy {
    /// Three retries at 100ms, 200ms, 400ms — sized for transient RPC
    /// failures, the common case for a submission sink.
    fn default() -> Self {
        Self {
            max_retries: 3,
            base_delay: Duration::from_millis(100),
        }
    }
}

/// `Retry` is a wrapper around an [`Executor`] that re-submits failed actions
/// with exponential backoff, per its [`RetryPolicy`]. Once the retries are
/// exhausted the *last* error is returned.
pub struct Retry<A> {
    executor: Box<dyn Executor<A>>,
    policy: RetryPolicy,
}

impl<A> Retry<A> {
    /// Creates a new `Retry` wrapping `executor` with the given `policy`.
    pub fn new(executor: Box<dyn Executor<A>>, policy: RetryPolicy) -> Self {
        Self { executor, policy }
    }
}

#[async_trait]
impl<A> Executor<A> for Retry<A>
where
    A: Clone + Send + Sync + 'static,
{
    async fn execute(&mut self, action: A) -> Result<()> {
        let mut retries = 0;
        loop {
            let Err(error) = self.executor.execute(action.clone()).await else {
                return Ok(());
            };
            if retries >= self.policy.max_retries {
                return Err(error);
            }
            // The shared exponential backoff curve. `retries` is 0-based here,
            // so the first retry waits exactly one `base_delay`.
            let delay = Backoff::new(self.policy.base_delay).delay(retries);
            tracing::warn!(?delay, retries, "execute failed; retrying: {error:#}");
            tokio::time::sleep(delay).await;
            retries += 1;
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::executor_ext::ExecutorExt;
    use crate::executor_ext::test_support::FlakyExecutor;
    use std::sync::atomic::Ordering;
    use tokio::time::Instant;

    fn policy(max_retries: u32) -> RetryPolicy {
        RetryPolicy {
            max_retries,
            base_delay: Duration::from_secs(1),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn first_attempt_success_needs_no_retry_and_no_delay() {
        let executor = FlakyExecutor::<u32>::new(0);
        let attempts = executor.attempts();
        let start = Instant::now();
        executor.retry(policy(3)).execute(7).await.unwrap();
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        assert_eq!(start.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn retries_transient_failures_until_success() {
        let executor = FlakyExecutor::<u32>::new(2);
        let attempts = executor.attempts();
        executor.retry(policy(3)).execute(7).await.unwrap();
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn backoff_doubles_between_retries() {
        let executor = FlakyExecutor::<u32>::new(2);
        let start = Instant::now();
        executor.retry(policy(3)).execute(7).await.unwrap();
        // Two retries: 1s before the first, 2s before the second.
        assert_eq!(start.elapsed(), Duration::from_secs(3));
    }

    #[tokio::test(start_paused = true)]
    async fn exhausted_retries_return_the_last_error() {
        let executor = FlakyExecutor::<u32>::new(u32::MAX);
        let attempts = executor.attempts();
        let err = executor
            .retry(policy(2))
            .execute(7)
            .await
            .expect_err("all attempts fail");
        // Initial attempt plus two retries.
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
        assert_eq!(err.to_string(), "transient failure 2");
    }

    #[tokio::test(start_paused = true)]
    async fn zero_retries_is_a_passthrough() {
        let executor = FlakyExecutor::<u32>::new(u32::MAX);
        let attempts = executor.attempts();
        assert!(executor.retry(policy(0)).execute(7).await.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }
}
