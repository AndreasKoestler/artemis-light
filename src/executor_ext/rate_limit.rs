use crate::types::Executor;
use anyhow::Result;
use async_trait::async_trait;
use std::collections::VecDeque;
use std::time::Duration;
use tokio::time::Instant;

/// `RateLimit` is a wrapper around an [`Executor`] that caps submissions to
/// `per_second` per sliding one-second window, to respect provider limits.
/// An action over the cap is not dropped — `execute` waits until the oldest
/// submission leaves the window, applying backpressure to the action channel.
///
/// Every attempt counts against the window, including failed ones: a failed
/// submission still spent provider quota.
pub struct RateLimit<A> {
    executor: Box<dyn Executor<A>>,
    per_second: u32,
    /// Submission instants within the last second, oldest first.
    sent: VecDeque<Instant>,
}

impl<A> RateLimit<A> {
    /// Creates a new `RateLimit` capping `executor` at `per_second`
    /// submissions per sliding second.
    ///
    /// # Panics
    ///
    /// Panics if `per_second` is zero — a cap of zero could never submit.
    pub fn new(executor: Box<dyn Executor<A>>, per_second: u32) -> Self {
        assert!(
            per_second > 0,
            "rate limit must allow at least one action per second"
        );
        Self {
            executor,
            per_second,
            sent: VecDeque::new(),
        }
    }
}

#[async_trait]
impl<A> Executor<A> for RateLimit<A>
where
    A: Send + Sync + 'static,
{
    async fn execute(&mut self, action: A) -> Result<()> {
        const WINDOW: Duration = Duration::from_secs(1);
        // Wait until the oldest submission leaves the window, then drop every
        // expired instant. The cap holds at any moment: submissions only
        // leave the window by aging out.
        if self.sent.len() >= self.per_second as usize {
            let oldest = self.sent[self.sent.len() - self.per_second as usize];
            tokio::time::sleep_until(oldest + WINDOW).await;
        }
        let now = Instant::now();
        while self.sent.front().is_some_and(|&t| t + WINDOW <= now) {
            self.sent.pop_front();
        }
        self.sent.push_back(now);
        self.executor.execute(action).await
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

    #[tokio::test(start_paused = true)]
    async fn actions_under_the_cap_pass_without_waiting() {
        let (executor, received) = recording();
        let mut limited = executor.rate_limit(3);
        let start = Instant::now();
        for n in 0..3 {
            limited.execute(n).await.unwrap();
        }
        assert_eq!(start.elapsed(), Duration::ZERO);
        assert_eq!(*received.lock().unwrap(), vec![0, 1, 2]);
    }

    #[tokio::test(start_paused = true)]
    async fn the_action_over_the_cap_waits_out_the_window() {
        let (executor, received) = recording();
        let mut limited = executor.rate_limit(2);
        let start = Instant::now();
        for n in 0..3 {
            limited.execute(n).await.unwrap();
        }
        // The third action waits until the first leaves the 1s window. It is
        // delayed, never dropped.
        assert_eq!(start.elapsed(), Duration::from_secs(1));
        assert_eq!(*received.lock().unwrap(), vec![0, 1, 2]);
    }

    #[tokio::test(start_paused = true)]
    async fn the_window_slides_rather_than_resetting() {
        let (executor, _) = recording();
        let mut limited = executor.rate_limit(1);
        let start = Instant::now();
        for n in 0..3 {
            limited.execute(n).await.unwrap();
        }
        // One per second: t=0, t=1, t=2 — each waits on the previous, not on
        // a fixed window boundary.
        assert_eq!(start.elapsed(), Duration::from_secs(2));
    }

    /// An executor whose execute always fails.
    struct FailingExecutor;

    #[async_trait]
    impl Executor<u32> for FailingExecutor {
        async fn execute(&mut self, _action: u32) -> Result<()> {
            anyhow::bail!("execute failed")
        }
    }

    #[tokio::test(start_paused = true)]
    async fn failed_attempts_propagate_and_still_count_against_the_window() {
        let mut limited = FailingExecutor.rate_limit(1);
        let start = Instant::now();
        assert!(limited.execute(0).await.is_err());
        assert!(limited.execute(1).await.is_err());
        // The second attempt waited on the first even though it failed: a
        // failed submission still spent provider quota.
        assert_eq!(start.elapsed(), Duration::from_secs(1));
    }

    #[test]
    #[should_panic(expected = "at least one action per second")]
    fn a_zero_cap_is_rejected_at_construction() {
        let (executor, _) = recording();
        let _ = executor.rate_limit(0);
    }
}
