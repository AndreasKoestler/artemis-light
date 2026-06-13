use crate::types::Executor;
use anyhow::Result;
use async_trait::async_trait;
use std::fmt::Debug;
use tokio::time::Instant;

/// An action that knows when it goes stale. The strategy that priced the
/// opportunity stamps the deadline at creation; [`Deadline`] enforces it at
/// submission time. The clock is [`tokio::time::Instant`], so the paused-time
/// test harness controls "now" completely.
pub trait Expires {
    /// The instant after which this action must not be submitted.
    fn expires_at(&self) -> Instant;
}

/// `Deadline` is a wrapper around an [`Executor`] that drops expired actions
/// instead of submitting them. The check runs at every `execute` call, so
/// every delay layer *outside* the wrapper — channel backlog, a rate
/// limiter's wait, each retry backoff — has already elapsed by the time it
/// runs; place it innermost. An expired action is logged and dropped with
/// `Ok(())`, the same drop shape as [`Gated`](super::Gated): expiry is normal
/// operation, not a fault, so it neither trips a
/// [`CircuitBreaker`](super::CircuitBreaker) nor keeps a
/// [`Retry`](super::Retry) loop alive.
pub struct Deadline<A> {
    executor: Box<dyn Executor<A>>,
}

impl<A> Deadline<A> {
    /// Creates a new `Deadline` around `executor`.
    pub fn new(executor: Box<dyn Executor<A>>) -> Self {
        Self { executor }
    }
}

#[async_trait]
impl<A> Executor<A> for Deadline<A>
where
    A: Expires + Debug + Send + Sync + 'static,
{
    async fn execute(&mut self, action: A) -> Result<()> {
        if Instant::now() >= action.expires_at() {
            tracing::warn!(?action, "action expired; dropping without submission");
            return Ok(());
        }
        self.executor.execute(action).await
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::executor_ext::ExecutorExt;
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicU32, Ordering},
    };
    use std::time::Duration;

    /// An action stamped with the freshness window it was priced against.
    #[derive(Clone, Debug)]
    struct TimedAction {
        id: u32,
        expires_at: Instant,
    }

    impl Expires for TimedAction {
        fn expires_at(&self) -> Instant {
            self.expires_at
        }
    }

    /// Live for a minute — far beyond anything a test advances past
    /// unintentionally.
    fn live(id: u32) -> TimedAction {
        TimedAction {
            id,
            expires_at: Instant::now() + Duration::from_secs(60),
        }
    }

    /// Expired on arrival: the check is `now >= expires_at`.
    fn expired(id: u32) -> TimedAction {
        TimedAction {
            id,
            expires_at: Instant::now(),
        }
    }

    /// Records the id of every action it executes.
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
    impl Executor<TimedAction> for RecordingExecutor {
        async fn execute(&mut self, action: TimedAction) -> Result<()> {
            self.received.lock().unwrap().push(action.id);
            Ok(())
        }
    }

    /// Fails every execution, counting attempts.
    struct FailingExecutor {
        attempts: Arc<AtomicU32>,
    }

    fn failing() -> (FailingExecutor, Arc<AtomicU32>) {
        let attempts = Arc::new(AtomicU32::new(0));
        (
            FailingExecutor {
                attempts: Arc::clone(&attempts),
            },
            attempts,
        )
    }

    #[async_trait]
    impl Executor<TimedAction> for FailingExecutor {
        async fn execute(&mut self, _action: TimedAction) -> Result<()> {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            anyhow::bail!("submission failed")
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_live_action_passes_through() {
        let (executor, received) = recording();
        executor.deadline().execute(live(7)).await.unwrap();
        assert_eq!(*received.lock().unwrap(), vec![7]);
    }

    #[tokio::test(start_paused = true)]
    async fn an_expired_action_is_dropped_with_ok() {
        let (executor, received) = recording();
        executor
            .deadline()
            .execute(expired(7))
            .await
            .expect("an expired action is dropped, not an error");
        assert!(received.lock().unwrap().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn an_action_expires_when_time_advances_past_its_deadline() {
        let (executor, received) = recording();
        let mut deadline = executor.deadline();
        let action = TimedAction {
            id: 7,
            expires_at: Instant::now() + Duration::from_secs(1),
        };

        deadline.execute(action.clone()).await.unwrap();
        tokio::time::advance(Duration::from_secs(2)).await;
        deadline.execute(action).await.unwrap();

        assert_eq!(
            *received.lock().unwrap(),
            vec![7],
            "only the pre-expiry submission reaches the inner executor"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn inner_errors_propagate_for_live_actions() {
        let (executor, attempts) = failing();
        let err = executor
            .deadline()
            .execute(live(7))
            .await
            .expect_err("a live action's failure is the inner executor's verdict");
        assert_eq!(err.to_string(), "submission failed");
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn an_action_expiring_mid_backoff_stops_the_retry_loop() {
        use crate::executor_ext::RetryPolicy;

        let (executor, attempts) = failing();
        let mut stack = executor.deadline().retry(RetryPolicy {
            max_retries: 3,
            base_delay: Duration::from_secs(1),
        });
        let action = TimedAction {
            id: 7,
            expires_at: Instant::now() + Duration::from_millis(1500),
        };

        stack
            .execute(action)
            .await
            .expect("expiry mid-backoff resolves Ok, not exhausted-retries Err");

        // Attempts at t=0 and t=1s are live and fail; the t=3s attempt finds
        // the action expired, returns Ok, and the retry loop stops — the
        // inner executor never sees a third attempt.
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn an_expired_drop_does_not_count_against_the_circuit_breaker() {
        let (executor, attempts) = failing();
        let breaker = executor.deadline().circuit_breaker(1);
        let operator = breaker.handle();
        let mut breaker = breaker;

        breaker.execute(expired(1)).await.unwrap();
        breaker.execute(expired(2)).await.unwrap();

        assert!(
            !operator.is_open(),
            "expired drops are Ok and must not trip the breaker"
        );
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            0,
            "expired actions never reach the failing inner executor"
        );
    }
}
