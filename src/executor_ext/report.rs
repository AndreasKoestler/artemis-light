use crate::types::Executor;
use anyhow::Result;
use async_trait::async_trait;
use std::fmt::Debug;
use tokio::sync::broadcast::Sender;

/// The verdict the executor stack reached for one action, fed back into the
/// pipeline as an event. `result` is `Ok(())` when the stack accepted the
/// action and `Err(message)` when it failed; the error is stringified because
/// [`anyhow::Error`] is not `Clone` and the outcome rides a broadcast channel.
///
/// `Ok(())` means the stack *accepted* the action — submitted, or deliberately
/// dropped by a [`gated`](super::ExecutorExt::gated) / `deadline` layer that
/// returns `Ok`. It is not a claim that the transaction landed on chain.
#[derive(Clone, Debug)]
pub struct ExecutionOutcome<A> {
    pub action: A,
    pub result: Result<(), String>,
}

/// `Report` wraps an [`Executor`] and publishes an [`ExecutionOutcome`] for
/// every action after submitting it, then returns the inner executor's result
/// unchanged. It is transparent — it never alters control flow — so it
/// composes anywhere in the reliability stack; place it outermost to report
/// the stack's final post-retry/post-fallback verdict. Reporting is
/// best-effort: a dropped receiver is logged and ignored, never failing the
/// submission.
pub struct Report<A> {
    executor: Box<dyn Executor<A>>,
    outcomes: Sender<ExecutionOutcome<A>>,
}

impl<A> Report<A> {
    /// Creates a new `Report` that publishes each action's verdict to
    /// `outcomes`.
    pub fn new(executor: Box<dyn Executor<A>>, outcomes: Sender<ExecutionOutcome<A>>) -> Self {
        Self { executor, outcomes }
    }
}

#[async_trait]
impl<A> Executor<A> for Report<A>
where
    A: Clone + Debug + Send + Sync + 'static,
{
    async fn execute(&mut self, action: A) -> Result<()> {
        let result = self.executor.execute(action.clone()).await;
        let outcome = ExecutionOutcome {
            action,
            result: result.as_ref().map(|_| ()).map_err(|e| format!("{e:#}")),
        };
        // Synchronous, non-blocking: a full channel drops the oldest outcome,
        // and no live receiver is a best-effort miss — never fail the
        // submission over reporting.
        if let Err(e) = self.outcomes.send(outcome) {
            tracing::debug!("no execution-outcome receiver; dropping verdict: {e}");
        }
        result
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::executor_ext::{ExecutorExt, RetryPolicy};
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicU32, Ordering},
    };
    use tokio::sync::broadcast;

    /// Records every action it executes; always succeeds.
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

    /// Fails its first `failures` executions, then succeeds.
    struct FlakyExecutor {
        failures: u32,
        attempts: Arc<AtomicU32>,
    }

    fn flaky(failures: u32) -> FlakyExecutor {
        FlakyExecutor {
            failures,
            attempts: Arc::new(AtomicU32::new(0)),
        }
    }

    #[async_trait]
    impl Executor<u32> for FlakyExecutor {
        async fn execute(&mut self, _action: u32) -> Result<()> {
            let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
            if attempt < self.failures {
                anyhow::bail!("transient failure {attempt}")
            }
            Ok(())
        }
    }

    /// Always fails with a fixed message.
    struct FailingExecutor;

    #[async_trait]
    impl Executor<u32> for FailingExecutor {
        async fn execute(&mut self, _action: u32) -> Result<()> {
            anyhow::bail!("submission rejected")
        }
    }

    #[tokio::test]
    async fn a_success_forwards_the_action_and_reports_ok() {
        let (executor, received) = recording();
        let (tx, mut rx) = broadcast::channel(8);
        let mut reporting = executor.report(tx);

        reporting.execute(7).await.unwrap();

        assert_eq!(
            *received.lock().unwrap(),
            vec![7],
            "the action reaches the inner executor"
        );
        let outcome = rx.try_recv().expect("an outcome was published");
        assert_eq!(outcome.action, 7);
        assert!(outcome.result.is_ok());
    }

    #[tokio::test]
    async fn a_failure_returns_the_inner_error_and_reports_it() {
        let (tx, mut rx) = broadcast::channel(8);
        let mut reporting = FailingExecutor.report(tx);

        let err = reporting
            .execute(9)
            .await
            .expect_err("the inner verdict propagates");
        assert_eq!(err.to_string(), "submission rejected");

        let outcome = rx.try_recv().expect("an outcome was published");
        assert_eq!(outcome.action, 9);
        assert_eq!(outcome.result.unwrap_err(), "submission rejected");
    }

    #[tokio::test]
    async fn reporting_is_best_effort_with_no_receiver() {
        let (tx, rx) = broadcast::channel(8);
        drop(rx); // no live receiver
        let (executor, received) = recording();
        let mut reporting = executor.report(tx);

        reporting
            .execute(7)
            .await
            .expect("a missing receiver must not fail the submission");
        assert_eq!(*received.lock().unwrap(), vec![7]);
    }

    #[tokio::test]
    async fn outermost_report_under_retry_reports_one_final_ok() {
        let (tx, mut rx) = broadcast::channel(8);
        // One transient failure, absorbed by retry; report is outermost.
        let mut stack = flaky(1)
            .retry(RetryPolicy {
                max_retries: 3,
                base_delay: std::time::Duration::from_millis(0),
            })
            .report(tx);

        stack.execute(5).await.unwrap();

        let outcome = rx.try_recv().expect("exactly one final outcome");
        assert_eq!(outcome.action, 5);
        assert!(outcome.result.is_ok(), "the final post-retry verdict is Ok");
        assert!(
            rx.try_recv().is_err(),
            "retry's internal failure is not reported"
        );
    }

    #[tokio::test]
    async fn report_and_channel_collector_close_the_loop() {
        use crate::collectors::ChannelCollector;
        use crate::types::Collector;
        use tokio_stream::StreamExt;

        let (tx, _rx) = broadcast::channel(8);

        // Collector side, subscribed before the submission so it sees it.
        let collector = ChannelCollector::new(tx.clone());
        let mut events = collector.subscribe().await.unwrap();

        // Executor side.
        let (executor, _received) = recording();
        let mut reporting = executor.report(tx);
        reporting.execute(123).await.unwrap();

        let outcome = events
            .next()
            .await
            .expect("the verdict re-entered as an event");
        assert_eq!(outcome.action, 123);
        assert!(outcome.result.is_ok());
    }
}
