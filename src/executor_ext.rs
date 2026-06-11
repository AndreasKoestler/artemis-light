use crate::types::Executor;

mod circuit_breaker;
mod fallback;
mod filter_map_action;
mod gated;
mod rate_limit;
mod retry;

pub use circuit_breaker::*;
pub use fallback::*;
pub use filter_map_action::*;
pub use gated::*;
pub use rate_limit::*;
pub use retry::*;

/// Extension trait that provides adapter combinators for types implementing
/// [`Executor`].
///
/// The engine broadcasts every action to every executor, so an executor
/// written against its own narrow action type uses
/// [`filter_map_action`](ExecutorExt::filter_map_action) to route only the
/// actions it handles — the action-channel counterpart of
/// [`StrategyExt::filter_map_event`](crate::strategy_ext::StrategyExt::filter_map_event).
pub trait ExecutorExt<A>: Executor<A> + Send + Sync + Sized + 'static {
    /// Route only matching actions to this executor: `f` projects each engine
    /// action down to this executor's action type. A `None` action is skipped
    /// with `Ok(())` — the inner executor never sees it.
    fn filter_map_action<F, A2>(self, f: F) -> FilterMapAction<A, F>
    where
        F: Fn(A2) -> Option<A> + Send + Sync + 'static,
    {
        FilterMapAction::new(Box::new(self), f)
    }

    /// Re-submit failed actions with exponential backoff, per `policy`.
    /// Transient RPC failures are the common case for a submission sink, so
    /// this is the innermost reliability layer most executors want.
    fn retry(self, policy: RetryPolicy) -> Retry<A> {
        Retry::new(Box::new(self), policy)
    }

    /// Try this executor first; on error, re-submit the action to `other` —
    /// primary RPC → backup RPC, or private relay → public mempool. The
    /// primary's error is logged; only the fallback's verdict is returned.
    fn fallback<E2>(self, other: E2) -> Fallback<A>
    where
        E2: Executor<A> + 'static,
    {
        Fallback::new(Box::new(self), Box::new(other))
    }

    /// Cap submissions to `per_second` per sliding second, to respect
    /// provider limits. An action over the cap waits rather than being
    /// dropped. Panics if `per_second` is zero.
    fn rate_limit(self, per_second: u32) -> RateLimit<A> {
        RateLimit::new(Box::new(self), per_second)
    }

    /// Stop submitting after `max_failures` consecutive failures: an open
    /// circuit fails fast until [`CircuitBreakerHandle::reset`]. Take a
    /// [`handle`](CircuitBreaker::handle) before handing the executor to the
    /// engine. Panics if `max_failures` is zero.
    fn circuit_breaker(self, max_failures: u32) -> CircuitBreaker<A> {
        CircuitBreaker::new(Box::new(self), max_failures)
    }

    /// Guard this executor with a kill switch the caller keeps: while `flag`
    /// is `true` actions execute normally; while it is `false` they are
    /// logged and dropped with `Ok(())`.
    fn gated(self, flag: std::sync::Arc<std::sync::atomic::AtomicBool>) -> Gated<A> {
        Gated::new(Box::new(self), flag)
    }

    /// Paper-trading mode: every action is logged and dropped; none ever
    /// reach this executor. A [`gated`](ExecutorExt::gated) whose flag is
    /// permanently off.
    fn dry_run(self) -> Gated<A> {
        self.gated(std::sync::Arc::new(std::sync::atomic::AtomicBool::new(
            false,
        )))
    }
}

impl<T: Executor<A> + 'static, A> ExecutorExt<A> for T {}

#[cfg(test)]
mod test {
    use super::{ExecutorExt, RetryPolicy};
    use crate::types::Executor;
    use anyhow::Result;
    use async_trait::async_trait;
    use std::sync::{Arc, Mutex};

    /// The umbrella action type a multi-executor engine would broadcast.
    #[derive(Clone, Debug)]
    enum Action {
        Submit(u32),
        /// Payload constructed but never read in tests — exists to prove
        /// unmatched variants are skipped.
        #[allow(dead_code)]
        Log(String),
    }

    /// Records every action it executes, for proving routing and skipping.
    struct RecordingExecutor {
        received: Arc<Mutex<Vec<u32>>>,
    }

    #[async_trait]
    impl Executor<u32> for RecordingExecutor {
        async fn execute(&mut self, action: u32) -> Result<()> {
            self.received.lock().unwrap().push(action);
            Ok(())
        }
    }

    /// An executor whose execute always fails, for proving error passthrough.
    struct FailingExecutor;

    #[async_trait]
    impl Executor<u32> for FailingExecutor {
        async fn execute(&mut self, _action: u32) -> Result<()> {
            anyhow::bail!("execute failed")
        }
    }

    #[tokio::test]
    async fn filter_map_action_routes_matching_and_skips_unmatched_actions() {
        let received = Arc::new(Mutex::new(Vec::new()));
        let mut executor = RecordingExecutor {
            received: Arc::clone(&received),
        }
        .filter_map_action(|a: Action| match a {
            Action::Submit(n) => Some(n),
            _ => None,
        });

        executor.execute(Action::Submit(7)).await.unwrap();
        executor
            .execute(Action::Log("not for me".into()))
            .await
            .expect("a skipped action is Ok, not an error");

        assert_eq!(
            *received.lock().unwrap(),
            vec![7],
            "the skipped action must never reach the inner executor"
        );
    }

    /// Fails its first `failures` executions, then succeeds, counting every
    /// attempt.
    struct FlakyExecutor {
        failures: u32,
        attempts: Arc<Mutex<u32>>,
    }

    #[async_trait]
    impl Executor<u32> for FlakyExecutor {
        async fn execute(&mut self, _action: u32) -> Result<()> {
            let mut attempts = self.attempts.lock().unwrap();
            *attempts += 1;
            if *attempts <= self.failures {
                anyhow::bail!("transient failure")
            }
            Ok(())
        }
    }

    #[tokio::test(start_paused = true)]
    async fn the_reliability_stack_composes_end_to_end() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let attempts = Arc::new(Mutex::new(0));
        let flag = Arc::new(AtomicBool::new(false));
        let mut stack = FlakyExecutor {
            failures: 1,
            attempts: Arc::clone(&attempts),
        }
        .retry(RetryPolicy::default())
        .circuit_breaker(2)
        .gated(Arc::clone(&flag));

        // Gated off: the action is dropped before retry or breaker see it.
        stack.execute(1).await.unwrap();
        assert_eq!(*attempts.lock().unwrap(), 0);

        // Gated on: retry absorbs the one transient failure underneath the
        // closed breaker.
        flag.store(true, Ordering::SeqCst);
        stack.execute(2).await.unwrap();
        assert_eq!(*attempts.lock().unwrap(), 2);
    }

    #[tokio::test]
    async fn filter_map_action_propagates_inner_errors_unchanged() {
        let mut executor = FailingExecutor.filter_map_action(|a: Action| match a {
            Action::Submit(n) => Some(n),
            _ => None,
        });
        assert!(executor.execute(Action::Submit(1)).await.is_err());
        // A skipped action still succeeds even on a failing inner executor.
        assert!(executor.execute(Action::Log("skip".into())).await.is_ok());
    }
}
