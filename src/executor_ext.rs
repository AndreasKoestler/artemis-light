use crate::types::Executor;

mod circuit_breaker;
mod deadline;
mod fallback;
mod filter_map_action;
mod gated;
mod rate_limit;
mod report;
mod retry;

#[cfg(test)]
pub(crate) mod test_support;

pub use circuit_breaker::*;
pub use deadline::*;
pub use fallback::*;
pub use filter_map_action::*;
pub use gated::*;
pub use rate_limit::*;
pub use report::*;
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
    /// dropped. A [`NonZeroU32`](std::num::NonZeroU32) rules out a zero cap.
    fn rate_limit(self, per_second: std::num::NonZeroU32) -> RateLimit<A> {
        RateLimit::new(Box::new(self), per_second)
    }

    /// Stop submitting after `max_failures` consecutive failures: an open
    /// circuit fails fast until [`CircuitBreakerHandle::reset`]. Take a
    /// [`handle`](CircuitBreaker::handle) before handing the executor to the
    /// engine. A [`NonZeroU32`](std::num::NonZeroU32) rules out a zero
    /// threshold (a circuit that starts open).
    fn circuit_breaker(self, max_failures: std::num::NonZeroU32) -> CircuitBreaker<A> {
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

    /// Drop actions whose deadline has passed instead of submitting them:
    /// the deadline travels with each action via [`Expires`], stamped by the
    /// strategy that priced it. Place it innermost in the reliability stack —
    /// the check runs at every `execute`, so every queueing or waiting layer
    /// outside has already elapsed, and each [`retry`](ExecutorExt::retry)
    /// attempt re-checks. An expired action is logged and dropped with
    /// `Ok(())`: invisible to `retry` and
    /// [`circuit_breaker`](ExecutorExt::circuit_breaker), because expiry is
    /// normal operation, not a fault.
    fn deadline(self) -> Deadline<A>
    where
        A: Expires,
    {
        Deadline::new(Box::new(self))
    }

    /// Publish each action's verdict to `outcomes` after submitting it, then
    /// return the inner executor's result unchanged. Transparent — it never
    /// alters control flow — so it composes anywhere; place it outermost to
    /// report the stack's final post-retry/post-fallback verdict. Pair it with
    /// a [`ChannelCollector`](crate::collectors::ChannelCollector) over the
    /// same channel to feed verdicts back to strategies as events. Reporting
    /// is best-effort: a dropped receiver is logged and ignored.
    fn report(self, outcomes: tokio::sync::broadcast::Sender<ExecutionOutcome<A>>) -> Report<A>
    where
        A: Clone,
    {
        Report::new(Box::new(self), outcomes)
    }
}

impl<T: Executor<A> + 'static, A> ExecutorExt<A> for T {}

#[cfg(test)]
mod test {
    use super::test_support::{FailingExecutor, FlakyExecutor, RecordingExecutor};
    use super::{ExecutorExt, RetryPolicy};
    use crate::types::Executor;
    use std::sync::Arc;

    /// The umbrella action type a multi-executor engine would broadcast.
    #[derive(Clone, Debug)]
    enum Action {
        Submit(u32),
        /// Payload constructed but never read in tests — exists to prove
        /// unmatched variants are skipped.
        #[allow(dead_code)]
        Log(String),
    }

    #[tokio::test]
    async fn filter_map_action_routes_matching_and_skips_unmatched_actions() {
        let (executor, received) = RecordingExecutor::<u32>::new();
        let mut executor = executor.filter_map_action(|a: Action| match a {
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

    #[tokio::test(start_paused = true)]
    async fn the_reliability_stack_composes_end_to_end() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let executor = FlakyExecutor::<u32>::new(1);
        let attempts = executor.attempts();
        let flag = Arc::new(AtomicBool::new(false));
        let mut stack = executor
            .retry(RetryPolicy::default())
            .circuit_breaker(std::num::NonZeroU32::new(2).unwrap())
            .gated(Arc::clone(&flag));

        // Gated off: the action is dropped before retry or breaker see it.
        stack.execute(1).await.unwrap();
        assert_eq!(attempts.load(Ordering::SeqCst), 0);

        // Gated on: retry absorbs the one transient failure underneath the
        // closed breaker.
        flag.store(true, Ordering::SeqCst);
        stack.execute(2).await.unwrap();
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn filter_map_action_propagates_inner_errors_unchanged() {
        let mut executor =
            FailingExecutor::<u32>::new("execute failed").filter_map_action(|a: Action| match a {
                Action::Submit(n) => Some(n),
                _ => None,
            });
        assert!(executor.execute(Action::Submit(1)).await.is_err());
        // A skipped action still succeeds even on a failing inner executor.
        assert!(executor.execute(Action::Log("skip".into())).await.is_ok());
    }
}
