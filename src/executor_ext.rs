use crate::types::Executor;

mod filter_map_action;

pub use filter_map_action::*;

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
}

impl<T: Executor<A> + 'static, A> ExecutorExt<A> for T {}

#[cfg(test)]
mod test {
    use super::ExecutorExt;
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
