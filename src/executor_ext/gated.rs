use crate::types::Executor;
use anyhow::Result;
use async_trait::async_trait;
use std::fmt::Debug;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

/// `Gated` is a wrapper around an [`Executor`] guarded by a kill switch: while
/// the flag is `true` actions execute normally; while it is `false` they are
/// logged and dropped with `Ok(())`. The caller keeps the flag, so flipping it
/// at runtime turns execution into logging — paper-trading mode and emergency
/// stop in one combinator. [`dry_run`](crate::executor_ext::ExecutorExt::dry_run)
/// is a `Gated` whose flag is permanently off.
pub struct Gated<A> {
    executor: Box<dyn Executor<A>>,
    enabled: Arc<AtomicBool>,
}

impl<A> Gated<A> {
    /// Creates a new `Gated` around `executor`, live while `enabled` is
    /// `true`.
    pub fn new(executor: Box<dyn Executor<A>>, enabled: Arc<AtomicBool>) -> Self {
        Self { executor, enabled }
    }
}

#[async_trait]
impl<A> Executor<A> for Gated<A>
where
    A: Debug + Send + Sync + 'static,
{
    async fn execute(&mut self, action: A) -> Result<()> {
        if self.enabled.load(Ordering::SeqCst) {
            self.executor.execute(action).await
        } else {
            tracing::info!(?action, "execution gated off; dropping action");
            Ok(())
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::executor_ext::ExecutorExt;
    use std::sync::{Mutex, atomic::Ordering};

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

    #[tokio::test]
    async fn a_live_gate_passes_actions_through() {
        let (executor, received) = recording();
        let flag = Arc::new(AtomicBool::new(true));
        executor.gated(Arc::clone(&flag)).execute(7).await.unwrap();
        assert_eq!(*received.lock().unwrap(), vec![7]);
    }

    #[tokio::test]
    async fn a_closed_gate_drops_actions_with_ok() {
        let (executor, received) = recording();
        let flag = Arc::new(AtomicBool::new(false));
        executor
            .gated(Arc::clone(&flag))
            .execute(7)
            .await
            .expect("a gated-off action is dropped, not an error");
        assert!(received.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn flipping_the_flag_at_runtime_is_the_kill_switch() {
        let (executor, received) = recording();
        let flag = Arc::new(AtomicBool::new(true));
        let mut gated = executor.gated(Arc::clone(&flag));

        gated.execute(1).await.unwrap();
        flag.store(false, Ordering::SeqCst);
        gated.execute(2).await.unwrap();
        flag.store(true, Ordering::SeqCst);
        gated.execute(3).await.unwrap();

        assert_eq!(
            *received.lock().unwrap(),
            vec![1, 3],
            "only actions while the flag was live reach the inner executor"
        );
    }

    #[tokio::test]
    async fn dry_run_never_reaches_the_inner_executor() {
        let (executor, received) = recording();
        let mut dry = executor.dry_run();
        dry.execute(7).await.unwrap();
        dry.execute(8).await.unwrap();
        assert!(received.lock().unwrap().is_empty());
    }
}
