mod instrument;
pub use instrument::*;

use crate::types::{Executor, Metrics};

pub trait ExecutorExt<A>: Executor<A> + Send + Sync + Sized {
    fn instrument<M>(self, metrics: M) -> ExecutorInstrument<Self, M>
    where
        M: Metrics<Self> + Send + Sync + 'static,
    {
        ExecutorInstrument::new(self, metrics)
    }
}

impl<T: Executor<A> + 'static, A> ExecutorExt<A> for T {}

#[cfg(test)]
mod test {
    use async_trait::async_trait;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use super::*;

    struct TestMetricsCollector {
        state: Arc<AtomicUsize>,
    }

    impl TestMetricsCollector {
        pub fn new(state: Arc<AtomicUsize>) -> Self {
            Self { state }
        }
    }

    impl Metrics<TestExecutor> for TestMetricsCollector {
        fn collect_metrics(
            &self,
            _executor: &TestExecutor,
        ) -> impl std::future::Future<Output = anyhow::Result<()>> + Send {
            self.state.fetch_add(1, Ordering::Relaxed);
            async { Ok(()) }
        }
    }

    struct TestExecutor {}

    impl TestExecutor {
        pub fn new() -> Self {
            Self {}
        }
    }

    #[async_trait]
    impl Executor<usize> for TestExecutor {
        async fn execute(&mut self, _action: usize) -> anyhow::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_instrument() {
        let state = Arc::new(AtomicUsize::new(0));
        let test_collector = TestMetricsCollector::new(Arc::clone(&state));
        let test_executor = TestExecutor::new();
        let mut instrumented = test_executor.instrument(test_collector);

        for a in 0..10 {
            instrumented.execute(a).await.unwrap();
        }
        assert_eq!(state.load(Ordering::Relaxed), 10);
    }
}
