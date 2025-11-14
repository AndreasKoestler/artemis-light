mod instrument;
pub use instrument::*;

use crate::types::{Metrics, Strategy};

pub trait StrategyExt<E, A>: Strategy<E, A> + Send + Sync + Sized {
    fn instrument<M>(self, metrics: M) -> StrategyInstrument<Self, M>
    where
        M: Metrics<A> + Send + Sync + 'static,
    {
        StrategyInstrument::new(self, metrics)
    }
}

impl<E, A, T: Strategy<E, A> + 'static> StrategyExt<E, A> for T {}

#[cfg(test)]
mod test {
    use async_trait::async_trait;
    use futures::{StreamExt, stream};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use super::*;
    use crate::types::ActionStream;
    use crate::types::Strategy;

    struct TestMetrics {
        call_count: Arc<AtomicUsize>,
    }

    impl TestMetrics {
        pub fn new(call_count: Arc<AtomicUsize>) -> Self {
            Self { call_count }
        }
    }

    struct TestStrategy {
        state: usize,
    }

    impl TestStrategy {
        pub fn new() -> Self {
            Self { state: 0 }
        }
    }

    #[async_trait]
    impl Strategy<usize, usize> for TestStrategy {
        async fn sync_state(&mut self) -> anyhow::Result<()> {
            Ok(())
        }

        async fn process_event(&mut self, event: usize) -> anyhow::Result<ActionStream<'_, usize>> {
            self.state += event;
            Ok(Box::pin(stream::iter(vec![event])))
        }
    }

    impl Metrics<usize> for TestMetrics {
        fn collect_metrics(
            &self,
            _action: &usize,
        ) -> impl std::future::Future<Output = anyhow::Result<()>> + Send {
            self.call_count.fetch_add(1, Ordering::Relaxed);
            async { Ok(()) }
        }
    }

    #[tokio::test]
    async fn test_instrument() {
        let call_count = Arc::new(AtomicUsize::new(0));
        let test_metrics = TestMetrics::new(Arc::clone(&call_count));
        let test_strategy = TestStrategy::new();
        let mut instrumented = test_strategy.instrument(test_metrics);

        // Test process_event
        for i in 0..5 {
            let _ = instrumented
                .process_event(i)
                .await
                .unwrap()
                .collect::<Vec<_>>()
                .await;
        }

        // Test sync_state
        instrumented.sync_state().await.unwrap();

        // We called process_event 5 times + sync_state 1 time = 6 total calls
        assert_eq!(call_count.load(Ordering::Relaxed), 5);
    }
}
