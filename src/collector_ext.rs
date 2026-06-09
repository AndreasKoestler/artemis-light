use crate::types::Collector;

mod filter_map;
mod map;
mod merge;

pub use filter_map::*;
pub use map::*;
pub use merge::*;

/// Extension trait that provides additional functionality for types implementing [`Collector`].
///
/// This trait adds methods for transforming and combining collector streams:
pub trait CollectorExt<E>: Collector<E> + Send + Sync + Sized + 'static {
    /// Map events from type `E` to type `E2` using a function `f`.
    fn map<F, E2>(self, f: F) -> Map<E, F>
    where
        F: Fn(E) -> E2 + Send + Sync + Clone + 'static,
    {
        Map::new(Box::new(self), f)
    }

    /// Filter and transform events from type `E` to type `E2` using a function `f`.
    fn filter_map<F, E2>(self, f: F) -> FilterMap<E, F>
    where
        F: Fn(E) -> Option<E2> + Send + Sync + Clone + 'static,
    {
        FilterMap::new(Box::new(self), f)
    }

    /// Merge two collectors into a single collector that emits events from both.
    fn merge<C>(self, other: C) -> Merge<Self, C>
    where
        C: Collector<E> + Send + Sync + 'static,
    {
        Merge::new(self, other)
    }
}

impl<T: Collector<E> + 'static, E> CollectorExt<E> for T {}

#[cfg(test)]
mod test {
    use super::CollectorExt;
    use crate::types::{Collector, CollectorStream};
    use anyhow::Result;
    use async_trait::async_trait;
    use futures::stream;
    use tokio_stream::StreamExt;
    use tokio_stream::{self};

    pub struct TestCollector {
        data: Vec<u8>,
    }

    impl TestCollector {
        pub fn new(data: Vec<u8>) -> Self {
            Self { data }
        }
    }

    /// Implementation of the [Collector](Collector) trait for the [BlockCollector](BlockCollector).
    #[async_trait]
    impl Collector<u8> for TestCollector {
        async fn subscribe(&self) -> Result<CollectorStream<'_, u8>> {
            Ok(Box::pin(stream::iter(self.data.clone())))
        }
    }

    #[tokio::test]
    async fn test_collector_map() {
        let collector = TestCollector {
            data: vec![1, 2, 3],
        };
        let collector = collector.map(|n| n + 1);
        let stream = collector.subscribe().await.unwrap();
        let event = stream.collect::<Vec<_>>().await;
        assert_eq!(event, vec![2, 3, 4]);
    }

    #[tokio::test]
    async fn test_filter_collector_map() {
        let collector = TestCollector {
            data: vec![1, 2, 3, 4],
        };
        let collector = collector.filter_map(|n| if n % 2 == 0 { Some(n) } else { None });
        let stream = collector.subscribe().await.unwrap();
        let event = stream.collect::<Vec<_>>().await;
        assert_eq!(event, vec![2, 4]);
    }

    #[tokio::test]
    async fn test_vec_merge_collector() {
        let block_collector = TestCollector::new(vec![1, 3, 5]);
        let block_collector_2 = TestCollector::new(vec![2, 4, 6]);
        let block_collector_3 = TestCollector::new(vec![7]);

        let collectors = vec![
            Box::new(block_collector),
            Box::new(block_collector_2),
            Box::new(block_collector_3),
        ];

        let mut res = collectors
            .subscribe()
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        res.sort();
        assert_eq!(res, vec![1, 2, 3, 4, 5, 6, 7])
    }

    #[tokio::test]
    async fn test_merge_collector() {
        let block_collector = TestCollector::new(vec![1, 3, 5]);
        let block_collector_2 = TestCollector::new(vec![2, 4, 6]);
        let merged = block_collector.merge(block_collector_2);
        let stream = merged.subscribe().await.unwrap();
        let mut res = stream.collect::<Vec<_>>().await;
        res.sort();
        assert_eq!(res, vec![1, 2, 3, 4, 5, 6]);
    }

    #[tokio::test]
    async fn test_collector_map_filter_merge() {
        let collector = TestCollector::new(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let collector = collector
            .map(|n| n + 1)
            .filter_map(|n| if n % 2 == 0 { Some(n) } else { None })
            .merge(TestCollector::new(vec![11, 12, 13, 14, 15]));
        let stream = collector.subscribe().await.unwrap();
        let mut res = stream.collect::<Vec<_>>().await;
        res.sort();
        assert_eq!(res, vec![2, 4, 6, 8, 10, 11, 12, 13, 14, 15]);
    }
}
