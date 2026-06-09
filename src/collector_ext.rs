use crate::types::Collector;

mod chain;
mod filter_map;
mod map;
mod merge;

pub use chain::*;
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

    /// Interleave this collector with `other`: events arrive in whichever
    /// order the sources produce them. See [`Merge`] for the full contract.
    fn merge<C>(self, other: C) -> Merge<Self, C>
    where
        C: Collector<E> + Send + Sync + 'static,
    {
        Merge::new(self, other)
    }

    /// Deliver all of this collector's events, then all of `other`'s. Both
    /// subscribe eagerly. See [`Chain`] for the full contract.
    fn chain<C>(self, other: C) -> Chain<Self, C>
    where
        C: Collector<E> + Send + Sync + 'static,
    {
        Chain::new(self, other)
    }
}

impl<T: Collector<E> + 'static, E> CollectorExt<E> for T {}

#[cfg(test)]
mod test {
    use super::{CollectorExt, chain_all, merge_all};
    use crate::types::{Collector, CollectorStream};
    use anyhow::Result;
    use async_trait::async_trait;
    use futures::stream;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::Duration;
    use tokio_stream::StreamExt;
    use tokio_stream::{self};

    pub struct TestCollector<T> {
        data: Vec<T>,
    }

    impl<T> TestCollector<T> {
        pub fn new(data: Vec<T>) -> Self {
            Self { data }
        }
    }

    #[async_trait]
    impl<T: Clone + Send + Sync + 'static> Collector<T> for TestCollector<T> {
        async fn subscribe(&self) -> Result<CollectorStream<'_, T>> {
            Ok(Box::pin(stream::iter(self.data.clone())))
        }
    }

    /// Emits its events, then stays pending forever — a live source that never
    /// ends, for proving delivery happens before any stream end.
    pub struct PendingCollector<T> {
        data: Vec<T>,
    }

    impl<T> PendingCollector<T> {
        pub fn new(data: Vec<T>) -> Self {
            Self { data }
        }
    }

    #[async_trait]
    impl<T: Clone + Send + Sync + 'static> Collector<T> for PendingCollector<T> {
        async fn subscribe(&self) -> Result<CollectorStream<'_, T>> {
            Ok(Box::pin(
                stream::iter(self.data.clone()).chain(stream::pending()),
            ))
        }
    }

    /// A collector whose subscribe always fails.
    pub struct FailingCollector;

    #[async_trait]
    impl Collector<i32> for FailingCollector {
        async fn subscribe(&self) -> Result<CollectorStream<'_, i32>> {
            anyhow::bail!("subscribe failed")
        }
    }

    /// Counts subscribe calls and yields an empty stream, for observing when a
    /// combinator subscribes its sources.
    pub struct CountingCollector {
        subscribes: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Collector<i32> for CountingCollector {
        async fn subscribe(&self) -> Result<CollectorStream<'_, i32>> {
            self.subscribes.fetch_add(1, Ordering::SeqCst);
            Ok(Box::pin(stream::iter(Vec::new())))
        }
    }

    /// An event type with no serde impls: combinators must not demand
    /// (de)serializability that no behaviour needs.
    #[derive(Clone, Debug, PartialEq)]
    struct Opaque(u8);

    #[tokio::test]
    async fn chain_delivers_sources_strictly_in_sequence() {
        let chained = TestCollector::new(vec![1, 3, 5]).chain(TestCollector::new(vec![2, 4, 6]));
        let stream = chained.subscribe().await.unwrap();
        let res = stream.collect::<Vec<_>>().await;
        assert_eq!(res, vec![1, 3, 5, 2, 4, 6]);
    }

    #[tokio::test]
    async fn chain_subscribes_to_later_sources_eagerly() {
        let subscribes = Arc::new(AtomicUsize::new(0));
        let chained = TestCollector::new(vec![1]).chain(CountingCollector {
            subscribes: Arc::clone(&subscribes),
        });
        let _stream = chained.subscribe().await.unwrap();
        // The second source must be live before the first segment is consumed,
        // so it buffers at its source instead of missing events.
        assert_eq!(subscribes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn chain_fails_subscribe_when_any_source_fails() {
        let chained = TestCollector::new(vec![1]).chain(FailingCollector);
        assert!(chained.subscribe().await.is_err());
    }

    #[tokio::test]
    async fn chain_all_delivers_in_registration_order() {
        let sources: Vec<Box<dyn Collector<i32>>> = vec![
            Box::new(TestCollector::new(vec![1, 2])),
            Box::new(TestCollector::new(vec![3])),
            Box::new(TestCollector::new(vec![4, 5])),
        ];
        let chained = chain_all(sources);
        let stream = chained.subscribe().await.unwrap();
        assert_eq!(stream.collect::<Vec<_>>().await, vec![1, 2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn chain_all_fails_subscribe_when_any_source_fails() {
        let sources: Vec<Box<dyn Collector<i32>>> = vec![
            Box::new(TestCollector::new(vec![1])),
            Box::new(FailingCollector),
        ];
        assert!(chain_all(sources).subscribe().await.is_err());
    }

    #[tokio::test]
    async fn merge_does_not_require_deserializable_events() {
        let merged = TestCollector::new(vec![Opaque(1)]).merge(TestCollector::new(vec![Opaque(2)]));
        let stream = merged.subscribe().await.unwrap();
        let mut res = stream.collect::<Vec<_>>().await;
        res.sort_by_key(|o| o.0);
        assert_eq!(res, vec![Opaque(1), Opaque(2)]);
    }

    #[tokio::test]
    async fn merge_delivers_from_both_sources_before_either_ends() {
        let merged = PendingCollector::new(vec![1]).merge(PendingCollector::new(vec![2]));
        let stream = merged.subscribe().await.unwrap();
        let mut res = tokio::time::timeout(
            Duration::from_secs(1),
            stream.take(2).collect::<Vec<_>>(),
        )
        .await
        .expect("merge starved a source: both events must arrive while neither stream has ended");
        res.sort();
        assert_eq!(res, vec![1, 2]);
    }

    #[tokio::test]
    async fn merge_all_delivers_from_every_source_before_any_ends() {
        let sources: Vec<Box<dyn Collector<i32>>> = vec![
            Box::new(PendingCollector::new(vec![1])),
            Box::new(PendingCollector::new(vec![2])),
            Box::new(PendingCollector::new(vec![3])),
        ];
        let merged = merge_all(sources);
        let stream = merged.subscribe().await.unwrap();
        let mut res =
            tokio::time::timeout(Duration::from_secs(1), stream.take(3).collect::<Vec<_>>())
                .await
                .expect(
                    "merge_all starved a source: all events must arrive while no stream has ended",
                );
        res.sort();
        assert_eq!(res, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn merge_all_fails_subscribe_when_any_source_fails() {
        let sources: Vec<Box<dyn Collector<i32>>> = vec![
            Box::new(TestCollector::new(vec![1])),
            Box::new(FailingCollector),
        ];
        assert!(merge_all(sources).subscribe().await.is_err());
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
