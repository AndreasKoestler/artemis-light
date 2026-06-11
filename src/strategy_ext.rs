use crate::types::Strategy;

mod filter_map_event;
mod map_action;

pub use filter_map_event::*;
pub use map_action::*;

/// Extension trait that provides adapter combinators for types implementing
/// [`Strategy`].
///
/// The engine broadcasts one event type to every strategy, so multi-source
/// engines use umbrella enums. These adapters mount a strategy written
/// against its own narrow types into such an engine — the consumer-side dual
/// of [`CollectorExt::map`](crate::collector_ext::CollectorExt::map), which
/// widens narrow *sources* into the umbrella type.
pub trait StrategyExt<E, A>: Strategy<E, A> + Send + Sync + Sized + 'static {
    /// Mount this strategy into an engine with a wider event type `E2`: `f`
    /// projects each engine event down to this strategy's event type,
    /// returning `None` for events this strategy doesn't consume. A `None`
    /// event yields an empty action stream — it is not an error.
    fn filter_map_event<F, E2>(self, f: F) -> FilterMapEvent<E, A, F>
    where
        F: Fn(E2) -> Option<E> + Send + Sync + 'static,
    {
        FilterMapEvent::new(Box::new(self), f)
    }

    /// Lift this strategy's actions into a wider action type `A2` — typically
    /// an umbrella-enum constructor: `.map_action(Action::Submit)`.
    fn map_action<F, A2>(self, f: F) -> MapAction<E, A, F>
    where
        F: Fn(A) -> A2 + Send + Sync + 'static,
    {
        MapAction::new(Box::new(self), f)
    }
}

impl<T: Strategy<E, A> + 'static, E, A> StrategyExt<E, A> for T {}

#[cfg(test)]
mod test {
    use super::StrategyExt;
    use crate::types::{ActionStream, Strategy};
    use anyhow::Result;
    use async_trait::async_trait;
    use futures::{StreamExt, stream};
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    /// The umbrella event type a multi-source engine would broadcast.
    #[derive(Clone, Debug)]
    enum Event {
        Num(u32),
        /// Payload constructed but never read in tests — exists to prove
        /// unmatched variants are skipped.
        #[allow(dead_code)]
        Text(String),
    }

    /// A narrow strategy: emits `n * 10` as its single action for event `n`.
    struct TimesTenStrategy;

    #[async_trait]
    impl Strategy<u32, u32> for TimesTenStrategy {
        async fn sync_state(&mut self) -> Result<()> {
            Ok(())
        }

        async fn process_event(&mut self, event: u32) -> Result<ActionStream<'_, u32>> {
            Ok(Box::pin(stream::iter(vec![event * 10])))
        }
    }

    /// Flags when `sync_state` reaches it, for proving delegation through
    /// wrappers.
    struct SyncProbe {
        synced: Arc<AtomicBool>,
    }

    #[async_trait]
    impl Strategy<u32, u32> for SyncProbe {
        async fn sync_state(&mut self) -> Result<()> {
            self.synced.store(true, Ordering::SeqCst);
            Ok(())
        }

        async fn process_event(&mut self, _event: u32) -> Result<ActionStream<'_, u32>> {
            Ok(Box::pin(stream::empty()))
        }
    }

    /// A strategy whose every method fails, for proving errors pass through
    /// wrappers unchanged.
    struct FailingStrategy;

    #[async_trait]
    impl Strategy<u32, u32> for FailingStrategy {
        async fn sync_state(&mut self) -> Result<()> {
            anyhow::bail!("sync failed")
        }

        async fn process_event(&mut self, _event: u32) -> Result<ActionStream<'_, u32>> {
            anyhow::bail!("process failed")
        }
    }

    #[tokio::test]
    async fn filter_map_event_routes_matching_events_to_the_inner_strategy() {
        let mut strategy = TimesTenStrategy.filter_map_event(|e: Event| match e {
            Event::Num(n) => Some(n),
            _ => None,
        });
        let actions = strategy
            .process_event(Event::Num(3))
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        assert_eq!(actions, vec![30]);
    }

    #[tokio::test]
    async fn filter_map_event_yields_an_empty_stream_not_an_error_for_unmatched_events() {
        let mut strategy = TimesTenStrategy.filter_map_event(|e: Event| match e {
            Event::Num(n) => Some(n),
            _ => None,
        });
        let actions = strategy
            .process_event(Event::Text("not for me".into()))
            .await
            .expect("an unmatched event is normal broadcast traffic, not an error")
            .collect::<Vec<_>>()
            .await;
        assert!(actions.is_empty());
    }

    #[tokio::test]
    async fn filter_map_event_delegates_sync_state_to_the_inner_strategy() {
        let synced = Arc::new(AtomicBool::new(false));
        let mut strategy = SyncProbe {
            synced: Arc::clone(&synced),
        }
        .filter_map_event(|e: Event| match e {
            Event::Num(n) => Some(n),
            _ => None,
        });
        strategy.sync_state().await.unwrap();
        assert!(synced.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn filter_map_event_propagates_inner_errors_unchanged() {
        let mut strategy = FailingStrategy.filter_map_event(|e: Event| match e {
            Event::Num(n) => Some(n),
            _ => None,
        });
        assert!(strategy.sync_state().await.is_err());
        assert!(strategy.process_event(Event::Num(1)).await.is_err());
    }

    /// The umbrella action type a multi-strategy engine would broadcast.
    #[derive(Clone, Debug, PartialEq)]
    enum Action {
        Submit(u32),
    }

    /// A narrow strategy emitting two actions per event: `n` and `n + 1`.
    struct PairStrategy;

    #[async_trait]
    impl Strategy<u32, u32> for PairStrategy {
        async fn sync_state(&mut self) -> Result<()> {
            Ok(())
        }

        async fn process_event(&mut self, event: u32) -> Result<ActionStream<'_, u32>> {
            Ok(Box::pin(stream::iter(vec![event, event + 1])))
        }
    }

    #[tokio::test]
    async fn map_action_transforms_every_action_preserving_order() {
        let mut strategy = PairStrategy.map_action(Action::Submit);
        let actions = strategy
            .process_event(7)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        assert_eq!(actions, vec![Action::Submit(7), Action::Submit(8)]);
    }

    #[tokio::test]
    async fn map_action_delegates_sync_state_and_propagates_errors() {
        let synced = Arc::new(AtomicBool::new(false));
        let mut probe = SyncProbe {
            synced: Arc::clone(&synced),
        }
        .map_action(Action::Submit);
        probe.sync_state().await.unwrap();
        assert!(synced.load(Ordering::SeqCst));

        let mut failing = FailingStrategy.map_action(Action::Submit);
        assert!(failing.sync_state().await.is_err());
        assert!(failing.process_event(1).await.is_err());
    }

    #[tokio::test]
    async fn filter_map_event_and_map_action_compose_end_to_end() {
        let mut strategy = PairStrategy
            .filter_map_event(|e: Event| match e {
                Event::Num(n) => Some(n),
                _ => None,
            })
            .map_action(Action::Submit);

        let actions = strategy
            .process_event(Event::Num(1))
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        assert_eq!(actions, vec![Action::Submit(1), Action::Submit(2)]);

        let skipped = strategy
            .process_event(Event::Text("not for me".into()))
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        assert!(skipped.is_empty());
    }
}
