use crate::types::{ActionStream, Strategy};
use anyhow::Result;
use async_trait::async_trait;
use futures::StreamExt;
use futures::future::ready;
use std::sync::Mutex;
use std::time::Duration;
use tokio::time::Instant;

/// `Cooldown` is a wrapper around a [`Strategy`] that suppresses its actions
/// for `duration` after it fires — a strategy that just submitted should not
/// immediately submit again on the next event.
///
/// The wrapper decides per *event*, when the engine calls `process_event`: a
/// cooling strategy still sees every event (so its internal state stays
/// current) but the actions it produces are dropped. The cooldown starts when
/// an action actually passes through — and is refreshed by each action of a
/// batch, so a multi-action batch passes whole and the period runs from its
/// last action.
pub struct Cooldown<E, A> {
    strategy: Box<dyn Strategy<E, A>>,
    duration: Duration,
    /// When the strategy last fired. A `Mutex` rather than a `Cell` because
    /// the action stream that stamps it must stay `Send`.
    last_fired: Mutex<Option<Instant>>,
}

impl<E, A> Cooldown<E, A> {
    /// Creates a new `Cooldown` suppressing `strategy`'s actions for
    /// `duration` after each fire.
    pub fn new(strategy: Box<dyn Strategy<E, A>>, duration: Duration) -> Self {
        Self {
            strategy,
            duration,
            last_fired: Mutex::new(None),
        }
    }
}

#[async_trait]
impl<E, A> Strategy<E, A> for Cooldown<E, A>
where
    E: Send + Sync + 'static,
    A: Send + Sync + 'static,
{
    async fn sync_state(&mut self) -> Result<()> {
        self.strategy.sync_state().await
    }

    async fn process_event(&mut self, event: E) -> Result<ActionStream<'_, A>> {
        let cooling = self
            .last_fired
            .lock()
            .unwrap()
            .is_some_and(|fired| fired.elapsed() < self.duration);
        // The inner strategy processes the event either way, so its state
        // stays current through the cooldown.
        let stream = self.strategy.process_event(event).await?;
        if cooling {
            tracing::debug!("strategy cooling down; suppressing its actions");
            return Ok(Box::pin(stream.filter(|_| ready(false))));
        }
        // `&mut self.strategy` and `&self.last_fired` are disjoint field
        // borrows, so the returned stream can hold both for the same
        // lifetime. Suppressed actions never reach this stamp, so they do
        // not refresh the cooldown.
        let last_fired = &self.last_fired;
        Ok(Box::pin(stream.inspect(move |_| {
            *last_fired.lock().unwrap() = Some(Instant::now());
        })))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::strategy_ext::StrategyExt;
    use futures::{StreamExt, stream};
    use std::sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    };

    /// Emits its event as its single action, counting every event it sees.
    struct EchoStrategy {
        events_seen: Arc<AtomicU32>,
    }

    fn echo() -> (EchoStrategy, Arc<AtomicU32>) {
        let events_seen = Arc::new(AtomicU32::new(0));
        (
            EchoStrategy {
                events_seen: Arc::clone(&events_seen),
            },
            events_seen,
        )
    }

    #[async_trait]
    impl Strategy<u32, u32> for EchoStrategy {
        async fn sync_state(&mut self) -> Result<()> {
            Ok(())
        }

        async fn process_event(&mut self, event: u32) -> Result<ActionStream<'_, u32>> {
            self.events_seen.fetch_add(1, Ordering::SeqCst);
            Ok(Box::pin(stream::iter(vec![event])))
        }
    }

    async fn actions_for(strategy: &mut impl Strategy<u32, u32>, event: u32) -> Vec<u32> {
        strategy
            .process_event(event)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await
    }

    #[tokio::test(start_paused = true)]
    async fn firing_starts_a_cooldown_that_suppresses_the_next_actions() {
        let (strategy, _) = echo();
        let mut cooled = strategy.cooldown(Duration::from_secs(60));
        assert_eq!(actions_for(&mut cooled, 1).await, vec![1]);
        assert!(
            actions_for(&mut cooled, 2).await.is_empty(),
            "the strategy just fired; its actions are suppressed"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn actions_resume_once_the_cooldown_elapses() {
        let (strategy, _) = echo();
        let mut cooled = strategy.cooldown(Duration::from_secs(60));
        assert_eq!(actions_for(&mut cooled, 1).await, vec![1]);
        tokio::time::advance(Duration::from_secs(61)).await;
        assert_eq!(actions_for(&mut cooled, 2).await, vec![2]);
    }

    #[tokio::test(start_paused = true)]
    async fn suppressed_actions_do_not_refresh_the_cooldown() {
        let (strategy, _) = echo();
        let mut cooled = strategy.cooldown(Duration::from_secs(60));
        assert_eq!(actions_for(&mut cooled, 1).await, vec![1]);
        // Half-way through the period the strategy tries to fire again; the
        // suppressed attempt must not restart the clock.
        tokio::time::advance(Duration::from_secs(31)).await;
        assert!(actions_for(&mut cooled, 2).await.is_empty());
        tokio::time::advance(Duration::from_secs(31)).await;
        assert_eq!(actions_for(&mut cooled, 3).await, vec![3]);
    }

    #[tokio::test(start_paused = true)]
    async fn the_inner_strategy_still_sees_events_while_cooling() {
        let (strategy, events_seen) = echo();
        let mut cooled = strategy.cooldown(Duration::from_secs(60));
        actions_for(&mut cooled, 1).await;
        actions_for(&mut cooled, 2).await;
        assert_eq!(
            events_seen.load(Ordering::SeqCst),
            2,
            "cooldown suppresses actions, not the events that keep state current"
        );
    }

    /// Emits no actions for even events, a pair of actions for odd ones.
    struct OddPairStrategy;

    #[async_trait]
    impl Strategy<u32, u32> for OddPairStrategy {
        async fn sync_state(&mut self) -> Result<()> {
            Ok(())
        }

        async fn process_event(&mut self, event: u32) -> Result<ActionStream<'_, u32>> {
            if event.is_multiple_of(2) {
                Ok(Box::pin(stream::empty()))
            } else {
                Ok(Box::pin(stream::iter(vec![event, event + 1])))
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn an_actionless_event_does_not_start_a_cooldown() {
        let mut cooled = OddPairStrategy.cooldown(Duration::from_secs(60));
        assert!(actions_for(&mut cooled, 2).await.is_empty());
        assert_eq!(
            actions_for(&mut cooled, 1).await,
            vec![1, 2],
            "only firing starts the cooldown, not merely processing an event"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_multi_action_batch_passes_whole_before_the_cooldown_engages() {
        let mut cooled = OddPairStrategy.cooldown(Duration::from_secs(60));
        assert_eq!(actions_for(&mut cooled, 1).await, vec![1, 2]);
        assert!(actions_for(&mut cooled, 3).await.is_empty());
    }

    /// A strategy whose every method fails.
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

    #[tokio::test(start_paused = true)]
    async fn inner_errors_propagate_unchanged() {
        let mut cooled = FailingStrategy.cooldown(Duration::from_secs(60));
        assert!(cooled.sync_state().await.is_err());
        assert!(cooled.process_event(1).await.is_err());
    }
}
