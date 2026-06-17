use crate::types::Executor;
use anyhow::Result;
use async_trait::async_trait;
use std::num::NonZeroU32;
use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};

/// Shared between the [`CircuitBreaker`] and its handles: the consecutive
/// failure counter and the threshold that opens the circuit.
#[derive(Debug)]
struct BreakerState {
    max_failures: u32,
    /// Consecutive failures since the last successful execution.
    failures: AtomicU32,
}

impl BreakerState {
    fn is_open(&self) -> bool {
        self.failures.load(Ordering::SeqCst) >= self.max_failures
    }
}

/// `CircuitBreaker` is a wrapper around an [`Executor`] that stops submitting
/// after `max_failures` consecutive failures: an open circuit fails fast
/// without reaching the inner executor, until a success path is restored by
/// an explicit [`CircuitBreakerHandle::reset`]. For a bot that signs
/// transactions, failing closed is a safety feature, not just resilience.
pub struct CircuitBreaker<A> {
    executor: Box<dyn Executor<A>>,
    state: Arc<BreakerState>,
}

impl<A> CircuitBreaker<A> {
    /// Creates a new `CircuitBreaker` that opens after `max_failures`
    /// consecutive failures of `executor`. A [`NonZeroU32`] makes a
    /// zero threshold (a circuit that starts open) unrepresentable.
    pub fn new(executor: Box<dyn Executor<A>>, max_failures: NonZeroU32) -> Self {
        Self {
            executor,
            state: Arc::new(BreakerState {
                max_failures: max_failures.get(),
                failures: AtomicU32::new(0),
            }),
        }
    }

    /// Returns a handle for observing and resetting the breaker. Take one
    /// before handing the executor to the engine — the engine consumes the
    /// executor, the handle is what the operator keeps.
    pub fn handle(&self) -> CircuitBreakerHandle {
        CircuitBreakerHandle {
            state: Arc::clone(&self.state),
        }
    }
}

/// The operator's view of a [`CircuitBreaker`]: observe whether the circuit
/// is open, and reset it once the underlying fault is fixed.
#[derive(Debug, Clone)]
pub struct CircuitBreakerHandle {
    state: Arc<BreakerState>,
}

impl CircuitBreakerHandle {
    /// Whether the circuit is open (actions are being rejected).
    pub fn is_open(&self) -> bool {
        self.state.is_open()
    }

    /// Closes the circuit by clearing the failure count. Execution resumes
    /// with the next action.
    pub fn reset(&self) {
        self.state.failures.store(0, Ordering::SeqCst);
    }
}

#[async_trait]
impl<A> Executor<A> for CircuitBreaker<A>
where
    A: Send + Sync + 'static,
{
    async fn execute(&mut self, action: A) -> Result<()> {
        if self.state.is_open() {
            anyhow::bail!(
                "circuit breaker open after {} consecutive failures; action rejected until reset",
                self.state.max_failures
            );
        }
        match self.executor.execute(action).await {
            Ok(()) => {
                self.state.failures.store(0, Ordering::SeqCst);
                Ok(())
            }
            Err(error) => {
                let failures = self.state.failures.fetch_add(1, Ordering::SeqCst) + 1;
                if failures >= self.state.max_failures {
                    tracing::error!(failures, "circuit breaker opened: {error:#}");
                }
                Err(error)
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::executor_ext::ExecutorExt;

    fn nz(n: u32) -> NonZeroU32 {
        NonZeroU32::new(n).unwrap()
    }

    /// Fails or succeeds per a script of outcomes, counting every attempt.
    struct ScriptedExecutor {
        outcomes: Vec<bool>,
        attempts: Arc<AtomicU32>,
    }

    fn scripted(outcomes: &[bool]) -> (ScriptedExecutor, Arc<AtomicU32>) {
        let attempts = Arc::new(AtomicU32::new(0));
        (
            ScriptedExecutor {
                outcomes: outcomes.to_vec(),
                attempts: Arc::clone(&attempts),
            },
            attempts,
        )
    }

    #[async_trait]
    impl Executor<u32> for ScriptedExecutor {
        async fn execute(&mut self, _action: u32) -> Result<()> {
            let attempt = self.attempts.fetch_add(1, Ordering::SeqCst) as usize;
            if self.outcomes.get(attempt).copied().unwrap_or(false) {
                Ok(())
            } else {
                anyhow::bail!("submission failed")
            }
        }
    }

    #[tokio::test]
    async fn an_open_circuit_fails_fast_without_reaching_the_inner_executor() {
        let (executor, attempts) = scripted(&[]);
        let mut breaker = executor.circuit_breaker(nz(2));
        assert!(breaker.execute(0).await.is_err());
        assert!(breaker.execute(1).await.is_err());
        // The circuit is now open: rejected before the inner executor.
        let err = breaker.execute(2).await.expect_err("circuit is open");
        assert!(err.to_string().contains("circuit breaker open"));
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn a_success_resets_the_consecutive_failure_count() {
        let (executor, attempts) = scripted(&[false, true, false, false]);
        let mut breaker = executor.circuit_breaker(nz(2));
        assert!(breaker.execute(0).await.is_err());
        breaker.execute(1).await.unwrap();
        assert!(breaker.execute(2).await.is_err());
        // Still closed: the success at action 1 reset the count.
        assert!(breaker.execute(3).await.is_err());
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            4,
            "every action up to the second consecutive failure reaches the inner executor"
        );
    }

    #[tokio::test]
    async fn the_handle_observes_and_resets_the_circuit() {
        let (executor, attempts) = scripted(&[false, true]);
        let mut breaker = executor.circuit_breaker(nz(1));
        let handle = breaker.handle();

        assert!(!handle.is_open());
        assert!(breaker.execute(0).await.is_err());
        assert!(handle.is_open());

        handle.reset();
        assert!(!handle.is_open());
        breaker
            .execute(1)
            .await
            .expect("a reset circuit submits again");
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    // A zero threshold is unrepresentable: `circuit_breaker` takes a
    // `NonZeroU32`, so a circuit that would start open cannot be constructed.
}
