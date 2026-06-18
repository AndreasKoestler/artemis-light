//! Canonical test adapters at the [`Executor`] seam, shared by the reliability
//! wrapper tests.
//!
//! The interface a wrapper test exercises *is* the `Executor` trait, so the
//! doubles that sit at that seam — recording, always-failing, flaky — live
//! here once rather than being re-minted (and quietly drifting) in every
//! wrapper module. Mirrors the `Store`-side doubles in
//! [`persistence::writer::test_support`](crate::persistence).

use crate::types::Executor;
use anyhow::Result;
use async_trait::async_trait;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

/// Records, in order, every action it executes; always succeeds.
pub(crate) struct RecordingExecutor<A> {
    received: Arc<Mutex<Vec<A>>>,
}

impl<A> RecordingExecutor<A> {
    /// The executor and a shared handle to the actions it will record. Clone
    /// the handle out before moving the executor into a wrapper stack.
    pub(crate) fn new() -> (Self, Arc<Mutex<Vec<A>>>) {
        let received = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                received: Arc::clone(&received),
            },
            received,
        )
    }
}

#[async_trait]
impl<A: Send + Sync + 'static> Executor<A> for RecordingExecutor<A> {
    async fn execute(&mut self, action: A) -> Result<()> {
        self.received.lock().unwrap().push(action);
        Ok(())
    }
}

/// An executor whose every execution fails with a fixed message, counting the
/// attempts it received.
pub(crate) struct FailingExecutor<A> {
    message: &'static str,
    attempts: Arc<AtomicU32>,
    _action: PhantomData<fn(A)>,
}

impl<A> FailingExecutor<A> {
    pub(crate) fn new(message: &'static str) -> Self {
        Self {
            message,
            attempts: Arc::new(AtomicU32::new(0)),
            _action: PhantomData,
        }
    }

    /// A shared handle to the running attempt count; clone it out before moving
    /// the executor into a wrapper stack.
    pub(crate) fn attempts(&self) -> Arc<AtomicU32> {
        Arc::clone(&self.attempts)
    }
}

#[async_trait]
impl<A: Send + 'static> Executor<A> for FailingExecutor<A> {
    async fn execute(&mut self, _action: A) -> Result<()> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        anyhow::bail!(self.message)
    }
}

/// Fails its first `failures` executions, then succeeds; counts every attempt.
pub(crate) struct FlakyExecutor<A> {
    failures: u32,
    attempts: Arc<AtomicU32>,
    _action: PhantomData<fn(A)>,
}

impl<A> FlakyExecutor<A> {
    pub(crate) fn new(failures: u32) -> Self {
        Self {
            failures,
            attempts: Arc::new(AtomicU32::new(0)),
            _action: PhantomData,
        }
    }

    /// A shared handle to the running attempt count; clone it out before moving
    /// the executor into a wrapper stack.
    pub(crate) fn attempts(&self) -> Arc<AtomicU32> {
        Arc::clone(&self.attempts)
    }
}

#[async_trait]
impl<A: Send + 'static> Executor<A> for FlakyExecutor<A> {
    async fn execute(&mut self, _action: A) -> Result<()> {
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
        if attempt < self.failures {
            anyhow::bail!("transient failure {attempt}")
        }
        Ok(())
    }
}
