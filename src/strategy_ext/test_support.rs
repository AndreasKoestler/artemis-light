//! Canonical test adapters at the [`Strategy`] seam, shared by the strategy
//! wrapper tests.
//!
//! The doubles a wrapper test needs at the `Strategy` seam — one that fails
//! every method, one that flags when `sync_state` reaches it — live here once
//! rather than being re-declared (and quietly drifting) in every wrapper
//! module. The counterpart of
//! [`executor_ext::test_support`](crate::executor_ext) on the strategy side.

use crate::types::{ActionStream, Strategy};
use anyhow::Result;
use async_trait::async_trait;
use futures::stream;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// A strategy whose every method fails, for proving errors pass through
/// wrappers unchanged.
pub(crate) struct FailingStrategy;

#[async_trait]
impl Strategy<u32, u32> for FailingStrategy {
    async fn sync_state(&mut self) -> Result<()> {
        anyhow::bail!("sync failed")
    }

    async fn process_event(&mut self, _event: u32) -> Result<ActionStream<'_, u32>> {
        anyhow::bail!("process failed")
    }
}

/// Flags when `sync_state` reaches it, for proving wrappers delegate it.
pub(crate) struct SyncProbe {
    synced: Arc<AtomicBool>,
}

impl SyncProbe {
    /// The probe and a shared flag set the moment `sync_state` reaches it.
    pub(crate) fn new() -> (Self, Arc<AtomicBool>) {
        let synced = Arc::new(AtomicBool::new(false));
        (
            Self {
                synced: Arc::clone(&synced),
            },
            synced,
        )
    }
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
