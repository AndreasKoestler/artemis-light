//! Executors are responsible for taking actions produced by strategies and
//! executing them in different domains. For example, an executor might take a
//! `SubmitTx` action and submit it to the mempool.

/// This executor submits transactions to the public mempool.
mod mempool_executor;

/// EIP-1559 fee pricing and replacement-fee escalation — the pure fee
/// arithmetic the mempool executor and its replacement loop share.
mod pricing;

pub use mempool_executor::*;
pub use pricing::*;
