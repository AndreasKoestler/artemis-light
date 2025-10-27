//! Collectors are responsible for collecting data from external sources and
//! turning them into internal events. For example, a collector might listen to
//! a stream of new blocks, and turn them into a stream of `NewBlock` events.

/// This collector listens to a stream of new blocks.
mod block_collector;

/// This collector listens to a stream of new event logs.
mod log_collector;

/// This collector listens to a stream of new pending transactions.
mod mempool_collector;

/// This collector listens to a stream of new events.
mod event_collector;

pub use block_collector::*;
pub use event_collector::*;
pub use log_collector::*;
pub use mempool_collector::*;
