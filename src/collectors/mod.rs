//! Collectors are responsible for collecting data from external sources and
//! turning them into internal events. For example, a collector might listen to
//! a stream of new blocks, and turn them into a stream of `NewBlock` events.

mod block_collector;
mod channel_collector;
mod event_collector;
mod log_collector;
mod mempool_collector;

/// Crate-private subscribe-or-poll downgrade shared by the collectors above.
mod fallback;

pub use block_collector::*;
pub use channel_collector::*;
pub use event_collector::*;
pub use log_collector::*;
pub use mempool_collector::*;
