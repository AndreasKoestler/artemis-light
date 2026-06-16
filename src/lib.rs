#![warn(unused_crate_dependencies)]
#![deny(unused_must_use, rust_2018_idioms)]
#![doc(test(
    no_crate_inject,
    attr(deny(warnings, rust_2018_idioms), allow(dead_code, unused_variables))
))]

//! A framework for reliable, long-running on-chain automation: event-driven
//! agents that watch a chain, decide, and act. It began as a stripped-down,
//! modernised fork of Paradigm's Artemis MEV framework.
//!
//! At its core, the library is architected as an event processing pipeline,
//! made up of three main components:
//!
//! 1. [Collectors](types::Collector): *Collectors* take in external events (such as pending txs,
//!    new blocks, event logs, etc.) and turn them into an internal
//!    *event* representation.
//!
//! 2. [Strategies](types::Strategy): *Strategies* contain the agent's decision logic. They take
//!    in *events* as inputs, and compute whether any action is warranted (for example, a
//!    strategy might listen to a stream of borrow and supply logs to see if there are any
//!    delinquent borrowers that can be liquidated)
//!
//! 3. [Executors](types::Executor): *Executors* process *actions*, and are responsible for executing
//!    them in different domains (for example, submitting txs, posting off-chain orders, etc.).
//!
//! These components are tied together by the [Engine](engine::Engine), which is responsible for
//! orchestrating the flow of data between them.
//!
//! Around that core, the crate layers what an unattended agent needs to stay
//! correct and alive: per-collector reconnect policies with fatal escalation,
//! [reliability wrappers](executor_ext) for executors (retry, fallback, rate
//! limiting, circuit breaking, kill switch / dry run), passive
//! [Observers](types::Observer), and [persistence] that records,
//! replays, and backfills events so a restarted agent resumes instead of
//! re-syncing.

/// This module contains [collector](types::Collector) implementations.
pub mod collectors;
/// This module contains the [Engine](engine::Engine) struct, which is responsible
/// for orchestrating data flows between components
pub mod engine;
/// This module contains [executor](types::Executor) implementations.
pub mod executors;
/// This module contains the core type definitions for Artemis.
pub mod types;

/// This module contains persistence: a SQL [`Store`](persistence::Store) and a
/// [`Persisted`](persistence::Persisted) collector wrapper that records events
/// and replays them on subscribe.
pub mod persistence;

/// This module contains syntax extensions for the `Collector` trait.
pub mod collector_ext;

/// This module contains syntax extensions for the `Strategy` trait.
pub mod strategy_ext;

/// This module contains syntax extensions for the `Executor` trait.
pub mod executor_ext;

/// Opt-in, read-only HTTP serving layer over the persisted tables. Compiled
/// only when the `serving` feature is enabled; absent otherwise so existing
/// pipelines build unchanged (serving-layer.OPTIN.1/.2).
#[cfg(feature = "serving")]
pub mod serving;

#[cfg(feature = "serving")]
pub use serving::ServingLayer;

// `tempfile` and `tower` are dev-dependencies used only by the serving-layer
// integration tests (`tests/serving.rs`, a separate crate). Reference them here
// in the lib's own test build so `#![warn(unused_crate_dependencies)]` does not
// flag them — the compiler-recommended resolution for test-only dev-deps.
// `testcontainers` / `testcontainers-modules` are the same case for the
// PostgreSQL integration tests (`tests/postgres.rs`); referenced unconditionally
// in the test build so the lint stays quiet whether or not `postgres` is enabled.
#[cfg(test)]
use {tempfile as _, testcontainers as _, testcontainers_modules as _, tower as _};
