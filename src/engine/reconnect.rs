//! The reconnect policy for a [collector](crate::types::Collector).
//!
//! When a collector's event stream is lost (the subscription ends) or can never
//! be established (stream creation fails), the engine consults a per-collector
//! [`ReconnectPolicy`] to decide whether to [`Retry`](Decision::Retry) after a
//! backoff or to declare the collector [`Fatal`](Decision::Fatal).
//!
//! The policy is a pure state machine: it owns the consecutive-failure counter
//! and the backoff curve, performs no I/O, and keeps no clock. The driver
//! supplies the actual sleeping and cancellation. This is what makes the
//! escalation behaviour testable without a process actually dying.

use std::time::Duration;

use crate::backoff::Backoff;

/// Configuration for a [`ReconnectPolicy`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReconnectConfig {
    /// Number of consecutive failures (stream creation *or* stream end) that
    /// escalates a collector to [`Fatal`](Decision::Fatal).
    pub max_failures: u32,
    /// Base unit for the exponential backoff. The delay before the Nth retry is
    /// `base_delay * 2^N`.
    pub base_delay: Duration,
}

impl Default for ReconnectConfig {
    /// The historical defaults: escalate on the 3rd consecutive failure, with a
    /// backoff of 2s, 4s before that.
    fn default() -> Self {
        Self {
            max_failures: 3,
            base_delay: Duration::from_secs(1),
        }
    }
}

/// What the engine should do after a collector's stream is lost or fails to open.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    /// Recreate the stream after waiting `after`.
    Retry { after: Duration },
    /// The collector is unrecoverable. The engine tears down all tasks and
    /// surfaces a fatal cause so the binary can restart with a fresh sync.
    Fatal,
}

/// Per-collector reconnect state machine. See the [module docs](self).
#[derive(Debug)]
pub struct ReconnectPolicy {
    config: ReconnectConfig,
    /// Consecutive failures since the last delivered event.
    failures: u32,
}

impl ReconnectPolicy {
    /// Creates a policy with the given configuration.
    pub fn new(config: ReconnectConfig) -> Self {
        Self {
            config,
            failures: 0,
        }
    }

    /// Records that the collector delivered a real event. This is the *only*
    /// thing that resets the failure counter: a stream that connects but never
    /// delivers must still march toward [`Fatal`](Decision::Fatal), so an
    /// orchestrator restarts the process rather than leaving a silent zombie.
    pub fn on_events_received(&mut self) {
        self.failures = 0;
    }

    /// Records that stream creation failed, and returns the engine's next move.
    pub fn on_creation_failed(&mut self) -> Decision {
        self.record_failure()
    }

    /// Records that the stream ended (e.g. the WebSocket dropped), and returns
    /// the engine's next move.
    pub fn on_stream_ended(&mut self) -> Decision {
        self.record_failure()
    }

    /// Stream creation and stream end feed one counter and share one escalation:
    /// a collector that can never open its stream is just as much a zombie as
    /// one whose stream keeps dropping.
    fn record_failure(&mut self) -> Decision {
        self.failures += 1;
        if self.failures >= self.config.max_failures {
            Decision::Fatal
        } else {
            Decision::Retry {
                after: self.backoff(),
            }
        }
    }

    /// The shared exponential backoff curve at the current failure count.
    /// `failures` is 1-based here (incremented before this is read), so the
    /// first retry already waits `2 * base_delay`.
    fn backoff(&self) -> Duration {
        Backoff::new(self.config.base_delay).delay(self.failures)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn policy(max_failures: u32) -> ReconnectPolicy {
        ReconnectPolicy::new(ReconnectConfig {
            max_failures,
            base_delay: Duration::from_secs(1),
        })
    }

    #[test]
    fn backoff_doubles_until_threshold() {
        let mut p = policy(4);
        assert_eq!(
            p.on_stream_ended(),
            Decision::Retry {
                after: Duration::from_secs(2)
            }
        );
        assert_eq!(
            p.on_stream_ended(),
            Decision::Retry {
                after: Duration::from_secs(4)
            }
        );
        assert_eq!(
            p.on_stream_ended(),
            Decision::Retry {
                after: Duration::from_secs(8)
            }
        );
        assert_eq!(p.on_stream_ended(), Decision::Fatal);
    }

    #[test]
    fn creation_failure_escalates_to_fatal() {
        let mut p = policy(2);
        assert!(matches!(p.on_creation_failed(), Decision::Retry { .. }));
        assert_eq!(p.on_creation_failed(), Decision::Fatal);
    }

    #[test]
    fn stream_end_escalates_to_fatal() {
        let mut p = policy(2);
        assert!(matches!(p.on_stream_ended(), Decision::Retry { .. }));
        assert_eq!(p.on_stream_ended(), Decision::Fatal);
    }

    #[test]
    fn creation_and_end_share_one_counter() {
        let mut p = policy(3);
        assert!(matches!(p.on_creation_failed(), Decision::Retry { .. }));
        assert!(matches!(p.on_stream_ended(), Decision::Retry { .. }));
        // Third consecutive failure, regardless of kind, is fatal.
        assert_eq!(p.on_creation_failed(), Decision::Fatal);
    }

    #[test]
    fn delivered_events_reset_the_counter() {
        let mut p = policy(3);
        assert!(matches!(p.on_stream_ended(), Decision::Retry { .. }));
        assert!(matches!(p.on_stream_ended(), Decision::Retry { .. }));
        p.on_events_received();
        // Counter is back to zero; we get two retries again before fatal.
        assert!(matches!(p.on_stream_ended(), Decision::Retry { .. }));
        assert!(matches!(p.on_stream_ended(), Decision::Retry { .. }));
        assert_eq!(p.on_stream_ended(), Decision::Fatal);
    }

    #[test]
    fn flapping_without_events_still_reaches_fatal() {
        // Connect, deliver nothing, disconnect — repeatedly. Without a real
        // event the counter never resets, so it must escalate.
        let mut p = policy(3);
        assert!(matches!(p.on_stream_ended(), Decision::Retry { .. }));
        assert!(matches!(p.on_stream_ended(), Decision::Retry { .. }));
        assert_eq!(p.on_stream_ended(), Decision::Fatal);
    }
}
