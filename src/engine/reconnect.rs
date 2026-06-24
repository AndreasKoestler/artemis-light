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
    /// How long a stream must have stayed open before it ended for the end to
    /// count as a *healthy* drop rather than a failure. A stream that lived at
    /// least this long is treated as a connection the provider merely recycled
    /// (load-balanced RPC WebSocket endpoints routinely close idle/long-lived
    /// subscriptions after a TTL), so its end resets the consecutive-failure
    /// counter — exactly like a delivered event — instead of advancing it toward
    /// [`Fatal`](Decision::Fatal). A stream that ends sooner is a flap or a
    /// never-opening zombie and still counts. The driver measures the uptime and
    /// hands it to [`on_stream_ended`](ReconnectPolicy::on_stream_ended); the
    /// policy keeps no clock.
    pub healthy_uptime: Duration,
}

impl Default for ReconnectConfig {
    /// The historical defaults: escalate on the 3rd consecutive failure, with a
    /// backoff of 2s, 4s before that. A stream open ≥30s before ending is a
    /// healthy provider-side recycle, not a failure.
    fn default() -> Self {
        Self {
            max_failures: 3,
            base_delay: Duration::from_secs(1),
            healthy_uptime: Duration::from_secs(30),
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

    /// Records that the stream ended (e.g. the WebSocket dropped) after being
    /// open for `uptime`, and returns the engine's next move.
    ///
    /// A stream that stayed open at least [`healthy_uptime`] is a connection the
    /// provider merely recycled, not a fault: its end resets the failure counter
    /// (like a delivered event) and retries on a fresh backoff, so a quiet but
    /// healthy subscription on an endpoint that periodically drops connections
    /// reconnects indefinitely instead of marching to [`Fatal`](Decision::Fatal).
    /// A stream that ends before then is a flap or a never-opening zombie and
    /// advances the counter exactly as a failure — so a genuinely broken source
    /// still escalates.
    ///
    /// [`healthy_uptime`]: ReconnectConfig::healthy_uptime
    pub fn on_stream_ended(&mut self, uptime: Duration) -> Decision {
        if uptime >= self.config.healthy_uptime {
            self.failures = 0;
            Decision::Retry {
                after: self.backoff(),
            }
        } else {
            self.record_failure()
        }
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
            healthy_uptime: Duration::from_secs(30),
        })
    }

    /// A stream end below `healthy_uptime` — i.e. a flap that counts as a failure.
    const FLAP: Duration = Duration::ZERO;

    #[test]
    fn backoff_doubles_until_threshold() {
        let mut p = policy(4);
        assert_eq!(
            p.on_stream_ended(FLAP),
            Decision::Retry {
                after: Duration::from_secs(2)
            }
        );
        assert_eq!(
            p.on_stream_ended(FLAP),
            Decision::Retry {
                after: Duration::from_secs(4)
            }
        );
        assert_eq!(
            p.on_stream_ended(FLAP),
            Decision::Retry {
                after: Duration::from_secs(8)
            }
        );
        assert_eq!(p.on_stream_ended(FLAP), Decision::Fatal);
    }

    #[test]
    fn stream_open_past_healthy_uptime_resets_counter() {
        let mut p = policy(3);
        // Two quick flaps bring it one short of Fatal.
        assert!(matches!(p.on_stream_ended(FLAP), Decision::Retry { .. }));
        assert!(matches!(p.on_stream_ended(FLAP), Decision::Retry { .. }));
        // A stream that lived past the healthy-uptime threshold is a provider
        // recycle, not a fault: it clears the counter and retries on base backoff.
        assert_eq!(
            p.on_stream_ended(Duration::from_secs(45)),
            Decision::Retry {
                after: Duration::from_secs(1)
            }
        );
        // Counter is back to zero, so it takes the full budget again before Fatal.
        assert!(matches!(p.on_stream_ended(FLAP), Decision::Retry { .. }));
        assert!(matches!(p.on_stream_ended(FLAP), Decision::Retry { .. }));
        assert_eq!(p.on_stream_ended(FLAP), Decision::Fatal);
    }

    #[test]
    fn healthy_drops_alone_never_escalate() {
        // A quiet-but-healthy subscription the provider keeps recycling: every
        // end is past `healthy_uptime`, so it must reconnect forever, never Fatal.
        let mut p = policy(2);
        for _ in 0..50 {
            assert!(matches!(
                p.on_stream_ended(Duration::from_secs(120)),
                Decision::Retry { .. }
            ));
        }
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
        assert!(matches!(p.on_stream_ended(FLAP), Decision::Retry { .. }));
        assert_eq!(p.on_stream_ended(FLAP), Decision::Fatal);
    }

    #[test]
    fn creation_and_end_share_one_counter() {
        let mut p = policy(3);
        assert!(matches!(p.on_creation_failed(), Decision::Retry { .. }));
        assert!(matches!(p.on_stream_ended(FLAP), Decision::Retry { .. }));
        // Third consecutive failure, regardless of kind, is fatal.
        assert_eq!(p.on_creation_failed(), Decision::Fatal);
    }

    #[test]
    fn delivered_events_reset_the_counter() {
        let mut p = policy(3);
        assert!(matches!(p.on_stream_ended(FLAP), Decision::Retry { .. }));
        assert!(matches!(p.on_stream_ended(FLAP), Decision::Retry { .. }));
        p.on_events_received();
        // Counter is back to zero; we get two retries again before fatal.
        assert!(matches!(p.on_stream_ended(FLAP), Decision::Retry { .. }));
        assert!(matches!(p.on_stream_ended(FLAP), Decision::Retry { .. }));
        assert_eq!(p.on_stream_ended(FLAP), Decision::Fatal);
    }

    #[test]
    fn flapping_without_events_still_reaches_fatal() {
        // Connect, deliver nothing, disconnect *quickly* — repeatedly. Without a
        // real event or a healthy-length connection the counter never resets, so
        // it must escalate.
        let mut p = policy(3);
        assert!(matches!(p.on_stream_ended(FLAP), Decision::Retry { .. }));
        assert!(matches!(p.on_stream_ended(FLAP), Decision::Retry { .. }));
        assert_eq!(p.on_stream_ended(FLAP), Decision::Fatal);
    }
}
