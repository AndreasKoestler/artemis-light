//! The exponential backoff curve shared by the two places that wait and try
//! again: the execution-side [`Retry`](crate::executor_ext::Retry) wrapper and
//! the collector-side [`ReconnectPolicy`](crate::engine::reconnect::ReconnectPolicy).
//!
//! The curve is `base * 2^attempt`, saturating rather than overflowing for
//! large attempt counts. It is deliberately stateless: it owns neither an
//! attempt counter nor a clock. Each caller keeps its own counter — `Retry`
//! resets it per action, the reconnect policy resets it on a delivered event —
//! and asks the curve only for the delay of a given attempt. That is why the
//! same curve serves `Retry`'s 0-based retry counter and the reconnect policy's
//! 1-based failure counter without either's reset semantics leaking into it.

use std::time::Duration;

/// An exponential backoff curve over a base delay: attempt `n` waits
/// `base * 2^n`, saturating rather than overflowing for large `n`.
///
/// Stateless by design (see the [module docs](self)); the caller supplies the
/// attempt number.
#[derive(Debug, Clone, Copy)]
pub struct Backoff {
    base: Duration,
}

impl Backoff {
    /// A curve doubling from `base`.
    pub fn new(base: Duration) -> Self {
        Self { base }
    }

    /// The delay for `attempt`: `base * 2^attempt`. The exponent saturates at
    /// [`u32::MAX`] and the multiply saturates at [`Duration::MAX`], so a large
    /// `attempt` yields a huge-but-finite delay rather than overflowing.
    pub fn delay(&self, attempt: u32) -> Duration {
        let factor = 2u32.checked_pow(attempt).unwrap_or(u32::MAX);
        self.base.saturating_mul(factor)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn doubles_each_attempt_from_the_base() {
        let backoff = Backoff::new(Duration::from_millis(100));
        assert_eq!(backoff.delay(0), Duration::from_millis(100));
        assert_eq!(backoff.delay(1), Duration::from_millis(200));
        assert_eq!(backoff.delay(2), Duration::from_millis(400));
        assert_eq!(backoff.delay(3), Duration::from_millis(800));
    }

    #[test]
    fn saturates_instead_of_overflowing_on_a_large_attempt() {
        let backoff = Backoff::new(Duration::from_secs(1));
        // 2^200 overflows `u32::pow`, so the factor saturates to `u32::MAX` and
        // the multiply saturates rather than panicking.
        assert_eq!(
            backoff.delay(200),
            Duration::from_secs(1).saturating_mul(u32::MAX)
        );
    }

    #[test]
    fn a_zero_base_stays_zero() {
        let backoff = Backoff::new(Duration::ZERO);
        assert_eq!(backoff.delay(5), Duration::ZERO);
    }
}
