//! EIP-1559 fee pricing and replacement-fee escalation: the pure fee
//! arithmetic the [`MempoolExecutor`](crate::executors::MempoolExecutor) and its
//! replacement loop share. No I/O lives here — the executor fetches the gas
//! estimate and provider fee suggestion, then this module turns them into a
//! [`Fees`] pair, and escalates that pair for each replacement.
//!
//! The EIP-1559 invariant `max_priority_fee_per_gas <= max_fee_per_gas` is owned
//! by [`Fees`] itself: its fields are private and its only constructor clamps,
//! so neither initial [`price_1559`] nor per-replacement [`escalate`] can emit a
//! pair that violates it. This is the same construct-time-invariant pattern as
//! [`EscalationPercent`](crate::executors::EscalationPercent).

/// An EIP-1559 fee pair with the invariant `max_priority_fee_per_gas <=
/// max_fee_per_gas` held *by construction*: the fields are private and the only
/// constructor, [`Fees::new`], clamps the priority fee down to the max fee. A
/// caller — whether initial [`price_1559`] or per-replacement [`escalate`] —
/// cannot build a pair that breaks it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fees {
    max_fee_per_gas: u128,
    max_priority_fee_per_gas: u128,
}

impl Fees {
    /// A fee pair, clamping `max_priority_fee_per_gas` down to `max_fee_per_gas`
    /// so the EIP-1559 invariant holds however the inputs were priced.
    pub fn new(max_fee_per_gas: u128, max_priority_fee_per_gas: u128) -> Self {
        Self {
            max_fee_per_gas,
            max_priority_fee_per_gas: max_priority_fee_per_gas.min(max_fee_per_gas),
        }
    }

    /// The ceiling on total per-gas fee.
    pub fn max_fee_per_gas(&self) -> u128 {
        self.max_fee_per_gas
    }

    /// The miner tip per gas; never exceeds
    /// [`max_fee_per_gas`](Self::max_fee_per_gas).
    pub fn max_priority_fee_per_gas(&self) -> u128 {
        self.max_priority_fee_per_gas
    }
}

/// A provider's EIP-1559 fee suggestion (the shape of
/// `Provider::estimate_eip1559_fees`).
#[derive(Debug, Clone, Copy)]
pub struct FeeEstimate {
    pub max_fee_per_gas: u128,
    pub max_priority_fee_per_gas: u128,
}

/// Information about the gas bid for a transaction.
#[derive(Debug, Clone)]
pub struct GasBidInfo {
    /// Total profit expected from opportunity
    pub total_profit: u128,

    /// Percentage of bid profit to use for gas, at most 100: bidding the whole
    /// profit (100) breaks even; anything above it makes the transaction
    /// itself the loss, so the executor refuses such actions.
    pub bid_percentage: u64,
}

/// Price a transaction's EIP-1559 fees. Without a `bid`, the priority fee is the
/// provider's suggestion scaled by `bump_percent` and `max_fee` is the base
/// headroom plus that bumped priority. With a `bid`, `max_fee` is the
/// opportunity's break-even (`total_profit / gas_usage`) taken at
/// `bid_percentage`. Either way the result is built through [`Fees::new`], which
/// preserves the `priority <= max_fee` invariant — the bid arm can price
/// `max_fee` below the bumped priority.
pub fn price_1559(
    est: FeeEstimate,
    gas_usage: u64,
    bump_percent: u64,
    bid: Option<&GasBidInfo>,
) -> anyhow::Result<Fees> {
    let base_headroom = est
        .max_fee_per_gas
        .saturating_sub(est.max_priority_fee_per_gas);
    let bumped_priority = est.max_priority_fee_per_gas * bump_percent as u128 / 100;

    let (max_fee_per_gas, max_priority_fee_per_gas) = match bid {
        Some(bid) => {
            if bid.bid_percentage > 100 {
                return Err(anyhow::anyhow!(
                    "bid_percentage {} exceeds 100: the gas bid would cost more \
                     than the opportunity's total profit",
                    bid.bid_percentage
                ));
            }
            if gas_usage == 0 {
                return Err(anyhow::anyhow!(
                    "Gas estimation returned 0, cannot calculate bid price"
                ));
            }
            let breakeven = bid.total_profit / gas_usage as u128;
            let max_fee = breakeven * bid.bid_percentage as u128 / 100;
            (max_fee, bumped_priority)
        }
        None => (base_headroom + bumped_priority, bumped_priority),
    };

    Ok(Fees::new(max_fee_per_gas, max_priority_fee_per_gas))
}

/// Escalate both fee fields for a replacement transaction by
/// `escalation_percent`. With `escalation_percent >= 110` (enforced by
/// [`EscalationPercent`](crate::executors::EscalationPercent) at construction)
/// both fields rise by at least the node's ~10% minimum bump. Scaling is
/// monotonic so the `priority <= max_fee` invariant survives, and the result is
/// built through [`Fees::new`] regardless.
pub fn escalate(fees: Fees, escalation_percent: u64) -> Fees {
    let scale = |v: u128| {
        // Multiply-then-divide keeps the result exact for the realistic fee
        // range; if the intermediate product overflows, saturate to `u128::MAX`
        // rather than wrapping or losing the `/100` precision on small fees.
        match v.checked_mul(escalation_percent as u128) {
            Some(product) => product / 100,
            None => u128::MAX,
        }
    };
    Fees::new(
        scale(fees.max_fee_per_gas),
        scale(fees.max_priority_fee_per_gas),
    )
}

#[cfg(test)]
mod test {
    use super::*;

    fn est(max_fee: u128, priority: u128) -> FeeEstimate {
        FeeEstimate {
            max_fee_per_gas: max_fee,
            max_priority_fee_per_gas: priority,
        }
    }

    #[test]
    fn new_clamps_priority_to_max_fee() {
        let fees = Fees::new(50, 80);
        assert_eq!(fees.max_fee_per_gas(), 50);
        assert_eq!(fees.max_priority_fee_per_gas(), 50);
    }

    #[test]
    fn new_leaves_a_valid_pair_untouched() {
        let fees = Fees::new(100, 10);
        assert_eq!(fees.max_fee_per_gas(), 100);
        assert_eq!(fees.max_priority_fee_per_gas(), 10);
    }

    #[test]
    fn no_bid_rides_priority_on_base_headroom() {
        // headroom = 100 - 10 = 90; bump 100% leaves priority = 10.
        let fees = price_1559(est(100, 10), 21_000, 100, None).unwrap();
        assert_eq!(fees.max_priority_fee_per_gas(), 10);
        assert_eq!(fees.max_fee_per_gas(), 90 + 10);
    }

    #[test]
    fn no_bid_applies_the_priority_bump() {
        // bump 150% on priority 10 -> 15; headroom 90 unchanged.
        let fees = price_1559(est(100, 10), 21_000, 150, None).unwrap();
        assert_eq!(fees.max_priority_fee_per_gas(), 15);
        assert_eq!(fees.max_fee_per_gas(), 90 + 15);
    }

    #[test]
    fn bid_caps_max_fee_at_the_breakeven_share() {
        // total_profit 2_100_000, gas 21_000 -> breakeven 100/gas unit;
        // bid 50% -> max_fee 50.
        let bid = GasBidInfo {
            total_profit: 2_100_000,
            bid_percentage: 50,
        };
        let fees = price_1559(est(1_000, 10), 21_000, 100, Some(&bid)).unwrap();
        assert_eq!(fees.max_fee_per_gas(), 50);
        // priority (bumped 10) fits under the cap.
        assert_eq!(fees.max_priority_fee_per_gas(), 10);
    }

    #[test]
    fn a_low_bid_cap_clamps_priority_to_max_fee() {
        // breakeven 100/unit, bid 5% -> max_fee 5; priority would be 10, clamps to 5.
        let bid = GasBidInfo {
            total_profit: 2_100_000,
            bid_percentage: 5,
        };
        let fees = price_1559(est(1_000, 10), 21_000, 100, Some(&bid)).unwrap();
        assert_eq!(fees.max_fee_per_gas(), 5);
        assert_eq!(fees.max_priority_fee_per_gas(), 5);
        assert!(fees.max_priority_fee_per_gas() <= fees.max_fee_per_gas());
    }

    #[test]
    fn over_100_percent_bid_is_rejected() {
        let bid = GasBidInfo {
            total_profit: 100,
            bid_percentage: 101,
        };
        assert!(price_1559(est(100, 10), 21_000, 100, Some(&bid)).is_err());
    }

    #[test]
    fn zero_gas_with_a_bid_is_rejected() {
        let bid = GasBidInfo {
            total_profit: 100,
            bid_percentage: 50,
        };
        assert!(price_1559(est(100, 10), 0, 100, Some(&bid)).is_err());
    }

    #[test]
    fn a_malformed_estimate_yields_zero_headroom_without_panic() {
        // priority > max_fee: saturating headroom = 0; priority bump = 100.
        // max_fee = headroom (0) + bumped priority (100) = 100.
        let fees = price_1559(est(10, 100), 21_000, 100, None).unwrap();
        assert_eq!(fees.max_fee_per_gas(), 100);
        assert_eq!(fees.max_priority_fee_per_gas(), 100);
    }

    #[test]
    fn escalate_raises_both_fields_by_the_percentage() {
        let fees = Fees::new(200, 20);
        let bumped = escalate(fees, 125);
        assert_eq!(bumped.max_fee_per_gas(), 250);
        assert_eq!(bumped.max_priority_fee_per_gas(), 25);
    }

    #[test]
    fn escalate_at_the_minimum_raises_at_least_ten_percent() {
        let fees = Fees::new(100, 10);
        let bumped = escalate(fees, 110);
        assert!(bumped.max_fee_per_gas() >= 110);
        assert!(bumped.max_priority_fee_per_gas() >= 11);
    }

    #[test]
    fn escalate_preserves_the_invariant() {
        let fees = Fees::new(200, 200);
        let bumped = escalate(fees, 130);
        assert!(bumped.max_priority_fee_per_gas() <= bumped.max_fee_per_gas());
    }

    #[test]
    fn escalate_saturates_on_huge_fees() {
        let fees = Fees::new(u128::MAX, u128::MAX);
        let bumped = escalate(fees, 200);
        assert_eq!(bumped.max_fee_per_gas(), u128::MAX);
    }
}
