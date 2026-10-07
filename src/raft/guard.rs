//! Pure per-round decision for the cluster guard.
//!
//! The guard loop probes every configured peer once per round and feeds the largest identical
//! foreign fingerprint group and the largest identical foreign epoch group into
//! [`ClusterGuard::observe`]. Two independent consecutive-round hold-downs decide whether the node
//! must fence itself: configuration identity fencing applies in every state, stale-survivor epoch
//! fencing applies to an initialized node regardless of cached leader metrics. The pure decision
//! keeps majority arithmetic and the strike hold-down testable without a transport.
//!
//! Structural limit: the epoch fence needs `majority = roster / 2 + 1` peers to report one
//! foreign epoch, but a node can only probe `roster - 1` others, so it can never fire on a 1- or
//! 2-node roster. Runtime admission expiry supplies an independent supervised-stop path.

use super::probe::ConsecutiveMajority;

/// Everything the guard learns in one probe round.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct GuardRound {
    /// The local node has joined a cluster incarnation (`cluster_epoch` is committed).
    pub(crate) local_epoch_known: bool,
    /// Largest group of peers reporting one identical fingerprint that differs from ours.
    pub(crate) foreign_config: usize,
    /// Largest group of peers reporting one identical concrete epoch that differs from ours.
    pub(crate) foreign_epoch: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GuardVerdict {
    Continue,
    FenceConfig,
    FenceEpoch,
}

pub(crate) struct ClusterGuard {
    majority: usize,
    config: ConsecutiveMajority,
    epoch: ConsecutiveMajority,
}

impl ClusterGuard {
    #[must_use]
    pub(crate) const fn new(roster_size: usize, required_strikes: u32) -> Self {
        Self {
            majority: roster_size / 2 + 1,
            config: ConsecutiveMajority::new(required_strikes),
            epoch: ConsecutiveMajority::new(required_strikes),
        }
    }

    #[must_use]
    pub(crate) const fn majority(&self) -> usize {
        self.majority
    }

    #[must_use]
    pub(crate) const fn config_strikes(&self) -> u32 {
        self.config.strikes()
    }

    #[must_use]
    pub(crate) const fn epoch_strikes(&self) -> u32 {
        self.epoch.strikes()
    }

    /// Record one round. Configuration fencing is evaluated first and wins when both hold-downs
    /// complete in the same round.
    pub(crate) fn observe(&mut self, round: GuardRound) -> GuardVerdict {
        if self.config.observe(round.foreign_config, self.majority) {
            return GuardVerdict::FenceConfig;
        }
        // A cached leader ID can outlive its incarnation when Pre-Vote prevents a new election.
        let foreign_epoch = if round.local_epoch_known {
            round.foreign_epoch
        } else {
            0
        };
        if self.epoch.observe(foreign_epoch, self.majority) {
            return GuardVerdict::FenceEpoch;
        }
        GuardVerdict::Continue
    }
}

#[cfg(test)]
mod tests {
    use super::{ClusterGuard, GuardRound, GuardVerdict};

    const STRIKES: u32 = 3;

    fn round(foreign_config: usize, foreign_epoch: usize) -> GuardRound {
        GuardRound {
            local_epoch_known: true,
            foreign_config,
            foreign_epoch,
        }
    }

    #[test]
    fn config_majority_fences_on_the_required_consecutive_round() {
        let mut guard = ClusterGuard::new(3, STRIKES);
        assert_eq!(guard.majority(), 2);
        assert_eq!(guard.observe(round(2, 0)), GuardVerdict::Continue);
        assert_eq!(guard.observe(round(2, 0)), GuardVerdict::Continue);
        assert_eq!(guard.config_strikes(), 2);
        assert_eq!(guard.observe(round(2, 0)), GuardVerdict::FenceConfig);
    }

    #[test]
    fn a_below_majority_round_resets_the_config_hold_down() {
        let mut guard = ClusterGuard::new(3, STRIKES);
        guard.observe(round(2, 0));
        guard.observe(round(2, 0));
        assert_eq!(guard.observe(round(1, 0)), GuardVerdict::Continue);
        assert_eq!(guard.config_strikes(), 0);
        assert_eq!(guard.observe(round(2, 0)), GuardVerdict::Continue);
        assert_eq!(guard.config_strikes(), 1);
    }

    #[test]
    fn epoch_fence_requires_an_initialized_node() {
        let mut guard = ClusterGuard::new(3, STRIKES);
        for _ in 0..STRIKES {
            let uninitialized = GuardRound {
                local_epoch_known: false,
                ..round(0, 2)
            };
            assert_eq!(guard.observe(uninitialized), GuardVerdict::Continue);
        }
        assert_eq!(guard.epoch_strikes(), 0);

        assert_eq!(guard.observe(round(0, 2)), GuardVerdict::Continue);
        assert_eq!(guard.observe(round(0, 2)), GuardVerdict::Continue);
        assert_eq!(guard.observe(round(0, 2)), GuardVerdict::FenceEpoch);
    }

    #[test]
    fn a_below_majority_round_resets_the_epoch_hold_down() {
        let mut guard = ClusterGuard::new(3, STRIKES);
        guard.observe(round(0, 2));
        guard.observe(round(0, 2));
        assert_eq!(guard.observe(round(0, 1)), GuardVerdict::Continue);
        assert_eq!(guard.epoch_strikes(), 0);
    }

    #[test]
    fn coherent_foreign_majority_overrides_a_cached_leader() {
        let mut guard = ClusterGuard::new(3, STRIKES);
        let stale = round(0, 2);
        assert_eq!(guard.observe(stale), GuardVerdict::Continue);
        assert_eq!(guard.observe(stale), GuardVerdict::Continue);
        assert_eq!(guard.observe(stale), GuardVerdict::FenceEpoch);
    }

    #[test]
    fn config_fence_wins_when_both_hold_downs_complete_together() {
        let mut guard = ClusterGuard::new(5, STRIKES);
        assert_eq!(guard.majority(), 3);
        guard.observe(round(3, 3));
        guard.observe(round(3, 3));
        assert_eq!(guard.observe(round(3, 3)), GuardVerdict::FenceConfig);
    }

    #[test]
    fn epoch_fence_is_unreachable_with_fewer_than_three_peers() {
        for roster in [1_usize, 2] {
            let mut guard = ClusterGuard::new(roster, STRIKES);
            let reachable_peers = roster - 1;
            assert!(reachable_peers < guard.majority());
            for _ in 0..(STRIKES * 2) {
                assert_eq!(
                    guard.observe(round(0, reachable_peers)),
                    GuardVerdict::Continue,
                    "roster {roster}"
                );
            }
            assert_eq!(guard.epoch_strikes(), 0);
        }
    }
}
