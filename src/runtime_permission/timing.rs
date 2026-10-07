//! Separate consumer, grantor and restart lifetimes under bounded monotonic clock rates.
//!
//! The largest supported elapsed-clock-rate ratio between participating runtimes is two.
//! Cleanup must finish within its supplied local budget. Neither bound covers suspended
//! clocks, indefinite scheduling/kernel delays, or shortening policy while old grants live.

use std::time::Duration;
use tokio::time::Instant;

const CLOCK_RATE_RATIO: u32 = 2;

#[derive(Clone, Copy, Debug)]
pub(crate) struct LeaseTiming {
    consumer_use: Duration,
    issuer_reservation: Duration,
    restart_quarantine: Duration,
    vip_activation_delay: Duration,
    rpc_budget: Duration,
    renewal_interval: Duration,
    renewal_round_budget: Duration,
}

impl LeaseTiming {
    pub(crate) fn for_config(
        cfg: &crate::config::Config,
        vip_count: usize,
    ) -> anyhow::Result<Self> {
        let count = u32::try_from(vip_count)
            .map_err(|_| anyhow::anyhow!("too many VIPs for admission cleanup budget"))?;
        let cleanup = crate::vip::SHUTDOWN_VIP_BUDGET
            .checked_mul(count)
            .and_then(|duration| duration.checked_add(crate::vip::RECONCILE_TICK))
            .ok_or_else(|| anyhow::anyhow!("admission cleanup budget overflow"))?;
        let freshness = crate::consensus_freshness::ConsensusFreshness::for_probe_cadence(
            cfg.health.interval_ms,
            cfg.health.effective_stale_missed_probes(),
        );
        let health_window = freshness.lifetime();
        let rpc_budget = (health_window / 6).min(Duration::from_millis(500));
        let renewal_interval = health_window / 4;
        // Elections need more time than VIP health proofs; see docs/runtime-admission.md.
        let election = Duration::from_millis(cfg.raft.election_timeout_max_ms)
            .checked_mul(3)
            .and_then(|duration| {
                duration.checked_add(Duration::from_millis(cfg.raft.heartbeat_interval_ms) * 3 / 2)
            })
            .and_then(|duration| {
                duration.checked_add(Duration::from_millis(cfg.raft.election_timeout_min_ms) * 2)
            })
            .ok_or_else(|| anyhow::anyhow!("admission election budget overflow"))?;
        let round_slots = u32::try_from(cfg.peers.len())
            .ok()
            .and_then(|count| count.checked_add(4))
            .ok_or_else(|| anyhow::anyhow!("admission renewal roster too large"))?;
        let round_budget = rpc_budget
            .checked_mul(round_slots)
            .ok_or_else(|| anyhow::anyhow!("admission renewal budget overflow"))?;
        let recovery_budget = election
            .checked_mul(CLOCK_RATE_RATIO)
            .and_then(|duration| duration.checked_add(round_budget * 4))
            .and_then(|duration| duration.checked_add(renewal_interval * 2))
            .ok_or_else(|| anyhow::anyhow!("admission recovery budget overflow"))?;
        let mut timing = Self::new(health_window.max(recovery_budget), cleanup)?;
        timing.rpc_budget = rpc_budget;
        timing.renewal_interval = renewal_interval;
        timing.renewal_round_budget = round_budget;
        Ok(timing)
    }

    pub(crate) fn new(consumer_use: Duration, cleanup_budget: Duration) -> anyhow::Result<Self> {
        anyhow::ensure!(
            !consumer_use.is_zero(),
            "admission lifetime must be positive"
        );
        let issuer_reservation = consumer_use
            .checked_mul(CLOCK_RATE_RATIO)
            .ok_or_else(|| anyhow::anyhow!("admission reservation duration overflow"))?;
        let restart_quarantine = issuer_reservation
            .checked_mul(CLOCK_RATE_RATIO)
            .ok_or_else(|| anyhow::anyhow!("admission quarantine duration overflow"))?;
        let vip_activation_delay = consumer_use
            .checked_add(cleanup_budget)
            .and_then(|duration| duration.checked_mul(CLOCK_RATE_RATIO))
            .ok_or_else(|| anyhow::anyhow!("VIP activation duration overflow"))?;
        Ok(Self {
            consumer_use,
            issuer_reservation,
            restart_quarantine,
            vip_activation_delay,
            rpc_budget: (consumer_use / 6).min(Duration::from_millis(500)),
            renewal_interval: consumer_use / 4,
            renewal_round_budget: consumer_use,
        })
    }

    pub(crate) fn consumer_use(self) -> Duration {
        self.consumer_use
    }
    pub(crate) fn issuer_reservation(self) -> Duration {
        self.issuer_reservation
    }
    pub(crate) fn restart_quarantine(self) -> Duration {
        self.restart_quarantine
    }

    pub(crate) fn rpc_budget(self) -> Duration {
        self.rpc_budget
    }

    pub(crate) fn renewal_interval(self) -> Duration {
        self.renewal_interval
    }

    pub(crate) fn renewal_round_budget(self) -> Duration {
        self.renewal_round_budget
    }

    pub(crate) fn startup_vip_delay(self) -> anyhow::Result<Duration> {
        self.restart_quarantine
            .checked_add(self.vip_activation_delay)
            .ok_or_else(|| anyhow::anyhow!("VIP startup duration overflow"))
    }

    /// Start before sending the challenge, never when its reply arrives.
    pub(crate) fn consumer_deadline(self, request_started: Instant) -> anyhow::Result<Instant> {
        deadline(request_started, self.consumer_use())
    }

    /// Start when the issuer reserves authority, before returning its receipt.
    pub(crate) fn reservation_deadline(self, reserved: Instant) -> anyhow::Result<Instant> {
        deadline(reserved, self.issuer_reservation())
    }

    /// A new boot has forgotten old grants; wait before issuing or using authority.
    pub(crate) fn quarantine_deadline(self, boot_started: Instant) -> anyhow::Result<Instant> {
        deadline(boot_started, self.restart_quarantine())
    }

    pub(crate) fn vip_activation_deadline(self, admitted: Instant) -> anyhow::Result<Instant> {
        deadline(admitted, self.vip_activation_delay)
    }
}

fn deadline(start: Instant, lifetime: Duration) -> anyhow::Result<Instant> {
    start
        .checked_add(lifetime)
        .ok_or_else(|| anyhow::anyhow!("admission deadline overflow"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::Instant;

    #[test]
    fn voting_quarantine_does_not_accumulate_kernel_cleanup_time() {
        let admission = Duration::from_millis(3_500);
        let without_vips = LeaseTiming::new(admission, Duration::ZERO).unwrap();
        let three_vips = LeaseTiming::new(admission, Duration::from_millis(36_250)).unwrap();
        assert_eq!(three_vips.issuer_reservation(), admission * 2);
        assert_eq!(three_vips.restart_quarantine(), admission * 4);
        assert_eq!(
            three_vips.restart_quarantine(),
            without_vips.restart_quarantine()
        );
    }

    #[test]
    fn configuration_uses_every_vip_cleanup_budget() {
        let cfg: crate::config::Config = serde_yaml::from_str(
            "node_id: 1\nraft_listen: '127.0.0.1:1000'\nclient_submit_listen: '127.0.0.1:2000'\npeers:\n  - {id: 1, raft_address: '127.0.0.1:1000', client_submit_address: '127.0.0.1:2000'}\nvips: []\nhealth: {command: [/bin/true], interval_ms: 1000, timeout_ms: 500}\ncluster_secret: runtime-timing-fixture-key-0123456789\ndry_run: true\n",
        ).unwrap();
        let proof = crate::consensus_freshness::ConsensusFreshness::for_probe_cadence(
            cfg.health.interval_ms,
            cfg.health.effective_stale_missed_probes(),
        );
        for count in [0, 1, 3, 20] {
            let timing = LeaseTiming::for_config(&cfg, count).unwrap();
            assert!(timing.consumer_use() >= proof.lifetime());
            let cleanup = crate::vip::RECONCILE_TICK
                + crate::vip::SHUTDOWN_VIP_BUDGET * u32::try_from(count).unwrap();
            assert_eq!(timing.issuer_reservation(), timing.consumer_use() * 2);
            let admitted = Instant::now();
            assert_eq!(
                timing.vip_activation_deadline(admitted).unwrap(),
                admitted + (timing.consumer_use() + cleanup) * 2
            );
        }
        assert!(LeaseTiming::for_config(&cfg, usize::MAX).is_err());
    }

    #[test]
    fn short_health_window_does_not_expire_permission_before_an_election() {
        let mut cfg: crate::config::Config =
            serde_yaml::from_str(include_str!("../../config.example.yaml")).unwrap();
        cfg.health.interval_ms = 500;
        cfg.health.timeout_ms = 900;
        cfg.health.stale_secs = Some(1);
        cfg.raft.election_timeout_min_ms = 2_000;
        cfg.raft.election_timeout_max_ms = 3_000;
        let health = crate::consensus_freshness::ConsensusFreshness::for_probe_cadence(
            cfg.health.interval_ms,
            cfg.health.effective_stale_missed_probes(),
        );
        assert_eq!(health.lifetime(), Duration::from_millis(1_250));
        let timing = LeaseTiming::for_config(&cfg, cfg.vips.len()).unwrap();
        assert_eq!(timing.rpc_budget(), health.lifetime() / 6);
        assert_eq!(timing.renewal_interval(), health.lifetime() / 4);
        assert!(
            timing.consumer_use() > Duration::from_millis(cfg.raft.election_timeout_max_ms),
            "permission {:?} cannot outlive an ordinary {}ms election",
            timing.consumer_use(),
            cfg.raft.election_timeout_max_ms
        );
        let mut faster = cfg.clone();
        faster.raft.election_timeout_min_ms /= 2;
        faster.raft.election_timeout_max_ms /= 2;
        let faster_timing = LeaseTiming::for_config(&faster, faster.vips.len()).unwrap();
        assert!(faster_timing.consumer_use() < timing.consumer_use());
        assert_eq!(faster_timing.rpc_budget(), timing.rpc_budget());
        assert_eq!(faster_timing.renewal_interval(), timing.renewal_interval());
    }

    #[test]
    fn a_long_health_window_remains_the_permission_floor() {
        let mut cfg: crate::config::Config =
            serde_yaml::from_str(include_str!("../../config.example.yaml")).unwrap();
        cfg.health.interval_ms = 1_000;
        cfg.health.stale_secs = Some(200);
        let health = crate::consensus_freshness::ConsensusFreshness::for_probe_cadence(
            cfg.health.interval_ms,
            cfg.health.effective_stale_missed_probes(),
        );
        let timing = LeaseTiming::for_config(&cfg, cfg.vips.len()).unwrap();
        assert_eq!(timing.consumer_use(), health.lifetime());
        assert_eq!(timing.rpc_budget(), Duration::from_millis(500));
        assert_eq!(timing.renewal_interval(), health.lifetime() / 4);
    }

    #[tokio::test(start_paused = true)]
    async fn reservation_and_quarantine_cover_distinct_clock_bounds() {
        let timing =
            LeaseTiming::new(Duration::from_millis(3_500), Duration::from_millis(36_250)).unwrap();
        assert_eq!(timing.consumer_use(), Duration::from_millis(3_500));
        assert_eq!(timing.issuer_reservation(), Duration::from_secs(7));
        assert_eq!(timing.restart_quarantine(), Duration::from_secs(14));
        assert_eq!(
            timing.startup_vip_delay().unwrap(),
            Duration::from_millis(93_500)
        );
        let start = Instant::now();
        assert_eq!(
            timing.consumer_deadline(start).unwrap(),
            start + Duration::from_millis(3_500)
        );
        assert_eq!(
            timing.reservation_deadline(start).unwrap(),
            start + Duration::from_secs(7)
        );
        assert_eq!(
            timing.quarantine_deadline(start).unwrap(),
            start + Duration::from_secs(14)
        );
        assert_eq!(
            timing.vip_activation_deadline(start).unwrap(),
            start + Duration::from_millis(79_500)
        );
    }

    #[test]
    fn zero_use_and_duration_overflow_are_rejected_not_saturated() {
        assert!(LeaseTiming::new(Duration::ZERO, Duration::from_secs(1)).is_err());
        assert!(
            LeaseTiming::new(Duration::MAX / 7, Duration::MAX / 3)
                .unwrap()
                .startup_vip_delay()
                .is_err()
        );
        for (use_time, cleanup) in [
            (Duration::MAX, Duration::ZERO),
            (Duration::from_secs(1), Duration::MAX),
            (Duration::MAX / 2, Duration::ZERO),
        ] {
            assert!(LeaseTiming::new(use_time, cleanup).is_err());
        }
        let no_vips = LeaseTiming::new(Duration::from_secs(1), Duration::ZERO).unwrap();
        assert_eq!(no_vips.issuer_reservation(), Duration::from_secs(2));
        assert_eq!(no_vips.restart_quarantine(), Duration::from_secs(4));
    }
}
