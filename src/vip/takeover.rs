//! Process-local delay for takeover without a previous holder's explicit release (#26).
use crate::raft::store::{VipAssignment, is_node_probe_fresh};
use std::collections::{BTreeMap, HashMap};
use std::net::IpAddr;
use std::time::Duration;
use tokio::time::Instant;

pub(crate) struct TakeoverDelay {
    lifetime: Duration,
    waiting: HashMap<IpAddr, (u64, Option<u64>, Instant)>,
}

impl TakeoverDelay {
    pub(crate) fn new(lifetime: Duration) -> Self {
        Self {
            lifetime,
            waiting: HashMap::new(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn ready(
        &mut self,
        vip: IpAddr,
        node_id: u64,
        assignment: Option<&VipAssignment>,
        ticks: &BTreeMap<u64, u64>,
        frontier: u64,
        stale: u64,
        activated_generation: Option<u64>,
    ) -> bool {
        let Some(assignment) = assignment.filter(|a| a.holder == node_id) else {
            self.waiting.remove(&vip);
            return false;
        };
        if activated_generation == Some(assignment.generation)
            || (assignment.previous_holder.is_some() && assignment.previous_holder_released)
            || (assignment.previous_holder.is_none() && assignment.generation == 1)
        {
            self.waiting.remove(&vip);
            return true;
        }
        if assignment
            .previous_holder
            .is_some_and(|previous| is_node_probe_fresh(previous, ticks, frontier, stale))
        {
            self.waiting.remove(&vip);
            return false;
        }
        let previous_tick = assignment
            .previous_holder
            .and_then(|node| ticks.get(&node).copied());
        let pending = self.waiting.entry(vip).or_insert_with(|| {
            tracing::debug!(%vip, node_id, generation = assignment.generation,
                    ?previous_tick, frontier, wait_ms = self.lifetime.as_millis(),
                    "waiting for previous VIP holder lease expiry");
            (assignment.generation, previous_tick, Instant::now())
        });
        // #26: a renewed holder can become stale again between local samples after bursty commits.
        if pending.0 != assignment.generation || pending.1 != previous_tick {
            tracing::debug!(%vip, node_id, generation = assignment.generation,
                ?previous_tick, frontier, "restarting previous VIP holder lease wait");
            *pending = (assignment.generation, previous_tick, Instant::now());
        }
        pending.2.elapsed() >= self.lifetime
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assignment() -> VipAssignment {
        VipAssignment {
            holder: 2,
            generation: 2,
            previous_holder: Some(1),
            previous_holder_released: false,
            activation_tick: 0,
        }
    }

    fn ready(
        delay: &mut TakeoverDelay,
        assignment: Option<&VipAssignment>,
        ticks: &BTreeMap<u64, u64>,
        activated: Option<u64>,
    ) -> bool {
        delay.ready(
            "192.0.2.1".parse().unwrap(),
            2,
            assignment,
            ticks,
            10,
            1,
            activated,
        )
    }

    #[tokio::test(start_paused = true)]
    async fn ownerless_gap_preserves_fresh_holder_fencing_and_stale_takeover_delay() {
        use crate::config::VipAddr;
        use crate::raft::store::reconcile_vip_assignments;

        let vip = "192.0.2.1".parse().unwrap();
        let table = vec![(
            VipAddr {
                addr: vip,
                prefix: 32,
            },
            "lo".into(),
        )];
        let mut assignments = BTreeMap::new();
        let mut generations = BTreeMap::new();
        let mut last_holders = BTreeMap::new();
        for holders in [
            BTreeMap::from([(vip, 1)]),
            BTreeMap::new(),
            BTreeMap::from([(vip, 2)]),
        ] {
            reconcile_vip_assignments(
                &holders,
                10,
                &table,
                &mut assignments,
                &mut generations,
                &mut last_holders,
            );
        }
        let assignment = assignments.get(&vip).unwrap();
        let mut delay = TakeoverDelay::new(Duration::from_secs(1));
        let fresh = BTreeMap::from([(1, 10)]);
        assert!(!ready(&mut delay, Some(assignment), &fresh, None));
        tokio::time::advance(Duration::from_secs(2)).await;
        assert!(!ready(&mut delay, Some(assignment), &fresh, None));

        let stale = BTreeMap::from([(1, 1)]);
        assert!(!ready(&mut delay, Some(assignment), &stale, None));
        tokio::time::advance(Duration::from_millis(999)).await;
        assert!(!ready(&mut delay, Some(assignment), &stale, None));
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(ready(&mut delay, Some(assignment), &stale, None));
    }

    #[tokio::test(start_paused = true)]
    async fn stale_takeover_waits_a_full_proof_lifetime_even_after_bursty_commits() {
        let mut delay = TakeoverDelay::new(Duration::from_secs(1));
        let assignment = assignment();
        let stale = BTreeMap::from([(1, 1)]);
        assert!(!ready(&mut delay, Some(&assignment), &stale, None));
        tokio::time::advance(Duration::from_millis(999)).await;
        assert!(!ready(&mut delay, Some(&assignment), &stale, None));
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(ready(&mut delay, Some(&assignment), &stale, None));
    }

    #[tokio::test(start_paused = true)]
    async fn missed_transient_health_renewal_restarts_the_stale_takeover_wait() {
        let mut delay = TakeoverDelay::new(Duration::from_secs(1));
        let assignment = assignment();
        assert!(!ready(
            &mut delay,
            Some(&assignment),
            &BTreeMap::from([(1, 1)]),
            None
        ));
        tokio::time::advance(Duration::from_millis(900)).await;
        // The holder renewed at tick 3, then queued survivor writes reached 10 before sampling.
        assert!(!ready(
            &mut delay,
            Some(&assignment),
            &BTreeMap::from([(1, 3)]),
            None
        ));
        tokio::time::advance(Duration::from_millis(100)).await;
        assert!(
            !ready(
                &mut delay,
                Some(&assignment),
                &BTreeMap::from([(1, 3)]),
                None
            ),
            "an already-stale renewed holder still needs a complete new lease wait"
        );
        tokio::time::advance(Duration::from_millis(900)).await;
        assert!(ready(
            &mut delay,
            Some(&assignment),
            &BTreeMap::from([(1, 3)]),
            None
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn fresh_previous_holder_resets_the_wait() {
        let mut delay = TakeoverDelay::new(Duration::from_secs(1));
        let assignment = assignment();
        let stale = BTreeMap::from([(1, 1)]);
        assert!(!ready(&mut delay, Some(&assignment), &stale, None));
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(!ready(
            &mut delay,
            Some(&assignment),
            &BTreeMap::from([(1, 10)]),
            None
        ));
        assert!(!ready(&mut delay, Some(&assignment), &stale, None));
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(ready(&mut delay, Some(&assignment), &stale, None));
    }

    #[tokio::test(start_paused = true)]
    async fn missing_changed_or_foreign_assignment_cannot_reuse_a_wait() {
        let mut delay = TakeoverDelay::new(Duration::from_secs(1));
        let mut assignment = assignment();
        let ticks = BTreeMap::new();
        assert!(!ready(&mut delay, Some(&assignment), &ticks, None));
        tokio::time::advance(Duration::from_secs(1)).await;
        assignment.generation += 1;
        assert!(!ready(&mut delay, Some(&assignment), &ticks, None));
        assert!(!ready(&mut delay, None, &ticks, None));
        assignment.holder = 3;
        assert!(!ready(&mut delay, Some(&assignment), &ticks, None));
        assignment.holder = 2;
        assert!(!ready(&mut delay, Some(&assignment), &ticks, None));
    }

    #[tokio::test(start_paused = true)]
    async fn explicit_release_cold_start_and_prior_activation_do_not_wait() {
        let mut delay = TakeoverDelay::new(Duration::from_secs(1));
        let mut assignment = assignment();
        let ticks = BTreeMap::new();
        assert!(ready(&mut delay, Some(&assignment), &ticks, Some(2)));
        assignment.previous_holder_released = true;
        assert!(ready(&mut delay, Some(&assignment), &ticks, None));
        assignment.previous_holder = None;
        assert!(
            !ready(&mut delay, Some(&assignment), &ticks, None),
            "a restored generation has no explicit release proof"
        );
        assignment.generation = 1;
        assert!(ready(&mut delay, Some(&assignment), &ticks, None));
    }
}
