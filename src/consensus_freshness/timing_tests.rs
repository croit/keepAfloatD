//! Cross-layer timing regressions: real interval scheduling, state-machine apply, and bind gates.
use super::ConsensusFreshness;
use crate::bind_policy::{BindGates, should_bind_or_keep_vip};
use crate::config::VipAddr;
use crate::raft::KafRequest;
use crate::raft::store::{KafStateMachine, new_store};
use crate::raft::types::TypeConfig;
use openraft::Membership;
use openraft::alias::EntryOf;
use openraft::entry::RaftEntry;
use openraft::storage::RaftStateMachine;
use openraft::testing::log_id;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{Instant, MissedTickBehavior};

async fn apply_health(sm: &mut KafStateMachine, index: &mut u64, node_id: u64) {
    *index += 1;
    let entry = EntryOf::<TypeConfig>::new_normal(
        log_id::<TypeConfig>(1, 1, *index),
        KafRequest::HealthUpdate {
            node_id,
            healthy: true,
        },
    );
    sm.apply(futures::stream::iter([Ok::<_, std::io::Error>((
        entry, None,
    ))]))
    .await
    .unwrap();
}

#[tokio::test(start_paused = true)]
async fn delayed_probe_completion_cannot_activate_before_old_holder_proof_expires() {
    let vip = "192.0.2.99".parse().unwrap();
    let table = Arc::new(vec![(VipAddr::host(vip), "lo".into())]);
    let (_, mut sm, state) = new_store(table, 1, false, 0);
    let members = [1, 2, 3];
    let entry = EntryOf::<TypeConfig>::new_membership(
        log_id::<TypeConfig>(1, 1, 0),
        Membership::new_with_defaults(vec![members.into_iter().collect::<BTreeSet<_>>()], members),
    );
    sm.apply(futures::stream::iter([Ok::<_, std::io::Error>((
        entry, None,
    ))]))
    .await
    .unwrap();
    let mut index = 0;
    // Historical committed prefix: node 1 owns the VIP and trails the frontier by one round.
    for _ in 0..9 {
        for node_id in members {
            apply_health(&mut sm, &mut index, node_id).await;
        }
    }
    for node_id in [2, 3] {
        apply_health(&mut sm, &mut index, node_id).await;
    }
    let start = Instant::now();
    let mut survivor_interval = tokio::time::interval(Duration::from_secs(1));
    survivor_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    survivor_interval.tick().await;

    // The old leader's successful probe catches up to round 10. Its next probe blocks and
    // transport is then isolated. An isolated leader can retain current_leader=Some(self).
    tokio::time::sleep_until(start + Duration::from_millis(980)).await;
    apply_health(&mut sm, &mut index, 1).await;
    let old_proof = ConsensusFreshness::for_probe_cadence(1_000, 1);
    old_proof.record_success(Instant::now());
    let (old_assignment, old_ticks, old_frontier) = {
        let state = state.read().await;
        assert_eq!(state.latest_probe_tick, 10);
        assert_eq!(state.vip_assignments[&vip].holder, 1);
        (
            state.vip_assignments[&vip].clone(),
            state.node_probe_ticks.clone(),
            state.latest_probe_tick,
        )
    };

    // The surviving majority has 520ms to elect a leader before this slow probe completes.
    // Its next scheduled tick is already ready; Skip permits a second immediate completion.
    tokio::time::sleep_until(start + Duration::from_millis(1_500)).await;
    apply_health(&mut sm, &mut index, 2).await;
    survivor_interval.tick().await;
    assert_eq!(start.elapsed(), Duration::from_millis(1_500));
    apply_health(&mut sm, &mut index, 2).await;
    let mut takeover_delay = crate::vip::takeover::TakeoverDelay::new(old_proof.lifetime());
    {
        let state = state.read().await;
        assert_eq!(state.latest_probe_tick, 12);
        assert_eq!(state.vip_assignments[&vip].holder, 2);
        assert_eq!(state.vip_assignments[&vip].activation_tick, 13);
        assert!(!takeover_delay.ready(
            vip,
            2,
            state.vip_assignments.get(&vip),
            &state.node_probe_ticks,
            state.latest_probe_tick,
            1,
            None
        ));
    }
    survivor_interval.tick().await;
    assert_eq!(start.elapsed(), Duration::from_secs(2));
    apply_health(&mut sm, &mut index, 2).await;
    // Timed-out requests may still commit later. Applying a queued burst must not buy
    // elapsed takeover time, regardless of how far it advances the replicated frontier.
    for _ in 0..8 {
        apply_health(&mut sm, &mut index, 2).await;
    }

    let old_can_keep = should_bind_or_keep_vip(
        BindGates {
            has_leader: true,
            local_healthy: true,
            consensus_fresh: old_proof.is_fresh(),
        },
        1,
        Some(&old_assignment),
        &old_ticks,
        old_frontier,
        1,
        Some(old_assignment.generation),
    );
    let state = state.read().await;
    let committed_takeover = should_bind_or_keep_vip(
        BindGates {
            has_leader: true,
            local_healthy: true,
            consensus_fresh: true,
        },
        2,
        state.vip_assignments.get(&vip),
        &state.node_probe_ticks,
        state.latest_probe_tick,
        1,
        None,
    );
    assert!(
        committed_takeover,
        "survivors must complete the committed takeover"
    );
    let local_takeover = takeover_delay.ready(
        vip,
        2,
        state.vip_assignments.get(&vip),
        &state.node_probe_ticks,
        state.latest_probe_tick,
        1,
        None,
    );
    assert!(
        !(old_can_keep && committed_takeover && local_takeover),
        "survivor activated after only 1020ms, but the old holder's 1500ms proof still permits its VIP"
    );
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(takeover_delay.ready(
        vip,
        2,
        state.vip_assignments.get(&vip),
        &state.node_probe_ticks,
        state.latest_probe_tick,
        1,
        None
    ));
    assert!(
        !old_proof.is_fresh(),
        "takeover must be available after the old proof has expired"
    );
}
