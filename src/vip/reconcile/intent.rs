//! Validate an in-flight local effect against renewed applied state (#26).
use crate::bind_policy::{BindGates, should_bind_or_keep_vip};
use crate::raft::KafStorageState;
use crate::raft::store::VipAssignment;
use std::net::IpAddr;

#[derive(Clone)]
pub(super) struct EffectIntent {
    pub(super) assignment: Option<VipAssignment>,
    pub(super) bind_policy: bool,
    pub(super) vip: IpAddr,
    node_id: u64,
    gates: BindGates,
    activated_generation: Option<u64>,
    previous_tick: Option<u64>,
}

impl EffectIntent {
    pub(super) fn activated(mut self) -> Self {
        self.activated_generation = self
            .assignment
            .as_ref()
            .map(|assignment| assignment.generation);
        self
    }

    pub(super) fn capture(
        vip: IpAddr,
        node_id: u64,
        gates: BindGates,
        state: &KafStorageState,
        activated_generation: Option<u64>,
    ) -> Self {
        let assignment = state.vip_assignments.get(&vip).cloned();
        let bind_policy = should_bind_or_keep_vip(
            gates,
            node_id,
            assignment.as_ref(),
            &state.node_probe_ticks,
            state.latest_probe_tick,
            state.stale_missed_probes,
            activated_generation,
        );
        let previous_tick = assignment
            .as_ref()
            .and_then(|a| a.previous_holder)
            .and_then(|node| state.node_probe_ticks.get(&node).copied());
        Self {
            assignment,
            bind_policy,
            vip,
            node_id,
            gates,
            activated_generation,
            previous_tick,
        }
    }

    pub(super) fn remains_valid(&self, state: &KafStorageState, gates: BindGates) -> bool {
        let current = Self::capture(
            self.vip,
            self.node_id,
            gates,
            state,
            self.activated_generation,
        );
        let mut expected = self.assignment.clone();
        if let Some(assignment) = expected.as_mut()
            && assignment.holder == self.node_id
            && self.activated_generation == Some(assignment.generation)
            && current
                .assignment
                .as_ref()
                .is_some_and(|a| a.previous_holder_released)
        {
            // A completed release opens the same handoff fence without revoking its incumbent.
            assignment.previous_holder_released = true;
        }
        if expected != current.assignment
            || self.gates != gates
            || self.bind_policy != current.bind_policy
        {
            return false;
        }
        // A currently stale holder may have renewed between samples. An already activated
        // generation does not depend on its predecessor's health, but first activation still does.
        let waiting_for_predecessor = self.assignment.as_ref().is_some_and(|a| {
            a.holder == self.node_id
                && a.previous_holder.is_some()
                && !a.previous_holder_released
                && self.activated_generation != Some(a.generation)
        });
        !waiting_for_predecessor || self.previous_tick == current.previous_tick
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn gates() -> BindGates {
        BindGates {
            has_leader: true,
            local_healthy: true,
            consensus_fresh: true,
        }
    }

    #[tokio::test]
    async fn activated_holder_survives_its_predecessors_release_ack() {
        let (_, _, state) = crate::raft::store::new_store(Arc::new(Vec::new()), 1, true, 0);
        let mut state = state.write().await;
        let vip = "192.0.2.1".parse().unwrap();
        state.latest_probe_tick = 10;
        state.node_probe_ticks.insert(2, 1);
        state.vip_assignments.insert(
            vip,
            VipAssignment {
                holder: 1,
                generation: 2,
                previous_holder: Some(2),
                previous_holder_released: false,
                activation_tick: 10,
            },
        );
        let pending = EffectIntent::capture(vip, 1, gates(), &state, None);
        let activated = pending.clone().activated();
        state
            .vip_assignments
            .get_mut(&vip)
            .unwrap()
            .previous_holder_released = true;
        assert!(
            !pending.remains_valid(&state, gates()),
            "first activation still needs a fresh intent"
        );
        assert!(
            activated.remains_valid(&state, gates()),
            "a release ack must not withdraw an activated incumbent"
        );
        state.vip_assignments.get_mut(&vip).unwrap().generation = 3;
        assert!(
            !activated.remains_valid(&state, gates()),
            "activation cannot cross generations"
        );
    }

    #[tokio::test]
    async fn renewed_intents_require_matching_generation_gates_and_activation_eligibility() {
        let (_, _, state) = crate::raft::store::new_store(Arc::new(Vec::new()), 1, true, 0);
        let mut state = state.write().await;
        let vip = "192.0.2.1".parse().unwrap();
        state.latest_probe_tick = 10;
        state.node_probe_ticks.insert(2, 1);
        state.vip_assignments.insert(
            vip,
            VipAssignment {
                holder: 1,
                generation: 2,
                previous_holder: Some(2),
                previous_holder_released: false,
                activation_tick: 10,
            },
        );
        let captured = EffectIntent::capture(vip, 1, gates(), &state, None);
        assert!(captured.remains_valid(&state, gates()));
        for closed in [
            BindGates {
                has_leader: false,
                ..gates()
            },
            BindGates {
                local_healthy: false,
                ..gates()
            },
            BindGates {
                consensus_fresh: false,
                ..gates()
            },
        ] {
            assert!(!captured.remains_valid(&state, closed));
        }
        state.node_probe_ticks.insert(2, 3);
        assert!(
            !captured.remains_valid(&state, gates()),
            "missed transient renewal invalidates first activation"
        );
        let activated = EffectIntent::capture(vip, 1, gates(), &state, Some(2));
        state.node_probe_ticks.insert(2, 10);
        assert!(
            activated.remains_valid(&state, gates()),
            "a safely activated generation survives predecessor recovery"
        );
        assert!(!captured.remains_valid(&state, gates()));
        state.vip_assignments.get_mut(&vip).unwrap().generation = 3;
        assert!(!activated.remains_valid(&state, gates()));
        state.vip_assignments.get_mut(&vip).unwrap().activation_tick = 11;
        let before_activation = EffectIntent::capture(vip, 1, gates(), &state, Some(3));
        state.latest_probe_tick = 11;
        assert!(!before_activation.remains_valid(&state, gates()));
        state.vip_assignments.remove(&vip);
        assert!(!activated.remains_valid(&state, gates()));
        let absent = EffectIntent::capture(vip, 1, gates(), &state, None);
        assert!(absent.remains_valid(&state, gates()));
    }

    #[tokio::test]
    async fn release_intent_survives_its_previous_holders_health_renewal() {
        let (_, _, state) = crate::raft::store::new_store(Arc::new(Vec::new()), 1, true, 0);
        let mut state = state.write().await;
        let vip = "192.0.2.1".parse().unwrap();
        state.vip_assignments.insert(
            vip,
            VipAssignment {
                holder: 2,
                generation: 2,
                previous_holder: Some(1),
                previous_holder_released: false,
                activation_tick: 0,
            },
        );
        state.node_probe_ticks.insert(1, 1);
        let release = EffectIntent::capture(vip, 1, gates(), &state, None);
        state.node_probe_ticks.insert(1, 2);
        assert!(
            release.remains_valid(&state, gates()),
            "renewals must not starve an unchanged release RPC"
        );
    }
}
