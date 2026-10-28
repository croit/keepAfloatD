//! Exact completed bindings, separate from speculative kernel cleanup tracking.

use super::reconcile::intent::EffectIntent;
use crate::config::VipAddr;
use crate::consensus_freshness::{AcquirePermit, ConsensusFreshness, RetainPermit, RetentionStamp};
use crate::health::HealthContinuity;
use crate::raft::KafRaft;
use crate::raft::KafStorageState;
use crate::raft::admission::AdmissionContext;
use openraft::async_runtime::WatchReceiver;
use std::sync::Arc;
use tokio::sync::RwLock;

pub(super) struct EffectAuthorization {
    pub(super) permit: Arc<AcquirePermit>,
    pub(super) health: HealthContinuity,
    pub(super) intent: EffectIntent,
    pub(super) raft: KafRaft,
    pub(super) state: Arc<RwLock<KafStorageState>>,
}

impl EffectAuthorization {
    pub(super) fn check_live(&self) -> bool {
        self.permit.check()
            && self.health.check()
            && self
                .raft
                .metrics()
                .borrow_watched()
                .current_leader
                .is_some()
    }

    fn matches_state(&self, state: &KafStorageState) -> bool {
        self.intent.remains_valid(
            state,
            crate::bind_policy::BindGates {
                has_leader: true,
                local_healthy: true,
                consensus_fresh: true,
            },
        )
    }

    pub(super) async fn check_applied(&self) -> bool {
        if !self.check_live() {
            return false;
        }
        let state = self.state.read().await;
        self.check_live() && self.matches_state(&state)
    }

    fn check_observed_state(&self) -> bool {
        self.check_live()
            && self
                .state
                .try_read()
                .map_or(true, |state| self.matches_state(&state))
    }

    /// Operations must await `check_applied` after yielding before each new effect and finalization.
    /// Pending state reads must not prevent already started commands from polling their timeouts.
    pub(super) async fn run<F: std::future::Future>(&self, operation: F) -> Option<F::Output> {
        self.permit
            .run(self.health.run(async {
                if !self.check_applied().await {
                    return None;
                }
                tokio::pin!(operation);
                let checked = std::future::poll_fn(|cx| {
                    if !self.check_observed_state() {
                        return std::task::Poll::Ready(None);
                    }
                    std::future::Future::poll(operation.as_mut(), cx)
                        .map(|result| self.check_observed_state().then_some(result))
                });
                tokio::pin!(checked);
                let mut tick = tokio::time::interval(super::RECONCILE_TICK);
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tokio::select! {
                        biased;
                        _ = tick.tick() => { if !self.check_observed_state() { return None; } }
                        result = &mut checked => return result,
                    }
                }
            }))
            .await
            .flatten()
            .flatten()
    }
}

#[derive(Clone)]
pub(super) struct CompletedBindingWitness {
    pub(super) intent: EffectIntent,
    pub(super) health: HealthContinuity,
    iface: String,
    vip: VipAddr,
    context: AdmissionContext,
    stamp: RetentionStamp,
}

impl CompletedBindingWitness {
    pub(super) fn completed(
        vip: VipAddr,
        iface: &str,
        intent: EffectIntent,
        health: HealthContinuity,
        permit: &AcquirePermit,
    ) -> Option<Self> {
        if !permit.check() || !health.check() || !intent.bind_policy {
            return None;
        }
        let assignment = intent.assignment.as_ref()?;
        if assignment.holder != permit.context().local_replica.physical_id {
            return None;
        }
        Some(Self {
            intent: intent.activated(),
            health,
            iface: iface.to_owned(),
            vip,
            context: permit.context().clone(),
            stamp: permit.stamp(),
        })
    }

    pub(super) fn retain(
        &self,
        freshness: &Arc<ConsensusFreshness>,
        vip: &VipAddr,
        iface: &str,
    ) -> Option<RetainPermit> {
        if self.vip.addr != vip.addr
            || self.vip.prefix != vip.prefix
            || self.iface != iface
            || !self.health.check()
        {
            return None;
        }
        let permit = freshness.retain(&self.stamp)?;
        (permit.context() == &self.context && permit.check()).then_some(permit)
    }

    pub(super) fn matches_state(&self, state: &KafStorageState) -> bool {
        self.intent.remains_valid(
            state,
            crate::bind_policy::BindGates {
                has_leader: true,
                local_healthy: self.health.check(),
                consensus_fresh: true,
            },
        )
    }
}

/// What a completed binding's side effects run under: its renewed retention and activated
/// intent, not the acquisition permit and pre-bind intent the bind was checked against (#39).
pub(super) struct RetainedEffect {
    witness: CompletedBindingWitness,
    freshness: Arc<ConsensusFreshness>,
    state: Arc<RwLock<KafStorageState>>,
}

impl RetainedEffect {
    pub(super) fn new(
        witness: CompletedBindingWitness,
        freshness: Arc<ConsensusFreshness>,
        state: Arc<RwLock<KafStorageState>>,
    ) -> Self {
        Self {
            witness,
            freshness,
            state,
        }
    }

    fn retained(&self) -> Option<RetainPermit> {
        self.witness
            .retain(&self.freshness, &self.witness.vip, &self.witness.iface)
    }

    pub(super) fn check_live(&self) -> bool {
        self.retained().is_some()
    }

    fn still_held(&self) -> bool {
        self.check_live()
            && self
                .state
                .try_read()
                .map_or(true, |state| self.witness.matches_state(&state))
    }

    /// Waits for applied state while the binding stays retained, across renewals, and reports
    /// whether the binding still matches it.
    async fn ready(&self) -> bool {
        while let Some(permit) = self.retained() {
            let matches = permit
                .run(async { self.witness.matches_state(&*self.state.read().await) })
                .await;
            if let Some(matches) = matches {
                return matches;
            }
        }
        false
    }

    /// Starts `operation` once the binding is confirmed against applied state. A started
    /// operation is not cut off at a retention deadline: a notify script runs to its own timeout.
    pub(super) async fn start<F: std::future::Future>(&self, operation: F) -> Option<F::Output> {
        if !self.ready().await {
            return None;
        }
        Some(operation.await)
    }

    /// Runs `operation` only for as long as the binding stays held.
    pub(super) async fn run<F: std::future::Future>(&self, operation: F) -> Option<F::Output> {
        if !self.ready().await {
            return None;
        }
        tokio::pin!(operation);
        let mut tick = tokio::time::interval(super::RECONCILE_TICK);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                _ = tick.tick() => { if !self.still_held() { return None; } }
                result = &mut operation => return self.still_held().then_some(result),
            }
        }
    }
}
