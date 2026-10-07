use super::state::{
    FailoverSemantics, KafSnapshot, KafStorageState, OWNERSHIP_ACTIVATION_HOLDOFF_TICKS,
    VipAssignment,
};
use super::{KafLogStore, KafStateMachine};
use crate::config::{Config, VipAddr};
use crate::raft::admission::ReplicaId;
use crate::raft::probe::config_identity_compatible;
use crate::raft::store::vip_logic::{EligibilityInputs, is_node_eligible};
use crate::raft::types::test_replica;
use crate::raft::types::{KafRequest, KafSnapshotData, TypeConfig};
use futures::stream;
use openraft::alias::{EntryOf, LogIdOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf, VoteOf};
use openraft::entry::RaftEntry;
use openraft::storage::{
    IOFlushed, LogState, RaftLogReader, RaftLogStorage, RaftSnapshotBuilder, RaftStateMachine,
};
use openraft::testing::log_id;
use openraft::{BasicNode, EntryPayload, LogId, Membership, OptionalSend, SnapshotMeta, Vote};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, Cursor};
use std::net::{IpAddr, Ipv4Addr};
use std::ops::Bound;
use std::sync::Arc;
use tokio::sync::RwLock;

#[path = "ownerless_gap_tests.rs"]
mod ownerless_gap_tests;

#[path = "snapshot_tests.rs"]
mod snapshot_tests;

#[path = "admission_tests.rs"]
mod admission_tests;

#[tokio::test]
async fn physical_health_survives_legacy_membership_until_admission_genesis() {
    use crate::raft::admission::Genesis;
    for semantics in [FailoverSemantics::Legacy, FailoverSemantics::V2] {
        let mut store = storage(&[], 3);
        store.state.write().await.failover_semantics = semantics;
        store
            .apply_to_state_machine(&[health_entry(1, 1, true)])
            .await
            .unwrap();
        let (health, ticks) = {
            let state = store.state.read().await;
            (state.node_health.clone(), state.node_probe_ticks.clone())
        };
        store
            .apply_to_state_machine(&[membership_entry(2, &[1])])
            .await
            .unwrap();
        {
            let state = store.state.read().await;
            assert_eq!(state.node_health, health, "semantics={semantics:?}");
            assert_eq!(state.node_probe_ticks, ticks);
        }
        let genesis = Genesis {
            config: crate::config::ClusterConfigFingerprint {
                version: 1,
                digest: [8; 32],
            },
            epoch: 42,
            voters: [test_replica(1)].into(),
        };
        store
            .apply_to_state_machine(&[log_entry(
                3,
                EntryPayload::Normal(KafRequest::AdmissionGenesis(genesis)),
            )])
            .await
            .unwrap();
        let state = store.state.read().await;
        assert!(state.node_health.is_empty());
        assert!(state.node_probe_ticks.is_empty());
        assert!(state.node_recovery_tick.is_empty());
        assert!(state.applied_progress.is_empty());
    }
}

#[tokio::test]
async fn admitted_membership_clears_replaced_boot_but_preserves_unchanged_boot() {
    use crate::raft::admission::{Genesis, HealthProgress};
    let mut store = storage(&[], 3);
    let genesis = Genesis {
        config: crate::config::ClusterConfigFingerprint {
            version: 1,
            digest: [8; 32],
        },
        epoch: 42,
        voters: [test_replica(1), test_replica(2)].into(),
    };
    store
        .apply_to_state_machine(&[
            membership_entry(1, &[1, 2]),
            log_entry(
                2,
                EntryPayload::Normal(KafRequest::AdmissionGenesis(genesis.clone())),
            ),
        ])
        .await
        .unwrap();
    for id in [1, 2] {
        store
            .apply_to_state_machine(&[log_entry(
                2 + id,
                EntryPayload::Normal(KafRequest::HealthProgress(HealthProgress {
                    node_id: id,
                    healthy: Some(true),
                    replica: test_replica(id),
                    epoch: genesis.epoch,
                    request_nonce: [id as u8; 32],
                    genesis: genesis.clone(),
                })),
            )])
            .await
            .unwrap();
    }
    let (health, ticks, progress) = {
        let state = store.state.read().await;
        (
            state.node_health.clone(),
            state.node_probe_ticks.clone(),
            state.applied_progress.clone(),
        )
    };
    let replacement = ReplicaId {
        physical_id: 2,
        boot_nonce: [99; 32],
    };
    let voters = BTreeSet::from([test_replica(1), replacement]);
    store
        .apply_to_state_machine(&[log_entry(
            5,
            EntryPayload::Membership(Membership::new_with_defaults(vec![voters.clone()], voters)),
        )])
        .await
        .unwrap();
    let state = store.state.read().await;
    assert_eq!(state.node_health.get(&1), health.get(&1));
    assert_eq!(state.node_probe_ticks.get(&1), ticks.get(&1));
    assert_eq!(state.applied_progress.get(&1), progress.get(&1));
    assert!(!state.node_health.contains_key(&2));
    assert!(!state.node_probe_ticks.contains_key(&2));
    assert!(!state.node_recovery_tick.contains_key(&2));
    assert!(!state.applied_progress.contains_key(&2));
}

#[tokio::test]
async fn healthy_learner_never_receives_vip_in_either_semantics() {
    let vip = ip4(192, 0, 2, 40);
    for semantics in [FailoverSemantics::Legacy, FailoverSemantics::V2] {
        let mut store = storage(&[vip], 3);
        store.state.write().await.failover_semantics = semantics;
        let membership = Membership::new_with_defaults(
            vec![BTreeSet::from([test_replica(2)])],
            [1, 2].map(test_replica),
        );
        store
            .apply_to_state_machine(&[
                log_entry(1, EntryPayload::Membership(membership)),
                health_entry(2, 1, true),
                health_entry(3, 2, true),
            ])
            .await
            .unwrap();
        assert_eq!(
            store.state.read().await.vip_assignments[&vip].holder,
            2,
            "learner received VIP under {semantics:?}"
        );
    }
}

#[tokio::test]
async fn applied_progress_requires_genesis_and_preserves_exact_challenge() {
    use crate::raft::admission::{Genesis, HealthProgress};
    let replica = test_replica(1);
    let genesis = Genesis {
        config: crate::config::ClusterConfigFingerprint {
            version: 1,
            digest: [8; 32],
        },
        epoch: u128::MAX,
        voters: BTreeSet::from([replica]),
    };
    let progress = HealthProgress {
        node_id: 1,
        healthy: Some(true),
        replica,
        epoch: u128::MAX,
        request_nonce: [9; 32],
        genesis: genesis.clone(),
    };
    let mut store = storage(&[], 3);
    store
        .apply_to_state_machine(&[log_entry(
            1,
            EntryPayload::Normal(KafRequest::HealthProgress(progress.clone())),
        )])
        .await
        .unwrap();
    assert!(store.state.read().await.node_health.is_empty());
    store
        .apply_to_state_machine(&[
            membership_entry(2, &[1]),
            log_entry(
                2,
                EntryPayload::Normal(KafRequest::AdmissionGenesis(genesis.clone())),
            ),
            log_entry(
                3,
                EntryPayload::Normal(KafRequest::HealthProgress(progress.clone())),
            ),
        ])
        .await
        .unwrap();
    let state = store.state.read().await;
    assert_eq!(state.genesis.as_ref(), Some(&genesis));
    assert_eq!(state.applied_progress.get(&1).unwrap().request, progress);
    assert_eq!(state.applied_progress.get(&1).unwrap().log_id, lid(1, 3));
    assert_eq!(state.node_health.get(&1), Some(&true));
    drop(state);
    let mut foreign = genesis;
    foreign.epoch -= 1;
    store
        .apply_to_state_machine(&[log_entry(
            4,
            EntryPayload::Normal(KafRequest::AdmissionGenesis(foreign)),
        )])
        .await
        .unwrap();
    assert_eq!(
        store.state.read().await.genesis.as_ref().unwrap().epoch,
        u128::MAX
    );
}

/// Combined test handle over one shared in-memory state, exposing the openraft-0.9-shaped method
/// names the migrated tests still use. It forwards each call to the matching 0.10 trait method on
/// the log or state-machine half, so the bug-1..6 regression tests read the same as before the
/// trait split (only the per-entry apply now goes through a stream, hidden behind
/// `apply_to_state_machine`).
struct TestStore {
    log: KafLogStore,
    sm: KafStateMachine,
    state: Arc<RwLock<KafStorageState>>,
}

impl TestStore {
    /// Drive the real `RaftStateMachine::apply` stream from a borrowed slice of entries, with no
    /// client responders attached (the follower-apply shape these regression tests model). The 0.9
    /// `apply_to_state_machine` returned `Vec<KafResponse>`; every caller discarded it, so this
    /// returns `()`.
    async fn apply_to_state_machine(&mut self, entries: &[EntryOf<TypeConfig>]) -> io::Result<()> {
        let items = entries
            .iter()
            .cloned()
            .map(|e| Ok::<_, io::Error>((e, None)));
        self.sm.apply(stream::iter(items)).await
    }

    async fn append_to_log<I>(&mut self, entries: I) -> io::Result<()>
    where
        I: IntoIterator<Item = EntryOf<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        self.log.append(entries, IOFlushed::noop()).await
    }

    async fn get_log_state(&mut self) -> io::Result<LogState<TypeConfig>> {
        self.log.get_log_state().await
    }

    async fn get_log_reader(&mut self) -> KafLogStore {
        self.log.get_log_reader().await
    }

    async fn read_committed(&mut self) -> io::Result<Option<LogIdOf<TypeConfig>>> {
        self.log.read_committed().await
    }

    async fn save_committed(&mut self, c: Option<LogIdOf<TypeConfig>>) -> io::Result<()> {
        self.log.save_committed(c).await
    }

    async fn save_vote(&mut self, v: &VoteOf<TypeConfig>) -> io::Result<()> {
        self.log.save_vote(v).await
    }

    async fn read_vote(&mut self) -> io::Result<Option<VoteOf<TypeConfig>>> {
        self.log.read_vote().await
    }

    /// 0.9 `purge_logs_upto(log_id)` removes `..=log_id.index` - maps directly to 0.10 `purge`.
    async fn purge_logs_upto(&mut self, log_id: LogIdOf<TypeConfig>) -> io::Result<()> {
        self.log.purge(log_id).await
    }

    /// 0.9 `delete_conflict_logs_since(log_id)` removes `log_id.index..` (inclusive of the id).
    /// 0.10 `truncate_after(Some(L))` removes `L.index()+1..` (exclusive). So a conflict-since at
    /// index `X` is `truncate_after(Some(X-1))` for `X >= 1`, and `truncate_after(None)` at `X == 0`
    /// (truncate the whole log). This preserves the exact removal set and the purge-floor-lowering
    /// behaviour the bug-3/4 reform regression pins.
    async fn delete_conflict_logs_since(&mut self, log_id: LogIdOf<TypeConfig>) -> io::Result<()> {
        let prev = log_id
            .index()
            .checked_sub(1)
            .map(|i| LogId::new(*log_id.committed_leader_id(), i));
        self.log.truncate_after(prev).await
    }

    async fn last_applied_state(
        &mut self,
    ) -> io::Result<(Option<LogIdOf<TypeConfig>>, StoredMembershipOf<TypeConfig>)> {
        self.sm.applied_state().await
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMetaOf<TypeConfig>,
        snapshot: Cursor<Vec<u8>>,
    ) -> io::Result<()> {
        self.sm.install_snapshot(meta, snapshot).await
    }

    async fn get_snapshot_builder(&mut self) -> KafStateMachine {
        self.sm.get_snapshot_builder().await
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> io::Result<Option<SnapshotOf<TypeConfig, KafSnapshotData>>> {
        self.sm.get_current_snapshot().await
    }
}

fn ip4(a: u8, b: u8, c: u8, d: u8) -> IpAddr {
    IpAddr::V4(Ipv4Addr::new(a, b, c, d))
}

/// Build a `LogId` at the given term/index via openraft's test helper (the 0.10
/// `CommittedLeaderId` is not a public root type; this constructs it through the type config).
fn lid(term: u64, index: u64) -> LogIdOf<TypeConfig> {
    log_id::<TypeConfig>(term, test_replica(0), index)
}

fn log_entry(
    index: u64,
    payload: EntryPayload<KafRequest, ReplicaId, BasicNode>,
) -> EntryOf<TypeConfig> {
    EntryOf::<TypeConfig>::new(lid(1, index), payload)
}

fn health_entry_term(term: u64, index: u64, node_id: u64, healthy: bool) -> EntryOf<TypeConfig> {
    EntryOf::<TypeConfig>::new_normal(
        lid(term, index),
        KafRequest::HealthUpdate { node_id, healthy },
    )
}

fn membership_entry(index: u64, voters: &[u64]) -> EntryOf<TypeConfig> {
    let set: BTreeSet<ReplicaId> = voters.iter().copied().map(test_replica).collect();
    // `Membership::new` rejects an empty voter config in 0.10 (`ensure_valid`); these tests
    // deliberately exercise the no-voters case (membership_without_voters_clears_assignments), so
    // use `new_with_defaults`, which builds the membership without that validation - matching the
    // 0.9 behaviour the regression suite relies on.
    EntryOf::<TypeConfig>::new_membership(
        lid(1, index),
        Membership::new_with_defaults(vec![set], voters.iter().copied().map(test_replica)),
    )
}

fn health_entry(index: u64, node_id: u64, healthy: bool) -> EntryOf<TypeConfig> {
    EntryOf::<TypeConfig>::new_normal(lid(1, index), KafRequest::HealthUpdate { node_id, healthy })
}

fn release_entry(index: u64, node_id: u64, vip: IpAddr, generation: u64) -> EntryOf<TypeConfig> {
    EntryOf::<TypeConfig>::new_normal(
        lid(1, index),
        KafRequest::VipReleased {
            node_id,
            vip,
            generation,
        },
    )
}

fn storage(vips: &[IpAddr], stale_missed_probes: u64) -> TestStore {
    storage_failback(vips, stale_missed_probes, true)
}

fn storage_failback(vips: &[IpAddr], stale_missed_probes: u64, failback: bool) -> TestStore {
    storage_failback_delay(vips, stale_missed_probes, failback, 0)
}

fn storage_failback_delay(
    vips: &[IpAddr],
    stale_missed_probes: u64,
    failback: bool,
    failback_delay_ticks: u64,
) -> TestStore {
    let vip_list: Arc<Vec<(VipAddr, String)>> = Arc::new(
        vips.iter()
            .map(|&a| (VipAddr::host(a), "lo".to_string()))
            .collect(),
    );
    let (log, sm, state) = super::new_store(
        vip_list,
        stale_missed_probes,
        failback,
        failback_delay_ticks,
    );
    TestStore { log, sm, state }
}

async fn enable_v2(storage: &TestStore) {
    storage.state.write().await.failover_semantics = FailoverSemantics::V2;
}

async fn assignment_of(storage: &TestStore, vip: IpAddr) -> Option<VipAssignment> {
    storage
        .state
        .read()
        .await
        .vip_assignments
        .get(&vip)
        .cloned()
}

#[tokio::test]
async fn health_updates_advance_frontier_and_assign_only_eligible_holders() {
    let v1 = ip4(10, 0, 0, 1);
    let v2 = ip4(10, 0, 0, 2);
    let mut s = storage(&[v1, v2], 3);

    // Two voters, both healthy.
    s.apply_to_state_machine(&[membership_entry(1, &[1, 2])])
        .await
        .unwrap();
    s.apply_to_state_machine(&[health_entry(2, 1, true)])
        .await
        .unwrap();
    s.apply_to_state_machine(&[health_entry(3, 2, true)])
        .await
        .unwrap();

    let st = s.state.read().await;
    // Frontier advanced and is the max of the per-node committed ticks.
    let max_tick = st.node_probe_ticks.values().copied().max().unwrap_or(0);
    assert_eq!(st.latest_probe_tick, max_tick);
    assert!(st.node_probe_ticks.contains_key(&1) && st.node_probe_ticks.contains_key(&2));

    // Both VIPs are assigned, and every assigned holder is actually eligible.
    assert_eq!(st.vip_assignments.len(), 2);
    for (vip, a) in &st.vip_assignments {
        assert!(
            is_node_eligible(
                a.holder,
                &EligibilityInputs {
                    node_health: &st.node_health,
                    node_probe_ticks: &st.node_probe_ticks,
                    latest_probe_tick: st.latest_probe_tick,
                    stale_missed_probes: st.stale_missed_probes,
                    failback_delay_ticks: st.failback_delay_ticks,
                    node_recovery_tick: &st.node_recovery_tick,
                },
                &st.node_failback_blocked,
            ),
            "holder {} of {vip} must be eligible",
            a.holder
        );
    }
}

#[tokio::test]
async fn initial_unhealthy_nodes_join_balanced_placement_without_failback() {
    let v1 = ip4(10, 0, 0, 1);
    let v2 = ip4(10, 0, 0, 2);
    let mut s = storage_failback_delay(&[v1, v2], 3, false, 10);
    enable_v2(&s).await;

    s.apply_to_state_machine(&[membership_entry(1, &[1, 2, 3])])
        .await
        .unwrap();
    s.apply_to_state_machine(&[health_entry(2, 1, true)])
        .await
        .unwrap();
    s.apply_to_state_machine(&[health_entry(3, 2, false)])
        .await
        .unwrap();
    s.apply_to_state_machine(&[health_entry(4, 3, false)])
        .await
        .unwrap();
    s.apply_to_state_machine(&[health_entry(5, 2, true)])
        .await
        .unwrap();
    s.apply_to_state_machine(&[health_entry(6, 3, true)])
        .await
        .unwrap();

    let st = s.state.read().await;
    assert!(
        st.node_nopreempt.is_empty(),
        "an initial unhealthy report must not mark a starting node nopreempt"
    );
    let holders: BTreeSet<u64> = st.vip_assignments.values().map(|a| a.holder).collect();
    assert_eq!(
        holders.len(),
        2,
        "two VIPs must have distinct holders after the starting nodes recover"
    );
}

/// Legacy-semantics twin of the regression above: the guards in the Legacy apply path must keep
/// a node whose first report is unhealthy out of the permanent block.
#[tokio::test]
async fn initial_unhealthy_nodes_join_balanced_placement_without_failback_legacy() {
    let v1 = ip4(10, 0, 0, 1);
    let v2 = ip4(10, 0, 0, 2);
    let mut s = storage_failback_delay(&[v1, v2], 3, false, 10);

    s.apply_to_state_machine(&[
        membership_entry(1, &[1, 2, 3]),
        health_entry(2, 1, true),
        health_entry(3, 2, false),
        health_entry(4, 3, false),
        health_entry(5, 2, true),
        health_entry(6, 3, true),
    ])
    .await
    .unwrap();

    let st = s.state.read().await;
    assert_eq!(st.failover_semantics, FailoverSemantics::Legacy);
    assert!(
        st.node_failback_blocked.is_empty(),
        "a first unhealthy report must not permanently block a starting node"
    );
    let holders: BTreeSet<u64> = st.vip_assignments.values().map(|a| a.holder).collect();
    assert_eq!(
        holders.len(),
        2,
        "two VIPs must have distinct holders after the starting nodes recover"
    );
}

/// Legacy failback timers start only on an observed unhealthy-to-healthy transition; a first
/// healthy report has nothing to recover from.
#[tokio::test]
async fn legacy_failback_recovery_timer_needs_a_prior_unhealthy_report() {
    let vip = ip4(10, 0, 0, 1);
    let mut s = storage_failback_delay(&[vip], 3, true, 2);

    s.apply_to_state_machine(&[membership_entry(1, &[1, 2]), health_entry(2, 1, true)])
        .await
        .unwrap();
    assert!(
        s.state.read().await.node_recovery_tick.is_empty(),
        "a first healthy report must not start a recovery timer"
    );

    s.apply_to_state_machine(&[health_entry(3, 1, false), health_entry(4, 1, true)])
        .await
        .unwrap();
    let st = s.state.read().await;
    assert_eq!(
        st.node_recovery_tick.get(&1).copied(),
        Some(st.latest_probe_tick),
        "recovery after an observed failure must start the failback timer"
    );
}

#[tokio::test]
async fn nopreempt_recovered_node_takes_orphan_when_replacement_fails() {
    let vip = ip4(10, 0, 0, 1);
    let mut s = storage_failback(&[vip], 3, false);
    enable_v2(&s).await;

    s.apply_to_state_machine(&[membership_entry(1, &[1, 2])])
        .await
        .unwrap();
    s.apply_to_state_machine(&[health_entry(2, 1, true)])
        .await
        .unwrap();
    s.apply_to_state_machine(&[health_entry(3, 2, true)])
        .await
        .unwrap();
    assert_eq!(assignment_of(&s, vip).await.unwrap().holder, 1);

    s.apply_to_state_machine(&[health_entry(4, 1, false)])
        .await
        .unwrap();
    assert_eq!(assignment_of(&s, vip).await.unwrap().holder, 2);
    s.apply_to_state_machine(&[health_entry(5, 1, true)])
        .await
        .unwrap();
    assert_eq!(assignment_of(&s, vip).await.unwrap().holder, 2);
    assert!(s.state.read().await.node_nopreempt.contains(&1));

    s.apply_to_state_machine(&[health_entry(6, 2, false)])
        .await
        .unwrap();
    assert_eq!(
        assignment_of(&s, vip).await.unwrap().holder,
        1,
        "the recovered nopreempt node must remain available for orphan failover"
    );
}

#[tokio::test]
async fn idle_node_failure_does_not_create_nopreempt_history() {
    let vip = ip4(10, 0, 0, 1);
    let mut s = storage_failback(&[vip], 3, false);
    enable_v2(&s).await;

    s.apply_to_state_machine(&[membership_entry(1, &[1, 2])])
        .await
        .unwrap();
    s.apply_to_state_machine(&[health_entry(2, 1, true)])
        .await
        .unwrap();
    s.apply_to_state_machine(&[health_entry(3, 2, true)])
        .await
        .unwrap();
    assert_eq!(assignment_of(&s, vip).await.unwrap().holder, 1);

    s.apply_to_state_machine(&[health_entry(4, 2, false)])
        .await
        .unwrap();
    assert!(
        !s.state.read().await.node_nopreempt.contains(&2),
        "a node that did not lose a VIP must not be suppressed from future balancing"
    );
}

#[tokio::test]
async fn silent_holder_recovery_observes_failback_delay() {
    let v1 = ip4(10, 0, 0, 1);
    let v2 = ip4(10, 0, 0, 2);
    let mut s = storage_failback_delay(&[v1, v2], 2, true, 3);
    enable_v2(&s).await;

    s.apply_to_state_machine(&[membership_entry(1, &[1, 2])])
        .await
        .unwrap();
    s.apply_to_state_machine(&[health_entry(2, 1, true)])
        .await
        .unwrap();
    s.apply_to_state_machine(&[health_entry(3, 2, true)])
        .await
        .unwrap();

    for index in 4..=6 {
        s.apply_to_state_machine(&[health_entry(index, 2, true)])
            .await
            .unwrap();
    }
    assert!(
        s.state
            .read()
            .await
            .vip_assignments
            .values()
            .all(|assignment| assignment.holder == 2),
        "the stale holder must lose its VIP"
    );

    s.apply_to_state_machine(&[health_entry(7, 1, true)])
        .await
        .unwrap();
    assert!(s.state.read().await.node_recovery_pending.contains(&1));
    assert!(
        s.state
            .read()
            .await
            .vip_assignments
            .values()
            .all(|assignment| assignment.holder == 2),
        "the silently recovered node must not rebalance before its delay"
    );

    let mut index = 8;
    for _ in 0..3 {
        s.apply_to_state_machine(&[health_entry(index, 2, true)])
            .await
            .unwrap();
        index += 1;
        s.apply_to_state_machine(&[health_entry(index, 1, true)])
            .await
            .unwrap();
        index += 1;
    }
    let holders: BTreeSet<u64> = s
        .state
        .read()
        .await
        .vip_assignments
        .values()
        .map(|assignment| assignment.holder)
        .collect();
    assert_eq!(holders, BTreeSet::from([1, 2]));
    assert!(!s.state.read().await.node_recovery_pending.contains(&1));
}

#[tokio::test]
async fn silently_recovered_nopreempt_node_only_receives_orphans() {
    let v1 = ip4(10, 0, 0, 1);
    let v2 = ip4(10, 0, 0, 2);
    let mut s = storage_failback(&[v1, v2], 2, false);
    enable_v2(&s).await;

    s.apply_to_state_machine(&[membership_entry(1, &[1, 2])])
        .await
        .unwrap();
    s.apply_to_state_machine(&[health_entry(2, 1, true)])
        .await
        .unwrap();
    s.apply_to_state_machine(&[health_entry(3, 2, true)])
        .await
        .unwrap();
    for index in 4..=6 {
        s.apply_to_state_machine(&[health_entry(index, 2, true)])
            .await
            .unwrap();
    }
    s.apply_to_state_machine(&[health_entry(7, 1, true)])
        .await
        .unwrap();
    assert!(s.state.read().await.node_nopreempt.contains(&1));
    assert!(
        s.state
            .read()
            .await
            .vip_assignments
            .values()
            .all(|assignment| assignment.holder == 2),
        "nopreempt return must not proactively rebalance"
    );

    s.apply_to_state_machine(&[health_entry(8, 2, false)])
        .await
        .unwrap();
    assert!(
        s.state
            .read()
            .await
            .vip_assignments
            .values()
            .all(|assignment| assignment.holder == 1),
        "nopreempt node must take every orphan when it is the healthy survivor"
    );
}

#[tokio::test]
async fn explicit_failure_oscillation_restarts_v2_recovery_delay() {
    let v1 = ip4(10, 0, 0, 1);
    let v2 = ip4(10, 0, 0, 2);
    let mut s = storage_failback_delay(&[v1, v2], 3, true, 3);
    enable_v2(&s).await;

    s.apply_to_state_machine(&[membership_entry(1, &[1, 2])])
        .await
        .unwrap();
    s.apply_to_state_machine(&[health_entry(2, 1, true)])
        .await
        .unwrap();
    s.apply_to_state_machine(&[health_entry(3, 2, true)])
        .await
        .unwrap();
    s.apply_to_state_machine(&[health_entry(4, 1, false)])
        .await
        .unwrap();
    s.apply_to_state_machine(&[health_entry(5, 1, true)])
        .await
        .unwrap();
    let first_recovery = s.state.read().await.node_recovery_tick[&1];

    s.apply_to_state_machine(&[health_entry(6, 2, true)])
        .await
        .unwrap();
    s.apply_to_state_machine(&[health_entry(7, 1, true)])
        .await
        .unwrap();
    s.apply_to_state_machine(&[health_entry(8, 1, false)])
        .await
        .unwrap();
    assert!(!s.state.read().await.node_recovery_tick.contains_key(&1));
    s.apply_to_state_machine(&[health_entry(9, 1, true)])
        .await
        .unwrap();
    let second_recovery = s.state.read().await.node_recovery_tick[&1];
    assert!(second_recovery > first_recovery);

    let mut index = 10;
    for _ in 0..2 {
        s.apply_to_state_machine(&[health_entry(index, 2, true)])
            .await
            .unwrap();
        index += 1;
        s.apply_to_state_machine(&[health_entry(index, 1, true)])
            .await
            .unwrap();
        index += 1;
    }
    assert!(s.state.read().await.node_recovery_pending.contains(&1));
    assert!(
        s.state
            .read()
            .await
            .vip_assignments
            .values()
            .all(|assignment| assignment.holder == 2)
    );
}

#[tokio::test]
async fn membership_without_voters_clears_assignments() {
    let v1 = ip4(10, 0, 0, 1);
    let mut s = storage(&[v1], 3);
    s.apply_to_state_machine(&[membership_entry(1, &[1])])
        .await
        .unwrap();
    s.apply_to_state_machine(&[health_entry(2, 1, true)])
        .await
        .unwrap();
    assert!(assignment_of(&s, v1).await.is_some());

    // A committed membership with no voters leaves nobody eligible.
    s.apply_to_state_machine(&[membership_entry(3, &[])])
        .await
        .unwrap();
    assert!(s.state.read().await.vip_assignments.is_empty());
}

#[tokio::test]
async fn vip_released_ack_requires_matching_generation_and_previous_holder() {
    let vip = ip4(10, 0, 0, 1);
    let mut s = storage(&[vip], 3);

    // Form a 2-voter cluster, both healthy: the single VIP lands on the lowest eligible id.
    s.apply_to_state_machine(&[membership_entry(1, &[1, 2])])
        .await
        .unwrap();
    s.apply_to_state_machine(&[health_entry(2, 1, true)])
        .await
        .unwrap();
    s.apply_to_state_machine(&[health_entry(3, 2, true)])
        .await
        .unwrap();
    let initial = assignment_of(&s, vip).await.unwrap();

    // Advance the frontier with node 2 only until node 1 falls outside the staleness window
    // and the VIP hands off (generation bumps, previous_holder recorded).
    let mut idx = 4;
    for _ in 0..6 {
        s.apply_to_state_machine(&[health_entry(idx, 2, true)])
            .await
            .unwrap();
        idx += 1;
    }
    let handed_off = assignment_of(&s, vip).await.unwrap();
    assert_ne!(handed_off.holder, initial.holder, "VIP should have moved");
    let prev = handed_off
        .previous_holder
        .expect("previous holder recorded");
    assert_eq!(prev, initial.holder);
    assert!(!handed_off.previous_holder_released);
    let generation = handed_off.generation;

    // Wrong generation: ignored.
    s.apply_to_state_machine(&[release_entry(idx, prev, vip, generation.saturating_sub(1))])
        .await
        .unwrap();
    idx += 1;
    assert!(
        !assignment_of(&s, vip)
            .await
            .unwrap()
            .previous_holder_released
    );

    // Right generation but wrong node: ignored.
    s.apply_to_state_machine(&[release_entry(idx, handed_off.holder, vip, generation)])
        .await
        .unwrap();
    idx += 1;
    assert!(
        !assignment_of(&s, vip)
            .await
            .unwrap()
            .previous_holder_released
    );

    // Correct previous holder + generation: the release ack lands.
    s.apply_to_state_machine(&[release_entry(idx, prev, vip, generation)])
        .await
        .unwrap();
    assert!(
        assignment_of(&s, vip)
            .await
            .unwrap()
            .previous_holder_released
    );
}

#[tokio::test]
async fn storage_trait_log_vote_and_snapshot_roundtrip() {
    let v1 = ip4(10, 0, 0, 1);
    let mut s = storage(&[v1], 3);

    // Append to the log, then read it back via the log reader + log state.
    s.append_to_log([membership_entry(1, &[1, 2]), health_entry(2, 1, true)])
        .await
        .unwrap();
    let ls = s.get_log_state().await.unwrap();
    assert_eq!(ls.last_log_id.map(|l| l.index()), Some(2));
    let mut reader = s.get_log_reader().await;
    assert_eq!(reader.try_get_log_entries(1..=2).await.unwrap().len(), 2);

    // Vote persistence.
    let vote = Vote::new(3, test_replica(1));
    s.save_vote(&vote).await.unwrap();
    assert_eq!(s.read_vote().await.unwrap(), Some(vote));

    // Apply entries so there is committed state to snapshot.
    s.apply_to_state_machine(&[
        membership_entry(3, &[1, 2]),
        health_entry(4, 1, true),
        health_entry(5, 2, true),
    ])
    .await
    .unwrap();
    let (applied, _membership) = s.last_applied_state().await.unwrap();
    assert!(applied.is_some());

    // Build a snapshot and confirm get_current_snapshot also produces one.
    let mut builder = s.get_snapshot_builder().await;
    let snap = builder.build_snapshot().await.unwrap();
    assert!(s.get_current_snapshot().await.unwrap().is_some());

    // Install that snapshot into a fresh store and verify the committed state transfers.
    let mut restored = storage(&[v1], 3);
    let bytes = snap.snapshot.into_inner();
    restored
        .install_snapshot(&snap.meta, Cursor::new(bytes))
        .await
        .unwrap();
    {
        let st = restored.state.read().await;
        assert_eq!(st.last_applied_log.map(|l| l.index()), Some(5));
        assert!(!st.vip_assignments.is_empty());
        // Post-install the store must satisfy last_purged <= last_applied <= last_log_id
        // (openraft's storage invariant). Pin it so the snapshot-install regression stays
        // covered even though this fresh store had no log to trim.
        let purged = st.last_purged_log_id.map(|l| l.index());
        let applied = st.last_applied_log.map(|l| l.index());
        let last_log = st.log.keys().next_back().copied();
        assert!(
            purged <= applied,
            "purged {purged:?} must be <= applied {applied:?}"
        );
        if let (Some(p), Some(ll)) = (purged, last_log) {
            assert!(p <= ll, "purged {p} must be <= last_log {ll}");
        }
    }

    // Log compaction entry points run without error.
    s.delete_conflict_logs_since(lid(1, 5)).await.unwrap();
    s.purge_logs_upto(lid(1, 1)).await.unwrap();
}

/// Build a snapshot whose `last_applied` index is `n` by applying a membership entry plus
/// `n - 1` health updates to a throwaway source store, then returning the serialized snapshot
/// bytes and meta. Used by the install-snapshot consistency tests below.
async fn snapshot_covering(vips: &[IpAddr], n: u64) -> (SnapshotMetaOf<TypeConfig>, Vec<u8>) {
    let mut src = storage(vips, 3);
    src.apply_to_state_machine(&[membership_entry(1, &[1, 2])])
        .await
        .unwrap();
    for i in 2..=n {
        src.apply_to_state_machine(&[health_entry(i, 1, true)])
            .await
            .unwrap();
    }
    let mut builder = src.get_snapshot_builder().await;
    let snap = builder.build_snapshot().await.unwrap();
    let meta = snap.meta.clone();
    let bytes = snap.snapshot.into_inner();
    (meta, bytes)
}

/// Installing a snapshot covering indices up to N must purge every local log entry <= N and
/// advance `last_purged_log_id` to N. Without this the store reports a contiguous log starting
/// at index 1 while openraft believes it is compacted to N; the next `get_log_entries` then
/// trips `Defensive(LogIndexNotFound)` and kills RaftCore.
#[tokio::test]
async fn install_snapshot_purges_covered_log_and_sets_purged_id() {
    let v1 = ip4(10, 0, 0, 1);
    let mut restored = storage(&[v1], 3);

    // Seed a lagging follower's stale low-index prefix.
    restored
        .append_to_log([membership_entry(1, &[1, 2]), health_entry(2, 1, true)])
        .await
        .unwrap();

    let (meta, bytes) = snapshot_covering(&[v1], 5).await;
    restored
        .install_snapshot(&meta, Cursor::new(bytes))
        .await
        .unwrap();

    let st = restored.state.read().await;
    assert_eq!(
        st.last_purged_log_id.map(|l| l.index()),
        Some(5),
        "last_purged_log_id must advance to the snapshot index"
    );
    assert!(
        st.log.keys().all(|&k| k > 5),
        "no log entry at or below the snapshot index may survive, got keys {:?}",
        st.log.keys().collect::<Vec<_>>()
    );
}

/// Log entries strictly above the snapshot index are a valid committed suffix and must survive.
#[tokio::test]
async fn install_snapshot_keeps_log_suffix_above_index() {
    let v1 = ip4(10, 0, 0, 1);
    let mut restored = storage(&[v1], 3);

    // Seed indices 1..=7; snapshot will cover up to 5, so 6 and 7 must remain.
    restored
        .append_to_log([
            membership_entry(1, &[1, 2]),
            health_entry(2, 1, true),
            health_entry(3, 1, true),
            health_entry(4, 1, true),
            health_entry(5, 1, true),
            health_entry(6, 1, true),
            health_entry(7, 1, true),
        ])
        .await
        .unwrap();

    let (meta, bytes) = snapshot_covering(&[v1], 5).await;
    restored
        .install_snapshot(&meta, Cursor::new(bytes))
        .await
        .unwrap();

    let st = restored.state.read().await;
    assert_eq!(st.last_purged_log_id.map(|l| l.index()), Some(5));
    assert!(st.log.contains_key(&6) && st.log.contains_key(&7));
    assert!(st.log.keys().all(|&k| k > 5));
}

/// `last_purged_log_id` is monotonic: installing an older snapshot must not move it backwards.
#[tokio::test]
async fn install_snapshot_does_not_move_purge_backwards() {
    let v1 = ip4(10, 0, 0, 1);
    let mut restored = storage(&[v1], 3);

    // Purge to a higher index than the stale snapshot will carry.
    restored.purge_logs_upto(lid(1, 9)).await.unwrap();

    let (meta, bytes) = snapshot_covering(&[v1], 5).await;
    restored
        .install_snapshot(&meta, Cursor::new(bytes))
        .await
        .unwrap();

    let st = restored.state.read().await;
    assert_eq!(
        st.last_purged_log_id.map(|l| l.index()),
        Some(9),
        "an older snapshot must not lower the purge point"
    );
}

/// Equal snapshot/log indices are not interchangeable when their leader identities differ.
/// Installing an older-history snapshot at the existing purge index must preserve the exact
/// local purge `LogId`, not merely its numeric index.
#[tokio::test]
async fn install_snapshot_preserves_purge_log_id_at_equal_index() {
    let v1 = ip4(10, 0, 0, 1);
    let mut restored = storage(&[v1], 3);

    restored.purge_logs_upto(lid(9, 5)).await.unwrap();
    let (meta, bytes) = snapshot_covering(&[v1], 5).await;
    restored
        .install_snapshot(&meta, Cursor::new(bytes))
        .await
        .unwrap();

    assert_eq!(
        restored.state.read().await.last_purged_log_id,
        Some(lid(9, 5)),
        "an equal-index snapshot must not replace the existing purge LogId"
    );
}

/// A snapshot with no `last_applied` (empty/initial) leaves the log and purge point untouched.
#[tokio::test]
async fn install_snapshot_empty_last_applied_is_noop_on_log() {
    let v1 = ip4(10, 0, 0, 1);
    let mut restored = storage(&[v1], 3);
    restored
        .append_to_log([membership_entry(1, &[1, 2]), health_entry(2, 1, true)])
        .await
        .unwrap();

    // Hand-craft an empty snapshot (last_applied = None).
    let snap = KafSnapshot::default();
    let bytes = serde_json::to_vec(&snap).unwrap();
    let meta = SnapshotMeta {
        last_log_id: None,
        last_membership: StoredMembershipOf::<TypeConfig>::default(),
        snapshot_id: "empty".to_string(),
    };
    restored
        .install_snapshot(&meta, Cursor::new(bytes))
        .await
        .unwrap();

    let st = restored.state.read().await;
    assert_eq!(st.last_purged_log_id, None);
    assert!(st.log.contains_key(&1) && st.log.contains_key(&2));
}

/// Two snapshots whose last-applied entry shares an index but was proposed in different terms
/// must get distinct `snapshot_id`s - openraft uses the id for snapshot identity/de-dup, so an
/// index-only id would let a stale snapshot masquerade as an already-installed newer one.
#[tokio::test]
async fn snapshot_id_distinguishes_same_index_different_term() {
    let v1 = ip4(10, 0, 0, 1);

    let mut a = storage(&[v1], 3);
    a.apply_to_state_machine(&[membership_entry(1, &[1])])
        .await
        .unwrap();
    a.apply_to_state_machine(&[health_entry_term(2, 2, 1, true)])
        .await
        .unwrap();
    let id_a = a
        .get_snapshot_builder()
        .await
        .build_snapshot()
        .await
        .unwrap()
        .meta
        .snapshot_id;

    let mut b = storage(&[v1], 3);
    b.apply_to_state_machine(&[membership_entry(1, &[1])])
        .await
        .unwrap();
    // Same index (2) but proposed in term 5 instead of term 2.
    b.apply_to_state_machine(&[health_entry_term(5, 2, 1, true)])
        .await
        .unwrap();
    let id_b = b
        .get_snapshot_builder()
        .await
        .build_snapshot()
        .await
        .unwrap()
        .meta
        .snapshot_id;

    assert_ne!(
        id_a, id_b,
        "snapshots at the same index in different terms must have distinct ids"
    );
}

/// Appending an entry at or below the purge boundary must not re-create a hole below
/// `last_purged_log_id`. openraft never legitimately appends there; guard so a stray entry
/// cannot resurrect the compacted prefix and desync the log view.
#[tokio::test]
async fn append_to_log_ignores_entries_at_or_below_purge_point() {
    let v1 = ip4(10, 0, 0, 1);
    let mut s = storage(&[v1], 3);

    s.purge_logs_upto(lid(1, 5)).await.unwrap();

    // Indices 3 and 5 are at/below the purge point and must be dropped; 6 is above and kept.
    s.append_to_log([
        health_entry(3, 1, true),
        health_entry(5, 1, true),
        health_entry(6, 1, true),
    ])
    .await
    .unwrap();

    let st = s.state.read().await;
    assert!(
        !st.log.contains_key(&3) && !st.log.contains_key(&5),
        "entries at or below the purge point must not be inserted"
    );
    assert!(
        st.log.contains_key(&6),
        "entries above the purge point are kept"
    );
}

/// `purge_logs_upto` is monotonic: a purge to a lower index must not move the boundary back.
#[tokio::test]
async fn purge_logs_upto_is_monotonic() {
    let v1 = ip4(10, 0, 0, 1);
    let mut s = storage(&[v1], 3);

    s.purge_logs_upto(lid(1, 9)).await.unwrap();
    // A lower purge must be ignored for the boundary.
    s.purge_logs_upto(lid(1, 4)).await.unwrap();

    let st = s.state.read().await;
    assert_eq!(
        st.last_purged_log_id.map(|l| l.index()),
        Some(9),
        "purge boundary must never move backwards"
    );
}

/// Regression for the restart-then-backfill crash: after a process restart the in-memory store
/// is empty (`last_purged_log_id = None`); when the leader backfills the committed suffix via
/// AppendEntries starting above the old (lost) purge floor, `append_to_log` must seed the floor
/// at `first_index - 1`. Otherwise openraft sees `last_purged = None` with a non-zero log floor,
/// scans from index 0, and trips `Defensive(LogIndexNotFound { want: 0, got: Some(<floor>) })`,
/// killing RaftCore. (Found by the real-cluster campaign; the earlier `install_snapshot` fix did
/// not cover the plain AppendEntries-backfill path.)
#[tokio::test]
async fn append_into_empty_log_above_zero_seeds_purge_floor() {
    let v1 = ip4(10, 0, 0, 1);
    let mut s = storage(&[v1], 3);
    // Fresh store: no purge, empty log (simulates the wiped in-memory store after restart).
    // Backfill starts at index 4000 (old purge floor 3999 + 1).
    s.append_to_log([health_entry(4000, 1, true), health_entry(4001, 1, true)])
        .await
        .unwrap();

    let ls = s.get_log_state().await.unwrap();
    assert_eq!(
        ls.last_purged_log_id.map(|l| l.index()),
        Some(3999),
        "append into an empty log starting at 4000 must seed last_purged_log_id to 3999"
    );
    // Invariant: last_purged.next_index() == lowest log key, so openraft never reads index 0.
    assert_eq!(
        ls.last_purged_log_id.map(|l| l.index() + 1),
        s.state.read().await.log.keys().next().copied(),
        "purge floor + 1 must equal the lowest log index (no get_log_id(0))"
    );
}

/// `get_log_state` read-side safety net: even if the floor was somehow left `None` with a
/// non-zero log floor, the reported `last_purged_log_id` must be derived to `lowest - 1` so
/// openraft's `load_log_ids` takes the `Some` branch instead of reading index 0.
#[tokio::test]
async fn get_log_state_derives_purge_floor_when_none_but_log_nonzero() {
    let v1 = ip4(10, 0, 0, 1);
    let mut s = storage(&[v1], 3);
    // Inject a non-zero log floor directly with last_purged_log_id left None (bypassing
    // append_to_log) to exercise the read-side guard in isolation.
    {
        let mut st = s.state.write().await;
        st.log.insert(4000, health_entry(4000, 1, true));
        st.log.insert(4001, health_entry(4001, 1, true));
        st.last_purged_log_id = None;
    }
    let ls = s.get_log_state().await.unwrap();
    assert_eq!(
        ls.last_purged_log_id.map(|l| l.index()),
        Some(3999),
        "get_log_state must derive the purge floor from the lowest log index when None"
    );
}

/// After a wiped restart the engine boots `committed = None`; if the leader then backfills a
/// committed suffix starting above the lost floor, the store must report `read_committed` at or
/// above that floor so openraft's first commit-driven apply never reads `get_log_entries(0..)`.
/// (Real cluster: returning `None` here made the apply read from index 0 against a 9000-based
/// log and trip `Defensive(LogIndexNotFound { want: 0, got: Some(9000) })`.)
#[tokio::test]
async fn read_committed_never_collapses_to_none_after_backfill() {
    let v1 = ip4(10, 0, 0, 1);
    let mut s = storage(&[v1], 3);
    s.append_to_log([health_entry(9000, 1, true), health_entry(9001, 1, true)])
        .await
        .unwrap();
    let c = s.read_committed().await.unwrap();
    assert!(
        c.map(|l| l.index()) >= Some(8999),
        "read_committed must report at/above the backfill floor so the engine commit-since \
             never collapses to 0: {c:?}"
    );
}

/// `save_committed` round-trips and `read_committed` clamps up to the applied frontier.
#[tokio::test]
async fn save_committed_roundtrips_and_clamps_to_applied() {
    let v1 = ip4(10, 0, 0, 1);
    let mut s = storage(&[v1], 3);
    s.save_committed(Some(lid(1, 42))).await.unwrap();
    assert_eq!(
        s.read_committed().await.unwrap().map(|l| l.index()),
        Some(42)
    );
    // Applying past the committed value raises read_committed to the applied frontier.
    s.apply_to_state_machine(&[membership_entry(50, &[1])])
        .await
        .unwrap();
    assert_eq!(
        s.read_committed().await.unwrap().map(|l| l.index()),
        Some(50),
        "read_committed must not sit below last_applied"
    );
}

/// Full-cluster-reform regression: a survivor holding a high purge floor (from snapshots) gets
/// its log truncated back to a low index and replayed from there. `delete_conflict_logs_since`
/// must lower the floor below the truncation point, or `append_to_log`'s drop-guard silently
/// drops the resent low prefix and the node crash-loops on `LogIndexNotFound { want: 0 }`.
#[tokio::test]
async fn reform_truncation_lowers_purge_floor_and_accepts_fresh_low_log() {
    let v1 = ip4(10, 0, 0, 1);
    let mut s = storage(&[v1], 3);
    // Survivor state: purged to 9000, log holds the post-snapshot suffix.
    s.purge_logs_upto(lid(1, 9000)).await.unwrap();
    s.append_to_log([health_entry(9001, 1, true), health_entry(9002, 1, true)])
        .await
        .unwrap();
    // Reform: leader truncates everything (conflict since index 0) and replays from 1.
    s.delete_conflict_logs_since(lid(2, 0)).await.unwrap();
    assert!(
        s.state.read().await.last_purged_log_id.is_none(),
        "truncation to index 0 must clear the stale high purge floor"
    );
    // The fresh low backfill must now be ACCEPTED (not dropped by the guard).
    s.append_to_log([
        membership_entry(1, &[1, 2, 3]),
        health_entry(2, 1, true),
        health_entry(3, 1, true),
    ])
    .await
    .unwrap();
    let st = s.state.read().await;
    assert_eq!(
        st.log.keys().next().copied(),
        Some(1),
        "fresh reform log must start at index 1, not be dropped by the drop-guard"
    );
    assert!(
        st.last_purged_log_id.map(|l| l.index()).unwrap_or(0) < 1,
        "purge floor must be below the new log floor after reform"
    );
}

/// `try_get_log_entries` must NOT panic on an inverted/empty range. `BTreeMap::range` panics
/// when start > end; openraft can hand the store such a range during stale-survivor incarnation
/// churn, which crashed RaftCore with `range start is greater than range end in BTreeMap`.
#[tokio::test]
async fn try_get_log_entries_does_not_panic_on_inverted_range() {
    let v1 = ip4(10, 0, 0, 1);
    let mut s = storage(&[v1], 3);
    s.append_to_log([health_entry(5, 1, true), health_entry(6, 1, true)])
        .await
        .unwrap();
    let mut reader = s.get_log_reader().await;
    // Inverted half-open range (start 9 > end 3): must return empty, not panic.
    #[allow(clippy::reversed_empty_ranges)]
    let res = reader.try_get_log_entries(9u64..3u64).await.unwrap();
    assert!(res.is_empty(), "inverted range must yield an empty slice");
    // Inverted inclusive range too.
    let res2 = reader
        .try_get_log_entries((Bound::Included(9u64), Bound::Included(3u64)))
        .await
        .unwrap();
    assert!(res2.is_empty());
    // A valid range still works.
    let res3 = reader.try_get_log_entries(5u64..=6u64).await.unwrap();
    assert_eq!(res3.len(), 2);
}

/// D7 stale-survivor regression for the `raft_core.rs:761` index underflow. `read_committed`
/// must never report a commit frontier ABOVE the highest servable log position
/// (`max(last_log_id, last_applied)`). When it did, openraft seeded its startup `committed`
/// to a stale high-INDEX id carried by `last_purged_log_id`; a reform commit at a higher term
/// but lower index then satisfied `update_committed` (LogId Ord is term-first), producing
/// `since = already_committed.next_index() > end = upto.index + 1`. The apply path requested
/// the inverted range `since..end`, the defensive check treated it as "empty range OK", and
/// `entries[entries.len() - 1]` underflowed to `usize::MAX`, panicking RaftCore.
#[tokio::test]
async fn read_committed_never_exceeds_servable_log() {
    let v1 = ip4(10, 0, 0, 1);
    let mut s = storage(&[v1], 3);
    // Stale survivor: purged to a high floor at a stale term; the suffix above it was lost on
    // the volatile restart, so the store can serve NOTHING and last_applied is empty too.
    s.purge_logs_upto(lid(35, 8)).await.unwrap();
    let committed = s.read_committed().await.unwrap();
    let last_log_index = s
        .get_log_state()
        .await
        .unwrap()
        .last_log_id
        .map(|l| l.index());
    let last_applied = s.state.read().await.last_applied_log.map(|l| l.index());
    let servable = last_log_index.max(last_applied).unwrap_or(0);
    assert!(
        committed.map(|c| c.index()).unwrap_or(0) <= servable,
        "read_committed {committed:?} exceeds the highest servable position {servable}; \
             openraft would seed a stale high-index frontier and later index entries[len-1] on an \
             inverted apply range, panicking at raft_core.rs:761"
    );
}

/// A conflict truncation ABOVE the floor must NOT lower it (only reform-below-floor does).
#[tokio::test]
async fn delete_conflict_above_floor_leaves_purge_floor_unchanged() {
    let v1 = ip4(10, 0, 0, 1);
    let mut s = storage(&[v1], 3);
    s.purge_logs_upto(lid(1, 9000)).await.unwrap();
    s.append_to_log([
        health_entry(9001, 1, true),
        health_entry(9002, 1, true),
        health_entry(9003, 1, true),
    ])
    .await
    .unwrap();
    // Truncate at 9002 (above the 9000 floor): floor must stay, 9001 survives.
    s.delete_conflict_logs_since(lid(1, 9002)).await.unwrap();
    let st = s.state.read().await;
    assert_eq!(st.last_purged_log_id.map(|l| l.index()), Some(9000));
    assert!(st.log.contains_key(&9001) && !st.log.contains_key(&9002));
}

/// openraft 0.10 migration: `truncate_after` replaces the 0.9 `delete_conflict_logs_since`, and
/// flips from inclusive (`remove index..`) to EXCLUSIVE (`remove index()+1..`, keep up to and
/// including `last_log_id`). This pins the off-by-one and the `None`-clears-everything path
/// directly against the new method (not the test shim), so the conflict-truncation purge-floor
/// regression (bug 3/4) stays covered under the new semantics.
#[tokio::test]
async fn truncate_after_is_exclusive_and_none_clears_log_and_floor() {
    let v1 = ip4(10, 0, 0, 1);
    let (mut log, _sm, state) = super::new_store(
        Arc::new(vec![(VipAddr::host(v1), "lo".to_string())]),
        3,
        true,
        0,
    );
    // Seed a contiguous log 1..=5.
    log.append((1..=5).map(|i| health_entry(i, 1, true)), IOFlushed::noop())
        .await
        .unwrap();

    // truncate_after(Some(2)) keeps ..=2 (1 and 2 survive), removes 3,4,5 - the EXCLUSIVE
    // boundary: index 2 is KEPT, not removed (the 0.9 inclusive call removed index 2).
    log.truncate_after(Some(lid(1, 2))).await.unwrap();
    {
        let st = state.read().await;
        assert!(st.log.contains_key(&1) && st.log.contains_key(&2));
        assert!(!st.log.contains_key(&3) && !st.log.contains_key(&5));
    }

    // truncate_after(None) removes the WHOLE log and, given a stale high floor, clears it to
    // None - the reform-to-zero path (0.9 `delete_conflict_logs_since(index 0)`).
    {
        let mut st = state.write().await;
        st.last_purged_log_id = Some(lid(1, 9000));
    }
    log.truncate_after(None).await.unwrap();
    let st = state.read().await;
    assert!(st.log.is_empty(), "truncate_after(None) must clear the log");
    assert!(
        st.last_purged_log_id.is_none(),
        "truncate_after(None) must clear a stale high purge floor so a fresh low backfill is accepted"
    );
}

/// The floor-seed must be monotonic and must not fire when a floor already exists.
#[tokio::test]
async fn append_does_not_move_existing_purge_floor() {
    let v1 = ip4(10, 0, 0, 1);
    let mut s = storage(&[v1], 3);
    s.purge_logs_upto(lid(1, 9)).await.unwrap();
    // Backfill above the floor: existing floor (9) must be preserved, not reseeded to 11.
    s.append_to_log([health_entry(12, 1, true)]).await.unwrap();
    assert_eq!(
        s.get_log_state()
            .await
            .unwrap()
            .last_purged_log_id
            .map(|l| l.index()),
        Some(9),
        "an existing purge floor must not be moved by a later append"
    );
}

/// Build a snapshot from a `failback: false` store after a node went unhealthy, so it carries a
/// non-empty `node_failback_blocked` set.
async fn snapshot_with_blocked_node(vips: &[IpAddr]) -> (SnapshotMetaOf<TypeConfig>, Vec<u8>) {
    let mut src = storage_failback(vips, 3, false);
    src.apply_to_state_machine(&[membership_entry(1, &[1, 2])])
        .await
        .unwrap();
    src.apply_to_state_machine(&[health_entry(2, 1, true)])
        .await
        .unwrap();
    // Under legacy semantics, failback:false permanently blocks the node until V2 activation.
    src.apply_to_state_machine(&[health_entry(3, 1, false)])
        .await
        .unwrap();
    assert!(
        src.state.read().await.node_failback_blocked.contains(&1),
        "precondition: source snapshot must carry a blocked node"
    );
    let mut builder = src.get_snapshot_builder().await;
    let snap = builder.build_snapshot().await.unwrap();
    let meta = snap.meta.clone();
    let bytes = snap.snapshot.into_inner();
    (meta, bytes)
}

/// A `failback: true` node must never hold a `node_failback_blocked` set: its own config can
/// never produce one. Installing a snapshot built by a `failback: false` peer must reconcile
/// the set to empty rather than adopt a blocked set this node could not have computed.
#[tokio::test]
async fn install_snapshot_clears_failback_blocked_when_local_failback_true() {
    let v1 = ip4(10, 0, 0, 1);
    let mut restored = storage_failback(&[v1], 3, true);

    let (meta, bytes) = snapshot_with_blocked_node(&[v1]).await;
    restored
        .install_snapshot(&meta, Cursor::new(bytes))
        .await
        .unwrap();

    let st = restored.state.read().await;
    assert!(
        st.node_failback_blocked.is_empty(),
        "a failback: true node must not adopt a blocked set, got {:?}",
        st.node_failback_blocked
    );
}

/// A `failback: false` node keeps the snapshot's blocked set, since that matches the semantics
/// its own config produces.
#[tokio::test]
async fn install_snapshot_keeps_failback_blocked_when_local_failback_false() {
    let v1 = ip4(10, 0, 0, 1);
    let mut restored = storage_failback(&[v1], 3, false);

    let (meta, bytes) = snapshot_with_blocked_node(&[v1]).await;
    restored
        .install_snapshot(&meta, Cursor::new(bytes))
        .await
        .unwrap();

    let st = restored.state.read().await;
    assert!(
        st.node_failback_blocked.contains(&1),
        "a failback: false node must retain the snapshot's blocked set"
    );
}

#[tokio::test]
async fn install_snapshot_restores_v2_nopreempt_state() {
    let vip = ip4(10, 0, 0, 1);
    let mut source = storage_failback(&[vip], 3, false);
    source
        .apply_to_state_machine(&[enable_v2_entry(1)])
        .await
        .unwrap();
    source
        .apply_to_state_machine(&[membership_entry(2, &[1, 2])])
        .await
        .unwrap();
    source
        .apply_to_state_machine(&[health_entry(3, 1, true)])
        .await
        .unwrap();
    source
        .apply_to_state_machine(&[health_entry(4, 2, true)])
        .await
        .unwrap();
    source
        .apply_to_state_machine(&[health_entry(5, 1, false)])
        .await
        .unwrap();
    assert!(source.state.read().await.node_nopreempt.contains(&1));

    let snapshot = source
        .get_snapshot_builder()
        .await
        .build_snapshot()
        .await
        .unwrap();
    let mut restored = storage_failback(&[vip], 3, false);
    restored
        .install_snapshot(&snapshot.meta, snapshot.snapshot)
        .await
        .unwrap();

    let state = restored.state.read().await;
    assert_eq!(state.failover_semantics, FailoverSemantics::V2);
    assert!(state.node_failback_blocked.is_empty());
    assert!(state.node_nopreempt.contains(&1));
}

#[tokio::test]
async fn install_snapshot_rejects_invalid_json_without_mutating_state() {
    let vip = ip4(10, 0, 0, 1);
    let mut source = storage(&[vip], 3);
    source
        .apply_to_state_machine(&[membership_entry(1, &[1, 2])])
        .await
        .unwrap();
    let snapshot = source
        .get_snapshot_builder()
        .await
        .build_snapshot()
        .await
        .unwrap();

    let mut restored = storage(&[vip], 3);
    restored.state.write().await.cluster_epoch = Some(77);
    let error = restored
        .install_snapshot(&snapshot.meta, Cursor::new(b"{".to_vec()))
        .await
        .unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(error.to_string().contains(&snapshot.meta.snapshot_id));
    assert_eq!(
        restored.state.read().await.cluster_epoch,
        Some(77),
        "an invalid snapshot must be rejected before any live state is replaced"
    );
}

#[tokio::test]
async fn install_v2_snapshot_on_failback_node_clears_nopreempt_history() {
    let vip = ip4(10, 0, 0, 1);
    let mut source = storage_failback(&[vip], 3, false);
    source
        .apply_to_state_machine(&[enable_v2_entry(1)])
        .await
        .unwrap();
    source
        .apply_to_state_machine(&[membership_entry(2, &[1, 2])])
        .await
        .unwrap();
    source
        .apply_to_state_machine(&[health_entry(3, 1, true)])
        .await
        .unwrap();
    source
        .apply_to_state_machine(&[health_entry(4, 2, true)])
        .await
        .unwrap();
    source
        .apply_to_state_machine(&[health_entry(5, 1, false)])
        .await
        .unwrap();
    assert!(source.state.read().await.node_nopreempt.contains(&1));

    let snapshot = source
        .get_snapshot_builder()
        .await
        .build_snapshot()
        .await
        .unwrap();
    let mut restored = storage_failback(&[vip], 3, true);
    restored
        .install_snapshot(&snapshot.meta, snapshot.snapshot)
        .await
        .unwrap();

    let state = restored.state.read().await;
    assert_eq!(state.failover_semantics, FailoverSemantics::V2);
    assert!(state.node_failback_blocked.is_empty());
    assert!(
        state.node_nopreempt.is_empty(),
        "failback:true must not inherit a nopreempt policy it cannot produce"
    );
}

#[derive(Debug, PartialEq, Eq)]
struct TransitionFingerprint {
    node_health: BTreeMap<u64, bool>,
    node_probe_ticks: BTreeMap<u64, u64>,
    latest_probe_tick: u64,
    vip_assignments: BTreeMap<IpAddr, VipAssignment>,
    vip_generation: BTreeMap<IpAddr, u64>,
    vip_last_holder: BTreeMap<IpAddr, u64>,
    node_recovery_tick: BTreeMap<u64, u64>,
    node_recovery_pending: BTreeSet<u64>,
    node_nopreempt: BTreeSet<u64>,
}

#[derive(Debug, Clone, Copy)]
struct TransitionPolicy {
    failback: bool,
    failback_delay_ticks: u64,
}

async fn run_v2_transition_sequence_with_policy(
    sequence: &[(u64, bool)],
    policy: TransitionPolicy,
) -> TransitionFingerprint {
    let vips = [ip4(10, 0, 0, 1), ip4(10, 0, 0, 2)];
    let mut store = storage_failback_delay(&vips, 2, policy.failback, policy.failback_delay_ticks);
    store
        .apply_to_state_machine(&[membership_entry(1, &[1, 2, 3])])
        .await
        .unwrap();
    store
        .apply_to_state_machine(&[enable_v2_entry(2)])
        .await
        .unwrap();
    for (offset, node_id) in [1_u64, 2, 3].into_iter().enumerate() {
        store
            .apply_to_state_machine(&[health_entry(3 + offset as u64, node_id, true)])
            .await
            .unwrap();
    }

    let initial = store.state.read().await;
    let mut previous_assignments = initial.vip_assignments.clone();
    let mut previous_generations = initial.vip_generation.clone();
    let mut last_holders: BTreeMap<IpAddr, u64> = previous_assignments
        .iter()
        .map(|(vip, assignment)| (*vip, assignment.holder))
        .collect();
    drop(initial);
    let mut expected_nopreempt = BTreeSet::new();
    let mut expected_recovery_pending = BTreeSet::new();
    let mut expected_recovery_tick = BTreeMap::new();

    for (offset, &(node_id, healthy)) in sequence.iter().enumerate() {
        store
            .apply_to_state_machine(&[health_entry(6 + offset as u64, node_id, healthy)])
            .await
            .unwrap();
        let state = store.state.read().await;
        let base_eligible = |candidate: u64| {
            *state.node_health.get(&candidate).unwrap_or(&false)
                && state.node_probe_ticks.get(&candidate).is_some_and(|tick| {
                    state.latest_probe_tick.saturating_sub(*tick) <= state.stale_missed_probes
                })
        };
        let failed_nodes: BTreeSet<u64> = previous_assignments
            .values()
            .map(|assignment| assignment.holder)
            .filter(|candidate| !base_eligible(*candidate))
            .collect();
        if policy.failback {
            for failed_node in failed_nodes {
                expected_recovery_pending.insert(failed_node);
                expected_recovery_tick.remove(&failed_node);
            }
            let pending: Vec<u64> = expected_recovery_pending.iter().copied().collect();
            for pending_node in pending {
                if !base_eligible(pending_node) {
                    expected_recovery_tick.remove(&pending_node);
                    continue;
                }
                let recovery_tick = *expected_recovery_tick
                    .entry(pending_node)
                    .or_insert(state.latest_probe_tick);
                if state.latest_probe_tick.saturating_sub(recovery_tick)
                    >= policy.failback_delay_ticks
                {
                    expected_recovery_pending.remove(&pending_node);
                    expected_recovery_tick.remove(&pending_node);
                }
            }
        } else {
            expected_nopreempt.extend(failed_nodes);
        }

        assert_eq!(
            state.node_nopreempt, expected_nopreempt,
            "policy={policy:?} sequence={sequence:?}: nopreempt history diverged"
        );
        assert_eq!(
            state.node_recovery_pending, expected_recovery_pending,
            "policy={policy:?} sequence={sequence:?}: recovery-pending history diverged"
        );
        assert_eq!(
            state.node_recovery_tick, expected_recovery_tick,
            "policy={policy:?} sequence={sequence:?}: recovery timing diverged"
        );
        let no_legacy_blocks = BTreeSet::new();
        let eligible: Vec<u64> = [1_u64, 2, 3]
            .into_iter()
            .filter(|candidate| {
                is_node_eligible(
                    *candidate,
                    &EligibilityInputs {
                        node_health: &state.node_health,
                        node_probe_ticks: &state.node_probe_ticks,
                        latest_probe_tick: state.latest_probe_tick,
                        stale_missed_probes: state.stale_missed_probes,
                        failback_delay_ticks: state.failback_delay_ticks,
                        node_recovery_tick: &state.node_recovery_tick,
                    },
                    &no_legacy_blocks,
                )
            })
            .collect();
        let mut load: BTreeMap<u64, usize> = eligible.iter().copied().map(|id| (id, 0)).collect();

        if eligible.is_empty() {
            assert!(
                state.vip_assignments.is_empty(),
                "sequence={sequence:?}: no eligible node may retain an assignment"
            );
        } else {
            assert_eq!(
                state.vip_assignments.len(),
                vips.len(),
                "sequence={sequence:?}: every VIP must have one assignment"
            );
        }

        for (vip, assignment) in &state.vip_assignments {
            assert!(
                eligible.contains(&assignment.holder),
                "policy={policy:?} sequence={sequence:?}: {vip} assigned to ineligible node {}",
                assignment.holder
            );
            *load
                .get_mut(&assignment.holder)
                .expect("assignment holder was proven eligible") += 1;
        }

        for vip in vips {
            let prior_generation = previous_generations.get(&vip).copied().unwrap_or_default();
            let generation = state.vip_generation.get(&vip).copied().unwrap_or_default();
            assert!(
                generation >= prior_generation,
                "policy={policy:?} sequence={sequence:?}: {vip} generation regressed from {prior_generation} to {generation}"
            );

            match (
                previous_assignments.get(&vip),
                state.vip_assignments.get(&vip),
            ) {
                (Some(previous), Some(current)) if previous.holder == current.holder => {
                    assert_eq!(
                        current, previous,
                        "policy={policy:?} sequence={sequence:?}: stable ownership metadata changed for {vip}"
                    );
                    assert_eq!(generation, prior_generation);
                }
                (Some(previous), Some(current)) => {
                    assert_eq!(generation, prior_generation.saturating_add(1));
                    assert_eq!(current.generation, generation);
                    assert_eq!(current.previous_holder, Some(previous.holder));
                    assert!(!current.previous_holder_released);
                    assert_eq!(
                        current.activation_tick,
                        state
                            .latest_probe_tick
                            .saturating_add(OWNERSHIP_ACTIVATION_HOLDOFF_TICKS)
                    );
                }
                (None, Some(current)) => {
                    assert_eq!(generation, prior_generation.saturating_add(1));
                    assert_eq!(current.generation, generation);
                    let previous_holder = last_holders.get(&vip).copied();
                    assert_eq!(current.previous_holder, previous_holder);
                    assert_eq!(current.previous_holder_released, previous_holder.is_none());
                    let expected_activation_tick = if prior_generation == 0 {
                        state.latest_probe_tick
                    } else {
                        state
                            .latest_probe_tick
                            .saturating_add(OWNERSHIP_ACTIVATION_HOLDOFF_TICKS)
                    };
                    assert_eq!(current.activation_tick, expected_activation_tick);
                }
                (Some(_), None) | (None, None) => {
                    assert_eq!(
                        generation, prior_generation,
                        "policy={policy:?} sequence={sequence:?}: ownerless {vip} changed generation"
                    );
                }
            }
        }

        if policy.failback
            && let (Some(min), Some(max)) = (load.values().min(), load.values().max())
        {
            assert!(
                max - min <= 1,
                "policy={policy:?} sequence={sequence:?}: failback placement is imbalanced: {load:?}"
            );
        }
        if policy.failback {
            assert!(
                state.node_nopreempt.is_empty(),
                "failback:true must not accumulate nopreempt history"
            );
        } else {
            assert!(
                state.node_recovery_pending.is_empty() && state.node_recovery_tick.is_empty(),
                "failback:false must not accumulate recovery-delay state"
            );
        }
        assert!(
            state
                .node_nopreempt
                .iter()
                .all(|node_id| [1_u64, 2, 3].contains(node_id)),
            "policy={policy:?} sequence={sequence:?}: nopreempt contains a non-member"
        );
        previous_assignments = state.vip_assignments.clone();
        previous_generations = state.vip_generation.clone();
        for (vip, assignment) in &previous_assignments {
            last_holders.insert(*vip, assignment.holder);
        }
        assert_eq!(state.vip_last_holder, last_holders);
    }

    let state = store.state.read().await;
    TransitionFingerprint {
        node_health: state.node_health.clone(),
        node_probe_ticks: state.node_probe_ticks.clone(),
        latest_probe_tick: state.latest_probe_tick,
        vip_assignments: state.vip_assignments.clone(),
        vip_generation: state.vip_generation.clone(),
        vip_last_holder: state.vip_last_holder.clone(),
        node_recovery_tick: state.node_recovery_tick.clone(),
        node_recovery_pending: state.node_recovery_pending.clone(),
        node_nopreempt: state.node_nopreempt.clone(),
    }
}

#[tokio::test]
async fn ownerless_gap_reassignment_restores_the_activation_holdoff() {
    let vip = ip4(10, 0, 0, 1);
    let mut store = storage(&[vip], 3);
    store
        .apply_to_state_machine(&[membership_entry(1, &[1, 2])])
        .await
        .unwrap();
    store
        .apply_to_state_machine(&[enable_v2_entry(2)])
        .await
        .unwrap();
    store
        .apply_to_state_machine(&[health_entry(3, 1, true)])
        .await
        .unwrap();
    assert_eq!(store.state.read().await.vip_assignments[&vip].holder, 1);

    store
        .apply_to_state_machine(&[health_entry(4, 1, false)])
        .await
        .unwrap();
    assert!(store.state.read().await.vip_assignments.is_empty());

    store
        .apply_to_state_machine(&[health_entry(5, 2, true)])
        .await
        .unwrap();
    let state = store.state.read().await;
    let reassigned = &state.vip_assignments[&vip];
    assert_eq!(reassigned.holder, 2);
    assert_eq!(reassigned.generation, 2);
    assert_eq!(
        reassigned.activation_tick,
        state.latest_probe_tick + OWNERSHIP_ACTIVATION_HOLDOFF_TICKS
    );
}

#[tokio::test]
async fn exhaustive_v2_sequences_cover_every_failback_policy_with_full_handoff_invariants() {
    const CHOICES: u64 = 6;
    const STEPS: usize = 4;
    let policies = [
        TransitionPolicy {
            failback: true,
            failback_delay_ticks: 0,
        },
        TransitionPolicy {
            failback: true,
            failback_delay_ticks: 2,
        },
        TransitionPolicy {
            failback: false,
            failback_delay_ticks: 0,
        },
    ];

    for policy in policies {
        for mut encoded in 0..CHOICES.pow(STEPS as u32) {
            let mut sequence = [(0_u64, false); STEPS];
            for event in &mut sequence {
                let choice = encoded % CHOICES;
                encoded /= CHOICES;
                *event = (choice / 2 + 1, choice % 2 == 1);
            }
            let first = run_v2_transition_sequence_with_policy(&sequence, policy).await;
            let replay = run_v2_transition_sequence_with_policy(&sequence, policy).await;
            assert_eq!(
                first, replay,
                "state replay diverged for policy={policy:?} sequence={sequence:?}"
            );
        }
    }
}

fn cluster_formed_entry(index: u64, cluster_id: u128) -> EntryOf<TypeConfig> {
    log_entry(
        index,
        EntryPayload::Normal(KafRequest::ClusterFormed {
            cluster_id,
            failover_semantics: FailoverSemantics::Legacy,
            config_identity_enforced: false,
        }),
    )
}

fn cluster_formed_v2_entry(index: u64, cluster_id: u128) -> EntryOf<TypeConfig> {
    log_entry(
        index,
        EntryPayload::Normal(KafRequest::ClusterFormed {
            cluster_id,
            failover_semantics: FailoverSemantics::V2,
            config_identity_enforced: false,
        }),
    )
}

fn enable_v2_entry(index: u64) -> EntryOf<TypeConfig> {
    log_entry(
        index,
        EntryPayload::Normal(KafRequest::EnableFailoverSemanticsV2),
    )
}

fn enable_config_identity_entry(index: u64) -> EntryOf<TypeConfig> {
    log_entry(
        index,
        EntryPayload::Normal(KafRequest::EnableConfigIdentityV1),
    )
}

#[tokio::test]
async fn fresh_formation_and_activation_enable_config_identity_monotonically() {
    let mut fresh = storage(&[ip4(10, 0, 0, 1)], 3);
    fresh
        .apply_to_state_machine(&[log_entry(
            1,
            EntryPayload::Normal(KafRequest::ClusterFormed {
                cluster_id: 42,
                failover_semantics: FailoverSemantics::V2,
                config_identity_enforced: true,
            }),
        )])
        .await
        .unwrap();
    assert!(fresh.state.read().await.config_identity_enforced);

    let mut upgraded = storage(&[ip4(10, 0, 0, 1)], 3);
    upgraded
        .apply_to_state_machine(&[enable_config_identity_entry(1)])
        .await
        .unwrap();
    assert!(upgraded.state.read().await.config_identity_enforced);

    upgraded
        .apply_to_state_machine(&[cluster_formed_entry(2, 99)])
        .await
        .unwrap();
    assert!(
        upgraded.state.read().await.config_identity_enforced,
        "later legacy formation data must not disable identity enforcement"
    );
}

#[tokio::test]
async fn config_gate_blocks_the_same_log_staleness_divergence() {
    let yaml = |stale_secs| {
        format!(
            r#"
node_id: 1
raft_listen: 127.0.0.1:17101
client_submit_listen: 127.0.0.1:18101
peers:
  - {{ id: 1, raft_address: 127.0.0.1:17101, client_submit_address: 127.0.0.1:18101 }}
  - {{ id: 2, raft_address: 127.0.0.1:17102, client_submit_address: 127.0.0.1:18102 }}
  - {{ id: 3, raft_address: 127.0.0.1:17103, client_submit_address: 127.0.0.1:18103 }}
vips:
  - {{ address: 10.0.0.1/32, interface: lo }}
health:
  command: [/bin/true]
  interval_ms: 1000
  timeout_ms: 500
  stale_secs: {stale_secs}
"#
        )
    };
    let strict: Config = serde_yaml::from_str(&yaml(1)).unwrap();
    let lenient: Config = serde_yaml::from_str(&yaml(3)).unwrap();
    let strict_fingerprint = strict.cluster_config_fingerprint().unwrap();
    let lenient_fingerprint = lenient.cluster_config_fingerprint().unwrap();
    assert_ne!(strict_fingerprint, lenient_fingerprint);

    let vip = ip4(10, 0, 0, 1);
    let mut strict_store = storage(&[vip], strict.health.effective_stale_missed_probes());
    let mut lenient_store = storage(&[vip], lenient.health.effective_stale_missed_probes());
    let identical_log = [
        membership_entry(1, &[1, 2, 3]),
        health_entry(2, 1, true),
        health_entry(3, 2, true),
        health_entry(4, 3, true),
        health_entry(5, 2, true),
        health_entry(6, 2, true),
    ];
    strict_store
        .apply_to_state_machine(&identical_log)
        .await
        .unwrap();
    lenient_store
        .apply_to_state_machine(&identical_log)
        .await
        .unwrap();

    assert_ne!(
        assignment_of(&strict_store, vip).await.unwrap().holder,
        assignment_of(&lenient_store, vip).await.unwrap().holder,
        "the regression setup must reproduce divergent ownership from one committed log"
    );
    assert!(!config_identity_compatible(
        strict_fingerprint,
        Some(lenient_fingerprint),
        false
    ));
}

#[tokio::test]
async fn fresh_v2_cluster_formation_activates_v2_semantics() {
    let vip = ip4(10, 0, 0, 1);
    let mut s = storage(&[vip], 3);

    s.apply_to_state_machine(&[cluster_formed_v2_entry(1, 42)])
        .await
        .unwrap();
    assert_eq!(
        s.state.read().await.failover_semantics,
        FailoverSemantics::V2
    );
}

#[tokio::test]
async fn activation_migrates_legacy_block_to_nopreempt_without_downgrade() {
    let vip = ip4(10, 0, 0, 1);
    let mut s = storage_failback(&[vip], 3, false);

    s.apply_to_state_machine(&[membership_entry(1, &[1, 2])])
        .await
        .unwrap();
    s.apply_to_state_machine(&[health_entry(2, 1, true)])
        .await
        .unwrap();
    s.apply_to_state_machine(&[health_entry(3, 2, true)])
        .await
        .unwrap();
    s.apply_to_state_machine(&[health_entry(4, 1, false)])
        .await
        .unwrap();
    assert_eq!(
        assignment_of(&s, vip).await.unwrap().previous_holder,
        Some(1)
    );
    assert!(s.state.read().await.node_failback_blocked.contains(&1));

    s.apply_to_state_machine(&[enable_v2_entry(5)])
        .await
        .unwrap();
    {
        let st = s.state.read().await;
        assert_eq!(st.failover_semantics, FailoverSemantics::V2);
        assert!(st.node_failback_blocked.is_empty());
        assert!(st.node_nopreempt.contains(&1));
    }

    s.apply_to_state_machine(&[cluster_formed_entry(6, 99)])
        .await
        .unwrap();
    assert_eq!(
        s.state.read().await.failover_semantics,
        FailoverSemantics::V2,
        "later legacy formation data must not downgrade an activated cluster"
    );
}

/// Legacy failback-enabled operation must never populate the permanent no-failback set. A normal
/// healthy-to-unhealthy transition uses the recovery-delay path instead; contaminating the set
/// would make that owner permanently ineligible after it recovers.
#[tokio::test]
async fn legacy_failback_enabled_failure_never_populates_permanent_block() {
    let vip = ip4(10, 0, 0, 1);
    let mut s = storage_failback_delay(&[vip], 3, true, 2);

    s.apply_to_state_machine(&[
        membership_entry(1, &[1, 2]),
        health_entry(2, 1, true),
        health_entry(3, 2, true),
        health_entry(4, 1, false),
    ])
    .await
    .unwrap();

    let state = s.state.read().await;
    assert!(
        state.node_failback_blocked.is_empty(),
        "failback-enabled failures must not create a permanent eligibility block"
    );
}

#[tokio::test]
async fn activation_conservatively_migrates_legacy_nopreempt_history_for_an_idle_node() {
    let v1 = ip4(10, 0, 0, 1);
    let v2 = ip4(10, 0, 0, 2);
    let mut s = storage_failback(&[v1, v2], 3, false);

    for (index, node_id) in [1_u64, 2, 3].into_iter().enumerate() {
        s.apply_to_state_machine(&[health_entry(1 + index as u64, node_id, true)])
            .await
            .unwrap();
    }
    s.apply_to_state_machine(&[membership_entry(4, &[1, 2, 3])])
        .await
        .unwrap();
    assert!(
        !s.state
            .read()
            .await
            .vip_assignments
            .values()
            .any(|assignment| assignment.holder == 3)
    );

    s.apply_to_state_machine(&[health_entry(5, 3, false)])
        .await
        .unwrap();
    assert!(s.state.read().await.node_failback_blocked.contains(&3));

    s.apply_to_state_machine(&[enable_v2_entry(6)])
        .await
        .unwrap();
    let state = s.state.read().await;
    assert!(state.node_failback_blocked.is_empty());
    assert!(state.node_nopreempt.contains(&3));
}

#[tokio::test]
async fn activation_conservatively_migrates_legacy_recovery_timer_for_an_idle_node() {
    let v1 = ip4(10, 0, 0, 1);
    let v2 = ip4(10, 0, 0, 2);
    let mut s = storage_failback_delay(&[v1, v2], 3, true, 10);

    for (index, node_id) in [1_u64, 2, 3].into_iter().enumerate() {
        s.apply_to_state_machine(&[health_entry(1 + index as u64, node_id, true)])
            .await
            .unwrap();
    }
    s.apply_to_state_machine(&[membership_entry(4, &[1, 2, 3])])
        .await
        .unwrap();
    s.apply_to_state_machine(&[health_entry(5, 3, false)])
        .await
        .unwrap();
    s.apply_to_state_machine(&[health_entry(6, 3, true)])
        .await
        .unwrap();
    assert!(s.state.read().await.node_recovery_tick.contains_key(&3));

    s.apply_to_state_machine(&[enable_v2_entry(7)])
        .await
        .unwrap();
    let state = s.state.read().await;
    assert!(state.node_recovery_pending.contains(&3));
    assert!(state.node_recovery_tick.contains_key(&3));
}

#[tokio::test]
async fn activation_migrates_legacy_recovery_timer_for_a_failed_owner() {
    let vip = ip4(10, 0, 0, 1);
    let mut s = storage_failback_delay(&[vip], 3, true, 10);

    s.apply_to_state_machine(&[membership_entry(1, &[1, 2])])
        .await
        .unwrap();
    s.apply_to_state_machine(&[health_entry(2, 1, true)])
        .await
        .unwrap();
    s.apply_to_state_machine(&[health_entry(3, 2, true)])
        .await
        .unwrap();
    s.apply_to_state_machine(&[health_entry(4, 1, false)])
        .await
        .unwrap();
    s.apply_to_state_machine(&[health_entry(5, 1, true)])
        .await
        .unwrap();
    assert_eq!(
        assignment_of(&s, vip).await.unwrap().previous_holder,
        Some(1)
    );
    assert!(s.state.read().await.node_recovery_tick.contains_key(&1));

    s.apply_to_state_machine(&[enable_v2_entry(6)])
        .await
        .unwrap();
    let state = s.state.read().await;
    assert!(state.node_recovery_pending.contains(&1));
    assert!(state.node_recovery_tick.contains_key(&1));
}

#[tokio::test]
async fn activation_preserves_nopreempt_after_chained_legacy_handoffs() {
    let v1 = ip4(10, 0, 0, 1);
    let v2 = ip4(10, 0, 0, 2);
    let mut s = storage_failback(&[v1, v2], 3, false);

    for (index, node_id) in [1_u64, 2, 3].into_iter().enumerate() {
        s.apply_to_state_machine(&[health_entry(1 + index as u64, node_id, true)])
            .await
            .unwrap();
    }
    s.apply_to_state_machine(&[membership_entry(4, &[1, 2, 3])])
        .await
        .unwrap();
    s.apply_to_state_machine(&[health_entry(5, 1, false)])
        .await
        .unwrap();
    s.apply_to_state_machine(&[health_entry(6, 1, true)])
        .await
        .unwrap();
    s.apply_to_state_machine(&[health_entry(7, 3, false)])
        .await
        .unwrap();
    assert!(s.state.read().await.node_failback_blocked.contains(&1));
    assert!(
        !s.state
            .read()
            .await
            .vip_assignments
            .values()
            .any(|assignment| assignment.previous_holder == Some(1))
    );

    s.apply_to_state_machine(&[enable_v2_entry(8)])
        .await
        .unwrap();
    let state = s.state.read().await;
    assert!(
        state.node_nopreempt.contains(&1),
        "overwritten handoff history must not let a real failed owner preempt after activation"
    );
    assert!(
        !state
            .vip_assignments
            .values()
            .any(|assignment| assignment.holder == 1)
    );
}

#[tokio::test]
async fn activation_preserves_recovery_delay_after_chained_legacy_handoffs() {
    let v1 = ip4(10, 0, 0, 1);
    let v2 = ip4(10, 0, 0, 2);
    let mut s = storage_failback_delay(&[v1, v2], 3, true, 10);

    for (index, node_id) in [1_u64, 2, 3].into_iter().enumerate() {
        s.apply_to_state_machine(&[health_entry(1 + index as u64, node_id, true)])
            .await
            .unwrap();
    }
    s.apply_to_state_machine(&[membership_entry(4, &[1, 2, 3])])
        .await
        .unwrap();
    s.apply_to_state_machine(&[health_entry(5, 1, false)])
        .await
        .unwrap();
    s.apply_to_state_machine(&[health_entry(6, 1, true)])
        .await
        .unwrap();
    s.apply_to_state_machine(&[health_entry(7, 3, false)])
        .await
        .unwrap();
    assert!(s.state.read().await.node_recovery_tick.contains_key(&1));
    assert!(
        !s.state
            .read()
            .await
            .vip_assignments
            .values()
            .any(|assignment| assignment.previous_holder == Some(1))
    );

    s.apply_to_state_machine(&[enable_v2_entry(8)])
        .await
        .unwrap();
    let state = s.state.read().await;
    assert!(state.node_recovery_pending.contains(&1));
    assert!(state.node_recovery_tick.contains_key(&1));
    assert!(
        !state
            .vip_assignments
            .values()
            .any(|assignment| assignment.holder == 1),
        "overwritten handoff history must not bypass the recovered owner's delay"
    );
}

#[tokio::test]
async fn apply_moves_only_the_orphaned_vip_and_keeps_survivors_sticky() {
    // Minimal-movement through the committed-state feedback loop: apply_to_state_machine reads
    // the committed vip_assignments back in as current_holders, so a topology change must move
    // only the orphaned VIP and keep every still-eligible holder (and its generation) stable.
    let v0 = ip4(10, 0, 0, 1);
    let v1 = ip4(10, 0, 0, 2);
    let v2 = ip4(10, 0, 0, 3);
    let mut s = storage(&[v0, v1, v2], 3);

    s.apply_to_state_machine(&[membership_entry(1, &[1, 2, 3])])
        .await
        .unwrap();
    for (i, node) in [1_u64, 2, 3].iter().enumerate() {
        s.apply_to_state_machine(&[health_entry(2 + i as u64, *node, true)])
            .await
            .unwrap();
    }

    // Once all three are eligible each node holds exactly one VIP. Read the baseline
    // dynamically (applying health one entry at a time produces intermediate rebalances).
    let mut base: Vec<(IpAddr, VipAssignment)> = Vec::new();
    for vip in [v0, v1, v2] {
        base.push((vip, assignment_of(&s, vip).await.unwrap()));
    }
    let holders: BTreeSet<u64> = base.iter().map(|(_, a)| a.holder).collect();
    assert_eq!(holders, BTreeSet::from([1, 2, 3]), "one VIP per node");

    // Node 3 stops publishing; the VIP it holds is the orphan, the other two are survivors.
    let (orphan_vip, orphan_base) = base
        .iter()
        .find(|(_, a)| a.holder == 3)
        .map(|(ip, a)| (*ip, a.clone()))
        .expect("node 3 holds one VIP");
    let survivors: Vec<(IpAddr, VipAssignment)> = base
        .iter()
        .filter(|(_, a)| a.holder != 3)
        .map(|(ip, a)| (*ip, a.clone()))
        .collect();

    let mut idx = 5;
    for _ in 0..6 {
        s.apply_to_state_machine(&[health_entry(idx, 1, true)])
            .await
            .unwrap();
        idx += 1;
        s.apply_to_state_machine(&[health_entry(idx, 2, true)])
            .await
            .unwrap();
        idx += 1;
    }

    // Survivors keep their VIPs and never bump generation (stickiness, no spurious handoff).
    for (vip, base_a) in &survivors {
        let now = assignment_of(&s, *vip).await.unwrap();
        assert_eq!(now.holder, base_a.holder, "survivor VIP must not move");
        assert_eq!(
            now.generation, base_a.generation,
            "survivor generation must not bump"
        );
    }

    // Only the orphaned VIP moves off the stale node; generation bumps, previous holder recorded.
    let moved = assignment_of(&s, orphan_vip).await.unwrap();
    assert_ne!(moved.holder, 3, "orphaned VIP must leave the stale node");
    assert!(moved.holder == 1 || moved.holder == 2);
    assert_eq!(moved.previous_holder, Some(3));
    assert!(moved.generation > orphan_base.generation);
}

#[tokio::test]
async fn cluster_formed_sets_epoch_once_first_write_wins() {
    // The cluster incarnation is set-once at the replicated state-machine level: the first
    // committed ClusterFormed wins and later ones (e.g. a second leader that committed before
    // observing the first) are ignored, so every node converges on the same value.
    let mut s = storage(&[ip4(10, 0, 0, 1)], 3);
    assert_eq!(s.state.read().await.cluster_epoch, None);

    s.apply_to_state_machine(&[cluster_formed_entry(1, 111)])
        .await
        .unwrap();
    assert_eq!(s.state.read().await.cluster_epoch, Some(111));

    s.apply_to_state_machine(&[cluster_formed_entry(2, 222)])
        .await
        .unwrap();
    assert_eq!(s.state.read().await.cluster_epoch, Some(111));
}
