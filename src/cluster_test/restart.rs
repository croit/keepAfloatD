use super::*;
use crate::config::Config;
use crate::raft::admission::{Genesis, ReplicaId};
use crate::raft::{FatalReason, KafRaft, KafStorageState, RaftControlTasks, RaftNetworkImpl};
use futures::FutureExt;
use openraft::async_runtime::WatchReceiver;
use std::collections::BTreeSet;
use tokio::sync::{RwLock, mpsc};
use tokio::time::Instant;

mod renewal_progress;

struct Node {
    cfg: Arc<Config>,
    raft: KafRaft,
    network: Arc<RaftNetworkImpl>,
    state: Arc<RwLock<KafStorageState>>,
    fatal: mpsc::UnboundedReceiver<FatalReason>,
    network_failure: mpsc::UnboundedReceiver<String>,
    controls: RaftControlTasks,
}

impl Node {
    async fn start(cfg: Arc<Config>, listener: ListenerSource) -> Self {
        let (raft, network, state, fatal, network_failure, controls) =
            crate::raft::start_raft(cfg.clone(), Arc::new(Vec::new()), listener)
                .await
                .unwrap();
        Self {
            cfg,
            raft,
            network,
            state,
            fatal,
            network_failure,
            controls,
        }
    }

    fn replica(&self) -> ReplicaId {
        self.controls.runtime().local_replica()
    }

    fn assert_running(&mut self) {
        match self.fatal.try_recv() {
            Err(mpsc::error::TryRecvError::Empty) => {}
            result => panic!("node {} guard stopped: {result:?}", self.cfg.node_id),
        }
        match self.network_failure.try_recv() {
            Err(mpsc::error::TryRecvError::Empty) => {}
            result => panic!("node {} network stopped: {result:?}", self.cfg.node_id),
        }
        if let Some(result) = self.controls.recv_failure().now_or_never() {
            panic!("node {} control stopped: {result:?}", self.cfg.node_id);
        }
    }

    async fn stop(&mut self) {
        let controls = self.controls.shutdown().await;
        let network = self.network.shutdown().await;
        let raft = self.raft.shutdown().await;
        controls.unwrap();
        network.unwrap();
        raft.unwrap();
        assert!(self.controls.runtime().current().is_none());
    }
}

async fn initial_nodes() -> Vec<Node> {
    let mut cluster = ClusterFixture::bind(3).await;
    let mut nodes = Vec::new();
    for index in 0..3 {
        nodes.push(Node::start(cluster.config(index, &[]), cluster.take_raft(index)).await);
    }
    nodes
}

#[tokio::test(start_paused = true)]
async fn healthy_majority_outlives_a_slow_election_without_restarting() {
    let mut cluster = ClusterFixture::bind(3).await;
    let mut nodes = Vec::new();
    for index in 0..3 {
        let mut cfg = cluster.config(index, &[]);
        let tune = Arc::make_mut(&mut cfg);
        tune.health.interval_ms = 500;
        tune.health.stale_secs = Some(1);
        tune.raft.election_timeout_min_ms = 2_000;
        tune.raft.election_timeout_max_ms = 3_000;
        tune.raft.heartbeat_interval_ms = 250;
        nodes.push(Node::start(cfg, cluster.take_raft(index)).await);
    }
    let result = std::panic::AssertUnwindSafe(async {
        let genesis = wait_stable(&mut nodes, None).await;
        let voters: BTreeSet<_> = nodes.iter().map(Node::replica).collect();
        let health = crate::consensus_freshness::ConsensusFreshness::for_probe_cadence(
            nodes[0].cfg.health.interval_ms,
            nodes[0].cfg.health.effective_stale_missed_probes(),
        );
        assert_eq!(health.lifetime(), Duration::from_millis(1_250));
        for node in &nodes {
            publish_history_with_retry(node).await;
        }
        let leader = nodes[0]
            .raft
            .metrics()
            .borrow_watched()
            .current_leader
            .unwrap();
        let leader_index = nodes
            .iter()
            .position(|node| node.replica() == leader)
            .unwrap();
        let history = publish_history_with_retry(&nodes[leader_index]).await;
        let mut stopped = nodes.remove(leader_index);
        stopped.stop().await;
        eprintln!(
            "stopped leader {} after stable membership and health progress",
            leader
        );
        let survivor_boots: Vec<_> = nodes.iter().map(Node::replica).collect();
        let deadlines: Vec<_> = nodes
            .iter()
            .map(|node| node.controls.runtime().current().unwrap().check().unwrap())
            .collect();
        let tune = &nodes[0].cfg.raft;
        let election_budget = Duration::from_millis(
            3 * tune.election_timeout_max_ms
                + 2 * tune.election_timeout_min_ms
                + 3 * tune.heartbeat_interval_ms / 2,
        ) + health.lifetime() * 4;
        assert!(
            advance_until(election_budget, async || {
                for (node, boot) in nodes.iter_mut().zip(&survivor_boots) {
                    node.assert_running();
                    assert_eq!(node.replica(), *boot);
                }
                let elected = nodes[0].raft.metrics().borrow_watched().current_leader;
                if elected.is_none_or(|id| !survivor_boots.contains(&id)) {
                    return false;
                }
                for node in &nodes {
                    if node.raft.metrics().borrow_watched().current_leader != elected {
                        return false;
                    }
                }
                stable_membership(&mut nodes, &voters, Some(&genesis)).await
            })
            .await,
            "the same two healthy boots must elect a leader without the stopped voter"
        );
        for node in &nodes {
            assert!(publish_history_with_retry(node).await.index > history.index);
        }
        let previous_expiry = *deadlines.iter().max().unwrap();
        let timing = crate::runtime_permission::LeaseTiming::for_config(&nodes[0].cfg, 0).unwrap();
        assert!(
            advance_until(timing.consumer_use() * 2, async || {
                if !stable_membership(&mut nodes, &voters, Some(&genesis)).await
                    || Instant::now() <= previous_expiry
                {
                    return false;
                }
                for ((node, boot), previous) in nodes.iter().zip(&survivor_boots).zip(&deadlines) {
                    assert_eq!(node.replica(), *boot);
                    if !node.controls.runtime().current().is_some_and(|session| {
                        session.check().is_ok_and(|deadline| deadline > *previous)
                    }) {
                        return false;
                    }
                    let state = node.state.read().await;
                    if state
                        .applied_progress
                        .get(&node.cfg.node_id)
                        .is_none_or(|progress| {
                            progress.request.replica != *boot
                                || progress.log_id.index <= history.index
                        })
                    {
                        return false;
                    }
                }
                true
            })
            .await,
            "survivors must commit fresh progress and renew beyond their pre-election leases"
        );
    })
    .catch_unwind()
    .await;
    for node in &mut nodes {
        node.stop().await;
    }
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

async fn stable_membership(
    nodes: &mut [Node],
    expected: &BTreeSet<ReplicaId>,
    expected_genesis: Option<&Genesis>,
) -> bool {
    let mut common = expected_genesis.cloned();
    for node in nodes {
        node.assert_running();
        let Some(session) = node.controls.runtime().current() else {
            return false;
        };
        let state = node.state.read().await;
        let Some(genesis) = state.genesis.as_ref() else {
            return false;
        };
        if let Some(common) = &common {
            assert_eq!(genesis, common, "committed genesis changed or diverged");
        } else {
            common = Some(genesis.clone());
        }
        let membership = state.last_membership.membership();
        if session.context().genesis != *genesis
            || membership.get_joint_config().len() != 1
            || membership.voter_ids().collect::<BTreeSet<_>>() != *expected
            || state.prepared_join.is_some()
            || state.committed.is_none()
            || state.last_applied_log.is_none()
            || node
                .raft
                .metrics()
                .borrow_watched()
                .current_leader
                .is_none()
        {
            return false;
        }
    }
    true
}

async fn wait_stable(nodes: &mut [Node], expected_genesis: Option<&Genesis>) -> Genesis {
    let voters = nodes.iter().map(Node::replica).collect();
    let budget = startup_budget(&nodes[0].cfg);
    assert!(
        advance_until(budget, async || {
            stable_membership(nodes, &voters, expected_genesis).await
        })
        .await,
        "all live boots must commit the same genesis and stable voter membership"
    );
    nodes[0].state.read().await.genesis.clone().unwrap()
}

async fn publish_history(node: &Node) -> openraft::alias::LogIdOf<crate::raft::TypeConfig> {
    let runtime = node.controls.runtime();
    let mut publish = Box::pin(runtime.submit_health(&node.network, true));
    let mut committed = None;
    assert!(
        advance_until(startup_budget(&node.cfg), async || {
            if let std::task::Poll::Ready(result) = futures::poll!(publish.as_mut()) {
                committed = Some(result.unwrap().1);
                true
            } else {
                false
            }
        })
        .await,
        "ordinary health progress must commit before restarting a follower"
    );
    committed.unwrap()
}

async fn publish_history_with_retry(
    node: &Node,
) -> openraft::alias::LogIdOf<crate::raft::TypeConfig> {
    let runtime = node.controls.runtime();
    let mut publish = Box::pin(runtime.submit_health(&node.network, true));
    let mut committed = None;
    let mut last_error = None;
    assert!(
        advance_until(startup_budget(&node.cfg), async || {
            match futures::poll!(publish.as_mut()) {
                std::task::Poll::Ready(Ok((_, log))) => {
                    committed = Some(log);
                    true
                }
                std::task::Poll::Ready(Err(error)) => {
                    last_error = Some(error);
                    publish = Box::pin(runtime.submit_health(&node.network, true));
                    false
                }
                std::task::Poll::Pending => false,
            }
        })
        .await,
        "ordinary health progress did not commit: {last_error:?}"
    );
    committed.unwrap()
}

async fn survive_renewal(nodes: &mut [Node], genesis: &Genesis) {
    let deadlines: Vec<_> = nodes
        .iter()
        .map(|node| node.controls.runtime().current().unwrap().check().unwrap())
        .collect();
    let previous_expiry = *deadlines.iter().max().unwrap();
    let voters = nodes.iter().map(Node::replica).collect();
    let timing = crate::runtime_permission::LeaseTiming::for_config(&nodes[0].cfg, 0).unwrap();
    assert!(
        advance_until(timing.consumer_use() * 2, async || {
            if !stable_membership(nodes, &voters, Some(genesis)).await
                || Instant::now() <= previous_expiry
            {
                return false;
            }
            for (node, previous) in nodes.iter().zip(&deadlines) {
                if !node.controls.runtime().current().is_some_and(|session| {
                    session.check().is_ok_and(|deadline| deadline > *previous)
                }) {
                    return false;
                }
                let state = node.state.read().await;
                if state
                    .applied_progress
                    .get(&node.cfg.node_id)
                    .is_none_or(|progress| progress.request.replica != node.replica())
                {
                    return false;
                }
            }
            true
        })
        .await,
        "every live boot must retain quorum admission beyond its original deadline"
    );
}

#[tokio::test(start_paused = true)]
async fn follower_restart_reuses_address_but_not_boot_and_preserves_genesis() {
    let mut nodes = initial_nodes().await;
    let result = std::panic::AssertUnwindSafe(async {
        let genesis = wait_stable(&mut nodes, None).await;
        let leader = nodes[0]
            .raft
            .metrics()
            .borrow_watched()
            .current_leader
            .unwrap();
        let follower = nodes
            .iter()
            .position(|node| node.replica() != leader)
            .unwrap();
        let writer = nodes
            .iter()
            .position(|node| node.replica() == leader)
            .unwrap();
        let writer_id = nodes[writer].cfg.node_id;
        let history = publish_history(&nodes[writer]).await;
        let old_boot = nodes[follower].replica();
        let cfg = nodes[follower].cfg.clone();
        nodes[follower].stop().await;
        let replacement = Node::start(cfg.clone(), ListenerSource::Configured).await;
        let new_boot = replacement.replica();
        assert_eq!(new_boot.physical_id, old_boot.physical_id);
        assert_ne!(new_boot.boot_nonce, old_boot.boot_nonce);
        assert_eq!(replacement.cfg.raft_listen, cfg.raft_listen);
        {
            let blank = replacement.state.read().await;
            assert!(blank.genesis.is_none());
            assert!(blank.log.is_empty());
            assert!(blank.last_applied_log.is_none());
        }
        nodes[follower] = replacement;
        wait_stable(&mut nodes, Some(&genesis)).await;
        for node in &nodes {
            let state = node.state.read().await;
            let voters: BTreeSet<_> = state.last_membership.membership().voter_ids().collect();
            assert!(!voters.contains(&old_boot));
            assert!(voters.contains(&new_boot));
            assert_eq!(state.node_health.get(&writer_id), Some(&true));
            assert!(state.last_applied_log.unwrap().index >= history.index);
            let completed = state.completed_joins.get(&new_boot.physical_id).unwrap();
            assert_eq!(completed.prepared.plan.consumer, new_boot);
            assert_eq!(completed.prepared.plan.genesis, genesis);
            let acknowledged = completed.prepared.learner_applied.unwrap();
            assert!(acknowledged.index > completed.prepared.prepared.index);
            assert!(completed.promoted.index > acknowledged.index);
        }
        survive_renewal(&mut nodes, &genesis).await;
    })
    .catch_unwind()
    .await;
    for node in &mut nodes {
        node.stop().await;
    }
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test(start_paused = true)]
async fn complete_restart_forms_a_new_genesis_without_persisted_state() {
    let mut nodes = initial_nodes().await;
    let result = std::panic::AssertUnwindSafe(async {
        let previous = wait_stable(&mut nodes, None).await;
        let old_boots: BTreeSet<_> = nodes.iter().map(Node::replica).collect();
        let configs: Vec<_> = nodes.iter().map(|node| node.cfg.clone()).collect();
        for node in &mut nodes {
            node.stop().await;
        }
        nodes.clear();
        for cfg in configs {
            nodes.push(Node::start(cfg, ListenerSource::Configured).await);
        }
        let current = wait_stable(&mut nodes, None).await;
        assert_ne!(current, previous);
        assert_ne!(current.epoch, previous.epoch);
        assert_eq!(current.config, previous.config);
        let new_boots: BTreeSet<_> = nodes.iter().map(Node::replica).collect();
        assert!(old_boots.is_disjoint(&new_boots));
        assert!(current.voters.is_subset(&new_boots));
        assert!(current.voters.len() > new_boots.len() / 2);
        survive_renewal(&mut nodes, &current).await;
    })
    .catch_unwind()
    .await;
    for node in &mut nodes {
        node.stop().await;
    }
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test(start_paused = true)]
async fn clean_follower_restart_bootstraps_from_a_forced_snapshot_after_membership_change() {
    let mut nodes = initial_nodes().await;
    let result = std::panic::AssertUnwindSafe(async {
        let genesis = wait_stable(&mut nodes, None).await;
        for force_snapshot in [false, true] {
            let leader = nodes[0]
                .raft
                .metrics()
                .borrow_watched()
                .current_leader
                .unwrap();
            let writer = nodes
                .iter()
                .position(|node| node.replica() == leader)
                .unwrap();
            let follower = nodes
                .iter()
                .position(|node| node.replica() != leader)
                .unwrap();
            let previous_voters: BTreeSet<_> = nodes.iter().map(Node::replica).collect();
            if force_snapshot {
                assert_ne!(previous_voters, genesis.voters);
            }
            let survivor_boots: Vec<_> = nodes
                .iter()
                .enumerate()
                .filter(|(index, _)| *index != follower)
                .map(|(index, node)| (index, node.replica()))
                .collect();
            let writer_id = nodes[writer].cfg.node_id;
            let history = publish_history(&nodes[writer]).await;
            let old_boot = nodes[follower].replica();
            let cfg = nodes[follower].cfg.clone();
            let budget = startup_budget(&cfg);
            tokio::time::timeout(budget, nodes[follower].stop())
                .await
                .expect("clean follower shutdown must finish within its budget");

            let mut compacted = None;
            if force_snapshot {
                nodes[writer].raft.trigger().snapshot().await.unwrap();
                assert!(
                    advance_until(budget, async || {
                        for &(index, boot) in &survivor_boots {
                            nodes[index].assert_running();
                            assert_eq!(nodes[index].replica(), boot);
                            if nodes[index].controls.runtime().current().is_none() {
                                return false;
                            }
                        }
                        compacted = nodes[writer].raft.metrics().borrow_watched().snapshot;
                        compacted.is_some_and(|log| log.index >= history.index)
                    })
                    .await,
                    "leader must finish a snapshot containing the committed history"
                );
                let prefix = compacted.unwrap();
                nodes[writer]
                    .raft
                    .trigger()
                    .purge_log(prefix.index)
                    .await
                    .unwrap();
                assert!(
                    advance_until(budget, async || {
                        for &(index, boot) in &survivor_boots {
                            nodes[index].assert_running();
                            assert_eq!(nodes[index].replica(), boot);
                            if nodes[index].controls.runtime().current().is_none() {
                                return false;
                            }
                        }
                        let state = nodes[writer].state.read().await;
                        state
                            .last_purged_log_id
                            .is_some_and(|log| log.index >= prefix.index)
                            && state.log.range(..=prefix.index).next().is_none()
                    })
                    .await,
                    "snapshot-covered prefix must be unavailable for ordinary log replay"
                );
            }

            let replacement =
                tokio::time::timeout(budget, Node::start(cfg.clone(), ListenerSource::Configured))
                    .await
                    .expect("fresh follower must start within its budget");
            let new_boot = replacement.replica();
            assert_eq!(new_boot.physical_id, old_boot.physical_id);
            assert_ne!(new_boot.boot_nonce, old_boot.boot_nonce);
            {
                let state = replacement.state.read().await;
                assert!(state.genesis.is_none());
                assert!(state.log.is_empty());
                assert!(state.last_applied_log.is_none());
                assert!(state.current_snapshot.is_none());
            }
            nodes[follower] = replacement;
            wait_stable(&mut nodes, Some(&genesis)).await;
            for &(index, boot) in &survivor_boots {
                nodes[index].assert_running();
                assert_eq!(nodes[index].replica(), boot);
            }
            for node in &nodes {
                let state = node.state.read().await;
                assert_eq!(state.genesis.as_ref(), Some(&genesis));
                assert_eq!(state.node_health.get(&writer_id), Some(&true));
                assert!(state.last_applied_log.unwrap().index >= history.index);
                let completed = state.completed_joins.get(&new_boot.physical_id).unwrap();
                assert_eq!(completed.prepared.plan.consumer, new_boot);
                assert_eq!(completed.prepared.plan.previous_voters, previous_voters);
                assert_eq!(completed.prepared.plan.genesis, genesis);
                assert!(
                    completed.prepared.learner_applied.unwrap().index
                        > completed.prepared.prepared.index
                );
                assert!(
                    completed.promoted.index > completed.prepared.learner_applied.unwrap().index
                );
                assert!(
                    !state
                        .last_membership
                        .membership()
                        .voter_ids()
                        .any(|id| id == old_boot)
                );
            }
            if let Some(prefix) = compacted {
                let state = nodes[follower].state.read().await;
                let installed = state
                    .current_snapshot
                    .as_ref()
                    .expect("fresh boot must install a snapshot");
                assert_eq!(installed.genesis.as_ref(), Some(&genesis));
                assert!(installed.last_applied.unwrap().index >= prefix.index);
                assert_eq!(installed.node_health.get(&writer_id), Some(&true));
            }
            survive_renewal(&mut nodes, &genesis).await;
        }
    })
    .catch_unwind()
    .await;
    for node in &mut nodes {
        node.stop().await;
    }
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}
