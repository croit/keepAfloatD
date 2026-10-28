use super::*;
use crate::config::Config;
use crate::consensus_freshness::ConsensusFreshness;
use crate::health::LocalHealth;
use crate::health_publication::RuntimePublisher;
use crate::raft::{FatalReason, KafRaft, KafStorageState, RaftControlTasks, RaftNetworkImpl};
use futures::FutureExt;
use tokio::sync::RwLock;

fn admission_timing() -> crate::runtime_permission::LeaseTiming {
    // Dry-run workers need no kernel cleanup wait, but activation must span a renewal.
    crate::runtime_permission::LeaseTiming::new(Duration::from_secs(10), Duration::from_millis(100))
        .unwrap()
}

fn activation_budget() -> Duration {
    let timing = admission_timing();
    timing.startup_vip_delay().unwrap() + timing.consumer_use() * 2
}

struct ServingNode {
    cfg: Arc<Config>,
    raft: KafRaft,
    network: Arc<RaftNetworkImpl>,
    state: Arc<RwLock<KafStorageState>>,
    controls: RaftControlTasks,
    fatal: mpsc::UnboundedReceiver<FatalReason>,
    network_failure: mpsc::UnboundedReceiver<String>,
    local: Arc<LocalVip>,
    health: Arc<LocalHealth>,
    freshness: Arc<ConsensusFreshness>,
    probe: ControlledProbe,
    tasks: Vec<JoinHandle<()>>,
}

impl ServingNode {
    async fn start(
        cfg: Arc<Config>,
        raft_listener: ListenerSource,
        submit: ListenerSource,
    ) -> Self {
        let table = Arc::new(cfg.sorted_vips());
        let (raft, network, state, fatal, network_failure, controls) =
            crate::raft::start_raft_for_test(
                cfg.clone(),
                table.clone(),
                raft_listener,
                admission_timing(),
            )
            .await
            .unwrap();
        let local = LocalVip::new(true);
        let health = Arc::new(LocalHealth::new(false));
        let freshness = Arc::new(ConsensusFreshness::for_probe_cadence(
            cfg.health.interval_ms,
            cfg.health.effective_stale_missed_probes(),
        ));
        let probe = ControlledProbe::new(true);
        // All fixture workers are aborted and joined before their runtime is stopped.
        let tasks = vec![
            tokio::spawn(crate::health_publication::run_with_probe(
                cfg.clone(),
                health.clone(),
                freshness.clone(),
                RuntimePublisher::new(controls.runtime(), network.clone()),
                probe.clone(),
            )),
            tokio::spawn(crate::vip::run_reconcile_loop(
                cfg.clone(),
                raft.clone(),
                state.clone(),
                local.clone(),
                table,
                health.clone(),
                freshness.clone(),
                cfg.node_id,
            )),
            tokio::spawn({
                let cfg = cfg.clone();
                let raft = raft.clone();
                async move {
                    crate::submit::run_submit_server(cfg, raft, submit)
                        .await
                        .unwrap()
                }
            }),
        ];
        Self {
            cfg,
            raft,
            network,
            state,
            controls,
            fatal,
            network_failure,
            local,
            health,
            freshness,
            probe,
            tasks,
        }
    }

    fn assert_running(&mut self) {
        assert!(matches!(
            self.fatal.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        assert!(matches!(
            self.network_failure.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        assert!(self.controls.recv_failure().now_or_never().is_none());
        assert!(self.tasks.iter().all(|task| !task.is_finished()));
    }

    async fn stop(&mut self) {
        for task in self.tasks.drain(..) {
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
        }
        self.local
            .unbind_all(
                &self.cfg.sorted_vips(),
                None,
                true,
                crate::vip::release_notify_state(true),
            )
            .await
            .unwrap();
        self.controls.shutdown().await.unwrap();
        self.network.shutdown().await.unwrap();
        self.raft.shutdown().await.unwrap();
    }
}

async fn bindings(nodes: &[ServingNode]) -> Vec<(IpAddr, u64)> {
    let mut bindings = Vec::new();
    for node in nodes {
        bindings.extend(
            node.local
                .bound_addrs()
                .await
                .into_iter()
                .map(|ip| (ip, node.cfg.node_id)),
        );
    }
    bindings.sort_unstable();
    bindings
}

async fn all_served(nodes: &[ServingNode], addresses: &[IpAddr]) -> bool {
    bindings(nodes)
        .await
        .iter()
        .map(|(ip, _)| *ip)
        .collect::<Vec<_>>()
        == addresses
}

#[tokio::test(start_paused = true)]
async fn admitted_probe_keeps_local_health_without_early_placement() {
    let mut cluster = ClusterFixture::bind(1).await;
    let address = ip4(192, 0, 2, 90);
    let vips = [VipConfig {
        address: VipAddr::host(address),
        interface: "lo".into(),
        vlan: None,
    }];
    let cfg = cluster.config(0, &vips);
    let budget = activation_budget();
    let mut node = ServingNode::start(cfg, cluster.take_raft(0), cluster.take_submit(0)).await;
    let result = std::panic::AssertUnwindSafe(async {
        assert!(
            advance_until(budget, async || node
                .state
                .read()
                .await
                .node_probe_ticks
                .contains_key(&1))
            .await
        );
        let runtime = node.controls.runtime();
        assert!(!runtime.vip_activation_ready().await);
        assert!(node.health.is_healthy());
        assert!(!node.freshness.is_fresh());
        assert!(node.local.bound_addrs().await.is_empty());
        assert!(
            node.state.read().await.vip_assignments.is_empty(),
            "an admitted healthy probe must not enter placement before activation"
        );
        let original_deadline = runtime.current().unwrap().check().unwrap();
        assert!(
            advance_until(budget, async || {
                node.assert_running();
                if !runtime.vip_activation_ready().await {
                    assert!(node.state.read().await.vip_assignments.is_empty());
                }
                node.local.bound_addrs().await.contains(&address)
            })
            .await,
            "healthy node never activated its VIP"
        );
        assert!(tokio::time::Instant::now() > original_deadline);
        assert!(runtime.current().unwrap().check().unwrap() > original_deadline);
        assert!(node.health.is_healthy());
        node.probe.set(false);
        assert!(
            advance_until(Duration::from_secs(1), async || {
                !node.health.is_healthy() && node.local.bound_addrs().await.is_empty()
            })
            .await,
            "real probe failure did not remove the VIP"
        );
    })
    .catch_unwind()
    .await;
    node.stop().await;
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

pub(in crate::cluster_test) async fn assert_restart_activation_preserves_service() {
    let mut cluster = ClusterFixture::bind(3).await;
    let vips: Vec<_> = (1..=3)
        .map(|suffix| VipConfig {
            address: VipAddr::host(ip4(192, 0, 2, suffix)),
            interface: "lo".into(),
            vlan: None,
        })
        .collect();
    let addresses: Vec<_> = vips.iter().map(|vip| vip.address.addr).collect();
    let mut nodes = Vec::new();
    for index in 0..3 {
        let mut cfg = cluster.config(index, &vips);
        Arc::make_mut(&mut cfg).health.interval_ms = 1_000;
        nodes.push(
            ServingNode::start(cfg, cluster.take_raft(index), cluster.take_submit(index)).await,
        );
    }
    let result = std::panic::AssertUnwindSafe(async {
        let budget = activation_budget();
        assert!(
            advance_until(budget, async || {
                all_served(&nodes, &addresses).await
                    && nodes[0].local.bound_addrs().await.len() == 1
                    && nodes[1].local.bound_addrs().await.len() == 1
                    && nodes[2].local.bound_addrs().await.len() == 1
            })
            .await,
            "initial cluster never served a balanced VIP set"
        );
        let leader = nodes[0].raft.current_leader().await.unwrap();
        let follower = nodes
            .iter()
            .position(|node| node.controls.runtime().local_replica() != leader)
            .unwrap();
        nodes[follower].probe.set(false);
        assert!(
            advance_until(budget, async || {
                !nodes[follower].health.is_healthy()
                    && nodes[follower].local.bound_addrs().await.is_empty()
                    && all_served(&nodes, &addresses).await
            })
            .await,
            "unhealthy holder did not release its VIP to the surviving majority"
        );
        let mut stopped = nodes.remove(follower);
        let old_boot = stopped.controls.runtime().local_replica();
        let cfg = stopped.cfg.clone();
        stopped.stop().await;
        assert!(
            advance_until(budget, async || all_served(&nodes, &addresses).await).await,
            "surviving majority never recovered the stopped holder's VIP"
        );
        let surviving_bindings = bindings(&nodes).await;
        nodes.push(
            ServingNode::start(cfg, ListenerSource::Configured, ListenerSource::Configured).await,
        );
        let runtime = nodes[2].controls.runtime();
        let replacement = runtime.local_replica();
        assert_ne!(replacement, old_boot);
        assert!(
            advance_until(budget, async || {
                nodes[2]
                    .state
                    .read()
                    .await
                    .applied_progress
                    .get(&replacement.physical_id)
                    .is_some_and(|progress| {
                        progress.request.replica == replacement
                            && progress.request.healthy.is_some()
                    })
            })
            .await,
            "replacement never published a real admitted probe"
        );
        assert!(
            !runtime.vip_activation_ready().await,
            "fixture skipped the activation pause"
        );
        let original_deadline = runtime.current().unwrap().check().unwrap();
        let progress_before =
            nodes[2].state.read().await.node_probe_ticks[&replacement.physical_id];
        assert!(
            advance_until(budget, async || {
                if runtime.vip_activation_ready().await {
                    return true;
                }
                for node in &mut nodes {
                    node.assert_running();
                }
                assert!(
                    nodes[2].health.is_healthy(),
                    "activation waiting changed local probe health"
                );
                assert!(
                    !nodes[2].freshness.is_fresh(),
                    "activation waiting armed VIP effects"
                );
                assert_eq!(
                    bindings(&nodes).await,
                    surviving_bindings,
                    "activation-ineligible replacement withdrew a served VIP: {:?}",
                    nodes[0].state.read().await.vip_assignments
                );
                false
            })
            .await,
            "replacement never reached activation readiness"
        );
        assert!(
            tokio::time::Instant::now() > original_deadline,
            "fixture did not cross the original admission deadline"
        );
        assert!(
            runtime.current().unwrap().check().unwrap() > original_deadline,
            "activation waiting must keep renewing admission"
        );
        assert!(
            nodes[2].state.read().await.node_probe_ticks[&replacement.physical_id]
                > progress_before,
            "activation waiting must keep committing probe progress"
        );
        assert!(
            advance_until(Duration::from_secs(10), async || {
                all_served(&nodes, &addresses).await
                    && nodes[2].local.bound_addrs().await.len() == 1
            })
            .await,
            "ready replacement did not join placement"
        );
        nodes[2].probe.set(false);
        assert!(
            advance_until(Duration::from_secs(10), async || {
                nodes[2].local.bound_addrs().await.is_empty()
                    && all_served(&nodes[..2], &addresses).await
            })
            .await,
            "real local health loss did not withdraw and hand off the VIP"
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
