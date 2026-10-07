//! Raft cluster (OpenRaft 0.10) over a small TCP/JSON framing layer.

pub mod admission;
mod control;
mod guard;
pub mod network;
pub mod probe;
pub mod store;
mod tasks;
pub mod types;

pub use network::RaftNetworkImpl;
pub use store::{KafStateMachine, KafStorageState};
pub use types::{FailoverSemantics, KafRequest, TypeConfig};

use crate::config::{Config, VipAddr};
use admission::runtime::RuntimeDriver;
use anyhow::Context;
use control::run_cluster_guard;
use openraft::Raft;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;

/// openraft 0.10 makes `Raft` generic over the state-machine type, so the alias must name our
/// state-machine half. The log-storage half is erased behind the `Raft::new` `LS` type parameter.
pub type KafRaft = Raft<TypeConfig, KafStateMachine>;

const CONTROL_TASK_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);

/// Consecutive guard rounds that must all observe a foreign majority before this node resets. The
/// hold-down avoids acting on a single transient probe round.
const GUARD_STRIKES_TO_RESET: u32 = 3;

/// Fatal consensus-safety condition that requires the composition root to stop all work, unbind
/// VIPs, and then return a non-zero process status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FatalReason {
    StaleSurvivor,
    ConfigMismatch,
}

impl FatalReason {
    #[must_use]
    pub const fn exit_code(self) -> i32 {
        match self {
            Self::StaleSurvivor => 3,
            Self::ConfigMismatch => 4,
        }
    }
}

pub struct RaftControlTasks {
    runtime: Arc<RuntimeDriver>,
    shutdown: Arc<AtomicBool>,
    tasks: Vec<tasks::SupervisedTask>,
    failure_rx: mpsc::UnboundedReceiver<String>,
}

impl RaftControlTasks {
    pub(crate) fn runtime(&self) -> Arc<RuntimeDriver> {
        self.runtime.clone()
    }

    pub async fn recv_failure(&mut self) -> Option<String> {
        self.failure_rx.recv().await
    }

    pub async fn shutdown(&mut self) -> anyhow::Result<()> {
        self.shutdown.store(true, Ordering::SeqCst);
        self.runtime.shutdown();
        tasks::stop_supervised_tasks(
            std::mem::take(&mut self.tasks),
            CONTROL_TASK_SHUTDOWN_TIMEOUT,
        )
        .await
    }
}

/// Build the Raft runtime, start the peer listener, and drive automatic cluster formation.
///
/// `vip_list` must be **identical** on every node (sorted by VIP address). Storage is in-memory for
/// v1. The state machine is constructed with the daemon's effective health probe staleness window
/// so that eligibility and handoff fencing are applied deterministically across all members.
pub async fn start_raft(
    cfg: Arc<Config>,
    vip_list: Arc<Vec<(VipAddr, String)>>,
    listener: crate::listener::ListenerSource,
) -> anyhow::Result<(
    KafRaft,
    Arc<RaftNetworkImpl>,
    Arc<tokio::sync::RwLock<KafStorageState>>,
    mpsc::UnboundedReceiver<FatalReason>,
    mpsc::UnboundedReceiver<String>,
    RaftControlTasks,
)> {
    let raft_cfg = build_openraft_config(&cfg)?;
    let raft_cfg = Arc::new(raft_cfg);

    // openraft 0.10 takes the log-storage and state-machine halves as two separate values (no
    // `Adaptor`). Both share one volatile in-memory state; `state_ref` is the third handle used by
    // the transport (epoch fencing) and the VIP reconciliation loop.
    let (log_store, state_machine, state_ref) = store::new_admitted_store(
        vip_list,
        cfg.health.effective_stale_missed_probes(),
        cfg.failback,
        cfg.effective_failback_delay_ticks(),
    );
    let timing = crate::runtime_permission::LeaseTiming::for_config(&cfg, cfg.vips.len())?;
    tracing::info!(
        quarantine_ms = timing.restart_quarantine().as_millis(),
        vip_activation_ms = (timing.startup_vip_delay()? - timing.restart_quarantine()).as_millis(),
        startup_safety_wait_ms = timing.startup_vip_delay()?.as_millis(),
        "runtime admission timing"
    );
    let admission = RuntimeDriver::new(
        cfg.clone(),
        state_ref.clone(),
        timing,
        tokio::time::Instant::now(),
    )?;
    let local_replica = admission.local_replica();
    let network = Arc::new(RaftNetworkImpl::new(
        cfg.clone(),
        state_ref.clone(),
        admission.clone(),
    )?);

    let raft = Raft::new(
        local_replica,
        raft_cfg,
        network.as_ref().clone(),
        log_store,
        state_machine,
    )
    .await
    .map_err(|e| anyhow::anyhow!("Raft::new: {:?}", e))?;
    admission.attach(raft.clone())?;

    // Start the transport first so peers can be probed and inbound status probes can be answered,
    // then drive automatic cluster formation.
    let network_failure_rx = network
        .start_with_listener(raft.clone(), listener)
        .await
        .context("raft network start")?;
    let (fatal_tx, fatal_rx) = mpsc::unbounded_channel();
    let control_shutdown = Arc::new(AtomicBool::new(false));
    let (control_failure_tx, control_failure_rx) = mpsc::unbounded_channel();
    let mut control_tasks = Vec::new();

    // Lifetime: admission stays supervised until shutdown and expiry stops the daemon.
    {
        let network = network.clone();
        let admission = admission.clone();
        control_tasks.push(tasks::spawn_supervised_task(
            "runtime admission task",
            control_shutdown.clone(),
            control_failure_tx.clone(),
            tasks::CleanExit::Unexpected,
            async move { admission.run(network).await },
        ));
    }

    // Lifetime: runs until shutdown. Resets this node (by exiting for a supervisor restart) if it
    // becomes a stale survivor of a cluster that reformed without it.
    {
        let cfg = cfg.clone();
        let network = network.clone();
        let state_ref = state_ref.clone();
        control_tasks.push(tasks::spawn_supervised_task(
            "stale-survivor guard task",
            control_shutdown.clone(),
            control_failure_tx,
            tasks::CleanExit::Allowed,
            async move {
                run_cluster_guard(cfg, network, state_ref, fatal_tx).await;
                Ok(())
            },
        ));
    }

    Ok((
        raft,
        network,
        state_ref,
        fatal_rx,
        network_failure_rx,
        RaftControlTasks {
            runtime: admission,
            shutdown: control_shutdown,
            tasks: control_tasks,
            failure_rx: control_failure_rx,
        },
    ))
}

fn build_openraft_config(cfg: &Config) -> anyhow::Result<openraft::Config> {
    openraft::Config {
        election_timeout_min: cfg.raft.election_timeout_min_ms,
        election_timeout_max: cfg.raft.election_timeout_max_ms,
        heartbeat_interval: cfg.raft.heartbeat_interval_ms,
        enable_pre_vote: Some(true),
        // Diskless restarts replay the live leader's log. Keep each unary AppendEntries batch
        // comfortably inside the heartbeat-derived RPC budget so a large catch-up batch cannot
        // time out, reconnect, and retry forever without advancing.
        max_payload_entries: 32,
        // A restarted follower has a new boot identity and joins as a new learner.
        allow_log_reversion: Some(false),
        ..Default::default()
    }
    .validate()
    .map_err(|e| anyhow::anyhow!("openraft config validate: {e}"))
}

#[cfg(test)]
mod tests {
    use super::{FailoverSemantics, build_openraft_config};
    use crate::config::{Config, HealthConfig, PeerConfig, RaftTuneConfig};
    use std::time::Duration;
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;

    fn control_test_config(second: String, third: String) -> Config {
        Config {
            node_id: 1,
            raft_listen: "127.0.0.1:17001".into(),
            client_submit_listen: "127.0.0.1:18001".into(),
            peers: vec![
                PeerConfig {
                    id: 1,
                    raft_address: "127.0.0.1:17001".into(),
                    client_submit_address: "127.0.0.1:18001".into(),
                },
                PeerConfig {
                    id: 2,
                    raft_address: second,
                    client_submit_address: "127.0.0.1:18002".into(),
                },
                PeerConfig {
                    id: 3,
                    raft_address: third,
                    client_submit_address: "127.0.0.1:18003".into(),
                },
            ],
            vips: Vec::new(),
            health: HealthConfig {
                command: vec!["/bin/true".into()],
                interval_ms: 1_000,
                timeout_ms: 500,
                stale_secs: Some(3),
            },
            raft: RaftTuneConfig::default(),
            cluster_secret: Some("cluster-test-secret-01234567890123".into()),
            cluster_secret_file: None,
            max_frame_bytes: 64 * 1024,
            submit_timeout_ms: 5_000,
            address_protocol: crate::config::DEFAULT_VIP_ADDRESS_PROTOCOL,
            dry_run: true,
            notify: None,
            failover_delay_secs: 0,
            failback: true,
            failback_delay_secs: 0,
        }
    }

    #[test]
    fn exact_boot_replicas_disallow_follower_log_reversion() {
        let config = control_test_config("127.0.0.1:17002".into(), "127.0.0.1:17003".into());
        let raft = build_openraft_config(&config).unwrap();

        assert_eq!(raft.allow_log_reversion, Some(false));
    }

    #[test]
    fn diskless_replay_uses_bounded_append_payloads() {
        let config = control_test_config("127.0.0.1:17002".into(), "127.0.0.1:17003".into());
        let raft = build_openraft_config(&config).unwrap();

        assert_eq!(raft.max_payload_entries, 32);
    }

    #[tokio::test]
    async fn raft_control_tasks_report_unexpected_exit_and_own_shutdown() {
        let runtime = || {
            let config = std::sync::Arc::new(control_test_config(
                "127.0.0.1:17002".into(),
                "127.0.0.1:17003".into(),
            ));
            let (_, _, state) =
                super::store::new_store(std::sync::Arc::new(Vec::new()), 3, true, 0);
            let timing = crate::runtime_permission::LeaseTiming::for_config(&config, 0).unwrap();
            super::RuntimeDriver::new(config, state, timing, tokio::time::Instant::now()).unwrap()
        };
        let shutdown = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (failure_tx, failure_rx) = mpsc::unbounded_channel();
        let unexpected = super::tasks::spawn_supervised_task(
            "runtime admission task",
            shutdown.clone(),
            failure_tx,
            super::tasks::CleanExit::Unexpected,
            async { Ok(()) },
        );
        let mut controls = super::RaftControlTasks {
            runtime: runtime(),
            shutdown,
            tasks: vec![unexpected],
            failure_rx,
        };

        assert!(
            controls
                .recv_failure()
                .await
                .unwrap()
                .contains("runtime admission task exited unexpectedly")
        );
        assert!(
            controls
                .shutdown()
                .await
                .unwrap_err()
                .to_string()
                .contains("runtime admission task")
        );

        let shutdown = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (failure_tx, failure_rx) = mpsc::unbounded_channel();
        let expected = super::tasks::spawn_supervised_task(
            "cluster guard",
            shutdown.clone(),
            failure_tx,
            super::tasks::CleanExit::Allowed,
            async { Ok(()) },
        );
        let mut controls = super::RaftControlTasks {
            runtime: runtime(),
            shutdown,
            tasks: vec![expected],
            failure_rx,
        };
        tokio::task::yield_now().await;

        assert!(controls.failure_rx.try_recv().is_err());
        controls.shutdown().await.unwrap();
    }

    #[test]
    fn fatal_reasons_keep_distinct_supervisor_exit_codes() {
        assert_eq!(super::FatalReason::StaleSurvivor.exit_code(), 3);
        assert_eq!(super::FatalReason::ConfigMismatch.exit_code(), 4);
    }

    async fn runtime_control_node() -> (
        std::sync::Arc<Config>,
        super::KafRaft,
        std::sync::Arc<super::RaftNetworkImpl>,
        std::sync::Arc<tokio::sync::RwLock<super::KafStorageState>>,
        super::RaftControlTasks,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let mut config = control_test_config(String::new(), String::new());
        config.peers.truncate(1);
        config.raft_listen = address.clone();
        config.peers[0].raft_address = address;
        let config = std::sync::Arc::new(config);
        let (raft, network, state, _, _, controls) = super::start_raft(
            config.clone(),
            std::sync::Arc::new(Vec::new()),
            crate::listener::ListenerSource::Bound(listener),
        )
        .await
        .unwrap();
        (config, raft, network, state, controls)
    }

    async fn wait_for_runtime_genesis(
        raft: &super::KafRaft,
        state: &tokio::sync::RwLock<super::KafStorageState>,
    ) {
        use openraft::async_runtime::WatchReceiver;

        tokio::time::timeout(Duration::from_secs(2), async {
            let mut metrics = raft.metrics();
            loop {
                let _ = metrics.borrow_watched();
                if state.read().await.genesis.is_some() {
                    return;
                }
                metrics.changed().await.unwrap();
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn runtime_controls_initialize_exact_genesis_and_both_capabilities() {
        let (config, raft, network, state, mut controls) = runtime_control_node().await;
        let runtime = controls.runtime();
        let local = runtime.local_replica();
        let timing = crate::runtime_permission::LeaseTiming::for_config(&config, 0).unwrap();
        assert!(runtime.current().is_none());
        assert!(!raft.is_initialized().await.unwrap());
        assert!(state.read().await.log.is_empty());

        tokio::time::advance(timing.restart_quarantine() - Duration::from_nanos(1)).await;
        assert!(runtime.current().is_none());
        assert!(!raft.is_initialized().await.unwrap());
        tokio::time::advance(Duration::from_nanos(1)).await;
        wait_for_runtime_genesis(&raft, &state).await;

        let genesis = runtime.current().unwrap().context().genesis.clone();
        assert_eq!(genesis.config, config.cluster_config_fingerprint().unwrap());
        assert_eq!(genesis.voters, [local].into());
        {
            let state = state.read().await;
            assert_eq!(state.genesis.as_ref(), Some(&genesis));
            assert_eq!(state.cluster_epoch, Some(genesis.epoch));
            assert_eq!(state.failover_semantics, FailoverSemantics::V2);
            assert!(state.config_identity_enforced);
            assert!(state.node_health.is_empty());
        }

        let repeated = raft
            .client_write(super::KafRequest::AdmissionGenesis(genesis.clone()))
            .await
            .unwrap();
        assert!(matches!(repeated.data, super::types::KafResponse::Ok));
        let mut foreign = genesis.clone();
        foreign.epoch ^= 1;
        let rejected = raft
            .client_write(super::KafRequest::AdmissionGenesis(foreign))
            .await
            .unwrap();
        assert!(matches!(
            rejected.data,
            super::types::KafResponse::Rejected(_)
        ));
        assert_eq!(state.read().await.genesis.as_ref(), Some(&genesis));

        controls.shutdown().await.unwrap();
        network.shutdown().await.unwrap();
        raft.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn runtime_control_shutdown_seals_before_task_cleanup() {
        use super::network::authorization::AdmissionController;

        let (_, raft, network, state, mut controls) = runtime_control_node().await;
        let runtime = controls.runtime();
        let local = runtime.local_replica();
        let started = tokio::time::Instant::now();
        controls.shutdown().await.unwrap();

        assert_eq!(tokio::time::Instant::now(), started);
        assert!(controls.tasks.is_empty());
        assert!(runtime.current().is_none());
        assert!(runtime.authorize_raft(local).is_err());
        assert!(runtime.attach(raft.clone()).is_err());
        assert!(state.read().await.genesis.is_none());
        assert!(state.read().await.log.is_empty());
        assert!(!raft.is_initialized().await.unwrap());
        assert!(controls.failure_rx.try_recv().is_err());
        controls.shutdown().await.unwrap();
        network.shutdown().await.unwrap();
        raft.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn runtime_control_shutdown_does_not_report_a_woken_worker_as_failed() {
        for _ in 0..64 {
            let (_, raft, network, _, mut controls) = runtime_control_node().await;
            tokio::task::yield_now().await;
            controls.shutdown().await.unwrap();
            assert!(controls.failure_rx.try_recv().is_err());
            network.shutdown().await.unwrap();
            raft.shutdown().await.unwrap();
        }
    }

    #[tokio::test(start_paused = true)]
    async fn runtime_controls_report_driver_exit_as_fatal() {
        use super::network::authorization::AdmissionController;

        let (_, raft, network, state, mut controls) = runtime_control_node().await;
        let runtime = controls.runtime();
        tokio::task::yield_now().await;
        network.shutdown().await.unwrap();
        let failure = tokio::time::timeout(Duration::from_secs(1), controls.recv_failure())
            .await
            .unwrap()
            .unwrap();
        assert!(failure.contains("runtime admission task exited unexpectedly"));
        assert!(runtime.current().is_none());
        assert!(runtime.authorize_raft(runtime.local_replica()).is_err());
        assert!(runtime.attach(raft.clone()).is_err());
        assert!(state.read().await.genesis.is_none());
        assert!(!raft.is_initialized().await.unwrap());
        assert!(
            controls
                .shutdown()
                .await
                .unwrap_err()
                .to_string()
                .contains("runtime admission task")
        );
        raft.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn cluster_guard_reports_coherent_foreign_epoch_after_hold_down() {
        let mut listeners = Vec::new();
        for _ in 0..3 {
            listeners.push(TcpListener::bind("127.0.0.1:0").await.unwrap());
        }
        let addresses: Vec<_> = listeners
            .iter()
            .map(|listener| listener.local_addr().unwrap().to_string())
            .collect();
        let mut cfg = control_test_config(addresses[1].clone(), addresses[2].clone());
        cfg.raft_listen = addresses[0].clone();
        cfg.peers[0].raft_address = addresses[0].clone();
        let cfg = std::sync::Arc::new(cfg);
        let mut nodes = Vec::new();
        for (index, listener) in listeners.into_iter().enumerate() {
            let mut peer = (*cfg).clone();
            peer.node_id = index as u64 + 1;
            peer.raft_listen = addresses[index].clone();
            let node = super::network::testing::start_transport(
                std::sync::Arc::new(peer),
                crate::listener::ListenerSource::Bound(listener),
            )
            .await;
            node.2.write().await.cluster_epoch = Some(if index == 0 { 11 } else { 22 });
            nodes.push(node);
        }
        let (fatal_tx, mut fatal_rx) = mpsc::unbounded_channel();
        let guard = tokio::spawn(super::run_cluster_guard(
            cfg,
            nodes[0].1.clone(),
            nodes[0].2.clone(),
            fatal_tx,
        ));
        let start = tokio::time::Instant::now();
        let verdict = loop {
            for _ in 0..10 {
                tokio::task::yield_now().await;
            }
            if let Ok(verdict) = fatal_rx.try_recv() {
                break verdict;
            }
            assert!(start.elapsed() < Duration::from_secs(8));
            tokio::time::advance(Duration::from_millis(10)).await;
        };
        assert_eq!(verdict, super::FatalReason::StaleSurvivor);
        assert!(start.elapsed() >= super::control::GUARD_POLL_INTERVAL * 3);
        guard.await.unwrap();
        for (raft, network, state) in nodes {
            assert!(!raft.is_initialized().await.unwrap());
            assert!(state.read().await.log.is_empty());
            network.shutdown().await.unwrap();
            raft.shutdown().await.unwrap();
        }
    }

    #[tokio::test(start_paused = true)]
    async fn cluster_guard_exits_when_network_has_stopped() {
        let (config, raft, network, state, mut controls) = runtime_control_node().await;
        controls.shutdown().await.unwrap();
        network.shutdown().await.unwrap();
        let (fatal_tx, mut fatal_rx) = mpsc::unbounded_channel();
        tokio::time::timeout(
            Duration::from_secs(2),
            super::run_cluster_guard(config, network, state, fatal_tx),
        )
        .await
        .unwrap();
        assert!(fatal_rx.try_recv().is_err());
        raft.shutdown().await.unwrap();
    }
}
