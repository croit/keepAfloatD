//! Raft cluster (OpenRaft 0.10) over a small TCP/JSON framing layer.

mod formation;
mod guard;
pub mod network;
pub mod probe;
pub mod store;
mod tasks;
pub mod types;

pub use network::RaftNetworkImpl;
pub use store::{KafStateMachine, KafStorageState};
pub use types::{FailoverSemantics, KafRequest, TypeConfig};

use crate::config::{ClusterConfigFingerprint, Config, VipAddr};
use anyhow::Context;
// `WatchReceiver` provides `borrow_watched()` on the metrics watch handle (0.10 renamed the 0.9
// `borrow()`); it must be in scope for the method to resolve.
use openraft::Raft;
use openraft::async_runtime::WatchReceiver;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::{RwLock, mpsc};

/// openraft 0.10 makes `Raft` generic over the state-machine type, so the alias must name our
/// state-machine half. The log-storage half is erased behind the `Raft::new` `LS` type parameter.
pub type KafRaft = Raft<TypeConfig, KafStateMachine>;

/// How often the epoch minter checks whether it must commit the cluster incarnation, and how often
/// the stale-survivor guard re-probes peers.
const GUARD_POLL_INTERVAL: Duration = Duration::from_millis(1000);

/// Per-probe wall-clock budget for the stale-survivor guard.
const GUARD_PROBE_TIMEOUT: Duration = Duration::from_secs(1);

/// Consecutive guard rounds that must all observe a foreign majority before this node resets. The
/// hold-down avoids acting on a single transient probe round.
const GUARD_STRIKES_TO_RESET: u32 = 3;

const CONTROL_TASK_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);

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
    shutdown: Arc<AtomicBool>,
    tasks: Vec<tasks::SupervisedTask>,
    failure_rx: mpsc::UnboundedReceiver<String>,
}

impl RaftControlTasks {
    pub async fn recv_failure(&mut self) -> Option<String> {
        self.failure_rx.recv().await
    }

    pub async fn shutdown(&mut self) -> anyhow::Result<()> {
        self.shutdown.store(true, Ordering::SeqCst);
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
    let (log_store, state_machine, state_ref) = store::new_store(
        vip_list,
        cfg.health.effective_stale_missed_probes(),
        cfg.failback,
        cfg.effective_failback_delay_ticks(),
    );
    let network = Arc::new(RaftNetworkImpl::new(cfg.clone(), state_ref.clone())?);

    let raft = Raft::new(
        cfg.node_id,
        raft_cfg,
        network.as_ref().clone(),
        log_store,
        state_machine,
    )
    .await
    .map_err(|e| anyhow::anyhow!("Raft::new: {:?}", e))?;

    // Start the transport first so peers can be probed and inbound status probes can be answered,
    // then drive automatic cluster formation.
    let network_failure_rx = network
        .start(raft.clone())
        .await
        .context("raft network start")?;
    let (fatal_tx, fatal_rx) = mpsc::unbounded_channel();
    let control_shutdown = Arc::new(AtomicBool::new(false));
    let (control_failure_tx, control_failure_rx) = mpsc::unbounded_channel();
    let mut control_tasks = Vec::new();

    // Lifetime: runs until the cluster is formed or an existing one is discovered, or until network
    // shutdown is requested. Spawned (not awaited) so startup never blocks waiting for a quorum.
    {
        let cfg = cfg.clone();
        let raft = raft.clone();
        let network = network.clone();
        let fatal_tx = fatal_tx.clone();
        control_tasks.push(tasks::spawn_supervised_task(
            "cluster formation task",
            control_shutdown.clone(),
            control_failure_tx.clone(),
            tasks::CleanExit::Allowed,
            async move {
                formation::auto_form_cluster(cfg, raft, network, fatal_tx).await;
                Ok(())
            },
        ));
    }

    // Lifetime: runs until shutdown. Commits the per-formation cluster incarnation once this node
    // leads a freshly formed cluster that has none yet.
    {
        let raft = raft.clone();
        let network = network.clone();
        let state_ref = state_ref.clone();
        let node_id = cfg.node_id;
        let cfg = cfg.clone();
        control_tasks.push(tasks::spawn_supervised_task(
            "epoch minter task",
            control_shutdown.clone(),
            control_failure_tx.clone(),
            tasks::CleanExit::Unexpected,
            async move {
                run_epoch_minter(cfg, raft, network, state_ref, node_id).await;
                Ok(())
            },
        ));
    }

    // Lifetime: runs until shutdown. Existing legacy clusters switch only after every configured
    // voter advertises support for the V2-only replicated activation command.
    {
        let cfg = cfg.clone();
        let raft = raft.clone();
        let network = network.clone();
        let state_ref = state_ref.clone();
        let node_id = cfg.node_id;
        control_tasks.push(tasks::spawn_supervised_task(
            "semantics activator task",
            control_shutdown.clone(),
            control_failure_tx.clone(),
            tasks::CleanExit::Unexpected,
            async move {
                run_semantics_activator(cfg, raft, network, state_ref, node_id).await;
                Ok(())
            },
        ));
    }

    // Lifetime: runs until shutdown. Resets this node (by exiting for a supervisor restart) if it
    // becomes a stale survivor of a cluster that reformed without it.
    {
        let cfg = cfg.clone();
        let raft = raft.clone();
        let network = network.clone();
        let state_ref = state_ref.clone();
        control_tasks.push(tasks::spawn_supervised_task(
            "stale-survivor guard task",
            control_shutdown.clone(),
            control_failure_tx,
            tasks::CleanExit::Allowed,
            async move {
                run_cluster_guard(cfg, raft, network, state_ref, fatal_tx).await;
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
        // Diskless restarts replay the live leader's log. Keep each unary AppendEntries batch
        // comfortably inside the heartbeat-derived RPC budget so a large catch-up batch cannot
        // time out, reconnect, and retry forever without advancing.
        max_payload_entries: 32,
        // Nodes are intentionally diskless and restart with empty logs. Let the leader reset its
        // remembered follower progress so it can replay the committed state after a restart.
        allow_log_reversion: Some(true),
        ..Default::default()
    }
    .validate()
    .map_err(|e| anyhow::anyhow!("openraft config validate: {e}"))
}

/// Mint a per-formation cluster incarnation from 16 bytes of kernel entropy. Linux-only daemon, so
/// reading `/dev/urandom` directly avoids pulling in an RNG dependency.
fn mint_cluster_id() -> std::io::Result<u128> {
    use std::io::Read;
    let mut buf = [0u8; 16];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut buf)?;
    Ok(u128::from_be_bytes(buf))
}

async fn await_control_write<F, T>(
    timeout: Duration,
    write: F,
) -> Result<T, tokio::time::error::Elapsed>
where
    F: std::future::Future<Output = T>,
{
    tokio::time::timeout(timeout, write).await
}

fn select_initial_failover_semantics(
    total_voters: usize,
    reachable_voters: usize,
    all_reachable_support_v2: bool,
) -> FailoverSemantics {
    if probe::fresh_formation_supports_v2(total_voters, reachable_voters, all_reachable_support_v2)
    {
        FailoverSemantics::V2
    } else {
        FailoverSemantics::Legacy
    }
}

/// Commit the cluster incarnation exactly once, when this node leads a cluster that has none yet.
///
/// Only the leader writes; the state machine keeps the first committed value, so leader churn or
/// concurrent attempts cannot change a cluster's incarnation. Polls rather than waiting on metrics
/// edges so a transient `client_write` failure (e.g. momentary loss of leadership) is simply
/// retried on the next tick.
async fn run_epoch_minter(
    cfg: Arc<Config>,
    raft: KafRaft,
    network: Arc<RaftNetworkImpl>,
    state_ref: Arc<RwLock<KafStorageState>>,
    node_id: u64,
) {
    loop {
        if network.is_shutting_down() {
            return;
        }
        let is_leader = raft.metrics().borrow_watched().current_leader == Some(node_id);
        let needs_epoch = is_leader && state_ref.read().await.cluster_epoch.is_none();
        if needs_epoch {
            let (reachable, all_v2, all_config_identity) =
                probe_capabilities(&cfg, None, network.config_fingerprint()).await;
            if reachable < cfg.peers.len() / 2 + 1 {
                tokio::time::sleep(GUARD_POLL_INTERVAL).await;
                continue;
            }
            let failover_semantics =
                select_initial_failover_semantics(cfg.peers.len(), reachable, all_v2);
            let config_identity_enforced = probe::fresh_formation_supports_config_identity(
                cfg.peers.len(),
                reachable,
                all_config_identity,
            );
            match mint_cluster_id() {
                Ok(cluster_id) => {
                    match await_control_write(
                        Duration::from_millis(cfg.submit_timeout_ms),
                        raft.client_write(KafRequest::ClusterFormed {
                            cluster_id,
                            failover_semantics,
                            config_identity_enforced,
                        }),
                    )
                    .await
                    {
                        Ok(Ok(_)) => {
                            tracing::info!("committed cluster incarnation {:#034x}", cluster_id);
                            if failover_semantics == FailoverSemantics::V2 {
                                network.drop_legacy_outbound().await;
                            }
                            if config_identity_enforced {
                                network.drop_legacy_config_outbound().await;
                            }
                        }
                        // Benign: lost leadership between the check and the write, or no quorum yet;
                        // retried on the next tick.
                        Ok(Err(e)) => tracing::warn!("commit cluster incarnation: {:?}", e),
                        Err(_) => tracing::warn!(
                            "commit cluster incarnation timed out after {}ms",
                            cfg.submit_timeout_ms
                        ),
                    }
                }
                Err(e) => tracing::error!("mint cluster incarnation: {}", e),
            }
        }
        tokio::time::sleep(GUARD_POLL_INTERVAL).await;
    }
}

async fn probe_capabilities(
    cfg: &Config,
    local_epoch: Option<u128>,
    local_fingerprint: ClusterConfigFingerprint,
) -> (usize, bool, bool) {
    let mut reachable = 1usize;
    let mut all_v2 = true;
    let mut all_config_identity = true;
    for peer in cfg.other_peers() {
        match network::probe_peer_status(
            &cfg.raft_listen,
            &peer.raft_address,
            cfg.node_id,
            cfg.cluster_secret.as_deref(),
            local_epoch,
            local_fingerprint,
            GUARD_PROBE_TIMEOUT,
        )
        .await
        {
            Ok(resp)
                if !resp.reports_foreign_epoch(local_epoch)
                    && !resp.reports_foreign_config(local_fingerprint) =>
            {
                reachable += 1;
                all_v2 &= resp.supports_failover_semantics_v2;
                all_config_identity &= resp.supports_config_identity_v1
                    && resp.config_fingerprint == Some(local_fingerprint);
            }
            // #26: an incompatible peer is outside this candidate formation. It must neither
            // count as reachable nor downgrade the capabilities of the matching quorum.
            Ok(_) => {}
            Err(_) => {}
        }
    }
    (reachable, all_v2, all_config_identity)
}

async fn run_semantics_activator(
    cfg: Arc<Config>,
    raft: KafRaft,
    network: Arc<RaftNetworkImpl>,
    state_ref: Arc<RwLock<KafStorageState>>,
    node_id: u64,
) {
    loop {
        tokio::time::sleep(GUARD_POLL_INTERVAL).await;
        if network.is_shutting_down() {
            return;
        }
        if raft.metrics().borrow_watched().current_leader != Some(node_id) {
            continue;
        }
        let (epoch, semantics, config_identity_enforced) = {
            let state = state_ref.read().await;
            (
                state.cluster_epoch,
                state.failover_semantics,
                state.config_identity_enforced,
            )
        };
        let Some(epoch) = epoch else {
            continue;
        };
        if semantics == FailoverSemantics::V2 && config_identity_enforced {
            continue;
        }
        let (reachable, all_v2, all_config_identity) =
            probe_capabilities(&cfg, Some(epoch), network.config_fingerprint()).await;
        if semantics != FailoverSemantics::V2
            && probe::all_voters_support_v2(cfg.peers.len(), reachable, all_v2)
        {
            match await_control_write(
                Duration::from_millis(cfg.submit_timeout_ms),
                raft.client_write(KafRequest::EnableFailoverSemanticsV2),
            )
            .await
            {
                Ok(Ok(_)) => {
                    network.drop_legacy_outbound().await;
                    tracing::info!("activated failover semantics V2 after all voters upgraded");
                }
                Ok(Err(error)) => tracing::warn!("activate failover semantics V2: {:?}", error),
                Err(_) => tracing::warn!(
                    "activate failover semantics V2 timed out after {}ms",
                    cfg.submit_timeout_ms
                ),
            }
        }
        if !config_identity_enforced
            && probe::all_voters_support_config_identity(
                cfg.peers.len(),
                reachable,
                all_config_identity,
            )
        {
            match await_control_write(
                Duration::from_millis(cfg.submit_timeout_ms),
                raft.client_write(KafRequest::EnableConfigIdentityV1),
            )
            .await
            {
                Ok(Ok(_)) => {
                    network.drop_legacy_config_outbound().await;
                    tracing::info!(
                        "activated cluster config identity after all voters advertised one matching fingerprint"
                    );
                }
                Ok(Err(error)) => tracing::warn!("activate cluster config identity: {:?}", error),
                Err(_) => tracing::warn!(
                    "activate cluster config identity timed out after {}ms",
                    cfg.submit_timeout_ms
                ),
            }
        }
    }
}

/// Detect that this node is a **stale survivor** - it still holds an old incarnation while a
/// majority of the roster has reformed under a new one - and reset by exiting for a supervisor
/// restart (returning blank, it rejoins via replication like any diskless reboot).
///
/// Safety of the reset rests on the trigger: the node must (1) hold a committed incarnation,
/// (2) currently have no leader, and (3) see a **majority of the whole roster** report the same
/// *different* concrete incarnation. Condition (3) is what proves this node is the minority that
/// one coherent cluster reformed around - it can hold nothing committed by that majority, so
/// discarding its state loses nothing. Distinct foreign incarnations never add into that proof.
/// A healthy follower in a normal election shares its peers' incarnation, so it never triggers.
async fn run_cluster_guard(
    cfg: Arc<Config>,
    raft: KafRaft,
    network: Arc<RaftNetworkImpl>,
    state_ref: Arc<RwLock<KafStorageState>>,
    fatal_tx: mpsc::UnboundedSender<FatalReason>,
) {
    let total = cfg.peers.len();
    let others: Vec<(u64, String)> = cfg
        .other_peers()
        .into_iter()
        .map(|p| (p.id, p.raft_address.clone()))
        .collect();
    let mut guard = guard::ClusterGuard::new(total, GUARD_STRIKES_TO_RESET);
    let local_fingerprint = network.config_fingerprint();
    loop {
        tokio::time::sleep(GUARD_POLL_INTERVAL).await;
        if network.is_shutting_down() {
            return;
        }
        let local_epoch = state_ref.read().await.cluster_epoch;
        let mut observed_epochs = Vec::new();
        let mut foreign_config_identities = Vec::new();
        for (_peer_id, addr) in &others {
            if let Ok(resp) = network::probe_peer_status(
                &cfg.raft_listen,
                addr,
                cfg.node_id,
                cfg.cluster_secret.as_deref(),
                local_epoch,
                local_fingerprint,
                GUARD_PROBE_TIMEOUT,
            )
            .await
            {
                observed_epochs.push(resp.cluster_epoch);
                foreign_config_identities.push(resp.config_fingerprint);
            }
        }
        let round = guard::GuardRound {
            local_epoch_known: local_epoch.is_some(),
            has_leader: raft.metrics().borrow_watched().current_leader.is_some(),
            foreign_config: probe::largest_foreign_identity_group(
                local_fingerprint,
                foreign_config_identities,
            ),
            foreign_epoch: probe::largest_foreign_epoch_group(local_epoch, observed_epochs),
        };
        // Config fencing is independent of epoch fencing: a mismatched minority must stop even
        // when it has not joined an incarnation, while epoch fencing needs an initialized,
        // leaderless survivor.
        let verdict = guard.observe(round);
        if round.foreign_config >= guard.majority() {
            tracing::warn!(
                "cluster configuration mismatch: {} of {} peers report a different fingerprint ({}/{} strikes)",
                round.foreign_config,
                total,
                guard.config_strikes(),
                GUARD_STRIKES_TO_RESET
            );
        }
        if round.local_epoch_known && !round.has_leader && round.foreign_epoch >= guard.majority() {
            tracing::warn!(
                "stale cluster incarnation: {} of {} peers report one coherent different cluster ({}/{} strikes)",
                round.foreign_epoch,
                total,
                guard.epoch_strikes(),
                GUARD_STRIKES_TO_RESET
            );
        }
        match verdict {
            guard::GuardVerdict::Continue => {}
            guard::GuardVerdict::FenceConfig => {
                tracing::error!("cluster configuration mismatch confirmed; shutting down safely");
                let _ = fatal_tx.send(FatalReason::ConfigMismatch);
                return;
            }
            guard::GuardVerdict::FenceEpoch => {
                tracing::error!(
                    "stale cluster incarnation confirmed; shutting down safely before rejoining with fresh state"
                );
                let _ = fatal_tx.send(FatalReason::StaleSurvivor);
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        FailoverSemantics, await_control_write, build_openraft_config, probe_capabilities,
        select_initial_failover_semantics,
    };
    use crate::config::{
        ClusterConfigFingerprint, Config, HealthConfig, PeerConfig, RaftTuneConfig,
    };
    use crate::raft::probe::ClusterStatusResponse;
    use std::future::{pending, ready};
    use std::time::{Duration, Instant};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;

    async fn answer_one_status_probe(listener: TcpListener, response: ClusterStatusResponse) {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut node_and_secret_len = [0_u8; 12];
        stream.read_exact(&mut node_and_secret_len).await.unwrap();
        assert_eq!(&node_and_secret_len[8..], &0_u32.to_be_bytes());
        let mut incarnation_flag = [0_u8; 1];
        stream.read_exact(&mut incarnation_flag).await.unwrap();
        assert_eq!(incarnation_flag, [0]);

        let mut frame_len = [0_u8; 4];
        stream.read_exact(&mut frame_len).await.unwrap();
        let mut request = vec![0_u8; u32::from_be_bytes(frame_len) as usize];
        stream.read_exact(&mut request).await.unwrap();

        let response = serde_json::to_vec(&response).unwrap();
        stream
            .write_all(&(response.len() as u32).to_be_bytes())
            .await
            .unwrap();
        stream.write_all(&response).await.unwrap();
    }

    fn capability_test_config(second: String, third: String) -> Config {
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
            cluster_secret: None,
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

    /// A diskless node restarts with an empty log; the leader must be allowed to rewind its
    /// remembered progress for that follower instead of stalling replication.
    #[test]
    fn diskless_restarts_allow_follower_log_reversion() {
        let config = capability_test_config("127.0.0.1:17002".into(), "127.0.0.1:17003".into());
        let raft = build_openraft_config(&config).unwrap();

        assert_eq!(raft.allow_log_reversion, Some(true));
    }

    #[test]
    fn diskless_replay_uses_bounded_append_payloads() {
        let config = capability_test_config("127.0.0.1:17002".into(), "127.0.0.1:17003".into());
        let raft = build_openraft_config(&config).unwrap();

        assert_eq!(raft.max_payload_entries, 32);
    }

    #[tokio::test]
    async fn control_write_timeout_releases_a_stalled_write() {
        let started = Instant::now();
        let result = await_control_write(Duration::from_millis(10), pending::<()>()).await;

        assert!(result.is_err());
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    #[tokio::test]
    async fn control_write_timeout_preserves_a_completed_result() {
        let result = await_control_write(Duration::from_secs(1), ready(42_u64)).await;

        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn raft_control_tasks_report_unexpected_exit_and_own_shutdown() {
        let shutdown = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (failure_tx, failure_rx) = mpsc::unbounded_channel();
        let unexpected = super::tasks::spawn_supervised_task(
            "epoch minter",
            shutdown.clone(),
            failure_tx,
            super::tasks::CleanExit::Unexpected,
            async { Ok(()) },
        );
        let mut controls = super::RaftControlTasks {
            shutdown,
            tasks: vec![unexpected],
            failure_rx,
        };

        assert!(
            controls
                .recv_failure()
                .await
                .unwrap()
                .contains("epoch minter exited unexpectedly")
        );
        assert!(
            controls
                .shutdown()
                .await
                .unwrap_err()
                .to_string()
                .contains("epoch minter")
        );

        let shutdown = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (failure_tx, failure_rx) = mpsc::unbounded_channel();
        let expected = super::tasks::spawn_supervised_task(
            "cluster formation",
            shutdown.clone(),
            failure_tx,
            super::tasks::CleanExit::Allowed,
            async { Ok(()) },
        );
        let mut controls = super::RaftControlTasks {
            shutdown,
            tasks: vec![expected],
            failure_rx,
        };
        tokio::task::yield_now().await;

        assert!(controls.failure_rx.try_recv().is_err());
        controls.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn foreign_config_peer_does_not_downgrade_matching_majority_capabilities() {
        let matching_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let foreign_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = capability_test_config(
            matching_listener.local_addr().unwrap().to_string(),
            foreign_listener.local_addr().unwrap().to_string(),
        );
        let local = config.cluster_config_fingerprint().unwrap();
        let foreign = ClusterConfigFingerprint {
            version: local.version,
            digest: [0xff; 32],
        };
        let matching_response = ClusterStatusResponse {
            supports_failover_semantics_v2: true,
            config_fingerprint: Some(local),
            supports_config_identity_v1: true,
            ..ClusterStatusResponse::default()
        };
        let foreign_response = ClusterStatusResponse {
            supports_failover_semantics_v2: true,
            config_fingerprint: Some(foreign),
            supports_config_identity_v1: true,
            ..ClusterStatusResponse::default()
        };

        let matching_server = tokio::spawn(answer_one_status_probe(
            matching_listener,
            matching_response,
        ));
        let foreign_server =
            tokio::spawn(answer_one_status_probe(foreign_listener, foreign_response));

        let capabilities = probe_capabilities(&config, None, local).await;
        matching_server.await.unwrap();
        foreign_server.await.unwrap();

        assert_eq!(capabilities, (2, true, true));
    }

    #[test]
    fn initial_semantics_require_a_v2_capable_reachable_majority() {
        assert_eq!(
            select_initial_failover_semantics(3, 3, true),
            FailoverSemantics::V2
        );
        assert_eq!(
            select_initial_failover_semantics(3, 2, true),
            FailoverSemantics::V2
        );
        assert_eq!(
            select_initial_failover_semantics(3, 1, true),
            FailoverSemantics::Legacy
        );
        assert_eq!(
            select_initial_failover_semantics(3, 3, false),
            FailoverSemantics::Legacy
        );
    }

    #[test]
    fn fatal_reasons_keep_distinct_supervisor_exit_codes() {
        assert_eq!(super::FatalReason::StaleSurvivor.exit_code(), 3);
        assert_eq!(super::FatalReason::ConfigMismatch.exit_code(), 4);
    }
}
