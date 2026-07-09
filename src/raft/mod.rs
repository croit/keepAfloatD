//! Raft cluster (OpenRaft 0.10) over a small TCP/JSON framing layer.

pub mod network;
pub mod probe;
pub mod store;
pub mod types;

pub use network::RaftNetworkImpl;
pub use store::{KafStateMachine, KafStorageState};
pub use types::{KafRequest, TypeConfig};

use crate::config::{Config, VipAddr};
use anyhow::Context;
// `WatchReceiver` provides `borrow_watched()` on the metrics watch handle (0.10 renamed the 0.9
// `borrow()`); it must be in scope for the method to resolve.
use openraft::Raft;
use openraft::async_runtime::WatchReceiver;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

/// openraft 0.10 makes `Raft` generic over the state-machine type, so the alias must name our
/// state-machine half. The log-storage half is erased behind the `Raft::new` `LS` type parameter.
pub type KafRaft = Raft<TypeConfig, KafStateMachine>;

/// How often the designated initializer re-probes peers while waiting to cold-form the cluster.
const FORMATION_PROBE_INTERVAL: Duration = Duration::from_millis(500);

/// Per-probe wall-clock budget during formation.
const FORMATION_PROBE_TIMEOUT: Duration = Duration::from_secs(1);

/// Emit a "still waiting" log on the first round and then every Nth round (~10s at 500ms) so a
/// stuck cold start (no majority reachable) is visible without spamming.
const FORMATION_LOG_EVERY_ROUNDS: u32 = 20;

/// How often the epoch minter checks whether it must commit the cluster incarnation, and how often
/// the stale-survivor guard re-probes peers.
const GUARD_POLL_INTERVAL: Duration = Duration::from_millis(1000);

/// Per-probe wall-clock budget for the stale-survivor guard.
const GUARD_PROBE_TIMEOUT: Duration = Duration::from_secs(1);

/// Consecutive guard rounds that must all observe a foreign majority before this node resets. The
/// hold-down avoids acting on a single transient probe round.
const GUARD_STRIKES_TO_RESET: u32 = 3;

/// Process exit code used when a node resets itself after detecting it is a stale survivor of a
/// cluster that reformed without it. Non-zero so `Restart=on-failure` supervisors relaunch it blank.
const STALE_SURVIVOR_EXIT_CODE: i32 = 3;

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
)> {
    let raft_cfg = openraft::Config {
        election_timeout_min: cfg.raft.election_timeout_min_ms,
        election_timeout_max: cfg.raft.election_timeout_max_ms,
        heartbeat_interval: cfg.raft.heartbeat_interval_ms,
        ..Default::default()
    }
    .validate()
    .map_err(|e| anyhow::anyhow!("openraft config validate: {e}"))?;
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
    let network = Arc::new(RaftNetworkImpl::new(cfg.clone(), state_ref.clone()));

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
    network
        .start(raft.clone())
        .await
        .context("raft network start")?;

    // Lifetime: runs until the cluster is formed or an existing one is discovered, or until network
    // shutdown is requested. Spawned (not awaited) so startup never blocks waiting for a quorum.
    {
        let cfg = cfg.clone();
        let raft = raft.clone();
        let network = network.clone();
        tokio::spawn(async move { auto_form_cluster(cfg, raft, network).await });
    }

    // Lifetime: runs until shutdown. Commits the per-formation cluster incarnation once this node
    // leads a freshly formed cluster that has none yet.
    {
        let raft = raft.clone();
        let network = network.clone();
        let state_ref = state_ref.clone();
        let node_id = cfg.node_id;
        tokio::spawn(async move { run_epoch_minter(raft, network, state_ref, node_id).await });
    }

    // Lifetime: runs until shutdown. Resets this node (by exiting for a supervisor restart) if it
    // becomes a stale survivor of a cluster that reformed without it.
    {
        let cfg = cfg.clone();
        let raft = raft.clone();
        let network = network.clone();
        let state_ref = state_ref.clone();
        tokio::spawn(async move { run_cluster_guard(cfg, raft, network, state_ref).await });
    }

    Ok((raft, network, state_ref))
}

/// Mint a per-formation cluster incarnation from 16 bytes of kernel entropy. Linux-only daemon, so
/// reading `/dev/urandom` directly avoids pulling in an RNG dependency.
fn mint_cluster_id() -> std::io::Result<u128> {
    use std::io::Read;
    let mut buf = [0u8; 16];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut buf)?;
    Ok(u128::from_be_bytes(buf))
}

/// Commit the cluster incarnation exactly once, when this node leads a cluster that has none yet.
///
/// Only the leader writes; the state machine keeps the first committed value, so leader churn or
/// concurrent attempts cannot change a cluster's incarnation. Polls rather than waiting on metrics
/// edges so a transient `client_write` failure (e.g. momentary loss of leadership) is simply
/// retried on the next tick.
async fn run_epoch_minter(
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
            match mint_cluster_id() {
                Ok(cluster_id) => {
                    match raft
                        .client_write(KafRequest::ClusterFormed { cluster_id })
                        .await
                    {
                        Ok(_) => {
                            tracing::info!("committed cluster incarnation {:#034x}", cluster_id)
                        }
                        // Benign: lost leadership between the check and the write, or no quorum yet;
                        // retried on the next tick.
                        Err(e) => tracing::warn!("commit cluster incarnation: {:?}", e),
                    }
                }
                Err(e) => tracing::error!("mint cluster incarnation: {}", e),
            }
        }
        tokio::time::sleep(GUARD_POLL_INTERVAL).await;
    }
}

/// Detect that this node is a **stale survivor** — it still holds an old incarnation while a
/// majority of the roster has reformed under a new one — and reset by exiting for a supervisor
/// restart (returning blank, it rejoins via replication like any diskless reboot).
///
/// Safety of the reset rests on the trigger: the node must (1) hold a committed incarnation,
/// (2) currently have no leader, and (3) see a **majority of the whole roster** report a *different*
/// concrete incarnation. Condition (3) is what proves this node is the minority that reformed
/// around — it can hold nothing committed by that majority, so discarding its state loses nothing.
/// A healthy follower in a normal election shares its peers' incarnation, so it never triggers.
async fn run_cluster_guard(
    cfg: Arc<Config>,
    raft: KafRaft,
    network: Arc<RaftNetworkImpl>,
    state_ref: Arc<RwLock<KafStorageState>>,
) {
    let total = cfg.peers.len();
    let majority = total / 2 + 1;
    let others: Vec<(u64, String)> = cfg
        .other_peers()
        .into_iter()
        .map(|p| (p.id, p.raft_address.clone()))
        .collect();
    let mut strikes: u32 = 0;
    loop {
        tokio::time::sleep(GUARD_POLL_INTERVAL).await;
        if network.is_shutting_down() {
            return;
        }
        // Only an initialized node holding a committed incarnation can be a stale survivor.
        let Some(local_epoch) = state_ref.read().await.cluster_epoch else {
            strikes = 0;
            continue;
        };
        // A node that currently sees a leader is participating in a cluster, not stranded.
        if raft.metrics().borrow_watched().current_leader.is_some() {
            strikes = 0;
            continue;
        }
        // Leaderless: probe peers and count those reporting a *different* concrete incarnation.
        let mut foreign = 0usize;
        for (_peer_id, addr) in &others {
            if let Ok(resp) = network::probe_peer_status(
                addr,
                cfg.node_id,
                cfg.cluster_secret.as_deref(),
                Some(local_epoch),
                cfg.max_frame_bytes,
                GUARD_PROBE_TIMEOUT,
            )
            .await
            {
                if resp.reports_foreign_epoch(Some(local_epoch)) {
                    foreign += 1;
                }
            }
        }
        if foreign >= majority {
            strikes = strikes.saturating_add(1);
            tracing::warn!(
                "stale cluster incarnation: {} of {} peers report a different cluster ({}/{} strikes)",
                foreign,
                total,
                strikes,
                GUARD_STRIKES_TO_RESET
            );
            if strikes >= GUARD_STRIKES_TO_RESET {
                tracing::error!(
                    "stale cluster incarnation confirmed; exiting to rejoin the reformed cluster with fresh state"
                );
                std::process::exit(STALE_SURVIVOR_EXIT_CODE);
            }
        } else {
            strikes = 0;
        }
    }
}

/// Quorum-gated automatic cluster formation.
///
/// Every node runs this; there is no special "bootstrap" node. Safety rests on two facts
/// (see `ARCHITECTURE.md`):
/// 1. **Identical-config initialize** — every node calls `Raft::initialize` with the *same*,
///    cluster-wide identical membership (from `peers`). OpenRaft documents concurrent `initialize`
///    with the same config as safe; the only unsafe case is *different* configs, which the shared
///    `peers` roster already rules out. Raft then elects a single leader among the reachable
///    majority, so any majority can form — or recover — the cluster even if the lowest-id node is
///    permanently gone (important for diskless/PXE nodes with no state across reboots).
/// 2. **Quorum gate + existing-cluster check** — a node initializes only after a majority of peers
///    (including itself) respond uninitialized. A network partition therefore yields at most one
///    side with a leader (quorum), never two. If any peer reports an existing cluster, the node
///    joins via replication instead — so a blank-rebooted node rejoins rather than re-forming.
async fn auto_form_cluster(cfg: Arc<Config>, raft: KafRaft, network: Arc<RaftNetworkImpl>) {
    match raft.is_initialized().await {
        Ok(true) => return, // already part of a cluster
        Ok(false) => {}
        Err(e) => {
            tracing::warn!("auto-form: is_initialized failed, skipping: {:?}", e);
            return;
        }
    }

    let members: BTreeMap<u64, openraft::BasicNode> = cfg
        .peers
        .iter()
        .map(|p| {
            (
                p.id,
                openraft::BasicNode {
                    addr: p.raft_address.clone(),
                },
            )
        })
        .collect();
    let total = cfg.peers.len();
    let others: Vec<(u64, String)> = cfg
        .other_peers()
        .into_iter()
        .map(|p| (p.id, p.raft_address.clone()))
        .collect();

    let mut rounds: u32 = 0;
    loop {
        if network.is_shutting_down() {
            return;
        }

        let mut reachable_uninit = 1usize; // count self (uninitialized)
        let mut found_existing = false;
        for (peer_id, addr) in &others {
            match network::probe_peer_status(
                addr,
                cfg.node_id,
                cfg.cluster_secret.as_deref(),
                // We only reach this loop while uninitialized, so we carry no incarnation yet.
                None,
                cfg.max_frame_bytes,
                FORMATION_PROBE_TIMEOUT,
            )
            .await
            {
                Ok(resp) if resp.indicates_existing_cluster() => {
                    tracing::info!(
                        "peer {} reports an existing cluster; joining via replication instead of forming a new one",
                        peer_id
                    );
                    found_existing = true;
                    break;
                }
                Ok(_) => reachable_uninit += 1,
                Err(_) => {} // peer not reachable yet; keep waiting
            }
        }

        match probe::formation_decision(total, reachable_uninit, found_existing) {
            probe::FormationDecision::Join => return, // replication will absorb this node
            probe::FormationDecision::Form => {
                match raft.initialize(members.clone()).await {
                    Ok(()) => tracing::info!(
                        "auto-formed Raft cluster: {} of {} peers reachable and uninitialized",
                        reachable_uninit,
                        total
                    ),
                    // Benign if another path already initialized us between probe and call.
                    Err(e) => tracing::warn!("Raft initialize: {:?}", e),
                }
                return;
            }
            probe::FormationDecision::Wait => {
                rounds = rounds.wrapping_add(1);
                if rounds == 1 || rounds.is_multiple_of(FORMATION_LOG_EVERY_ROUNDS) {
                    tracing::warn!(
                        "waiting to cold-form cluster: {} of {} peers reachable and uninitialized",
                        reachable_uninit,
                        total
                    );
                }
                tokio::time::sleep(FORMATION_PROBE_INTERVAL).await;
            }
        }
    }
}
