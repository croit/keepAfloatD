//! Supervised cluster-incarnation and configuration guard.

use super::{
    FatalReason, GUARD_STRIKES_TO_RESET, KafStorageState, RaftNetworkImpl, guard, network, probe,
};
use crate::config::Config;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{RwLock, mpsc};

/// How often the cluster guard re-probes peers.
pub(super) const GUARD_POLL_INTERVAL: Duration = Duration::from_millis(1000);

/// Per-probe wall-clock budget for the cluster guard.
const GUARD_PROBE_TIMEOUT: Duration = Duration::from_secs(1);

/// Detect that this node is a **stale survivor** - it still holds an old incarnation while a
/// majority of the roster has reformed under a new one - and reset by exiting for a supervisor
/// restart (returning blank, it rejoins via replication like any diskless reboot).
///
/// Safety of the reset rests on the trigger: the node must (1) hold a committed incarnation,
/// and (2) see a **majority of the whole roster** report the same
/// *different* concrete incarnation. Condition (2) is what proves this node is the minority that
/// one coherent cluster reformed around - it can hold nothing committed by that majority, so
/// discarding its state loses nothing. Distinct foreign incarnations never add into that proof.
/// A healthy follower in a normal election shares its peers' incarnation, so it never triggers.
pub(super) async fn run_cluster_guard(
    cfg: Arc<Config>,
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
        for (peer_id, addr) in &others {
            if let Ok(resp) = network::probe_peer_status(
                cfg.as_ref(),
                addr,
                *peer_id,
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
            foreign_config: probe::largest_foreign_identity_group(
                local_fingerprint,
                foreign_config_identities,
            ),
            foreign_epoch: probe::largest_foreign_epoch_group(local_epoch, observed_epochs),
        };
        // Config fencing is independent of epoch fencing: a mismatched minority must stop even
        // when it has not joined an incarnation, while epoch fencing needs an initialized node.
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
        if round.local_epoch_known && round.foreign_epoch >= guard.majority() {
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
