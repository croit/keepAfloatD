//! Quorum-gated cold formation for the diskless Raft cluster.

use super::{FatalReason, GUARD_STRIKES_TO_RESET, KafRaft, RaftNetworkImpl, probe};
use crate::config::{ClusterConfigFingerprint, Config};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

/// How often nodes re-probe peers while waiting to cold-form the cluster.
const PROBE_INTERVAL: Duration = Duration::from_millis(500);

/// Per-probe wall-clock budget during formation.
const PROBE_TIMEOUT: Duration = Duration::from_secs(1);

/// Emit a waiting log first and then every N rounds (~10s) without spamming.
const LOG_EVERY_ROUNDS: u32 = 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PeerFormationObservation {
    ForeignConfig,
    ExistingCluster,
    Uninitialized,
    Ignored,
}

struct FormationRoundSummary {
    reachable_uninit: usize,
    found_existing: bool,
    foreign_config_identities: Vec<Option<ClusterConfigFingerprint>>,
}

impl FormationRoundSummary {
    fn new() -> Self {
        Self {
            // The local blank node always counts as one reachable uninitialized voter.
            reachable_uninit: 1,
            found_existing: false,
            foreign_config_identities: Vec::new(),
        }
    }

    fn observe(
        &mut self,
        local_fingerprint: ClusterConfigFingerprint,
        response: &probe::ClusterStatusResponse,
    ) -> PeerFormationObservation {
        if response.reports_foreign_config(local_fingerprint) {
            self.foreign_config_identities
                .push(response.config_fingerprint);
            return PeerFormationObservation::ForeignConfig;
        }
        if probe::can_join_existing_cluster(response, local_fingerprint) {
            self.found_existing = true;
            return PeerFormationObservation::ExistingCluster;
        }
        if probe::counts_for_cold_formation(response, local_fingerprint) {
            self.reachable_uninit += 1;
            return PeerFormationObservation::Uninitialized;
        }
        PeerFormationObservation::Ignored
    }

    fn largest_foreign_group(&self, local_fingerprint: ClusterConfigFingerprint) -> usize {
        probe::largest_foreign_identity_group(
            local_fingerprint,
            self.foreign_config_identities.iter().copied(),
        )
    }

    #[cfg(test)]
    fn from_responses(
        local_fingerprint: ClusterConfigFingerprint,
        responses: impl IntoIterator<Item = probe::ClusterStatusResponse>,
    ) -> Self {
        let mut summary = Self::new();
        for response in responses {
            summary.observe(local_fingerprint, &response);
        }
        summary
    }
}

fn formation_round_decision(
    total_peers: usize,
    reachable_uninit: usize,
    found_existing: bool,
    largest_foreign_group: usize,
) -> probe::FormationDecision {
    // A coherent foreign majority must receive the full hold-down before any matching stale
    // minority can make this blank node return from the guard loop and retry forever.
    if largest_foreign_group > total_peers / 2 {
        probe::FormationDecision::Wait
    } else {
        probe::formation_decision(total_peers, reachable_uninit, found_existing)
    }
}

/// Quorum-gated automatic cluster formation.
///
/// Every node runs this; there is no special bootstrap node. Every initializer proposes the same
/// configured membership, and a node initializes only after a majority reports itself
/// uninitialized. An existing compatible cluster wins over cold formation, while one coherent
/// majority advertising another configuration fences the local process.
pub(super) async fn auto_form_cluster(
    cfg: Arc<Config>,
    raft: KafRaft,
    network: Arc<RaftNetworkImpl>,
    fatal_tx: mpsc::UnboundedSender<FatalReason>,
) {
    match raft.is_initialized().await {
        Ok(true) => return,
        Ok(false) => {}
        Err(error) => {
            tracing::warn!("auto-form: is_initialized failed, skipping: {:?}", error);
            return;
        }
    }

    let members: BTreeMap<u64, openraft::BasicNode> = cfg
        .peers
        .iter()
        .map(|peer| {
            (
                peer.id,
                openraft::BasicNode {
                    addr: peer.raft_address.clone(),
                },
            )
        })
        .collect();
    let total = cfg.peers.len();
    let others: Vec<(u64, String)> = cfg
        .other_peers()
        .into_iter()
        .map(|peer| (peer.id, peer.raft_address.clone()))
        .collect();

    let mut rounds: u32 = 0;
    let mut config_guard = probe::ConsecutiveMajority::new(GUARD_STRIKES_TO_RESET);
    let local_fingerprint = network.config_fingerprint();
    loop {
        if network.is_shutting_down() {
            return;
        }

        let mut summary = FormationRoundSummary::new();
        for (peer_id, address) in &others {
            if let Ok(response) = super::network::probe_peer_status(
                &cfg.raft_listen,
                address,
                cfg.node_id,
                cfg.cluster_secret.as_deref(),
                // This loop is reachable only while the local node has no incarnation.
                None,
                local_fingerprint,
                PROBE_TIMEOUT,
            )
            .await
            {
                match summary.observe(local_fingerprint, &response) {
                    PeerFormationObservation::ForeignConfig => tracing::warn!(
                        "cluster configuration mismatch: peer {} advertises a different fingerprint",
                        peer_id
                    ),
                    PeerFormationObservation::ExistingCluster => {
                        tracing::info!("peer {} reports a compatible existing cluster", peer_id)
                    }
                    PeerFormationObservation::Uninitialized | PeerFormationObservation::Ignored => {
                    }
                }
            }
        }

        let foreign_config = summary.largest_foreign_group(local_fingerprint);
        if config_guard.observe(foreign_config, total / 2 + 1) {
            tracing::error!(
                "cluster configuration mismatch confirmed against one coherent roster majority; shutting down safely"
            );
            let _ = fatal_tx.send(FatalReason::ConfigMismatch);
            return;
        }

        match formation_round_decision(
            total,
            summary.reachable_uninit,
            summary.found_existing,
            foreign_config,
        ) {
            probe::FormationDecision::Join => return,
            probe::FormationDecision::Form => {
                match raft.initialize(members.clone()).await {
                    Ok(()) => tracing::info!(
                        "auto-formed Raft cluster: {} of {} peers reachable and uninitialized",
                        summary.reachable_uninit,
                        total
                    ),
                    // Another path may have initialized us after the probe.
                    Err(error) => tracing::warn!("Raft initialize: {:?}", error),
                }
                return;
            }
            probe::FormationDecision::Wait => {
                rounds = rounds.wrapping_add(1);
                if rounds == 1 || rounds.is_multiple_of(LOG_EVERY_ROUNDS) {
                    tracing::warn!(
                        "waiting to cold-form cluster: {} of {} peers reachable and uninitialized",
                        summary.reachable_uninit,
                        total
                    );
                }
                tokio::time::sleep(PROBE_INTERVAL).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ClusterConfigFingerprint;
    use crate::raft::probe::{ClusterStatusResponse, FormationDecision};

    fn fingerprint(byte: u8) -> ClusterConfigFingerprint {
        ClusterConfigFingerprint {
            version: 1,
            digest: [byte; 32],
        }
    }

    fn existing(config_fingerprint: ClusterConfigFingerprint) -> ClusterStatusResponse {
        ClusterStatusResponse {
            initialized: true,
            member_count: 5,
            config_fingerprint: Some(config_fingerprint),
            ..ClusterStatusResponse::default()
        }
    }

    #[test]
    fn coherent_foreign_majority_overrides_one_matching_existing_peer_in_every_order() {
        let local = fingerprint(1);
        let matching_existing = existing(local);
        let foreign = existing(fingerprint(2));
        let response_orders = [
            vec![
                matching_existing.clone(),
                foreign.clone(),
                foreign.clone(),
                foreign.clone(),
            ],
            vec![
                foreign.clone(),
                matching_existing.clone(),
                foreign.clone(),
                foreign.clone(),
            ],
            vec![foreign.clone(), foreign.clone(), foreign, matching_existing],
        ];

        for responses in response_orders {
            let summary = FormationRoundSummary::from_responses(local, responses);
            assert!(summary.found_existing);
            assert_eq!(summary.largest_foreign_group(local), 3);
            assert_eq!(
                formation_round_decision(
                    5,
                    summary.reachable_uninit,
                    summary.found_existing,
                    summary.largest_foreign_group(local),
                ),
                FormationDecision::Wait,
                "a matching stale minority must not bypass the foreign-majority hold-down"
            );
        }
    }
}
