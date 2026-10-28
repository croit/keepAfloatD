//! Discovery supplies exact current boots and history, never permission.

use super::*;
use futures::stream::{FuturesUnordered, StreamExt};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::time::Duration;

pub(super) struct Discovery {
    pub(super) boots: BTreeSet<ReplicaId>,
    pub(super) histories: BTreeMap<ReplicaId, Genesis>,
}

pub(super) fn cohort_genesis(cfg: &Config, voters: BTreeSet<ReplicaId>) -> anyhow::Result<Genesis> {
    let config = cfg.cluster_config_fingerprint()?;
    let mut hash = Sha256::new();
    hash.update(b"keepafloatd-cold-cohort-epoch-v1\0");
    hash.update([config.version]);
    hash.update(config.digest);
    for voter in &voters {
        hash.update(voter.physical_id.to_be_bytes());
        hash.update(voter.boot_nonce);
    }
    let digest = hash.finalize();
    let mut epoch = [0; 16];
    epoch.copy_from_slice(&digest[..16]);
    let genesis = Genesis {
        config,
        epoch: u128::from_be_bytes(epoch),
        voters,
    };
    genesis.validate_roster(&cfg.peers.iter().map(|peer| peer.id).collect())?;
    Ok(genesis)
}

impl RuntimeDriver {
    pub(super) async fn discover(
        &self,
        network: &RaftNetworkImpl,
        budget: Duration,
    ) -> anyhow::Result<Discovery> {
        self.discover_peers(network, budget, false).await
    }

    pub(super) async fn discover_for_health(
        &self,
        network: &RaftNetworkImpl,
        budget: Duration,
    ) -> anyhow::Result<Discovery> {
        self.discover_peers(network, budget, true).await
    }

    async fn discover_peers(
        &self,
        network: &RaftNetworkImpl,
        budget: Duration,
        reject_invalid: bool,
    ) -> anyhow::Result<Discovery> {
        let fingerprint = self.cfg.cluster_config_fingerprint()?;
        let peers = self.cfg.other_peers();
        let mut results: FuturesUnordered<_> = peers
            .iter()
            .map(|peer| async move {
                let status = crate::raft::network::probe_peer_status(
                    &self.cfg,
                    &peer.raft_address,
                    peer.id,
                    None,
                    fingerprint,
                    budget,
                )
                .await?;
                let replica = status
                    .replica
                    .ok_or_else(|| anyhow::anyhow!("status has no authenticated boot"))?;
                anyhow::ensure!(
                    replica.physical_id == peer.id,
                    "status physical identity differs"
                );
                let peer_fingerprint = status
                    .config_fingerprint
                    .ok_or_else(|| anyhow::anyhow!("status has no configuration fingerprint"))?;
                if peer_fingerprint != fingerprint {
                    tracing::debug!(
                        peer_id = peer.id,
                        "excluding admission discovery peer with different configuration"
                    );
                    return Ok(None);
                }
                let response = network
                    .management_rpc(replica, ManagementAction::Discover, budget)
                    .await?;
                let ManagementResult::Discovery { genesis, .. } = response.result else {
                    anyhow::bail!("unexpected discovery response");
                };
                if let Some(genesis) = &genesis {
                    genesis
                        .validate_roster(&self.cfg.peers.iter().map(|peer| peer.id).collect())?;
                    anyhow::ensure!(
                        genesis.config == fingerprint,
                        "discovered genesis configuration differs"
                    );
                }
                Ok::<_, anyhow::Error>(Some((replica, genesis)))
            })
            .collect();
        let mut discovery = Discovery {
            boots: [self.local].into(),
            histories: BTreeMap::new(),
        };
        while let Some(result) = results.next().await {
            match result {
                Ok(Some((replica, genesis))) => {
                    discovery.boots.insert(replica);
                    if let Some(genesis) = genesis {
                        discovery.histories.insert(replica, genesis);
                    }
                }
                Ok(None) => {}
                Err(error) => {
                    tracing::debug!(error = %format_args!("{error:#}"), "admission discovery peer unavailable");
                    // An unauthenticated peer cannot veto proofs from the authenticated majority.
                    if reject_invalid
                        && error
                            .downcast_ref::<crate::raft::network::UnauthenticatedDiscovery>()
                            .is_none()
                        && crate::consensus_freshness::failure_kind(&error)
                            == crate::consensus_freshness::FailureKind::Rejected
                    {
                        return Err(error);
                    }
                }
            }
        }
        Ok(discovery)
    }
}
