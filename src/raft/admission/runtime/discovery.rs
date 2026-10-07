//! Discovery supplies exact current boots and history, never permission.

use super::*;
use futures::future::join_all;
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
        let fingerprint = self.cfg.cluster_config_fingerprint()?;
        let peers = self.cfg.other_peers();
        let results = join_all(peers.iter().map(|peer| async move {
            let status = crate::raft::network::probe_peer_status(
                &self.cfg,
                &peer.raft_address,
                peer.id,
                None,
                fingerprint,
                budget,
            )
            .await?;
            anyhow::ensure!(
                status.config_fingerprint == Some(fingerprint),
                "discovery configuration differs"
            );
            let replica = status
                .replica
                .ok_or_else(|| anyhow::anyhow!("status has no authenticated boot"))?;
            anyhow::ensure!(
                replica.physical_id == peer.id,
                "status physical identity differs"
            );
            let response = network
                .management_rpc(replica, ManagementAction::Discover, budget)
                .await?;
            let ManagementResult::Discovery { genesis, .. } = response.result else {
                anyhow::bail!("unexpected discovery response");
            };
            if let Some(genesis) = &genesis {
                genesis.validate_roster(&self.cfg.peers.iter().map(|peer| peer.id).collect())?;
                anyhow::ensure!(
                    genesis.config == fingerprint,
                    "discovered genesis configuration differs"
                );
            }
            Ok::<_, anyhow::Error>((replica, genesis))
        }))
        .await;
        let mut discovery = Discovery {
            boots: [self.local].into(),
            histories: BTreeMap::new(),
        };
        for result in results {
            match result {
                Ok((replica, genesis)) => {
                    discovery.boots.insert(replica);
                    if let Some(genesis) = genesis {
                        discovery.histories.insert(replica, genesis);
                    }
                }
                Err(error) => tracing::debug!(%error, "admission discovery peer unavailable"),
            }
        }
        Ok(discovery)
    }
}
