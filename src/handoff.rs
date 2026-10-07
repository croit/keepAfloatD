//! Bounded shutdown notification after VIP effects and health publishing have stopped.

use crate::config::Config;
use crate::raft::RaftNetworkImpl;
use crate::raft::admission::runtime::RuntimeDriver;
use crate::raft::{KafRaft, KafRequest, KafStorageState};
use crate::submit;
use anyhow::Context;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

/// Publish only after successful local cleanup. Quorum loss must not delay shutdown indefinitely.
pub(crate) async fn publish(
    cfg: &Arc<Config>,
    raft: &KafRaft,
    state: &Arc<RwLock<KafStorageState>>,
    runtime: &RuntimeDriver,
    network: &RaftNetworkImpl,
) -> anyhow::Result<()> {
    let session = runtime
        .current()
        .context("shutdown handoff requires active admission")?;
    let deadline = session
        .check()?
        .min(tokio::time::Instant::now() + Duration::from_millis(cfg.submit_timeout_ms));
    tokio::time::timeout_at(deadline, async {
        runtime.submit_health(network, false).await?;
        session.check()?;
        tracing::info!("shutdown unhealthy report committed and applied");
        let releases = pending_releases(&*state.read().await, cfg.node_id);
        for request in releases {
            tracing::debug!(%request, "publishing shutdown VIP release");
            submit::submit_request(cfg, raft, request.clone()).await?;
            tracing::info!(%request, "shutdown VIP release committed");
        }
        Ok(())
    })
    .await
    .context("shutdown handoff exceeded the shared submit timeout")?
}

fn pending_releases(state: &KafStorageState, node_id: u64) -> Vec<KafRequest> {
    let mut assignments: Vec<_> = state.vip_assignments.iter().collect();
    assignments.sort_unstable_by_key(|(vip, _)| **vip);
    assignments
        .into_iter()
        .filter(|(_, assignment)| {
            assignment.previous_holder == Some(node_id) && !assignment.previous_holder_released
        })
        .map(|(vip, assignment)| KafRequest::VipReleased {
            node_id,
            vip: *vip,
            generation: assignment.generation,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::store::{VipAssignment, new_store};

    #[tokio::test]
    async fn unadmitted_runtime_cannot_report_a_successful_empty_handoff() {
        let cfg: Arc<Config> = Arc::new(serde_yaml::from_str(
            "node_id: 1\nraft_listen: '127.0.0.1:1000'\nclient_submit_listen: '127.0.0.1:2000'\npeers:\n  - {id: 1, raft_address: '127.0.0.1:1000', client_submit_address: '127.0.0.1:2000'}\nvips: []\nhealth: {command: [/bin/true], interval_ms: 1000, timeout_ms: 500}\ncluster_secret: handoff-unit-secret-01234567890123\n",
        ).unwrap());
        let (log, machine, state) =
            crate::raft::store::new_admitted_store(Arc::new(vec![]), 3, true, 0);
        let runtime = RuntimeDriver::new(
            cfg.clone(),
            state.clone(),
            crate::runtime_permission::LeaseTiming::new(
                Duration::from_secs(5),
                Duration::from_secs(1),
            )
            .unwrap(),
            tokio::time::Instant::now(),
        )
        .unwrap();
        let network = RaftNetworkImpl::new(cfg.clone(), state.clone(), runtime.clone()).unwrap();
        let raft = KafRaft::new(
            runtime.local_replica(),
            Arc::new(openraft::Config {
                enable_tick: false,
                ..Default::default()
            }),
            network.clone(),
            log,
            machine,
        )
        .await
        .unwrap();
        runtime.attach(raft.clone()).unwrap();
        let result = publish(&cfg, &raft, &state, &runtime, &network).await;
        runtime.shutdown();
        raft.shutdown().await.unwrap();
        assert!(result.unwrap_err().to_string().contains("active admission"));
        assert!(state.read().await.node_health.is_empty());
    }

    #[tokio::test]
    async fn releases_only_unacknowledged_local_generations_in_address_order() {
        let (_, _, state) = new_store(Arc::new(Vec::new()), 3, true, 0);
        let mut state = state.write().await;
        for (suffix, previous_holder, released) in [
            (4, Some(1), false),
            (3, Some(2), false),
            (2, Some(1), true),
            (1, Some(1), false),
            (5, None, false),
        ] {
            state.vip_assignments.insert(
                format!("192.0.2.{suffix}").parse().unwrap(),
                VipAssignment {
                    holder: 3,
                    generation: suffix,
                    previous_holder,
                    previous_holder_released: released,
                    activation_tick: 100,
                },
            );
        }
        assert_eq!(
            pending_releases(&state, 1),
            vec![
                KafRequest::VipReleased {
                    node_id: 1,
                    vip: "192.0.2.1".parse().unwrap(),
                    generation: 1
                },
                KafRequest::VipReleased {
                    node_id: 1,
                    vip: "192.0.2.4".parse().unwrap(),
                    generation: 4
                },
            ]
        );
        assert!(pending_releases(&state, 9).is_empty());
    }
}
