use super::*;
use crate::config::PeerConfig;
use crate::raft::admission::{JoinPlan, ReceivedGrant};
use crate::raft::store::new_store;
use crate::raft::types::test_replica;
use openraft::testing::log_id;
use std::time::Duration;

async fn admitted_learner() -> (Arc<RuntimeDriver>, PreparedJoin) {
    let mut cfg = (*super::tests::config()).clone();
    for id in [2, 3] {
        cfg.peers.push(PeerConfig {
            id,
            raft_address: format!("127.0.0.1:{}", 1000 + id),
            client_submit_address: format!("127.0.0.1:{}", 2000 + id),
        });
    }
    let cfg = Arc::new(cfg);
    let (_, _, state) = new_store(Arc::new(vec![]), 3, true, 0);
    let runtime = RuntimeDriver::new(
        cfg.clone(),
        state.clone(),
        LeaseTiming::new(Duration::from_secs(5), Duration::ZERO).unwrap(),
        Instant::now(),
    )
    .unwrap();
    tokio::time::advance(runtime.timing.restart_quarantine()).await;
    let previous_voters = [test_replica(1), test_replica(2), test_replica(3)].into();
    let genesis = Genesis {
        config: cfg.cluster_config_fingerprint().unwrap(),
        epoch: 42,
        voters: previous_voters,
    };
    let prepared = PreparedJoin {
        plan: JoinPlan {
            genesis: genesis.clone(),
            consumer: runtime.local,
            request_nonce: [17; 32],
            previous_voters: genesis.voters.clone(),
            next_voters: [runtime.local, test_replica(2), test_replica(3)].into(),
        },
        prepared: log_id::<crate::raft::TypeConfig>(1, test_replica(2), 4),
        learner_applied: None,
    };
    let mut round = runtime
        .core
        .lock()
        .unwrap()
        .begin(genesis, AdmissionMode::Join { prepared: None })
        .unwrap();
    round.bind_prepared(&prepared).unwrap();
    let grants: Vec<_> = [test_replica(2), test_replica(3)]
        .into_iter()
        .map(|issuer| {
            let binding = [issuer.physical_id as u8; 32];
            let record = SignedAdmission::sign(
                cfg.cluster_secret.as_deref(),
                11,
                issuer,
                runtime.local,
                binding,
                round.request().clone(),
            )
            .unwrap();
            ReceivedGrant::authenticate(
                cfg.cluster_secret.as_deref(),
                issuer,
                runtime.local,
                binding,
                round.request(),
                record,
            )
            .unwrap()
        })
        .collect();
    runtime
        .install(&mut round, &grants, Some(prepared.clone()))
        .await
        .unwrap();
    (runtime, prepared)
}

#[tokio::test(start_paused = true)]
async fn verified_join_allows_initial_replication_without_granting_a_vote() {
    let (runtime, prepared) = admitted_learner().await;
    let authority = runtime
        .authorize_raft(test_replica(2))
        .expect("verified JOIN must permit initial replication before local catch-up");
    assert_eq!(authority.voters, prepared.plan.previous_voters);
    assert!(!authority.voters.contains(&runtime.local));
    assert!(
        runtime
            .authorize_raft(ReplicaId {
                physical_id: 2,
                boot_nonce: [99; 32]
            })
            .is_err()
    );
    assert!(!runtime.vip_activation_ready().await);
    let state = runtime.state.read().await;
    assert!(state.genesis.is_none());
    assert!(state.prepared_join.is_none());
    assert!(state.last_applied_log.is_none());
}

#[tokio::test(start_paused = true)]
async fn concurrent_raft_authorizations_share_unchanged_bootstrap_evidence() {
    let (runtime, _) = admitted_learner().await;
    let start = std::sync::Barrier::new(8);
    let handle = tokio::runtime::Handle::current();
    let failures = std::thread::scope(|scope| {
        let readers: Vec<_> = (0..8)
            .map(|_| {
                scope.spawn(|| {
                    let _runtime = handle.enter();
                    start.wait();
                    (0..100)
                        .filter_map(|_| runtime.authorize_raft(test_replica(2)).err())
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        readers
            .into_iter()
            .flat_map(|reader| reader.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert!(
        failures.is_empty(),
        "concurrent reads denied unchanged authority: {} errors, first: {:?}",
        failures.len(),
        failures.first()
    );
}

#[tokio::test(start_paused = true)]
async fn transport_authorization_waits_for_transient_state_writer() {
    let (runtime, _) = admitted_learner().await;
    let writer = runtime.state.write().await;
    let authorize = runtime.authorize_raft_async(test_replica(2));
    tokio::pin!(authorize);
    assert!(
        futures::poll!(&mut authorize).is_pending(),
        "temporary state contention must wait, not reject a healthy transport"
    );
    drop(writer);
    let authorization = authorize.await.unwrap();
    assert_eq!(authorization.peer, test_replica(2));
    assert!(runtime.state.try_write().is_ok());
}

#[tokio::test(start_paused = true)]
async fn transport_authorization_rechecks_authority_after_waiting() {
    for condition in ["expired", "shutdown", "foreign", "unbound", "peer"] {
        let (runtime, prepared) = admitted_learner().await;
        let mut writer = runtime.state.write().await;
        let peer = if condition == "peer" {
            ReplicaId {
                physical_id: 2,
                boot_nonce: [99; 32],
            }
        } else {
            test_replica(2)
        };
        let authorize = runtime.authorize_raft_async(peer);
        tokio::pin!(authorize);
        assert!(futures::poll!(&mut authorize).is_pending());
        match condition {
            "expired" => tokio::time::advance(runtime.timing.consumer_use()).await,
            "shutdown" => runtime.shutdown(),
            "foreign" => {
                let mut foreign = prepared.plan.genesis;
                foreign.epoch += 1;
                writer.genesis = Some(foreign);
            }
            "unbound" => writer.admission = None,
            "peer" => (),
            _ => unreachable!(),
        }
        drop(writer);
        assert!(
            authorize.await.is_err(),
            "accepted {condition} after contention"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn transport_authorization_wait_respects_caller_deadline() {
    let (runtime, _) = admitted_learner().await;
    let writer = runtime.state.write().await;
    let authorize = tokio::time::timeout(
        runtime.rpc_budget(),
        runtime.authorize_raft_async(test_replica(2)),
    );
    tokio::pin!(authorize);
    assert!(futures::poll!(&mut authorize).is_pending());
    tokio::time::advance(runtime.rpc_budget()).await;
    assert!(authorize.await.is_err());
    drop(writer);
    assert!(runtime.authorize_raft_async(test_replica(2)).await.is_ok());
    runtime.shutdown();
    assert!(runtime.authorize_raft_async(test_replica(2)).await.is_err());
}

#[tokio::test(start_paused = true)]
async fn applied_history_cannot_replace_a_verified_join_with_another_operation() {
    let (runtime, prepared) = admitted_learner().await;
    {
        let mut state = runtime.state.write().await;
        state.last_applied_log = Some(prepared.prepared);
        state.genesis = Some(prepared.plan.genesis.clone());
    }
    assert!(runtime.authorize_raft(test_replica(2)).is_err());
    let mut wrong = prepared.clone();
    wrong.plan.request_nonce[0] ^= 1;
    runtime.state.write().await.prepared_join = Some(wrong);
    assert!(runtime.authorize_raft(test_replica(2)).is_err());
}

#[tokio::test(start_paused = true)]
async fn verified_bootstrap_does_not_survive_its_local_permission() {
    let (runtime, _) = admitted_learner().await;
    assert!(runtime.authorize_raft(test_replica(2)).is_ok());
    tokio::time::advance(runtime.timing.consumer_use()).await;
    assert!(runtime.authorize_raft(test_replica(2)).is_err());
    assert!(!runtime.vip_activation_ready().await);
}

#[tokio::test(start_paused = true)]
async fn verified_bootstrap_rejects_foreign_applied_genesis() {
    let (runtime, prepared) = admitted_learner().await;
    let mut foreign = prepared.plan.genesis;
    foreign.epoch += 1;
    runtime.state.write().await.genesis = Some(foreign);
    assert!(runtime.authorize_raft(test_replica(2)).is_err());
}
