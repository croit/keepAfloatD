use super::*;
use crate::config::PeerConfig;
use crate::raft::admission::{AppliedHealthProgress, HealthProgress, ReceivedGrant};
use crate::raft::store::{KafStateMachine, new_admitted_store};
use crate::raft::types::{KafRequest, TypeConfig};
use openraft::alias::EntryOf;
use openraft::storage::RaftStateMachine;
use openraft::testing::log_id;
use openraft::{EntryPayload, Membership};

const BINDING: [u8; 32] = [29; 32];

struct Fixture {
    issuer: Arc<RuntimeDriver>,
    consumer: Arc<RuntimeDriver>,
    machine: KafStateMachine,
    progress: AppliedHealthProgress,
    request: AdmissionRpc,
}

async fn apply(machine: &mut KafStateMachine, entry: EntryOf<TypeConfig>) {
    machine
        .apply(futures::stream::iter([Ok((entry, None))]))
        .await
        .unwrap();
}

impl Fixture {
    async fn new() -> Self {
        let mut nodes = Vec::new();
        let mut machines = Vec::new();
        for id in 1..=3 {
            let mut cfg = (*super::tests::config()).clone();
            cfg.node_id = id;
            cfg.peers = (1..=3)
                .map(|id| PeerConfig {
                    id,
                    raft_address: format!("127.0.0.1:{}", 1000 + id),
                    client_submit_address: format!("127.0.0.1:{}", 2000 + id),
                })
                .collect();
            cfg.raft_listen = cfg.peers[(id - 1) as usize].raft_address.clone();
            cfg.client_submit_listen = cfg.peers[(id - 1) as usize].client_submit_address.clone();
            let (_, machine, state) = new_admitted_store(Arc::new(vec![]), 3, true, 0);
            let timing = LeaseTiming::for_config(&cfg, 0).unwrap();
            nodes.push(RuntimeDriver::new(Arc::new(cfg), state, timing, Instant::now()).unwrap());
            machines.push(machine);
        }
        tokio::time::advance(nodes[0].timing.restart_quarantine()).await;
        let voters: BTreeSet<_> = nodes.iter().map(|node| node.local).collect();
        let genesis = discovery::cohort_genesis(&nodes[0].cfg, voters.clone()).unwrap();
        for consumer in &nodes {
            let mut round = consumer
                .begin(genesis.clone(), AdmissionMode::Cold)
                .unwrap();
            let mut grants = Vec::new();
            for issuer in &nodes {
                let signed = SignedAdmission::sign(
                    consumer.cfg.cluster_secret.as_deref(),
                    10,
                    consumer.local,
                    issuer.local,
                    BINDING,
                    round.request().clone(),
                )
                .unwrap();
                let response = issuer
                    .dispatch(consumer.local, BINDING, signed)
                    .await
                    .unwrap();
                grants.push(
                    ReceivedGrant::authenticate(
                        consumer.cfg.cluster_secret.as_deref(),
                        issuer.local,
                        consumer.local,
                        BINDING,
                        round.request(),
                        response,
                    )
                    .unwrap(),
                );
            }
            consumer.install(&mut round, &grants, None).await.unwrap();
        }
        let consumer = nodes[0].clone();
        let issuer = nodes[1].clone();
        let mut machine = machines.remove(1);
        for (index, payload) in [
            EntryPayload::Membership(Membership::new_with_defaults(vec![voters.clone()], voters)),
            EntryPayload::Normal(KafRequest::AdmissionGenesis(genesis.clone())),
            EntryPayload::Normal(KafRequest::HealthProgress(HealthProgress {
                node_id: consumer.local.physical_id,
                healthy: Some(true),
                replica: consumer.local,
                epoch: genesis.epoch,
                request_nonce: [17; 32],
                genesis: genesis.clone(),
            })),
        ]
        .into_iter()
        .enumerate()
        {
            apply(
                &mut machine,
                EntryOf::<TypeConfig> {
                    log_id: log_id::<TypeConfig>(1, consumer.local, index as u64 + 1),
                    payload,
                },
            )
            .await;
        }
        let mut round = consumer
            .begin(genesis.clone(), AdmissionMode::Renew { progress: None })
            .unwrap();
        let progress = AppliedHealthProgress {
            request: HealthProgress {
                node_id: consumer.local.physical_id,
                healthy: None,
                replica: consumer.local,
                epoch: genesis.epoch,
                request_nonce: round.request().request_nonce,
                genesis,
            },
            log_id: log_id::<TypeConfig>(1, consumer.local, 4),
        };
        round.bind_progress(&progress).unwrap();
        let request = SignedAdmission::sign(
            consumer.cfg.cluster_secret.as_deref(),
            10,
            consumer.local,
            issuer.local,
            BINDING,
            round.request().clone(),
        )
        .unwrap();
        Self {
            issuer,
            consumer,
            machine,
            progress,
            request,
        }
    }

    async fn apply_progress(&mut self, progress: AppliedHealthProgress) {
        apply(
            &mut self.machine,
            EntryOf::<TypeConfig> {
                log_id: progress.log_id,
                payload: EntryPayload::Normal(KafRequest::HealthProgress(progress.request)),
            },
        )
        .await;
    }
}

#[tokio::test(start_paused = true)]
async fn signed_renew_waits_for_exact_follower_apply_without_advancing_health_or_permission() {
    let mut fixture = Fixture::new().await;
    let issuer = fixture.issuer.clone();
    let consumer_permission = fixture.consumer.authority.current().unwrap();
    let issuer_permission = issuer.authority.current().unwrap();
    let before = {
        let state = issuer.state.read().await;
        (
            state.node_health.clone(),
            state.node_probe_ticks.clone(),
            state.latest_probe_tick,
        )
    };
    let grant = issuer.dispatch(fixture.consumer.local, BINDING, fixture.request.clone());
    tokio::pin!(grant);
    assert!(
        futures::poll!(&mut grant).is_pending(),
        "valid renewal before follower apply must wait, not reject"
    );
    assert!(
        issuer.state.try_write().is_ok(),
        "grant wait blocked state-machine apply"
    );
    assert_eq!(issuer.authority.current().unwrap(), issuer_permission);
    fixture.apply_progress(fixture.progress.clone()).await;
    let response = tokio::time::timeout(issuer.rpc_budget(), grant)
        .await
        .unwrap()
        .unwrap();
    response
        .verify(
            issuer.cfg.cluster_secret.as_deref(),
            11,
            issuer.local,
            fixture.consumer.local,
            BINDING,
        )
        .unwrap();
    assert_eq!(response.payload, fixture.request.payload);
    let state = issuer.state.read().await;
    assert_eq!(state.node_health, before.0);
    assert_eq!(state.node_probe_ticks, before.1);
    assert_eq!(state.latest_probe_tick, before.2);
    assert_eq!(
        state
            .applied_progress
            .get(&fixture.consumer.local.physical_id),
        Some(&fixture.progress)
    );
    assert_eq!(issuer.authority.current().unwrap(), issuer_permission);
    assert_eq!(
        fixture.consumer.authority.current().unwrap(),
        consumer_permission
    );
}

#[tokio::test(start_paused = true)]
async fn signed_renew_with_exact_progress_already_applied_grants_immediately() {
    let mut fixture = Fixture::new().await;
    fixture.apply_progress(fixture.progress.clone()).await;
    let grant = fixture
        .issuer
        .dispatch(fixture.consumer.local, BINDING, fixture.request.clone());
    tokio::pin!(grant);
    let std::task::Poll::Ready(Ok(response)) = futures::poll!(&mut grant) else {
        panic!("exact applied evidence must grant immediately");
    };
    response
        .verify(
            fixture.issuer.cfg.cluster_secret.as_deref(),
            11,
            fixture.issuer.local,
            fixture.consumer.local,
            BINDING,
        )
        .unwrap();
    assert_eq!(response.payload, fixture.request.payload);
}

#[tokio::test(start_paused = true)]
async fn renew_rejects_missing_progress_or_replaced_boot_with_a_valid_signature() {
    for replaced_boot in [false, true] {
        let mut fixture = Fixture::new().await;
        fixture.apply_progress(fixture.progress.clone()).await;
        let mut payload = fixture.request.payload.clone();
        if replaced_boot {
            payload.consumer = ReplicaId::fresh(fixture.consumer.local.physical_id).unwrap();
        } else {
            payload.mode = AdmissionMode::Renew { progress: None };
        }
        let peer = payload.consumer;
        let request = SignedAdmission::sign(
            fixture.issuer.cfg.cluster_secret.as_deref(),
            10,
            peer,
            fixture.issuer.local,
            BINDING,
            payload,
        )
        .unwrap();
        let grant = fixture.issuer.dispatch(peer, BINDING, request);
        tokio::pin!(grant);
        assert!(
            matches!(futures::poll!(&mut grant), std::task::Poll::Ready(Err(_))),
            "missing progress or replaced consumer boot was accepted"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn renew_cannot_grant_if_exact_apply_arrives_after_the_rpc_deadline() {
    let mut fixture = Fixture::new().await;
    let issuer = fixture.issuer.clone();
    let grant = issuer.dispatch(fixture.consumer.local, BINDING, fixture.request.clone());
    tokio::pin!(grant);
    assert!(futures::poll!(&mut grant).is_pending());
    tokio::time::advance(issuer.rpc_budget()).await;
    fixture.apply_progress(fixture.progress.clone()).await;
    assert!(
        matches!(futures::poll!(&mut grant), std::task::Poll::Ready(Err(_))),
        "late apply must not win over the expired RPC deadline"
    );
}

#[tokio::test(start_paused = true)]
async fn renew_state_lock_contention_is_included_in_the_existing_rpc_budget() {
    let fixture = Fixture::new().await;
    let issuer = &fixture.issuer;
    let permission = issuer.authority.current().unwrap();
    let writer = issuer.state.write().await;
    let grant = issuer.dispatch(fixture.consumer.local, BINDING, fixture.request.clone());
    tokio::pin!(grant);
    assert!(futures::poll!(&mut grant).is_pending());
    tokio::time::advance(issuer.rpc_budget()).await;
    assert!(
        matches!(futures::poll!(&mut grant), std::task::Poll::Ready(Err(_))),
        "state lock wait exceeded the existing RPC budget"
    );
    drop(writer);
    assert_eq!(issuer.authority.current().unwrap(), permission);
}

#[tokio::test(start_paused = true)]
async fn renew_rejects_wrong_nonce_or_log_term_at_the_applied_frontier() {
    for wrong_nonce in [true, false] {
        let mut fixture = Fixture::new().await;
        let mut wrong = fixture.progress.clone();
        if wrong_nonce {
            wrong.request.request_nonce[0] ^= 1;
        } else {
            wrong.log_id = log_id::<TypeConfig>(2, fixture.consumer.local, wrong.log_id.index);
        }
        fixture.apply_progress(wrong).await;
        let grant =
            fixture
                .issuer
                .dispatch(fixture.consumer.local, BINDING, fixture.request.clone());
        tokio::pin!(grant);
        assert!(
            matches!(futures::poll!(&mut grant), std::task::Poll::Ready(Err(_))),
            "applied mismatching evidence must fail immediately"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn renew_rechecks_nonce_and_term_after_waiting_for_apply() {
    for wrong_nonce in [true, false] {
        let mut fixture = Fixture::new().await;
        let issuer = fixture.issuer.clone();
        let grant = issuer.dispatch(fixture.consumer.local, BINDING, fixture.request.clone());
        tokio::pin!(grant);
        assert!(
            futures::poll!(&mut grant).is_pending(),
            "valid pending challenge was rejected before apply"
        );
        let mut wrong = fixture.progress.clone();
        if wrong_nonce {
            wrong.request.request_nonce[0] ^= 1;
        } else {
            wrong.log_id = log_id::<TypeConfig>(2, fixture.consumer.local, wrong.log_id.index);
        }
        fixture.apply_progress(wrong).await;
        assert!(
            tokio::time::timeout(issuer.rpc_budget(), grant)
                .await
                .unwrap()
                .is_err()
        );
    }
}

#[tokio::test(start_paused = true)]
async fn renew_unresolved_apply_times_out_within_existing_rpc_budget() {
    let fixture = Fixture::new().await;
    let issuer = &fixture.issuer;
    let permission = issuer.authority.current().unwrap();
    let before = {
        let state = issuer.state.read().await;
        (
            state.applied_progress.clone(),
            state.node_probe_ticks.clone(),
            state.latest_probe_tick,
        )
    };
    let started = Instant::now();
    let grant = issuer.dispatch(fixture.consumer.local, BINDING, fixture.request.clone());
    tokio::pin!(grant);
    assert!(
        futures::poll!(&mut grant).is_pending(),
        "unapplied progress must get its existing RPC budget"
    );
    tokio::time::advance(issuer.rpc_budget()).await;
    assert!(
        matches!(futures::poll!(&mut grant), std::task::Poll::Ready(Err(_))),
        "unresolved apply exceeded the existing RPC budget"
    );
    assert_eq!(Instant::now() - started, issuer.rpc_budget());
    let after = issuer.state.read().await;
    assert_eq!(after.applied_progress, before.0);
    assert_eq!(after.node_probe_ticks, before.1);
    assert_eq!(after.latest_probe_tick, before.2);
    assert_eq!(issuer.authority.current().unwrap(), permission);
}

#[tokio::test(start_paused = true)]
async fn renew_fails_closed_if_authority_changes_during_apply_wait() {
    for change in ["expired", "shutdown", "genesis", "unbound"] {
        let mut fixture = Fixture::new().await;
        let issuer = fixture.issuer.clone();
        if change == "expired" {
            tokio::time::advance(issuer.timing.consumer_use() - issuer.rpc_budget() / 2).await;
        }
        let grant = issuer.dispatch(fixture.consumer.local, BINDING, fixture.request.clone());
        tokio::pin!(grant);
        assert!(
            futures::poll!(&mut grant).is_pending(),
            "{change}: rejected before waiting for apply"
        );
        fixture.apply_progress(fixture.progress.clone()).await;
        match change {
            "expired" => tokio::time::advance(issuer.rpc_budget() / 2).await,
            "shutdown" => issuer.shutdown(),
            "genesis" => issuer.state.write().await.genesis.as_mut().unwrap().epoch += 1,
            "unbound" => issuer.state.write().await.admission = None,
            _ => unreachable!(),
        }
        assert!(
            tokio::time::timeout(issuer.rpc_budget(), grant)
                .await
                .unwrap()
                .is_err(),
            "accepted {change} authority after waiting"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn renew_with_expired_shutdown_or_mismatched_genesis_never_grants() {
    for change in ["expired", "shutdown", "genesis"] {
        let mut fixture = Fixture::new().await;
        fixture.apply_progress(fixture.progress.clone()).await;
        let issuer = &fixture.issuer;
        match change {
            "expired" => tokio::time::advance(issuer.timing.consumer_use()).await,
            "shutdown" => issuer.shutdown(),
            "genesis" => issuer.state.write().await.genesis.as_mut().unwrap().epoch += 1,
            _ => unreachable!(),
        }
        assert!(
            issuer
                .dispatch(fixture.consumer.local, BINDING, fixture.request.clone())
                .await
                .is_err(),
            "accepted {change} authority"
        );
    }
}
