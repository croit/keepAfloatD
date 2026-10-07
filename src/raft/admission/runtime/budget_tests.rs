use super::*;
use crate::raft::admission::ReceivedGrant;
use crate::raft::store::new_store;
use std::time::Duration;

fn driver() -> (Arc<RuntimeDriver>, Arc<RaftNetworkImpl>) {
    let cfg = super::tests::config();
    let (_, _, state) = new_store(Arc::new(vec![]), 3, true, 0);
    let timing = LeaseTiming::for_config(&cfg, 0).unwrap();
    let runtime = RuntimeDriver::new(cfg.clone(), state.clone(), timing, Instant::now()).unwrap();
    let network = Arc::new(RaftNetworkImpl::new(cfg, state, runtime.clone()).unwrap());
    (runtime, network)
}

async fn admit(runtime: &RuntimeDriver) {
    tokio::time::advance(runtime.timing.restart_quarantine()).await;
    let local = runtime.local;
    let genesis = discovery::cohort_genesis(&runtime.cfg, [local].into()).unwrap();
    let mut round = runtime
        .core
        .lock()
        .unwrap()
        .begin(genesis, AdmissionMode::Cold)
        .unwrap();
    let binding = [7; 32];
    let request = SignedAdmission::sign(
        runtime.cfg.cluster_secret.as_deref(),
        10,
        local,
        local,
        binding,
        round.request().clone(),
    )
    .unwrap();
    let response = runtime.dispatch(local, binding, request).await.unwrap();
    let grant = ReceivedGrant::authenticate(
        runtime.cfg.cluster_secret.as_deref(),
        local,
        local,
        binding,
        round.request(),
        response,
    )
    .unwrap();
    runtime.install(&mut round, &[grant], None).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn local_grant_state_contention_ends_at_the_rpc_budget() {
    let (runtime, network) = driver();
    tokio::time::advance(runtime.timing.restart_quarantine()).await;
    let genesis = discovery::cohort_genesis(&runtime.cfg, [runtime.local].into()).unwrap();
    let round = runtime
        .core
        .lock()
        .unwrap()
        .begin(genesis, AdmissionMode::Cold)
        .unwrap();
    let locked = runtime.state.write().await;
    let started = Instant::now();
    let result = tokio::time::timeout(
        runtime.rpc_budget() + Duration::from_millis(1),
        runtime.grants(&network, &round, &[runtime.local].into()),
    )
    .await;
    drop(locked);
    runtime.shutdown();
    network.shutdown().await.unwrap();
    let grants = result
        .expect("local grant consumed more than its RPC budget")
        .unwrap();
    assert!(grants.is_empty());
    assert_eq!(Instant::now() - started, runtime.rpc_budget());
}

#[tokio::test(start_paused = true)]
async fn renewal_round_lock_times_out_without_expiring_authority() {
    let (runtime, network) = driver();
    admit(&runtime).await;
    let locked = runtime.round.lock().await;
    let budget = runtime.timing.renewal_round_budget();
    let started = Instant::now();
    let result = tokio::time::timeout(
        budget + Duration::from_millis(1),
        runtime.submit_health(&network, true),
    )
    .await;
    let still_admitted = runtime.current().is_some();
    drop(locked);
    runtime.shutdown();
    network.shutdown().await.unwrap();
    let error = result
        .expect("round lock consumed the admission lifetime")
        .unwrap_err();
    assert!(
        error.to_string().contains("round lock timed out"),
        "{error:#}"
    );
    assert!(still_admitted);
    assert_eq!(Instant::now() - started, budget);
}

#[tokio::test(start_paused = true)]
async fn genesis_phase_state_contention_has_an_independent_budget() {
    let (runtime, network) = driver();
    admit(&runtime).await;
    let locked = runtime.state.write().await;
    let budget = runtime.timing.renewal_round_budget();
    let started = Instant::now();
    let result = tokio::time::timeout(
        budget + Duration::from_millis(1),
        runtime.drive_round(&network),
    )
    .await;
    drop(locked);
    let still_admitted = runtime.current().is_some();
    runtime.shutdown();
    network.shutdown().await.unwrap();
    let error = result
        .expect("genesis phase consumed the admission lifetime")
        .unwrap_err();
    assert!(
        error.to_string().contains("genesis phase timed out"),
        "{error:#}"
    );
    assert!(still_admitted);
    assert_eq!(Instant::now() - started, budget);
}

#[tokio::test(start_paused = true)]
async fn renewal_body_timeout_releases_round_lock_and_preserves_authority() {
    let (runtime, network) = driver();
    admit(&runtime).await;
    let locked = runtime.state.write().await;
    let budget = runtime.timing.renewal_round_budget();
    let started = Instant::now();
    let result = tokio::time::timeout(
        budget + Duration::from_millis(1),
        runtime.submit_health(&network, true),
    )
    .await;
    drop(locked);
    let still_admitted = runtime.current().is_some();
    let round_released = runtime.round.try_lock().is_ok();
    runtime.shutdown();
    network.shutdown().await.unwrap();
    let error = result
        .expect("renewal body exceeded its independent budget")
        .unwrap_err();
    assert!(
        error.to_string().contains("renewal phase timed out"),
        "{error:#}"
    );
    assert!(still_admitted && round_released);
    assert_eq!(Instant::now() - started, budget);
}

#[tokio::test(start_paused = true)]
async fn local_grant_retry_succeeds_after_the_state_lock_is_released() {
    let (runtime, network) = driver();
    tokio::time::advance(runtime.timing.restart_quarantine()).await;
    let genesis = discovery::cohort_genesis(&runtime.cfg, [runtime.local].into()).unwrap();
    let mut round = runtime
        .core
        .lock()
        .unwrap()
        .begin(genesis, AdmissionMode::Cold)
        .unwrap();
    let locked = runtime.state.write().await;
    assert!(
        runtime
            .grants(&network, &round, &[runtime.local].into())
            .await
            .unwrap()
            .is_empty()
    );
    drop(locked);
    let grants = runtime
        .grants(&network, &round, &[runtime.local].into())
        .await
        .unwrap();
    assert_eq!(grants.len(), 1);
    runtime.install(&mut round, &grants, None).await.unwrap();
    assert!(runtime.current().is_some());
    runtime.shutdown();
    network.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn established_voter_renews_before_waiting_for_membership_maintenance() {
    use crate::raft::admission::{AdmissionCommand, JoinPlan};
    use crate::raft::types::KafRequest;
    use tokio::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let submit = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut cfg = (*super::tests::config()).clone();
    cfg.raft_listen = listener.local_addr().unwrap().to_string();
    cfg.peers[0].raft_address = cfg.raft_listen.clone();
    cfg.client_submit_listen = submit.local_addr().unwrap().to_string();
    cfg.peers[0].client_submit_address = cfg.client_submit_listen.clone();
    let cfg = Arc::new(cfg);
    let (log, machine, state) = new_store(Arc::new(vec![]), 3, true, 0);
    let runtime = RuntimeDriver::new(
        cfg.clone(),
        state.clone(),
        LeaseTiming::for_config(&cfg, 0).unwrap(),
        Instant::now(),
    )
    .unwrap();
    let network = Arc::new(RaftNetworkImpl::new(cfg, state.clone(), runtime.clone()).unwrap());
    let raft = KafRaft::new(
        runtime.local,
        Arc::new(openraft::Config {
            enable_tick: false,
            ..Default::default()
        }),
        network.as_ref().clone(),
        log,
        machine,
    )
    .await
    .unwrap();
    runtime.attach(raft.clone()).unwrap();
    admit(&runtime).await;
    let genesis = runtime.current().unwrap().context().genesis.clone();
    raft.initialize(
        [(runtime.local, runtime.node(runtime.local).unwrap())]
            .into_iter()
            .collect::<std::collections::BTreeMap<_, _>>(),
    )
    .await
    .unwrap();
    raft.client_write(KafRequest::AdmissionGenesis(genesis.clone()))
        .await
        .unwrap();
    let replacement = ReplicaId::fresh(runtime.local.physical_id).unwrap();
    let plan = JoinPlan {
        genesis,
        consumer: replacement,
        request_nonce: [19; 32],
        previous_voters: [runtime.local].into(),
        next_voters: [replacement].into(),
    };
    raft.client_write(KafRequest::AdmissionMembership(
        AdmissionCommand::PrepareJoin(plan),
    ))
    .await
    .unwrap();
    assert!(state.read().await.prepared_join.is_some());
    assert!(state.read().await.applied_progress.is_empty());
    let mut work = Box::pin(runtime.drive_round(&network));
    let mut accepted = Box::pin(listener.accept());
    let mut reached_maintenance = false;
    let deadline = Instant::now() + runtime.timing.renewal_round_budget();
    while Instant::now() < deadline && !reached_maintenance {
        for _ in 0..20 {
            let progress = futures::poll!(work.as_mut());
            assert!(
                progress.is_pending(),
                "round ended before maintenance: {progress:?}"
            );
            if let std::task::Poll::Ready(stream) = futures::poll!(accepted.as_mut()) {
                stream.unwrap();
                reached_maintenance = true;
                break;
            }
            tokio::task::yield_now().await;
        }
        if !reached_maintenance {
            tokio::time::advance(Duration::from_millis(1)).await;
        }
    }
    let renewed = state
        .read()
        .await
        .applied_progress
        .contains_key(&runtime.local.physical_id);
    let health_unchanged = state.read().await.node_health.is_empty();
    drop(work);
    runtime.shutdown();
    network.shutdown().await.unwrap();
    raft.shutdown().await.unwrap();
    assert!(
        reached_maintenance,
        "did not reach the pending learner discovery"
    );
    assert!(
        renewed,
        "maintenance ran before the established voter's committed renewal"
    );
    assert!(
        health_unchanged,
        "admission-only renewal manufactured service health"
    );
}
