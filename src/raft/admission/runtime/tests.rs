use super::*;
use crate::raft::admission::AdmissionRequest;
use crate::raft::network::authorization::AdmissionController as NetworkController;
use crate::raft::store::new_store;
use std::time::Duration;

pub(super) fn config() -> Arc<Config> {
    Arc::new(serde_yaml::from_str(
        "node_id: 1\nraft_listen: '127.0.0.1:1000'\nclient_submit_listen: '127.0.0.1:2000'\npeers:\n  - {id: 1, raft_address: '127.0.0.1:1000', client_submit_address: '127.0.0.1:2000'}\nvips: []\nhealth: {command: [/bin/true], interval_ms: 1000, timeout_ms: 500}\ncluster_secret: runtime-driver-fixture-key-0123456789\ndry_run: true\n",
    ).unwrap())
}

pub(super) fn driver() -> Arc<RuntimeDriver> {
    let (_, _, state) = new_store(Arc::new(vec![]), 3, true, 0);
    RuntimeDriver::new(
        config(),
        state,
        LeaseTiming::new(Duration::from_secs(5), Duration::from_secs(1)).unwrap(),
        Instant::now(),
    )
    .unwrap()
}

#[tokio::test(start_paused = true)]
async fn fresh_driver_denies_raft_and_has_distinct_process_identity() {
    let first = driver();
    let second = driver();
    assert_ne!(first.local_replica(), second.local_replica());
    assert_eq!(first.local_replica().physical_id, 1);
    assert!(first.current().is_none());
    assert!(!first.vip_activation_ready().await);
    assert!(first.authorize_raft(first.local_replica()).is_err());
    tokio::time::advance(Duration::from_secs(60)).await;
    assert!(first.authorize_raft(first.local_replica()).is_err());
}

#[tokio::test(start_paused = true)]
async fn shutdown_denies_discovery_and_grants_without_reviving_authority() {
    let runtime = driver();
    runtime.shutdown();
    let local = runtime.local_replica();
    let request = SignedAdmission::sign(
        config().cluster_secret.as_deref(),
        REQUEST_ROLE,
        local,
        local,
        [7; 32],
        ManagementRequest {
            nonce: [8; 32],
            action: ManagementAction::Discover,
        },
    )
    .unwrap();
    assert!(runtime.management(local, [7; 32], request).await.is_err());
    assert!(runtime.current().is_none());
}

#[tokio::test(start_paused = true)]
async fn discovery_is_signed_and_bound_to_the_exact_channel_and_challenge() {
    let runtime = driver();
    let local = runtime.local_replica();
    let request = SignedAdmission::sign(
        config().cluster_secret.as_deref(),
        REQUEST_ROLE,
        local,
        local,
        [7; 32],
        ManagementRequest {
            nonce: [8; 32],
            action: ManagementAction::Discover,
        },
    )
    .unwrap();
    assert!(
        runtime
            .management(local, [9; 32], request.clone())
            .await
            .is_err()
    );
    let response = runtime.management(local, [7; 32], request).await.unwrap();
    response
        .verify(
            config().cluster_secret.as_deref(),
            RESPONSE_ROLE,
            local,
            local,
            [7; 32],
        )
        .unwrap();
    assert_eq!(response.payload.nonce, [8; 32]);
    assert_eq!(
        response.payload.result,
        ManagementResult::Discovery {
            genesis: None,
            voters: BTreeSet::new(),
        }
    );
    assert!(runtime.current().is_none());
}

#[tokio::test(start_paused = true)]
async fn reserve_rejects_a_changed_authenticated_boot_and_quarantine() {
    let runtime = driver();
    let local = runtime.local_replica();
    let genesis = discovery::cohort_genesis(config().as_ref(), [local].into()).unwrap();
    let request = SignedAdmission::sign(
        config().cluster_secret.as_deref(),
        10,
        local,
        local,
        [7; 32],
        AdmissionRequest {
            consumer: local,
            genesis,
            request_nonce: [8; 32],
            mode: AdmissionMode::Cold,
        },
    )
    .unwrap();
    assert!(
        runtime
            .dispatch(local, [7; 32], request.clone())
            .await
            .is_err()
    );
    tokio::time::advance(Duration::from_secs(24)).await;
    let other = ReplicaId::fresh(1).unwrap();
    assert!(
        runtime
            .dispatch(other, [7; 32], request.clone())
            .await
            .is_err()
    );
    assert!(
        runtime
            .dispatch(local, [9; 32], request.clone())
            .await
            .is_err()
    );
    let response = runtime
        .dispatch(local, [7; 32], request.clone())
        .await
        .unwrap();
    response
        .verify(
            config().cluster_secret.as_deref(),
            11,
            local,
            local,
            [7; 32],
        )
        .unwrap();
    assert_eq!(response.payload, request.payload);
    assert!(runtime.current().is_none());
}

#[test]
fn cohort_genesis_is_deterministic_and_includes_every_exact_boot() {
    let local = ReplicaId::fresh(1).unwrap();
    let first = discovery::cohort_genesis(config().as_ref(), [local].into()).unwrap();
    let second = discovery::cohort_genesis(config().as_ref(), [local].into()).unwrap();
    assert_eq!(first, second);
    let replacement = ReplicaId::fresh(1).unwrap();
    let changed = discovery::cohort_genesis(config().as_ref(), [replacement].into()).unwrap();
    assert_ne!(first.epoch, changed.epoch);
    assert!(discovery::cohort_genesis(config().as_ref(), BTreeSet::new()).is_err());
    assert!(discovery::cohort_genesis(config().as_ref(), [local, replacement].into()).is_err());
}

#[tokio::test(start_paused = true)]
async fn activation_timestamp_is_installed_by_the_first_certificate_only() {
    use crate::raft::admission::ReceivedGrant;
    let runtime = driver();
    let local = runtime.local_replica();
    tokio::time::advance(runtime.timing.restart_quarantine()).await;
    let genesis = discovery::cohort_genesis(config().as_ref(), [local].into()).unwrap();
    let mut round = runtime
        .core
        .lock()
        .unwrap()
        .begin(genesis, AdmissionMode::Cold)
        .unwrap();
    let record = SignedAdmission::sign(
        config().cluster_secret.as_deref(),
        10,
        local,
        local,
        [7; 32],
        round.request().clone(),
    )
    .unwrap();
    let grant = runtime.dispatch(local, [7; 32], record).await.unwrap();
    let grant = ReceivedGrant::authenticate(
        config().cluster_secret.as_deref(),
        local,
        local,
        [7; 32],
        round.request(),
        grant,
    )
    .unwrap();
    let accepted = Instant::now();
    runtime.install(&mut round, &[grant], None).await.unwrap();
    assert_eq!(
        *runtime.first_verified_admission.lock().unwrap(),
        Some(accepted)
    );
    assert!(!runtime.vip_activation_ready().await);
    let lock = runtime.state.write().await;
    assert!(runtime.authorize_raft(local).is_err());
    assert!(!runtime.vip_activation_ready().await);
    drop(lock);
    runtime.shutdown();
    assert!(!runtime.vip_activation_ready().await);
    assert_eq!(
        *runtime.first_verified_admission.lock().unwrap(),
        Some(accepted)
    );
}

#[tokio::test(start_paused = true)]
async fn an_admission_bound_to_another_driver_is_never_current() {
    use crate::raft::admission::ReceivedGrant;
    let first = driver();
    tokio::time::advance(first.timing.restart_quarantine()).await;
    let local = first.local_replica();
    let genesis = discovery::cohort_genesis(config().as_ref(), [local].into()).unwrap();
    let mut round = first
        .core
        .lock()
        .unwrap()
        .begin(genesis, AdmissionMode::Cold)
        .unwrap();
    let request = SignedAdmission::sign(
        config().cluster_secret.as_deref(),
        10,
        local,
        local,
        [7; 32],
        round.request().clone(),
    )
    .unwrap();
    let record = first.dispatch(local, [7; 32], request).await.unwrap();
    let grant = ReceivedGrant::authenticate(
        config().cluster_secret.as_deref(),
        local,
        local,
        [7; 32],
        round.request(),
        record,
    )
    .unwrap();
    first.install(&mut round, &[grant], None).await.unwrap();
    assert!(first.current().is_some());
    let second =
        RuntimeDriver::new(config(), first.state.clone(), first.timing, Instant::now()).unwrap();
    assert!(second.current().is_none());
    assert!(second.authorize_raft(local).is_err());
}

#[tokio::test(start_paused = true)]
async fn run_without_attachment_fails_and_terminally_seals_the_driver() {
    let runtime = driver();
    let network =
        Arc::new(RaftNetworkImpl::new(config(), runtime.state.clone(), runtime.clone()).unwrap());
    let error = runtime.run(network).await.unwrap_err();
    assert!(error.to_string().contains("not been attached"));
    assert!(runtime.stopped.load(Ordering::SeqCst));
    assert!(runtime.current().is_none());
}

#[tokio::test(start_paused = true)]
async fn health_submission_without_admission_never_attempts_network_io() {
    let runtime = driver();
    let network = RaftNetworkImpl::new(config(), runtime.state.clone(), runtime.clone()).unwrap();
    assert!(runtime.submit_health(&network, true).await.is_err());
    assert!(runtime.submit_health(&network, false).await.is_err());
    assert!(runtime.state.read().await.applied_progress.is_empty());
    assert!(runtime.state.read().await.node_probe_ticks.is_empty());
}

#[tokio::test(start_paused = true)]
async fn management_mutations_require_admission_even_with_a_valid_signature() {
    let runtime = driver();
    let local = runtime.local_replica();
    let genesis = discovery::cohort_genesis(config().as_ref(), [local].into()).unwrap();
    let request = SignedAdmission::sign(
        config().cluster_secret.as_deref(),
        REQUEST_ROLE,
        local,
        local,
        [7; 32],
        ManagementRequest {
            nonce: [8; 32],
            action: ManagementAction::PrepareJoin {
                genesis,
                operation_nonce: [9; 32],
            },
        },
    )
    .unwrap();
    assert!(runtime.management(local, [7; 32], request).await.is_err());
    assert!(runtime.state.read().await.prepared_join.is_none());
}

async fn admit_driver() -> Arc<RuntimeDriver> {
    use crate::raft::admission::ReceivedGrant;
    let runtime = driver();
    tokio::time::advance(runtime.timing.restart_quarantine()).await;
    let local = runtime.local_replica();
    let genesis = discovery::cohort_genesis(config().as_ref(), [local].into()).unwrap();
    let mut round = runtime
        .core
        .lock()
        .unwrap()
        .begin(genesis, AdmissionMode::Cold)
        .unwrap();
    let request = SignedAdmission::sign(
        config().cluster_secret.as_deref(),
        10,
        local,
        local,
        [7; 32],
        round.request().clone(),
    )
    .unwrap();
    let record = runtime.dispatch(local, [7; 32], request).await.unwrap();
    let grant = ReceivedGrant::authenticate(
        config().cluster_secret.as_deref(),
        local,
        local,
        [7; 32],
        round.request(),
        record,
    )
    .unwrap();
    runtime.install(&mut round, &[grant], None).await.unwrap();
    runtime
}

fn outstanding_progress(runtime: &RuntimeDriver) -> crate::raft::admission::AppliedHealthProgress {
    let genesis = runtime.current().unwrap().context().genesis.clone();
    crate::raft::admission::AppliedHealthProgress {
        request: crate::raft::admission::HealthProgress {
            node_id: runtime.local.physical_id,
            healthy: None,
            replica: runtime.local,
            epoch: genesis.epoch,
            request_nonce: [8; 32],
            genesis,
        },
        log_id: openraft::testing::log_id::<crate::raft::TypeConfig>(1, runtime.local, 3),
    }
}

fn open_activation_window(runtime: &RuntimeDriver) {
    let delay = runtime
        .timing
        .vip_activation_deadline(Instant::now())
        .unwrap()
        - Instant::now();
    *runtime.first_verified_admission.lock().unwrap() = Some(Instant::now() - delay);
}

#[tokio::test(start_paused = true)]
async fn activation_check_waits_for_state_instead_of_invalidating_a_fresh_proof() {
    let runtime = admit_driver().await;
    open_activation_window(&runtime);
    assert!(runtime.vip_activation_ready().await);
    let held = runtime.state.write().await;
    let check = runtime.vip_activation_ready();
    tokio::pin!(check);
    assert!(
        futures::poll!(&mut check).is_pending(),
        "temporary state contention invalidated an otherwise fresh proof"
    );
    drop(held);
    assert!(check.await);
}

#[tokio::test(start_paused = true)]
async fn activation_check_cannot_wait_past_permission_expiry() {
    let runtime = admit_driver().await;
    open_activation_window(&runtime);
    let held = runtime.state.write().await;
    let check = runtime.vip_activation_ready();
    tokio::pin!(check);
    assert!(futures::poll!(&mut check).is_pending());
    tokio::time::advance(runtime.timing.consumer_use()).await;
    assert!(!check.await);
    drop(held);
    assert!(!runtime.vip_activation_ready().await);
}

#[tokio::test(start_paused = true)]
async fn activation_check_stops_waiting_when_runtime_is_shutdown() {
    let runtime = admit_driver().await;
    open_activation_window(&runtime);
    let held = runtime.state.write().await;
    let check = runtime.vip_activation_ready();
    tokio::pin!(check);
    assert!(futures::poll!(&mut check).is_pending());
    runtime.shutdown();
    assert!(!check.await);
    drop(held);
}

#[tokio::test(start_paused = true)]
async fn activation_check_rejects_a_missing_session_after_contention() {
    let runtime = admit_driver().await;
    open_activation_window(&runtime);
    let mut held = runtime.state.write().await;
    let check = runtime.vip_activation_ready();
    tokio::pin!(check);
    assert!(futures::poll!(&mut check).is_pending());
    held.admission = None;
    drop(held);
    assert!(!check.await);
}

#[tokio::test(start_paused = true)]
async fn wait_progress_bounds_missing_apply_without_renewing_permission() {
    let runtime = admit_driver().await;
    let applied = outstanding_progress(&runtime);
    let deadline = runtime.current().unwrap().check().unwrap();
    let wait = runtime.wait_progress(&applied);
    tokio::pin!(wait);
    assert!(futures::poll!(&mut wait).is_pending());
    tokio::time::advance(runtime.rpc_budget()).await;
    let std::task::Poll::Ready(result) = futures::poll!(&mut wait) else {
        panic!("local progress wait remains pending after its apply budget");
    };
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("local progress apply timed out")
    );
    assert_eq!(runtime.current().unwrap().check().unwrap(), deadline);
    {
        let mut state = runtime.state.write().await;
        assert!(state.applied_progress.is_empty());
        assert!(state.node_probe_ticks.is_empty());
        state
            .applied_progress
            .insert(runtime.local.physical_id, applied.clone());
        state.last_applied_log = Some(applied.log_id);
    }
    runtime.wait_progress(&applied).await.unwrap();
    assert_eq!(runtime.current().unwrap().check().unwrap(), deadline);
}

#[tokio::test(start_paused = true)]
async fn wait_progress_bounds_state_contention() {
    let runtime = admit_driver().await;
    let applied = outstanding_progress(&runtime);
    let held = runtime.state.write().await;
    let wait = runtime.wait_progress(&applied);
    tokio::pin!(wait);
    assert!(futures::poll!(&mut wait).is_pending());
    tokio::time::advance(runtime.rpc_budget()).await;
    let std::task::Poll::Ready(result) = futures::poll!(&mut wait) else {
        panic!("local progress lock wait remains pending after its apply budget");
    };
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("local progress apply timed out")
    );
    drop(held);
    assert!(runtime.current().is_some());
}

#[tokio::test(start_paused = true)]
async fn wait_progress_requires_exact_challenge_and_applied_index() {
    let runtime = admit_driver().await;
    let applied = outstanding_progress(&runtime);
    let wait = runtime.wait_progress(&applied);
    tokio::pin!(wait);
    assert!(futures::poll!(&mut wait).is_pending());
    tokio::time::advance(runtime.rpc_budget() / 4).await;
    runtime
        .state
        .write()
        .await
        .applied_progress
        .insert(runtime.local.physical_id, applied.clone());
    assert!(futures::poll!(&mut wait).is_pending());
    runtime.state.write().await.last_applied_log = Some(applied.log_id);
    tokio::time::advance(Duration::from_millis(10)).await;
    wait.await.unwrap();

    let mut different = applied.clone();
    different.request.request_nonce = [9; 32];
    assert!(
        runtime
            .wait_progress(&different)
            .await
            .unwrap_err()
            .to_string()
            .contains("locally applied progress differs from outstanding challenge")
    );
}

#[tokio::test(start_paused = true)]
async fn coherent_session_waits_for_transient_state_contention() {
    let runtime = admit_driver().await;
    let held = runtime.state.write().await;
    let session = runtime.coherent_session();
    tokio::pin!(session);
    assert!(futures::poll!(&mut session).is_pending());
    assert!(runtime.authority.current().is_some());
    drop(held);
    assert_eq!(
        session.await.unwrap().context().local_replica,
        runtime.local
    );
    assert!(!runtime.stopped.load(Ordering::SeqCst));
}

#[tokio::test(start_paused = true)]
async fn coherent_session_cannot_wait_past_the_authority_deadline() {
    let runtime = admit_driver().await;
    let held = runtime.state.write().await;
    let session = runtime.coherent_session();
    tokio::pin!(session);
    assert!(futures::poll!(&mut session).is_pending());
    tokio::time::advance(runtime.timing.consumer_use()).await;
    assert!(session.await.is_err());
    drop(held);
    assert!(runtime.current().is_none());
}

#[tokio::test(start_paused = true)]
async fn coherent_session_wakes_on_shutdown_while_the_state_is_busy() {
    let runtime = admit_driver().await;
    let held = runtime.state.write().await;
    let session = runtime.coherent_session();
    tokio::pin!(session);
    assert!(futures::poll!(&mut session).is_pending());
    runtime.shutdown();
    assert!(session.await.is_err());
    drop(held);
}

fn renewed_certificate(runtime: &RuntimeDriver) -> crate::raft::admission::VerifiedAdmission {
    use crate::raft::admission::{AppliedHealthProgress, HealthProgress, ReceivedGrant};
    let session = runtime.current().unwrap();
    let core = runtime.core.lock().unwrap();
    let mut round = core
        .begin(
            session.context().genesis.clone(),
            AdmissionMode::Renew { progress: None },
        )
        .unwrap();
    let progress = AppliedHealthProgress {
        request: HealthProgress {
            node_id: runtime.local.physical_id,
            healthy: None,
            replica: runtime.local,
            epoch: session.context().genesis.epoch,
            request_nonce: round.request().request_nonce,
            genesis: session.context().genesis.clone(),
        },
        log_id: openraft::testing::log_id::<crate::raft::TypeConfig>(1, runtime.local, 3),
    };
    round.bind_progress(&progress).unwrap();
    let signed = SignedAdmission::sign(
        config().cluster_secret.as_deref(),
        11,
        runtime.local,
        runtime.local,
        [7; 32],
        round.request().clone(),
    )
    .unwrap();
    let grant = ReceivedGrant::authenticate(
        config().cluster_secret.as_deref(),
        runtime.local,
        runtime.local,
        [7; 32],
        round.request(),
        signed,
    )
    .unwrap();
    core.complete(&mut round, &[grant]).unwrap()
}

#[tokio::test(start_paused = true)]
async fn verified_renewal_at_the_same_clock_tick_preserves_the_deadline() {
    let runtime = admit_driver().await;
    let original = runtime.authority.current().unwrap().1;
    let renewed = renewed_certificate(&runtime);
    assert_eq!(renewed.deadline(), original);
    let session = runtime.authority.accept(&renewed).unwrap();
    assert_eq!(session.check().unwrap(), original);
    tokio::time::advance(runtime.timing.consumer_use()).await;
    assert!(session.check().is_err());
    assert!(runtime.authority.accept(&renewed).is_err());
    assert!(runtime.current().is_none());
}

#[tokio::test(start_paused = true)]
async fn admission_deadline_wait_follows_concurrent_verified_renewal() {
    let runtime = admit_driver().await;
    let original = runtime.authority.current().unwrap().1;
    tokio::time::advance(runtime.timing.consumer_use() / 2).await;
    let renewed = renewed_certificate(&runtime);
    let (send, receive) = tokio::sync::oneshot::channel();
    let wait = runtime.within_admission(async { Ok(receive.await?) });
    tokio::pin!(wait);
    assert!(futures::poll!(&mut wait).is_pending());
    runtime.authority.accept(&renewed).unwrap();
    tokio::time::advance(original - Instant::now()).await;
    assert!(
        futures::poll!(&mut wait).is_pending(),
        "an obsolete deadline must not terminate current authority"
    );
    send.send(7).unwrap();
    assert_eq!(wait.await.unwrap(), 7);
    assert!(runtime.current().is_some());
}

#[tokio::test(start_paused = true)]
async fn coherent_session_follows_concurrent_verified_renewal() {
    let runtime = admit_driver().await;
    let original = runtime.authority.current().unwrap().1;
    tokio::time::advance(runtime.timing.consumer_use() / 2).await;
    let renewed = renewed_certificate(&runtime);
    let held = runtime.state.write().await;
    let wait = runtime.coherent_session();
    tokio::pin!(wait);
    assert!(futures::poll!(&mut wait).is_pending());
    runtime.authority.accept(&renewed).unwrap();
    tokio::time::advance(original - Instant::now()).await;
    assert!(futures::poll!(&mut wait).is_pending());
    drop(held);
    assert_eq!(wait.await.unwrap().check().unwrap(), renewed.deadline());
}

#[tokio::test(start_paused = true)]
async fn admission_work_stops_at_actual_expiry_and_on_shutdown() {
    for shutdown in [false, true] {
        let runtime = admit_driver().await;
        let wait = runtime.within_admission(std::future::pending::<anyhow::Result<()>>());
        tokio::pin!(wait);
        assert!(futures::poll!(&mut wait).is_pending());
        if shutdown {
            runtime.shutdown();
        } else {
            tokio::time::advance(runtime.timing.consumer_use()).await;
        }
        assert!(futures::poll!(&mut wait).is_ready(), "shutdown={shutdown}");
        assert!(runtime.current().is_none());
    }
}
