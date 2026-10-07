use super::*;
use crate::config::ClusterConfigFingerprint;
use crate::raft::admission::{
    AdmissionContext, AdmissionDenied, AdmissionFence, AdmissionSession, Genesis, ReplicaId,
};
use crate::raft::store::KafLogStore;
use crate::raft::types::test_replica;
use crate::runtime_permission::{AuthorityContext, RuntimePermission};
use futures::stream;
use openraft::Vote;
use openraft::async_runtime::WatchReceiver;
use openraft::storage::{IOFlushed, RaftLogStorage};
use openraft::testing::log_id;
use openraft::type_config::TypeConfigExt;
use std::time::Duration;
use tokio::time::Instant;

struct DeadlineFence {
    context: AdmissionContext,
    permission: RuntimePermission,
}

impl AdmissionFence for DeadlineFence {
    fn local_replica(&self) -> ReplicaId {
        self.context.local_replica
    }

    fn check(&self, expected: &AdmissionContext) -> Result<Instant, AdmissionDenied> {
        if expected != &self.context {
            return Err(AdmissionDenied("wrong context"));
        }
        self.permission
            .current()
            .map(|(_, deadline)| deadline)
            .ok_or(AdmissionDenied("permission sealed"))
    }
}

fn context() -> AdmissionContext {
    AdmissionContext {
        local_replica: test_replica(1),
        genesis: Genesis {
            config: ClusterConfigFingerprint {
                version: 1,
                digest: [8; 32],
            },
            epoch: 17,
            voters: BTreeSet::from([test_replica(1)]),
        },
    }
}

fn storage() -> (KafLogStore, KafStateMachine, Arc<RwLock<KafStorageState>>) {
    crate::raft::store::new_admitted_store(Arc::new(vec![]), 3, true, 0)
}

async fn bind(state: &Arc<RwLock<KafStorageState>>) -> Arc<DeadlineFence> {
    let context = context();
    let permission = RuntimePermission::quarantined(Instant::now());
    assert!(permission.admit(
        AuthorityContext {
            boot_nonce: context.local_replica.boot_nonce,
            cluster_epoch: context.genesis.epoch,
            genesis_digest: context.genesis.digest(),
            admission_generation: 1,
        },
        Instant::now() + Duration::from_secs(5)
    ));
    let fence = Arc::new(DeadlineFence {
        context: context.clone(),
        permission,
    });
    state
        .write()
        .await
        .bind_admission(AdmissionSession::new(context, fence.clone()).unwrap())
        .unwrap();
    fence
}

fn entry(index: u64) -> EntryOf<TypeConfig> {
    EntryOf::<TypeConfig> {
        log_id: log_id::<TypeConfig>(1, test_replica(1), index),
        payload: EntryPayload::Blank,
    }
}

fn fingerprint(state: &KafStorageState) -> serde_json::Value {
    serde_json::to_value((
        KafSnapshot::from(state),
        &state.log,
        state.vote,
        state.committed,
        state.last_purged_log_id,
        &state.current_snapshot,
    ))
    .unwrap()
}

#[derive(Clone, Copy, Debug)]
enum Mutation {
    Vote,
    Commit,
    Append,
    Truncate,
    Purge,
    Apply,
    Snapshot,
}

async fn mutate(
    kind: Mutation,
    mut log: KafLogStore,
    mut machine: KafStateMachine,
    snapshot: SnapshotOf<TypeConfig, KafSnapshotData>,
) -> io::Result<()> {
    match kind {
        Mutation::Vote => log.save_vote(&Vote::new(2, test_replica(1))).await,
        Mutation::Commit => log.save_committed(Some(entry(2).log_id)).await,
        Mutation::Append => log.append([entry(2)], IOFlushed::noop()).await,
        Mutation::Truncate => log.truncate_after(None).await,
        Mutation::Purge => log.purge(entry(1).log_id).await,
        Mutation::Apply => machine.apply(stream::iter([Ok((entry(2), None))])).await,
        Mutation::Snapshot => {
            machine
                .install_snapshot(&snapshot.meta, snapshot.snapshot)
                .await
        }
    }
}

async fn queued_write(kind: Mutation) {
    let (mut log, mut machine, state) = storage();
    bind(&state).await;
    log.append([entry(1)], IOFlushed::noop()).await.unwrap();
    machine
        .apply(stream::iter([Ok((entry(1), None))]))
        .await
        .unwrap();
    state.write().await.genesis = Some(context().genesis);
    let snapshot = machine.build_snapshot().await.unwrap();
    let held = state.write().await;
    let before = fingerprint(&held);
    assert!(held.admission.as_ref().unwrap().check().is_ok());
    let mut queued = Box::pin(mutate(kind, log, machine, snapshot));
    assert!(futures::poll!(queued.as_mut()).is_pending());
    tokio::time::advance(Duration::from_secs(5)).await;
    assert!(held.admission.as_ref().unwrap().check().is_err());
    drop(held);
    let result = queued.await;
    assert_eq!(
        fingerprint(&*state.read().await),
        before,
        "{kind:?} mutated expired storage"
    );
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::PermissionDenied);
}

#[tokio::test(start_paused = true)]
async fn queued_vote_rechecks_expired_admission() {
    queued_write(Mutation::Vote).await;
}
#[tokio::test(start_paused = true)]
async fn queued_commit_rechecks_expired_admission() {
    queued_write(Mutation::Commit).await;
}
#[tokio::test(start_paused = true)]
async fn queued_append_rechecks_expired_admission() {
    queued_write(Mutation::Append).await;
}
#[tokio::test(start_paused = true)]
async fn queued_truncate_rechecks_expired_admission() {
    queued_write(Mutation::Truncate).await;
}
#[tokio::test(start_paused = true)]
async fn queued_purge_rechecks_expired_admission() {
    queued_write(Mutation::Purge).await;
}
#[tokio::test(start_paused = true)]
async fn queued_apply_rechecks_expired_admission() {
    queued_write(Mutation::Apply).await;
}
#[tokio::test(start_paused = true)]
async fn queued_snapshot_rechecks_expired_admission() {
    queued_write(Mutation::Snapshot).await;
}

#[tokio::test(start_paused = true)]
async fn apply_releases_lock_while_waiting_and_rechecks_each_entry() {
    let (_, mut machine, state) = storage();
    let mut builder = machine.clone();
    bind(&state).await;
    let (sender, receiver) = futures::channel::mpsc::unbounded();
    sender.unbounded_send(Ok((entry(1), None))).unwrap();
    let mut applying = Box::pin(machine.apply(receiver));
    assert!(futures::poll!(applying.as_mut()).is_pending());
    let before = {
        let held = state
            .try_write()
            .expect("apply held write lock while awaiting stream");
        assert_eq!(held.last_applied_log, Some(entry(1).log_id));
        fingerprint(&held)
    };
    let snapshot = builder.build_snapshot().await.unwrap();
    let captured: KafSnapshot = serde_json::from_slice(snapshot.snapshot.get_ref()).unwrap();
    assert_eq!(captured.last_applied, Some(entry(1).log_id));
    assert_eq!(snapshot.meta.last_log_id, captured.last_applied);
    tokio::time::advance(Duration::from_secs(5)).await;
    sender.unbounded_send(Ok((entry(2), None))).unwrap();
    drop(sender);
    let result = applying.await;
    assert_eq!(fingerprint(&*state.read().await), before);
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::PermissionDenied);
}

struct NoNetwork;

impl openraft::network::RaftNetworkFactory<TypeConfig> for NoNetwork {
    type Network =
        <crate::raft::network::RaftNetworkImpl as openraft::network::RaftNetworkFactory<
            TypeConfig,
        >>::Network;

    async fn new_client(&mut self, _: ReplicaId, _: &openraft::BasicNode) -> Self::Network {
        panic!("blank strict-store initialization must not contact a peer")
    }
}

#[tokio::test(start_paused = true)]
async fn blank_strict_store_starts_raft_without_admission_or_writes() {
    for enable_tick in [false, true] {
        let (log, machine, state) = storage();
        let before = fingerprint(&*state.read().await);
        let raft = crate::raft::KafRaft::new(
            test_replica(1),
            Arc::new(openraft::Config {
                enable_tick,
                ..Default::default()
            }),
            NoNetwork,
            log,
            machine,
        )
        .await
        .unwrap();
        tokio::time::advance(Duration::from_secs(10)).await;
        assert!(!raft.is_initialized().await.unwrap());
        assert!(raft.metrics().borrow_watched().running_state.is_ok());
        assert_eq!(fingerprint(&*state.read().await), before);
        assert!(state.read().await.admission.is_none());
        raft.shutdown().await.unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn missing_admission_denies_all_storage_mutations() {
    let (_, mut source, source_state) = storage();
    bind(&source_state).await;
    source
        .apply(stream::iter([Ok((entry(1), None))]))
        .await
        .unwrap();
    source_state.write().await.genesis = Some(context().genesis);
    for kind in [
        Mutation::Vote,
        Mutation::Commit,
        Mutation::Append,
        Mutation::Truncate,
        Mutation::Purge,
        Mutation::Apply,
        Mutation::Snapshot,
    ] {
        let (log, machine, state) = storage();
        let before = fingerprint(&*state.read().await);
        let result = mutate(kind, log, machine, source.build_snapshot().await.unwrap()).await;
        assert_eq!(
            result.unwrap_err().kind(),
            io::ErrorKind::PermissionDenied,
            "{kind:?}"
        );
        assert_eq!(fingerprint(&*state.read().await), before, "{kind:?}");
        assert!(state.read().await.admission.is_none());
    }
}

#[tokio::test(start_paused = true)]
async fn admission_allows_writes_before_deadline_and_snapshot_keeps_local_permission() {
    let (_, mut source, source_state) = storage();
    bind(&source_state).await;
    source
        .apply(stream::iter([Ok((entry(1), None))]))
        .await
        .unwrap();
    source_state.write().await.genesis = Some(context().genesis);
    for kind in [
        Mutation::Vote,
        Mutation::Commit,
        Mutation::Append,
        Mutation::Truncate,
        Mutation::Purge,
        Mutation::Apply,
        Mutation::Snapshot,
    ] {
        let (log, machine, state) = storage();
        let fence = bind(&state).await;
        let snapshot = source.build_snapshot().await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(snapshot.snapshot.get_ref()).unwrap();
        assert!(json.get("admission").is_none());
        assert!(json.get("admission_required").is_none());
        tokio::time::advance(Duration::from_secs(5) - Duration::from_nanos(1)).await;
        mutate(kind, log.clone(), machine, snapshot).await.unwrap();
        assert!(
            state
                .read()
                .await
                .admission
                .as_ref()
                .unwrap()
                .check()
                .is_ok()
        );
        fence.permission.seal();
        let before = fingerprint(&*state.read().await);
        let mut log = log;
        assert_eq!(
            log.save_vote(&Vote::new(9, test_replica(1)))
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::PermissionDenied
        );
        assert_eq!(fingerprint(&*state.read().await), before);
    }
}

#[tokio::test(start_paused = true)]
async fn cancelled_queued_mutation_never_applies() {
    let (mut log, _, state) = storage();
    bind(&state).await;
    let held = state.write().await;
    let before = fingerprint(&held);
    let vote = Vote::new(1, test_replica(1));
    let mut queued = Box::pin(log.save_vote(&vote));
    assert!(futures::poll!(queued.as_mut()).is_pending());
    drop(queued);
    drop(held);
    assert_eq!(fingerprint(&*state.read().await), before);
}

#[tokio::test(start_paused = true)]
async fn append_checks_each_entry_and_reports_flush_failure() {
    let (mut log, _, state) = storage();
    let fence = bind(&state).await;
    let entries = [entry(1), entry(2)].into_iter().inspect(|entry| {
        if entry.log_id.index() == 2 {
            fence.permission.seal();
        }
    });
    let (sender, receiver) = TypeConfig::oneshot();
    let result = log.append(entries, IOFlushed::signal(sender)).await;
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::PermissionDenied);
    assert_eq!(
        receiver.await.unwrap().unwrap_err().kind(),
        io::ErrorKind::PermissionDenied
    );
    let state = state.read().await;
    assert_eq!(state.log.len(), 1);
    assert_eq!(state.log[&1].log_id, entry(1).log_id);
    assert!(state.last_purged_log_id.is_none());
}
