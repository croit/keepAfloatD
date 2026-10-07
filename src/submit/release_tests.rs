use super::*;
use crate::raft::admission::{AdmissionContext, ReplicaId};
use crate::raft::network::testing;
use crate::raft::types::test_replica;
use tokio::time::Instant;

async fn authority(deadline: Instant) -> proof::ReleaseAuthority {
    let cfg = tests::cfg_with(Some("release-unit-secret"));
    let context = AdmissionContext {
        local_replica: test_replica(1),
        genesis: crate::raft::admission::Genesis {
            config: cfg.cluster_config_fingerprint().unwrap(),
            epoch: 1,
            voters: [test_replica(1), test_replica(2)].into(),
        },
    };
    let controller = testing::with_deadline(context, deadline);
    let authorization = controller.authorize_raft(test_replica(1)).unwrap();
    let (_, _, state) = crate::raft::store::new_store(Arc::new(vec![]), 3, true, 0);
    let session = authorization.session;
    {
        let mut state = state.write().await;
        state.genesis = Some(session.context().genesis.clone());
        state.last_membership = authorization.committed_membership;
        state.bind_admission(session.clone()).unwrap();
    }
    proof::ReleaseAuthority { session, state }
}

#[tokio::test(start_paused = true)]
async fn release_authority_expires_at_the_original_session_deadline() {
    let deadline = Instant::now() + Duration::from_secs(1);
    let authority = authority(deadline).await;
    authority.check(test_replica(2)).await.unwrap();
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(authority.check(test_replica(2)).await.is_err());
}

#[tokio::test(start_paused = true)]
async fn release_authority_lock_wait_cannot_outlast_admission() {
    let deadline = Instant::now() + Duration::from_secs(1);
    let authority = authority(deadline).await;
    let lock = authority.state.write().await;
    let check = authority.check(test_replica(2));
    tokio::pin!(check);
    assert!(futures::poll!(&mut check).is_pending());
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(check.await.is_err());
    drop(lock);
}

#[tokio::test]
async fn release_authority_rejects_another_boot_of_the_same_physical_peer() {
    let authority = authority(Instant::now() + Duration::from_secs(1)).await;
    authority.check(test_replica(2)).await.unwrap();
    assert!(
        authority
            .check(ReplicaId {
                physical_id: 2,
                boot_nonce: [99; 32],
            })
            .await
            .is_err()
    );
}

#[tokio::test]
async fn release_authority_rejects_changed_committed_genesis() {
    let authority = authority(Instant::now() + Duration::from_secs(1)).await;
    authority
        .state
        .write()
        .await
        .genesis
        .as_mut()
        .unwrap()
        .epoch += 1;
    assert!(authority.check(test_replica(2)).await.is_err());
}

#[tokio::test]
async fn release_authority_rejects_removed_local_voter() {
    let authority = authority(Instant::now() + Duration::from_secs(1)).await;
    authority.state.write().await.last_membership = openraft::StoredMembership::new(
        None,
        openraft::Membership::new_with_defaults(vec![[test_replica(2)].into()], []),
    );
    assert!(authority.check(test_replica(2)).await.is_err());
}

#[tokio::test]
async fn release_authority_rejects_missing_store_binding() {
    let authority = authority(Instant::now() + Duration::from_secs(1)).await;
    authority.state.write().await.admission = None;
    assert!(authority.check(test_replica(2)).await.is_err());
}
