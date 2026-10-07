//! Exact, bounded authorization fixtures for ordinary transport unit tests.

use super::authorization::*;
use crate::config::Config;
use crate::raft::admission::{AdmissionContext, AdmissionFence, Genesis};
use crate::raft::types::{TypeConfig, test_replica};
use openraft::alias::StoredMembershipOf;
use std::collections::BTreeSet;
use std::sync::Arc;
use tokio::time::{Duration, Instant};

struct FixtureFence {
    context: AdmissionContext,
    deadline: Instant,
}

impl AdmissionFence for FixtureFence {
    fn local_replica(&self) -> ReplicaId {
        self.context.local_replica
    }
    fn check(&self, expected: &AdmissionContext) -> Result<Instant, AdmissionDenied> {
        if expected != &self.context {
            return Err(AdmissionDenied("unexpected fixture context"));
        }
        Ok(self.deadline)
    }
}

struct FixtureController {
    session: AdmissionSession,
    voters: BTreeSet<ReplicaId>,
    memberships: Vec<openraft::Membership<ReplicaId, openraft::BasicNode>>,
}

impl AdmissionController for FixtureController {
    fn local_replica(&self) -> ReplicaId {
        self.session.context().local_replica
    }
    fn authorize_raft(&self, peer: ReplicaId) -> Result<RaftAuthorization, AdmissionDenied> {
        self.session.check()?;
        if !self.voters.contains(&peer) {
            return Err(AdmissionDenied("unexpected fixture boot"));
        }
        Ok(RaftAuthorization {
            session: self.session.clone(),
            peer,
            configured_physical_ids: self
                .voters
                .iter()
                .map(|replica| replica.physical_id)
                .collect(),
            voters: self.voters.clone(),
            log_replicas: self.voters.clone(),
            memberships: self.memberships.clone(),
            committed_membership: StoredMembershipOf::<TypeConfig>::new(
                None,
                self.memberships[0].clone(),
            ),
            last_applied: None,
            prepared_join: None,
            verified_bootstrap: None,
            replication: None,
        })
    }
    fn dispatch(
        &self,
        _: ReplicaId,
        _: [u8; 32],
        _: AdmissionRpc,
    ) -> futures::future::BoxFuture<'_, anyhow::Result<AdmissionRpc>> {
        Box::pin(async { anyhow::bail!("fixture has no admission protocol") })
    }
}

pub(crate) fn controller(config: &Config) -> Arc<dyn AdmissionController> {
    let voters = config
        .peers
        .iter()
        .map(|peer| test_replica(peer.id))
        .collect();
    let context = AdmissionContext {
        local_replica: test_replica(config.node_id),
        genesis: Genesis {
            config: config.cluster_config_fingerprint().unwrap(),
            epoch: 1,
            voters,
        },
    };
    let nodes: std::collections::BTreeMap<_, _> = config
        .peers
        .iter()
        .map(|peer| {
            (
                test_replica(peer.id),
                openraft::BasicNode::new(peer.raft_address.clone()),
            )
        })
        .collect();
    let memberships =
        vec![openraft::Membership::new(vec![context.genesis.voters.clone()], nodes).unwrap()];
    fixture_authority(
        context,
        Instant::now() + Duration::from_secs(60),
        memberships,
    )
}

pub(crate) fn with_context(context: AdmissionContext) -> Arc<dyn AdmissionController> {
    with_deadline(context, Instant::now() + Duration::from_secs(60))
}

pub(crate) fn with_deadline(
    context: AdmissionContext,
    deadline: Instant,
) -> Arc<dyn AdmissionController> {
    let memberships = vec![openraft::Membership::new_with_defaults(
        vec![context.genesis.voters.clone()],
        [],
    )];
    fixture_authority(context, deadline, memberships)
}

fn fixture_authority(
    context: AdmissionContext,
    deadline: Instant,
    memberships: Vec<openraft::Membership<ReplicaId, openraft::BasicNode>>,
) -> Arc<dyn AdmissionController> {
    let voters = context.genesis.voters.clone();
    let fence = Arc::new(FixtureFence {
        context: context.clone(),
        deadline,
    });
    Arc::new(FixtureController {
        session: AdmissionSession::new(context, fence).unwrap(),
        voters,
        memberships,
    })
}

pub(crate) fn vote_request() -> openraft::raft::VoteRequest<crate::raft::TypeConfig> {
    openraft::raft::VoteRequest::new(
        openraft::alias::VoteOf::<crate::raft::TypeConfig>::new(7, test_replica(1)),
        None,
    )
}

pub(crate) fn vote_response(
    granted: bool,
) -> openraft::raft::VoteResponse<crate::raft::TypeConfig> {
    openraft::raft::VoteResponse::new(
        openraft::alias::VoteOf::<crate::raft::TypeConfig>::new(7, test_replica(1)),
        None,
        granted,
    )
}

pub(crate) async fn start_transport(
    config: Arc<Config>,
    listener: crate::listener::ListenerSource,
) -> (
    crate::raft::KafRaft,
    Arc<super::RaftNetworkImpl>,
    Arc<tokio::sync::RwLock<crate::raft::KafStorageState>>,
) {
    let (log, machine, state) = crate::raft::store::new_store(Arc::new(Vec::new()), 3, true, 0);
    let network = Arc::new(
        super::RaftNetworkImpl::new(config.clone(), state.clone(), controller(&config)).unwrap(),
    );
    let raft = crate::raft::KafRaft::new(
        test_replica(config.node_id),
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
    network
        .start_with_listener(raft.clone(), listener)
        .await
        .unwrap();
    (raft, network, state)
}
