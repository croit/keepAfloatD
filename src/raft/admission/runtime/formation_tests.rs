//! Discovery and physical-majority contracts of the production admission runtime.

use super::*;
use crate::auth::{Listener, Peer};
use crate::config::PeerConfig;
use crate::raft::admission::{AdmissionRequest, ReceivedGrant};
use crate::raft::probe::{ClusterStatusRequest, ClusterStatusResponse};
use crate::raft::store::new_store;
use crate::raft::types::test_replica;
use serde::{Serialize, de::DeserializeOwned};
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};

#[derive(Clone, Copy)]
enum Reply {
    Blank,
    Existing,
    LeaderOnly,
    HistorylessExisting,
    Foreign,
    Legacy,
    LegacyExisting,
}

struct Fixture {
    runtime: Arc<RuntimeDriver>,
    network: RaftNetworkImpl,
    peers: Vec<TcpListener>,
}

impl Fixture {
    async fn new(count: u64) -> Self {
        let mut cfg = (*super::tests::config()).clone();
        let mut peers = Vec::new();
        for id in 2..=count {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            cfg.peers.push(PeerConfig {
                id,
                raft_address: listener.local_addr().unwrap().to_string(),
                client_submit_address: format!("127.0.0.1:{}", 2000 + id),
            });
            peers.push(listener);
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
        let network = RaftNetworkImpl::new(cfg, state, runtime.clone()).unwrap();
        Self {
            runtime,
            network,
            peers,
        }
    }

    async fn round(&self, replies: &[Reply]) -> discovery::Discovery {
        assert_eq!(replies.len(), self.peers.len());
        let server = futures::future::join_all(self.peers.iter().zip(replies).enumerate().map(
            |(index, (listener, reply))| {
                self.reply(listener, test_replica(index as u64 + 2), *reply)
            },
        ));
        let client = self
            .runtime
            .discover(&self.network, Duration::from_millis(100));
        let (_, result) = tokio::time::timeout(Duration::from_secs(1), async {
            tokio::join!(server, client)
        })
        .await
        .expect("bounded discovery round must complete");
        result.unwrap()
    }

    async fn reply(&self, listener: &TcpListener, peer: ReplicaId, reply: Reply) {
        let cfg = &self.runtime.cfg;
        let local = self.runtime.local_replica();
        let fingerprint = cfg.cluster_config_fingerprint().unwrap();
        let (mut stream, _) = listener.accept().await.unwrap();
        crate::auth::server_bound(
            &mut stream,
            Peer::for_replica(peer, None, true),
            cfg.cluster_secret.as_deref(),
            Listener::Raft,
        )
        .await
        .unwrap();
        let request: ClusterStatusRequest = read(&mut stream, "status").await;
        assert_eq!(request.probe_from, local.physical_id);
        assert_eq!(request.config_fingerprint, Some(fingerprint));
        let mut identity = fingerprint;
        if matches!(reply, Reply::Foreign) {
            identity.digest[0] ^= 1;
        }
        write(
            &mut stream,
            "status",
            ClusterStatusResponse {
                initialized: matches!(
                    reply,
                    Reply::Existing | Reply::HistorylessExisting | Reply::LegacyExisting
                ),
                current_leader: matches!(reply, Reply::Existing | Reply::LeaderOnly)
                    .then_some(peer),
                config_fingerprint: (!matches!(reply, Reply::Legacy | Reply::LegacyExisting))
                    .then_some(identity),
                ..Default::default()
            },
        )
        .await;
        if matches!(
            reply,
            Reply::Foreign | Reply::Legacy | Reply::LegacyExisting
        ) {
            return;
        }
        let (mut stream, _) = listener.accept().await.unwrap();
        let authenticated = crate::auth::server_bound(
            &mut stream,
            Peer::for_replica(peer, None, true),
            cfg.cluster_secret.as_deref(),
            Listener::Raft,
        )
        .await
        .unwrap();
        assert_eq!(authenticated.peer.replica(), Some(local));
        let request: SignedAdmission<ManagementRequest> =
            read(&mut stream, "admission_control").await;
        request
            .verify(
                cfg.cluster_secret.as_deref(),
                REQUEST_ROLE,
                local,
                peer,
                authenticated.binding,
            )
            .unwrap();
        assert_eq!(request.payload.action, ManagementAction::Discover);
        let genesis = matches!(reply, Reply::Existing | Reply::LeaderOnly).then(|| Genesis {
            config: fingerprint,
            epoch: 17,
            voters: cfg.peers.iter().map(|node| test_replica(node.id)).collect(),
        });
        let response = SignedAdmission::sign(
            cfg.cluster_secret.as_deref(),
            RESPONSE_ROLE,
            peer,
            local,
            authenticated.binding,
            ManagementResponse {
                nonce: request.payload.nonce,
                result: ManagementResult::Discovery {
                    voters: genesis
                        .as_ref()
                        .map_or_else(BTreeSet::new, |g| g.voters.clone()),
                    genesis,
                },
            },
        )
        .unwrap();
        write(&mut stream, "admission_control", response).await;
    }

    async fn assert_unadmitted(&self) {
        assert!(self.runtime.current().is_none());
        assert!(self.runtime.authorize_raft(self.runtime.local).is_err());
        assert!(!self.runtime.vip_activation_ready().await);
        let state = self.runtime.state.read().await;
        assert!(state.admission.is_none());
        assert!(state.genesis.is_none());
        assert!(state.last_applied_log.is_none());
    }

    async fn stop(self) {
        self.network.shutdown().await.unwrap();
        self.runtime.shutdown();
    }
}

async fn read<T: DeserializeOwned>(stream: &mut TcpStream, tag: &str) -> T {
    let (bytes, ()) = crate::frame::read(stream, 64 * 1024, |_| Ok(()))
        .await
        .unwrap();
    let mut envelope: BTreeMap<String, T> = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(envelope.len(), 1);
    envelope.remove(tag).expect("expected protocol operation")
}

async fn write<T: Serialize>(stream: &mut TcpStream, tag: &str, payload: T) {
    let bytes = serde_json::to_vec(&BTreeMap::from([(tag, payload)])).unwrap();
    stream
        .write_all(&u32::try_from(bytes.len()).unwrap().to_be_bytes())
        .await
        .unwrap();
    stream.write_all(&bytes).await.unwrap();
}

#[tokio::test]
async fn discovery_retries_when_history_disappears_without_granting_authority() {
    let fixture = Fixture::new(3).await;
    let existing = fixture.round(&[Reply::Existing, Reply::Blank]).await;
    assert_eq!(existing.histories.len(), 1);
    assert_eq!(existing.boots.len(), 3);
    fixture.assert_unadmitted().await;
    let blank = fixture.round(&[Reply::Blank, Reply::Blank]).await;
    assert!(blank.histories.is_empty());
    assert_eq!(blank.boots, existing.boots);
    assert!(discovery::cohort_genesis(&fixture.runtime.cfg, blank.boots).is_ok());
    fixture.assert_unadmitted().await;
    fixture.stop().await;
}

#[tokio::test]
async fn repeated_existing_history_does_not_initialize_or_admit() {
    let fixture = Fixture::new(3).await;
    for _ in 0..2 {
        let found = fixture.round(&[Reply::Existing, Reply::Blank]).await;
        assert_eq!(found.histories.len(), 1);
        fixture.assert_unadmitted().await;
    }
    fixture.stop().await;
}

#[tokio::test]
async fn signed_history_not_status_flags_controls_discovery_history() {
    let fixture = Fixture::new(3).await;
    let found = fixture
        .round(&[Reply::LeaderOnly, Reply::HistorylessExisting])
        .await;
    assert_eq!(found.boots.len(), 3);
    assert_eq!(
        found.histories.keys().copied().collect::<Vec<_>>(),
        vec![test_replica(2)]
    );
    fixture.assert_unadmitted().await;
    let legacy = fixture.round(&[Reply::LegacyExisting, Reply::Blank]).await;
    assert_eq!(
        legacy.boots,
        [fixture.runtime.local, test_replica(3)].into()
    );
    assert!(legacy.histories.is_empty());
    fixture.assert_unadmitted().await;
    fixture.stop().await;
}

#[tokio::test]
async fn missing_peers_do_not_remain_in_the_next_discovery_cohort() {
    let mut fixture = Fixture::new(3).await;
    let first = fixture.round(&[Reply::Existing, Reply::Blank]).await;
    assert_eq!(first.histories.len(), 1);
    drop(fixture.peers.pop());
    drop(fixture.peers.pop());
    let next = fixture
        .runtime
        .discover(&fixture.network, Duration::from_millis(100))
        .await
        .unwrap();
    assert_eq!(next.boots, [fixture.runtime.local].into());
    assert!(next.histories.is_empty());
    assert!(discovery::cohort_genesis(&fixture.runtime.cfg, next.boots).is_err());
    fixture.assert_unadmitted().await;
    fixture.stop().await;
}

#[tokio::test]
async fn foreign_and_legacy_blank_peers_cannot_supply_a_cohort() {
    let fixture = Fixture::new(3).await;
    fixture.round(&[Reply::Existing, Reply::Blank]).await;
    let next = fixture.round(&[Reply::Foreign, Reply::Legacy]).await;
    assert_eq!(next.boots, [fixture.runtime.local].into());
    assert!(next.histories.is_empty());
    assert!(discovery::cohort_genesis(&fixture.runtime.cfg, next.boots).is_err());
    fixture.assert_unadmitted().await;
    fixture.stop().await;
}

#[tokio::test]
async fn foreign_majority_cannot_combine_with_matching_history_to_admit() {
    let fixture = Fixture::new(5).await;
    for matching in 0..4 {
        let mut replies = [Reply::Foreign; 4];
        replies[matching] = Reply::Existing;
        let found = fixture.round(&replies).await;
        assert_eq!(found.boots.len(), 2);
        assert_eq!(found.histories.len(), 1);
        assert!(discovery::cohort_genesis(&fixture.runtime.cfg, found.boots).is_err());
        fixture.assert_unadmitted().await;
    }
    fixture.stop().await;
}

#[tokio::test]
async fn cohort_requires_strict_physical_majority_for_every_roster_size() {
    for total in 1..=7 {
        let fixture = Fixture::new(total).await;
        for reachable in 0..=total {
            let voters = (1..=reachable)
                .map(|id| {
                    if id == 1 {
                        fixture.runtime.local
                    } else {
                        test_replica(id)
                    }
                })
                .collect();
            let result = discovery::cohort_genesis(&fixture.runtime.cfg, voters);
            assert_eq!(
                result.is_ok(),
                reachable > total / 2,
                "total={total}, reachable={reachable}"
            );
        }
        fixture.stop().await;
    }
}

#[tokio::test(start_paused = true)]
async fn cold_install_requires_consent_from_every_proposed_boot() {
    let fixture = Fixture::new(3).await;
    let runtime = &fixture.runtime;
    let voters = [runtime.local, test_replica(2), test_replica(3)].into();
    let genesis = discovery::cohort_genesis(&runtime.cfg, voters).unwrap();
    assert!(
        runtime
            .core
            .lock()
            .unwrap()
            .begin(genesis.clone(), AdmissionMode::Cold)
            .is_err()
    );
    tokio::time::advance(runtime.timing.restart_quarantine()).await;
    let mut round = runtime
        .core
        .lock()
        .unwrap()
        .begin(genesis, AdmissionMode::Cold)
        .unwrap();
    let grants: Vec<_> = [runtime.local, test_replica(2), test_replica(3)]
        .into_iter()
        .map(|issuer| {
            let binding = [issuer.physical_id as u8; 32];
            let response: SignedAdmission<AdmissionRequest> = SignedAdmission::sign(
                runtime.cfg.cluster_secret.as_deref(),
                11,
                issuer,
                runtime.local,
                binding,
                round.request().clone(),
            )
            .unwrap();
            ReceivedGrant::authenticate(
                runtime.cfg.cluster_secret.as_deref(),
                issuer,
                runtime.local,
                binding,
                round.request(),
                response,
            )
            .unwrap()
        })
        .collect();
    assert!(
        runtime
            .install(&mut round, &grants[..2], None)
            .await
            .is_err()
    );
    fixture.assert_unadmitted().await;
    runtime.install(&mut round, &grants, None).await.unwrap();
    assert!(runtime.current().is_some());
    assert!(!runtime.vip_activation_ready().await);
    fixture.stop().await;
}
