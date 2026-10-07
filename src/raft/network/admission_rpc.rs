//! Channel-bound admission requests dispatched to the supplied controller.

use super::authorization::{AdmissionController, AdmissionRpc, ReplicaId};
use crate::raft::admission::{AdmissionRequest, ReceivedGrant, SignedAdmission};
use anyhow::Context;

const REQUEST_ROLE: u8 = 10;
const RESPONSE_ROLE: u8 = 11;

pub(super) type SignedAdmissionRecord = AdmissionRpc;

pub(super) async fn dispatch(
    controller: &dyn AdmissionController,
    peer: ReplicaId,
    binding: [u8; 32],
    secret: Option<&str>,
    request: AdmissionRpc,
) -> anyhow::Result<AdmissionRpc> {
    let local = controller.local_replica();
    request.verify(secret, REQUEST_ROLE, peer, local, binding)?;
    anyhow::ensure!(
        request.payload.consumer == peer,
        "admission consumer differs from authenticated boot"
    );
    let expected = request.payload.clone();
    let response = controller.dispatch(peer, binding, request).await?;
    response.verify(secret, RESPONSE_ROLE, local, peer, binding)?;
    anyhow::ensure!(
        response.payload == expected,
        "admission response differs from request"
    );
    Ok(response)
}

impl super::RaftNetworkImpl {
    /// Sign a controller request only after authenticating the exact boot and channel.
    pub async fn admission_rpc(
        &self,
        target: ReplicaId,
        request: AdmissionRequest,
        budget: std::time::Duration,
    ) -> anyhow::Result<ReceivedGrant> {
        use super::request::{Operation, decode_payload, encode};
        use super::wire::{read_framed_bounded, write_framed};
        let exchange = async {
            let peer = self
                .config
                .get_peer(target.physical_id)
                .ok_or_else(|| anyhow::anyhow!("admission target is not configured"))?;
            let local = self.admission.local_replica();
            anyhow::ensure!(
                request.consumer == local,
                "admission request belongs to another boot"
            );
            let mut stream = crate::connection_admission::connect_from_advertised(
                &self.config.raft_listen,
                &peer.raft_address,
            )
            .await?;
            let authenticated = crate::auth::client_bound(
                &mut stream,
                crate::auth::Peer::for_replica(local, None, true),
                target.physical_id,
                self.config.cluster_secret.as_deref(),
                crate::auth::Listener::Raft,
            )
            .await?;
            anyhow::ensure!(
                authenticated.peer.replica() == Some(target),
                "admission destination boot changed"
            );
            let binding = authenticated.binding;
            let record = SignedAdmission::sign(
                self.config.cluster_secret.as_deref(),
                REQUEST_ROLE,
                local,
                target,
                binding,
                request.clone(),
            )?;
            let body = encode(Operation::Admission, &record)?;
            anyhow::ensure!(
                body.len() as u64 <= u64::from(self.config.max_frame_bytes),
                "admission request exceeds max_frame_bytes"
            );
            write_framed(&mut stream, &body).await?;
            let body = read_framed_bounded(&mut stream, self.config.max_frame_bytes).await?;
            let response: AdmissionRpc = decode_payload(&body, Operation::Admission)?;
            Ok(ReceivedGrant::authenticate(
                self.config.cluster_secret.as_deref(),
                authenticated
                    .peer
                    .replica()
                    .context("admission peer omitted boot identity")?,
                local,
                authenticated.binding,
                &request,
                response,
            )?)
        };
        tokio::time::timeout(budget, exchange)
            .await
            .map_err(|_| anyhow::anyhow!("admission exchange timed out"))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::admission::{AdmissionDenied, AdmissionMode, Genesis};
    use crate::raft::network::authorization::RaftAuthorization;
    use crate::raft::types::test_replica;
    use futures::future::BoxFuture;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    struct QuarantinedController {
        local: ReplicaId,
        calls: AtomicUsize,
    }

    impl AdmissionController for QuarantinedController {
        fn local_replica(&self) -> ReplicaId {
            self.local
        }
        fn authorize_raft(&self, _: ReplicaId) -> Result<RaftAuthorization, AdmissionDenied> {
            Err(AdmissionDenied("startup quarantine"))
        }
        fn dispatch(
            &self,
            peer: ReplicaId,
            binding: [u8; 32],
            request: AdmissionRpc,
        ) -> BoxFuture<'_, anyhow::Result<AdmissionRpc>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                Ok(SignedAdmission::sign(
                    Some("network-test-secret"),
                    RESPONSE_ROLE,
                    self.local_replica(),
                    peer,
                    binding,
                    request.payload,
                )?)
            })
        }
        fn management(
            &self,
            peer: ReplicaId,
            binding: [u8; 32],
            request: SignedAdmission<super::super::authorization::management::ManagementRequest>,
        ) -> BoxFuture<
            '_,
            anyhow::Result<
                SignedAdmission<super::super::authorization::management::ManagementResponse>,
            >,
        > {
            use super::super::authorization::management::{
                ManagementAction, ManagementResponse, ManagementResult,
            };
            self.calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                anyhow::ensure!(
                    matches!(request.payload.action, ManagementAction::Discover),
                    "fixture only permits discovery"
                );
                Ok(SignedAdmission::sign(
                    Some("network-test-secret"),
                    13,
                    self.local_replica(),
                    peer,
                    binding,
                    ManagementResponse {
                        nonce: request.payload.nonce,
                        result: ManagementResult::Discovery {
                            genesis: None,
                            voters: Default::default(),
                        },
                    },
                )?)
            })
        }
    }

    fn request(consumer: ReplicaId) -> AdmissionRequest {
        AdmissionRequest {
            consumer,
            request_nonce: [3; 32],
            mode: AdmissionMode::Cold,
            genesis: Genesis {
                config: crate::config::ClusterConfigFingerprint {
                    version: 1,
                    digest: [9; 32],
                },
                epoch: 9,
                voters: [test_replica(1), test_replica(2)].into(),
            },
        }
    }

    #[tokio::test]
    async fn admission_dispatch_requires_actual_channel_and_exact_consumer_before_controller() {
        let controller = QuarantinedController {
            local: test_replica(1),
            calls: AtomicUsize::new(0),
        };
        let peer = test_replica(2);
        for wrong in 0..4 {
            let payload = request(if wrong == 1 { test_replica(3) } else { peer });
            let record = SignedAdmission::sign(
                Some("network-test-secret"),
                REQUEST_ROLE,
                peer,
                test_replica(1),
                if wrong == 2 { [7; 32] } else { [4; 32] },
                payload,
            )
            .unwrap();
            let authenticated = if wrong == 3 {
                ReplicaId {
                    boot_nonce: [8; 32],
                    ..peer
                }
            } else {
                peer
            };
            let result = dispatch(
                &controller,
                authenticated,
                [4; 32],
                Some("network-test-secret"),
                record,
            )
            .await;
            assert_eq!(result.is_ok(), wrong == 0);
        }
        assert_eq!(controller.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn admission_grants_bind_the_actual_response_channel_before_raft_initialization() {
        use crate::raft::network::request::{Operation, decode_payload, encode};
        use crate::raft::network::wire::{read_framed_bounded, write_framed};
        for altered in 0..4 {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let mut config = (*super::super::tests::test_network(65_536, &[1, 2]).config).clone();
            config.peers[1].raft_address = listener.local_addr().unwrap().to_string();
            let (_, _, state) = crate::raft::store::new_store(Arc::new(Vec::new()), 3, true, 0);
            let authority = Arc::new(QuarantinedController {
                local: test_replica(1),
                calls: AtomicUsize::new(0),
            });
            let network =
                super::super::RaftNetworkImpl::new(Arc::new(config), state.clone(), authority)
                    .unwrap();
            let server = async {
                let (mut stream, _) = listener.accept().await.unwrap();
                let authenticated = crate::auth::server_bound(
                    &mut stream,
                    crate::auth::Peer::for_replica(test_replica(2), None, true),
                    Some("network-test-secret"),
                    crate::auth::Listener::Raft,
                )
                .await
                .unwrap();
                let body = read_framed_bounded(&mut stream, 65_536).await.unwrap();
                let request: AdmissionRpc = decode_payload(&body, Operation::Admission).unwrap();
                request
                    .verify(
                        Some("network-test-secret"),
                        REQUEST_ROLE,
                        authenticated.peer.replica().unwrap(),
                        test_replica(2),
                        authenticated.binding,
                    )
                    .unwrap();
                let mut payload = request.payload;
                if altered == 1 {
                    payload.request_nonce = [8; 32];
                }
                let sender = if altered == 2 {
                    ReplicaId {
                        boot_nonce: [8; 32],
                        ..test_replica(2)
                    }
                } else {
                    test_replica(2)
                };
                let binding = if altered == 3 {
                    [8; 32]
                } else {
                    authenticated.binding
                };
                let record = SignedAdmission::sign(
                    Some("network-test-secret"),
                    RESPONSE_ROLE,
                    sender,
                    test_replica(1),
                    binding,
                    payload,
                )
                .unwrap();
                write_framed(&mut stream, &encode(Operation::Admission, &record).unwrap())
                    .await
                    .unwrap();
            };
            let exchange = network.admission_rpc(
                test_replica(2),
                request(test_replica(1)),
                std::time::Duration::from_secs(1),
            );
            let (result, ()) = tokio::time::timeout(std::time::Duration::from_secs(2), async {
                tokio::join!(exchange, server)
            })
            .await
            .unwrap();
            assert_eq!(result.is_ok(), altered == 0);
            let state = state.read().await;
            assert!(state.vote.is_none());
            assert!(state.log.is_empty());
        }
    }

    #[tokio::test]
    async fn quarantined_listener_serves_bound_admission_management_and_status_but_never_raft() {
        use crate::raft::network::request::{Operation, decode_payload, encode};
        use crate::raft::network::wire::{
            read_framed_bounded, write_framed, write_replica_handshake,
        };
        use crate::raft::probe::{ClusterStatusRequest, ClusterStatusResponse};
        use std::time::Duration;
        use tokio::io::AsyncReadExt;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let mut client_config =
            (*super::super::tests::test_network(65_536, &[1, 2]).config).clone();
        client_config.peers[1].raft_address = address.to_string();
        let mut server_config = client_config.clone();
        server_config.node_id = 2;
        server_config.raft_listen = address.to_string();
        let (log, machine, state) = crate::raft::store::new_store(Arc::new(Vec::new()), 3, true, 0);
        let authority = Arc::new(QuarantinedController {
            local: test_replica(2),
            calls: AtomicUsize::new(0),
        });
        let server = super::super::RaftNetworkImpl::new(
            Arc::new(server_config),
            state.clone(),
            authority.clone(),
        )
        .unwrap();
        let raft = crate::raft::KafRaft::new(
            test_replica(2),
            Arc::new(openraft::Config {
                enable_tick: false,
                ..Default::default()
            }),
            server.clone(),
            log,
            machine,
        )
        .await
        .unwrap();
        let (failures, _failure_rx) = tokio::sync::mpsc::unbounded_channel();
        let task = super::super::server::spawn_raft_accept_task(
            listener,
            super::super::server::RaftAcceptContext {
                admission: authority.clone(),
                config: server.config.clone(),
                raft: raft.clone(),
                state_ref: state.clone(),
                config_fingerprint: server.config_fingerprint,
                shutdown: server.shutdown.clone(),
            },
            failures,
        );
        server.tasks.lock().await.push(task);
        let (_, _, client_state) = crate::raft::store::new_store(Arc::new(Vec::new()), 3, true, 0);
        let client = super::super::RaftNetworkImpl::new(
            Arc::new(client_config),
            client_state,
            Arc::new(QuarantinedController {
                local: test_replica(1),
                calls: AtomicUsize::new(0),
            }),
        )
        .unwrap();
        let result = tokio::time::timeout(Duration::from_secs(2), async {
            client
                .admission_rpc(
                    test_replica(2),
                    request(test_replica(1)),
                    Duration::from_secs(1),
                )
                .await
                .unwrap();
            assert_eq!(authority.calls.load(Ordering::SeqCst), 1);
            use super::super::authorization::management::{ManagementAction, ManagementResult};
            let response = client
                .management_rpc(
                    test_replica(2),
                    ManagementAction::Discover,
                    Duration::from_secs(1),
                )
                .await
                .unwrap();
            assert!(matches!(
                response.result,
                ManagementResult::Discovery { genesis: None, .. }
            ));
            assert_eq!(authority.calls.load(Ordering::SeqCst), 2);
            let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
            assert_eq!(
                write_replica_handshake(
                    &mut stream,
                    test_replica(1),
                    2,
                    Some("network-test-secret"),
                    None,
                    true
                )
                .await
                .unwrap(),
                test_replica(2)
            );
            let status = ClusterStatusRequest {
                probe_from: 1,
                config_fingerprint: Some(client.config_fingerprint),
                supports_cancellation_safe_rpc_v1: true,
            };
            write_framed(&mut stream, &encode(Operation::Status, &status).unwrap())
                .await
                .unwrap();
            let body = read_framed_bounded(&mut stream, 65_536).await.unwrap();
            assert!(
                !decode_payload::<ClusterStatusResponse>(&body, Operation::Status)
                    .unwrap()
                    .initialized
            );
            write_framed(
                &mut stream,
                &encode(Operation::Vote, &super::super::testing::vote_request()).unwrap(),
            )
            .await
            .unwrap();
            assert_eq!(stream.read(&mut [0]).await.unwrap(), 0);
            assert!(!raft.is_initialized().await.unwrap());
            let state = state.read().await;
            assert!(state.vote.is_none());
            assert!(state.log.is_empty());
        })
        .await;
        server.shutdown().await.unwrap();
        raft.shutdown().await.unwrap();
        result.expect("quarantined transport did not finish its bounded exchanges");
    }
}
