//! Authenticated management data, independent of permission to use Raft.

use super::{AdmissionController, ReplicaId};
use crate::raft::admission::{Genesis, HealthProgress, JoinPlan, PreparedJoin, SignedAdmission};
use crate::raft::network::request::{Operation, decode_payload, encode};
use crate::raft::network::wire::{read_framed_bounded, write_framed};
use crate::raft::types::TypeConfig;
use openraft::alias::LogIdOf;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::time::Duration;

pub const REQUEST_ROLE: u8 = 12;
pub const RESPONSE_ROLE: u8 = 13;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagementRequest {
    pub nonce: [u8; 32],
    pub action: ManagementAction,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum ManagementAction {
    Discover,
    PrepareJoin {
        genesis: Genesis,
        operation_nonce: [u8; 32],
    },
    LearnerReady {
        genesis: Genesis,
        operation_nonce: [u8; 32],
        prepared: LogIdOf<TypeConfig>,
    },
    CancelJoin {
        plan: JoinPlan,
        prepared: LogIdOf<TypeConfig>,
    },
    Progress(HealthProgress),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagementResponse {
    pub nonce: [u8; 32],
    pub result: ManagementResult,
}

/// These results carry data, never admission grants or local permission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum ManagementResult {
    Discovery {
        genesis: Option<Genesis>,
        voters: BTreeSet<ReplicaId>,
    },
    Prepared(Box<PreparedJoin>),
    Applied(LogIdOf<TypeConfig>),
    Redirect(ReplicaId),
}

fn verify_request(
    secret: Option<&str>,
    peer: ReplicaId,
    local: ReplicaId,
    binding: [u8; 32],
    request: &SignedAdmission<ManagementRequest>,
) -> anyhow::Result<()> {
    request.verify(secret, REQUEST_ROLE, peer, local, binding)?;
    if let ManagementAction::Progress(progress) = &request.payload.action {
        progress.validate()?;
        anyhow::ensure!(
            progress.replica == peer,
            "management progress differs from authenticated boot"
        );
    }
    Ok(())
}

fn verify_response(
    secret: Option<&str>,
    sender: ReplicaId,
    recipient: ReplicaId,
    binding: [u8; 32],
    nonce: [u8; 32],
    response: &SignedAdmission<ManagementResponse>,
) -> anyhow::Result<()> {
    response.verify(secret, RESPONSE_ROLE, sender, recipient, binding)?;
    anyhow::ensure!(
        response.payload.nonce == nonce,
        "management response nonce mismatch"
    );
    Ok(())
}

pub(in crate::raft::network) async fn dispatch(
    controller: &dyn AdmissionController,
    peer: ReplicaId,
    binding: [u8; 32],
    secret: Option<&str>,
    request: SignedAdmission<ManagementRequest>,
    budget: Duration,
) -> anyhow::Result<SignedAdmission<ManagementResponse>> {
    anyhow::ensure!(!budget.is_zero(), "management dispatch timed out");
    tokio::time::timeout(budget, async {
        let local = controller.local_replica();
        verify_request(secret, peer, local, binding, &request)?;
        let nonce = request.payload.nonce;
        let response = controller.management(peer, binding, request).await?;
        verify_response(secret, local, peer, binding, nonce, &response)?;
        Ok(response)
    })
    .await
    .map_err(|_| anyhow::anyhow!("management dispatch timed out"))?
}

impl crate::raft::network::RaftNetworkImpl {
    /// Return management data only after checking the actual peer, channel and challenge.
    pub async fn management_rpc(
        &self,
        target: ReplicaId,
        action: ManagementAction,
        budget: Duration,
    ) -> anyhow::Result<ManagementResponse> {
        anyhow::ensure!(!budget.is_zero(), "management exchange timed out");
        let exchange = async {
            let peer = self
                .config
                .get_peer(target.physical_id)
                .ok_or_else(|| anyhow::anyhow!("management target is not configured"))?;
            let local = self.admission.local_replica();
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
                "management destination boot changed"
            );
            let mut nonce = [0; 32];
            getrandom::fill(&mut nonce)
                .map_err(|_| anyhow::anyhow!("management challenge entropy unavailable"))?;
            let request = SignedAdmission::sign(
                self.config.cluster_secret.as_deref(),
                REQUEST_ROLE,
                local,
                target,
                authenticated.binding,
                ManagementRequest { nonce, action },
            )?;
            let body = encode(Operation::AdmissionControl, &request)?;
            anyhow::ensure!(
                body.len() as u64 <= u64::from(self.config.max_frame_bytes),
                "management request exceeds max_frame_bytes"
            );
            write_framed(&mut stream, &body).await?;
            let body = read_framed_bounded(&mut stream, self.config.max_frame_bytes).await?;
            let response: SignedAdmission<ManagementResponse> =
                decode_payload(&body, Operation::AdmissionControl)?;
            verify_response(
                self.config.cluster_secret.as_deref(),
                target,
                local,
                authenticated.binding,
                nonce,
                &response,
            )?;
            Ok(response.payload)
        };
        tokio::time::timeout(budget, exchange)
            .await
            .map_err(|_| anyhow::anyhow!("management exchange timed out"))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::network::authorization::{AdmissionDenied, AdmissionRpc, RaftAuthorization};
    use crate::raft::network::request::{Operation, decode_payload, encode};
    use crate::raft::network::wire::{read_framed_bounded, write_framed};
    use crate::raft::types::test_replica;
    use futures::future::BoxFuture;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tokio::net::TcpListener;

    const SECRET: Option<&str> = Some("network-test-secret");
    const BUDGET: Duration = Duration::from_secs(1);

    struct Controller {
        calls: AtomicUsize,
        response_fault: u8,
    }

    impl AdmissionController for Controller {
        fn local_replica(&self) -> ReplicaId {
            test_replica(2)
        }
        fn authorize_raft(&self, _: ReplicaId) -> Result<RaftAuthorization, AdmissionDenied> {
            Err(AdmissionDenied("startup quarantine"))
        }
        fn dispatch(
            &self,
            _: ReplicaId,
            _: [u8; 32],
            _: AdmissionRpc,
        ) -> BoxFuture<'_, anyhow::Result<AdmissionRpc>> {
            Box::pin(async { anyhow::bail!("no admission grants") })
        }
        fn management(
            &self,
            peer: ReplicaId,
            binding: [u8; 32],
            request: SignedAdmission<ManagementRequest>,
        ) -> BoxFuture<'_, anyhow::Result<SignedAdmission<ManagementResponse>>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                if self.response_fault == 8 {
                    return futures::future::pending().await;
                }
                Ok(response(
                    peer,
                    binding,
                    request.payload.nonce,
                    self.response_fault,
                ))
            })
        }
    }

    fn response(
        peer: ReplicaId,
        binding: [u8; 32],
        nonce: [u8; 32],
        fault: u8,
    ) -> SignedAdmission<ManagementResponse> {
        let mut record = SignedAdmission::sign(
            SECRET,
            if fault == 4 {
                REQUEST_ROLE
            } else {
                RESPONSE_ROLE
            },
            if fault == 2 {
                ReplicaId {
                    boot_nonce: [8; 32],
                    ..test_replica(2)
                }
            } else {
                test_replica(2)
            },
            if fault == 5 { test_replica(3) } else { peer },
            if fault == 3 { [8; 32] } else { binding },
            ManagementResponse {
                nonce: if fault == 1 { [8; 32] } else { nonce },
                result: ManagementResult::Discovery {
                    genesis: None,
                    voters: BTreeSet::new(),
                },
            },
        )
        .unwrap();
        if fault == 6 {
            record.tag[0] ^= 1;
        }
        record
    }

    fn request(action: ManagementAction) -> SignedAdmission<ManagementRequest> {
        SignedAdmission::sign(
            SECRET,
            REQUEST_ROLE,
            test_replica(1),
            test_replica(2),
            [4; 32],
            ManagementRequest {
                nonce: [3; 32],
                action,
            },
        )
        .unwrap()
    }

    #[tokio::test]
    async fn management_validates_before_invoking_controller() {
        let controller = Controller {
            calls: AtomicUsize::new(0),
            response_fault: 0,
        };
        for fault in 0..6 {
            let mut record = request(ManagementAction::Discover);
            match fault {
                1 => record.sender.boot_nonce = [8; 32],
                2 => record.recipient.boot_nonce = [8; 32],
                3 => record.binding = [8; 32],
                4 => record.payload.nonce = [8; 32],
                5 => {
                    record = SignedAdmission::sign(
                        SECRET,
                        RESPONSE_ROLE,
                        record.sender,
                        record.recipient,
                        record.binding,
                        record.payload,
                    )
                    .unwrap()
                }
                _ => {}
            }
            let result = dispatch(
                &controller,
                test_replica(1),
                [4; 32],
                SECRET,
                record,
                BUDGET,
            )
            .await;
            assert_eq!(result.is_ok(), fault == 0, "fault {fault}");
        }
        assert_eq!(controller.calls.load(Ordering::SeqCst), 1);
        assert!(controller.authorize_raft(test_replica(1)).is_err());
    }

    #[tokio::test]
    async fn management_rejects_unbound_controller_responses() {
        for fault in 1..7 {
            let controller = Controller {
                calls: AtomicUsize::new(0),
                response_fault: fault,
            };
            assert!(
                dispatch(
                    &controller,
                    test_replica(1),
                    [4; 32],
                    SECRET,
                    request(ManagementAction::Discover),
                    BUDGET
                )
                .await
                .is_err(),
                "fault {fault}"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn management_dispatch_honors_its_budget() {
        let controller = Controller {
            calls: AtomicUsize::new(0),
            response_fault: 8,
        };
        let start = tokio::time::Instant::now();
        let error = dispatch(
            &controller,
            test_replica(1),
            [4; 32],
            SECRET,
            request(ManagementAction::Discover),
            Duration::from_millis(7),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("timed out"));
        assert_eq!(start.elapsed(), Duration::from_millis(7));
    }

    #[tokio::test]
    async fn management_default_controller_fails_closed() {
        let network = crate::raft::network::tests::test_network(65_536, &[1, 2]);
        assert!(
            network
                .admission
                .management(
                    test_replica(2),
                    [4; 32],
                    request(ManagementAction::Discover)
                )
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn management_progress_requires_the_authenticated_sender() {
        let controller = Controller {
            calls: AtomicUsize::new(0),
            response_fault: 0,
        };
        for fault in 0..3 {
            let progress = HealthProgress {
                node_id: if fault == 1 { 9 } else { 1 },
                replica: if fault == 0 {
                    test_replica(3)
                } else {
                    test_replica(1)
                },
                epoch: if fault == 2 { 9 } else { 1 },
                healthy: Some(true),
                request_nonce: [6; 32],
                genesis: Genesis {
                    config: crate::config::ClusterConfigFingerprint {
                        version: 1,
                        digest: [9; 32],
                    },
                    epoch: 1,
                    voters: [test_replica(1), test_replica(2)].into(),
                },
            };
            assert!(
                dispatch(
                    &controller,
                    test_replica(1),
                    [4; 32],
                    SECRET,
                    request(ManagementAction::Progress(progress)),
                    BUDGET
                )
                .await
                .is_err()
            );
        }
        assert_eq!(controller.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn management_zero_budget_does_not_invoke_controller() {
        let controller = Controller {
            calls: AtomicUsize::new(0),
            response_fault: 0,
        };
        assert!(
            dispatch(
                &controller,
                test_replica(1),
                [4; 32],
                SECRET,
                request(ManagementAction::Discover),
                Duration::ZERO
            )
            .await
            .is_err()
        );
        assert_eq!(controller.calls.load(Ordering::SeqCst), 0);
    }

    async fn client(listener: &TcpListener, cap: u32) -> crate::raft::network::RaftNetworkImpl {
        let network = crate::raft::network::tests::test_network(cap, &[1, 2]);
        let mut config = (*network.config).clone();
        config.peers[1].raft_address = listener.local_addr().unwrap().to_string();
        crate::raft::network::RaftNetworkImpl::new(
            Arc::new(config),
            network.state_ref,
            network.admission,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn management_rpc_returns_only_checked_responses() {
        for fault in 0..9 {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let network = client(&listener, 65_536).await;
            let server = async {
                let (mut stream, _) = listener.accept().await.unwrap();
                let authenticated = crate::auth::server_bound(
                    &mut stream,
                    crate::auth::Peer::for_replica(test_replica(2), None, true),
                    SECRET,
                    crate::auth::Listener::Raft,
                )
                .await
                .unwrap();
                let body = read_framed_bounded(&mut stream, 65_536).await.unwrap();
                let request: SignedAdmission<ManagementRequest> =
                    decode_payload(&body, Operation::AdmissionControl).unwrap();
                request
                    .verify(
                        SECRET,
                        REQUEST_ROLE,
                        authenticated.peer.replica().unwrap(),
                        test_replica(2),
                        authenticated.binding,
                    )
                    .unwrap();
                if fault == 8 {
                    use tokio::io::AsyncWriteExt;
                    stream.write_all(&65_537_u32.to_be_bytes()).await.unwrap();
                    return;
                }
                let record = response(
                    test_replica(1),
                    authenticated.binding,
                    request.payload.nonce,
                    fault,
                );
                let operation = if fault == 7 {
                    Operation::Admission
                } else {
                    Operation::AdmissionControl
                };
                write_framed(&mut stream, &encode(operation, &record).unwrap())
                    .await
                    .unwrap();
            };
            let (result, ()) = tokio::time::timeout(BUDGET * 2, async {
                tokio::join!(
                    network.management_rpc(test_replica(2), ManagementAction::Discover, BUDGET),
                    server
                )
            })
            .await
            .unwrap();
            assert_eq!(result.is_ok(), fault == 0, "fault {fault}: {result:?}");
            let state = network.state_ref.read().await;
            assert!(state.vote.is_none());
            assert!(state.log.is_empty());
        }
    }

    #[tokio::test]
    async fn management_rpc_rejects_changed_boot_before_sending_a_frame() {
        use tokio::io::AsyncReadExt;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let network = client(&listener, 65_536).await;
        let server = async {
            let (mut stream, _) = listener.accept().await.unwrap();
            crate::auth::server_bound(
                &mut stream,
                crate::auth::Peer::for_replica(
                    ReplicaId {
                        boot_nonce: [8; 32],
                        ..test_replica(2)
                    },
                    None,
                    true,
                ),
                SECRET,
                crate::auth::Listener::Raft,
            )
            .await
            .unwrap();
            assert_eq!(stream.read(&mut [0]).await.unwrap(), 0);
        };
        let (result, ()) = tokio::time::timeout(BUDGET * 2, async {
            tokio::join!(
                network.management_rpc(test_replica(2), ManagementAction::Discover, BUDGET),
                server
            )
        })
        .await
        .unwrap();
        assert!(result.unwrap_err().to_string().contains("boot changed"));
    }

    #[tokio::test]
    async fn management_rpc_enforces_outbound_frame_cap() {
        use tokio::io::AsyncReadExt;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let network = client(&listener, 1).await;
        let server = async {
            let (mut stream, _) = listener.accept().await.unwrap();
            crate::auth::server_bound(
                &mut stream,
                crate::auth::Peer::for_replica(test_replica(2), None, true),
                SECRET,
                crate::auth::Listener::Raft,
            )
            .await
            .unwrap();
            assert_eq!(stream.read(&mut [0]).await.unwrap(), 0);
        };
        let (result, ()) = tokio::time::timeout(BUDGET * 2, async {
            tokio::join!(
                network.management_rpc(test_replica(2), ManagementAction::Discover, BUDGET),
                server
            )
        })
        .await
        .unwrap();
        assert!(result.unwrap_err().to_string().contains("max_frame_bytes"));
    }

    #[tokio::test]
    async fn management_rpc_budget_covers_handshake_and_response_waits() {
        use tokio::io::AsyncReadExt;
        for authenticated in [false, true] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let network = client(&listener, 65_536).await;
            let server = async {
                let (mut stream, _) = listener.accept().await.unwrap();
                if authenticated {
                    crate::auth::server_bound(
                        &mut stream,
                        crate::auth::Peer::for_replica(test_replica(2), None, true),
                        SECRET,
                        crate::auth::Listener::Raft,
                    )
                    .await
                    .unwrap();
                    read_framed_bounded(&mut stream, 65_536).await.unwrap();
                }
                stream.read_to_end(&mut Vec::new()).await.unwrap();
            };
            let (result, ()) = tokio::time::timeout(BUDGET * 2, async {
                tokio::join!(
                    network.management_rpc(
                        test_replica(2),
                        ManagementAction::Discover,
                        Duration::from_millis(100)
                    ),
                    server
                )
            })
            .await
            .unwrap();
            assert!(result.unwrap_err().to_string().contains("timed out"));
        }
    }

    #[tokio::test]
    async fn management_rpc_rejects_unconfigured_targets_and_zero_budget() {
        let network = crate::raft::network::tests::test_network(65_536, &[1, 2]);
        let error = network
            .management_rpc(test_replica(3), ManagementAction::Discover, BUDGET)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("not configured"));
        let error = network
            .management_rpc(test_replica(2), ManagementAction::Discover, Duration::ZERO)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("timed out"));
    }
}
