use super::*;
use crate::raft::types::test_replica;

#[test]
fn nonce_bound_progress_survives_submit_decoding() {
    let input = serde_json::json!({"request": {"HealthProgress": {
        "node_id": 1,
        "healthy": true,
        "replica": format!("{:016x}:{}", 1, "01".repeat(32)),
        "epoch": 42,
        "request_nonce": vec![2; 32],
        "genesis": {
            "config": {"version": 1, "digest": vec![3; 32]},
            "epoch": 42,
            "voters": [format!("{:016x}:{}", 1, "01".repeat(32))]
        }
    }}});
    let decoded = serde_json::from_value::<SubmitEnvelope>(input.clone());
    assert!(
        decoded.is_ok(),
        "boot-bound progress request rejected: {decoded:?}"
    );
    assert_eq!(serde_json::to_value(decoded.unwrap()).unwrap(), input);
}

#[test]
fn nonce_bound_progress_decoder_preserves_full_width_epoch_and_nonce() {
    use crate::raft::admission::{Genesis, HealthProgress, ReplicaId};
    let replica = ReplicaId {
        physical_id: u64::MAX,
        boot_nonce: [255; 32],
    };
    for epoch in [u64::MAX as u128 + 1, u128::MAX] {
        let request = KafRequest::HealthProgress(HealthProgress {
            node_id: u64::MAX,
            healthy: Some(true),
            replica,
            epoch,
            request_nonce: [254; 32],
            genesis: Genesis {
                config: crate::config::ClusterConfigFingerprint {
                    version: 1,
                    digest: [253; 32],
                },
                epoch,
                voters: std::collections::BTreeSet::from([replica]),
            },
        });
        let bytes = serde_json::to_vec(&SubmitEnvelope {
            request: request.clone(),
        })
        .unwrap();
        let decoded: SubmitEnvelope = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(decoded.request, request);
    }
}

#[test]
fn unsigned_progress_cannot_enter_ordinary_submit_admission() {
    use crate::raft::admission::{Genesis, HealthProgress, ReplicaId};
    let cfg = config("127.0.0.1:2000", "127.0.0.1:2001");
    let replica = ReplicaId {
        physical_id: 1,
        boot_nonce: [1; 32],
    };
    let progress = HealthProgress {
        node_id: 1,
        healthy: Some(true),
        replica,
        epoch: 42,
        request_nonce: [2; 32],
        genesis: Genesis {
            config: cfg.cluster_config_fingerprint().unwrap(),
            epoch: 42,
            voters: std::collections::BTreeSet::from([replica]),
        },
    };
    let result = super::super::validate_and_extract(
        &cfg,
        "127.0.0.1".parse().unwrap(),
        SubmitEnvelope {
            request: KafRequest::HealthProgress(progress),
        },
    );
    assert!(
        result.is_err(),
        "unsigned boot progress reached ordinary Raft submit"
    );
}

fn config(address: &str, raft_address: &str) -> Arc<Config> {
    let mut cfg = super::super::tests::cfg_with(Some("proof-test-secret-0123456789012345"));
    cfg.client_submit_listen = address.into();
    cfg.raft_listen = raft_address.into();
    cfg.peers[0].client_submit_address = address.into();
    cfg.peers[0].raft_address = raft_address.into();
    Arc::new(cfg)
}

struct Fixture {
    cfg: Arc<Config>,
    raft: KafRaft,
    listener: TcpListener,
    state: Arc<tokio::sync::RwLock<crate::raft::KafStorageState>>,
    session: AdmissionSession,
}

impl Fixture {
    async fn new(lifetime: Duration) -> Self {
        use crate::raft::admission::AdmissionContext;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut cfg = (*config("127.0.0.1:0", "127.0.0.1:0")).clone();
        cfg.peers[1].client_submit_address = listener.local_addr().unwrap().to_string();
        cfg.submit_timeout_ms = 5000;
        let cfg = Arc::new(cfg);
        let context = AdmissionContext {
            local_replica: test_replica(1),
            genesis: Genesis {
                config: cfg.cluster_config_fingerprint().unwrap(),
                epoch: 1,
                voters: [test_replica(1), test_replica(2)].into(),
            },
        };
        let controller = crate::raft::network::testing::with_deadline(
            context,
            tokio::time::Instant::now() + lifetime,
        );
        let authorization = controller.authorize_raft(test_replica(1)).unwrap();
        let session = authorization.session;
        let (log, machine, state) =
            crate::raft::store::new_admitted_store(Arc::new(vec![]), 3, true, 0);
        {
            let mut state = state.write().await;
            state.genesis = Some(session.context().genesis.clone());
            state.last_membership = authorization.committed_membership;
            state.bind_admission(session.clone()).unwrap();
        }
        let network =
            crate::raft::RaftNetworkImpl::new(cfg.clone(), state.clone(), controller).unwrap();
        let raft = KafRaft::new(
            test_replica(1),
            Arc::new(openraft::Config {
                enable_tick: false,
                ..Default::default()
            }),
            network,
            log,
            machine,
        )
        .await
        .unwrap();
        raft.append_entries(openraft::raft::AppendEntriesRequest {
            vote: openraft::Vote::new_committed(1, test_replica(2)),
            prev_log_id: None,
            entries: vec![],
            leader_commit: None,
        })
        .await
        .unwrap();
        Self {
            cfg,
            raft,
            listener,
            state,
            session,
        }
    }

    fn request(&self) -> KafRequest {
        KafRequest::VipReleased {
            node_id: 1,
            vip: "192.0.2.1".parse().unwrap(),
            generation: 7,
        }
    }

    async fn submit(&self) -> anyhow::Result<()> {
        super::super::submit_request(&self.cfg, &self.raft, self.request()).await
    }

    async fn accept(&self) -> (TcpStream, [u8; 32]) {
        let (mut socket, _) = self.listener.accept().await.unwrap();
        let auth = crate::auth::server_bound(
            &mut socket,
            crate::auth::Peer::for_replica(test_replica(2), None, true),
            self.cfg.cluster_secret.as_deref(),
            crate::auth::Listener::Submit,
        )
        .await
        .unwrap();
        let bytes = read_framed_bounded(&mut socket, SUBMIT_FRAME_MAX_BYTES)
            .await
            .unwrap();
        let signed: SignedAdmission<ReleaseEnvelope> = serde_json::from_slice(&bytes).unwrap();
        signed
            .verify(
                self.cfg.cluster_secret.as_deref(),
                RELEASE_REQUEST_ROLE,
                test_replica(1),
                test_replica(2),
                auth.binding,
            )
            .unwrap();
        assert_eq!(signed.payload.genesis, self.session.context().genesis);
        assert_eq!(signed.payload.request, self.request());
        (socket, auth.binding)
    }

    fn signed(
        &self,
        binding: [u8; 32],
        response: SubmitResponse,
    ) -> SignedAdmission<SubmitResponse> {
        SignedAdmission::sign(
            self.cfg.cluster_secret.as_deref(),
            RELEASE_RESPONSE_ROLE,
            test_replica(2),
            test_replica(1),
            binding,
            response,
        )
        .unwrap()
    }

    async fn exchange(&self, make_reply: impl FnOnce([u8; 32]) -> Vec<u8>) -> anyhow::Result<()> {
        let server = async {
            let (mut socket, binding) = self.accept().await;
            write_submit_frame_with_timeout(
                &mut socket,
                &make_reply(binding),
                Duration::from_secs(1),
            )
            .await
            .unwrap();
        };
        let (result, ()) = tokio::time::timeout(Duration::from_secs(2), async {
            tokio::join!(self.submit(), server)
        })
        .await
        .expect("release fixture stalled");
        result
    }
}

fn accepted() -> SubmitResponse {
    SubmitResponse {
        ok: true,
        message: String::new(),
        log_id: Some(openraft::testing::log_id::<crate::raft::TypeConfig>(
            1,
            test_replica(2),
            7,
        )),
    }
}

#[tokio::test]
async fn legacy_server_is_rejected_without_an_unhealthy_fallback() {
    let fixture = Fixture::new(Duration::from_secs(60)).await;
    let server = async {
        let (mut socket, _) = fixture.listener.accept().await.unwrap();
        crate::auth::server(
            &mut socket,
            crate::auth::Peer::new(2, None, true),
            fixture.cfg.cluster_secret.as_deref(),
            crate::auth::Listener::Submit,
        )
        .await
        .unwrap();
        assert!(
            read_framed_bounded(&mut socket, SUBMIT_FRAME_MAX_BYTES)
                .await
                .is_err()
        );
    };
    let (result, ()) = tokio::join!(fixture.submit(), server);
    fixture.raft.shutdown().await.unwrap();
    assert!(result.unwrap_err().to_string().contains("boot changed"));
}

#[tokio::test]
async fn capable_acknowledgement_returns_its_committed_log_id_without_renewing_admission() {
    let fixture = Fixture::new(Duration::from_secs(60)).await;
    let deadline = fixture.session.check().unwrap();
    fixture
        .exchange(|binding| serde_json::to_vec(&fixture.signed(binding, accepted())).unwrap())
        .await
        .unwrap();
    assert_eq!(fixture.session.check().unwrap(), deadline);
    fixture.raft.shutdown().await.unwrap();
}

#[tokio::test]
async fn acknowledgement_without_index_is_rejected_without_unhealthy_fallback() {
    let fixture = Fixture::new(Duration::from_secs(60)).await;
    let result = fixture
        .exchange(|binding| {
            let mut response = accepted();
            response.log_id = None;
            serde_json::to_vec(&fixture.signed(binding, response)).unwrap()
        })
        .await;
    fixture.raft.shutdown().await.unwrap();
    assert!(result.unwrap_err().to_string().contains("committed log ID"));
}

#[tokio::test]
async fn legacy_health_cannot_obtain_an_application_proof_even_when_admitted() {
    let fixture = Fixture::new(Duration::from_secs(60)).await;
    for healthy in [true, false] {
        let result = super::super::submit_request(
            &fixture.cfg,
            &fixture.raft,
            KafRequest::HealthUpdate {
                node_id: 1,
                healthy,
            },
        )
        .await;
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("only admitted VIP releases")
        );
    }
    assert!(fixture.state.read().await.node_health.is_empty());
    fixture.raft.shutdown().await.unwrap();
}

#[tokio::test]
async fn release_proof_rejects_acknowledgement_from_a_previous_channel() {
    let fixture = Fixture::new(Duration::from_secs(60)).await;
    let result = fixture
        .exchange(|_| serde_json::to_vec(&fixture.signed([0; 32], accepted())).unwrap())
        .await;
    fixture.raft.shutdown().await.unwrap();
    assert!(result.is_err());
}

#[tokio::test]
async fn release_proof_rejects_acknowledgement_from_another_boot() {
    let fixture = Fixture::new(Duration::from_secs(60)).await;
    let result = fixture
        .exchange(|binding| {
            let signed = SignedAdmission::sign(
                fixture.cfg.cluster_secret.as_deref(),
                RELEASE_RESPONSE_ROLE,
                ReplicaId {
                    physical_id: 2,
                    boot_nonce: [99; 32],
                },
                test_replica(1),
                binding,
                accepted(),
            )
            .unwrap();
            serde_json::to_vec(&signed).unwrap()
        })
        .await;
    fixture.raft.shutdown().await.unwrap();
    assert!(result.is_err());
}

#[tokio::test]
async fn release_proof_stalled_response_is_bounded_by_admission_not_the_submit_budget() {
    let fixture = Fixture::new(Duration::from_secs(1)).await;
    let submission = fixture.submit();
    tokio::pin!(submission);
    let (_socket, _) = tokio::select! {
        request = fixture.accept() => request,
        result = &mut submission => panic!("submission ended before request: {result:?}"),
    };
    tokio::time::pause();
    assert!(futures::poll!(&mut submission).is_pending());
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(submission.await.is_err());
    tokio::time::resume();
    fixture.raft.shutdown().await.unwrap();
}

#[tokio::test]
async fn release_proof_rejects_a_request_role_record_as_an_acknowledgement() {
    let fixture = Fixture::new(Duration::from_secs(60)).await;
    let result = fixture
        .exchange(|binding| {
            serde_json::to_vec(
                &SignedAdmission::sign(
                    fixture.cfg.cluster_secret.as_deref(),
                    RELEASE_REQUEST_ROLE,
                    test_replica(2),
                    test_replica(1),
                    binding,
                    accepted(),
                )
                .unwrap(),
            )
            .unwrap()
        })
        .await;
    fixture.raft.shutdown().await.unwrap();
    assert!(result.is_err());
}

#[tokio::test]
async fn release_proof_rejects_modified_acknowledgement_content() {
    let fixture = Fixture::new(Duration::from_secs(60)).await;
    let result = fixture
        .exchange(|binding| {
            let mut signed = fixture.signed(binding, accepted());
            signed.payload.log_id = Some(openraft::testing::log_id::<crate::raft::TypeConfig>(
                1,
                test_replica(2),
                8,
            ));
            serde_json::to_vec(&signed).unwrap()
        })
        .await;
    fixture.raft.shutdown().await.unwrap();
    assert!(result.is_err());
}

#[tokio::test]
async fn release_proof_rejects_unsigned_acknowledgement_with_an_index() {
    let fixture = Fixture::new(Duration::from_secs(60)).await;
    let result = fixture
        .exchange(|_| serde_json::to_vec(&accepted()).unwrap())
        .await;
    fixture.raft.shutdown().await.unwrap();
    assert!(result.is_err());
}

#[tokio::test]
async fn release_forwarding_rejects_changed_genesis_before_acknowledgement() {
    let fixture = Fixture::new(Duration::from_secs(60)).await;
    let server = async {
        let (mut socket, binding) = fixture.accept().await;
        fixture.state.write().await.genesis.as_mut().unwrap().epoch += 1;
        let bytes = serde_json::to_vec(&fixture.signed(binding, accepted())).unwrap();
        write_submit_frame_with_timeout(&mut socket, &bytes, Duration::from_secs(1))
            .await
            .unwrap();
    };
    let (result, ()) = tokio::join!(fixture.submit(), server);
    fixture.raft.shutdown().await.unwrap();
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("committed genesis")
    );
}

#[tokio::test]
async fn release_forwarding_rejects_removed_leader_boot_before_acknowledgement() {
    let fixture = Fixture::new(Duration::from_secs(60)).await;
    let server = async {
        let (mut socket, binding) = fixture.accept().await;
        fixture.state.write().await.last_membership = openraft::StoredMembership::new(
            None,
            openraft::Membership::new_with_defaults(vec![[test_replica(1)].into()], []),
        );
        let bytes = serde_json::to_vec(&fixture.signed(binding, accepted())).unwrap();
        write_submit_frame_with_timeout(&mut socket, &bytes, Duration::from_secs(1))
            .await
            .unwrap();
    };
    let (result, ()) = tokio::join!(fixture.submit(), server);
    fixture.raft.shutdown().await.unwrap();
    assert!(result.unwrap_err().to_string().contains("committed voter"));
}

#[tokio::test]
async fn release_forwarding_rejects_unbound_runtime_before_acknowledgement() {
    let fixture = Fixture::new(Duration::from_secs(60)).await;
    let server = async {
        let (mut socket, binding) = fixture.accept().await;
        fixture.state.write().await.admission = None;
        let bytes = serde_json::to_vec(&fixture.signed(binding, accepted())).unwrap();
        write_submit_frame_with_timeout(&mut socket, &bytes, Duration::from_secs(1))
            .await
            .unwrap();
    };
    let (result, ()) = tokio::join!(fixture.submit(), server);
    fixture.raft.shutdown().await.unwrap();
    assert!(result.unwrap_err().to_string().contains("not bound"));
}

async fn assert_rejection_without_fallback(include_log: bool) {
    let fixture = Fixture::new(Duration::from_secs(60)).await;
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = async {
        let (mut socket, binding) = fixture.accept().await;
        let mut response = accepted();
        response.ok = false;
        response.message = "admission rejected".into();
        if !include_log {
            response.log_id = None;
        }
        let bytes = serde_json::to_vec(&fixture.signed(binding, response)).unwrap();
        write_submit_frame_with_timeout(&mut socket, &bytes, Duration::from_secs(1))
            .await
            .unwrap();
        tokio::select! {
            _ = fixture.listener.accept() => panic!("rejection triggered an unrelated fallback"),
            _ = stopped => {}
        }
    };
    let client = async {
        let result = fixture.submit().await;
        let _ = stop.send(());
        result
    };
    let (result, ()) = tokio::join!(client, server);
    fixture.raft.shutdown().await.unwrap();
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("admission rejected")
    );
}

#[tokio::test]
async fn leader_rejection_does_not_publish_an_unhealthy_fallback() {
    assert_rejection_without_fallback(false).await;
}

#[tokio::test]
async fn rejected_acknowledgement_cannot_grant_a_log_id_proof() {
    assert_rejection_without_fallback(true).await;
}
