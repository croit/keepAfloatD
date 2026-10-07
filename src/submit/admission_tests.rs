use super::*;
use crate::admission::{CONNECTIONS_PER_PEER, ConnectionAdmission as SourceAdmission};
use crate::connection_admission::AuthenticatedConnection;
use crate::raft::types::test_replica;
use crate::raft::{RaftNetworkImpl, store::new_store};
use std::net::{IpAddr, SocketAddr};

struct Fixture {
    cfg: Config,
    raft: KafRaft,
    client: Option<TcpStream>,
    server: TcpStream,
    from: SocketAddr,
    global: ConnectionAdmission,
    source: SourceAdmission,
    genesis: crate::raft::admission::Genesis,
}

impl Fixture {
    async fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (server, from) = listener.accept().await.unwrap();
        let cfg = super::tests::cfg_with(Some("admission-test-secret"));
        let (log, machine, state) = new_store(Arc::new(Vec::new()), 3, true, 0);
        let controller = crate::raft::network::testing::controller(&cfg);
        let authorization = controller.authorize_raft(test_replica(1)).unwrap();
        let genesis = authorization.session.context().genesis.clone();
        {
            let mut state = state.write().await;
            state.genesis = Some(genesis.clone());
            state.last_membership = authorization.committed_membership;
            state.bind_admission(authorization.session).unwrap();
        }
        let network = RaftNetworkImpl::new(Arc::new(cfg.clone()), state, controller).unwrap();
        let raft = KafRaft::new(
            test_replica(1),
            Arc::new(openraft::Config::default()),
            network,
            log,
            machine,
        )
        .await
        .unwrap();
        Self {
            cfg,
            raft,
            client: Some(client),
            server,
            from,
            global: ConnectionAdmission::new(1, 1),
            source: SourceAdmission::new([from]),
            genesis,
        }
    }

    fn request(&mut self, secret: &str) -> tokio::task::JoinHandle<TcpStream> {
        self.request_as(secret, 1)
    }

    fn request_as(
        &mut self,
        secret: &str,
        authenticated_id: u64,
    ) -> tokio::task::JoinHandle<TcpStream> {
        self.request_as_replica(secret, test_replica(authenticated_id))
    }

    fn request_as_replica(
        &mut self,
        secret: &str,
        replica: crate::raft::admission::ReplicaId,
    ) -> tokio::task::JoinHandle<TcpStream> {
        let mut client = self.client.take().unwrap();
        let secret = secret.to_owned();
        let genesis = self.genesis.clone();
        tokio::spawn(async move {
            let authenticated = crate::auth::client_bound(
                &mut client,
                crate::auth::Peer::for_replica(replica, None, true),
                1,
                Some(&secret),
                crate::auth::Listener::Submit,
            )
            .await;
            let Ok(authenticated) = authenticated else {
                client.shutdown().await.unwrap();
                return client;
            };
            let signed = crate::raft::admission::SignedAdmission::sign(
                Some(&secret),
                proof::RELEASE_REQUEST_ROLE,
                replica,
                test_replica(1),
                authenticated.binding,
                proof::ReleaseEnvelope {
                    genesis,
                    request: KafRequest::VipReleased {
                        node_id: 1,
                        vip: "192.0.2.1".parse().unwrap(),
                        generation: 1,
                    },
                },
            )
            .unwrap();
            let body = serde_json::to_vec(&signed).unwrap();
            write_submit_frame_with_timeout(&mut client, &body, Duration::from_secs(1))
                .await
                .unwrap();
            client
        })
    }
}

#[tokio::test]
async fn signed_release_requires_current_server_admission_and_exact_sender_boot() {
    for case in ["unbound", "foreign-boot", "foreign-genesis"] {
        let mut fixture = Fixture::new().await;
        let mut sender = test_replica(1);
        let expected = match case {
            "unbound" => {
                fixture
                    .raft
                    .with_state_machine(|machine| {
                        let state = machine.shared_state();
                        Box::pin(async move {
                            state.write().await.admission = None;
                        })
                    })
                    .await
                    .unwrap();
                "no bound admission session"
            }
            "foreign-boot" => {
                sender.boot_nonce = [99; 32];
                "unique committed voter"
            }
            _ => {
                fixture.genesis.epoch += 1;
                "genesis differs"
            }
        };
        let client = fixture.request_as_replica("admission-test-secret", sender);
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            handle_one_submit(
                &mut fixture.server,
                fixture.from,
                &fixture.raft,
                &fixture.cfg,
                fixture.global.try_begin().unwrap(),
                fixture.source.try_begin(fixture.from.ip()).unwrap(),
            ),
        )
        .await
        .unwrap();
        let error = result.unwrap_err().to_string();
        assert!(error.contains(expected), "{case}: {error}");
        drop(client.await.unwrap());
        assert!(
            fixture
                .global
                .try_begin()
                .unwrap()
                .try_authenticate()
                .is_ok()
        );
        let _recovered = fill_authenticated(&fixture.source, fixture.from.ip());
        fixture.raft.shutdown().await.unwrap();
    }
}

fn fill_authenticated(source: &SourceAdmission, ip: IpAddr) -> Vec<AuthenticatedConnection> {
    (0..CONNECTIONS_PER_PEER)
        .map(|_| {
            source
                .try_begin(ip)
                .expect("source handshake slot leaked")
                .try_authenticate()
                .ok()
                .expect("source authenticated slot leaked")
        })
        .collect()
}

#[tokio::test(start_paused = true)]
async fn accept_error_does_not_stop_submit_listener() {
    let f = Fixture::new().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let listener = crate::listener::test_support::FaultyListener {
        inner: listener,
        faults: Arc::new(std::sync::atomic::AtomicUsize::new(1)),
    };
    let mut cfg = f.cfg.clone();
    cfg.submit_timeout_ms = 50;
    cfg.peers[0].client_submit_address = address.to_string();
    let mut server = Box::pin(serve_submit_listener(
        listener,
        Arc::new(cfg),
        f.raft.clone(),
    ));
    assert!(
        futures::poll!(&mut server).is_pending(),
        "accept error stopped submit"
    );
    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::time::resume();
    let request = async {
        let mut client = TcpStream::connect(address).await.unwrap();
        crate::auth::client(
            &mut client,
            crate::auth::Peer::for_replica(test_replica(1), None, true),
            1,
            Some("admission-test-secret"),
            crate::auth::Listener::Submit,
        )
        .await
        .unwrap();
        let body = serde_json::to_vec(&SubmitEnvelope {
            request: KafRequest::HealthUpdate {
                node_id: 1,
                healthy: true,
            },
        })
        .unwrap();
        write_submit_frame_with_timeout(&mut client, &body, Duration::from_secs(1))
            .await
            .unwrap();
        assert!(
            read_framed_bounded(&mut client, SUBMIT_FRAME_MAX_BYTES)
                .await
                .is_err(),
            "unsigned health submission must be rejected after listener recovery"
        );
    };
    tokio::time::timeout(Duration::from_secs(2), async {
        tokio::select! {
            result = &mut server => panic!("accept error stopped submit: {result:?}"),
            () = request => {},
        }
    })
    .await
    .expect("submit listener did not recover");
    drop(server);
    f.raft.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn submit_shutdown_cancels_accept_backoff() {
    let f = Fixture::new().await;
    let faults = Arc::new(std::sync::atomic::AtomicUsize::new(2));
    let listener = crate::listener::test_support::FaultyListener {
        inner: TcpListener::bind("127.0.0.1:0").await.unwrap(),
        faults: faults.clone(),
    };
    let mut server = Box::pin(serve_submit_listener(
        listener,
        Arc::new(f.cfg),
        f.raft.clone(),
    ));
    assert!(futures::poll!(&mut server).is_pending());
    assert_eq!(faults.load(std::sync::atomic::Ordering::SeqCst), 1);
    drop(server);
    tokio::time::advance(Duration::from_secs(2)).await;
    assert_eq!(faults.load(std::sync::atomic::Ordering::SeqCst), 1);
    f.raft.shutdown().await.unwrap();
}

#[tokio::test]
async fn submit_retains_only_authenticated_slots_until_completion_or_cancellation() {
    for complete in [false, true] {
        let mut f = Fixture::new().await;
        let client = f.request("admission-test-secret");
        let mut handler = Box::pin(handle_one_submit(
            &mut f.server,
            f.from,
            &f.raft,
            &f.cfg,
            f.global.try_begin().unwrap(),
            f.source.try_begin(f.from.ip()).unwrap(),
        ));
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                assert!(futures::poll!(&mut handler).is_pending());
                if f.global.try_begin().is_some() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("submit did not reach authenticated dispatch");

        let pending: Vec<_> = (0..CONNECTIONS_PER_PEER)
            .map(|_| f.source.try_begin(f.from.ip()).unwrap())
            .collect();
        assert!(f.source.try_begin(f.from.ip()).is_none());
        let authenticated: Vec<_> = pending
            .into_iter()
            .take(CONNECTIONS_PER_PEER - 1)
            .map(|permit| permit.try_authenticate().ok().unwrap())
            .collect();
        assert!(
            f.source
                .try_begin(f.from.ip())
                .unwrap()
                .try_authenticate()
                .is_err()
        );
        assert!(f.global.try_begin().unwrap().try_authenticate().is_err());
        let mut client = client.await.unwrap();
        if complete {
            tokio::time::timeout(Duration::from_secs(1), &mut handler)
                .await
                .unwrap()
                .unwrap();
            let body = read_framed_bounded(&mut client, SUBMIT_FRAME_MAX_BYTES)
                .await
                .unwrap();
            let signed: crate::raft::admission::SignedAdmission<SubmitResponse> =
                serde_json::from_slice(&body).unwrap();
            let response = signed.payload;
            assert!(
                !response.ok,
                "an uninitialized Raft must not commit a request"
            );
        }
        drop(handler);
        drop(authenticated);
        let _recovered = fill_authenticated(&f.source, f.from.ip());
        assert!(f.global.try_begin().unwrap().try_authenticate().is_ok());
        f.raft.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn submit_rejection_releases_both_budgets_including_partial_transition() {
    for (secret, global_full, source_full, expected) in [
        ("wrong-secret", false, true, "early eof"),
        (
            "admission-test-secret",
            true,
            false,
            "authenticated submit connection limit reached",
        ),
        (
            "admission-test-secret",
            false,
            true,
            "source authenticated submit connection limit reached",
        ),
    ] {
        let mut f = Fixture::new().await;
        let global_held = global_full.then(|| {
            f.global
                .try_begin()
                .unwrap()
                .try_authenticate()
                .ok()
                .unwrap()
        });
        let source_held = source_full.then(|| fill_authenticated(&f.source, f.from.ip()));
        let client = f.request(secret);
        let error = tokio::time::timeout(
            Duration::from_secs(1),
            handle_one_submit(
                &mut f.server,
                f.from,
                &f.raft,
                &f.cfg,
                f.global.try_begin().unwrap(),
                f.source.try_begin(f.from.ip()).unwrap(),
            ),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert_eq!(error.to_string(), expected);
        drop(client.await.unwrap());
        drop(global_held);
        drop(source_held);
        let _recovered = fill_authenticated(&f.source, f.from.ip());
        assert!(f.global.try_begin().unwrap().try_authenticate().is_ok());
        f.raft.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn authenticated_submit_cannot_claim_another_same_ip_peer() {
    let mut f = Fixture::new().await;
    let client = f.request_as("admission-test-secret", 2);
    let error = tokio::time::timeout(
        Duration::from_secs(1),
        handle_one_submit(
            &mut f.server,
            f.from,
            &f.raft,
            &f.cfg,
            f.global.try_begin().unwrap(),
            f.source.try_begin(f.from.ip()).unwrap(),
        ),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(
        error.to_string(),
        "submit node_id differs from authenticated peer"
    );
    drop(client.await.unwrap());
    assert!(f.global.try_begin().unwrap().try_authenticate().is_ok());
    let _recovered = fill_authenticated(&f.source, f.from.ip());
    f.raft.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancellation_during_submit_authentication_releases_both_pending_budgets() {
    let mut f = Fixture::new().await;
    let mut handler = Box::pin(handle_one_submit(
        &mut f.server,
        f.from,
        &f.raft,
        &f.cfg,
        f.global.try_begin().unwrap(),
        f.source.try_begin(f.from.ip()).unwrap(),
    ));
    assert!(futures::poll!(&mut handler).is_pending());
    assert!(f.global.try_begin().is_none());
    drop(handler);
    assert!(f.global.try_begin().unwrap().try_authenticate().is_ok());
    let _recovered = fill_authenticated(&f.source, f.from.ip());
    f.raft.shutdown().await.unwrap();
}
