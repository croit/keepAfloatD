use super::*;

fn config(address: &str, raft_address: &str) -> Arc<Config> {
    Arc::new(serde_yaml::from_str(&format!(
        "node_id: 1\nraft_listen: '{raft_address}'\nclient_submit_listen: '{address}'\n\
         peers:\n  - id: 1\n    raft_address: '{raft_address}'\n    client_submit_address: '{address}'\n\
         vips: []\nhealth:\n  command: [/bin/true]\n  interval_ms: 1000\n  timeout_ms: 500\n\
         dry_run: true\n"
    )).unwrap())
}

#[tokio::test]
async fn legacy_server_rejects_proof_before_receiving_only_an_unhealthy_fallback() {
    let listener = TcpListener::bind("127.249.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let cfg = config(&address, "127.249.0.1:1");
    let fenced = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let server_fenced = fenced.clone();
    let server = tokio::spawn(async move {
        #[derive(Deserialize)]
        struct LegacyEnvelope {
            request: KafRequest,
        }
        let (mut first, _) = listener.accept().await.unwrap();
        let body = super::super::read_framed_bounded(&mut first, 4096)
            .await
            .unwrap();
        assert!(
            serde_json::from_slice::<LegacyEnvelope>(&body).is_err(),
            "legacy must never decode the proof request as a healthy update"
        );
        drop(first);
        let (mut fallback, _) = listener.accept().await.unwrap();
        let body = super::super::read_framed_bounded(&mut fallback, 4096)
            .await
            .unwrap();
        let envelope: LegacyEnvelope = serde_json::from_slice(&body).unwrap();
        assert!(
            server_fenced.load(std::sync::atomic::Ordering::SeqCst),
            "local binding eligibility must be fenced before the unhealthy fallback can commit"
        );
        assert_eq!(
            envelope.request,
            KafRequest::HealthUpdate {
                node_id: 1,
                healthy: false
            }
        );
        super::super::write_submit_frame_with_timeout(
            &mut fallback,
            br#"{"ok":true,"message":""}"#,
            Duration::from_secs(1),
        )
        .await
        .unwrap();
    });
    assert!(
        forward_health(&cfg, &address, true, || {
            fenced.store(true, std::sync::atomic::Ordering::SeqCst);
        })
        .await
        .unwrap()
        .is_none()
    );
    server.await.unwrap();
}

#[tokio::test]
async fn capable_acknowledgement_returns_its_committed_log_id() {
    let listener = TcpListener::bind("127.249.0.2:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let cfg = config(&address, "127.249.0.2:1");
    let id = openraft::testing::log_id::<TypeConfig>(2, 1, 20);
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let body = super::super::read_framed_bounded(&mut socket, 4096)
            .await
            .unwrap();
        let envelope: SubmitEnvelope = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            envelope.request,
            KafRequest::HealthUpdate {
                node_id: 1,
                healthy: true
            }
        );
        let response = serde_json::to_vec(&SubmitResponse {
            ok: true,
            message: String::new(),
            log_id: Some(id),
        })
        .unwrap();
        super::super::write_submit_frame_with_timeout(
            &mut socket,
            &response,
            Duration::from_secs(1),
        )
        .await
        .unwrap();
    });
    assert_eq!(
        forward_health(&cfg, &address, true, || panic!(
            "capable ACK must not fence"
        ))
        .await
        .unwrap(),
        Some(id)
    );
    server.await.unwrap();
}

#[tokio::test]
async fn acknowledgement_without_index_fences_before_unhealthy_fallback() {
    let listener = TcpListener::bind("127.249.0.4:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let cfg = config(&address, "127.249.0.4:1");
    let fenced = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let server_fenced = fenced.clone();
    let server = tokio::spawn(async move {
        for fallback in [false, true] {
            let (mut socket, _) = listener.accept().await.unwrap();
            let body = super::super::read_framed_bounded(&mut socket, 4096)
                .await
                .unwrap();
            let envelope: SubmitEnvelope = serde_json::from_slice(&body).unwrap();
            assert_eq!(
                envelope.request,
                KafRequest::HealthUpdate {
                    node_id: 1,
                    healthy: !fallback
                }
            );
            if fallback {
                assert!(server_fenced.load(std::sync::atomic::Ordering::SeqCst));
            }
            super::super::write_submit_frame_with_timeout(
                &mut socket,
                br#"{"ok":true,"message":""}"#,
                Duration::from_secs(1),
            )
            .await
            .unwrap();
        }
    });
    assert!(
        forward_health(&cfg, &address, true, || {
            fenced.store(true, std::sync::atomic::Ordering::SeqCst);
        })
        .await
        .unwrap()
        .is_none()
    );
    server.await.unwrap();
}

#[tokio::test]
async fn acknowledged_entry_must_be_locally_applied_before_proof_completes() {
    use openraft::async_runtime::WatchReceiver;
    let listener = std::net::TcpListener::bind("127.249.0.3:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let cfg = config("127.249.0.3:1", &address);
    drop(listener);
    let (raft, network, _, _, _, mut controls) =
        crate::raft::start_raft(cfg.clone(), Arc::new(Vec::new()))
            .await
            .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while raft.current_leader().await != Some(1) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let current = raft
        .metrics()
        .borrow_watched()
        .last_applied
        .map(|id| id.index)
        .unwrap_or(0);
    let target = openraft::testing::log_id::<TypeConfig>(1, 1, current + 50);
    let waiter = wait_for_applied(&raft, target, Duration::from_secs(2));
    tokio::pin!(waiter);
    let completed_early = futures::poll!(&mut waiter).is_ready();
    if !completed_early {
        for _ in 0..50 {
            raft.client_write(KafRequest::HealthUpdate {
                node_id: 1,
                healthy: true,
            })
            .await
            .unwrap();
        }
        waiter.await.unwrap();
    }
    let own_ack = submit_health(&cfg, &raft, true, || panic!("local leader supports proof"))
        .await
        .unwrap()
        .unwrap();
    assert!(raft.metrics().borrow_watched().last_applied.unwrap().index >= own_ack.index);
    controls.shutdown().await.unwrap();
    network.shutdown().await.unwrap();
    raft.shutdown().await.unwrap();
    assert!(
        !completed_early,
        "an ACK ahead of local apply must not make proof usable"
    );
}
