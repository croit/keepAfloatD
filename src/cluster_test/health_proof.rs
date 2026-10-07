use super::fixtures::ControlledProbe;
use super::*;
use crate::raft::admission::{ReplicaId, SignedAdmission};
use crate::raft::network::authorization::management::{
    ManagementAction, ManagementRequest, ManagementResponse, ManagementResult, REQUEST_ROLE,
    RESPONSE_ROLE,
};
use anyhow::Context;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, mpsc};

const PROXY_SECRET: &str = "cluster-test-secret-01234567890123";
const PROXY_NONCE: [u8; 32] = [7; 32];

#[derive(serde::Serialize, serde::Deserialize)]
enum ManagementEnvelope<T> {
    #[serde(rename = "admission_control")]
    Control(T),
}

async fn proxy_management(
    mut client: TcpStream,
    upstream: u16,
    target: ReplicaId,
) -> anyhow::Result<()> {
    if client.peek(&mut [0_u8; 1]).await? == 0 {
        return Ok(());
    }
    let authenticated = crate::auth::server_bound(
        &mut client,
        crate::auth::Peer::for_replica(target, None, true),
        Some(PROXY_SECRET),
        crate::auth::Listener::Raft,
    )
    .await
    .context("read proxy request authentication")?;
    let peer = authenticated
        .peer
        .replica()
        .context("missing sender boot")?;
    let body = read_frame(&mut client)
        .await
        .context("read proxy request")?;
    let ManagementEnvelope::Control(request): ManagementEnvelope<
        SignedAdmission<ManagementRequest>,
    > = serde_json::from_slice(&body)?;
    request.verify(
        Some(PROXY_SECRET),
        REQUEST_ROLE,
        peer,
        target,
        authenticated.binding,
    )?;
    let (mut client_read, mut client_write) = client.split();
    let mut trailing = [0; 1];
    tokio::select! {
        biased;
        closed = client_read.read(&mut trailing) => {
            match closed {
                Ok(0) => Ok(()),
                Err(error) if peer_disconnected(&error) => Ok(()),
                Ok(_) => anyhow::bail!("unexpected trailing proxy request"),
                Err(error) => Err(error).context("watch proxy client"),
            }
        }
        result = async {
            let mut server = test_cluster_connect(upstream).await?;
            let upstream = crate::auth::client_bound(
                &mut server,
                crate::auth::Peer::for_replica(peer, None, true),
                target.physical_id,
                Some(PROXY_SECRET),
                crate::auth::Listener::Raft,
            ).await?;
            anyhow::ensure!(upstream.peer.replica() == Some(target), "upstream boot changed");
            let forwarded = SignedAdmission::sign(
                Some(PROXY_SECRET), REQUEST_ROLE, peer, target,
                upstream.binding, request.payload.clone(),
            )?;
            write_frame(&mut server, &serde_json::to_vec(&ManagementEnvelope::Control(forwarded))?)
                .await.context("write proxy request upstream")?;
            let body = read_frame(&mut server).await.context("read upstream proxy response")?;
            let ManagementEnvelope::Control(response): ManagementEnvelope<SignedAdmission<ManagementResponse>> =
                serde_json::from_slice(&body)?;
            response.verify(Some(PROXY_SECRET), RESPONSE_ROLE, target, peer, upstream.binding)?;
            anyhow::ensure!(response.payload.nonce == request.payload.nonce, "upstream challenge changed");
            let reply = SignedAdmission::sign(
                Some(PROXY_SECRET), RESPONSE_ROLE, target, peer,
                authenticated.binding, response.payload,
            )?;
            write_proxy_response(&mut client_write, &serde_json::to_vec(&ManagementEnvelope::Control(reply))?).await
        } => result,
    }
}

async fn read_frame(stream: &mut TcpStream) -> anyhow::Result<Vec<u8>> {
    let size = stream.read_u32().await?;
    anyhow::ensure!(size <= 4096, "oversized test submit frame");
    let mut body = vec![0; size as usize];
    stream.read_exact(&mut body).await?;
    Ok(body)
}

async fn write_frame(
    stream: &mut (impl tokio::io::AsyncWrite + Unpin),
    body: &[u8],
) -> std::io::Result<()> {
    stream.write_u32(body.len() as u32).await?;
    stream.write_all(body).await?;
    Ok(())
}

fn peer_disconnected(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::ConnectionReset
    )
}

async fn write_proxy_response(
    client: &mut (impl tokio::io::AsyncWrite + Unpin),
    reply: &[u8],
) -> anyhow::Result<()> {
    match write_frame(client, reply).await {
        Err(error) if peer_disconnected(&error) => {
            tracing::debug!(%error, "proxy client disconnected while writing response");
            Ok(())
        }
        result => result.context("write proxy response"),
    }
}

async fn proxy_with_cancelled_request(prefix: &[u8]) -> anyhow::Result<()> {
    let listener = TcpListener::bind((CLUSTER_TEST_ADDR, 0)).await.unwrap();
    let mut client = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (server, _) = listener.accept().await.unwrap();
    client.write_all(prefix).await.unwrap();
    client.shutdown().await.unwrap();
    tokio::time::timeout(
        Duration::from_secs(1),
        proxy_management(server, 0, crate::raft::types::test_replica(2)),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn proxy_allows_cancellation_before_a_request_starts() {
    proxy_with_cancelled_request(&[]).await.unwrap();
}

#[tokio::test]
async fn proxy_rejects_a_truncated_request() {
    let error = proxy_with_cancelled_request(&[0, 0]).await.unwrap_err();
    assert!(error.to_string().contains("read proxy request"), "{error}");
}

async fn start_proxy() -> (
    TcpStream,
    TcpListener,
    tokio::task::JoinHandle<anyhow::Result<()>>,
    [u8; 32],
) {
    let listener = TcpListener::bind((CLUSTER_TEST_ADDR, 0)).await.unwrap();
    let upstream = TcpListener::bind((CLUSTER_TEST_ADDR, 0)).await.unwrap();
    let mut client = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (server, _) = listener.accept().await.unwrap();
    let port = upstream.local_addr().unwrap().port();
    let proxy = tokio::spawn(async move {
        tokio::time::timeout(
            Duration::from_secs(1),
            proxy_management(server, port, crate::raft::types::test_replica(2)),
        )
        .await?
    });
    let authenticated = crate::auth::client_bound(
        &mut client,
        crate::auth::Peer::for_replica(crate::raft::types::test_replica(1), None, true),
        2,
        Some(PROXY_SECRET),
        crate::auth::Listener::Raft,
    )
    .await
    .unwrap();
    (client, upstream, proxy, authenticated.binding)
}

async fn start_forwarded_proxy() -> (
    TcpStream,
    TcpStream,
    tokio::task::JoinHandle<anyhow::Result<()>>,
    [u8; 32],
    [u8; 32],
) {
    let (mut client, upstream, proxy, client_binding) = start_proxy().await;
    let request = SignedAdmission::sign(
        Some(PROXY_SECRET),
        REQUEST_ROLE,
        crate::raft::types::test_replica(1),
        crate::raft::types::test_replica(2),
        client_binding,
        ManagementRequest {
            nonce: PROXY_NONCE,
            action: ManagementAction::Discover,
        },
    )
    .unwrap();
    write_frame(
        &mut client,
        &serde_json::to_vec(&ManagementEnvelope::Control(&request)).unwrap(),
    )
    .await
    .unwrap();
    let (mut server, _) = upstream.accept().await.unwrap();
    let authenticated = crate::auth::server_bound(
        &mut server,
        crate::auth::Peer::for_replica(crate::raft::types::test_replica(2), None, true),
        Some(PROXY_SECRET),
        crate::auth::Listener::Raft,
    )
    .await
    .unwrap();
    let ManagementEnvelope::Control(forwarded): ManagementEnvelope<
        SignedAdmission<ManagementRequest>,
    > = serde_json::from_slice(&read_frame(&mut server).await.unwrap()).unwrap();
    forwarded
        .verify(
            Some(PROXY_SECRET),
            REQUEST_ROLE,
            crate::raft::types::test_replica(1),
            crate::raft::types::test_replica(2),
            authenticated.binding,
        )
        .unwrap();
    assert_eq!(forwarded.payload, request.payload);
    assert_ne!(forwarded.binding, client_binding);
    (client, server, proxy, client_binding, authenticated.binding)
}

fn management_reply(binding: [u8; 32]) -> SignedAdmission<ManagementResponse> {
    SignedAdmission::sign(
        Some(PROXY_SECRET),
        RESPONSE_ROLE,
        crate::raft::types::test_replica(2),
        crate::raft::types::test_replica(1),
        binding,
        ManagementResponse {
            nonce: PROXY_NONCE,
            result: ManagementResult::Discovery {
                genesis: None,
                voters: [crate::raft::types::test_replica(2)].into(),
            },
        },
    )
    .unwrap()
}

#[tokio::test]
async fn proxy_cancels_forwarding_when_a_complete_request_is_abandoned() {
    let (mut client, mut server, proxy, _, _) = start_forwarded_proxy().await;
    client.shutdown().await.unwrap();
    drop(client);

    proxy
        .await
        .unwrap()
        .expect("proxy kept awaiting upstream after its client cancelled the complete request");
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), server.read(&mut [0; 1]))
            .await
            .unwrap()
            .unwrap(),
        0,
        "cancelled forwarding must close its upstream connection",
    );
}

#[tokio::test]
async fn proxy_cancels_forwarding_when_a_complete_request_is_reset() {
    let (client, mut server, proxy, _, _) = start_forwarded_proxy().await;
    client.set_zero_linger().unwrap();
    drop(client);
    proxy
        .await
        .unwrap()
        .expect("reset client must cancel forwarding");
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), server.read(&mut [0; 1]))
            .await
            .unwrap()
            .unwrap(),
        0,
    );
}

#[tokio::test]
async fn proxy_forwards_a_complete_upstream_response() {
    let (mut client, mut server, proxy, client_binding, upstream_binding) =
        start_forwarded_proxy().await;
    let reply = management_reply(upstream_binding);
    write_frame(
        &mut server,
        &serde_json::to_vec(&ManagementEnvelope::Control(&reply)).unwrap(),
    )
    .await
    .unwrap();
    let ManagementEnvelope::Control(received): ManagementEnvelope<
        SignedAdmission<ManagementResponse>,
    > = serde_json::from_slice(&read_frame(&mut client).await.unwrap()).unwrap();
    received
        .verify(
            Some(PROXY_SECRET),
            RESPONSE_ROLE,
            crate::raft::types::test_replica(2),
            crate::raft::types::test_replica(1),
            client_binding,
        )
        .unwrap();
    assert_eq!(received.payload, reply.payload);
    assert_ne!(received.binding, upstream_binding);
    proxy.await.unwrap().unwrap();
}

#[tokio::test]
async fn proxy_rejects_unverified_management_responses() {
    for corrupt_binding in [false, true] {
        let (mut client, mut server, proxy, _, upstream_binding) = start_forwarded_proxy().await;
        let mut reply = management_reply(upstream_binding);
        if corrupt_binding {
            reply.binding[0] ^= 1;
        } else {
            reply.tag[0] ^= 1;
        }
        write_frame(
            &mut server,
            &serde_json::to_vec(&ManagementEnvelope::Control(reply)).unwrap(),
        )
        .await
        .unwrap();
        let error = proxy.await.unwrap().unwrap_err();
        assert!(error.to_string().contains("mismatch"), "{error}");
        assert_eq!(client.read(&mut [0; 1]).await.unwrap(), 0);
    }
}

#[tokio::test]
async fn proxy_rejects_a_signed_response_for_another_challenge() {
    let (mut client, mut server, proxy, _, upstream_binding) = start_forwarded_proxy().await;
    let mut payload = management_reply(upstream_binding).payload;
    payload.nonce[0] ^= 1;
    let reply = SignedAdmission::sign(
        Some(PROXY_SECRET),
        RESPONSE_ROLE,
        crate::raft::types::test_replica(2),
        crate::raft::types::test_replica(1),
        upstream_binding,
        payload,
    )
    .unwrap();
    write_frame(
        &mut server,
        &serde_json::to_vec(&ManagementEnvelope::Control(reply)).unwrap(),
    )
    .await
    .unwrap();
    let error = proxy.await.unwrap().unwrap_err();
    assert!(
        error.to_string().contains("upstream challenge changed"),
        "{error}"
    );
    assert_eq!(client.read(&mut [0; 1]).await.unwrap(), 0);
}

#[tokio::test]
async fn proxy_rejects_a_truncated_upstream_response() {
    let (_client, mut server, proxy, _, _) = start_forwarded_proxy().await;
    server.write_all(&[0, 0]).await.unwrap();
    server.shutdown().await.unwrap();
    let error = proxy.await.unwrap().unwrap_err();
    assert!(
        error.to_string().contains("read upstream proxy response"),
        "{error}"
    );
}

#[tokio::test]
async fn proxy_rejects_trailing_request_data() {
    let (mut client, _server, proxy, _, _) = start_forwarded_proxy().await;
    client.write_all(&[1]).await.unwrap();
    let error = proxy.await.unwrap().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("unexpected trailing proxy request"),
        "{error}"
    );
}

#[tokio::test]
async fn proxy_rejects_incomplete_authenticated_frames() {
    for reset in [false, true] {
        let (mut client, _upstream, proxy, _) = start_proxy().await;
        client.write_all(&[0, 0, 0, 10, b'{']).await.unwrap();
        if reset {
            client.set_zero_linger().unwrap();
        } else {
            client.shutdown().await.unwrap();
        }
        drop(client);
        let error = proxy.await.unwrap().unwrap_err();
        assert!(error.to_string().contains("read proxy request"), "{error}");
    }
}

#[tokio::test]
async fn proxy_rejects_malformed_authenticated_json() {
    let (mut client, _upstream, proxy, _) = start_proxy().await;
    write_frame(&mut client, b"{").await.unwrap();
    let error = proxy.await.unwrap().unwrap_err();
    assert!(error.is::<serde_json::Error>(), "{error}");
}

#[tokio::test]
async fn proxy_preserves_upstream_resets() {
    let (_client, server, proxy, _, _) = start_forwarded_proxy().await;
    server.set_zero_linger().unwrap();
    drop(server);
    let error = proxy.await.unwrap().unwrap_err();
    assert!(
        error.to_string().contains("read upstream proxy response"),
        "{error}"
    );
    assert_eq!(
        error.downcast_ref::<std::io::Error>().unwrap().kind(),
        std::io::ErrorKind::ConnectionReset,
    );
}

struct DisconnectingReplyWriter {
    written: Vec<u8>,
    error: std::io::ErrorKind,
}

impl tokio::io::AsyncWrite for DisconnectingReplyWriter {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        bytes: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        if self.written.len() == 4 {
            return std::task::Poll::Ready(Err(self.error.into()));
        }
        let count = bytes.len().min(4 - self.written.len());
        self.written.extend_from_slice(&bytes[..count]);
        std::task::Poll::Ready(Ok(count))
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn proxy_cancels_if_client_disconnects_during_response_write() {
    for error in [
        std::io::ErrorKind::BrokenPipe,
        std::io::ErrorKind::ConnectionReset,
    ] {
        let mut client = DisconnectingReplyWriter {
            written: Vec::new(),
            error,
        };
        let reply = br#"{"ok":true,"message":""}"#;
        write_proxy_response(&mut client, reply)
            .await
            .expect("disconnect after the response header must cancel the reply");
        assert_eq!(client.written, (reply.len() as u32).to_be_bytes());
    }
}

#[tokio::test]
async fn proxy_preserves_other_response_write_errors() {
    for error in [
        std::io::ErrorKind::PermissionDenied,
        std::io::ErrorKind::WriteZero,
        std::io::ErrorKind::TimedOut,
        std::io::ErrorKind::UnexpectedEof,
    ] {
        let mut client = DisconnectingReplyWriter {
            written: Vec::new(),
            error,
        };
        let error_returned = write_proxy_response(&mut client, b"reply")
            .await
            .unwrap_err();
        assert_eq!(
            error_returned
                .downcast_ref::<std::io::Error>()
                .unwrap()
                .kind(),
            error
        );
        assert!(error_returned.to_string().contains("write proxy response"));
    }
}

async fn notifications(log: &Path, vip: IpAddr) -> anyhow::Result<Vec<String>> {
    let prefix = format!("INSTANCE {vip} ");
    let content = match tokio::fs::read_to_string(log).await {
        Ok(content) => content,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(error) => return Err(error.into()),
    };
    Ok(content
        .lines()
        .filter_map(|line| line.strip_prefix(&prefix).map(str::to_owned))
        .collect())
}

async fn wait_notifications(log: &Path, vip: IpAddr, expected: &[String]) -> anyhow::Result<()> {
    let mut poll = tokio::time::interval(Duration::from_millis(10));
    loop {
        poll.tick().await;
        let states = notifications(log, vip).await?;
        anyhow::ensure!(
            expected.starts_with(&states),
            "unexpected notify sequence: {states:?}"
        );
        if states.len() == expected.len() {
            return Ok(());
        }
    }
}

async fn remove_fixtures(directory: &Path) -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(1), async {
        let mut poll = tokio::time::interval(Duration::from_millis(10));
        loop {
            poll.tick().await;
            match tokio::fs::remove_dir_all(directory).await {
                Ok(()) => return Ok(()),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
                // A detached notify hook may still open its log after daemon shutdown.
                Err(error) if error.kind() == std::io::ErrorKind::DirectoryNotEmpty => (),
                Err(error) => return Err(anyhow::Error::from(error)),
            }
        }
    })
    .await?
}

#[tokio::test(start_paused = true)]
async fn health_loop_characterization_notifies_master_then_fault() {
    let mut cluster = ClusterFixture::bind(1).await;
    let address = ip4(192, 0, 2, 97);
    let vips = vec![VipConfig {
        address: VipAddr::host(address),
        interface: "lo".into(),
        vlan: None,
    }];
    let directory = std::env::temp_dir().join(format!(
        "kafd-health-characterization-{}-{}",
        std::process::id(),
        cluster.config(0, &[]).raft_listen.replace(':', "-")
    ));
    tokio::fs::create_dir(&directory).await.unwrap();
    let flag = directory.join("healthy");
    let script = directory.join("notify.sh");
    tokio::fs::write(&flag, b"").await.unwrap();
    tokio::fs::write(
        &script,
        "#!/bin/sh\nprintf '%s %s %s\\n' \"$1\" \"$2\" \"$3\" >> \"$0.log\"\n",
    )
    .await
    .unwrap();
    tokio::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755))
        .await
        .unwrap();
    let mut cfg = (*cluster.config(0, &vips)).clone();
    cfg.health.command = vec![
        "/bin/sh".into(),
        "-c".into(),
        "test -f \"$1\"".into(),
        "health".into(),
        flag.to_str().unwrap().into(),
    ];
    cfg.notify = Some(script.to_str().unwrap().into());
    cfg.dry_run = false;
    let cfg = Arc::new(cfg);
    let budget = startup_budget(&cfg);
    let table = Arc::new(cfg.sorted_vips());
    let local = LocalVip::new(true);
    let (stop, stopped) = oneshot::channel();
    let daemon = tokio::spawn(run_with_listeners(
        cfg,
        table,
        local.clone(),
        async {
            let _ = stopped.await;
        },
        cluster.take_raft(0),
        cluster.take_submit(0),
    ));
    assert!(
        advance_until(budget, async || local
            .bound_addrs()
            .await
            .contains(&address))
        .await
    );
    tokio::time::resume();
    let log = directory.join("notify.sh.log");
    let result = tokio::time::timeout(Duration::from_secs(3), async {
        wait_notifications(&log, address, &["MASTER".into()])
            .await
            .unwrap();
        tokio::fs::remove_file(&flag).await.unwrap();
        wait_notifications(&log, address, &["MASTER".into(), "FAULT".into()])
            .await
            .unwrap();
        assert!(local.bound_addrs().await.is_empty());
        assert!(!daemon.is_finished());
    })
    .await;
    stop.send(()).unwrap();
    assert_eq!(join_daemon(daemon).await.unwrap(), None);
    remove_fixtures(&directory).await.unwrap();
    result.unwrap();
}

#[tokio::test(start_paused = true)]
async fn runtime_publisher_distinguishes_pending_ready_contention_and_shutdown() {
    use crate::health_publication::{PublicationReadiness, Publisher, RuntimePublisher};
    let mut cluster = ClusterFixture::bind(1).await;
    let cfg = cluster.config(0, &[]);
    let budget = startup_budget(&cfg);
    let (raft, network, state, _fatal, _network_failure, mut controls) = crate::raft::start_raft(
        cfg.clone(),
        Arc::new(cfg.sorted_vips()),
        cluster.take_raft(0),
    )
    .await
    .unwrap();
    let runtime = controls.runtime();
    let publisher = RuntimePublisher::new(runtime.clone(), network.clone());
    assert_eq!(
        publisher.readiness().await.unwrap(),
        PublicationReadiness::Pending
    );
    assert!(!publisher.activation_ready().await);
    assert!(
        advance_until(budget, async || matches!(
            publisher.readiness().await,
            Ok(PublicationReadiness::Ready)
        ))
        .await
    );
    assert!(
        !publisher.activation_ready().await,
        "publication admission must precede VIP activation"
    );

    let held = state.write().await;
    let ready = publisher.readiness();
    tokio::pin!(ready);
    assert!(
        futures::poll!(&mut ready).is_pending(),
        "state contention must wait, not revoke admission"
    );
    drop(held);
    assert_eq!(ready.await.unwrap(), PublicationReadiness::Ready);

    let held = state.write().await;
    let ready = publisher.readiness();
    tokio::pin!(ready);
    assert!(futures::poll!(&mut ready).is_pending());
    runtime.shutdown();
    assert!(
        ready.await.is_err(),
        "shutdown must interrupt the coherent read"
    );
    drop(held);
    assert!(
        publisher.readiness().await.is_err(),
        "a sealed runtime must not become pending"
    );
    controls.shutdown().await.unwrap();
    network.shutdown().await.unwrap();
    raft.shutdown().await.unwrap();
}

struct ControlledPublisher {
    runtime: crate::health_publication::RuntimePublisher,
    mode: std::sync::atomic::AtomicU8,
    blocked: Semaphore,
    events: mpsc::UnboundedSender<&'static str>,
    completed: AtomicU64,
}

#[tokio::test(start_paused = true)]
async fn daemon_terminal_admission_expiry_cleans_an_acquired_vip_without_rebinding() {
    let mut cluster = ClusterFixture::bind(1).await;
    let address = ip4(192, 0, 2, 96);
    let vips = [VipConfig {
        address: VipAddr::host(address),
        interface: "lo".into(),
        vlan: None,
    }];
    let cfg = cluster.config(0, &vips);
    let startup = startup_budget(&cfg);
    let timing = crate::runtime_permission::LeaseTiming::for_config(&cfg, vips.len()).unwrap();
    let local = LocalVip::new(true);
    let probe = ControlledProbe::new(true);
    let daemon = tokio::spawn(run_with_probe(
        cfg.clone(),
        Arc::new(cfg.sorted_vips()),
        local.clone(),
        std::future::pending(),
        cluster.take_raft(0),
        cluster.take_submit(0),
        probe.clone(),
    ));
    let acquired = advance_until(startup, async || {
        local.bound_addrs().await.contains(&address) && local.bind_attempts(address).await > 0
    })
    .await;
    if !acquired {
        daemon.abort();
        panic!(
            "daemon did not acquire VIP before expiry: {:?}",
            daemon.await
        );
    }
    assert!(
        !daemon.is_finished(),
        "daemon exited before admission expiry"
    );

    // One clock jump prevents renewal work from running between the old and expired deadlines.
    tokio::time::advance(timing.consumer_use() + Duration::from_millis(1)).await;
    let error = join_daemon(daemon)
        .await
        .expect_err("terminal admission expiry must fail the supervised daemon");
    let diagnostic = format!("{error:#}");
    assert!(
        diagnostic.contains("runtime admission task"),
        "{diagnostic}"
    );
    assert!(
        diagnostic.contains("expired")
            || diagnostic.contains("sealed")
            || diagnostic.contains("no active admission"),
        "daemon failed for a reason other than terminal admission: {diagnostic}"
    );
    assert!(
        local.bound_addrs().await.is_empty(),
        "supervisor left the VIP bound"
    );
    let attempts_after_cleanup = local.bind_attempts(address).await;
    assert!(
        probe.0.load(Ordering::SeqCst),
        "the service probe must stay healthy"
    );

    tokio::time::advance(timing.consumer_use() + crate::vip::RECONCILE_TICK * 2).await;
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    assert!(
        local.bound_addrs().await.is_empty(),
        "stopped daemon rebound the VIP"
    );
    assert_eq!(
        local.bind_attempts(address).await,
        attempts_after_cleanup,
        "a reconciliation worker survived terminal daemon cleanup"
    );
}

struct PendingPublisher {
    ready: std::sync::atomic::AtomicBool,
    attempts: AtomicU64,
}

impl crate::health_publication::Publisher for Arc<PendingPublisher> {
    async fn readiness(&self) -> anyhow::Result<crate::health_publication::PublicationReadiness> {
        use crate::health_publication::PublicationReadiness;
        Ok(if self.ready.load(Ordering::SeqCst) {
            PublicationReadiness::Ready
        } else {
            PublicationReadiness::Pending
        })
    }

    async fn publish(&self, _: bool) -> anyhow::Result<crate::health_publication::AppliedProbe> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        anyhow::bail!("actual publication failure");
    }

    async fn activation_ready(&self) -> bool {
        false
    }
}

#[tokio::test(start_paused = true)]
async fn pending_admission_probes_without_publishing_and_logs_real_errors() {
    let (logs, _guard) = crate::warning_limit::test_support::LogCapture::start(
        "keepafloatd::health_publication=warn",
    );
    let cluster = ClusterFixture::bind(1).await;
    let mut cfg = (*cluster.config(0, &[])).clone();
    cfg.health.command.clear();
    let local = Arc::new(crate::health::LocalHealth::new(true));
    let freshness = Arc::new(crate::consensus_freshness::ConsensusFreshness::new(
        Duration::from_secs(1),
    ));
    freshness.record_success(tokio::time::Instant::now());
    let publisher = Arc::new(PendingPublisher {
        ready: std::sync::atomic::AtomicBool::new(false),
        attempts: AtomicU64::new(0),
    });
    let task = tokio::spawn(crate::health_publication::run(
        Arc::new(cfg),
        local.clone(),
        freshness.clone(),
        publisher.clone(),
    ));
    assert!(
        advance_until(Duration::from_secs(1), async || !local.is_healthy()).await,
        "quarantine must keep probing local health"
    );
    assert_eq!(
        publisher.attempts.load(Ordering::SeqCst),
        0,
        "pending admission must not publish"
    );
    assert!(
        !freshness.is_fresh(),
        "pending admission cannot retain a usable proof"
    );
    assert!(
        !logs.text().contains("health raft submit"),
        "expected quarantine must not warn per probe"
    );
    publisher.ready.store(true, Ordering::SeqCst);
    assert!(
        advance_until(Duration::from_secs(1), async || publisher
            .attempts
            .load(Ordering::SeqCst)
            > 0)
        .await
    );
    assert!(
        logs.text().contains("actual publication failure"),
        "real publication errors must remain observable"
    );
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
}

impl crate::health_publication::Publisher for Arc<ControlledPublisher> {
    async fn readiness(&self) -> anyhow::Result<crate::health_publication::PublicationReadiness> {
        self.runtime.readiness().await
    }

    async fn publish(
        &self,
        healthy: bool,
    ) -> anyhow::Result<crate::health_publication::AppliedProbe> {
        match self.mode.load(Ordering::SeqCst) {
            1 => {
                anyhow::ensure!(healthy, "proof rejection must not publish unhealthy");
                self.events.send("rejected")?;
                anyhow::bail!("injected health publication rejection");
            }
            2 => {
                anyhow::ensure!(healthy, "blocked publication must remain healthy");
                let proof = self.runtime.publish(healthy).await?;
                self.events.send("blocked")?;
                self.blocked.acquire().await?.forget();
                self.events.send("completed")?;
                Ok(proof)
            }
            _ => {
                let proof = self.runtime.publish(healthy).await?;
                self.completed.fetch_add(1, Ordering::SeqCst);
                Ok(proof)
            }
        }
    }

    async fn activation_ready(&self) -> bool {
        self.runtime.activation_ready().await
    }
}

#[tokio::test(start_paused = true)]
async fn runtime_rejected_health_publication_releases_with_backup_and_recovers() {
    assert_runtime_health_fencing(false).await;
}

#[tokio::test(start_paused = true)]
async fn runtime_blocked_health_publication_fences_before_completion_and_recovers() {
    assert_runtime_health_fencing(true).await;
}

async fn assert_runtime_health_fencing(block_reply: bool) {
    let mut cluster = ClusterFixture::bind(1).await;
    let address = ip4(192, 0, 2, 98);
    let vips = vec![VipConfig {
        address: VipAddr::host(address),
        interface: "lo".into(),
        vlan: None,
    }];
    let directory = std::env::temp_dir().join(format!(
        "kafd-runtime-health-{}-{}",
        std::process::id(),
        cluster.config(0, &[]).raft_listen.replace(':', "-")
    ));
    tokio::fs::create_dir(&directory).await.unwrap();
    let script = directory.join("notify.sh");
    tokio::fs::write(
        &script,
        "#!/bin/sh\nprintf '%s %s %s\\n' \"$1\" \"$2\" \"$3\" >> \"$0.log\"\n",
    )
    .await
    .unwrap();
    tokio::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755))
        .await
        .unwrap();
    let mut cfg = (*cluster.config(0, &vips)).clone();
    cfg.health.stale_secs = Some(1);
    cfg.notify = Some(script.to_str().unwrap().into());
    cfg.dry_run = false;
    let cfg = Arc::new(cfg);
    let budget = startup_budget(&cfg);
    let table = Arc::new(cfg.sorted_vips());
    let local = LocalVip::new(true);
    let local_health = Arc::new(crate::health::LocalHealth::new(false));
    let freshness = Arc::new(
        crate::consensus_freshness::ConsensusFreshness::for_probe_cadence(
            cfg.health.interval_ms,
            cfg.health.effective_stale_missed_probes(),
        ),
    );
    let (raft, network, state, _fatal, _network_failure, mut controls) =
        crate::raft::start_raft(cfg.clone(), table.clone(), cluster.take_raft(0))
            .await
            .unwrap();
    let (events, mut received) = mpsc::unbounded_channel();
    let publisher = Arc::new(ControlledPublisher {
        runtime: crate::health_publication::RuntimePublisher::new(
            controls.runtime(),
            network.clone(),
        ),
        mode: std::sync::atomic::AtomicU8::new(0),
        blocked: Semaphore::new(0),
        events,
        completed: AtomicU64::new(0),
    });
    let probe = ControlledProbe::new(true);
    let health = tokio::spawn(crate::health_publication::run_with_probe(
        cfg.clone(),
        local_health.clone(),
        freshness.clone(),
        publisher.clone(),
        probe.clone(),
    ));
    let reconcile = tokio::spawn(crate::vip::run_reconcile_loop(
        cfg.clone(),
        raft.clone(),
        state.clone(),
        local.clone(),
        table.clone(),
        local_health.clone(),
        freshness.clone(),
        cfg.node_id,
    ));
    assert!(
        advance_until(budget, async || local
            .bound_addrs()
            .await
            .contains(&address))
        .await,
        "runtime health did not acquire VIP"
    );
    tokio::time::resume();
    let log = directory.join("notify.sh.log");
    let outcome = tokio::time::timeout(Duration::from_secs(8), async {
        wait_notifications(&log, address, &["MASTER".into()]).await?;
        publisher
            .mode
            .store(if block_reply { 2 } else { 1 }, Ordering::SeqCst);
        anyhow::ensure!(
            received.recv().await == Some(if block_reply { "blocked" } else { "rejected" })
        );
        let completed_before = publisher.completed.load(Ordering::SeqCst);
        wait_notifications(&log, address, &["MASTER".into(), "BACKUP".into()]).await?;
        anyhow::ensure!(
            local.bound_addrs().await.is_empty(),
            "VIP remained bound after proof loss"
        );
        anyhow::ensure!(local_health.is_healthy(), "proof loss changed local health");
        anyhow::ensure!(
            !freshness.is_fresh(),
            "rejection or expiry renewed freshness"
        );
        anyhow::ensure!(
            state.read().await.node_health.get(&cfg.node_id) == Some(&true),
            "proof loss published unhealthy"
        );
        anyhow::ensure!(
            !health.is_finished() && !reconcile.is_finished(),
            "fencing stopped a worker"
        );
        anyhow::ensure!(
            controls.runtime().health_publication_ready().await?
                == crate::health_publication::PublicationReadiness::Ready,
            "test lost runtime permission instead of health proof"
        );
        anyhow::ensure!(
            publisher.completed.load(Ordering::SeqCst) == completed_before,
            "fault injection allowed fresh publication"
        );
        if block_reply {
            anyhow::ensure!(
                received.try_recv().is_err(),
                "blocked reply completed before fencing"
            );
            publisher.mode.store(1, Ordering::SeqCst);
            publisher.blocked.add_permits(1);
            anyhow::ensure!(received.recv().await == Some("completed"));
            anyhow::ensure!(received.recv().await == Some("rejected"));
            anyhow::ensure!(
                !freshness.is_fresh(),
                "delayed proof manufactured new freshness"
            );
            anyhow::ensure!(
                local.bound_addrs().await.is_empty(),
                "delayed proof rebound VIP"
            );
        }
        publisher.mode.store(0, Ordering::SeqCst);
        wait_notifications(
            &log,
            address,
            &["MASTER".into(), "BACKUP".into(), "MASTER".into()],
        )
        .await?;
        anyhow::ensure!(local.bound_addrs().await.contains(&address));
        anyhow::ensure!(freshness.is_fresh());
        anyhow::ensure!(
            publisher.completed.load(Ordering::SeqCst) > completed_before,
            "recovery requires a new real quorum proof"
        );
        probe.set(false);
        wait_notifications(
            &log,
            address,
            &[
                "MASTER".into(),
                "BACKUP".into(),
                "MASTER".into(),
                "FAULT".into(),
            ],
        )
        .await?;
        anyhow::ensure!(local.bound_addrs().await.is_empty());
        anyhow::ensure!(!local_health.is_healthy());
        Ok::<(), anyhow::Error>(())
    })
    .await;
    health.abort();
    reconcile.abort();
    assert!(health.await.unwrap_err().is_cancelled());
    assert!(reconcile.await.unwrap_err().is_cancelled());
    local
        .unbind_all(
            &table,
            cfg.notify.as_deref(),
            cfg.dry_run,
            crate::vip::release_notify_state(local_health.is_healthy()),
        )
        .await
        .unwrap();
    local.shutdown_notifications().await.unwrap();
    controls.shutdown().await.unwrap();
    network.shutdown().await.unwrap();
    raft.shutdown().await.unwrap();
    remove_fixtures(&directory).await.unwrap();
    outcome
        .expect("runtime health regression timed out")
        .unwrap();
}
