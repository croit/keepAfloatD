//! Listener pressure must leave capacity for an admitted, signed VIP release.

use super::{ClusterFixture, advance_until, ip4, test_cluster_connect_to};
use crate::config::{Config, VipAddr, VipConfig};
use crate::raft::admission::{Genesis, ReplicaId};
use crate::raft::{KafRequest, TypeConfig};
use crate::submit::test_wire;
use openraft::alias::LogIdOf;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::oneshot;

async fn write_frame(stream: &mut TcpStream, body: &[u8]) -> anyhow::Result<()> {
    let mut frame = u32::try_from(body.len())?.to_be_bytes().to_vec();
    frame.extend_from_slice(body);
    stream.write_all(&frame).await?;
    Ok(())
}

async fn read_frame(stream: &mut TcpStream) -> anyhow::Result<Vec<u8>> {
    let (bytes, ()) = crate::frame::read(stream, 4096, |_| Ok(()))
        .await
        .map_err(crate::frame::ReadError::into_io)?;
    Ok(bytes)
}

async fn signed_release_submit(
    port: u16,
    cfg: &Config,
    replica: ReplicaId,
    genesis: Genesis,
    request: KafRequest,
    acknowledged: Option<oneshot::Sender<()>>,
) -> anyhow::Result<LogIdOf<TypeConfig>> {
    tokio::time::timeout(Duration::from_secs(1), async {
        let mut address: std::net::SocketAddr = cfg.raft_listen.parse()?;
        address.set_port(port);
        let mut stream = test_cluster_connect_to(&address.to_string()).await?;
        let authenticated = crate::auth::client_bound(
            &mut stream,
            crate::auth::Peer::for_replica(replica, Some(genesis.epoch), true),
            replica.physical_id,
            cfg.cluster_secret.as_deref(),
            crate::auth::Listener::Submit,
        )
        .await?;
        anyhow::ensure!(
            authenticated.peer.replica() == Some(replica),
            "submit responder changed its boot"
        );
        let bytes = test_wire::release(cfg, replica, authenticated.binding, genesis, request)?;
        write_frame(&mut stream, &bytes).await?;
        let response = read_frame(&mut stream).await?;
        let applied = test_wire::acknowledgement(cfg, replica, authenticated.binding, &response)?;
        if let Some(acknowledged) = acknowledged {
            acknowledged
                .send(())
                .map_err(|_| anyhow::anyhow!("acknowledgement observer disappeared"))?;
        }
        // EOF orders source-permit reuse after the server drops the request handler.
        let mut trailing = [0];
        anyhow::ensure!(
            stream.read(&mut trailing).await? == 0,
            "submit server sent data after its response"
        );
        Ok(applied)
    })
    .await?
}

fn release() -> KafRequest {
    KafRequest::VipReleased {
        node_id: 1,
        vip: ip4(192, 0, 2, 1),
        generation: 1,
    }
}

#[tokio::test]
async fn signed_release_submit_waits_for_server_close_after_verified_acknowledgement() {
    let cluster = ClusterFixture::bind(1).await;
    let cfg = cluster.config(0, &[]);
    let replica = ReplicaId::fresh(1).unwrap();
    let genesis = Genesis {
        config: cfg.cluster_config_fingerprint().unwrap(),
        epoch: 17,
        voters: [replica].into(),
    };
    let log_id = openraft::testing::log_id::<TypeConfig>(1, replica, 3);
    let address: std::net::SocketAddr = cfg.raft_listen.parse().unwrap();
    let listener = tokio::net::TcpListener::bind((address.ip(), 0))
        .await
        .unwrap();
    let port = listener.local_addr().unwrap().port();
    let (acknowledged_tx, acknowledged_rx) = oneshot::channel();
    let (close_tx, close_rx) = oneshot::channel();
    let server_cfg = cfg.clone();
    // Lifetime: the test closes the server and joins it before returning.
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let authenticated = crate::auth::server_bound(
            &mut stream,
            crate::auth::Peer::for_replica(replica, None, true),
            server_cfg.cluster_secret.as_deref(),
            crate::auth::Listener::Submit,
        )
        .await
        .unwrap();
        assert_eq!(authenticated.peer.replica(), Some(replica));
        let request = read_frame(&mut stream).await.unwrap();
        let reply = test_wire::acknowledge_request(
            &server_cfg,
            replica,
            authenticated.binding,
            &request,
            log_id,
        )
        .unwrap();
        write_frame(&mut stream, &reply).await.unwrap();
        close_rx.await.unwrap();
    });
    let submit = signed_release_submit(
        port,
        &cfg,
        replica,
        genesis,
        release(),
        Some(acknowledged_tx),
    );
    tokio::pin!(submit);
    tokio::select! {
        biased;
        result = &mut submit => panic!("submit finished before server closure: {result:?}"),
        result = acknowledged_rx => result.unwrap(),
    }
    assert!(
        futures::poll!(&mut submit).is_pending(),
        "a signed response alone must not prove slot reuse"
    );
    close_tx.send(()).unwrap();
    assert_eq!(submit.await.unwrap(), log_id);
    server.await.unwrap();
}

async fn assert_shed(mut stream: TcpStream, listener: &str) {
    let mut byte = [0];
    let result = tokio::time::timeout(Duration::from_secs(1), stream.read(&mut byte))
        .await
        .expect("excess source connection must be shed promptly");
    assert!(
        matches!(result, Err(_) | Ok(0)),
        "{listener} exceeded the eight-connection quota: {result:?}"
    );
}

async fn close_stalled(mut stream: TcpStream) {
    stream.shutdown().await.unwrap();
    let mut byte = [0];
    let result = tokio::time::timeout(Duration::from_secs(1), stream.read(&mut byte))
        .await
        .unwrap();
    assert!(
        matches!(result, Err(_) | Ok(0)),
        "server did not close a disconnected handshake: {result:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn submit_admission_sheds_saturation_and_recovers() {
    let mut cluster = ClusterFixture::bind(1).await;
    let ports = cluster.ports();
    let cfg = cluster.config(
        0,
        &[VipConfig {
            address: VipAddr::host(ip4(192, 0, 2, 1)),
            interface: "lo".into(),
            vlan: None,
        }],
    );
    let (raft, network, state, _, _, mut controls) = crate::raft::start_raft(
        cfg.clone(),
        Arc::new(cfg.sorted_vips()),
        cluster.take_raft(0),
    )
    .await
    .unwrap();
    let timing = crate::runtime_permission::LeaseTiming::for_config(&cfg, cfg.vips.len()).unwrap();
    assert!(
        advance_until(
            timing.restart_quarantine() + timing.consumer_use(),
            async || state.read().await.genesis.is_some()
        )
        .await
    );
    let runtime = controls.runtime();
    let replica = runtime.local_replica();
    let genesis = state.read().await.genesis.clone().unwrap();
    tokio::time::resume();
    let server = tokio::spawn(crate::submit::run_submit_server(
        cfg.clone(),
        raft.clone(),
        cluster.take_submit(0),
    ));

    let mut raft_stalled = Vec::new();
    for _ in 0..8 {
        raft_stalled.push(test_cluster_connect_to(&cfg.raft_listen).await.unwrap());
    }
    assert_shed(
        test_cluster_connect_to(&cfg.raft_listen).await.unwrap(),
        "Raft",
    )
    .await;
    for stream in raft_stalled {
        close_stalled(stream).await;
    }
    let mut recovered_raft = test_cluster_connect_to(&cfg.raft_listen).await.unwrap();
    let authenticated = tokio::time::timeout(
        Duration::from_secs(1),
        crate::auth::client_bound(
            &mut recovered_raft,
            crate::auth::Peer::for_replica(replica, Some(genesis.epoch), true),
            cfg.node_id,
            cfg.cluster_secret.as_deref(),
            crate::auth::Listener::Raft,
        ),
    )
    .await
    .unwrap()
    .expect("Raft capacity must recover after client disconnect");
    assert_eq!(authenticated.peer.replica(), Some(replica));
    close_stalled(recovered_raft).await;

    let mut stalled = Vec::new();
    for _ in 0..7 {
        stalled.push(
            test_cluster_connect_to(&cfg.client_submit_listen)
                .await
                .unwrap(),
        );
    }
    let applied = signed_release_submit(ports[1], &cfg, replica, genesis.clone(), release(), None)
        .await
        .expect("reserved slot must carry legitimate release traffic");
    assert_release_applied(&state, applied).await;
    stalled.push(
        test_cluster_connect_to(&cfg.client_submit_listen)
            .await
            .unwrap(),
    );
    assert_shed(
        test_cluster_connect_to(&cfg.client_submit_listen)
            .await
            .unwrap(),
        "submit",
    )
    .await;
    close_stalled(stalled.pop().unwrap()).await;
    let applied = signed_release_submit(ports[1], &cfg, replica, genesis, release(), None)
        .await
        .expect("submit capacity must recover after client disconnect");
    assert_release_applied(&state, applied).await;
    for stream in stalled {
        close_stalled(stream).await;
    }
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
    controls.shutdown().await.unwrap();
    network.shutdown().await.unwrap();
    raft.shutdown().await.unwrap();
}

async fn assert_release_applied(
    state: &tokio::sync::RwLock<crate::raft::KafStorageState>,
    log_id: LogIdOf<TypeConfig>,
) {
    let state = state.read().await;
    assert!(
        state
            .last_applied_log
            .is_some_and(|applied| applied.index >= log_id.index)
    );
    assert!(
        matches!(&state.log[&log_id.index].payload, openraft::EntryPayload::Normal(request) if *request == release())
    );
}
