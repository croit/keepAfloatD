use super::*;
use crate::auth::{Listener, Peer};
use crate::raft::probe::{ClusterStatusRequest, ClusterStatusResponse};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;

struct ForeignPeer {
    stop: oneshot::Sender<()>,
    task: JoinHandle<()>,
    replies: Arc<[AtomicUsize; 2]>,
}

impl ForeignPeer {
    async fn start(cluster: &mut ClusterFixture) -> Self {
        let cfg = cluster.config(2, &[]);
        let listener = cluster
            .take_raft(2)
            .bind(cfg.raft_listen.parse().unwrap())
            .await
            .unwrap();
        let (stop, stopped) = oneshot::channel();
        let replies = Arc::new([AtomicUsize::new(0), AtomicUsize::new(0)]);
        let observed = replies.clone();
        // The fixture stays reachable until explicitly stopped and owns every connection task.
        let task = tokio::spawn(serve_foreign_peer(listener, cfg, observed, stopped));
        Self {
            stop,
            task,
            replies,
        }
    }

    async fn stop(self) {
        self.stop.send(()).unwrap();
        self.task.await.unwrap();
    }
}

async fn serve_foreign_peer(
    listener: TcpListener,
    cfg: Arc<Config>,
    replies: Arc<[AtomicUsize; 2]>,
    mut stop: oneshot::Receiver<()>,
) {
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            _ = &mut stop => break,
            completed = connections.join_next(), if !connections.is_empty() => {
                completed.unwrap().unwrap();
            }
            accepted = listener.accept() => {
                let (stream, _) = accepted.unwrap();
                let cfg = cfg.clone();
                let replies = replies.clone();
                connections.spawn(async move {
                    answer_foreign_status(stream, cfg, replies).await;
                });
            }
        }
    }
    connections.abort_all();
    while let Some(completed) = connections.join_next().await {
        if let Err(error) = completed {
            assert!(error.is_cancelled(), "foreign fixture failed: {error}");
        }
    }
}

async fn answer_foreign_status(
    mut stream: TcpStream,
    cfg: Arc<Config>,
    replies: Arc<[AtomicUsize; 2]>,
) {
    let peer = crate::raft::types::test_replica(cfg.node_id);
    let authenticated = crate::auth::server_bound(
        &mut stream,
        Peer::for_replica(peer, None, true),
        cfg.cluster_secret.as_deref(),
        Listener::Raft,
    )
    .await
    .unwrap();
    let (bytes, ()) = crate::frame::read(&mut stream, 64 * 1024, |_| Ok(()))
        .await
        .unwrap();
    let envelope: BTreeMap<String, ClusterStatusRequest> =
        serde_json::from_slice(&bytes).expect("foreign peer must receive only status discovery");
    assert_eq!(envelope.len(), 1);
    let request = &envelope["status"];
    assert_eq!(request.probe_from, authenticated.peer.id);
    let mut foreign = cfg.cluster_config_fingerprint().unwrap();
    foreign.digest[0] ^= 1;
    let response = ClusterStatusResponse {
        config_fingerprint: Some(foreign),
        supports_config_identity_v1: true,
        ..Default::default()
    };
    let body = serde_json::to_vec(&BTreeMap::from([("status", response)])).unwrap();
    stream.write_u32(body.len() as u32).await.unwrap();
    stream.write_all(&body).await.unwrap();
    replies[request.probe_from as usize - 1].fetch_add(1, Ordering::SeqCst);
}

#[tokio::test(start_paused = true)]
async fn reachable_foreign_config_minority_cannot_veto_majority_health_progress() {
    let mut cluster = ClusterFixture::bind(3).await;
    let foreign = ForeignPeer::start(&mut cluster).await;
    let mut nodes = Vec::new();
    for index in 0..2 {
        nodes.push(Node::start(cluster.config(index, &[]), cluster.take_raft(index)).await);
    }
    let result = std::panic::AssertUnwindSafe(async {
        let genesis = wait_stable(&mut nodes, None).await;
        assert_eq!(genesis.voters, nodes.iter().map(Node::replica).collect());
        eprintln!("matching majority formed with a reachable foreign-config peer");
        let mut previous_logs = Vec::new();
        for node in &nodes {
            previous_logs.push(publish_history(node).await);
        }
        survive_renewal(&mut nodes, &genesis).await;
        for (index, node) in nodes.iter().enumerate() {
            let before = foreign.replies[index].load(Ordering::SeqCst);
            let log = publish_history(node).await;
            assert!(log.index > previous_logs[index].index);
            assert!(foreign.replies[index].load(Ordering::SeqCst) > before);
            let state = node.state.read().await;
            let progress = &state.applied_progress[&node.cfg.node_id];
            assert_eq!(progress.request.healthy, Some(true));
            assert_eq!(progress.request.replica, node.replica());
            assert_eq!(progress.request.genesis, genesis);
            assert!(!state.applied_progress.contains_key(&3));
            assert!(!foreign.task.is_finished());
        }
    })
    .catch_unwind()
    .await;
    for node in &mut nodes {
        node.stop().await;
    }
    foreign.stop().await;
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test(start_paused = true)]
async fn foreign_config_peer_cannot_supply_the_missing_matching_quorum() {
    let mut cluster = ClusterFixture::bind(3).await;
    let foreign = ForeignPeer::start(&mut cluster).await;
    drop(cluster.take_raft(1));
    let mut node = Node::start(cluster.config(0, &[]), cluster.take_raft(0)).await;
    let result = std::panic::AssertUnwindSafe(async {
        let admitted = advance_until(startup_budget(&node.cfg), async || {
            node.assert_running();
            node.controls.runtime().current().is_some()
        })
        .await;
        assert!(!admitted, "a foreign-config candidate cannot supply quorum");
        assert!(
            node.controls
                .runtime()
                .submit_health(&node.network, true)
                .await
                .is_err()
        );
        let state = node.state.read().await;
        assert!(state.genesis.is_none());
        assert!(state.admission.is_none());
        assert!(state.applied_progress.is_empty());
        assert!(state.vip_assignments.is_empty());
        assert!(foreign.replies[0].load(Ordering::SeqCst) > 0);
        assert!(!foreign.task.is_finished());
    })
    .catch_unwind()
    .await;
    node.stop().await;
    foreign.stop().await;
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}
