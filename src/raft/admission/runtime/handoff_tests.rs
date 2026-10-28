//! The shared handoff deadline also bounds a health mutation awaiting commit.

use super::*;
use crate::listener::ListenerSource;
use crate::raft::KafRequest;
use std::time::Duration;
use tokio::net::TcpListener;

struct HandoffCluster {
    cfg: Arc<Config>,
    raft: KafRaft,
    network: Arc<RaftNetworkImpl>,
    state: Arc<RwLock<KafStorageState>>,
    controls: crate::raft::RaftControlTasks,
}

impl HandoffCluster {
    async fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let submit_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut cfg = (*super::tests::config()).clone();
        cfg.raft_listen = listener.local_addr().unwrap().to_string();
        cfg.peers[0].raft_address = cfg.raft_listen.clone();
        cfg.client_submit_listen = submit_listener.local_addr().unwrap().to_string();
        cfg.peers[0].client_submit_address = cfg.client_submit_listen.clone();
        cfg.submit_timeout_ms = 50;
        let cfg = Arc::new(cfg);
        let timing = LeaseTiming::for_config(&cfg, 0).unwrap();
        let (raft, network, state, _, _, controls) = crate::raft::start_raft(
            cfg.clone(),
            Arc::new(Vec::new()),
            ListenerSource::Bound(listener),
        )
        .await
        .unwrap();
        let runtime = controls.runtime();
        let startup_deadline = Instant::now() + timing.restart_quarantine() + timing.consumer_use();
        loop {
            for _ in 0..10 {
                tokio::task::yield_now().await;
            }
            if state.read().await.genesis.is_some() && runtime.current().is_some() {
                break;
            }
            assert!(Instant::now() < startup_deadline, "genesis was not applied");
            tokio::time::advance(Duration::from_millis(10)).await;
        }
        runtime.submit_health(&network, true).await.unwrap();
        Self {
            cfg,
            raft,
            network,
            state,
            controls,
        }
    }
}

#[tokio::test(start_paused = true)]
async fn pending_health_commit_cannot_outlive_the_shared_handoff_timeout() {
    let HandoffCluster {
        cfg,
        raft,
        network,
        state,
        mut controls,
    } = HandoffCluster::new().await;
    let runtime = controls.runtime();
    let session = runtime.current().unwrap();
    let applied_before = state.read().await.last_applied_log;

    // Exclude a concurrent renewal before holding only the real mutation barrier.
    let round = runtime.round.lock().await;
    let mutation = runtime.mutation.lock().await;
    drop(round);
    assert!(runtime.current().is_some());
    let started = Instant::now();
    assert!(session.check().unwrap() > started + Duration::from_millis(50));
    let mut publish = Box::pin(crate::handoff::publish(
        &cfg, &raft, &state, &runtime, &network,
    ));
    assert!(futures::poll!(publish.as_mut()).is_pending());
    assert!(
        runtime.round.try_lock().is_err(),
        "handoff did not enter health renewal"
    );
    assert_eq!(Instant::now(), started);
    tokio::time::advance(Duration::from_millis(49)).await;
    assert!(futures::poll!(publish.as_mut()).is_pending());
    tokio::time::advance(Duration::from_millis(1)).await;
    let result = publish.await;
    assert_eq!(Instant::now() - started, Duration::from_millis(50));
    assert!(format!("{:#}", result.unwrap_err()).contains("exceeded the shared submit timeout"));
    assert!(
        runtime.current().is_some(),
        "the admission deadline did not cause the timeout"
    );
    assert_eq!(state.read().await.last_applied_log, applied_before);
    assert_eq!(
        state.read().await.node_health.get(&cfg.node_id),
        Some(&true)
    );
    drop(mutation);

    // A subsequent applied report orders the check after cancellation of the blocked handoff.
    runtime.submit_health(&network, true).await.unwrap();
    {
        let state = state.read().await;
        assert_eq!(state.node_health.get(&cfg.node_id), Some(&true));
        assert!(
            !state.log.values().any(|entry| matches!(
                &entry.payload,
                openraft::EntryPayload::Normal(KafRequest::HealthProgress(progress))
                    if progress.healthy == Some(false)
            )),
            "timed-out handoff committed unhealthy progress"
        );
    }
    controls.shutdown().await.unwrap();
    network.shutdown().await.unwrap();
    raft.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn busy_state_waits_and_completes_handoff() {
    let HandoffCluster {
        cfg,
        raft,
        network,
        state,
        mut controls,
    } = HandoffCluster::new().await;
    let runtime = controls.runtime();
    let held = state.write().await;
    assert!(runtime.authority.current().is_some());
    assert!(
        runtime.current().is_none(),
        "the synchronous read cannot acquire the busy state"
    );
    let mut publish = Box::pin(crate::handoff::publish(
        &cfg, &raft, &state, &runtime, &network,
    ));
    assert!(
        futures::poll!(publish.as_mut()).is_pending(),
        "live admission must wait for the busy state"
    );
    tokio::time::advance(Duration::from_millis(10)).await;
    drop(held);
    publish.await.unwrap();
    assert_eq!(
        state.read().await.node_health.get(&cfg.node_id),
        Some(&false)
    );
    controls.shutdown().await.unwrap();
    network.shutdown().await.unwrap();
    raft.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn busy_state_acquisition_cannot_outlive_the_shared_handoff_timeout() {
    let HandoffCluster {
        cfg,
        raft,
        network,
        state,
        mut controls,
    } = HandoffCluster::new().await;
    let runtime = controls.runtime();
    let held = state.write().await;
    let applied_before = held.last_applied_log;
    let started = Instant::now();
    assert!(runtime.authority.current().unwrap().1 > started + Duration::from_millis(50));
    let mut publish = Box::pin(crate::handoff::publish(
        &cfg, &raft, &state, &runtime, &network,
    ));
    assert!(
        futures::poll!(publish.as_mut()).is_pending(),
        "live admission must wait for the busy state"
    );
    tokio::time::advance(Duration::from_millis(49)).await;
    assert!(futures::poll!(publish.as_mut()).is_pending());
    tokio::time::advance(Duration::from_millis(1)).await;
    assert!(
        format!("{:#}", publish.await.unwrap_err()).contains("exceeded the shared submit timeout")
    );
    assert_eq!(Instant::now() - started, Duration::from_millis(50));
    assert!(runtime.authority.current().is_some());
    assert_eq!(held.last_applied_log, applied_before);
    assert_eq!(held.node_health.get(&cfg.node_id), Some(&true));
    drop(held);
    controls.shutdown().await.unwrap();
    network.shutdown().await.unwrap();
    raft.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn state_acquisition_does_not_restart_the_shared_handoff_timeout() {
    let HandoffCluster {
        cfg,
        raft,
        network,
        state,
        mut controls,
    } = HandoffCluster::new().await;
    let runtime = controls.runtime();
    let round = runtime.round.lock().await;
    let mutation = runtime.mutation.lock().await;
    drop(round);
    let held = state.write().await;
    let started = Instant::now();
    let mut publish = Box::pin(crate::handoff::publish(
        &cfg, &raft, &state, &runtime, &network,
    ));
    assert!(
        futures::poll!(publish.as_mut()).is_pending(),
        "live admission must wait for the busy state"
    );
    tokio::time::advance(Duration::from_millis(30)).await;
    drop(held);
    assert!(futures::poll!(publish.as_mut()).is_pending());
    assert!(
        runtime.round.try_lock().is_err(),
        "handoff did not enter health renewal"
    );
    tokio::time::advance(Duration::from_millis(19)).await;
    assert!(futures::poll!(publish.as_mut()).is_pending());
    tokio::time::advance(Duration::from_millis(1)).await;
    assert!(
        format!("{:#}", publish.await.unwrap_err()).contains("exceeded the shared submit timeout")
    );
    assert_eq!(Instant::now() - started, Duration::from_millis(50));
    assert_eq!(
        state.read().await.node_health.get(&cfg.node_id),
        Some(&true)
    );
    drop(mutation);
    runtime.submit_health(&network, true).await.unwrap();
    assert!(!state.read().await.log.values().any(|entry| matches!(
        &entry.payload,
        openraft::EntryPayload::Normal(KafRequest::HealthProgress(progress))
            if progress.healthy == Some(false)
    )));
    controls.shutdown().await.unwrap();
    network.shutdown().await.unwrap();
    raft.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn admission_shutdown_interrupts_busy_state_handoff() {
    let HandoffCluster {
        cfg,
        raft,
        network,
        state,
        mut controls,
    } = HandoffCluster::new().await;
    let runtime = controls.runtime();
    let held = state.write().await;
    let mut publish = Box::pin(crate::handoff::publish(
        &cfg, &raft, &state, &runtime, &network,
    ));
    assert!(
        futures::poll!(publish.as_mut()).is_pending(),
        "live admission must wait for the busy state"
    );
    let stopped = Instant::now();
    runtime.shutdown();
    let std::task::Poll::Ready(result) = futures::poll!(publish.as_mut()) else {
        panic!("revoked handoff still waits for the state lock");
    };
    assert!(format!("{:#}", result.unwrap_err()).contains("active admission"));
    assert_eq!(Instant::now(), stopped);
    assert_eq!(held.node_health.get(&cfg.node_id), Some(&true));
    drop(held);
    drop(publish);
    controls.shutdown().await.unwrap();
    network.shutdown().await.unwrap();
    raft.shutdown().await.unwrap();
}
