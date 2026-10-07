//! The shared handoff deadline also bounds a health mutation awaiting commit.

use super::*;
use crate::listener::ListenerSource;
use crate::raft::KafRequest;
use std::time::Duration;
use tokio::net::TcpListener;

#[tokio::test(start_paused = true)]
async fn pending_health_commit_cannot_outlive_the_shared_handoff_timeout() {
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
    let (raft, network, state, _, _, mut controls) = crate::raft::start_raft(
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
