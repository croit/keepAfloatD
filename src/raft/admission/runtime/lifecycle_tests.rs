use super::*;
use crate::raft::store::new_store;
use crate::raft::tasks::{CleanExit, spawn_supervised_task, stop_supervised_tasks};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

async fn attached_driver() -> (Arc<RuntimeDriver>, Arc<RaftNetworkImpl>, KafRaft) {
    let cfg = super::tests::config();
    let (log, machine, state) = new_store(Arc::new(vec![]), 3, true, 0);
    let runtime = RuntimeDriver::new(
        cfg.clone(),
        state.clone(),
        LeaseTiming::new(Duration::from_secs(5), Duration::from_secs(1)).unwrap(),
        Instant::now(),
    )
    .unwrap();
    let network = Arc::new(RaftNetworkImpl::new(cfg, state, runtime.clone()).unwrap());
    let raft = KafRaft::new(
        runtime.local_replica(),
        Arc::new(openraft::Config {
            enable_tick: false,
            ..Default::default()
        }),
        network.as_ref().clone(),
        log,
        machine,
    )
    .await
    .unwrap();
    runtime.attach(raft.clone()).unwrap();
    (runtime, network, raft)
}

async fn assert_stopped_without_admission(runtime: &RuntimeDriver, raft: &KafRaft) {
    assert!(runtime.stopped.load(Ordering::SeqCst));
    assert!(runtime.raft().is_err());
    assert!(runtime.current().is_none());
    assert!(runtime.authorize_raft(runtime.local_replica()).is_err());
    assert!(futures::poll!(Box::pin(runtime.authority.wait_until_sealed())).is_ready());
    {
        let state = runtime.state.read().await;
        assert!(state.admission.is_none());
        assert!(state.genesis.is_none());
        assert!(state.last_applied_log.is_none());
    }
    assert!(runtime.first_verified_admission.lock().unwrap().is_none());
    assert!(!raft.is_initialized().await.unwrap());
    assert!(runtime.attach(raft.clone()).is_err());
}

#[tokio::test(start_paused = true)]
async fn supervised_cancellation_seals_and_detaches_quarantined_driver() {
    let (runtime, network, raft) = attached_driver().await;
    let (failure_tx, mut failure_rx) = mpsc::unbounded_channel();
    let (started_tx, started_rx) = oneshot::channel();
    let running = runtime.clone();
    let transport = network.clone();
    let task = spawn_supervised_task(
        "admission runtime",
        Arc::new(AtomicBool::new(false)),
        failure_tx,
        CleanExit::Unexpected,
        async move {
            let mut run = Box::pin(running.run(transport));
            assert!(futures::poll!(run.as_mut()).is_pending());
            started_tx.send(()).unwrap();
            run.await
        },
    );
    started_rx.await.unwrap();
    assert_eq!(Instant::now(), runtime.boot);
    assert!(!runtime.stopped.load(Ordering::SeqCst));
    stop_supervised_tasks(vec![task], Duration::from_millis(100))
        .await
        .unwrap();
    assert_eq!(Instant::now(), runtime.boot);
    assert_stopped_without_admission(&runtime, &raft).await;
    assert!(failure_rx.try_recv().is_err());
    network.shutdown().await.unwrap();
    raft.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn supervised_shutdown_finishes_cleanly_before_quarantine_ends() {
    let (runtime, network, raft) = attached_driver().await;
    let (failure_tx, mut failure_rx) = mpsc::unbounded_channel();
    let (started_tx, started_rx) = oneshot::channel();
    let shutdown = Arc::new(AtomicBool::new(false));
    let running = runtime.clone();
    let transport = network.clone();
    let task = spawn_supervised_task(
        "admission runtime",
        shutdown.clone(),
        failure_tx,
        CleanExit::Unexpected,
        async move {
            let mut run = Box::pin(running.run(transport));
            assert!(futures::poll!(run.as_mut()).is_pending());
            started_tx.send(()).unwrap();
            run.await
        },
    );
    started_rx.await.unwrap();
    shutdown.store(true, Ordering::SeqCst);
    runtime.shutdown();
    task.handle.await.unwrap().unwrap();
    assert_eq!(Instant::now(), runtime.boot);
    assert_stopped_without_admission(&runtime, &raft).await;
    assert!(failure_rx.try_recv().is_err());
    network.shutdown().await.unwrap();
    raft.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn network_shutdown_stops_driver_before_quarantine_ends() {
    let (runtime, network, raft) = attached_driver().await;
    let mut run = Box::pin(runtime.run(network.clone()));
    assert!(futures::poll!(run.as_mut()).is_pending());
    network.shutdown().await.unwrap();
    assert!(network.is_shutting_down());
    let completion = futures::poll!(run.as_mut());
    drop(run);
    assert_eq!(Instant::now(), runtime.boot);
    assert_stopped_without_admission(&runtime, &raft).await;
    raft.shutdown().await.unwrap();
    assert!(
        matches!(completion, std::task::Poll::Ready(Ok(()))),
        "network shutdown must stop the quarantined admission driver cleanly: {completion:?}"
    );
}
