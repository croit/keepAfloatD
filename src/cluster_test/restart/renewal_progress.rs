use super::*;

#[tokio::test(start_paused = true)]
async fn concurrent_healthy_publishers_keep_progress_applying() {
    let mut cluster = ClusterFixture::bind(3).await;
    let mut nodes = Vec::new();
    for index in 0..3 {
        let mut cfg = cluster.config(index, &[]);
        let tune = Arc::make_mut(&mut cfg);
        tune.health.interval_ms = 500;
        tune.health.stale_secs = Some(1);
        tune.raft.heartbeat_interval_ms = 250;
        nodes.push(Node::start(cfg, cluster.take_raft(index)).await);
    }
    let result = std::panic::AssertUnwindSafe(async {
        wait_stable(&mut nodes, None).await;
        for node in &nodes {
            publish_history_with_retry(node).await;
        }
        let publishers = nodes.iter().map(|node| async move {
            let runtime = node.controls.runtime();
            let id = node.cfg.node_id;
            let mut failures = Vec::new();
            for attempt in 0..12 {
                if let Err(error) = runtime.submit_health(&node.network, true).await {
                    failures.push(format!("node {id}, attempt {attempt}: {error:#}"));
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            failures
        });
        // Poll all publishers on the virtual runtime before advancing its clock.
        let mut publications = Box::pin(futures::future::join_all(publishers));
        let mut failures = Vec::new();
        let completed = advance_until(Duration::from_secs(50), async || {
            if let std::task::Poll::Ready(results) = futures::poll!(publications.as_mut()) {
                failures = results.into_iter().flatten().collect();
                true
            } else {
                false
            }
        })
        .await;
        drop(publications);
        assert!(
            completed,
            "ordinary concurrent health publication did not finish"
        );
        assert!(
            failures.is_empty(),
            "normal concurrent health failures: {failures:#?}"
        );
    })
    .catch_unwind()
    .await;
    for node in &mut nodes {
        node.stop().await;
    }
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test(start_paused = true)]
async fn healthy_follower_applies_progress_with_a_heartbeat_longer_than_rpc_budget() {
    let mut cluster = ClusterFixture::bind(3).await;
    let mut nodes = Vec::new();
    for index in 0..3 {
        let mut cfg = cluster.config(index, &[]);
        let tune = Arc::make_mut(&mut cfg);
        tune.health.interval_ms = 500;
        tune.health.stale_secs = Some(1);
        tune.raft.heartbeat_interval_ms = 250;
        nodes.push(Node::start(cfg, cluster.take_raft(index)).await);
    }
    let result = std::panic::AssertUnwindSafe(async {
        wait_stable(&mut nodes, None).await;
        for node in &nodes {
            publish_history_with_retry(node).await;
        }
        let leader = nodes[0].raft.metrics().borrow_watched().current_leader.unwrap();
        let follower = nodes.iter().find(|node| node.replica() != leader).unwrap();
        let leader_node = nodes.iter().find(|node| node.replica() == leader).unwrap();
        let timing = crate::runtime_permission::LeaseTiming::for_config(&follower.cfg, 0).unwrap();
        assert!(timing.rpc_budget() < Duration::from_millis(250));
        let mut failures = Vec::new();
        let mut successes = 0;
        for attempt in 0..12 {
            let runtime = follower.controls.runtime();
            let started = Instant::now();
            let mut submit = Box::pin(runtime.submit_health(&follower.network, true));
            let mut outcome = None;
            assert!(advance_until(timing.renewal_round_budget() * 2, async || {
                if let std::task::Poll::Ready(result) = futures::poll!(submit.as_mut()) {
                    outcome = Some(result);
                    true
                } else {
                    false
                }
            }).await, "ordinary health renewal exceeded its round bounds");
            match outcome.unwrap() {
                Ok(_) => successes += 1,
                Err(error) => {
                    let local = follower.state.read().await;
                    let remote = leader_node.state.read().await;
                    failures.push(format!(
                        "attempt {attempt}, elapsed {:?}, error {error:#}, local applied {:?}, leader applied {:?}, local progress {:?}, leader progress {:?}",
                        started.elapsed(), local.last_applied_log, remote.last_applied_log,
                        local.applied_progress.get(&follower.cfg.node_id),
                        remote.applied_progress.get(&follower.cfg.node_id),
                    ));
                }
            }
            advance_until(Duration::from_millis(500), async || false).await;
        }
        assert!(
            failures.is_empty(),
            "healthy follower renewed {successes}/12 times without injected faults: {failures:#?}"
        );
    }).catch_unwind().await;
    for node in &mut nodes {
        node.stop().await;
    }
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}
