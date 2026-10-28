use super::*;

async fn nodes_with_wrong_keys(first_wrong: usize) -> Vec<Node> {
    let mut cluster = ClusterFixture::bind(3).await;
    let mut nodes = Vec::new();
    for index in 0..3 {
        let mut cfg = cluster.config(index, &[]);
        if index >= first_wrong {
            Arc::make_mut(&mut cfg).cluster_secret =
                Some(format!("wrong-key-for-test-node-{index}-0123456789"));
        }
        nodes.push(Node::start(cfg, cluster.take_raft(index)).await);
    }
    nodes
}

#[tokio::test(start_paused = true)]
async fn wrong_key_minority_cannot_block_majority_health_progress() {
    let mut nodes = nodes_with_wrong_keys(2).await;
    let result = std::panic::AssertUnwindSafe(async {
        let genesis = wait_stable(&mut nodes[..2], None).await;
        for node in &nodes[..2] {
            publish_history_with_retry(node).await;
            let state = node.state.read().await;
            let progress = &state.applied_progress[&node.cfg.node_id];
            assert_eq!(progress.request.healthy, Some(true));
            assert_eq!(progress.request.replica, node.replica());
            assert_eq!(progress.request.genesis, genesis);
            assert!(!state.applied_progress.contains_key(&nodes[2].cfg.node_id));
        }
        survive_renewal(&mut nodes[..2], &genesis).await;
        assert!(nodes[2].controls.runtime().current().is_none());
        assert!(nodes[2].state.read().await.admission.is_none());
        assert!(nodes[2].state.read().await.applied_progress.is_empty());
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
async fn mismatched_keys_cannot_manufacture_a_physical_majority() {
    let mut nodes = nodes_with_wrong_keys(1).await;
    let result = std::panic::AssertUnwindSafe(async {
        let formed = advance_until(startup_budget(&nodes[0].cfg), async || {
            nodes
                .iter()
                .any(|node| node.controls.runtime().current().is_some())
        })
        .await;
        assert!(!formed, "one authenticated node cannot form a quorum");
        for node in &nodes {
            assert!(
                node.controls
                    .runtime()
                    .submit_health(&node.network, true)
                    .await
                    .is_err()
            );
            let state = node.state.read().await;
            assert!(state.admission.is_none());
            assert!(state.applied_progress.is_empty());
        }
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
async fn wrong_key_peer_never_binds_while_majority_serves_and_cleans_up() {
    let mut cluster = ClusterFixture::bind(3).await;
    let vip = VipConfig {
        address: VipAddr::host("192.0.2.19".parse().unwrap()),
        interface: "lo".into(),
        vlan: None,
    };
    let mut locals = Vec::new();
    let mut shutdowns = Vec::new();
    let mut handles = Vec::new();
    let mut budget = Duration::ZERO;
    for index in 0..3 {
        let mut cfg = cluster.config(index, std::slice::from_ref(&vip));
        let tune = Arc::make_mut(&mut cfg);
        tune.health.interval_ms = 1_000;
        tune.health.stale_secs = Some(3);
        if index == 2 {
            tune.cluster_secret = Some("wrong-daemon-key-fixture-0123456789".into());
        }
        budget = budget.max(startup_budget(&cfg));
        let local = LocalVip::new(true);
        let (tx, rx) = oneshot::channel::<()>();
        handles.push(tokio::spawn(run_with_probe(
            cfg.clone(),
            Arc::new(cfg.sorted_vips()),
            local.clone(),
            async move {
                let _ = rx.await;
            },
            cluster.take_raft(index),
            cluster.take_submit(index),
            ControlledProbe::new(true),
        )));
        locals.push(local);
        shutdowns.push(tx);
    }
    let result = std::panic::AssertUnwindSafe(async {
        let served = advance_until(budget, async || {
            assert!(handles.iter().all(|handle| !handle.is_finished()));
            assert!(locals[2].bound_addrs().await.is_empty());
            let mut addresses = locals[0].bound_addrs().await;
            addresses.extend(locals[1].bound_addrs().await);
            assert!(addresses.len() <= 1, "VIP must never have two holders");
            addresses == [vip.address.addr]
        })
        .await;
        assert!(
            served,
            "authenticated majority must serve the configured VIP"
        );
    })
    .catch_unwind()
    .await;
    for stop in shutdowns {
        stop.send(()).unwrap();
    }
    for handle in handles {
        join_daemon(handle).await.unwrap();
    }
    for local in locals {
        assert!(local.bound_addrs().await.is_empty());
    }
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}
