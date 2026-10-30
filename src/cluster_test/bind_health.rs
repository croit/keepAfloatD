use super::*;
use std::os::unix::process::ExitStatusExt;
use std::process::{ExitStatus, Output};

fn empty_inventory() -> std::io::Result<Output> {
    Ok(Output {
        status: ExitStatus::from_raw(0),
        stdout: b"[]".to_vec(),
        stderr: Vec::new(),
    })
}

struct BindFaultCluster {
    addr: IpAddr,
    budget: Duration,
    locals: Vec<Arc<LocalVip>>,
    probes: Vec<ControlledProbe>,
    stops: Vec<oneshot::Sender<()>>,
    handles: Vec<JoinHandle<anyhow::Result<Option<crate::raft::FatalReason>>>>,
}

/// Three daemons where only node 0 starts healthy, so placement picks it, and its first bind
/// returns `first_bind`.
async fn start_bind_fault_cluster(first_bind: std::io::Result<ExitStatus>) -> BindFaultCluster {
    let mut cluster = ClusterFixture::bind(3).await;
    let addr = ip4(192, 0, 2, 99);
    let vips = [VipConfig {
        address: VipAddr::host(addr),
        interface: "lo".into(),
        vlan: None,
    }];
    let mut first_bind = Some(first_bind);
    let mut started = BindFaultCluster {
        addr,
        budget: Duration::ZERO,
        locals: Vec::new(),
        probes: Vec::new(),
        stops: Vec::new(),
        handles: Vec::new(),
    };
    for index in 0..3 {
        let mut cfg = (*cluster.config(index, &vips)).clone();
        cfg.failover_delay_secs = 30;
        cfg.health.interval_ms = 1_000;
        // Stale expiry must not satisfy the four-second bind-fault takeover check.
        cfg.health.stale_secs = Some(10);
        started.budget = started.budget.max(startup_budget(&cfg));
        let probe = ControlledProbe::new(index == 0);
        let cfg = Arc::new(cfg);
        let local = LocalVip::new(index != 0);
        if let Some(result) = first_bind.take().filter(|_| index == 0) {
            local
                .force_startup_discovery_result(empty_inventory())
                .await;
            local
                .force_startup_marker_discovery_results(empty_inventory(), empty_inventory())
                .await;
            local
                .force_next_startup_cleanup_results(
                    ("lo", addr, 32),
                    Ok(ExitStatus::from_raw(0)),
                    None,
                )
                .await;
            local.force_next_bind_result(("lo", addr, 32), result).await;
        }
        let (stop, stopped) = oneshot::channel();
        // Every daemon is stopped and joined before the calling test returns.
        started.handles.push(tokio::spawn(run_with_probe(
            cfg.clone(),
            Arc::new(cfg.sorted_vips()),
            local.clone(),
            async {
                let _ = stopped.await;
            },
            cluster.take_raft(index),
            cluster.take_submit(index),
            probe.clone(),
        )));
        started.probes.push(probe);
        started.locals.push(local);
        started.stops.push(stop);
    }
    started
}

impl BindFaultCluster {
    async fn wait_for_failed_bind_and_peer_takeover(&self) {
        let addr = self.addr;
        assert!(
            advance_until(self.budget, async || self.locals[0]
                .bind_attempts(addr)
                .await
                > 0)
            .await,
            "the initially healthy node did not attempt binding after runtime admission"
        );
        for probe in &self.probes[1..] {
            probe.set(true);
        }
        assert!(
            advance_until(Duration::from_secs(4), async || {
                !self.locals[1].bound_addrs().await.is_empty()
                    || !self.locals[2].bound_addrs().await.is_empty()
            })
            .await,
            "a healthy peer did not acquire the VIP after the failed bind"
        );
        assert_eq!(
            self.locals[0].bind_attempts(addr).await,
            1,
            "healthy probes must not retry the failed bind"
        );
        assert!(self.locals[0].bound_addrs().await.is_empty());
    }

    /// Stop every daemon still running and return node 0's result.
    async fn stop(self) -> anyhow::Result<Option<crate::raft::FatalReason>> {
        for stop in self.stops {
            let _ = stop.send(());
        }
        let mut handles = self.handles.into_iter();
        let first = join_daemon(handles.next().unwrap()).await;
        for handle in handles {
            join_daemon(handle).await.unwrap();
        }
        for local in self.locals {
            assert!(
                local.bound_addrs().await.is_empty(),
                "a node retained its VIP after shutdown"
            );
        }
        first
    }
}

async fn run_bind_fault_case(
    first_bind: std::io::Result<ExitStatus>,
    check: impl AsyncFnOnce(&BindFaultCluster),
) -> anyhow::Result<Option<crate::raft::FatalReason>> {
    let cluster = start_bind_fault_cluster(first_bind).await;
    let outcome = std::panic::AssertUnwindSafe(check(&cluster));
    use futures::FutureExt;
    let outcome = outcome.catch_unwind().await;
    let result = cluster.stop().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
    result
}

#[tokio::test(start_paused = true)]
async fn bind_fault_is_published_despite_successful_service_probes() {
    let result = run_bind_fault_case(
        Err(std::io::Error::other("interface missing")),
        async |cluster| {
            cluster.wait_for_failed_bind_and_peer_takeover().await;
            assert!(!cluster.handles[0].is_finished());
        },
    )
    .await;
    assert!(result.unwrap().is_none());
}

/// Node 0 binds, then a reassert of its VIP fails with `reassert_error`, optionally while another
/// killed command of node 0 may still run. Returns node 0's result and whether a peer took the VIP
/// within four seconds of node 0 stopping.
async fn run_reassert_timeout(
    reassert_error: std::io::Error,
    unreaped_command: bool,
) -> (anyhow::Result<Option<crate::raft::FatalReason>>, bool) {
    let mut fast_takeover = false;
    let result = run_bind_fault_case(Ok(ExitStatus::from_raw(0)), async |cluster| {
        let addr = cluster.addr;
        assert!(
            advance_until(cluster.budget, async || !cluster.locals[0]
                .bound_addrs()
                .await
                .is_empty())
            .await,
            "node 0 did not complete its initial binding"
        );
        for probe in &cluster.probes[1..] {
            probe.set(true);
        }
        // Healthy holders are sticky, so the peers only become eligible standbys.
        tokio::time::advance(Duration::from_secs(5)).await;
        assert!(!cluster.locals[0].bound_addrs().await.is_empty());
        // While a writer is unsettled every cleanup deletes again, once per reconcile tick.
        let cleanups = if unreaped_command { 6 } else { 1 };
        cluster.locals[0]
            .force_unbind_results(
                ("lo", addr, 32),
                (0..cleanups).map(|_| Ok(ExitStatus::from_raw(0))).collect(),
            )
            .await;
        let _running = unreaped_command.then(|| cluster.locals[0].hold_unreaped_command());
        cluster.locals[0]
            .force_next_bind_result(("lo", addr, 32), Err(reassert_error))
            .await;
        if unreaped_command {
            cluster.locals[0]
                .force_marker_delete_results(
                    addr,
                    (1..cleanups).map(|_| Ok(ExitStatus::from_raw(0))).collect(),
                )
                .await;
        }
        assert!(
            advance_until(Duration::from_secs(4), async || cluster.handles[0]
                .is_finished())
            .await,
            "a timed-out reassert must stop the daemon for a supervised restart"
        );
        assert!(cluster.locals[0].bound_addrs().await.is_empty());
        let peer_holds = async || {
            !cluster.locals[1].bound_addrs().await.is_empty()
                || !cluster.locals[2].bound_addrs().await.is_empty()
        };
        fast_takeover = advance_until(Duration::from_secs(4), peer_holds).await;
        assert!(
            fast_takeover || advance_until(Duration::from_secs(120), peer_holds).await,
            "a healthy peer never took over the VIP"
        );
    })
    .await;
    (result, fast_takeover)
}

fn assert_restart_requested(result: anyhow::Result<Option<crate::raft::FatalReason>>) {
    let error = result.expect_err("a supervised restart needs a failing exit status");
    assert!(
        format!("{error:#}").contains("VIP command timed out"),
        "{error:#}"
    );
}

#[tokio::test(start_paused = true)]
async fn timed_out_reassert_hands_off_at_once_when_every_command_is_reaped() {
    for (kind, error) in [
        (std::io::ErrorKind::TimedOut, "child command exceeded 250ms"),
        (
            std::io::ErrorKind::Other,
            "child command timed out and child reap also timed out",
        ),
    ] {
        let (result, fast_takeover) =
            run_reassert_timeout(crate::process::deadline_missed(kind, error.into()), false).await;
        assert_restart_requested(result);
        assert!(
            fast_takeover,
            "{error}: nothing that could still change an address runs"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn timed_out_reassert_waits_for_the_fence_while_a_killed_command_may_run() {
    let (result, fast_takeover) = run_reassert_timeout(
        crate::process::deadline_missed(
            std::io::ErrorKind::Other,
            "child command timed out and child reap also timed out".into(),
        ),
        true,
    )
    .await;
    assert_restart_requested(result);
    assert!(
        !fast_takeover,
        "a killed command that may still run must not let the stop report its release"
    );
}

#[tokio::test(start_paused = true)]
async fn a_lost_vip_command_restarts_the_daemon_without_acknowledging_its_release() {
    let mut fast_takeover = true;
    let result = run_bind_fault_case(Ok(ExitStatus::from_raw(0)), async |cluster| {
        assert!(
            advance_until(cluster.budget, async || !cluster.locals[0]
                .bound_addrs()
                .await
                .is_empty())
            .await,
            "node 0 did not complete its initial binding"
        );
        for probe in &cluster.probes[1..] {
            probe.set(true);
        }
        tokio::time::advance(Duration::from_secs(5)).await;
        // Scripted answers for a reassert that may run before the stop, and for the stop's cleanup.
        cluster.locals[0]
            .force_next_bind_result(("lo", cluster.addr, 32), Ok(ExitStatus::from_raw(0)))
            .await;
        cluster.locals[0]
            .force_unbind_results(("lo", cluster.addr, 32), vec![Ok(ExitStatus::from_raw(0))])
            .await;
        cluster.locals[0]
            .force_marker_delete_results(cluster.addr, vec![Ok(ExitStatus::from_raw(0))])
            .await;
        cluster.locals[0].lose_command();
        assert!(
            advance_until(Duration::from_secs(4), async || cluster.handles[0]
                .is_finished())
            .await,
            "a VIP command that could not be reaped must stop the daemon for a supervised restart"
        );
        fast_takeover = advance_until(Duration::from_secs(4), async || {
            !cluster.locals[1].bound_addrs().await.is_empty()
                || !cluster.locals[2].bound_addrs().await.is_empty()
        })
        .await;
    })
    .await;
    let error = result.expect_err("a supervised restart needs a failing exit status");
    assert!(
        format!("{error:#}").contains("could not be reaped"),
        "{error:#}"
    );
    assert!(
        !fast_takeover,
        "a lost command may still re-add the address"
    );
}
