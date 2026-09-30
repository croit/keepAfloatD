//! keepAfloatD - VIP failover daemon (OpenRaft + script-based health checks).
//!
//! Process lifecycle
//! -----------------
//! 1. Parse CLI / load + validate the YAML configuration.
//! 2. Reclaim any VIPs left on the configured interfaces by a previous instance
//!    ([`vip::LocalVip::startup_cleanup`]).
//! 3. Build the Raft runtime, start the peer transport and submit listener.
//! 4. Spawn the health-publishing task and the VIP reconciliation loop.
//! 5. Block on `SIGINT`, `SIGTERM` or `SIGHUP`. On signal: stop the reconcile loop, run
//!    [`vip::LocalVip::unbind_all`] to remove every VIP this process bound, then shut down the
//!    submit server, the Raft network and Raft itself before exiting. `SIGHUP` requests
//!    a restart by returning a failure status after cleanup.
//!
//! See module `bind_policy`, `vip` and `README.md` for the binding rules and failure-handling
//! semantics.

mod admission;
mod bind_policy;
mod config;
mod connection_admission;
mod consensus_freshness;
mod health;
mod process;
mod raft;
mod secret;
#[cfg(not(test))]
mod shutdown;
mod submit;
mod vip;

#[cfg(test)]
mod cluster_test;

use crate::config::{Config, VipAddr};
use crate::raft::{FatalReason, start_raft};
// `WatchReceiver` provides `borrow_watched()`/`changed()` on the 0.10 metrics watch handle.
use openraft::async_runtime::WatchReceiver;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::Context;
#[cfg(not(test))]
use clap::Parser;

enum StopReason {
    Complete(Option<FatalReason>),
    Failed(anyhow::Error),
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum FinishedTask {
    None,
    Submit,
    Health,
    Vip,
    LeaderWatch,
}

fn unexpected_task_failure(
    name: &str,
    result: Result<(), tokio::task::JoinError>,
) -> anyhow::Error {
    match result {
        Ok(()) => anyhow::anyhow!("{name} exited unexpectedly"),
        Err(error) => anyhow::anyhow!("{name} task failed: {error}"),
    }
}

fn submit_task_failure(
    result: Result<anyhow::Result<()>, tokio::task::JoinError>,
) -> anyhow::Error {
    match result {
        Ok(Ok(())) => anyhow::anyhow!("submit server exited unexpectedly"),
        Ok(Err(error)) => error.context("submit server"),
        Err(error) => anyhow::anyhow!("submit server task failed: {error}"),
    }
}

fn supervision_channel_failure(name: &str, failure: Option<String>) -> anyhow::Error {
    failure.map_or_else(
        || anyhow::anyhow!("{name} supervision channel closed unexpectedly"),
        anyhow::Error::msg,
    )
}

fn record_optional_failure(failures: &mut Vec<String>, failure: Option<String>) {
    if let Some(failure) = failure {
        failures.push(failure);
    }
}

fn record_lifecycle_result<E: std::fmt::Display>(
    failures: &mut Vec<String>,
    name: &str,
    result: Result<(), E>,
) {
    if let Err(error) = result {
        failures.push(format!("{name}: {error}"));
    }
}

async fn stop_daemon_task<T>(
    name: &str,
    handle: &mut tokio::task::JoinHandle<T>,
) -> Option<String> {
    handle.abort();
    match handle.await {
        Ok(_) => Some(format!("{name} exited before cancellation")),
        Err(error) if error.is_cancelled() => None,
        Err(error) => Some(format!("{name} task failed: {error}")),
    }
}

fn finish_daemon_run(
    stop_reason: StopReason,
    lifecycle_failures: Vec<String>,
) -> anyhow::Result<Option<FatalReason>> {
    match stop_reason {
        StopReason::Complete(reason) if lifecycle_failures.is_empty() => Ok(reason),
        StopReason::Complete(_) => {
            anyhow::bail!("daemon shutdown failed: {}", lifecycle_failures.join("; "))
        }
        StopReason::Failed(error) if lifecycle_failures.is_empty() => Err(error),
        StopReason::Failed(error) => Err(error.context(format!(
            "additional daemon shutdown failures: {}",
            lifecycle_failures.join("; ")
        ))),
    }
}

#[cfg(not(test))]
#[derive(Parser, Debug)]
#[command(name = "keepafloatd", version)]
struct Cli {
    /// Path to the YAML configuration file.
    #[arg(short, long, default_value = "config.yaml")]
    config: String,
}

#[cfg(not(test))]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Plain output: the daemon's home is systemd, whose journal already timestamps every
    // line and is not a TTY. ANSI colour codes and an RFC3339 timestamp would both be
    // captured verbatim into the journal as noise, so disable them unconditionally.
    tracing_subscriber::fmt()
        .with_ansi(false)
        .without_time()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    let signals = shutdown::Signals::install().context("install shutdown signals")?;
    let cfg = Config::load_path(&cli.config).context("load config")?;
    let vip_table: Arc<Vec<_>> = Arc::new(cfg.sorted_vips());
    let vip_local = vip::LocalVip::new_with_address_protocol(cfg.dry_run, cfg.address_protocol);

    let mut signal_result = Ok(());
    let fatal_reason = run(cfg, vip_table, vip_local, async {
        signal_result = signals.wait().await;
    })
    .await?;
    if let Some(reason) = fatal_reason {
        // Every task has stopped and every locally tracked VIP has been removed at this point.
        std::process::exit(reason.exit_code());
    }
    signal_result
}

/// Wire up the full daemon - orphan VIP cleanup, Raft transport + auto-formation, the submit
/// server, health publishing and VIP reconciliation - then run until `shutdown` resolves and tear
/// everything down (stop reconciling, unbind every VIP this process holds, stop the remaining
/// tasks, and shut down the Raft network and Raft itself).
///
/// Extracted from `main` so the whole lifecycle can be driven from an integration test with an
/// injected shutdown trigger instead of a real `SIGINT`/`SIGTERM`.
async fn run(
    cfg: Arc<Config>,
    vip_table: Arc<Vec<(VipAddr, String)>>,
    vip_local: Arc<vip::LocalVip>,
    shutdown: impl Future<Output = ()>,
) -> anyhow::Result<Option<FatalReason>> {
    let node_id = cfg.node_id;

    // Reclaim any orphan VIPs from a previous instance before joining Raft. Doing this before
    // start_raft ensures peers cannot observe us as a holder while we still have a stale address
    // bound.
    vip_local
        .startup_cleanup(vip_table.as_ref())
        .await
        .context("startup vip cleanup")?;

    let (raft, net, sm, mut fatal_rx, mut network_failure_rx, mut control_tasks) =
        start_raft(cfg.clone(), vip_table.clone())
            .await
            .context("start raft")?;

    let mut leader_watch_task = {
        let raft = raft.clone();
        // Watch Raft metrics until shutdown so E2E and operators can see leader changes.
        tokio::spawn(async move {
            let mut metrics = raft.metrics();
            let mut last_leader = None;
            loop {
                let current_leader = metrics.borrow_watched().current_leader;
                if current_leader != last_leader {
                    tracing::info!("raft current leader is now {:?}", current_leader);
                    last_leader = current_leader;
                }
                if metrics.changed().await.is_err() {
                    break;
                }
            }
        })
    };

    let mut submit_task = {
        let cfg = cfg.clone();
        let raft = raft.clone();
        tokio::spawn(async move { submit::run_submit_server(cfg, raft).await })
    };

    let local_healthy = Arc::new(AtomicBool::new(false));
    let consensus_fresh = Arc::new(consensus_freshness::ConsensusFreshness::for_probe_cadence(
        cfg.health.interval_ms,
        cfg.health.effective_stale_missed_probes(),
    ));

    let mut health_task: tokio::task::JoinHandle<()> = {
        let cfg = cfg.clone();
        let raft = raft.clone();
        let local_healthy = local_healthy.clone();
        let consensus_fresh = consensus_fresh.clone();
        tokio::spawn(async move {
            let failover_delay_ticks = cfg.effective_failover_delay_ticks();
            let mut failure_dampener = health::FailureDampener::new(failover_delay_ticks);
            let mut tick =
                tokio::time::interval(tokio::time::Duration::from_millis(cfg.health.interval_ms));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                let raw_ok = health::run_health_check(&cfg.health).await;
                let ok = failure_dampener.observe(raw_ok);
                let previous_ok = local_healthy.swap(ok, Ordering::SeqCst);
                if !raw_ok && ok && failure_dampener.failure_ticks() == 1 {
                    tracing::warn!(
                        failover_delay_secs = cfg.failover_delay_secs,
                        "health probe failed; delaying failover"
                    );
                } else if previous_ok && !ok {
                    tracing::warn!("effective local health became unhealthy");
                } else if !previous_ok && ok {
                    tracing::info!("effective local health became healthy");
                }
                let proof_started = tokio::time::Instant::now();
                match submit::submit_health(&cfg, &raft, ok, || {
                    fence_unavailable_health_proof(&consensus_fresh);
                })
                .await
                {
                    Ok(Some(_)) => consensus_fresh.record_success(proof_started),
                    Ok(None) => {
                        // #26: legacy leaders see only unhealthy reports from a proof-only follower.
                        fence_unavailable_health_proof(&consensus_fresh);
                    }
                    Err(e) => {
                        consensus_fresh.invalidate();
                        tracing::warn!("health raft submit: {}", e);
                    }
                }
            }
        })
    };

    let mut vip_task = tokio::spawn(vip::run_reconcile_loop(
        cfg.clone(),
        raft.clone(),
        sm.clone(),
        vip_local.clone(),
        vip_table.clone(),
        local_healthy.clone(),
        consensus_fresh.clone(),
        node_id,
    ));

    tokio::pin!(shutdown);
    let (stop_reason, finished_task) = tokio::select! {
        _ = &mut shutdown => (StopReason::Complete(None), FinishedTask::None),
        reason = fatal_rx.recv() => match reason {
            Some(reason) => (StopReason::Complete(Some(reason)), FinishedTask::None),
            None => (
                StopReason::Failed(anyhow::anyhow!("fatal control channel closed unexpectedly")),
                FinishedTask::None,
            ),
        },
        result = &mut submit_task => (
            StopReason::Failed(submit_task_failure(result)),
            FinishedTask::Submit,
        ),
        failure = network_failure_rx.recv() => (
            StopReason::Failed(supervision_channel_failure("network task", failure)),
            FinishedTask::None,
        ),
        failure = control_tasks.recv_failure() => (
            StopReason::Failed(supervision_channel_failure("Raft control task", failure)),
            FinishedTask::None,
        ),
        result = &mut health_task => (
            StopReason::Failed(unexpected_task_failure("health", result)),
            FinishedTask::Health,
        ),
        result = &mut vip_task => (
            StopReason::Failed(unexpected_task_failure("VIP reconciliation", result)),
            FinishedTask::Vip,
        ),
        result = &mut leader_watch_task => (
            StopReason::Failed(unexpected_task_failure("leader watcher", result)),
            FinishedTask::LeaderWatch,
        ),
    };

    let mut lifecycle_failures = Vec::new();
    if finished_task != FinishedTask::Vip {
        let result = stop_daemon_task("VIP reconciliation", &mut vip_task).await;
        record_optional_failure(&mut lifecycle_failures, result);
    }
    if finished_task != FinishedTask::Health {
        let result = stop_daemon_task("health", &mut health_task).await;
        record_optional_failure(&mut lifecycle_failures, result);
    }
    let shutdown_state = crate::vip::release_notify_state(local_healthy.load(Ordering::SeqCst));
    let vip_cleanup = vip_local
        .unbind_all(
            vip_table.as_ref(),
            cfg.notify.as_deref(),
            cfg.dry_run,
            shutdown_state,
        )
        .await;
    record_lifecycle_result(&mut lifecycle_failures, "graceful VIP cleanup", vip_cleanup);

    if finished_task != FinishedTask::Submit {
        let result = stop_daemon_task("submit server", &mut submit_task).await;
        record_optional_failure(&mut lifecycle_failures, result);
    }
    if finished_task != FinishedTask::LeaderWatch {
        let result = stop_daemon_task("leader watcher", &mut leader_watch_task).await;
        record_optional_failure(&mut lifecycle_failures, result);
    }
    record_lifecycle_result(
        &mut lifecycle_failures,
        "Raft control task shutdown",
        control_tasks.shutdown().await,
    );
    record_lifecycle_result(
        &mut lifecycle_failures,
        "network shutdown",
        net.shutdown().await,
    );
    if let Err(error) = raft.shutdown().await {
        lifecycle_failures.push(format!("Raft shutdown: {error:?}"));
    }

    finish_daemon_run(stop_reason, lifecycle_failures)
}

fn fence_unavailable_health_proof(consensus_fresh: &consensus_freshness::ConsensusFreshness) {
    // #26: protocol availability fences consensus eligibility, not the local probe result.
    consensus_fresh.invalidate();
}
