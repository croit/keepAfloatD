//! keepAfloatD — VIP failover daemon (OpenRaft + script-based health checks).
//!
//! Process lifecycle
//! -----------------
//! 1. Parse CLI / load + validate the YAML configuration.
//! 2. Reclaim any VIPs left on the configured interfaces by a previous instance
//!    ([`vip::LocalVip::startup_cleanup`]).
//! 3. Build the Raft runtime, start the peer transport and submit listener.
//! 4. Spawn the health-publishing task and the VIP reconciliation loop.
//! 5. Block on `SIGINT` or `SIGTERM`. On signal: stop the reconcile loop, run
//!    [`vip::LocalVip::unbind_all`] to remove every VIP this process bound, then shut down the
//!    submit server, the Raft network and Raft itself before exiting.
//!
//! See module `bind_policy`, `vip` and `README.md` for the binding rules and failure-handling
//! semantics.

mod bind_policy;
mod config;
mod health;
mod raft;
mod submit;
mod vip;

#[cfg(test)]
mod cluster_test;

use crate::config::{Config, VipAddr};
use crate::raft::{KafRequest, start_raft};
// `WatchReceiver` provides `borrow_watched()`/`changed()` on the 0.10 metrics watch handle.
use openraft::async_runtime::WatchReceiver;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::Context;
use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "keepafloatd", version)]
struct Cli {
    /// Path to the YAML configuration file.
    #[arg(short, long, default_value = "config.yaml")]
    config: String,
}

#[cfg(unix)]
async fn wait_for_shutdown_signal() -> anyhow::Result<&'static str> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut sigterm = signal(SignalKind::terminate()).context("listen for SIGTERM")?;
    tokio::select! {
        _ = tokio::signal::ctrl_c() => Ok("SIGINT"),
        _ = sigterm.recv() => Ok("SIGTERM"),
    }
}

#[cfg(not(unix))]
async fn wait_for_shutdown_signal() -> anyhow::Result<&'static str> {
    tokio::signal::ctrl_c().await.context("wait for ctrl_c")?;
    Ok("SIGINT")
}

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
    let cfg = Config::load_path(&cli.config).context("load config")?;
    let vip_table: Arc<Vec<_>> = Arc::new(cfg.sorted_vips());
    let vip_local = vip::LocalVip::new(cfg.dry_run);

    run(cfg, vip_table, vip_local, async {
        match wait_for_shutdown_signal().await {
            Ok(name) => tracing::info!("shutting down on {}", name),
            Err(e) => tracing::error!("shutdown signal wait failed: {e}"),
        }
    })
    .await
}

/// Wire up the full daemon — orphan VIP cleanup, Raft transport + auto-formation, the submit
/// server, health publishing and VIP reconciliation — then run until `shutdown` resolves and tear
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
) -> anyhow::Result<()> {
    let node_id = cfg.node_id;

    // Reclaim any orphan VIPs from a previous instance before joining Raft. Doing this before
    // start_raft ensures peers cannot observe us as a holder while we still have a stale address
    // bound.
    vip_local
        .startup_cleanup(vip_table.as_ref())
        .await
        .context("startup vip cleanup")?;

    let (raft, net, sm) = start_raft(cfg.clone(), vip_table.clone())
        .await
        .context("start raft")?;

    let leader_watch_task = {
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

    let submit_task = {
        let cfg = cfg.clone();
        let raft = raft.clone();
        tokio::spawn(async move {
            if let Err(e) = submit::run_submit_server(cfg, raft).await {
                tracing::error!("submit server: {}", e);
            }
        })
    };

    let local_healthy = Arc::new(AtomicBool::new(false));
    let consensus_fresh = Arc::new(AtomicBool::new(false));

    let health_task = {
        let cfg = cfg.clone();
        let raft = raft.clone();
        let local_healthy = local_healthy.clone();
        let consensus_fresh = consensus_fresh.clone();
        tokio::spawn(async move {
            let mut tick =
                tokio::time::interval(tokio::time::Duration::from_millis(cfg.health.interval_ms));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                let ok = health::run_health_check(&cfg.health).await;
                local_healthy.store(ok, Ordering::SeqCst);
                let req = KafRequest::HealthUpdate {
                    node_id: cfg.node_id,
                    healthy: ok,
                };
                match submit::submit_request(&cfg, &raft, req).await {
                    Ok(()) => consensus_fresh.store(true, Ordering::SeqCst),
                    Err(e) => {
                        consensus_fresh.store(false, Ordering::SeqCst);
                        tracing::warn!("health raft submit: {}", e);
                    }
                }
            }
        })
    };

    let vip_task = tokio::spawn(vip::run_reconcile_loop(
        cfg.clone(),
        raft.clone(),
        sm.clone(),
        vip_local.clone(),
        vip_table.clone(),
        local_healthy.clone(),
        consensus_fresh.clone(),
        node_id,
    ));

    shutdown.await;

    // Stop reconciliation and health publishing first so local_healthy is stable when we
    // read it to determine the shutdown notify state. submit can still run during cleanup.
    vip_task.abort();
    let _ = vip_task.await;
    health_task.abort();
    let _ = health_task.await;
    let shutdown_state = crate::vip::release_notify_state(local_healthy.load(Ordering::SeqCst));
    vip_local
        .unbind_all(
            vip_table.as_ref(),
            cfg.notify.as_deref(),
            cfg.dry_run,
            shutdown_state,
        )
        .await;

    submit_task.abort();
    leader_watch_task.abort();
    let _ = net.shutdown().await;
    let _ = raft.shutdown().await;

    Ok(())
}
