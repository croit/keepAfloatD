//! Linux `ip addr` bind/release, optional gratuitous ARP, and the VIP reconciliation loop.
//!
//! Crash- and restart-safety
//! -------------------------
//! On startup the daemon must reclaim ownership of any address it might have left on the
//! interface from a previous instance (kill -9, OOM, systemd `Restart=on-failure`). The
//! [`LocalVip::startup_cleanup`] method removes every configured VIP from the kernel before we
//! re-join Raft.
//!
//! On graceful shutdown ([`LocalVip::unbind_all`]) every still-bound address is removed. Crash
//! recovery on the next process start re-runs `startup_cleanup` to handle the un-graceful path
//! symmetrically.
//!
//! Reconciliation loop
//! -------------------
//! [`run_reconcile_loop`] reads the committed fenced assignment state without holding the
//! state-machine read guard across system commands. Binding requires the local node to be the
//! committed holder *and* for the previous holder fence to be satisfied. Losing local health,
//! losing consensus freshness, losing leader visibility or losing ownership all force an unbind.

use crate::config::{Config, VipAddr};
use crate::raft::store::VipAssignment;
use crate::raft::{KafRaft, KafRequest, KafStorageState};
use crate::submit;
// `WatchReceiver` provides `borrow_watched()` on the 0.10 metrics watch handle.
use openraft::async_runtime::WatchReceiver;
use std::collections::HashMap;
use std::collections::HashSet;
use std::net::IpAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::process::Command;
use tokio::sync::RwLock;

/// `ip` address-family selector for an address. The prefix length is carried per-VIP in
/// [`VipAddr::prefix`]; it defaults to a host route (`/32` for IPv4, `/128` for IPv6) but can be
/// widened via the config CIDR suffix. A host route keeps the VIP
/// local to this machine; routing/ARP for the address is handled by gratuitous ARP after bind.
const fn ip_family(ip: IpAddr) -> &'static str {
    match ip {
        IpAddr::V4(_) => "-4",
        IpAddr::V6(_) => "-6",
    }
}

/// Apply Linux secondary addresses with `ip` and optional IPv4 gratuitous ARP.
///
/// All process invocations use `tokio::process::Command` so the daemon's tokio runtime is not
/// blocked while `ip` or `arping` runs.
pub struct LocalVip {
    bound: RwLock<HashSet<IpAddr>>,
    dry_run: bool,
}

impl LocalVip {
    /// Wrap in [`Arc`] for use from async VIP reconciliation tasks.
    pub fn new(dry_run: bool) -> Arc<Self> {
        Arc::new(Self {
            bound: RwLock::new(HashSet::new()),
            dry_run,
        })
    }

    /// Reclaim any configured VIPs that are still attached to their interfaces from a previous
    /// process. After this returns, the in-memory `bound` set is empty so that subsequent
    /// reconciliation can re-add only the VIPs the cluster currently agrees this node should hold.
    pub async fn startup_cleanup(&self, vips: &[(VipAddr, String)]) -> anyhow::Result<()> {
        if self.dry_run {
            for (vip, iface) in vips {
                tracing::info!(
                    target: "keepafloatd::vip",
                    "dry-run: would reclaim {}/{} on {iface} if present",
                    vip.addr,
                    vip.prefix
                );
            }
            return Ok(());
        }
        for (vip, iface) in vips {
            let (ip, prefix) = (vip.addr, vip.prefix);
            let result = Command::new("ip")
                .args([
                    ip_family(ip),
                    "addr",
                    "del",
                    &format!("{ip}/{prefix}"),
                    "dev",
                    iface,
                ])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()
                .await;
            match result {
                Ok(s) if s.success() => {
                    tracing::info!(
                        target: "keepafloatd::vip",
                        "startup_cleanup: reclaimed orphan {ip}/{prefix} on {iface}"
                    );
                }
                Ok(_) => {
                    tracing::debug!(
                        target: "keepafloatd::vip",
                        "startup_cleanup: {ip}/{prefix} on {iface} not present (ok)"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        target: "keepafloatd::vip",
                        "startup_cleanup: spawn ip del {ip}/{prefix} on {iface}: {e}"
                    );
                }
            }
        }
        Ok(())
    }

    /// Add the secondary address on `iface` with prefix length `prefix`.
    ///
    /// Re-asserts the address on every call: the reconcile loop invokes this each tick while the
    /// node is the holder, and `ip addr replace` re-adds an address the kernel dropped out of band
    /// (link down/up, NetworkManager, `ip addr flush`, DHCP renew) instead of skipping it because
    /// the in-memory set still says we hold it. Gratuitous ARP fires only on a genuine first bind
    /// so re-assertion does not spam the segment.
    pub async fn bind(&self, iface: &str, ip: IpAddr, prefix: u8) -> anyhow::Result<()> {
        let first_bind = !self.bound.read().await.contains(&ip);
        // Record ownership *before* the syscall so a task abort mid-bind (SIGTERM →
        // vip_task.abort()) still leaves the address recorded for unbind_all to reclaim; otherwise
        // the child could finish attaching the address while the dropped future never inserted it,
        // leaking the VIP past graceful shutdown.
        self.bound.write().await.insert(ip);
        if self.dry_run {
            if first_bind {
                tracing::info!(target: "keepafloatd::vip", "dry-run: would bind {ip}/{prefix} on {iface}");
            }
            return Ok(());
        }
        let status = Command::new("ip")
            .args([
                ip_family(ip),
                "addr",
                "replace",
                &format!("{ip}/{prefix}"),
                "dev",
                iface,
            ])
            .kill_on_drop(true)
            .status()
            .await;
        match status {
            Ok(s) if s.success() => {}
            Ok(s) => {
                self.bound.write().await.remove(&ip);
                anyhow::bail!("ip addr replace failed: {s}");
            }
            Err(e) => {
                self.bound.write().await.remove(&ip);
                return Err(e.into());
            }
        }
        if first_bind {
            let _ = Command::new("arping")
                .args(["-q", "-U", "-c", "2", "-I", iface, &ip.to_string()])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .kill_on_drop(true)
                .status()
                .await;
            tracing::info!(target: "keepafloatd::vip", "bound {ip}/{prefix} on {iface}");
        }
        Ok(())
    }

    /// Remove the address if it was previously added by this [`LocalVip`] instance on this host.
    /// `prefix` must match the prefix the address was bound with so the kernel del matches.
    pub async fn unbind(&self, iface: &str, ip: IpAddr, prefix: u8) -> anyhow::Result<()> {
        if !self.bound.read().await.contains(&ip) {
            return Ok(());
        }
        if self.dry_run {
            tracing::info!(target: "keepafloatd::vip", "dry-run: would unbind {ip}/{prefix} on {iface}");
        } else {
            let status = Command::new("ip")
                .args([
                    ip_family(ip),
                    "addr",
                    "del",
                    &format!("{ip}/{prefix}"),
                    "dev",
                    iface,
                ])
                .status()
                .await?;
            anyhow::ensure!(status.success(), "ip addr del failed: {status}");
            tracing::info!(target: "keepafloatd::vip", "unbound {ip}/{prefix} on {iface}");
        }
        self.bound.write().await.remove(&ip);
        Ok(())
    }

    /// Remove every address this instance currently has bound.
    ///
    /// When `notify` is set, fires the notify script with `shutdown_state` for each VIP that was
    /// actually bound at call time. All spawned script tasks are awaited before this function
    /// returns, so the caller knows every script has been submitted to the OS before shutdown
    /// proceeds. Suppressed by `dry_run`.
    pub async fn unbind_all(
        &self,
        vips: &[(VipAddr, String)],
        notify: Option<&str>,
        dry_run: bool,
        shutdown_state: VipState,
    ) {
        let mut handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();
        for (vip, iface) in vips {
            // was_bound is true only when notify.is_some(), so the unwrap below is safe.
            let was_bound = notify.is_some() && self.bound.read().await.contains(&vip.addr);
            if let Err(e) = self.unbind(iface, vip.addr, vip.prefix).await {
                tracing::warn!(
                    target: "keepafloatd::vip",
                    "unbind_all: {}/{} on {iface}: {e}",
                    vip.addr,
                    vip.prefix
                );
            } else if was_bound {
                if let Some(h) = fire_notify_script(
                    notify.unwrap(),
                    &vip.addr.to_string(),
                    shutdown_state,
                    dry_run,
                ) {
                    handles.push(h);
                }
            }
        }
        for h in handles {
            let _ = h.await;
        }
    }

    /// Snapshot of the addresses this instance currently considers bound (tests only).
    #[cfg(test)]
    pub(crate) async fn bound_addrs(&self) -> Vec<IpAddr> {
        let mut v: Vec<IpAddr> = self.bound.read().await.iter().copied().collect();
        v.sort_unstable();
        v
    }
}

#[derive(Clone)]
struct ReconcileSnapshot {
    assignments: HashMap<IpAddr, VipAssignment>,
    node_health: HashMap<u64, bool>,
    node_probe_ticks: HashMap<u64, u64>,
    latest_probe_tick: u64,
    stale_missed_probes: u64,
    failback_delay_ticks: u64,
    node_recovery_tick: HashMap<u64, u64>,
    node_failback_blocked: HashSet<u64>,
}

/// VIP ownership state passed to the notify script.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum VipState {
    Master,
    Backup,
    Fault,
}

impl VipState {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            VipState::Master => "MASTER",
            VipState::Backup => "BACKUP",
            VipState::Fault => "FAULT",
        }
    }
}

/// Choose the notify state for a VIP release.
///
/// `FAULT` when this node's own health check failed — the local node is the cause of the
/// release. `BACKUP` for any cluster-level reason (orderly reassignment, leader re-election,
/// stale consensus): per keepalived convention `FAULT` signals a *local* failure only.
pub(crate) fn release_notify_state(local_ok: bool) -> VipState {
    if local_ok {
        VipState::Backup
    } else {
        VipState::Fault
    }
}

/// Spawn the notify script for a VIP ownership transition.
///
/// Invoked as `<script> INSTANCE <vip_addr> MASTER|BACKUP|FAULT` (keepalived-compatible).
/// When `dry_run` is true the invocation is suppressed and logged instead.
///
/// Returns `Some(handle)` when the task was spawned so callers can await it on shutdown;
/// returns `None` when `dry_run` is true (no task spawned). The reconcile loop discards the
/// handle (fire-and-forget); `unbind_all` awaits all handles before returning.
fn fire_notify_script(
    script: &str,
    vip_addr: &str,
    state: VipState,
    dry_run: bool,
) -> Option<tokio::task::JoinHandle<()>> {
    let state_str = state.as_str();
    if dry_run {
        tracing::info!(
            target: "keepafloatd::vip",
            "dry-run: would notify {script} INSTANCE {vip_addr} {state_str}"
        );
        return None;
    }
    let script = script.to_owned();
    let vip_addr = vip_addr.to_owned();
    Some(tokio::spawn(async move {
        let run = Command::new(&script)
            .args(["INSTANCE", &vip_addr, state_str])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true)
            .status();
        // Bound the user script: a blocking notify hook (network call, held lock, downed service)
        // must not stall unbind_all — which awaits every notify task — until systemd's
        // TimeoutStopSec SIGKILLs the whole daemon and skips orderly Raft/network shutdown.
        match tokio::time::timeout(NOTIFY_SCRIPT_TIMEOUT, run).await {
            Ok(Ok(s)) if s.success() => tracing::debug!(
                target: "keepafloatd::vip",
                "notify {script} INSTANCE {vip_addr} {state_str}: ok"
            ),
            Ok(Ok(s)) => tracing::warn!(
                target: "keepafloatd::vip",
                "notify {script} INSTANCE {vip_addr} {state_str}: exit {s}"
            ),
            Ok(Err(e)) => tracing::warn!(
                target: "keepafloatd::vip",
                "notify {script} INSTANCE {vip_addr} {state_str}: spawn failed: {e}"
            ),
            Err(_) => tracing::warn!(
                target: "keepafloatd::vip",
                "notify {script} INSTANCE {vip_addr} {state_str}: timed out after {}s, killed",
                NOTIFY_SCRIPT_TIMEOUT.as_secs()
            ),
        }
    }))
}

/// Reconciliation tick period.
const RECONCILE_TICK: tokio::time::Duration = tokio::time::Duration::from_millis(250);

/// Upper bound on a user notify script. Kept under systemd's default `TimeoutStopSec=15` so a
/// hanging hook is killed and teardown proceeds rather than the daemon being SIGKILLed wholesale.
const NOTIFY_SCRIPT_TIMEOUT: tokio::time::Duration = tokio::time::Duration::from_secs(10);

/// Run the VIP reconciliation loop until cancelled (typically via task abort on shutdown).
#[allow(clippy::too_many_arguments)]
pub async fn run_reconcile_loop(
    cfg: Arc<Config>,
    raft: KafRaft,
    sm: Arc<RwLock<KafStorageState>>,
    vip_local: Arc<LocalVip>,
    vip_table: Arc<Vec<(VipAddr, String)>>,
    local_healthy: Arc<AtomicBool>,
    consensus_fresh: Arc<AtomicBool>,
    node_id: u64,
) {
    let mut tick = tokio::time::interval(RECONCILE_TICK);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut released_generations: HashMap<IpAddr, u64> = HashMap::new();

    loop {
        tick.tick().await;

        let has_leader = raft.metrics().borrow_watched().current_leader.is_some();
        let local_ok = local_healthy.load(Ordering::SeqCst);
        let consensus_ok = consensus_fresh.load(Ordering::SeqCst);

        let snapshot = {
            let st = sm.read().await;
            ReconcileSnapshot {
                assignments: st.vip_assignments.clone(),
                node_health: st.node_health.clone(),
                node_probe_ticks: st.node_probe_ticks.clone(),
                latest_probe_tick: st.latest_probe_tick,
                stale_missed_probes: st.stale_missed_probes,
                failback_delay_ticks: st.failback_delay_ticks,
                node_recovery_tick: st.node_recovery_tick.clone(),
                node_failback_blocked: st.node_failback_blocked.clone(),
            }
        };

        for (vip, iface) in vip_table.iter() {
            let addr = vip.addr;
            let prefix = vip.prefix;
            let assignment = snapshot.assignments.get(&addr);
            let want_bind = crate::bind_policy::should_bind_vip(
                has_leader,
                local_ok,
                consensus_ok,
                node_id,
                assignment,
                &snapshot.node_health,
                &snapshot.node_probe_ticks,
                snapshot.latest_probe_tick,
                snapshot.stale_missed_probes,
                snapshot.failback_delay_ticks,
                &snapshot.node_recovery_tick,
                &snapshot.node_failback_blocked,
            );

            if want_bind {
                released_generations.remove(&addr);
                // Snapshot before bind so we detect genuine first-bind transitions only.
                let was_not_bound =
                    cfg.notify.is_some() && !vip_local.bound.read().await.contains(&addr);
                if let Err(e) = vip_local.bind(iface, addr, prefix).await {
                    tracing::warn!("bind {}: {}", addr, e);
                } else if was_not_bound {
                    if let Some(script) = cfg.notify.as_deref() {
                        let _ = fire_notify_script(
                            script,
                            &addr.to_string(),
                            VipState::Master,
                            cfg.dry_run,
                        );
                    }
                }
                continue;
            }

            // Snapshot before unbind so we detect genuine last-release transitions only.
            let was_bound = cfg.notify.is_some() && vip_local.bound.read().await.contains(&addr);
            if let Err(e) = vip_local.unbind(iface, addr, prefix).await {
                tracing::warn!("unbind {}: {}", addr, e);
            } else if was_bound {
                // FAULT only when local health failed; cluster events → BACKUP.
                if let Some(script) = cfg.notify.as_deref() {
                    let _ = fire_notify_script(
                        script,
                        &addr.to_string(),
                        release_notify_state(local_ok),
                        cfg.dry_run,
                    );
                }
            }

            if let Some(assignment) = assignment {
                maybe_publish_release(
                    &cfg,
                    &raft,
                    &consensus_fresh,
                    node_id,
                    addr,
                    assignment,
                    &mut released_generations,
                )
                .await;
            } else {
                released_generations.remove(&addr);
            }
        }
    }
}

/// Whether this node still owes a `VipReleased` ack for `vip`: it is the recorded previous holder,
/// the assignment has not yet been marked released, and it has not already submitted an ack for this
/// generation. Pure so the dedup/fencing decision can be unit-tested without a live `KafRaft`.
#[must_use]
fn should_publish_release(
    assignment: &VipAssignment,
    node_id: u64,
    vip: IpAddr,
    released_generations: &HashMap<IpAddr, u64>,
) -> bool {
    assignment.previous_holder == Some(node_id)
        && !assignment.previous_holder_released
        && released_generations.get(&vip).copied() != Some(assignment.generation)
}

async fn maybe_publish_release(
    cfg: &Arc<Config>,
    raft: &KafRaft,
    consensus_fresh: &Arc<AtomicBool>,
    node_id: u64,
    vip: IpAddr,
    assignment: &VipAssignment,
    released_generations: &mut HashMap<IpAddr, u64>,
) {
    if !should_publish_release(assignment, node_id, vip, released_generations) {
        // Not (or no longer) our obligation. Clear any stale dedup marker only when this node is
        // no longer the previous holder (or it has already been released), mirroring the original
        // two-stage guard; an already-acked-this-generation case leaves the marker in place.
        if assignment.previous_holder != Some(node_id) || assignment.previous_holder_released {
            released_generations.remove(&vip);
        }
        return;
    }

    let req = KafRequest::VipReleased {
        node_id,
        vip,
        generation: assignment.generation,
    };
    match submit::submit_request(cfg, raft, req).await {
        Ok(()) => {
            consensus_fresh.store(true, Ordering::SeqCst);
            released_generations.insert(vip, assignment.generation);
        }
        Err(e) => {
            consensus_fresh.store(false, Ordering::SeqCst);
            tracing::warn!(
                "vip release submit {} gen {}: {}",
                vip,
                assignment.generation,
                e
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        LocalVip, VipAddr, VipAssignment, VipState, fire_notify_script, release_notify_state,
        should_publish_release,
    };
    use std::collections::HashMap;
    use std::net::{IpAddr, Ipv4Addr};

    fn ip4(a: u8, b: u8, c: u8, d: u8) -> IpAddr {
        IpAddr::V4(Ipv4Addr::new(a, b, c, d))
    }

    fn handoff(previous_holder: Option<u64>, released: bool, generation: u64) -> VipAssignment {
        VipAssignment {
            holder: 2,
            generation,
            previous_holder,
            previous_holder_released: released,
            activation_tick: 0,
        }
    }

    #[test]
    fn should_publish_release_only_when_this_node_owes_a_fresh_ack() {
        let vip = ip4(10, 0, 0, 1);
        let empty = HashMap::new();
        // This node is the previous holder, not yet released, no prior ack: owes one.
        assert!(should_publish_release(
            &handoff(Some(1), false, 4),
            1,
            vip,
            &empty
        ));
        // Some other node is the previous holder: nothing owed here.
        assert!(!should_publish_release(
            &handoff(Some(9), false, 4),
            1,
            vip,
            &empty
        ));
        // No previous holder (first assignment): nothing to release.
        assert!(!should_publish_release(
            &handoff(None, false, 4),
            1,
            vip,
            &empty
        ));
        // Already released in committed state: nothing owed.
        assert!(!should_publish_release(
            &handoff(Some(1), true, 4),
            1,
            vip,
            &empty
        ));
    }

    #[test]
    fn should_publish_release_dedups_by_generation() {
        let vip = ip4(10, 0, 0, 1);
        // Already acked this exact generation: do not resubmit.
        let acked_same = HashMap::from([(vip, 4_u64)]);
        assert!(!should_publish_release(
            &handoff(Some(1), false, 4),
            1,
            vip,
            &acked_same
        ));
        // Acked an older generation; a new handoff (gen 5) still owes an ack.
        let acked_old = HashMap::from([(vip, 4_u64)]);
        assert!(should_publish_release(
            &handoff(Some(1), false, 5),
            1,
            vip,
            &acked_old
        ));
    }

    #[tokio::test]
    async fn dry_run_bind_is_idempotent_and_tracks_bound() {
        let vip = LocalVip::new(true);
        let ip = ip4(10, 0, 0, 1);
        vip.bind("lo", ip, 32).await.unwrap();
        assert!(vip.bound.read().await.contains(&ip));
        // Second bind of the same address is a no-op and stays Ok.
        vip.bind("lo", ip, 32).await.unwrap();
        assert_eq!(vip.bound.read().await.len(), 1);
    }

    #[tokio::test]
    async fn dry_run_unbind_only_removes_known_addresses() {
        let vip = LocalVip::new(true);
        let ip = ip4(10, 0, 0, 1);
        // Unbinding an address we never bound is a safe no-op.
        vip.unbind("lo", ip, 32).await.unwrap();
        assert!(vip.bound.read().await.is_empty());
        // Bind then unbind clears it.
        vip.bind("lo", ip, 32).await.unwrap();
        vip.unbind("lo", ip, 32).await.unwrap();
        assert!(vip.bound.read().await.is_empty());
    }

    #[tokio::test]
    async fn dry_run_unbind_all_clears_every_bound_address() {
        let vip = LocalVip::new(true);
        let table = vec![
            (VipAddr::host(ip4(10, 0, 0, 1)), "lo".to_string()),
            (VipAddr::host(ip4(10, 0, 0, 2)), "lo".to_string()),
        ];
        for (v, iface) in &table {
            vip.bind(iface, v.addr, v.prefix).await.unwrap();
        }
        assert_eq!(vip.bound.read().await.len(), 2);
        vip.unbind_all(&table, None, true, VipState::Backup).await;
        assert!(vip.bound.read().await.is_empty());
    }

    #[tokio::test]
    async fn dry_run_startup_cleanup_is_noop_and_leaves_bound_empty() {
        let vip = LocalVip::new(true);
        let table = vec![(VipAddr::host(ip4(10, 0, 0, 1)), "lo".to_string())];
        vip.startup_cleanup(&table).await.unwrap();
        assert!(vip.bound.read().await.is_empty());
    }

    // VLAN sub-interface tests — verify that bind/unbind/startup_cleanup/unbind_all work
    // correctly when the effective interface is a VLAN sub-interface string (e.g. "eth0.100").
    // The interface name is computed by config::sorted_vips(); vip.rs treats it as an opaque
    // string, so dry-run tests here confirm the bookkeeping is correct regardless of the format.

    #[tokio::test]
    async fn dry_run_bind_with_vlan_interface_tracks_ip_not_subinterface() {
        let vip = LocalVip::new(true);
        let ip = ip4(10, 0, 0, 1);
        vip.bind("eth0.100", ip, 24).await.unwrap();
        assert!(vip.bound.read().await.contains(&ip));
        assert_eq!(vip.bound.read().await.len(), 1);
    }

    #[tokio::test]
    async fn dry_run_unbind_with_vlan_interface_removes_bound_ip() {
        let vip = LocalVip::new(true);
        let ip = ip4(10, 0, 0, 1);
        vip.bind("eth0.100", ip, 24).await.unwrap();
        vip.unbind("eth0.100", ip, 24).await.unwrap();
        assert!(vip.bound.read().await.is_empty());
    }

    #[tokio::test]
    async fn dry_run_unbind_all_with_vlan_interface_clears_all() {
        let vip = LocalVip::new(true);
        let table = vec![
            (VipAddr::host(ip4(10, 0, 0, 1)), "eth0.100".to_string()),
            (VipAddr::host(ip4(10, 0, 0, 2)), "eth0.100".to_string()),
        ];
        for (v, iface) in &table {
            vip.bind(iface, v.addr, v.prefix).await.unwrap();
        }
        assert_eq!(vip.bound.read().await.len(), 2);
        vip.unbind_all(&table, None, true, VipState::Backup).await;
        assert!(vip.bound.read().await.is_empty());
    }

    #[tokio::test]
    async fn dry_run_startup_cleanup_with_vlan_interface_leaves_bound_empty() {
        let vip = LocalVip::new(true);
        let table = vec![(VipAddr::host(ip4(10, 0, 0, 1)), "eth0.100".to_string())];
        vip.startup_cleanup(&table).await.unwrap();
        assert!(vip.bound.read().await.is_empty());
    }

    #[test]
    fn vip_state_strings_match_keepalived_convention() {
        assert_eq!(VipState::Master.as_str(), "MASTER");
        assert_eq!(VipState::Backup.as_str(), "BACKUP");
        assert_eq!(VipState::Fault.as_str(), "FAULT");
    }

    #[test]
    fn release_notify_state_is_backup_when_healthy_and_fault_when_unhealthy() {
        // FAULT only on local health failure; cluster events (no leader, stale raft) → BACKUP.
        assert_eq!(release_notify_state(true), VipState::Backup);
        assert_eq!(release_notify_state(false), VipState::Fault);
    }

    #[tokio::test]
    async fn fire_notify_script_spawns_exactly_one_task() {
        let rt = tokio::runtime::Handle::current();
        let before = rt.metrics().num_alive_tasks();
        // Path does not need to exist for the spawn itself to succeed (the task will fail).
        let handle = fire_notify_script("/nonexistent/notify", "10.0.0.1", VipState::Master, false);
        assert!(
            handle.is_some(),
            "fire_notify_script must return Some(handle) when not dry_run"
        );
        let after = rt.metrics().num_alive_tasks();
        assert!(after > before, "fire_notify_script must spawn a task");
        handle.unwrap().abort();
    }

    #[tokio::test]
    async fn fire_notify_script_dry_run_does_not_spawn() {
        let rt = tokio::runtime::Handle::current();
        let before = rt.metrics().num_alive_tasks();
        let handle = fire_notify_script("/nonexistent/notify", "10.0.0.1", VipState::Master, true);
        assert!(handle.is_none(), "dry_run must return None");
        let after = rt.metrics().num_alive_tasks();
        assert_eq!(after, before, "dry_run must not spawn a task");
    }
}
