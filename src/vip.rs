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

mod effects;
mod notify;
mod ownership;
mod reconcile;
mod startup;
pub(crate) mod takeover;

pub use reconcile::run_reconcile_loop;
#[cfg(test)]
use reconcile::should_publish_release;

use crate::config::VipAddr;
use crate::raft::store::VipAssignment;
#[cfg(not(test))]
use effects::presence_probe_command;
#[cfg(test)]
use effects::presence_probe_target;
use effects::{
    IP_COMMAND_TIMEOUT, bind_command_arguments, ensure_failed_delete_is_absent, ip_family,
};
#[cfg(test)]
use effects::{command_output_with_timeout, command_status_with_timeout};
#[cfg(test)]
use notify::fire_notify_script_with_timeout;
pub(crate) use notify::release_notify_state;
use notify::{VipState, fire_notify_script};
use std::collections::HashMap;
use std::collections::HashSet;
#[cfg(test)]
use std::collections::VecDeque;
use std::net::IpAddr;
#[cfg(test)]
use std::os::unix::process::ExitStatusExt;
use std::sync::Arc;
use tokio::process::Command;
#[cfg(test)]
use tokio::sync::Mutex;
use tokio::sync::RwLock;

/// Clear a speculative tracking entry only when this was the first bind attempt. A failed
/// reassert can be a child process interrupted by the daemon's SIGTERM after the VIP was already
/// present; retaining that entry lets graceful shutdown still remove it (#22).
const fn should_clear_tracking_after_bind_failure(first_bind: bool) -> bool {
    first_bind
}

fn should_reannounce_after_release(
    assignment: &VipAssignment,
    vip: IpAddr,
    already_bound: bool,
    announced_release_generations: &HashMap<IpAddr, u64>,
) -> bool {
    already_bound
        && assignment.previous_holder.is_some()
        && assignment.previous_holder_released
        && announced_release_generations.get(&vip).copied() != Some(assignment.generation)
}

/// A diskless restart replays committed entries from index zero. Until the local state machine has
/// reached the leader-reported committed frontier, an intermediate prefix can still name this node
/// as a VIP holder even though a later committed handoff revoked it (#26).
const fn startup_effects_may_arm(
    has_leader: bool,
    last_applied_index: Option<u64>,
    cluster_committed_index: Option<u64>,
) -> bool {
    if !has_leader {
        return false;
    }
    match (last_applied_index, cluster_committed_index) {
        (Some(applied), Some(committed)) => applied >= committed,
        _ => false,
    }
}

/// Apply Linux secondary addresses with `ip` and optional IPv4 gratuitous ARP.
///
/// All process invocations use `tokio::process::Command` so the daemon's tokio runtime is not
/// blocked while `ip` or `arping` runs.
pub struct LocalVip {
    bound: RwLock<HashSet<IpAddr>>,
    pending_first_bind: RwLock<HashSet<IpAddr>>,
    dry_run: bool,
    #[cfg(test)]
    bind_command_delay_ms: std::sync::atomic::AtomicU64,
    #[cfg(test)]
    bind_completions: Mutex<HashMap<IpAddr, usize>>,
    #[cfg(test)]
    bind_starts: Mutex<HashMap<IpAddr, usize>>,
    #[cfg(test)]
    next_announcement: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    ownership_marker: ownership::OwnershipMarker,
    #[cfg(test)]
    next_bind_result: Mutex<Option<std::io::Result<std::process::ExitStatus>>>,
    #[cfg(test)]
    unbind_results: Mutex<VecDeque<std::io::Result<std::process::ExitStatus>>>,
    #[cfg(test)]
    unbind_probe_results: Mutex<VecDeque<std::io::Result<std::process::Output>>>,
    #[cfg(test)]
    next_startup_delete_result: Mutex<Option<std::io::Result<std::process::ExitStatus>>>,
    #[cfg(test)]
    next_startup_probe_result: Mutex<Option<std::io::Result<std::process::Output>>>,
    #[cfg(test)]
    next_startup_discovery_result: Mutex<Option<std::io::Result<std::process::Output>>>,
    #[cfg(test)]
    next_startup_marker_discovery_results: Mutex<
        Option<(
            std::io::Result<std::process::Output>,
            std::io::Result<std::process::Output>,
        )>,
    >,
}

impl LocalVip {
    /// Wrap in [`Arc`] for use from async VIP reconciliation tasks.
    #[cfg(test)]
    pub fn new(dry_run: bool) -> Arc<Self> {
        Self::new_with_address_protocol(dry_run, crate::config::DEFAULT_VIP_ADDRESS_PROTOCOL)
    }

    /// Use a distinct protocol per co-located daemon so crash discovery cannot cross instances.
    pub fn new_with_address_protocol(dry_run: bool, address_protocol: u8) -> Arc<Self> {
        Arc::new(Self {
            bound: RwLock::new(HashSet::new()),
            pending_first_bind: RwLock::new(HashSet::new()),
            dry_run,
            #[cfg(test)]
            bind_command_delay_ms: std::sync::atomic::AtomicU64::new(0),
            #[cfg(test)]
            bind_completions: Mutex::new(HashMap::new()),
            #[cfg(test)]
            bind_starts: Mutex::new(HashMap::new()),
            #[cfg(test)]
            next_announcement: Mutex::new(None),
            ownership_marker: ownership::OwnershipMarker::new(address_protocol),
            #[cfg(test)]
            next_bind_result: Mutex::new(None),
            #[cfg(test)]
            unbind_results: Mutex::new(VecDeque::new()),
            #[cfg(test)]
            unbind_probe_results: Mutex::new(VecDeque::new()),
            #[cfg(test)]
            next_startup_delete_result: Mutex::new(None),
            #[cfg(test)]
            next_startup_probe_result: Mutex::new(None),
            #[cfg(test)]
            next_startup_discovery_result: Mutex::new(None),
            #[cfg(test)]
            next_startup_marker_discovery_results: Mutex::new(None),
        })
    }

    #[cfg(test)]
    async fn force_next_bind_result(&self, result: std::io::Result<std::process::ExitStatus>) {
        let needs_cleanup = result.as_ref().map_or(true, |status| !status.success());
        self.ownership_marker
            .force_next_replace_result(Ok(std::process::ExitStatus::from_raw(0)))
            .await;
        *self.next_bind_result.lock().await = Some(result);
        if needs_cleanup {
            self.ownership_marker
                .force_next_bind_presence_result(Ok(std::process::Output {
                    status: std::process::ExitStatus::from_raw(0),
                    stdout: Vec::new(),
                    stderr: Vec::new(),
                }))
                .await;
            self.ownership_marker
                .force_delete_results(vec![Ok(std::process::ExitStatus::from_raw(0))])
                .await;
        }
    }

    #[cfg(test)]
    async fn force_next_bind_presence_result(&self, result: std::io::Result<std::process::Output>) {
        self.ownership_marker
            .force_next_bind_presence_result(result)
            .await;
    }

    #[cfg(test)]
    async fn force_bind_results(&self, results: Vec<std::io::Result<std::process::ExitStatus>>) {
        let mut results = results.into_iter();
        let marker = results
            .next()
            .unwrap_or_else(|| Err(std::io::Error::other("no forced marker replace result")));
        self.ownership_marker
            .force_next_replace_result(marker)
            .await;
        *self.next_bind_result.lock().await = results.next();
    }

    #[cfg(test)]
    async fn force_unbind_results(&self, results: Vec<std::io::Result<std::process::ExitStatus>>) {
        *self.unbind_results.lock().await = results.into();
    }

    #[cfg(test)]
    async fn remaining_forced_unbind_results(&self) -> usize {
        self.unbind_results.lock().await.len()
    }

    #[cfg(test)]
    async fn force_unbind_probe_results(
        &self,
        results: Vec<std::io::Result<std::process::Output>>,
    ) {
        *self.unbind_probe_results.lock().await = results.into();
    }

    #[cfg(test)]
    async fn force_next_startup_cleanup_results(
        &self,
        delete: std::io::Result<std::process::ExitStatus>,
        probe: Option<std::io::Result<std::process::Output>>,
    ) {
        *self.next_startup_delete_result.lock().await = Some(delete);
        *self.next_startup_probe_result.lock().await = probe;
    }

    #[cfg(test)]
    async fn force_startup_discovery_result(&self, result: std::io::Result<std::process::Output>) {
        *self.next_startup_discovery_result.lock().await = Some(result);
    }

    #[cfg(test)]
    async fn force_startup_marker_discovery_results(
        &self,
        ipv4: std::io::Result<std::process::Output>,
        ipv6: std::io::Result<std::process::Output>,
    ) {
        *self.next_startup_marker_discovery_results.lock().await = Some((ipv4, ipv6));
    }

    #[cfg(test)]
    async fn force_marker_delete_results(
        &self,
        results: Vec<std::io::Result<std::process::ExitStatus>>,
    ) {
        self.ownership_marker.force_delete_results(results).await;
    }

    #[cfg(test)]
    async fn force_marker_delete_probe_results(
        &self,
        results: Vec<std::io::Result<std::process::Output>>,
    ) {
        self.ownership_marker
            .force_delete_probe_results(results)
            .await;
    }

    #[cfg(test)]
    async fn remaining_marker_delete_results(&self) -> usize {
        self.ownership_marker.remaining_delete_results().await
    }

    #[cfg(test)]
    async fn has_forced_startup_delete_result(&self) -> bool {
        self.next_startup_delete_result.lock().await.is_some()
    }

    async fn bind_command_status(
        &self,
        _command: &mut Command,
    ) -> std::io::Result<std::process::ExitStatus> {
        #[cfg(test)]
        {
            self.next_bind_result
                .lock()
                .await
                .take()
                .unwrap_or_else(|| Err(std::io::Error::other("no forced bind command result")))
        }
        #[cfg(not(test))]
        {
            crate::process::run_status(_command, IP_COMMAND_TIMEOUT).await
        }
    }

    /// Add the secondary address on `iface` with prefix length `prefix`.
    ///
    /// Re-asserts the address on every call: the reconcile loop invokes this each tick while the
    /// node is the holder, and `ip addr replace` re-adds an address the kernel dropped out of band
    /// (link down/up, NetworkManager, `ip addr flush`, DHCP renew) instead of skipping it because
    /// the in-memory set still says we hold it. Gratuitous ARP fires only on a genuine first bind
    /// so re-assertion does not spam the segment.
    pub async fn bind(&self, iface: &str, ip: IpAddr, prefix: u8) -> anyhow::Result<()> {
        #[cfg(test)]
        {
            *self.bind_starts.lock().await.entry(ip).or_default() += 1;
        }
        let was_pending = self.pending_first_bind.read().await.contains(&ip);
        let first_bind = was_pending || !self.bound.read().await.contains(&ip);
        // Record ownership *before* the syscall so SIGTERM followed by vip_task.abort() still leaves
        // the address recorded for unbind_all to reclaim; otherwise
        // the child could finish attaching the address while the dropped future never inserted it,
        // leaking the VIP past graceful shutdown.
        self.bound.write().await.insert(ip);
        if first_bind {
            self.pending_first_bind.write().await.insert(ip);
        }
        if self.dry_run {
            #[cfg(test)]
            {
                let delay = self
                    .bind_command_delay_ms
                    .load(std::sync::atomic::Ordering::SeqCst);
                if delay > 0 {
                    for _ in 0..2 {
                        tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                    }
                }
                *self.bind_completions.lock().await.entry(ip).or_default() += 1;
            }
            self.pending_first_bind.write().await.remove(&ip);
            if first_bind {
                tracing::info!(target: "keepafloatd::vip", "dry-run: would bind {ip}/{prefix} on {iface}");
            }
            return Ok(());
        }
        if let Err(error) = self.ownership_marker.replace(ip).await {
            if should_clear_tracking_after_bind_failure(first_bind) {
                // #30: an I/O error may mean `ip route replace` reached the kernel before its
                // process timed out or lost its result. Keep cleanup armed and preserve first-bind
                // semantics for a retry until absence can be proven by the normal unbind path.
                if was_pending || error.downcast_ref::<std::io::Error>().is_some() {
                    self.pending_first_bind.write().await.insert(ip);
                } else {
                    self.bound.write().await.remove(&ip);
                    self.pending_first_bind.write().await.remove(&ip);
                }
            }
            return Err(error);
        }

        let mut address_command = Command::new("ip");
        address_command
            .args(bind_command_arguments(ip, prefix, iface))
            .kill_on_drop(true);
        let bind_error = match self.bind_command_status(&mut address_command).await {
            Ok(status) if status.success() => None,
            Ok(status) => Some(anyhow::anyhow!("ip addr replace failed: {status}")),
            Err(error) => Some(error.into()),
        };
        if let Some(bind_error) = bind_error {
            if should_clear_tracking_after_bind_failure(first_bind) {
                self.ownership_marker
                    .remove_after_failed_first_bind(ip, iface, prefix)
                    .await
                    .map_err(|cleanup_error| anyhow::anyhow!("{bind_error}; {cleanup_error}"))?;
                self.bound.write().await.remove(&ip);
                self.pending_first_bind.write().await.remove(&ip);
            }
            // #30: a failed periodic reassert can leave the already-bound address intact. Keep its
            // existing marker so a later crash still attributes and reclaims that address.
            return Err(bind_error);
        }
        if first_bind {
            self.announce(iface, ip).await;
            self.pending_first_bind.write().await.remove(&ip);
            tracing::info!(target: "keepafloatd::vip", "bound {ip}/{prefix} on {iface}");
        }
        Ok(())
    }

    async fn is_confirmed_bound(&self, ip: IpAddr) -> bool {
        self.bound.read().await.contains(&ip) && !self.pending_first_bind.read().await.contains(&ip)
    }

    /// Send gratuitous ARP for an already-bound IPv4 VIP. `arping` remains optional: inability to
    /// run it does not change consensus ownership or fail reconciliation.
    async fn announce(&self, iface: &str, ip: IpAddr) {
        #[cfg(test)]
        if let Some(completed) = self.next_announcement.lock().await.take() {
            let _ = completed.await;
            return;
        }
        if self.dry_run || !ip.is_ipv4() {
            return;
        }
        let mut command = Command::new("arping");
        command
            .args(["-q", "-U", "-c", "2", "-I", iface, &ip.to_string()])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true);
        let _ = crate::process::run_status(&mut command, IP_COMMAND_TIMEOUT).await;
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
            let mut command = Command::new("ip");
            command
                .args([
                    ip_family(ip),
                    "addr",
                    "del",
                    &format!("{ip}/{prefix}"),
                    "dev",
                    iface,
                ])
                .kill_on_drop(true);
            #[cfg(test)]
            let status = match self.unbind_results.lock().await.pop_front() {
                Some(result) => result?,
                None => command_status_with_timeout(command.status(), IP_COMMAND_TIMEOUT).await?,
            };
            #[cfg(not(test))]
            let status = crate::process::run_status(&mut command, IP_COMMAND_TIMEOUT).await?;
            if !status.success() {
                #[cfg(not(test))]
                let mut probe_command = presence_probe_command(iface, ip, prefix);
                #[cfg(test)]
                let probe = self
                    .unbind_probe_results
                    .lock()
                    .await
                    .pop_front()
                    .unwrap_or_else(|| {
                        Err(std::io::Error::other("no forced unbind presence result"))
                    });
                #[cfg(not(test))]
                let probe =
                    crate::process::run_output(&mut probe_command, IP_COMMAND_TIMEOUT).await;
                ensure_failed_delete_is_absent(status, probe)?;
            }
            self.ownership_marker.delete(ip).await?;
            tracing::info!(target: "keepafloatd::vip", "unbound {ip}/{prefix} on {iface}");
        }
        self.bound.write().await.remove(&ip);
        self.pending_first_bind.write().await.remove(&ip);
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
    ) -> anyhow::Result<()> {
        let mut handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();
        let mut failures = Vec::new();
        for (vip, iface) in vips {
            let was_bound = notify.is_some() && self.is_confirmed_bound(vip.addr).await;
            let mut unbound = false;
            for attempt in 1..=SHUTDOWN_UNBIND_ATTEMPTS {
                match self.unbind(iface, vip.addr, vip.prefix).await {
                    Ok(()) => {
                        unbound = true;
                        break;
                    }
                    Err(e) if attempt < SHUTDOWN_UNBIND_ATTEMPTS => {
                        tracing::warn!(
                            target: "keepafloatd::vip",
                            "unbind_all: {}/{} on {iface}: {e}; retrying ({attempt}/{SHUTDOWN_UNBIND_ATTEMPTS})",
                            vip.addr,
                            vip.prefix
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            target: "keepafloatd::vip",
                            "unbind_all: {}/{} on {iface}: {e}; exhausted {SHUTDOWN_UNBIND_ATTEMPTS} attempts",
                            vip.addr,
                            vip.prefix
                        );
                        failures.push(format!("{}/{} on {iface}: {e}", vip.addr, vip.prefix));
                    }
                }
            }
            if unbound
                && was_bound
                && let Some(script) = notify
                && let Some(h) =
                    fire_notify_script(script, &vip.addr.to_string(), shutdown_state, dry_run)
            {
                handles.push(h);
            }
        }
        for h in handles {
            let _ = h.await;
        }
        anyhow::ensure!(
            failures.is_empty(),
            "shutdown left VIP cleanup failures: {}",
            failures.join("; ")
        );
        Ok(())
    }

    /// Snapshot of the addresses this instance currently considers bound (tests only).
    #[cfg(test)]
    pub(crate) async fn bound_addrs(&self) -> Vec<IpAddr> {
        let mut v: Vec<IpAddr> = self.bound.read().await.iter().copied().collect();
        v.sort_unstable();
        v
    }
}

/// Systemd can signal a shutdown-time `ip addr del` child along with the daemon's cgroup. Immediate
/// retries run after that signal sweep and keep graceful cleanup bounded well below TimeoutStopSec.
const SHUTDOWN_UNBIND_ATTEMPTS: usize = 3;

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn cancelled_first_announcement_keeps_bind_pending_until_retry() {
        let vip = super::LocalVip::new(false);
        let address = "192.0.2.99".parse().unwrap();
        vip.force_next_bind_result(Ok(ExitStatus::from_raw(0)))
            .await;
        let (finish, announcement) = tokio::sync::oneshot::channel();
        *vip.next_announcement.lock().await = Some(announcement);
        let mut bind = Box::pin(vip.bind("lo", address, 32));
        assert!(futures::poll!(bind.as_mut()).is_pending());
        assert!(vip.bound_addrs().await.contains(&address));
        drop(bind);
        drop(finish);
        assert!(
            !vip.is_confirmed_bound(address).await,
            "an interrupted first announcement must remain pending for MASTER and ARP retry"
        );
        vip.force_next_bind_result(Ok(ExitStatus::from_raw(0)))
            .await;
        let (finish, announcement) = tokio::sync::oneshot::channel();
        *vip.next_announcement.lock().await = Some(announcement);
        finish.send(()).unwrap();
        vip.bind("lo", address, 32).await.unwrap();
        assert!(vip.is_confirmed_bound(address).await);
        assert!(
            vip.next_announcement.lock().await.is_none(),
            "retry must finish the announcement"
        );
    }
    use super::{
        LocalVip, VipAddr, VipAssignment, VipState, command_output_with_timeout,
        command_status_with_timeout, ensure_failed_delete_is_absent, fire_notify_script,
        fire_notify_script_with_timeout, presence_probe_target, release_notify_state,
        should_clear_tracking_after_bind_failure, should_publish_release,
        should_reannounce_after_release, startup_effects_may_arm,
    };
    use std::collections::HashMap;
    use std::io;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::process::ExitStatusExt;
    use std::process::{ExitStatus, Output};
    use std::time::Duration;

    fn ip4(a: u8, b: u8, c: u8, d: u8) -> IpAddr {
        IpAddr::V4(Ipv4Addr::new(a, b, c, d))
    }

    #[test]
    fn ip_family_selects_the_address_family_without_shelling_out() {
        assert_eq!(super::ip_family(ip4(192, 0, 2, 1)), "-4");
        assert_eq!(super::ip_family(IpAddr::V6(Ipv6Addr::LOCALHOST)), "-6");
    }

    #[test]
    fn failed_startup_delete_rejects_an_address_that_is_still_present() {
        let delete_status = ExitStatus::from_raw(2 << 8);
        let probe = Output {
            status: ExitStatus::from_raw(0),
            stdout: b"2: ens5 inet 192.0.2.210/32 scope global ens5\n".to_vec(),
            stderr: Vec::new(),
        };

        assert!(ensure_failed_delete_is_absent(delete_status, Ok(probe)).is_err());
    }

    #[test]
    fn failed_startup_delete_accepts_verified_absence() {
        let delete_status = ExitStatus::from_raw(2 << 8);
        let probe = Output {
            status: ExitStatus::from_raw(0),
            stdout: Vec::new(),
            stderr: Vec::new(),
        };

        ensure_failed_delete_is_absent(delete_status, Ok(probe)).unwrap();
    }

    #[test]
    fn failed_startup_delete_rejects_an_unverifiable_result() {
        let delete_status = ExitStatus::from_raw(2 << 8);
        let probe = Output {
            status: ExitStatus::from_raw(1 << 8),
            stdout: Vec::new(),
            stderr: Vec::new(),
        };

        assert!(ensure_failed_delete_is_absent(delete_status, Ok(probe)).is_err());
        assert!(
            ensure_failed_delete_is_absent(
                delete_status,
                Err(io::Error::new(io::ErrorKind::NotFound, "missing ip")),
            )
            .is_err()
        );
    }

    #[test]
    fn startup_presence_probe_targets_the_exact_host_not_its_subnet() {
        assert_eq!(
            presence_probe_target(ip4(192, 0, 2, 210), 24),
            "192.0.2.210"
        );
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
            &empty,
            true,
        ));
        // Some other node is the previous holder: nothing owed here.
        assert!(!should_publish_release(
            &handoff(Some(9), false, 4),
            1,
            vip,
            &empty,
            true,
        ));
        // No previous holder (first assignment): nothing to release.
        assert!(!should_publish_release(
            &handoff(None, false, 4),
            1,
            vip,
            &empty,
            true,
        ));
        // Already released in committed state: nothing owed.
        assert!(!should_publish_release(
            &handoff(Some(1), true, 4),
            1,
            vip,
            &empty,
            true,
        ));
    }

    #[test]
    fn failed_unbind_never_authorizes_a_replacement() {
        let vip = ip4(10, 0, 0, 1);
        assert!(!should_publish_release(
            &handoff(Some(1), false, 4),
            1,
            vip,
            &HashMap::new(),
            false,
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
            &acked_same,
            true,
        ));
        // Acked an older generation; a new handoff (gen 5) still owes an ack.
        let acked_old = HashMap::from([(vip, 4_u64)]);
        assert!(should_publish_release(
            &handoff(Some(1), false, 5),
            1,
            vip,
            &acked_old,
            true,
        ));
    }

    #[test]
    fn released_previous_holder_triggers_one_reannouncement_of_an_existing_bind() {
        let vip = ip4(10, 0, 0, 210);
        let released = handoff(Some(1), true, 4);
        let unreleased = handoff(Some(1), false, 4);
        let initial = handoff(None, true, 4);
        let mut announced = HashMap::new();

        assert!(should_reannounce_after_release(
            &released, vip, true, &announced
        ));
        assert!(!should_reannounce_after_release(
            &released, vip, false, &announced
        ));
        assert!(!should_reannounce_after_release(
            &unreleased,
            vip,
            true,
            &announced
        ));
        assert!(!should_reannounce_after_release(
            &initial, vip, true, &announced
        ));

        announced.insert(vip, released.generation);
        assert!(!should_reannounce_after_release(
            &released, vip, true, &announced
        ));
        assert!(should_reannounce_after_release(
            &handoff(Some(1), true, 5),
            vip,
            true,
            &announced
        ));
    }

    #[test]
    fn startup_effects_wait_for_the_leaders_committed_frontier() {
        assert!(!startup_effects_may_arm(false, Some(12), Some(12)));
        assert!(!startup_effects_may_arm(true, None, Some(12)));
        assert!(!startup_effects_may_arm(true, Some(11), Some(12)));
        assert!(!startup_effects_may_arm(true, Some(12), None));
        assert!(startup_effects_may_arm(true, Some(12), Some(12)));
        assert!(startup_effects_may_arm(true, Some(13), Some(12)));
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

    #[test]
    fn bind_failure_clears_only_a_new_tracking_entry() {
        assert!(should_clear_tracking_after_bind_failure(true));
        assert!(
            !should_clear_tracking_after_bind_failure(false),
            "an interrupted reassert must stay tracked so graceful shutdown removes the VIP"
        );
    }

    #[tokio::test]
    async fn failed_bind_results_preserve_only_preexisting_tracking() {
        let vip = LocalVip::new(false);
        let initial = ip4(10, 0, 0, 1);
        vip.force_next_bind_result(Ok(ExitStatus::from_raw(1 << 8)))
            .await;
        assert!(vip.bind("lo", initial, 32).await.is_err());
        assert!(!vip.bound.read().await.contains(&initial));

        let reasserted = ip4(10, 0, 0, 2);
        vip.bound.write().await.insert(reasserted);
        vip.force_next_bind_result(Ok(ExitStatus::from_raw(1 << 8)))
            .await;
        assert!(vip.bind("lo", reasserted, 32).await.is_err());
        assert!(vip.bound.read().await.contains(&reasserted));
        assert_eq!(
            vip.remaining_marker_delete_results().await,
            1,
            "a failed reassert must preserve its crash-ownership marker"
        );

        let spawn_failure = ip4(10, 0, 0, 3);
        vip.force_next_bind_result(Err(io::Error::new(io::ErrorKind::NotFound, "missing ip")))
            .await;
        assert!(vip.bind("lo", spawn_failure, 32).await.is_err());
        assert!(!vip.bound.read().await.contains(&spawn_failure));

        vip.force_next_bind_result(Err(io::Error::new(io::ErrorKind::Interrupted, "SIGTERM")))
            .await;
        assert!(vip.bind("lo", reasserted, 32).await.is_err());
        assert!(vip.bound.read().await.contains(&reasserted));
        assert_eq!(vip.remaining_marker_delete_results().await, 1);
    }

    #[tokio::test]
    async fn uncertain_first_bind_retains_tracking_and_marker_for_cleanup() {
        let vip = LocalVip::new(false);
        let address = ip4(10, 0, 0, 4);
        vip.force_next_bind_result(Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "netlink response timed out",
        )))
        .await;
        vip.force_next_bind_presence_result(Ok(Output {
            status: ExitStatus::from_raw(0),
            stdout: b"address is present".to_vec(),
            stderr: Vec::new(),
        }))
        .await;

        let error = vip.bind("test0", address, 32).await.unwrap_err();

        assert!(error.to_string().contains("marker retained"));
        assert!(vip.bound.read().await.contains(&address));
        assert!(
            vip.pending_first_bind.read().await.contains(&address),
            "an address with an ambiguous first-bind result must not be reported as confirmed"
        );
        assert!(!vip.is_confirmed_bound(address).await);
        assert_eq!(vip.remaining_marker_delete_results().await, 1);
    }

    #[tokio::test]
    async fn marker_route_failure_prevents_the_address_bind() {
        let vip = LocalVip::new(false);
        let address = ip4(10, 0, 0, 4);
        vip.force_bind_results(vec![Ok(ExitStatus::from_raw(2 << 8))])
            .await;

        let error = vip.bind("test0", address, 32).await.unwrap_err();

        assert!(error.to_string().contains("route marker replace"));
        assert!(!vip.bound.read().await.contains(&address));
    }

    #[tokio::test]
    async fn ambiguous_marker_creation_retains_cleanup_and_first_bind_semantics() {
        let vip = LocalVip::new(false);
        let address = ip4(10, 0, 0, 5);
        vip.force_bind_results(vec![Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "marker command timed out after applying the route",
        ))])
        .await;

        assert!(vip.bind("test0", address, 32).await.is_err());
        assert!(
            vip.bound.read().await.contains(&address),
            "an ambiguous marker result must remain tracked for graceful cleanup"
        );
        assert!(!vip.is_confirmed_bound(address).await);

        vip.force_next_bind_result(Ok(ExitStatus::from_raw(2 << 8)))
            .await;
        vip.force_next_bind_presence_result(Ok(Output {
            status: ExitStatus::from_raw(0),
            stdout: b"address is still present".to_vec(),
            stderr: Vec::new(),
        }))
        .await;
        assert!(vip.bind("test0", address, 32).await.is_err());
        assert!(
            vip.bound.read().await.contains(&address),
            "a present address must retain pending first-bind cleanup"
        );
        assert_eq!(vip.remaining_marker_delete_results().await, 1);

        vip.force_next_bind_result(Ok(ExitStatus::from_raw(2 << 8)))
            .await;
        assert!(vip.bind("test0", address, 32).await.is_err());
        assert!(!vip.bound.read().await.contains(&address));
    }

    #[tokio::test]
    async fn ambiguous_marker_retry_becomes_confirmed_only_after_address_success() {
        let vip = LocalVip::new(false);
        let address = ip4(10, 0, 0, 7);
        vip.force_bind_results(vec![Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "marker command result lost",
        ))])
        .await;
        assert!(vip.bind("test0", address, 32).await.is_err());
        assert!(!vip.is_confirmed_bound(address).await);

        vip.force_bind_results(vec![
            Ok(ExitStatus::from_raw(0)),
            Ok(ExitStatus::from_raw(0)),
        ])
        .await;
        vip.bind("test0", address, 32).await.unwrap();

        assert!(vip.is_confirmed_bound(address).await);
        assert!(!vip.pending_first_bind.read().await.contains(&address));
    }

    #[tokio::test]
    async fn ambiguous_marker_retry_retains_cleanup_when_address_probe_fails() {
        let vip = LocalVip::new(false);
        let address = ip4(10, 0, 0, 6);
        vip.force_bind_results(vec![Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "marker command result lost",
        ))])
        .await;
        assert!(vip.bind("test0", address, 32).await.is_err());

        vip.force_next_bind_result(Ok(ExitStatus::from_raw(2 << 8)))
            .await;
        vip.force_next_bind_presence_result(Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "address probe denied",
        )))
        .await;

        assert!(vip.bind("test0", address, 32).await.is_err());
        assert!(vip.bound.read().await.contains(&address));
        assert_eq!(vip.remaining_marker_delete_results().await, 1);
    }

    #[tokio::test]
    async fn successful_bind_and_unbind_create_then_remove_the_marker() {
        let vip = LocalVip::new(false);
        let address = ip4(10, 0, 0, 5);
        vip.force_bind_results(vec![
            Ok(ExitStatus::from_raw(0)),
            Ok(ExitStatus::from_raw(0)),
        ])
        .await;

        vip.bind("test0", address, 32).await.unwrap();
        assert!(vip.bound.read().await.contains(&address));

        vip.force_unbind_results(vec![Ok(ExitStatus::from_raw(0))])
            .await;
        vip.force_marker_delete_results(vec![Ok(ExitStatus::from_raw(0))])
            .await;
        vip.unbind("test0", address, 32).await.unwrap();

        assert!(!vip.bound.read().await.contains(&address));
        assert_eq!(vip.remaining_marker_delete_results().await, 0);
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
        vip.unbind_all(&table, None, true, VipState::Backup)
            .await
            .unwrap();
        assert!(vip.bound.read().await.is_empty());
    }

    #[tokio::test]
    async fn shutdown_unbind_retries_after_sigterm_interrupts_ip_delete() {
        let vip = LocalVip::new(false);
        let addr = ip4(10, 0, 0, 1);
        let table = vec![(VipAddr::host(addr), "test0".to_string())];
        vip.bound.write().await.insert(addr);
        vip.force_unbind_results(vec![
            Ok(ExitStatus::from_raw(15)),
            Ok(ExitStatus::from_raw(0)),
        ])
        .await;

        vip.unbind_all(&table, None, false, VipState::Backup)
            .await
            .unwrap();

        assert!(vip.bound.read().await.is_empty());
        assert_eq!(vip.remaining_forced_unbind_results().await, 0);
    }

    #[tokio::test]
    async fn steady_state_unbind_accepts_kernel_verified_absence() {
        let vip = LocalVip::new(false);
        let addr = ip4(10, 0, 0, 1);
        vip.bound.write().await.insert(addr);
        vip.force_unbind_results(vec![Ok(ExitStatus::from_raw(2 << 8))])
            .await;
        vip.force_unbind_probe_results(vec![Ok(Output {
            status: ExitStatus::from_raw(0),
            stdout: Vec::new(),
            stderr: Vec::new(),
        })])
        .await;

        vip.unbind("test0", addr, 32).await.unwrap();

        assert!(vip.bound.read().await.is_empty());
    }

    #[tokio::test]
    async fn failed_address_delete_does_not_remove_the_marker() {
        let vip = LocalVip::new(false);
        let address = ip4(10, 0, 0, 6);
        vip.bound.write().await.insert(address);
        vip.force_unbind_results(vec![Ok(ExitStatus::from_raw(2 << 8))])
            .await;
        vip.force_unbind_probe_results(vec![Ok(Output {
            status: ExitStatus::from_raw(0),
            stdout: b"still present".to_vec(),
            stderr: Vec::new(),
        })])
        .await;
        vip.force_marker_delete_results(vec![Ok(ExitStatus::from_raw(0))])
            .await;

        assert!(vip.unbind("test0", address, 32).await.is_err());

        assert!(vip.bound.read().await.contains(&address));
        assert_eq!(vip.remaining_marker_delete_results().await, 1);
    }

    #[tokio::test]
    async fn failed_marker_delete_accepts_verified_absence() {
        let vip = LocalVip::new(false);
        let address = ip4(10, 0, 0, 7);
        vip.bound.write().await.insert(address);
        vip.force_unbind_results(vec![Ok(ExitStatus::from_raw(0))])
            .await;
        vip.force_marker_delete_results(vec![Ok(ExitStatus::from_raw(2 << 8))])
            .await;
        vip.force_marker_delete_probe_results(vec![Ok(Output {
            status: ExitStatus::from_raw(0),
            stdout: b"[]".to_vec(),
            stderr: Vec::new(),
        })])
        .await;

        vip.unbind("test0", address, 32).await.unwrap();

        assert!(!vip.bound.read().await.contains(&address));
    }

    #[tokio::test]
    async fn failed_marker_delete_rejects_a_marker_that_is_still_present() {
        let vip = LocalVip::new_with_address_protocol(false, 246);
        let address = ip4(10, 0, 0, 8);
        vip.bound.write().await.insert(address);
        vip.force_unbind_results(vec![Ok(ExitStatus::from_raw(0))])
            .await;
        vip.force_marker_delete_results(vec![Ok(ExitStatus::from_raw(2 << 8))])
            .await;
        vip.force_marker_delete_probe_results(vec![Ok(Output {
            status: ExitStatus::from_raw(0),
            stdout: br#"[{"type":"9","dst":"10.0.0.8","table":"10246","protocol":246}]"#.to_vec(),
            stderr: Vec::new(),
        })])
        .await;

        assert!(vip.unbind("test0", address, 32).await.is_err());

        assert!(vip.bound.read().await.contains(&address));
    }

    #[tokio::test]
    async fn shutdown_unbind_stops_after_the_bounded_attempt_count() {
        let vip = LocalVip::new(false);
        let addr = ip4(10, 0, 0, 1);
        let table = vec![(VipAddr::host(addr), "test0".to_string())];
        vip.bound.write().await.insert(addr);
        vip.force_unbind_results(vec![
            Ok(ExitStatus::from_raw(15)),
            Ok(ExitStatus::from_raw(15)),
            Ok(ExitStatus::from_raw(15)),
            Ok(ExitStatus::from_raw(0)),
        ])
        .await;

        let result = vip.unbind_all(&table, None, false, VipState::Backup).await;

        assert!(result.is_err());
        assert!(vip.bound.read().await.contains(&addr));
        assert_eq!(vip.remaining_forced_unbind_results().await, 1);
    }

    #[tokio::test]
    async fn ip_command_status_timeout_is_reported_as_a_timed_out_error() {
        let result = command_status_with_timeout(
            std::future::pending::<io::Result<ExitStatus>>(),
            Duration::from_millis(1),
        )
        .await;

        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::TimedOut);
    }

    #[tokio::test]
    async fn ip_command_output_timeout_is_reported_as_a_timed_out_error() {
        let result = command_output_with_timeout(
            std::future::pending::<io::Result<Output>>(),
            Duration::from_millis(1),
        )
        .await;

        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::TimedOut);
    }

    #[tokio::test]
    async fn dry_run_unbind_all_exercises_notify_without_spawning() {
        let vip = LocalVip::new(true);
        let table = vec![(VipAddr::host(ip4(10, 0, 0, 1)), "lo".to_string())];
        vip.bind("lo", table[0].0.addr, table[0].0.prefix)
            .await
            .unwrap();

        vip.unbind_all(&table, Some("/nonexistent/notify"), true, VipState::Backup)
            .await
            .unwrap();

        assert!(vip.bound.read().await.is_empty());
    }

    #[tokio::test]
    async fn dry_run_os_unbind_all_awaits_real_notify_tasks() {
        let vip = LocalVip::new(true);
        let table = vec![(VipAddr::host(ip4(10, 0, 0, 1)), "lo".to_string())];
        vip.bind("lo", table[0].0.addr, table[0].0.prefix)
            .await
            .unwrap();

        vip.unbind_all(&table, Some("/bin/true"), false, VipState::Backup)
            .await
            .unwrap();

        assert!(vip.bound.read().await.is_empty());
    }

    #[tokio::test]
    async fn dry_run_startup_cleanup_is_noop_and_leaves_bound_empty() {
        let vip = LocalVip::new(true);
        let table = vec![(VipAddr::host(ip4(10, 0, 0, 1)), "lo".to_string())];
        vip.startup_cleanup(&table).await.unwrap();
        assert!(vip.bound.read().await.is_empty());
    }

    #[tokio::test]
    async fn startup_cleanup_handles_delete_success_verified_absence_and_spawn_failure() {
        let vip = LocalVip::new(false);
        let table = vec![(VipAddr::host(ip4(192, 0, 2, 1)), "test0".to_string())];

        vip.force_next_startup_cleanup_results(Ok(ExitStatus::from_raw(0)), None)
            .await;
        vip.startup_cleanup(&table).await.unwrap();

        let absent = Output {
            status: ExitStatus::from_raw(0),
            stdout: Vec::new(),
            stderr: Vec::new(),
        };
        vip.force_next_startup_cleanup_results(Ok(ExitStatus::from_raw(2 << 8)), Some(Ok(absent)))
            .await;
        vip.startup_cleanup(&table).await.unwrap();

        vip.force_next_startup_cleanup_results(
            Err(io::Error::new(io::ErrorKind::NotFound, "missing ip")),
            None,
        )
        .await;
        assert!(vip.startup_cleanup(&table).await.is_err());
    }

    #[tokio::test]
    async fn startup_cleanup_reclaims_only_this_instance_marker() {
        let vip = LocalVip::new_with_address_protocol(false, 245);
        let discovery = Output {
            status: ExitStatus::from_raw(0),
            stdout: br#"[{"ifname":"eth0.200","addr_info":[
                {"family":"inet","local":"192.0.2.30","prefixlen":24,"protocol":"0xf5"},
                {"family":"inet","local":"192.0.2.31","prefixlen":24},
                {"family":"inet","local":"192.0.2.32","prefixlen":24,"protocol":"0xf6"}
            ]}]"#
                .to_vec(),
            stderr: Vec::new(),
        };
        vip.force_startup_discovery_result(Ok(discovery)).await;
        vip.force_next_startup_cleanup_results(Ok(ExitStatus::from_raw(0)), None)
            .await;

        vip.startup_cleanup(&[]).await.unwrap();
        assert!(!vip.has_forced_startup_delete_result().await);
    }

    #[tokio::test]
    async fn startup_cleanup_removes_route_marked_address_and_marker() {
        let vip = LocalVip::new_with_address_protocol(false, 245);
        let success = |stdout: &[u8]| Output {
            status: ExitStatus::from_raw(0),
            stdout: stdout.to_vec(),
            stderr: Vec::new(),
        };
        vip.force_startup_discovery_result(Ok(success(
            br#"[{"ifname":"eth0.200","addr_info":[
                {"family":"inet","local":"192.0.2.30","prefixlen":24}
            ]}]"#,
        )))
        .await;
        vip.force_startup_marker_discovery_results(
            Ok(success(
                br#"[{"type":"9","dst":"192.0.2.30","table":"10245","protocol":"245"}]"#,
            )),
            Ok(success(br#"[]"#)),
        )
        .await;
        vip.force_next_startup_cleanup_results(Ok(ExitStatus::from_raw(0)), None)
            .await;
        vip.force_marker_delete_results(vec![Ok(ExitStatus::from_raw(0))])
            .await;

        vip.startup_cleanup(&[]).await.unwrap();

        assert!(!vip.has_forced_startup_delete_result().await);
        assert_eq!(vip.remaining_marker_delete_results().await, 0);
    }

    #[tokio::test]
    async fn startup_discovered_marker_is_retained_when_address_remains() {
        let vip = LocalVip::new_with_address_protocol(false, 245);
        let success = |stdout: &[u8]| Output {
            status: ExitStatus::from_raw(0),
            stdout: stdout.to_vec(),
            stderr: Vec::new(),
        };
        vip.force_startup_discovery_result(Ok(success(
            br#"[{"ifname":"eth0","addr_info":[
                {"family":"inet","local":"192.0.2.30","prefixlen":32}
            ]}]"#,
        )))
        .await;
        vip.force_startup_marker_discovery_results(
            Ok(success(
                br#"[{"type":"9","dst":"192.0.2.30","table":"10245","protocol":245}]"#,
            )),
            Ok(success(br#"[]"#)),
        )
        .await;
        vip.force_next_startup_cleanup_results(
            Ok(ExitStatus::from_raw(2 << 8)),
            Some(Ok(success(b"address is still present"))),
        )
        .await;
        vip.force_marker_delete_results(vec![Ok(ExitStatus::from_raw(0))])
            .await;

        assert!(vip.startup_cleanup(&[]).await.is_err());
        assert_eq!(
            vip.remaining_marker_delete_results().await,
            1,
            "startup must not delete ownership evidence while its address remains"
        );
    }

    #[tokio::test]
    async fn startup_discovered_marker_is_retained_when_address_probe_fails() {
        let vip = LocalVip::new_with_address_protocol(false, 245);
        let success = |stdout: &[u8]| Output {
            status: ExitStatus::from_raw(0),
            stdout: stdout.to_vec(),
            stderr: Vec::new(),
        };
        vip.force_startup_discovery_result(Ok(success(
            br#"[{"ifname":"eth0","addr_info":[
                {"family":"inet","local":"192.0.2.30","prefixlen":32}
            ]}]"#,
        )))
        .await;
        vip.force_startup_marker_discovery_results(
            Ok(success(
                br#"[{"type":"9","dst":"192.0.2.30","table":"10245","protocol":245}]"#,
            )),
            Ok(success(br#"[]"#)),
        )
        .await;
        vip.force_next_startup_cleanup_results(
            Ok(ExitStatus::from_raw(2 << 8)),
            Some(Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "address probe denied",
            ))),
        )
        .await;
        vip.force_marker_delete_results(vec![Ok(ExitStatus::from_raw(0))])
            .await;

        assert!(vip.startup_cleanup(&[]).await.is_err());
        assert_eq!(vip.remaining_marker_delete_results().await, 1);
    }

    #[tokio::test]
    async fn startup_cleanup_removes_marker_left_before_address_bind() {
        let vip = LocalVip::new_with_address_protocol(false, 245);
        let success = |stdout: &[u8]| Output {
            status: ExitStatus::from_raw(0),
            stdout: stdout.to_vec(),
            stderr: Vec::new(),
        };
        vip.force_startup_discovery_result(Ok(success(br#"[]"#)))
            .await;
        vip.force_startup_marker_discovery_results(
            Ok(success(
                br#"[{"type":"9","dst":"192.0.2.99","table":"10245","protocol":245}]"#,
            )),
            Ok(success(br#"[]"#)),
        )
        .await;
        vip.force_next_startup_cleanup_results(Ok(ExitStatus::from_raw(0)), None)
            .await;
        vip.force_marker_delete_results(vec![Ok(ExitStatus::from_raw(0))])
            .await;

        vip.startup_cleanup(&[]).await.unwrap();

        assert!(vip.has_forced_startup_delete_result().await);
        assert_eq!(vip.remaining_marker_delete_results().await, 0);
    }

    #[tokio::test]
    async fn startup_marker_delete_failure_accepts_verified_absence() {
        let vip = LocalVip::new_with_address_protocol(false, 245);
        let success = |stdout: &[u8]| Output {
            status: ExitStatus::from_raw(0),
            stdout: stdout.to_vec(),
            stderr: Vec::new(),
        };
        vip.force_startup_discovery_result(Ok(success(br#"[]"#)))
            .await;
        vip.force_startup_marker_discovery_results(
            Ok(success(
                br#"[{"type":"9","dst":"192.0.2.99","table":"10245","protocol":245}]"#,
            )),
            Ok(success(br#"[]"#)),
        )
        .await;
        vip.force_marker_delete_results(vec![Ok(ExitStatus::from_raw(2 << 8))])
            .await;
        vip.force_marker_delete_probe_results(vec![Ok(success(br#"[]"#))])
            .await;

        vip.startup_cleanup(&[]).await.unwrap();
    }

    #[tokio::test]
    async fn startup_marker_delete_failure_rejects_a_present_marker() {
        let vip = LocalVip::new_with_address_protocol(false, 245);
        let success = |stdout: &[u8]| Output {
            status: ExitStatus::from_raw(0),
            stdout: stdout.to_vec(),
            stderr: Vec::new(),
        };
        let marker = br#"[{"type":"9","dst":"192.0.2.99","table":"10245","protocol":245}]"#;
        vip.force_startup_discovery_result(Ok(success(br#"[]"#)))
            .await;
        vip.force_startup_marker_discovery_results(Ok(success(marker)), Ok(success(br#"[]"#)))
            .await;
        vip.force_marker_delete_results(vec![Ok(ExitStatus::from_raw(2 << 8))])
            .await;
        vip.force_marker_delete_probe_results(vec![Ok(success(marker))])
            .await;

        assert!(vip.startup_cleanup(&[]).await.is_err());
    }

    #[tokio::test]
    async fn startup_marker_delete_failure_rejects_an_unreadable_probe() {
        let vip = LocalVip::new_with_address_protocol(false, 245);
        let success = |stdout: &[u8]| Output {
            status: ExitStatus::from_raw(0),
            stdout: stdout.to_vec(),
            stderr: Vec::new(),
        };
        vip.force_startup_discovery_result(Ok(success(br#"[]"#)))
            .await;
        vip.force_startup_marker_discovery_results(
            Ok(success(
                br#"[{"type":"9","dst":"192.0.2.99","table":"10245","protocol":245}]"#,
            )),
            Ok(success(br#"[]"#)),
        )
        .await;
        vip.force_marker_delete_results(vec![Ok(ExitStatus::from_raw(2 << 8))])
            .await;
        vip.force_marker_delete_probe_results(vec![Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "route probe denied",
        ))])
        .await;

        assert!(vip.startup_cleanup(&[]).await.is_err());
    }

    // VLAN sub-interface tests - verify that bind/unbind/startup_cleanup/unbind_all work
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
        vip.unbind_all(&table, None, true, VipState::Backup)
            .await
            .unwrap();
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
        // FAULT is only for local health failures; cluster events use BACKUP.
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

    #[tokio::test]
    async fn notify_task_completes_for_success_nonzero_and_spawn_failure() {
        for script in ["/bin/true", "/bin/false", "/nonexistent/notify"] {
            fire_notify_script(script, "192.0.2.1", VipState::Master, false)
                .expect("non-dry-run notify must spawn")
                .await
                .expect("notify wrapper task must not panic");
        }
    }

    #[tokio::test]
    async fn notify_task_returns_when_hanging_script_hits_timeout() {
        static NOTIFY_TIMEOUT_TEST_ID: std::sync::atomic::AtomicU64 =
            std::sync::atomic::AtomicU64::new(0);
        let test_id = NOTIFY_TIMEOUT_TEST_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!(
            "keepafloatd_notify_timeout_{}_{}",
            std::process::id(),
            test_id
        ));
        std::fs::create_dir(&dir).unwrap();
        let script = dir.join("notify.sh");
        let marker = dir.join("started");
        std::fs::write(
            &script,
            format!(
                "#!/bin/sh\nprintf started > '{}'\nexec sleep 30\n",
                marker.display()
            ),
        )
        .unwrap();
        let mut permissions = std::fs::metadata(&script).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&script, permissions).unwrap();

        let handle = fire_notify_script_with_timeout(
            script.to_str().unwrap(),
            "192.0.2.1",
            VipState::Master,
            false,
            Duration::from_secs(2),
        )
        .expect("non-dry-run notify must spawn");
        let mut started = false;
        for _ in 0..100 {
            if marker.exists() {
                started = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(started, "notify script did not start before its timeout");
        tokio::time::timeout(Duration::from_secs(4), handle)
            .await
            .expect("notify task did not honor its two-second subprocess timeout")
            .expect("notify wrapper task must not panic");
        let marker_contents = tokio::fs::read_to_string(&marker)
            .await
            .expect("notify script did not start");
        assert_eq!(marker_contents, "started");

        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn notify_timeout_kills_the_script_process_group() {
        let dir = std::env::temp_dir().join(format!(
            "keepafloatd_notify_descendants_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&dir).unwrap();
        let script = dir.join("notify.sh");
        let parent_file = dir.join("parent.pid");
        let child_file = dir.join("child.pid");
        std::fs::write(
            &script,
            format!(
                "#!/bin/sh\necho $$ > '{}'\nsleep 30 &\necho $! > '{}'\nwait\n",
                parent_file.display(),
                child_file.display()
            ),
        )
        .unwrap();
        let mut permissions = std::fs::metadata(&script).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&script, permissions).unwrap();

        let handle = fire_notify_script_with_timeout(
            script.to_str().unwrap(),
            "192.0.2.1",
            VipState::Master,
            false,
            Duration::from_secs(2),
        )
        .unwrap();
        let mut pids = Vec::new();
        for path in [&parent_file, &child_file] {
            for _ in 0..100 {
                if let Ok(contents) = tokio::fs::read_to_string(path).await {
                    pids.push(contents.trim().parse::<u32>().unwrap());
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
        assert_eq!(
            pids.len(),
            2,
            "notify script did not publish both process ids"
        );
        tokio::time::timeout(Duration::from_secs(4), handle)
            .await
            .expect("notify timeout wrapper hung")
            .unwrap();

        let mut exited = false;
        for _ in 0..100 {
            if pids
                .iter()
                .all(|pid| !std::path::Path::new(&format!("/proc/{pid}")).exists())
            {
                exited = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        for pid in &pids {
            if std::path::Path::new(&format!("/proc/{pid}")).exists() {
                let _ = tokio::process::Command::new("/bin/kill")
                    .args(["-KILL", &pid.to_string()])
                    .status()
                    .await;
            }
        }
        std::fs::remove_dir_all(dir).unwrap();
        assert!(exited, "notify timeout left a descendant process running");
    }
}
