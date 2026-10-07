//! Linux `ip addr` bind/release, neighbor announcements, and the VIP reconciliation loop.
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

mod announce;
mod command;
mod effects;
mod notify;
mod ownership;
mod reconcile;
mod secondary;
mod startup;
pub(crate) mod takeover;
#[cfg(test)]
mod test_support;

pub(crate) use reconcile::RECONCILE_TICK;
pub use reconcile::run_reconcile_loop;
#[cfg(test)]
use reconcile::should_publish_release;
pub(crate) use secondary::warn_ipv4_secondary_removal;

use crate::config::VipAddr;
use crate::raft::store::VipAssignment;
use effects::presence_probe_command;
use effects::{IP_COMMAND_TIMEOUT, bind_command_arguments, delete_command, verify_delete_result};
#[cfg(test)]
use effects::{ensure_failed_delete_is_absent, ip_family, presence_probe_target};
use notify::VipState;
#[cfg(test)]
use notify::fire_notify_script;
#[cfg(test)]
use notify::fire_notify_script_with_timeout;
pub(crate) use notify::release_notify_state;
use std::collections::HashMap;
use std::collections::HashSet;
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

/// Apply Linux secondary addresses with `ip` and best-effort ARP/NA announcements.
///
/// All process invocations use `tokio::process::Command` so the daemon's tokio runtime is not
/// blocked while `ip`, `arping` or `ndptool` runs.
pub struct LocalVip {
    bound: RwLock<HashSet<IpAddr>>,
    pending_first_bind: RwLock<HashSet<IpAddr>>,
    announcements: announce::Announcements,
    notifications: notify::Notifications,
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
    runner: Arc<dyn command::CommandRunner>,
    #[cfg(test)]
    commands: Arc<command::scripted::ScriptedRunner>,
}

impl LocalVip {
    /// Wrap in [`Arc`] for use from async VIP reconciliation tasks.
    #[cfg(test)]
    pub fn new(dry_run: bool) -> Arc<Self> {
        Self::new_with_address_protocol(dry_run, crate::config::DEFAULT_VIP_ADDRESS_PROTOCOL)
    }

    /// Use a distinct protocol per co-located daemon so crash discovery cannot cross instances.
    pub fn new_with_address_protocol(dry_run: bool, address_protocol: u8) -> Arc<Self> {
        #[cfg(test)]
        let runner = Arc::new(command::scripted::ScriptedRunner::default());
        #[cfg(not(test))]
        let runner = Arc::new(command::SystemCommandRunner);
        Arc::new(Self {
            runner: runner.clone(),
            #[cfg(test)]
            commands: runner.clone(),
            bound: RwLock::new(HashSet::new()),
            pending_first_bind: RwLock::new(HashSet::new()),
            announcements: announce::Announcements::default(),
            notifications: notify::Notifications::default(),
            dry_run,
            #[cfg(test)]
            bind_command_delay_ms: std::sync::atomic::AtomicU64::new(0),
            #[cfg(test)]
            bind_completions: Mutex::new(HashMap::new()),
            #[cfg(test)]
            bind_starts: Mutex::new(HashMap::new()),
            #[cfg(test)]
            next_announcement: Mutex::new(None),
            ownership_marker: ownership::OwnershipMarker::new(address_protocol, runner),
        })
    }

    #[cfg(test)]
    pub(crate) async fn bind_attempts(&self, address: IpAddr) -> usize {
        self.bind_starts
            .lock()
            .await
            .get(&address)
            .copied()
            .unwrap_or(0)
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
        let bind_error = match self
            .runner
            .status(&mut address_command, IP_COMMAND_TIMEOUT)
            .await
        {
            Ok(status) if status.success() => None,
            Ok(status) => Some(anyhow::anyhow!("ip addr replace failed: {status}")),
            Err(error) => Some(anyhow::Error::from(error).context("VIP operation=address_replace")),
        };
        if let Some(bind_error) = bind_error {
            if should_clear_tracking_after_bind_failure(first_bind) {
                self.ownership_marker
                    .remove_after_failed_first_bind(ip, prefix)
                    .await
                    .map_err(|cleanup_error| {
                        anyhow::anyhow!("{bind_error:#}; {cleanup_error:#}")
                    })?;
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

    /// Start an owned background neighbor announcement. Failure is logged but never changes
    /// ownership; unbind cancels the command before removing the address.
    async fn announce(&self, iface: &str, ip: IpAddr) {
        if self.dry_run {
            return;
        }
        #[cfg(test)]
        if let Some(completed) = self.next_announcement.lock().await.take() {
            self.announcements
                .start(iface, ip, async move {
                    completed.await.map_err(std::io::Error::other)?;
                    Ok(std::process::ExitStatus::from_raw(0))
                })
                .await;
            return;
        }
        self.announcements.send(iface, ip).await;
    }

    /// Remove the address if it was previously added by this [`LocalVip`] instance on this host.
    /// `prefix` must match the prefix the address was bound with so the kernel del matches.
    pub async fn unbind(&self, iface: &str, ip: IpAddr, prefix: u8) -> anyhow::Result<()> {
        self.announcements.cancel(ip).await;
        if !self.bound.read().await.contains(&ip) {
            return Ok(());
        }
        if self.dry_run {
            tracing::info!(target: "keepafloatd::vip", "dry-run: would unbind {ip}/{prefix} on {iface}");
        } else {
            let mut command = delete_command(ip, prefix, iface);
            command.kill_on_drop(true);
            let status = self.runner.status(&mut command, IP_COMMAND_TIMEOUT).await?;
            verify_delete_result(status, async {
                let mut probe_command = presence_probe_command(ip, prefix);
                self.runner
                    .output(&mut probe_command, IP_COMMAND_TIMEOUT)
                    .await
            })
            .await?;
            self.ownership_marker.delete(ip).await?;
            tracing::info!(target: "keepafloatd::vip", "unbound {ip}/{prefix} on {iface}");
        }
        self.bound.write().await.remove(&ip);
        self.pending_first_bind.write().await.remove(&ip);
        Ok(())
    }

    /// Remove every address this instance currently has bound.
    ///
    /// Enqueue release hooks without delaying cleanup. At daemon stop, call
    /// `shutdown_notifications` after cleanup and ownership handoff to drain the worker.
    pub async fn unbind_all(
        &self,
        vips: &[(VipAddr, String)],
        notify: Option<&str>,
        dry_run: bool,
        shutdown_state: VipState,
    ) -> anyhow::Result<()> {
        self.unbind_all_with_progress(vips, notify, dry_run, shutdown_state, || {})
            .await
    }

    pub(crate) async fn unbind_all_with_progress(
        &self,
        vips: &[(VipAddr, String)],
        notify: Option<&str>,
        dry_run: bool,
        shutdown_state: VipState,
        mut progress: impl FnMut(),
    ) -> anyhow::Result<()> {
        let mut failures = Vec::new();
        for (vip, iface) in vips {
            progress();
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
            {
                self.notify_transition(script, &vip.addr.to_string(), shutdown_state, dry_run)
                    .await;
            }
        }
        anyhow::ensure!(
            failures.is_empty(),
            "shutdown left VIP cleanup failures: {}",
            failures.join("; ")
        );
        Ok(())
    }

    /// Submit a best-effort transition to the instance's ordered, bounded hook queue.
    pub(crate) async fn notify_transition(
        &self,
        script: &str,
        vip_addr: &str,
        state: VipState,
        dry_run: bool,
    ) {
        self.notifications
            .send(script, vip_addr, state, dry_run)
            .await;
    }

    /// Observe a fatal worker failure from the daemon's supervision select.
    pub(crate) async fn notification_failure(&self) -> String {
        self.notifications.failed().await
    }

    /// Stop accepting transitions, then drain or cancel and join the owned worker.
    pub(crate) async fn shutdown_notifications(&self) -> anyhow::Result<()> {
        self.notifications
            .shutdown(notify::SHUTDOWN_DRAIN_TIMEOUT)
            .await
    }

    /// Snapshot of the addresses this instance currently considers bound (tests only).
    #[cfg(test)]
    pub(crate) async fn bound_addrs(&self) -> Vec<IpAddr> {
        let mut v: Vec<IpAddr> = self.bound.read().await.iter().copied().collect();
        v.sort_unstable();
        v
    }
}

/// Immediate retries recover from interrupted commands without an unbounded shutdown loop.
const SHUTDOWN_UNBIND_ATTEMPTS: usize = 3;

/// Address and marker deletion, each with verification, for every permitted attempt.
pub(crate) const SHUTDOWN_VIP_BUDGET: std::time::Duration =
    effects::DELETE_BUDGET.saturating_mul(2 * SHUTDOWN_UNBIND_ATTEMPTS as u32);

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn command_characterization_retains_tracking_until_marker_absence() {
        for (address, prefix) in [("192.0.2.99", 24), ("2001:db8::99", 64)] {
            let ip: IpAddr = address.parse().unwrap();
            let vip = LocalVip::new_with_address_protocol(false, 245);
            vip.bound.write().await.insert(ip);
            vip.force_unbind_results(
                ("test0.200", ip, prefix),
                vec![
                    Ok(ExitStatus::from_raw(0)),
                    Ok(ExitStatus::from_raw(2 << 8)),
                ],
            )
            .await;
            vip.force_unbind_probe_results(
                ip,
                vec![Ok(Output {
                    status: ExitStatus::from_raw(0),
                    stdout: Vec::new(),
                    stderr: Vec::new(),
                })],
            )
            .await;
            vip.force_marker_delete_results(
                ip,
                vec![
                    Err(io::Error::new(
                        io::ErrorKind::Interrupted,
                        "marker interrupted",
                    )),
                    Ok(ExitStatus::from_raw(2 << 8)),
                ],
            )
            .await;
            vip.force_marker_delete_probe_results(
                ip,
                vec![Ok(Output {
                    status: ExitStatus::from_raw(0),
                    stdout: b"[]".to_vec(),
                    stderr: Vec::new(),
                })],
            )
            .await;

            assert!(vip.unbind("test0.200", ip, prefix).await.is_err());
            assert_eq!(vip.bound_addrs().await, vec![ip]);
            vip.unbind("test0.200", ip, prefix).await.unwrap();
            assert!(vip.bound_addrs().await.is_empty());
            assert_eq!(vip.remaining_forced_unbind_results().await, 0);
            assert_eq!(vip.remaining_marker_delete_results().await, 0);
        }
    }

    #[tokio::test]
    async fn bind_diagnostics_distinguish_marker_and_address_without_losing_io_kind() {
        for phase in ["marker_replace", "address_replace"] {
            let local = LocalVip::new(false);
            let address = "192.0.2.99".parse().unwrap();
            local.bound.write().await.insert(address);
            let timeout = std::io::Error::new(std::io::ErrorKind::TimedOut, "child wait expired");
            let results = if phase == "marker_replace" {
                vec![Err(timeout)]
            } else {
                vec![Ok(ExitStatus::from_raw(0)), Err(timeout)]
            };
            local
                .force_bind_results(("test0", address, 32), results)
                .await;
            let error = local.bind("test0", address, 32).await.unwrap_err();
            assert_eq!(
                error.downcast_ref::<std::io::Error>().unwrap().kind(),
                std::io::ErrorKind::TimedOut
            );
            assert!(format!("{error:#}").contains(phase), "{error:#}");
            assert!(local.bound.read().await.contains(&address));
        }
    }

    #[tokio::test]
    async fn shutdown_progress_covers_every_vip_and_all_verification_retries() {
        let local = LocalVip::new(false);
        let table: Vec<_> = (1..=20)
            .map(|suffix| (VipAddr::host(ip4(192, 0, 2, suffix)), "test0".to_owned()))
            .collect();
        for (vip, _) in &table {
            local.bound.write().await.insert(vip.addr);
            let mut address_deletes = Vec::new();
            let mut address_probes = Vec::new();
            let mut marker_deletes = Vec::new();
            let mut marker_probes = Vec::new();
            for attempt in 1..=super::SHUTDOWN_UNBIND_ATTEMPTS {
                address_deletes.push(Ok(ExitStatus::from_raw(2 << 8)));
                address_probes.push(Ok(Output {
                    status: ExitStatus::from_raw(0),
                    stdout: Vec::new(),
                    stderr: Vec::new(),
                }));
                marker_deletes.push(Ok(ExitStatus::from_raw(2 << 8)));
                let stdout = if attempt == super::SHUTDOWN_UNBIND_ATTEMPTS {
                    b"[]".to_vec()
                } else {
                    format!(
                        r#"[{{"type":"throw","dst":"{}","protocol":246,"table":10246}}]"#,
                        vip.addr
                    )
                    .into_bytes()
                };
                marker_probes.push(Ok(Output {
                    status: ExitStatus::from_raw(0),
                    stdout,
                    stderr: Vec::new(),
                }));
            }
            local
                .force_unbind_results(("test0", vip.addr, vip.prefix), address_deletes)
                .await;
            local
                .force_unbind_probe_results(vip.addr, address_probes)
                .await;
            local
                .force_marker_delete_results(vip.addr, marker_deletes)
                .await;
            local
                .force_marker_delete_probe_results(vip.addr, marker_probes)
                .await;
        }
        let mut checkpoints = 0;
        local
            .unbind_all_with_progress(&table, None, false, VipState::Backup, || {
                checkpoints += 1;
            })
            .await
            .unwrap();
        assert_eq!(checkpoints, table.len());
        assert!(local.bound_addrs().await.is_empty());
        assert_eq!(local.remaining_forced_unbind_results().await, 0);
        assert_eq!(local.remaining_marker_delete_results().await, 0);
        local.commands.assert_finished();
        assert_eq!(super::SHUTDOWN_VIP_BUDGET, Duration::from_secs(12));
    }

    #[tokio::test]
    async fn shutdown_progress_continues_after_exhausted_cleanup_without_claiming_success() {
        let local = LocalVip::new(false);
        let table: Vec<_> = (1..=2)
            .map(|suffix| (VipAddr::host(ip4(192, 0, 2, suffix)), "test0".to_owned()))
            .collect();
        for (vip, _) in &table {
            local.bound.write().await.insert(vip.addr);
            local
                .force_unbind_results(
                    ("test0", vip.addr, vip.prefix),
                    (0..3)
                        .map(|_| Err(io::Error::other("delete failed")))
                        .collect(),
                )
                .await;
        }
        let mut checkpoints = 0;
        let result = local
            .unbind_all_with_progress(&table, None, false, VipState::Backup, || {
                checkpoints += 1;
            })
            .await;
        assert!(result.is_err());
        assert_eq!(checkpoints, table.len());
        assert_eq!(local.bound_addrs().await.len(), 2);
        assert_eq!(local.remaining_forced_unbind_results().await, 0);
    }

    #[tokio::test]
    async fn pending_announcement_does_not_block_bind() {
        for (address, prefix) in [("192.0.2.99", 32), ("2001:db8::99", 128)] {
            let vip = super::LocalVip::new(false);
            let address = address.parse().unwrap();
            vip.force_next_bind_result(("lo", address, prefix), Ok(ExitStatus::from_raw(0)))
                .await;
            let (_finish, announcement) = tokio::sync::oneshot::channel();
            *vip.next_announcement.lock().await = Some(announcement);
            let mut bind = Box::pin(vip.bind("lo", address, prefix));
            assert!(
                matches!(
                    futures::poll!(bind.as_mut()),
                    std::task::Poll::Ready(Ok(()))
                ),
                "binding must not wait for the neighbor announcement"
            );
            assert!(vip.is_confirmed_bound(address).await);
        }
    }

    #[tokio::test]
    async fn address_reassertion_does_not_replace_a_pending_announcement() {
        for (address, prefix) in [("192.0.2.99", 32), ("2001:db8::99", 128)] {
            let vip = LocalVip::new(false);
            let address = address.parse().unwrap();
            vip.force_next_bind_result(("test0", address, prefix), Ok(ExitStatus::from_raw(0)))
                .await;
            let (first, announcement) = tokio::sync::oneshot::channel();
            *vip.next_announcement.lock().await = Some(announcement);
            vip.bind("test0", address, prefix).await.unwrap();
            let (_second, announcement) = tokio::sync::oneshot::channel();
            *vip.next_announcement.lock().await = Some(announcement);
            vip.force_next_bind_result(("test0", address, prefix), Ok(ExitStatus::from_raw(0)))
                .await;
            vip.bind("test0", address, prefix).await.unwrap();
            assert!(
                !first.is_closed(),
                "reassertion must not cancel the original announcement"
            );
            assert!(vip.next_announcement.lock().await.is_some());
        }
    }

    #[tokio::test]
    async fn unbind_cancels_announcement_before_address_removal() {
        for (address, prefix) in [("192.0.2.99", 32), ("2001:db8::99", 128)] {
            let vip = super::LocalVip::new(false);
            let address = address.parse().unwrap();
            vip.force_next_bind_result(("lo", address, prefix), Ok(ExitStatus::from_raw(0)))
                .await;
            let (mut finish, announcement) = tokio::sync::oneshot::channel();
            *vip.next_announcement.lock().await = Some(announcement);
            vip.bind("lo", address, prefix).await.unwrap();
            assert!(vip.is_confirmed_bound(address).await);
            let bound_guard = vip.bound.write().await;
            let mut unbind = Box::pin(vip.unbind("lo", address, prefix));
            assert!(futures::poll!(unbind.as_mut()).is_pending());
            finish.closed().await;
            assert!(bound_guard.contains(&address));
            drop(bound_guard);
            vip.force_unbind_results(("lo", address, prefix), vec![Ok(ExitStatus::from_raw(0))])
                .await;
            vip.force_marker_delete_results(address, vec![Ok(ExitStatus::from_raw(0))])
                .await;
            unbind.await.unwrap();
            assert!(vip.bound_addrs().await.is_empty());
        }
    }

    #[tokio::test]
    async fn dry_run_does_not_start_announcements_in_either_family() {
        for address in ["192.0.2.99", "2001:db8::99"] {
            let vip = LocalVip::new(true);
            let (_finish, announcement) = tokio::sync::oneshot::channel();
            *vip.next_announcement.lock().await = Some(announcement);
            vip.announce("test0", address.parse().unwrap()).await;
            assert!(vip.next_announcement.lock().await.is_some());
        }
    }

    #[tokio::test]
    async fn ipv6_bind_starts_announcement_and_unbind_cancels_it() {
        let vip = LocalVip::new(false);
        let address = "2001:db8::99".parse().unwrap();
        vip.force_next_bind_result(("test0", address, 128), Ok(ExitStatus::from_raw(0)))
            .await;
        let (mut finish, announcement) = tokio::sync::oneshot::channel();
        *vip.next_announcement.lock().await = Some(announcement);
        vip.bind("test0", address, 128).await.unwrap();
        assert!(vip.is_confirmed_bound(address).await);
        assert!(
            vip.next_announcement.lock().await.is_none(),
            "IPv6 must start an NA"
        );
        vip.force_unbind_results(("test0", address, 128), vec![Ok(ExitStatus::from_raw(0))])
            .await;
        vip.force_marker_delete_results(address, vec![Ok(ExitStatus::from_raw(0))])
            .await;
        vip.unbind("test0", address, 128).await.unwrap();
        finish.closed().await;
        assert!(vip.bound_addrs().await.is_empty());
    }
    use super::{
        LocalVip, VipAddr, VipAssignment, VipState, ensure_failed_delete_is_absent,
        fire_notify_script, fire_notify_script_with_timeout, presence_probe_target,
        release_notify_state, should_clear_tracking_after_bind_failure, should_publish_release,
        should_reannounce_after_release, startup_effects_may_arm,
    };
    use std::collections::HashMap;
    use std::io;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
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
        vip.force_next_bind_result(("lo", initial, 32), Ok(ExitStatus::from_raw(1 << 8)))
            .await;
        assert!(vip.bind("lo", initial, 32).await.is_err());
        assert!(!vip.bound.read().await.contains(&initial));

        let reasserted = ip4(10, 0, 0, 2);
        vip.bound.write().await.insert(reasserted);
        vip.force_next_bind_result(("lo", reasserted, 32), Ok(ExitStatus::from_raw(1 << 8)))
            .await;
        assert!(vip.bind("lo", reasserted, 32).await.is_err());
        assert!(vip.bound.read().await.contains(&reasserted));
        assert_eq!(
            vip.remaining_marker_delete_results().await,
            1,
            "a failed reassert must preserve its crash-ownership marker"
        );

        let spawn_failure = ip4(10, 0, 0, 3);
        vip.force_next_bind_result(
            ("lo", spawn_failure, 32),
            Err(io::Error::new(io::ErrorKind::NotFound, "missing ip")),
        )
        .await;
        assert!(vip.bind("lo", spawn_failure, 32).await.is_err());
        assert!(!vip.bound.read().await.contains(&spawn_failure));

        vip.force_next_bind_result(
            ("lo", reasserted, 32),
            Err(io::Error::new(io::ErrorKind::Interrupted, "SIGTERM")),
        )
        .await;
        assert!(vip.bind("lo", reasserted, 32).await.is_err());
        assert!(vip.bound.read().await.contains(&reasserted));
        assert_eq!(vip.remaining_marker_delete_results().await, 1);
    }

    #[tokio::test]
    async fn uncertain_first_bind_retains_tracking_and_marker_for_cleanup() {
        let vip = LocalVip::new(false);
        let address = ip4(10, 0, 0, 4);
        vip.force_next_bind_result(
            ("test0", address, 32),
            Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "netlink response timed out",
            )),
        )
        .await;
        vip.force_next_bind_presence_result(
            address,
            Ok(Output {
                status: ExitStatus::from_raw(0),
                stdout: b"address is present".to_vec(),
                stderr: Vec::new(),
            }),
        )
        .await;

        let error = vip.bind("test0", address, 32).await.unwrap_err();

        assert!(error.to_string().contains("marker retained"));
        assert!(error.to_string().contains("operation=address_replace"));
        assert!(
            error.to_string().contains("netlink response timed out"),
            "{error:#}"
        );
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
        vip.force_bind_results(
            ("test0", address, 32),
            vec![Ok(ExitStatus::from_raw(2 << 8))],
        )
        .await;

        let error = vip.bind("test0", address, 32).await.unwrap_err();

        assert!(error.to_string().contains("route marker replace"));
        assert!(!vip.bound.read().await.contains(&address));
    }

    #[tokio::test]
    async fn ambiguous_marker_creation_retains_cleanup_and_first_bind_semantics() {
        let vip = LocalVip::new(false);
        let address = ip4(10, 0, 0, 5);
        vip.force_bind_results(
            ("test0", address, 32),
            vec![Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "marker command timed out after applying the route",
            ))],
        )
        .await;

        assert!(vip.bind("test0", address, 32).await.is_err());
        assert!(
            vip.bound.read().await.contains(&address),
            "an ambiguous marker result must remain tracked for graceful cleanup"
        );
        assert!(!vip.is_confirmed_bound(address).await);

        vip.force_next_bind_result(("test0", address, 32), Ok(ExitStatus::from_raw(2 << 8)))
            .await;
        vip.force_next_bind_presence_result(
            address,
            Ok(Output {
                status: ExitStatus::from_raw(0),
                stdout: b"address is still present".to_vec(),
                stderr: Vec::new(),
            }),
        )
        .await;
        assert!(vip.bind("test0", address, 32).await.is_err());
        assert!(
            vip.bound.read().await.contains(&address),
            "a present address must retain pending first-bind cleanup"
        );
        assert_eq!(vip.remaining_marker_delete_results().await, 1);

        vip.force_next_bind_result(("test0", address, 32), Ok(ExitStatus::from_raw(2 << 8)))
            .await;
        assert!(vip.bind("test0", address, 32).await.is_err());
        assert!(!vip.bound.read().await.contains(&address));
    }

    #[tokio::test]
    async fn ambiguous_marker_retry_becomes_confirmed_only_after_address_success() {
        let vip = LocalVip::new(false);
        let address = ip4(10, 0, 0, 7);
        vip.force_bind_results(
            ("test0", address, 32),
            vec![Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "marker command result lost",
            ))],
        )
        .await;
        assert!(vip.bind("test0", address, 32).await.is_err());
        assert!(!vip.is_confirmed_bound(address).await);

        vip.force_bind_results(
            ("test0", address, 32),
            vec![Ok(ExitStatus::from_raw(0)), Ok(ExitStatus::from_raw(0))],
        )
        .await;
        vip.bind("test0", address, 32).await.unwrap();

        assert!(vip.is_confirmed_bound(address).await);
        assert!(!vip.pending_first_bind.read().await.contains(&address));
    }

    #[tokio::test]
    async fn ambiguous_marker_retry_retains_cleanup_when_address_probe_fails() {
        let vip = LocalVip::new(false);
        let address = ip4(10, 0, 0, 6);
        vip.force_bind_results(
            ("test0", address, 32),
            vec![Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "marker command result lost",
            ))],
        )
        .await;
        assert!(vip.bind("test0", address, 32).await.is_err());

        vip.force_next_bind_result(("test0", address, 32), Ok(ExitStatus::from_raw(2 << 8)))
            .await;
        vip.force_next_bind_presence_result(
            address,
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "address probe denied",
            )),
        )
        .await;

        assert!(vip.bind("test0", address, 32).await.is_err());
        assert!(vip.bound.read().await.contains(&address));
        assert_eq!(vip.remaining_marker_delete_results().await, 1);
    }

    #[tokio::test]
    async fn successful_bind_and_unbind_create_then_remove_the_marker() {
        let vip = LocalVip::new(false);
        let address = ip4(10, 0, 0, 5);
        vip.force_bind_results(
            ("test0", address, 32),
            vec![Ok(ExitStatus::from_raw(0)), Ok(ExitStatus::from_raw(0))],
        )
        .await;

        vip.bind("test0", address, 32).await.unwrap();
        assert!(vip.bound.read().await.contains(&address));

        vip.force_unbind_results(("test0", address, 32), vec![Ok(ExitStatus::from_raw(0))])
            .await;
        vip.force_marker_delete_results(address, vec![Ok(ExitStatus::from_raw(0))])
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
        vip.force_unbind_probe_results(addr, vec![Err(io::Error::other("presence unavailable"))])
            .await;
        vip.force_marker_delete_results(addr, vec![Ok(ExitStatus::from_raw(0))])
            .await;
        vip.force_unbind_results(
            ("test0", addr, 32),
            vec![Ok(ExitStatus::from_raw(15)), Ok(ExitStatus::from_raw(0))],
        )
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
        vip.force_marker_delete_results(addr, vec![Ok(ExitStatus::from_raw(0))])
            .await;
        vip.force_unbind_results(("test0", addr, 32), vec![Ok(ExitStatus::from_raw(2 << 8))])
            .await;
        vip.force_unbind_probe_results(
            addr,
            vec![Ok(Output {
                status: ExitStatus::from_raw(0),
                stdout: Vec::new(),
                stderr: Vec::new(),
            })],
        )
        .await;

        vip.unbind("test0", addr, 32).await.unwrap();

        assert!(vip.bound.read().await.is_empty());
    }

    fn delete_probe_output(success: bool, stdout: &[u8]) -> Output {
        Output {
            status: ExitStatus::from_raw(if success { 0 } else { 1 << 8 }),
            stdout: stdout.to_vec(),
            stderr: Vec::new(),
        }
    }

    #[tokio::test]
    async fn delete_verification_startup_reports_distinct_outcomes() {
        use std::sync::{Arc, Mutex};

        let test_name = "vip::tests::delete_verification_startup_reports_distinct_outcomes";
        if std::env::var("KEEPAFLOATD_VIP_DIAGNOSTIC_TEST").as_deref() != Ok(test_name) {
            let child = tokio::time::timeout(
                Duration::from_secs(3),
                tokio::process::Command::new(std::env::current_exe().unwrap())
                    .args(["--exact", test_name, "--nocapture", "--color=never"])
                    .env("KEEPAFLOATD_VIP_DIAGNOSTIC_TEST", test_name)
                    .stdin(std::process::Stdio::null())
                    .kill_on_drop(true)
                    .output(),
            )
            .await
            .expect("isolated VIP diagnostic test timed out")
            .unwrap();
            assert!(
                child.status.success(),
                "{}\n{}",
                String::from_utf8_lossy(&child.stdout),
                String::from_utf8_lossy(&child.stderr)
            );
            assert!(
                String::from_utf8_lossy(&child.stdout)
                    .contains(&format!("test {test_name} ... ok"))
            );
            return;
        }

        #[derive(Clone, Default)]
        struct LogBuffer(Arc<Mutex<Vec<u8>>>);

        impl io::Write for LogBuffer {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(bytes);
                Ok(bytes.len())
            }

            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        let buffer = LogBuffer::default();
        let writer = buffer.clone();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_env_filter("keepafloatd::vip=debug")
            .with_writer(move || writer.clone())
            .finish();
        // The child isolates process-wide callsite caches from parallel test subscribers.
        tracing::subscriber::set_global_default(subscriber).unwrap();
        for deleted in [true, false] {
            buffer.0.lock().unwrap().clear();
            let vip = LocalVip::new_with_address_protocol(false, 245);
            vip.force_startup_marker_discovery_results(
                Ok(delete_probe_output(
                    true,
                    br#"[{"type":"9","dst":"192.0.2.30","table":"10245","protocol":245}]"#,
                )),
                Ok(delete_probe_output(true, b"[]")),
            )
            .await;
            vip.force_next_startup_cleanup_results(
                ("test0", ip4(192, 0, 2, 30), 32),
                Ok(ExitStatus::from_raw(if deleted { 0 } else { 2 << 8 })),
                Some(Ok(delete_probe_output(true, b""))),
            )
            .await;
            vip.force_marker_delete_results(ip4(192, 0, 2, 30), vec![Ok(ExitStatus::from_raw(0))])
                .await;
            let table = [(VipAddr::host(ip4(192, 0, 2, 30)), "test0".into())];
            vip.startup_cleanup(&table).await.unwrap();

            let logs = String::from_utf8(buffer.0.lock().unwrap().clone()).unwrap();
            let reclaimed = "startup_cleanup: reclaimed orphan 192.0.2.30/32 on test0";
            let absent = "startup_cleanup: 192.0.2.30/32 on test0 not present (ok)";
            assert_eq!(logs.contains(reclaimed), deleted, "{logs}");
            assert_eq!(logs.contains(absent), !deleted, "{logs}");
            assert!(
                logs.contains("startup_cleanup: removed ownership marker for 192.0.2.30"),
                "{logs}"
            );
            assert_eq!(vip.remaining_marker_delete_results().await, 0);
        }
    }

    #[tokio::test]
    async fn delete_verification_success_leaves_probes_unused() {
        for (address, prefix) in [("192.0.2.30", 24), ("2001:db8::30", 64)] {
            let address = address.parse().unwrap();
            let vip = LocalVip::new(false);
            let table = vec![(
                VipAddr {
                    addr: address,
                    prefix,
                },
                "test0.200".into(),
            )];
            vip.force_next_startup_cleanup_results(
                ("test0.200", address, prefix),
                Ok(ExitStatus::from_raw(0)),
                Some(Err(io::Error::other("unused startup probe"))),
            )
            .await;
            vip.startup_cleanup(&table).await.unwrap();
            assert!(vip.remaining_address_probes(address) > 0);

            vip.bound.write().await.insert(address);
            vip.pending_first_bind.write().await.insert(address);
            vip.force_unbind_results(
                ("test0.200", address, prefix),
                vec![Ok(ExitStatus::from_raw(0))],
            )
            .await;
            vip.force_unbind_probe_results(
                address,
                vec![Err(io::Error::other("unused unbind probe"))],
            )
            .await;
            vip.force_marker_delete_results(address, vec![Ok(ExitStatus::from_raw(0))])
                .await;
            vip.unbind("test0.200", address, prefix).await.unwrap();
            assert_eq!(vip.remaining_address_probes(address), 1);
            assert_eq!(vip.remaining_marker_delete_results().await, 0);
            assert!(!vip.bound.read().await.contains(&address));
            assert!(!vip.pending_first_bind.read().await.contains(&address));
        }
    }

    #[tokio::test]
    async fn delete_verification_execution_errors_preserve_context_and_tracking() {
        for kind in [io::ErrorKind::NotFound, io::ErrorKind::TimedOut] {
            let vip = LocalVip::new(false);
            let address = ip4(192, 0, 2, 30);
            let table = vec![(VipAddr::host(address), "test0".into())];
            vip.force_next_startup_cleanup_results(
                ("test0", address, 32),
                Err(io::Error::new(kind, "delete unavailable")),
                Some(Err(io::Error::other("unused probe"))),
            )
            .await;
            assert_eq!(
                vip.startup_cleanup(&table).await.unwrap_err().to_string(),
                "startup_cleanup: spawn ip del 192.0.2.30/32 on test0: delete unavailable"
            );
            assert!(vip.remaining_address_probes(address) > 0);

            vip.bound.write().await.insert(address);
            vip.pending_first_bind.write().await.insert(address);
            vip.force_unbind_results(
                ("test0", address, 32),
                vec![Err(io::Error::new(kind, "delete unavailable"))],
            )
            .await;
            vip.force_unbind_probe_results(address, vec![Err(io::Error::other("unused probe"))])
                .await;
            vip.force_marker_delete_results(address, vec![Ok(ExitStatus::from_raw(0))])
                .await;
            let error = vip.unbind("test0", address, 32).await.unwrap_err();
            assert_eq!(error.downcast_ref::<io::Error>().unwrap().kind(), kind);
            assert_eq!(vip.remaining_address_probes(address), 1);
            assert_eq!(vip.remaining_marker_delete_results().await, 1);
            assert!(vip.bound.read().await.contains(&address));
            assert!(vip.pending_first_bind.read().await.contains(&address));
        }
    }

    #[tokio::test]
    async fn delete_verification_probe_failures_preserve_tracking_and_markers() {
        for startup in [false, true] {
            for case in 0..4 {
                let vip = LocalVip::new(false);
                let address = ip4(192, 0, 2, 30);
                let table = vec![(VipAddr::host(address), "test0".into())];
                let (probe, diagnostic) = match case {
                    0 => (
                        Ok(delete_probe_output(true, b"still present")),
                        "the address is still present",
                    ),
                    1 => (
                        Ok(delete_probe_output(false, b"")),
                        "presence verification failed",
                    ),
                    2 => (
                        Err(io::Error::new(
                            io::ErrorKind::PermissionDenied,
                            "probe denied",
                        )),
                        "presence verification could not run: probe denied",
                    ),
                    _ => (
                        Err(io::Error::new(io::ErrorKind::TimedOut, "probe timed out")),
                        "presence verification could not run: probe timed out",
                    ),
                };
                vip.bound.write().await.insert(address);
                vip.pending_first_bind.write().await.insert(address);
                vip.force_marker_delete_results(address, vec![Ok(ExitStatus::from_raw(0))])
                    .await;
                let result = if startup {
                    vip.force_startup_marker_discovery_results(
                        Ok(delete_probe_output(
                            true,
                            br#"[{"type":"9","dst":"192.0.2.30","table":"10246","protocol":246}]"#,
                        )),
                        Ok(delete_probe_output(true, b"[]")),
                    )
                    .await;
                    vip.force_next_startup_cleanup_results(
                        ("test0", address, 32),
                        Ok(ExitStatus::from_raw(2 << 8)),
                        Some(probe),
                    )
                    .await;
                    vip.startup_cleanup(&table).await
                } else {
                    vip.force_unbind_results(
                        ("test0", address, 32),
                        vec![Ok(ExitStatus::from_raw(2 << 8))],
                    )
                    .await;
                    vip.force_unbind_probe_results(address, vec![probe]).await;
                    vip.unbind("test0", address, 32).await
                };
                assert!(result.unwrap_err().to_string().contains(diagnostic));
                assert!(vip.bound.read().await.contains(&address));
                assert!(vip.pending_first_bind.read().await.contains(&address));
                assert_eq!(vip.remaining_marker_delete_results().await, 1);
            }
        }
    }

    #[tokio::test]
    async fn delete_verification_cancellation_preserves_tracking_and_markers() {
        let vip = LocalVip::new(false);
        let address = ip4(192, 0, 2, 30);
        vip.bound.write().await.insert(address);
        vip.pending_first_bind.write().await.insert(address);
        vip.force_unbind_results(
            ("test0", address, 32),
            vec![Ok(ExitStatus::from_raw(2 << 8))],
        )
        .await;
        vip.force_marker_delete_results(address, vec![Ok(ExitStatus::from_raw(0))])
            .await;
        let probe_guard = vip.pause_address_probe(address);
        {
            let mut unbind = Box::pin(vip.unbind("test0", address, 32));
            assert!(futures::poll!(&mut unbind).is_pending());
        }
        drop(probe_guard);
        assert_eq!(vip.remaining_forced_unbind_results().await, 0);
        assert_eq!(vip.remaining_marker_delete_results().await, 1);
        assert!(vip.bound.read().await.contains(&address));
        assert!(vip.pending_first_bind.read().await.contains(&address));

        let table = vec![(VipAddr::host(address), "test0".into())];
        vip.force_next_startup_cleanup_results(
            ("test0", address, 32),
            Ok(ExitStatus::from_raw(2 << 8)),
            None,
        )
        .await;
        let probe_guard = vip.pause_address_probe(address);
        {
            let mut cleanup = Box::pin(vip.startup_cleanup(&table));
            assert!(futures::poll!(&mut cleanup).is_pending());
        }
        drop(probe_guard);
        assert!(!vip.has_forced_startup_delete_result().await);
        assert_eq!(vip.remaining_marker_delete_results().await, 1);
        assert!(vip.bound.read().await.contains(&address));
    }

    #[tokio::test]
    async fn delete_verification_dry_run_does_not_consume_commands() {
        let vip = LocalVip::new(true);
        let address = ip4(192, 0, 2, 30);
        let table = vec![(VipAddr::host(address), "test0".into())];
        vip.force_next_startup_cleanup_results(
            ("test0", address, 32),
            Err(io::Error::other("unused delete")),
            Some(Err(io::Error::other("unused probe"))),
        )
        .await;
        vip.force_unbind_results(
            ("test0", address, 32),
            vec![Err(io::Error::other("unused delete"))],
        )
        .await;
        vip.force_unbind_probe_results(address, vec![Err(io::Error::other("unused probe"))])
            .await;
        vip.force_marker_delete_results(address, vec![Ok(ExitStatus::from_raw(0))])
            .await;
        vip.startup_cleanup(&table).await.unwrap();
        vip.bound.write().await.insert(address);
        vip.unbind("test0", address, 32).await.unwrap();
        assert!(vip.has_forced_startup_delete_result().await);
        assert!(vip.remaining_address_probes(address) > 0);
        assert_eq!(vip.remaining_forced_unbind_results().await, 1);
        assert_eq!(vip.remaining_address_probes(address), 1);
        assert_eq!(vip.remaining_marker_delete_results().await, 1);
        assert!(!vip.bound.read().await.contains(&address));
    }

    #[tokio::test]
    async fn failed_address_delete_does_not_remove_the_marker() {
        let vip = LocalVip::new(false);
        let address = ip4(10, 0, 0, 6);
        vip.bound.write().await.insert(address);
        vip.force_unbind_results(
            ("test0", address, 32),
            vec![Ok(ExitStatus::from_raw(2 << 8))],
        )
        .await;
        vip.force_unbind_probe_results(
            address,
            vec![Ok(Output {
                status: ExitStatus::from_raw(0),
                stdout: b"still present".to_vec(),
                stderr: Vec::new(),
            })],
        )
        .await;
        vip.force_marker_delete_results(address, vec![Ok(ExitStatus::from_raw(0))])
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
        vip.force_unbind_results(("test0", address, 32), vec![Ok(ExitStatus::from_raw(0))])
            .await;
        vip.force_marker_delete_results(address, vec![Ok(ExitStatus::from_raw(2 << 8))])
            .await;
        vip.force_marker_delete_probe_results(
            address,
            vec![Ok(Output {
                status: ExitStatus::from_raw(0),
                stdout: b"[]".to_vec(),
                stderr: Vec::new(),
            })],
        )
        .await;

        vip.unbind("test0", address, 32).await.unwrap();

        assert!(!vip.bound.read().await.contains(&address));
    }

    #[tokio::test]
    async fn failed_marker_delete_rejects_a_marker_that_is_still_present() {
        let vip = LocalVip::new_with_address_protocol(false, 246);
        let address = ip4(10, 0, 0, 8);
        vip.bound.write().await.insert(address);
        vip.force_unbind_results(("test0", address, 32), vec![Ok(ExitStatus::from_raw(0))])
            .await;
        vip.force_marker_delete_results(address, vec![Ok(ExitStatus::from_raw(2 << 8))])
            .await;
        vip.force_marker_delete_probe_results(
            address,
            vec![Ok(Output {
                status: ExitStatus::from_raw(0),
                stdout: br#"[{"type":"9","dst":"10.0.0.8","table":"10246","protocol":246}]"#
                    .to_vec(),
                stderr: Vec::new(),
            })],
        )
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
        vip.force_unbind_probe_results(
            addr,
            (0..3)
                .map(|_| Err(io::Error::other("presence unavailable")))
                .collect(),
        )
        .await;
        vip.force_unbind_results(
            ("test0", addr, 32),
            vec![
                Ok(ExitStatus::from_raw(15)),
                Ok(ExitStatus::from_raw(15)),
                Ok(ExitStatus::from_raw(15)),
                Ok(ExitStatus::from_raw(0)),
            ],
        )
        .await;

        let result = vip.unbind_all(&table, None, false, VipState::Backup).await;

        assert!(result.is_err());
        assert!(vip.bound.read().await.contains(&addr));
        assert_eq!(vip.remaining_forced_unbind_results().await, 1);
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
    async fn dry_run_os_unbind_all_enqueues_real_notify_tasks() {
        let vip = LocalVip::new(true);
        let table = vec![(VipAddr::host(ip4(10, 0, 0, 1)), "lo".to_string())];
        vip.bind("lo", table[0].0.addr, table[0].0.prefix)
            .await
            .unwrap();

        vip.unbind_all(&table, Some("/bin/true"), false, VipState::Backup)
            .await
            .unwrap();
        vip.shutdown_notifications().await.unwrap();

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

        vip.force_next_startup_cleanup_results(
            ("test0", table[0].0.addr, 32),
            Ok(ExitStatus::from_raw(0)),
            None,
        )
        .await;
        vip.startup_cleanup(&table).await.unwrap();

        let absent = Output {
            status: ExitStatus::from_raw(0),
            stdout: Vec::new(),
            stderr: Vec::new(),
        };
        vip.force_next_startup_cleanup_results(
            ("test0", table[0].0.addr, 32),
            Ok(ExitStatus::from_raw(2 << 8)),
            Some(Ok(absent)),
        )
        .await;
        vip.startup_cleanup(&table).await.unwrap();

        vip.force_next_startup_cleanup_results(
            ("test0", table[0].0.addr, 32),
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
        vip.force_next_startup_cleanup_results(
            ("eth0.200", ip4(192, 0, 2, 30), 24),
            Ok(ExitStatus::from_raw(0)),
            None,
        )
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
        vip.force_next_startup_cleanup_results(
            ("eth0.200", ip4(192, 0, 2, 30), 24),
            Ok(ExitStatus::from_raw(0)),
            None,
        )
        .await;
        vip.force_marker_delete_results(ip4(192, 0, 2, 30), vec![Ok(ExitStatus::from_raw(0))])
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
            ("eth0", ip4(192, 0, 2, 30), 32),
            Ok(ExitStatus::from_raw(2 << 8)),
            Some(Ok(success(b"address is still present"))),
        )
        .await;
        vip.force_marker_delete_results(ip4(192, 0, 2, 30), vec![Ok(ExitStatus::from_raw(0))])
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
            ("eth0", ip4(192, 0, 2, 30), 32),
            Ok(ExitStatus::from_raw(2 << 8)),
            Some(Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "address probe denied",
            ))),
        )
        .await;
        vip.force_marker_delete_results(ip4(192, 0, 2, 30), vec![Ok(ExitStatus::from_raw(0))])
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
        vip.force_next_startup_cleanup_results(
            ("test0", ip4(192, 0, 2, 99), 32),
            Ok(ExitStatus::from_raw(0)),
            None,
        )
        .await;
        vip.force_marker_delete_results(ip4(192, 0, 2, 99), vec![Ok(ExitStatus::from_raw(0))])
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
        vip.force_marker_delete_results(ip4(192, 0, 2, 99), vec![Ok(ExitStatus::from_raw(2 << 8))])
            .await;
        vip.force_marker_delete_probe_results(ip4(192, 0, 2, 99), vec![Ok(success(br#"[]"#))])
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
        vip.force_marker_delete_results(ip4(192, 0, 2, 99), vec![Ok(ExitStatus::from_raw(2 << 8))])
            .await;
        vip.force_marker_delete_probe_results(ip4(192, 0, 2, 99), vec![Ok(success(marker))])
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
        vip.force_marker_delete_results(ip4(192, 0, 2, 99), vec![Ok(ExitStatus::from_raw(2 << 8))])
            .await;
        vip.force_marker_delete_probe_results(
            ip4(192, 0, 2, 99),
            vec![Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "route probe denied",
            ))],
        )
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
        let fixture = super::notify::Fixture::new("printf started > started\nexec sleep 30");
        let marker = fixture.0.join("started");
        let handle = fire_notify_script_with_timeout(
            &fixture.script(),
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
    }

    #[tokio::test]
    async fn notify_timeout_kills_the_script_process_group() {
        let (logs, _guard) = crate::warning_limit::test_support::LogCapture::start("debug");
        let fixture = super::notify::Fixture::new(
            "echo $$ > parent.pid\nsleep 30 &\necho $! > child.pid\nwait",
        );
        let parent_file = fixture.0.join("parent.pid");
        let child_file = fixture.0.join("child.pid");
        // Keep fixture input writable to catch accidental direct execution.
        let _body_writer = std::fs::OpenOptions::new()
            .append(true)
            .open(fixture.0.join("body"))
            .unwrap();
        let handle = fire_notify_script_with_timeout(
            &fixture.script(),
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
            "notify script did not publish both process ids: {}",
            logs.text()
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
        assert!(exited, "notify timeout left a descendant process running");
    }
}
