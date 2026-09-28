//! Startup reclamation of addresses left behind by a previous process.
//!
//! Runs before the node joins Raft: every configured VIP and every crash orphan discovered
//! through the kernel ownership marker is removed so reconciliation starts from a clean host.

use super::LocalVip;
#[cfg(test)]
use super::effects::command_output_with_timeout;
use super::effects::{
    IP_COMMAND_TIMEOUT, ensure_failed_delete_is_absent, ip_family, presence_probe_command,
};
use super::ownership;
use crate::config::VipAddr;
#[cfg(test)]
use std::os::unix::process::ExitStatusExt;
use tokio::process::Command;

impl LocalVip {
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
        #[cfg(test)]
        let discovered = {
            let empty_output = || std::process::Output {
                status: std::process::ExitStatus::from_raw(0),
                stdout: b"[]".to_vec(),
                stderr: Vec::new(),
            };
            let addresses = match self.next_startup_discovery_result.lock().await.take() {
                Some(result) => result?,
                None => empty_output(),
            };
            let (ipv4_routes, ipv6_routes) = match self
                .next_startup_marker_discovery_results
                .lock()
                .await
                .take()
            {
                Some((ipv4, ipv6)) => (ipv4?, ipv6?),
                None => (empty_output(), empty_output()),
            };
            ownership::parse_discovery_outputs(
                addresses,
                ipv4_routes,
                ipv6_routes,
                self.ownership_marker.protocol(),
            )?
        };
        #[cfg(not(test))]
        let discovered =
            ownership::discover_owned_addresses(self.ownership_marker.protocol()).await?;
        let cleanup_targets = ownership::merge_cleanup_targets(vips, discovered.addresses);
        for (vip, iface) in &cleanup_targets {
            let (ip, prefix) = (vip.addr, vip.prefix);
            let mut delete = Command::new("ip");
            delete
                .args([
                    ip_family(ip),
                    "addr",
                    "del",
                    &format!("{ip}/{prefix}"),
                    "dev",
                    iface,
                ])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null());
            #[cfg(test)]
            let result = match self.next_startup_delete_result.lock().await.take() {
                Some(result) => result,
                None => Err(std::io::Error::other(
                    "no forced startup cleanup delete result",
                )),
            };
            #[cfg(not(test))]
            let result = crate::process::run_status(&mut delete, IP_COMMAND_TIMEOUT).await;
            match result {
                Ok(s) if s.success() => {
                    tracing::info!(
                        target: "keepafloatd::vip",
                        "startup_cleanup: reclaimed orphan {ip}/{prefix} on {iface}"
                    );
                }
                Ok(status) => {
                    let mut probe_command = presence_probe_command(iface, ip, prefix);
                    #[cfg(test)]
                    let probe = match self.next_startup_probe_result.lock().await.take() {
                        Some(result) => result,
                        None => {
                            command_output_with_timeout(probe_command.output(), IP_COMMAND_TIMEOUT)
                                .await
                        }
                    };
                    #[cfg(not(test))]
                    let probe =
                        crate::process::run_output(&mut probe_command, IP_COMMAND_TIMEOUT).await;
                    ensure_failed_delete_is_absent(status, probe)?;
                    tracing::debug!(
                        target: "keepafloatd::vip",
                        "startup_cleanup: {ip}/{prefix} on {iface} not present (ok)"
                    );
                }
                Err(e) => {
                    return Err(anyhow::anyhow!(
                        "startup_cleanup: spawn ip del {ip}/{prefix} on {iface}: {e}"
                    ));
                }
            }
        }
        for ip in discovered.marker_routes {
            self.ownership_marker.delete(ip).await.map_err(|error| {
                anyhow::anyhow!("startup_cleanup: remove ownership marker for {ip}: {error}")
            })?;
            tracing::debug!(
                target: "keepafloatd::vip",
                "startup_cleanup: removed ownership marker for {ip}"
            );
        }
        Ok(())
    }
}
