//! Startup reclamation of addresses left behind by a previous process.
//!
//! Runs before the node joins Raft: every configured VIP and every crash orphan discovered
//! through the kernel ownership marker is removed so reconciliation starts from a clean host.

use super::LocalVip;
use super::effects::{
    DELETE_BUDGET, DeleteOutcome, IP_COMMAND_TIMEOUT, IP_OUTPUT_BUDGET, delete_command,
    presence_probe_command, verify_delete_result,
};
use super::ownership;
use crate::config::VipAddr;

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
        crate::stop_budget::checkpoint(IP_OUTPUT_BUDGET);
        let discovered = ownership::discover_owned_addresses(
            self.runner.as_ref(),
            self.ownership_marker.protocol(),
        )
        .await?;
        let cleanup_targets = ownership::merge_cleanup_targets(vips, discovered.addresses);
        for (vip, iface) in &cleanup_targets {
            crate::stop_budget::checkpoint(DELETE_BUDGET);
            let (ip, prefix) = (vip.addr, vip.prefix);
            let mut delete = delete_command(ip, prefix, iface);
            delete
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null());
            let result = self.runner.status(&mut delete, IP_COMMAND_TIMEOUT).await;
            let status = result.map_err(|error| {
                anyhow::anyhow!("startup_cleanup: spawn ip del {ip}/{prefix} on {iface}: {error}")
            })?;
            let outcome = verify_delete_result(status, async {
                let mut probe_command = presence_probe_command(ip, prefix);
                self.runner
                    .output(&mut probe_command, IP_COMMAND_TIMEOUT)
                    .await
            })
            .await?;
            match outcome {
                DeleteOutcome::Deleted => {
                    tracing::info!(
                        target: "keepafloatd::vip",
                        "startup_cleanup: reclaimed orphan {ip}/{prefix} on {iface}"
                    );
                }
                DeleteOutcome::AlreadyAbsent => {
                    tracing::debug!(
                        target: "keepafloatd::vip",
                        "startup_cleanup: {ip}/{prefix} on {iface} not present (ok)"
                    );
                }
            }
        }
        for ip in discovered.marker_routes {
            crate::stop_budget::checkpoint(DELETE_BUDGET);
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
