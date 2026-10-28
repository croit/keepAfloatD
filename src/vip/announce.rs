//! Owned, best-effort neighbor announcements, independent of address reconciliation.

use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::net::IpAddr;
use std::process::{ExitStatus, Stdio};
use std::sync::Arc;
use std::time::Duration;
use tokio::process::Command;
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tracing::instrument::WithSubscriber;

// Two default-interval packets need longer than an address mutation's budget.
const ANNOUNCEMENT_TIMEOUT: Duration = Duration::from_secs(3);

type PendingAnnouncements = Arc<Mutex<JoinSet<()>>>;

#[derive(Default)]
pub(super) struct Announcements {
    // Keep one slot per configured VIP so an interrupted waiter cannot orphan its tasks.
    tasks: Mutex<HashMap<IpAddr, PendingAnnouncements>>,
}

impl Announcements {
    #[cfg(test)]
    pub(super) async fn send(&self, iface: &str, ip: IpAddr) {
        let mut command = command(iface, ip);
        self.start(iface, ip, async move {
            crate::process::run_status(&mut command, ANNOUNCEMENT_TIMEOUT).await
        })
        .await;
    }

    pub(super) async fn send_authorized(
        &self,
        iface: &str,
        ip: IpAddr,
        effect: Arc<super::witness::RetainedEffect>,
    ) {
        let mut command = command(iface, ip);
        self.start(iface, ip, async move {
            effect
                .run(crate::process::run_status(
                    &mut command,
                    ANNOUNCEMENT_TIMEOUT,
                ))
                .await
                .unwrap_or_else(|| {
                    Err(io::Error::new(
                        io::ErrorKind::Interrupted,
                        "announcement permission revoked",
                    ))
                })
        })
        .await;
    }

    pub(super) async fn start(
        &self,
        iface: &str,
        ip: IpAddr,
        run: impl Future<Output = io::Result<ExitStatus>> + Send + 'static,
    ) {
        let pending = self.tasks.lock().await.entry(ip).or_default().clone();
        let mut pending = pending.lock().await;
        cancel(&mut pending, ip).await;
        let iface = iface.to_owned();
        let kind = if ip.is_ipv4() {
            "gratuitous ARP"
        } else {
            "unsolicited NA"
        };
        // Lifetime: owned by this VIP's JoinSet and canceled before unbind or replacement.
        pending.spawn(
            async move {
                match run.await {
                    Ok(status) if status.success() => {
                        tracing::debug!(%ip, %iface, kind, "VIP announcement completed");
                    }
                    Ok(status) => {
                        tracing::warn!(%ip, %iface, kind, %status, "VIP announcement failed");
                    }
                    Err(error) => {
                        tracing::warn!(%ip, %iface, kind, %error, "VIP announcement failed");
                    }
                }
            }
            .with_current_subscriber(),
        );
    }

    pub(super) async fn cancel(&self, ip: IpAddr) {
        let pending = self.tasks.lock().await.get(&ip).cloned();
        if let Some(pending) = pending {
            let mut pending = pending.lock().await;
            cancel(&mut pending, ip).await;
        }
    }
}

async fn cancel(pending: &mut JoinSet<()>, ip: IpAddr) {
    pending.abort_all();
    while let Some(result) = pending.join_next().await {
        if let Err(error) = result
            && !error.is_cancelled()
        {
            tracing::warn!(%ip, %error, "VIP announcement task failed");
        }
    }
}

fn command(iface: &str, ip: IpAddr) -> Command {
    let mut command = if ip.is_ipv4() {
        let mut command = Command::new("arping");
        command.args(["-q", "-U", "-c", "2", "-I", iface, &ip.to_string()]);
        command
    } else {
        let mut command = Command::new("ndptool");
        command.args(["-t", "na", "-U", "-i", iface, "-T", &ip.to_string(), "send"]);
        command
    };
    command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    command
}

#[cfg(test)]
mod tests;
