//! Read-only startup diagnosis of Linux IPv4 secondary-address removal.

use crate::config::VipAddr;
use std::collections::BTreeSet;
use std::future::Future;
use std::io;
use std::path::Path;
use tokio::io::AsyncReadExt;

pub(crate) async fn warn_ipv4_secondary_removal(vips: &[(VipAddr, String)], dry_run: bool) {
    check_with(vips, dry_run, |iface| {
        read_enabled(Path::new("/proc/sys/net/ipv4/conf"), iface)
    })
    .await;
}

async fn read_enabled(root: &Path, iface: String) -> io::Result<bool> {
    let file = tokio::fs::File::open(root.join(iface).join("promote_secondaries")).await?;
    let mut value = String::new();
    file.take(33).read_to_string(&mut value).await?;
    if value.len() <= 32 {
        match value.trim() {
            "0" => return Ok(false),
            "1" => return Ok(true),
            _ => {}
        }
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "expected a boolean sysctl value (0 or 1)",
    ))
}

async fn check_with<F, R>(vips: &[(VipAddr, String)], dry_run: bool, mut read: F)
where
    F: FnMut(String) -> R,
    R: Future<Output = io::Result<bool>>,
{
    if dry_run {
        return;
    }
    let interfaces: BTreeSet<_> = vips
        .iter()
        .filter(|(vip, _)| vip.addr.is_ipv4())
        .map(|(_, iface)| iface.as_str())
        .collect();
    if interfaces.is_empty() {
        return;
    }
    let global = bounded_read(read("all".into())).await;
    // Linux enables promotion if either conf/all or the interface enables it.
    if matches!(global, Ok(true)) {
        return;
    }
    for iface in interfaces {
        let local = bounded_read(read(iface.into())).await;
        match (&global, &local) {
            (_, Ok(true)) => {}
            (Ok(false), Ok(false)) => tracing::warn!(
                target: "keepafloatd::vip", iface = %iface,
                "IPv4 secondary address promotion is disabled; removing a primary address can remove other addresses in the same subnet; enable promote_secondaries for this interface to preserve them"
            ),
            _ => tracing::warn!(
                target: "keepafloatd::vip", iface = %iface, global = ?global, local = ?local,
                "could not verify IPv4 secondary address promotion; check promote_secondaries before removing primary addresses"
            ),
        }
    }
}

async fn bounded_read(read: impl Future<Output = io::Result<bool>>) -> io::Result<bool> {
    crate::stop_budget::checkpoint(super::effects::IP_COMMAND_TIMEOUT);
    tokio::time::timeout(super::effects::IP_COMMAND_TIMEOUT, read)
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "sysctl read timed out"))?
}

#[cfg(test)]
mod tests;
