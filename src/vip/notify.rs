//! Keepalived-compatible, bounded VIP transition hooks.

use tokio::process::Command;

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
            Self::Master => "MASTER",
            Self::Backup => "BACKUP",
            Self::Fault => "FAULT",
        }
    }
}

/// Use `FAULT` only when the local health check caused release; cluster-level releases use
/// `BACKUP`, matching keepalived's convention.
pub(crate) fn release_notify_state(local_ok: bool) -> VipState {
    if local_ok {
        VipState::Backup
    } else {
        VipState::Fault
    }
}

/// Spawn `<script> INSTANCE <vip_addr> MASTER|BACKUP|FAULT`, bounded below systemd's stop budget.
pub(super) fn fire_notify_script(
    script: &str,
    vip_addr: &str,
    state: VipState,
    dry_run: bool,
) -> Option<tokio::task::JoinHandle<()>> {
    fire_notify_script_with_timeout(script, vip_addr, state, dry_run, NOTIFY_SCRIPT_TIMEOUT)
}

pub(super) fn fire_notify_script_with_timeout(
    script: &str,
    vip_addr: &str,
    state: VipState,
    dry_run: bool,
    timeout: tokio::time::Duration,
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
        let mut command = Command::new(&script);
        command.args(["INSTANCE", &vip_addr, state_str]);
        crate::process::null_stdio(&mut command);
        match crate::process::run_status(&mut command, timeout).await {
            Ok(status) if status.success() => tracing::debug!(
                target: "keepafloatd::vip",
                "notify {script} INSTANCE {vip_addr} {state_str}: ok"
            ),
            Ok(status) => tracing::warn!(
                target: "keepafloatd::vip",
                "notify {script} INSTANCE {vip_addr} {state_str}: exit {status}"
            ),
            Err(error) if error.kind() == std::io::ErrorKind::TimedOut => tracing::warn!(
                target: "keepafloatd::vip",
                "notify {script} INSTANCE {vip_addr} {state_str}: timed out after {}s, killed",
                timeout.as_secs_f64()
            ),
            Err(error) => tracing::warn!(
                target: "keepafloatd::vip",
                "notify {script} INSTANCE {vip_addr} {state_str}: failed: {error}"
            ),
        }
    }))
}

const NOTIFY_SCRIPT_TIMEOUT: tokio::time::Duration = tokio::time::Duration::from_secs(10);
