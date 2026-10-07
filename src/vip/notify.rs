//! Keepalived-compatible, bounded VIP transition hooks.

use futures::FutureExt;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;
use tokio::process::Command;
use tokio::sync::{Mutex, mpsc, watch};
use tokio::task::JoinSet;
use tracing::instrument::WithSubscriber;

const QUEUE_CAPACITY: usize = 64;
pub(super) const SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(1);
const SHUTDOWN_JOIN_TIMEOUT: Duration = Duration::from_secs(1);
type Hook = Pin<Box<dyn Future<Output = ()> + Send>>;

#[derive(Default)]
struct Lifecycle {
    closed: bool,
    sender: Option<mpsc::Sender<Hook>>,
    tasks: JoinSet<anyhow::Result<()>>,
}

pub(super) struct Notifications {
    lifecycle: Mutex<Lifecycle>,
    failure: watch::Sender<Option<String>>,
}

impl Default for Notifications {
    fn default() -> Self {
        Self {
            lifecycle: Mutex::new(Lifecycle::default()),
            failure: watch::channel(None).0,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Rejected {
    Full,
    Closed,
}

impl Notifications {
    pub(super) async fn send(&self, script: &str, vip_addr: &str, state: VipState, dry_run: bool) {
        if dry_run {
            tracing::info!(target: "keepafloatd::vip",
                "dry-run: would notify {script} INSTANCE {vip_addr} {}", state.as_str());
            return;
        }
        let hook = run_notify_script(
            script.to_owned(),
            vip_addr.to_owned(),
            state,
            NOTIFY_SCRIPT_TIMEOUT,
        );
        if let Err(reason) = self.enqueue(Box::pin(hook)).await {
            tracing::warn!(target: "keepafloatd::vip", ?reason, capacity = QUEUE_CAPACITY,
                "notify dropped: {script} INSTANCE {vip_addr} {}", state.as_str());
        }
    }

    async fn enqueue(&self, hook: Hook) -> Result<(), Rejected> {
        let mut lifecycle = self.lifecycle.lock().await;
        if lifecycle.closed {
            return Err(Rejected::Closed);
        }
        if lifecycle.sender.is_none() {
            let (sender, mut receiver) = mpsc::channel::<Hook>(QUEUE_CAPACITY);
            let failure = self.failure.clone();
            // Lifetime: LocalVip owns this worker; shutdown drains or aborts and joins it.
            lifecycle.tasks.spawn(
                async move {
                    let result = std::panic::AssertUnwindSafe(async move {
                        while let Some(hook) = receiver.recv().await {
                            hook.await;
                        }
                    })
                    .catch_unwind()
                    .await;
                    if result.is_err() {
                        let message = "notification worker panicked".to_owned();
                        tracing::error!(target: "keepafloatd::vip", "{message}");
                        failure.send_replace(Some(message.clone()));
                        anyhow::bail!(message);
                    }
                    Ok(())
                }
                .with_current_subscriber(),
            );
            lifecycle.sender = Some(sender);
        }
        match lifecycle
            .sender
            .as_ref()
            .map(|sender| sender.try_send(hook))
        {
            Some(Ok(())) => Ok(()),
            Some(Err(mpsc::error::TrySendError::Full(_))) => Err(Rejected::Full),
            _ => Err(Rejected::Closed),
        }
    }

    pub(super) async fn failed(&self) -> String {
        let mut receiver = self.failure.subscribe();
        match receiver.wait_for(Option::is_some).await {
            Ok(message) => message.clone().unwrap_or_default(),
            Err(error) => format!("notification supervision channel closed: {error}"),
        }
    }

    pub(super) async fn shutdown(&self, grace: Duration) -> anyhow::Result<()> {
        let mut tasks = {
            let mut lifecycle = self.lifecycle.lock().await;
            lifecycle.closed = true;
            lifecycle.sender.take();
            std::mem::take(&mut lifecycle.tasks)
        };
        match tokio::time::timeout(grace, tasks.join_next()).await {
            Ok(Some(result)) => result??,
            Ok(None) => {}
            Err(_) => {
                tracing::warn!(target: "keepafloatd::vip",
                    "notify shutdown drain expired; cancelling active hook and discarding queued hooks");
                tasks.abort_all();
                match tokio::time::timeout(SHUTDOWN_JOIN_TIMEOUT, tasks.join_next()).await {
                    Ok(Some(Err(error))) if error.is_cancelled() => {}
                    Ok(Some(result)) => result??,
                    Ok(None) => {}
                    Err(_) => anyhow::bail!("notification worker did not stop after cancellation"),
                }
            }
        }
        Ok(())
    }
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
#[cfg(test)]
pub(super) fn fire_notify_script(
    script: &str,
    vip_addr: &str,
    state: VipState,
    dry_run: bool,
) -> Option<tokio::task::JoinHandle<()>> {
    fire_notify_script_with_timeout(script, vip_addr, state, dry_run, NOTIFY_SCRIPT_TIMEOUT)
}

#[cfg(test)]
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
    Some(tokio::spawn(
        run_notify_script(script.to_owned(), vip_addr.to_owned(), state, timeout)
            .with_current_subscriber(),
    ))
}

async fn run_notify_script(script: String, vip_addr: String, state: VipState, timeout: Duration) {
    let state_str = state.as_str();
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
}

const NOTIFY_SCRIPT_TIMEOUT: tokio::time::Duration = tokio::time::Duration::from_secs(10);

#[cfg(test)]
pub(super) use tests::Fixture;

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::Duration;

    pub(in crate::vip) struct Fixture(pub(in crate::vip) PathBuf);

    impl Fixture {
        pub(in crate::vip) fn new(body: &str) -> Self {
            static NEXT: AtomicUsize = AtomicUsize::new(0);
            let path = std::env::temp_dir().join(format!(
                "keepafloatd_notify_actor_{}_{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir(&path).unwrap();
            std::fs::write(path.join("body"), body).unwrap();
            // An immutable executable avoids ETXTBSY from freshly written scripts.
            std::os::unix::fs::symlink(
                concat!(env!("CARGO_MANIFEST_DIR"), "/src/vip/notify/fixture.sh"),
                path.join("notify.sh"),
            )
            .unwrap();
            Self(path)
        }

        pub(in crate::vip) fn script(&self) -> String {
            self.0.join("notify.sh").to_str().unwrap().to_owned()
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).unwrap();
        }
    }

    async fn isolated_logging(
        case: &str,
    ) -> Option<crate::warning_limit::test_support::LogCapture> {
        let test_name = format!("vip::notify::tests::{case}");
        const CHILD_ENV: &str = "KEEPAFLOATD_NOTIFY_LOG_TEST";
        if std::env::var(CHILD_ENV).as_deref() != Ok(test_name.as_str()) {
            let child = tokio::time::timeout(
                Duration::from_secs(3),
                Command::new(std::env::current_exe().unwrap())
                    .args(["--exact", &test_name, "--nocapture", "--color=never"])
                    .env(CHILD_ENV, &test_name)
                    .stdin(std::process::Stdio::null())
                    .kill_on_drop(true)
                    .output(),
            )
            .await
            .expect("isolated notification logging test timed out")
            .unwrap();
            let stdout = String::from_utf8_lossy(&child.stdout);
            let stderr = String::from_utf8_lossy(&child.stderr);
            assert!(child.status.success(), "{test_name}: {stdout}\n{stderr}");
            assert!(
                stdout.contains(&format!("test {test_name} ... ok")),
                "child did not run {test_name}: {stdout}\n{stderr}"
            );
            return None;
        }

        let logs = crate::warning_limit::test_support::LogCapture::default();
        let writer = logs.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .without_time()
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish();
        // A process-wide subscriber prevents unsubscribed tasks from caching disabled callsites.
        tracing::subscriber::set_global_default(subscriber).unwrap();
        Some(logs)
    }

    async fn blocked_hook(notifications: &Notifications) -> tokio::sync::oneshot::Sender<()> {
        let (started, ready) = tokio::sync::oneshot::channel();
        let (release, wait) = tokio::sync::oneshot::channel();
        notifications
            .enqueue(Box::pin(async move {
                started.send(()).unwrap();
                let _ = wait.await;
            }))
            .await
            .unwrap();
        ready.await.unwrap();
        release
    }

    async fn wait_for(path: &Path) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while !path.exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("hook did not publish its checkpoint");
    }

    #[tokio::test]
    async fn transition_hooks_complete_in_submission_order() {
        let fixture = Fixture::new(
            "if [ \"$3\" = MASTER ]; then\n  touch started\n  while [ ! -f release ]; do sleep 0.01; done\nfi\nprintf '%s %s %s\\n' \"$1\" \"$2\" \"$3\" >> events\nif [ \"$3\" = BACKUP ]; then touch backup; fi",
        );
        let notifications = Notifications::default();
        notifications
            .send(&fixture.script(), "192.0.2.1", VipState::Master, false)
            .await;
        wait_for(&fixture.0.join("started")).await;
        notifications
            .send(&fixture.script(), "192.0.2.1", VipState::Backup, false)
            .await;
        let _ = tokio::time::timeout(Duration::from_millis(200), async {
            while !fixture.0.join("backup").exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        std::fs::write(fixture.0.join("release"), "").unwrap();
        notifications
            .shutdown(Duration::from_secs(1))
            .await
            .unwrap();
        let events = std::fs::read_to_string(fixture.0.join("events")).unwrap();
        assert_eq!(
            events,
            "INSTANCE 192.0.2.1 MASTER\nINSTANCE 192.0.2.1 BACKUP\n"
        );
    }

    #[tokio::test]
    async fn actor_dry_run_never_starts_a_worker() {
        let Some(logs) = isolated_logging("actor_dry_run_never_starts_a_worker").await else {
            return;
        };
        let notifications = Notifications::default();
        notifications
            .send("/nonexistent/notify", "192.0.2.1", VipState::Master, true)
            .await;
        assert!(notifications.lifecycle.lock().await.sender.is_none());
        notifications.shutdown(Duration::ZERO).await.unwrap();
        assert!(
            logs.text()
                .contains("dry-run: would notify /nonexistent/notify INSTANCE 192.0.2.1 MASTER")
        );
    }

    #[tokio::test]
    async fn bounded_queue_rejects_newest_and_drains_accepted_hooks_in_order() {
        let Some(logs) =
            isolated_logging("bounded_queue_rejects_newest_and_drains_accepted_hooks_in_order")
                .await
        else {
            return;
        };
        let notifications = Notifications::default();
        let release = blocked_hook(&notifications).await;
        let completed = Arc::new(Mutex::new(Vec::new()));
        for index in 0..QUEUE_CAPACITY {
            let completed = completed.clone();
            notifications
                .enqueue(Box::pin(async move {
                    completed.lock().await.push(index);
                }))
                .await
                .unwrap();
        }
        assert_eq!(
            notifications
                .enqueue(Box::pin(async {
                    panic!("overflow hook ran");
                }))
                .await,
            Err(Rejected::Full)
        );
        assert_eq!(notifications.lifecycle.lock().await.tasks.len(), 1);
        notifications
            .send("/nonexistent/notify", "192.0.2.2", VipState::Fault, false)
            .await;
        assert!(logs.text().contains("reason=Full"));
        assert!(
            logs.text()
                .contains("notify dropped: /nonexistent/notify INSTANCE 192.0.2.2 FAULT")
        );
        assert!(completed.lock().await.is_empty());
        release.send(()).unwrap();
        notifications
            .shutdown(Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(
            *completed.lock().await,
            (0..QUEUE_CAPACITY).collect::<Vec<_>>()
        );
        assert_eq!(
            notifications.enqueue(Box::pin(async {})).await,
            Err(Rejected::Closed)
        );
        notifications
            .send("/nonexistent/notify", "192.0.2.2", VipState::Backup, false)
            .await;
        assert!(logs.text().contains("reason=Closed"));
        notifications.shutdown(Duration::ZERO).await.unwrap();
    }

    #[tokio::test]
    async fn shutdown_cancels_the_active_hook_and_discards_the_queue() {
        let Some(logs) =
            isolated_logging("shutdown_cancels_the_active_hook_and_discards_the_queue").await
        else {
            return;
        };
        let notifications = Notifications::default();
        let release = blocked_hook(&notifications).await;
        notifications
            .enqueue(Box::pin(async {
                panic!("discarded hook ran");
            }))
            .await
            .unwrap();
        notifications.shutdown(Duration::ZERO).await.unwrap();
        assert!(release.send(()).is_err());
        assert!(notifications.lifecycle.lock().await.tasks.is_empty());
        assert!(
            logs.text()
                .contains("cancelling active hook and discarding queued hooks")
        );
    }

    #[tokio::test]
    async fn worker_panic_is_reported_to_supervision_and_shutdown() {
        let vip = crate::vip::LocalVip::new(true);
        vip.notifications
            .enqueue(Box::pin(async {
                panic!("injected hook panic");
            }))
            .await
            .unwrap();
        let failure = tokio::time::timeout(Duration::from_secs(1), vip.notification_failure())
            .await
            .unwrap();
        assert_eq!(failure, "notification worker panicked");
        assert_eq!(
            vip.notifications.enqueue(Box::pin(async {})).await,
            Err(Rejected::Closed)
        );
        assert!(
            vip.shutdown_notifications()
                .await
                .unwrap_err()
                .to_string()
                .contains("notification worker panicked")
        );
    }

    #[tokio::test]
    async fn hook_failures_do_not_stop_later_transitions() {
        let Some(logs) = isolated_logging("hook_failures_do_not_stop_later_transitions").await
        else {
            return;
        };
        let notifications = Notifications::default();
        let fixture = Fixture::new("exec sleep 30");
        for script in ["/bin/true", "/bin/false", "/nonexistent/notify"] {
            notifications
                .send(script, "192.0.2.1", VipState::Master, false)
                .await;
        }
        notifications
            .enqueue(Box::pin(run_notify_script(
                fixture.script(),
                "192.0.2.1".to_owned(),
                VipState::Fault,
                Duration::from_millis(50),
            )))
            .await
            .unwrap();
        let (done, finished) = tokio::sync::oneshot::channel();
        notifications
            .enqueue(Box::pin(async {
                done.send(()).unwrap();
            }))
            .await
            .unwrap();
        notifications
            .shutdown(Duration::from_secs(1))
            .await
            .unwrap();
        finished.await.unwrap();
        let logs = logs.text();
        assert!(logs.contains("MASTER: ok"), "{logs}");
        assert!(logs.contains("MASTER: exit exit status: 1"), "{logs}");
        assert!(logs.contains("MASTER: failed:"), "{logs}");
        assert!(
            logs.contains("FAULT: timed out after 0.05s, killed"),
            "{logs}"
        );
    }

    #[tokio::test]
    async fn slow_hook_does_not_block_cleanup_or_subsequent_reconciliation() {
        let vip = crate::vip::LocalVip::new(true);
        let release = blocked_hook(&vip.notifications).await;
        let address = "192.0.2.1".parse().unwrap();
        let table = vec![(crate::config::VipAddr::host(address), "lo".to_owned())];
        vip.bind("lo", address, 32).await.unwrap();
        tokio::time::timeout(
            Duration::from_millis(100),
            vip.unbind_all(&table, Some("/bin/true"), false, VipState::Backup),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(vip.bound_addrs().await.is_empty());
        vip.bind("lo", address, 32).await.unwrap();
        vip.notify_transition("/bin/true", "192.0.2.1", VipState::Master, false)
            .await;
        release.send(()).unwrap();
        vip.shutdown_notifications().await.unwrap();
    }

    #[tokio::test]
    async fn queued_master_then_fault_preserves_order_while_all_addresses_are_cleaned() {
        let fixture = Fixture::new(
            "if [ \"$3\" = MASTER ]; then\n  touch started\n  while [ ! -f release ]; do sleep 0.01; done\nfi\nprintf '%s %s\\n' \"$2\" \"$3\" >> events",
        );
        let vip = crate::vip::LocalVip::new(true);
        let mut table = Vec::new();
        for address in ["192.0.2.1", "192.0.2.2"] {
            let ip = address.parse().unwrap();
            vip.bind("lo", ip, 32).await.unwrap();
            vip.notify_transition(&fixture.script(), address, VipState::Master, false)
                .await;
            table.push((crate::config::VipAddr::host(ip), "lo".to_owned()));
        }
        wait_for(&fixture.0.join("started")).await;
        tokio::time::timeout(
            Duration::from_millis(100),
            vip.unbind_all(&table, Some(&fixture.script()), false, VipState::Fault),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(vip.bound_addrs().await.is_empty());
        assert!(
            !fixture.0.join("events").exists(),
            "cleanup waited for the blocked MASTER hook"
        );
        std::fs::write(fixture.0.join("release"), "").unwrap();
        vip.shutdown_notifications().await.unwrap();
        assert_eq!(
            std::fs::read_to_string(fixture.0.join("events")).unwrap(),
            "192.0.2.1 MASTER\n192.0.2.2 MASTER\n192.0.2.1 FAULT\n192.0.2.2 FAULT\n"
        );
    }

    async fn assert_processes_gone(fixture: &Fixture) {
        let pids = std::fs::read_to_string(fixture.0.join("pids")).unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while pids
                .split_whitespace()
                .any(|pid| Path::new(&format!("/proc/{pid}")).exists())
            {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("notification worker left a subprocess behind");
    }

    #[tokio::test]
    async fn shutdown_and_owner_drop_kill_active_hook_descendants() {
        for drain in [true, false] {
            let fixture = Fixture::new(
                "sleep 30 &\nprintf '%s %s' \"$$\" \"$!\" > pids\ntouch started\nwait",
            );
            let notifications = Notifications::default();
            notifications
                .send(&fixture.script(), "192.0.2.1", VipState::Master, false)
                .await;
            wait_for(&fixture.0.join("started")).await;
            if drain {
                notifications.shutdown(Duration::ZERO).await.unwrap();
            }
            drop(notifications);
            assert_processes_gone(&fixture).await;
        }
    }

    #[tokio::test]
    async fn cancelling_shutdown_still_owns_the_active_subprocess() {
        let fixture =
            Fixture::new("sleep 30 &\nprintf '%s %s' \"$$\" \"$!\" > pids\ntouch started\nwait");
        let notifications = Notifications::default();
        notifications
            .send(&fixture.script(), "192.0.2.1", VipState::Master, false)
            .await;
        wait_for(&fixture.0.join("started")).await;
        let mut shutdown = Box::pin(notifications.shutdown(Duration::from_secs(1)));
        assert!(futures::poll!(shutdown.as_mut()).is_pending());
        drop(shutdown);
        assert_processes_gone(&fixture).await;
    }
}
