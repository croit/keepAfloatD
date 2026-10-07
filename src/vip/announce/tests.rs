use super::*;
use std::os::unix::process::ExitStatusExt;
use std::sync::{Arc, Mutex as StdMutex};
use tokio::sync::oneshot;

fn address() -> IpAddr {
    "192.0.2.99".parse().unwrap()
}

fn success() -> io::Result<ExitStatus> {
    Ok(ExitStatus::from_raw(0))
}

async fn finish(announcements: &Announcements, ip: IpAddr) {
    let mut tasks = announcements.tasks.lock().await.remove(&ip).unwrap();
    while let Some(result) = tasks.join_next().await {
        result.unwrap();
    }
}

#[derive(Clone, Default)]
struct LogBuffer(Arc<StdMutex<Vec<u8>>>);

impl io::Write for LogBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

async fn isolated_logging(case: &str) -> Option<(LogBuffer, tracing::Dispatch)> {
    let test_name = format!("vip::announce::tests::{case}");
    if std::env::var("KEEPAFLOATD_ANNOUNCEMENT_LOG_TEST").as_deref() != Ok(&test_name) {
        let child = tokio::time::timeout(
            Duration::from_secs(3),
            Command::new(std::env::current_exe().unwrap())
                .args(["--exact", &test_name, "--nocapture", "--color=never"])
                .env("KEEPAFLOATD_ANNOUNCEMENT_LOG_TEST", &test_name)
                .stdin(Stdio::null())
                .kill_on_drop(true)
                .output(),
        )
        .await
        .expect("isolated announcement logging test timed out")
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

    let buffer = LogBuffer::default();
    let writer = buffer.clone();
    let subscriber = tracing::Dispatch::new(
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .without_time()
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish(),
    );
    // A process-wide subscriber prevents unsubscribed tasks from caching disabled callsites.
    tracing::dispatcher::set_global_default(subscriber.clone()).unwrap();
    Some((buffer, subscriber))
}

#[tokio::test]
async fn capture_survives_an_unsubscribed_first_announcement() {
    let Some((buffer, subscriber)) =
        isolated_logging("capture_survives_an_unsubscribed_first_announcement").await
    else {
        return;
    };
    let announcements = Announcements::default();
    announcements
        .start("test0", address(), async { success() })
        .await;
    finish(&announcements, address()).await;
    buffer.0.lock().unwrap().clear();

    async {
        tracing::debug!("announcement log capture active");
        for ip in [address(), "2001:db8::99".parse().unwrap()] {
            announcements.start("test0", ip, async { success() }).await;
            finish(&announcements, ip).await;
        }
    }
    .with_subscriber(subscriber)
    .await;
    let logs = String::from_utf8(buffer.0.lock().unwrap().clone()).unwrap();
    assert!(logs.contains("announcement log capture active"), "{logs}");
    assert_eq!(
        logs.matches("VIP announcement completed").count(),
        2,
        "{logs}"
    );
}

#[test]
fn command_requests_two_unsolicited_packets_with_a_separate_budget() {
    let command = command("test0", address());
    assert_eq!(command.as_std().get_program(), "arping");
    assert_eq!(
        command.as_std().get_args().collect::<Vec<_>>(),
        ["-q", "-U", "-c", "2", "-I", "test0", "192.0.2.99"]
    );
    assert!(ANNOUNCEMENT_TIMEOUT >= Duration::from_secs(2));
    assert!(ANNOUNCEMENT_TIMEOUT > super::super::effects::IP_COMMAND_TIMEOUT);
}

#[tokio::test]
async fn pending_announcements_do_not_block_other_addresses() {
    let announcements = Announcements::default();
    let (mut first, pending) = oneshot::channel::<()>();
    announcements
        .start("test0", address(), async move {
            pending.await.unwrap();
            success()
        })
        .await;
    let other = "2001:db8::100".parse().unwrap();
    announcements
        .start("test0", other, async { success() })
        .await;
    finish(&announcements, other).await;
    assert!(!first.is_closed());
    announcements.cancel(address()).await;
    first.closed().await;
    assert!(announcements.tasks.lock().await.is_empty());
    announcements.cancel(address()).await;
}

#[test]
fn ipv6_command_advertises_the_exact_target_on_the_selected_interface() {
    let command = command("test0", "2001:db8::99".parse().unwrap());
    assert_eq!(command.as_std().get_program(), "ndptool");
    assert_eq!(
        command.as_std().get_args().collect::<Vec<_>>(),
        [
            "-t",
            "na",
            "-U",
            "-i",
            "test0",
            "-T",
            "2001:db8::99",
            "send"
        ]
    );
}

#[tokio::test]
async fn replacement_cancels_the_previous_command_before_starting() {
    let announcements = Announcements::default();
    let (first, pending) = oneshot::channel::<()>();
    announcements
        .start("test0", address(), async move {
            pending.await.unwrap();
            success()
        })
        .await;
    announcements
        .start("test1", address(), async move {
            assert!(
                first.is_closed(),
                "old work must be gone before replacement"
            );
            success()
        })
        .await;
    finish(&announcements, address()).await;
}

#[tokio::test]
async fn dropping_the_owner_cancels_all_announcements() {
    let announcements = Announcements::default();
    let mut senders = Vec::new();
    for ip in [address(), "2001:db8::100".parse().unwrap()] {
        let (sender, pending) = oneshot::channel::<()>();
        announcements
            .start("test0", ip, async move {
                pending.await.unwrap();
                success()
            })
            .await;
        senders.push(sender);
    }
    drop(announcements);
    for mut sender in senders {
        sender.closed().await;
    }
}

#[tokio::test]
async fn results_log_success_exit_spawn_and_timeout_with_address_context() {
    let Some((buffer, subscriber)) =
        isolated_logging("results_log_success_exit_spawn_and_timeout_with_address_context").await
    else {
        return;
    };
    async {
        let announcements = Announcements::default();
        for ip in [address(), "2001:db8::99".parse().unwrap()] {
            for result in [
                success(),
                Ok(ExitStatus::from_raw(2 << 8)),
                Err(io::Error::new(io::ErrorKind::NotFound, "tool missing")),
                Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "announcement deadline",
                )),
            ] {
                announcements
                    .start("test0", ip, async move { result })
                    .await;
                finish(&announcements, ip).await;
            }
        }
    }
    .with_subscriber(subscriber)
    .await;
    let logs = String::from_utf8(buffer.0.lock().unwrap().clone()).unwrap();
    assert_eq!(logs.matches("VIP announcement completed").count(), 2);
    assert_eq!(logs.matches("VIP announcement failed").count(), 6);
    assert_eq!(logs.matches("gratuitous ARP").count(), 4);
    assert_eq!(logs.matches("unsolicited NA").count(), 4);
    assert_eq!(logs.matches("ip=192.0.2.99").count(), 4);
    assert_eq!(logs.matches("ip=2001:db8::99").count(), 4);
    assert_eq!(logs.matches("iface=test0").count(), 8);
    assert!(logs.contains("exit status: 2"), "{logs}");
    assert!(logs.contains("tool missing"), "{logs}");
    assert!(logs.contains("announcement deadline"), "{logs}");
}

#[tokio::test]
async fn unexpected_task_failure_is_observed_on_cleanup() {
    let Some((buffer, subscriber)) =
        isolated_logging("unexpected_task_failure_is_observed_on_cleanup").await
    else {
        return;
    };
    let announcements = Announcements::default();
    let mut tasks = JoinSet::new();
    let handle = tasks.spawn(async { panic!("test announcement panic") });
    while !handle.is_finished() {
        tokio::task::yield_now().await;
    }
    announcements.tasks.lock().await.insert(address(), tasks);
    announcements
        .cancel(address())
        .with_subscriber(subscriber)
        .await;
    let logs = String::from_utf8(buffer.0.lock().unwrap().clone()).unwrap();
    assert!(logs.contains("VIP announcement task failed"), "{logs}");
    assert!(logs.contains("192.0.2.99"), "{logs}");
    assert!(logs.contains("test announcement panic"), "{logs}");
}
