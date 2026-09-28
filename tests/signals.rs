//! Exercise real process signals without changing host interfaces.

use std::fs::{self, File};
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::process::{Child, Command};
use tokio::time::{sleep, timeout};

static NEXT_FIXTURE: AtomicUsize = AtomicUsize::new(0);

struct Daemon {
    child: Child,
    dir: PathBuf,
}

impl Daemon {
    async fn start() -> Self {
        let dir = std::env::temp_dir().join(format!(
            "keepafloatd-signals-{}-{}",
            std::process::id(),
            NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&dir).unwrap();
        let raft = TcpListener::bind("127.0.0.1:0").unwrap();
        let submit = TcpListener::bind("127.0.0.1:0").unwrap();
        let raft_addr = raft.local_addr().unwrap();
        let submit_addr = submit.local_addr().unwrap();
        let config = format!(
            r#"node_id: 1
raft_listen: "{raft_addr}"
client_submit_listen: "{submit_addr}"
peers:
  - id: 1
    raft_address: "{raft_addr}"
    client_submit_address: "{submit_addr}"
vips:
  - address: "192.0.2.100"
    interface: lo
health:
  command: ["/bin/sh", "-c", "exit 0"]
  interval_ms: 50
  timeout_ms: 200
  stale_secs: 6
raft:
  heartbeat_interval_ms: 50
  election_timeout_min_ms: 150
  election_timeout_max_ms: 300
cluster_secret: "signal-regression-fixture"
dry_run: true
"#
        );
        fs::write(dir.join("config.yaml"), config).unwrap();
        let log = File::create(dir.join("daemon.log")).unwrap();
        drop((raft, submit));
        let child = Command::new(env!("CARGO_BIN_EXE_keepafloatd"))
            .arg("--config")
            .arg(dir.join("config.yaml"))
            .env("RUST_LOG", "info")
            .stdin(Stdio::null())
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut daemon = Self { child, dir };
        timeout(Duration::from_secs(5), async {
            loop {
                assert!(
                    daemon.child.try_wait().unwrap().is_none(),
                    "daemon exited before bind: {}",
                    daemon.logs()
                );
                if daemon
                    .logs()
                    .contains("dry-run: would bind 192.0.2.100/32 on lo")
                {
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("daemon did not bind: {}", daemon.logs()));
        daemon
    }

    fn logs(&self) -> String {
        fs::read_to_string(self.dir.join("daemon.log")).unwrap()
    }

    async fn assert_shutdown(mut self, signal: &str, expected_code: i32) {
        let sent = timeout(
            Duration::from_secs(1),
            Command::new("/bin/sh")
                .args([
                    "-c",
                    "kill -s \"$1\" \"$2\"",
                    "signal-test",
                    signal,
                    &self.child.id().unwrap().to_string(),
                ])
                .env("PATH", "")
                .kill_on_drop(true)
                .status(),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(sent.success());
        let status = timeout(Duration::from_secs(2), self.child.wait())
            .await
            .unwrap()
            .unwrap();
        let logs = self.logs();
        assert!(
            logs.contains(&format!("shutting down on SIG{signal}")),
            "missing graceful signal handling; status={status}, logs={logs}"
        );
        assert!(
            logs.contains("dry-run: would unbind 192.0.2.100/32 on lo"),
            "{logs}"
        );
        assert_eq!(status.code(), Some(expected_code), "{logs}");
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        self.child.start_kill().expect("stop signal-test child");
        fs::remove_dir_all(&self.dir).unwrap();
    }
}

#[tokio::test]
async fn sighup_releases_vips_and_requests_restart() {
    Daemon::start().await.assert_shutdown("HUP", 1).await;
}

#[tokio::test]
async fn sigint_releases_vips_and_stops_successfully() {
    Daemon::start().await.assert_shutdown("INT", 0).await;
}

#[tokio::test]
async fn sigterm_releases_vips_and_stops_successfully() {
    Daemon::start().await.assert_shutdown("TERM", 0).await;
}
