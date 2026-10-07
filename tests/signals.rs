//! Exercise real process signals without changing host interfaces.

#[path = "signals/cleanup.rs"]
mod cleanup;

#[path = "signals/network_logs.rs"]
mod network_logs;

use std::fs::{self, File};
use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::process::{Child, Command};
use tokio::time::{Instant, sleep, timeout};

static NEXT_FIXTURE: AtomicUsize = AtomicUsize::new(0);

struct Daemon {
    child: Child,
    dir: PathBuf,
}

impl Daemon {
    async fn start() -> Self {
        Self::start_with(|_| {})
            .await
            .unwrap_or_else(|error| panic!("{error}"))
    }

    async fn start_with(mut configure: impl FnMut(&mut String)) -> Result<Self, String> {
        let mut last_conflict = String::new();
        for attempt in 1..=3 {
            let mut daemon = Self::spawn(&mut configure);
            match daemon.wait_ready().await {
                Ok(()) => return Ok(daemon),
                Err(error) if is_bind_conflict(&error) => {
                    eprintln!("signal-test bind conflict on attempt {attempt}: {error}");
                    last_conflict = error;
                }
                Err(error) => return Err(error),
            }
        }
        Err(format!(
            "daemon failed after 3 bind conflicts: {last_conflict}"
        ))
    }

    fn spawn(configure: impl FnOnce(&mut String)) -> Self {
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
        let mut config = format!(
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
  stale_secs: 1
raft:
  heartbeat_interval_ms: 50
  election_timeout_min_ms: 150
  election_timeout_max_ms: 300
cluster_secret: "signal-regression-fixture-0123456789"
dry_run: true
"#
        );
        configure(&mut config);
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
        Self { child, dir }
    }

    async fn wait_ready(&mut self) -> Result<(), String> {
        let started = Instant::now();
        let convergence = Duration::from_secs(5);
        let mut deadline = started + convergence;
        let mut policy_seen = false;
        loop {
            if self.child.try_wait().unwrap().is_some() {
                return Err(format!("daemon exited before bind: {}", self.logs()));
            }
            let logs = self.logs();
            if !policy_seen && let Some(delay) = startup_safety_delay(&logs)? {
                deadline = started
                    .checked_add(delay)
                    .and_then(|value| value.checked_add(convergence))
                    .ok_or_else(|| "startup safety deadline overflow".to_owned())?;
                policy_seen = true;
            }
            if startup_ready(&logs) {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "daemon did not become ready within its startup policy: {logs}"
                ));
            }
            sleep(Duration::from_millis(10)).await;
        }
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

fn startup_safety_delay(logs: &str) -> Result<Option<Duration>, String> {
    let Some(line) = logs
        .lines()
        .find(|line| line.contains("runtime admission timing"))
    else {
        return Ok(None);
    };
    let delay = line
        .split_whitespace()
        .find_map(|field| field.strip_prefix("startup_safety_wait_ms="))
        .ok_or_else(|| format!("startup timing lacks safety wait: {line}"))?
        .parse::<u64>()
        .map_err(|error| format!("invalid startup safety wait: {error}"))?;
    Ok(Some(Duration::from_millis(delay)))
}

#[test]
fn startup_budget_uses_the_reported_admission_policy() {
    assert_eq!(startup_safety_delay("unrelated log").unwrap(), None);
    assert_eq!(
        startup_safety_delay(
            " INFO runtime admission timing quarantine_ms=4100 startup_safety_wait_ms=30650\n"
        )
        .unwrap(),
        Some(Duration::from_millis(30650))
    );
    assert!(startup_safety_delay("runtime admission timing").is_err());
    assert!(
        startup_safety_delay("runtime admission timing startup_safety_wait_ms=invalid").is_err()
    );
}

fn startup_ready(logs: &str) -> bool {
    logs.contains("Raft listening on ")
        && logs.contains("client_submit listening on ")
        && logs.contains("dry-run: would bind 192.0.2.100/32 on lo")
}

fn is_bind_conflict(logs: &str) -> bool {
    let Some((_, error)) = logs.rsplit_once("\nError: ") else {
        return false;
    };
    let listener_failed = error.starts_with("submit server\n")
        || (error.starts_with("start raft\n") && error.contains("raft network start\n"));
    listener_failed && error.contains(&format!("(os error {})", libc::EADDRINUSE))
}

#[test]
fn startup_readiness_requires_both_listeners_and_a_vip() {
    let raft = " INFO keepafloatd::raft::network: Raft listening on 127.0.0.1:1\n";
    let submit = " INFO keepafloatd::submit: client_submit listening on 127.0.0.1:2\n";
    let vip = " INFO keepafloatd::vip: dry-run: would bind 192.0.2.100/32 on lo\n";
    for incomplete in [
        vip.to_owned(),
        format!("{raft}{vip}"),
        format!("{submit}{vip}"),
        format!("{raft}{submit}"),
    ] {
        assert!(!startup_ready(&incomplete), "{incomplete}");
    }
    assert!(startup_ready(&format!("{raft}{submit}{vip}")));
}

#[test]
fn startup_retry_does_not_match_unrelated_errors() {
    let conflict = std::io::Error::from_raw_os_error(libc::EADDRINUSE);
    let denied = std::io::Error::from_raw_os_error(libc::EACCES);
    for logs in [
        format!("WARN earlier bind: {conflict}\n"),
        format!("\nError: load config\n\nCaused by:\n    {conflict}\n"),
        format!("\nError: start raft\n\nCaused by:\n    log store\n    {conflict}\n"),
        format!("\nError: submit server\n\nCaused by:\n    {denied}\n"),
        format!("\nError: submit server\n    {conflict}\nError: load config\n"),
    ] {
        assert!(!is_bind_conflict(&logs), "{logs}");
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        self.child.start_kill().expect("stop signal-test child");
        fs::remove_dir_all(&self.dir).unwrap();
    }
}

fn set_endpoint(config: &mut String, endpoint: &str, address: SocketAddr) {
    let (listen, advertised) = match endpoint {
        "raft" => ("raft_listen", "raft_address"),
        "submit" => ("client_submit_listen", "client_submit_address"),
        _ => panic!("unknown endpoint"),
    };
    let mut value: serde_yaml::Value = serde_yaml::from_str(config).unwrap();
    value[listen] = address.to_string().into();
    value["peers"][0][advertised] = address.to_string().into();
    *config = serde_yaml::to_string(&value).unwrap();
}

async fn assert_startup_recovers_from_conflict(endpoint: &str) {
    let blocker = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = blocker.local_addr().unwrap();
    let mut attempts = 0;
    let daemon = Daemon::start_with(|config| {
        attempts += 1;
        if attempts == 1 {
            set_endpoint(config, endpoint, address);
        }
    })
    .await
    .unwrap_or_else(|error| panic!("{error}"));
    assert!((2..=3).contains(&attempts), "startup attempts: {attempts}");
    daemon.assert_shutdown("TERM", 0).await;
}

#[tokio::test]
async fn startup_retries_a_raft_port_conflict() {
    assert_startup_recovers_from_conflict("raft").await;
}

#[tokio::test]
async fn startup_retries_a_submit_port_conflict() {
    assert_startup_recovers_from_conflict("submit").await;
}

#[tokio::test]
async fn startup_limits_persistent_port_conflicts() {
    for endpoint in ["raft", "submit"] {
        let blocker = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = blocker.local_addr().unwrap();
        let mut attempts = 0;
        let result = Daemon::start_with(|config| {
            attempts += 1;
            set_endpoint(config, endpoint, address);
        })
        .await;
        let error = result
            .err()
            .expect("occupied endpoint must prevent startup");
        assert_eq!(attempts, 3, "{endpoint}: {error}");
        assert!(error.contains("after 3 bind conflicts"), "{error}");
        assert!(
            error.contains(&format!("(os error {})", libc::EADDRINUSE)),
            "{error}"
        );
    }
}

#[tokio::test]
async fn startup_does_not_retry_invalid_configuration() {
    let mut attempts = 0;
    let result = Daemon::start_with(|config| {
        attempts += 1;
        *config = config.replace("signal-regression-fixture-0123456789", "short-secret");
    })
    .await;
    let error = result.err().expect("invalid configuration must fail");
    assert_eq!(attempts, 1);
    assert!(error.contains("load config"), "{error}");
    assert!(error.contains("32 to 256"), "{error}");
}

#[tokio::test]
async fn sighup_releases_vips_and_requests_restart() {
    Daemon::start().await.assert_shutdown("HUP", 1).await;
}

#[tokio::test]
async fn sigquit_releases_vips_and_requests_restart() {
    Daemon::start().await.assert_shutdown("QUIT", 1).await;
}

#[tokio::test]
async fn sigint_releases_vips_and_stops_successfully() {
    Daemon::start().await.assert_shutdown("INT", 0).await;
}

#[tokio::test]
async fn sigterm_releases_vips_and_stops_successfully() {
    Daemon::start().await.assert_shutdown("TERM", 0).await;
}
