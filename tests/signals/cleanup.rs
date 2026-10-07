use super::{Duration, NEXT_FIXTURE, Ordering, TcpListener, fs, timeout};
use std::os::unix::fs::{PermissionsExt, symlink};
use std::path::PathBuf;
use std::process::{Output, Stdio};
use tokio::process::Command;

struct Fixture {
    dir: PathBuf,
    config: String,
    _listeners: [TcpListener; 2],
}

impl Fixture {
    fn new() -> Self {
        let dir = std::env::temp_dir().join(format!(
            "keepafloatd-cleanup-{}-{}",
            std::process::id(),
            NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&dir).unwrap();
        let listeners = [
            TcpListener::bind("127.0.0.1:0").unwrap(),
            TcpListener::bind("127.0.0.1:0").unwrap(),
        ];
        let raft = listeners[0].local_addr().unwrap();
        let submit = listeners[1].local_addr().unwrap();
        let config = format!(
            r#"node_id: 1
raft_listen: "{raft}"
client_submit_listen: "{submit}"
peers:
  - id: 1
    raft_address: "{raft}"
    client_submit_address: "{submit}"
vips:
  - address: "192.0.2.100/24"
    interface: cleanup0
  - address: "2001:db8::100/64"
    interface: cleanup0
health:
  command: ["/bin/sh", "-c", "printf ran > health-ran"]
  interval_ms: 100
  timeout_ms: 200
  stale_secs: 6
notify: "{}"
cluster_secret: "cleanup-regression-fixture-0123456789"
address_protocol: 245
dry_run: true
"#,
            dir.join("notify").display()
        );
        let fixture = Self {
            dir,
            config,
            _listeners: listeners,
        };
        for name in ["ip", "notify"] {
            let source = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("tests/signals/fixtures")
                .join(format!("{name}.sh"));
            symlink(source, fixture.dir.join(name)).unwrap();
        }
        fixture
    }

    async fn run(&self, cleanup_only: bool, mode: &str) -> Output {
        fs::write(self.dir.join("config.yaml"), &self.config).unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_keepafloatd"));
        command.args(["--config", "config.yaml"]);
        if cleanup_only {
            command.arg("--cleanup-only");
        }
        let socket = self.dir.join("stop-budget.sock");
        command.env_remove("NOTIFY_SOCKET");
        if socket.exists() {
            command.env("NOTIFY_SOCKET", socket);
        }
        let output = timeout(
            Duration::from_secs(3),
            command
                .current_dir(&self.dir)
                .env("PATH", &self.dir)
                .env("RUST_LOG", "info")
                .env("CLEANUP_FIXTURE_MODE", mode)
                .stdin(Stdio::null())
                .kill_on_drop(true)
                .output(),
        )
        .await
        .expect("cleanup process did not finish promptly")
        .unwrap();
        let logs = logs(&output);
        assert!(!logs.contains("Raft listening on "), "{logs}");
        assert!(!logs.contains("client_submit listening on "), "{logs}");
        if cleanup_only {
            assert!(!logs.contains("openraft::"), "Raft started: {logs}");
        }
        assert!(!self.dir.join("health-ran").exists(), "health check ran");
        assert!(!self.dir.join("notify-ran").exists(), "notify hook ran");
        output
    }

    fn commands(&self) -> String {
        fs::read_to_string(self.dir.join("ip-commands")).unwrap_or_default()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.dir).unwrap();
    }
}

fn logs(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

#[tokio::test]
async fn command_characterization_discovers_before_addresses_before_markers() {
    let mut fixture = Fixture::new();
    fixture.config = fixture.config.replace("dry_run: true", "dry_run: false");
    let output = fixture.run(true, "ok").await;
    assert!(output.status.success(), "{}", logs(&output));
    let commands = fixture.commands();
    let commands: Vec<_> = commands.lines().collect();
    let mut discovery = commands[..3].to_vec();
    discovery.sort_unstable();
    assert_eq!(
        discovery,
        [
            "-N -j -4 route show table all",
            "-N -j -6 route show table all",
            "-N -j addr show",
        ]
    );
    assert_eq!(commands.len(), 9);
    assert!(
        commands[3..7]
            .iter()
            .all(|command| command.contains(" addr del "))
    );
    assert_eq!(
        &commands[7..],
        [
            "-4 route del table 10245 throw 192.0.2.101/32 proto 245",
            "-6 route del table 10245 throw 2001:db8::101/128 proto 245",
        ]
    );
}

#[test]
fn packaged_unit_accepts_stop_budget_from_daemon_and_cleanup_command() {
    let unit = include_str!("../../deploy/systemd/keepafloatd@.service");
    assert!(unit.lines().any(|line| line == "NotifyAccess=exec"));
    assert!(unit.lines().any(|line| line == "TimeoutStopSec=15"));
}

#[tokio::test]
async fn cleanup_only_extends_stop_budget_for_discovered_work() {
    use std::os::unix::net::UnixDatagram;

    let mut fixture = Fixture::new();
    fixture.config = fixture.config.replace("dry_run: true", "dry_run: false");
    let socket = UnixDatagram::bind(fixture.dir.join("stop-budget.sock")).unwrap();
    socket.set_nonblocking(true).unwrap();
    let output = fixture.run(true, "ok").await;
    assert!(output.status.success(), "{}", logs(&output));

    let mut messages = Vec::new();
    let mut bytes = [0; 256];
    loop {
        match socket.recv(&mut bytes) {
            Ok(size) => messages.push(String::from_utf8(bytes[..size].to_vec()).unwrap()),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(error) => panic!("receive stop budget: {error}"),
        }
    }
    assert!(
        messages.len() >= 7,
        "discovery, four configured/orphan addresses and two orphan markers need finite budgets: {messages:?}"
    );
    for message in messages {
        let micros: u64 = message
            .strip_prefix("EXTEND_TIMEOUT_USEC=")
            .unwrap()
            .parse()
            .unwrap();
        assert!((15_000_000..u64::MAX).contains(&micros));
    }
}

#[tokio::test]
async fn cleanup_only_keeps_cleaning_when_supervisor_notifications_fail() {
    let mut fixture = Fixture::new();
    fixture.config = fixture.config.replace("dry_run: true", "dry_run: false");
    fs::write(fixture.dir.join("stop-budget.sock"), "not a socket").unwrap();
    let output = fixture.run(true, "ok").await;
    assert!(output.status.success(), "{}", logs(&output));
    assert!(logs(&output).contains("could not extend systemd stop budget"));
    assert!(logs(&output).contains("cleanup-only VIP cleanup complete"));
    assert_eq!(
        fixture
            .commands()
            .lines()
            .filter(|line| line.contains(" del "))
            .count(),
        6
    );
}

#[test]
fn cleanup_commands_use_immutable_repository_fixtures() {
    let fixture = Fixture::new();
    for name in ["ip", "notify"] {
        let source = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/signals/fixtures")
            .join(format!("{name}.sh"));
        assert_eq!(fs::read_link(fixture.dir.join(name)).unwrap(), source);
        assert_ne!(
            fs::metadata(source).unwrap().permissions().mode() & 0o111,
            0
        );
    }
}

#[tokio::test]
async fn startup_characterization_cleans_before_raft_and_never_reaches_health() {
    let fixture = Fixture::new();
    let output = fixture.run(false, "ok").await;
    let logs = logs(&output);
    assert!(!output.status.success(), "{logs}");
    assert!(
        logs.contains("dry-run: would reclaim 192.0.2.100/24 on cleanup0"),
        "{logs}"
    );
    assert!(logs.contains("start raft"), "{logs}");
    assert!(fixture.commands().is_empty());
}

#[tokio::test]
async fn startup_characterization_validates_before_any_cleanup() {
    let mut fixture = Fixture::new();
    fixture.config = fixture
        .config
        .replace("address_protocol: 245", "address_protocol: 0");
    let output = fixture.run(false, "ok").await;
    let logs = logs(&output);
    assert!(!output.status.success(), "{logs}");
    assert!(logs.contains("load config"), "{logs}");
    assert!(!logs.contains("would reclaim"), "{logs}");
    assert!(fixture.commands().is_empty());
}

#[tokio::test]
async fn cleanup_only_dry_run_exits_without_listeners_health_or_ip_commands() {
    let fixture = Fixture::new();
    let output = fixture.run(true, "ok").await;
    let logs = logs(&output);
    assert!(output.status.success(), "status={}; {logs}", output.status);
    assert!(
        logs.contains("dry-run: would reclaim 192.0.2.100/24 on cleanup0"),
        "{logs}"
    );
    assert!(
        logs.contains("dry-run: would reclaim 2001:db8::100/64 on cleanup0"),
        "{logs}"
    );
    assert!(fixture.commands().is_empty());
}

#[tokio::test]
async fn cleanup_only_validates_config_before_any_cleanup() {
    let mut fixture = Fixture::new();
    fixture.config = fixture
        .config
        .replace("address_protocol: 245", "address_protocol: 0");
    let output = fixture.run(true, "ok").await;
    let logs = logs(&output);
    assert!(!output.status.success(), "{logs}");
    assert!(logs.contains("load config"), "{logs}");
    assert!(!logs.contains("would reclaim"), "{logs}");
    assert!(fixture.commands().is_empty());
}

#[tokio::test]
async fn cleanup_only_reclaims_configured_and_owned_orphans_not_unrelated_addresses() {
    let mut fixture = Fixture::new();
    fixture.config = fixture.config.replace("dry_run: true", "dry_run: false");
    let output = fixture.run(true, "ok").await;
    assert!(output.status.success(), "{}", logs(&output));
    let commands = fixture.commands();
    let mut deletes: Vec<_> = commands
        .lines()
        .filter(|line| line.contains(" del "))
        .collect();
    deletes.sort_unstable();
    let mut expected = vec![
        "-4 addr del 192.0.2.100/24 dev cleanup0",
        "-4 addr del 192.0.2.101/24 dev cleanup0",
        "-6 addr del 2001:db8::100/64 dev cleanup0",
        "-6 addr del 2001:db8::101/64 dev cleanup0",
        "-4 route del table 10245 throw 192.0.2.101/32 proto 245",
        "-6 route del table 10245 throw 2001:db8::101/128 proto 245",
    ];
    expected.sort_unstable();
    assert_eq!(deletes, expected);
}

#[tokio::test]
async fn cleanup_only_accepts_already_absent_addresses_after_exact_host_probes() {
    let mut fixture = Fixture::new();
    fixture.config = fixture.config.replace("dry_run: true", "dry_run: false");
    let output = fixture.run(true, "absent").await;
    assert!(output.status.success(), "{}", logs(&output));
    let commands = fixture.commands();
    assert!(
        commands.contains("-4 -o addr show to 192.0.2.100\n"),
        "{commands}"
    );
    assert!(
        commands.contains("-6 -o addr show to 2001:db8::100\n"),
        "{commands}"
    );
}

#[tokio::test]
async fn cleanup_only_propagates_discovery_address_and_marker_failures() {
    for (mode, message) in [
        ("discovery-failed", "`ip -j addr show` failed"),
        ("address-present", "address is still present"),
        ("marker-present", "marker is still present"),
    ] {
        let mut fixture = Fixture::new();
        fixture.config = fixture.config.replace("dry_run: true", "dry_run: false");
        let output = fixture.run(true, mode).await;
        let logs = logs(&output);
        assert!(!output.status.success(), "{mode}: {logs}");
        assert!(logs.contains(message), "{mode}: {logs}");
        if mode != "marker-present" {
            assert!(!fixture.commands().contains(" route del "));
        }
    }
}

#[test]
fn packaged_stop_hook_uses_the_same_instance_configuration_and_propagates_failure() {
    let unit = include_str!("../../deploy/systemd/keepafloatd@.service");
    let start = unit
        .lines()
        .find_map(|line| line.strip_prefix("ExecStart="))
        .unwrap();
    let stop = unit
        .lines()
        .find_map(|line| line.strip_prefix("ExecStopPost="));
    assert_eq!(stop, Some(format!("{start} --cleanup-only").as_str()));
}
