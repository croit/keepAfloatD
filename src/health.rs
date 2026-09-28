//! Local script/command health probe.
//!
//! The shared `process` module owns Linux process-group setup and teardown so cancellation can
//! synchronously kill the direct probe and every descendant before Tokio drops the async task.

use crate::config::HealthConfig;
use crate::process::Completion;
use std::process::Stdio;
use std::time::Duration;
use tokio::process::Command;

/// Converts raw probe results into effective local health with deterministic failure dampening.
///
/// Only a node that has already been healthy receives the configured grace period. This prevents
/// a failing startup probe from becoming optimistically healthy merely because a delay is set.
pub(crate) struct FailureDampener {
    delay_ticks: u64,
    failure_ticks: u64,
    effective_healthy: bool,
}

impl FailureDampener {
    #[must_use]
    pub(crate) fn new(delay_ticks: u64) -> Self {
        Self {
            delay_ticks,
            failure_ticks: 0,
            effective_healthy: false,
        }
    }

    /// Observe one raw probe result and return the health value exposed to binding and Raft.
    pub(crate) fn observe(&mut self, raw_healthy: bool) -> bool {
        if raw_healthy {
            self.failure_ticks = 0;
            self.effective_healthy = true;
            return true;
        }
        if !self.effective_healthy {
            return false;
        }

        self.failure_ticks = self.failure_ticks.saturating_add(1);
        if self.failure_ticks > self.delay_ticks {
            self.effective_healthy = false;
        }
        self.effective_healthy
    }

    #[must_use]
    pub(crate) fn failure_ticks(&self) -> u64 {
        self.failure_ticks
    }
}

/// Bound diagnostic previews so a noisy probe cannot flood logs or memory.
const MAX_CAPTURE_BYTES: usize = 4 * 1024;

/// Run `command[0]` with `command[1..]` as argv, returning `true` only for exit status 0.
///
/// Failed probes stay unhealthy exactly as before, but now emit structured diagnostics that
/// distinguish spawn failures, non-zero exits, wait errors and wall-clock timeouts.
pub async fn run_health_check(cfg: &HealthConfig) -> bool {
    let (prog, args) = match cfg.command.split_first() {
        Some((p, a)) => (p, a),
        None => return false,
    };

    let mut command = Command::new(prog);
    command
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let child = match crate::process::spawn_grouped(&mut command) {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(
                command = ?cfg.command,
                error = %e,
                "health probe spawn failed"
            );
            return false;
        }
    };
    let timeout = Duration::from_millis(cfg.timeout_ms.max(1));

    let run = child.wait_captured(timeout, MAX_CAPTURE_BYTES).await;
    let stdout_preview = run.stdout.preview();
    let stderr_preview = run.stderr.preview();
    match run.completion {
        Completion::Exited(status) => {
            if let Some(error) = run.cleanup_error {
                tracing::warn!(
                    command = ?cfg.command,
                    %error,
                    "health probe exited but descendant cleanup failed"
                );
                return false;
            }
            if status.success() {
                return true;
            }
            tracing::warn!(
                command = ?cfg.command,
                exit_status = %status,
                exit_code = ?status.code(),
                stdout = ?stdout_preview,
                stdout_truncated_bytes = run.stdout.truncated_bytes(),
                stderr = ?stderr_preview,
                stderr_truncated_bytes = run.stderr.truncated_bytes(),
                "health probe exited non-zero"
            );
            false
        }
        Completion::WaitFailed(e) => {
            tracing::warn!(
                command = ?cfg.command,
                error = %e,
                cleanup_error = ?run.cleanup_error.map(|error| error.to_string()),
                stdout = ?stdout_preview,
                stdout_truncated_bytes = run.stdout.truncated_bytes(),
                stderr = ?stderr_preview,
                stderr_truncated_bytes = run.stderr.truncated_bytes(),
                "health probe wait failed"
            );
            false
        }
        Completion::TimedOut { reap_error } => {
            tracing::warn!(
                command = ?cfg.command,
                timeout_ms = cfg.timeout_ms,
                cleanup_error = ?run.cleanup_error.map(|error| error.to_string()),
                reap_error = ?reap_error,
                stdout = ?stdout_preview,
                stdout_truncated_bytes = run.stdout.truncated_bytes(),
                stderr = ?stderr_preview,
                stderr_truncated_bytes = run.stderr.truncated_bytes(),
                "health probe timed out"
            );
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex};
    use tracing::instrument::WithSubscriber;

    #[derive(Clone, Default)]
    struct DiagnosticBuffer(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for DiagnosticBuffer {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    async fn recorded_probe(cfg: &HealthConfig) -> (bool, String) {
        let output = DiagnosticBuffer::default();
        let writer = output.clone();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_max_level(tracing::Level::WARN)
            .with_writer(move || writer.clone())
            .finish();
        let healthy = run_health_check(cfg).with_subscriber(subscriber).await;
        let text = String::from_utf8(output.0.lock().unwrap().clone()).unwrap();
        (healthy, text)
    }

    #[tokio::test]
    async fn failed_probe_reports_bounded_output_and_exit_code() {
        let cfg = health_cfg(
            &[
                "/bin/sh",
                "-c",
                "i=0; while [ $i -lt 5000 ]; do printf x; i=$((i+1)); done; printf failure >&2; exit 7",
            ],
            2000,
        );
        let (healthy, log) = recorded_probe(&cfg).await;
        assert!(!healthy);
        assert!(log.contains("health probe exited non-zero"), "{log}");
        assert!(log.contains("exit_code=Some(7)"), "{log}");
        assert!(log.contains("stderr=\"failure\""), "{log}");
        assert!(log.contains("stdout_truncated_bytes=904"), "{log}");
        assert!(log.contains(&"x".repeat(MAX_CAPTURE_BYTES)), "{log}");
        assert!(!log.contains(&"x".repeat(MAX_CAPTURE_BYTES + 1)));
    }

    #[tokio::test]
    async fn failed_spawn_and_timeout_have_distinct_diagnostics() {
        let (healthy, log) =
            recorded_probe(&health_cfg(&["/definitely/missing-keepafloatd-probe"], 200)).await;
        assert!(!healthy);
        assert!(log.contains("health probe spawn failed"), "{log}");
        assert!(!log.contains("health probe timed out"));

        let (healthy, log) =
            recorded_probe(&health_cfg(&["/bin/sh", "-c", "exec sleep 10"], 200)).await;
        assert!(!healthy);
        assert!(log.contains("health probe timed out"), "{log}");
        assert!(log.contains("timeout_ms=200"), "{log}");
        assert!(!log.contains("health probe spawn failed"));
    }

    #[tokio::test]
    async fn successful_probe_emits_no_failure_diagnostics() {
        let (healthy, log) = recorded_probe(&health_cfg(&["/bin/sh", "-c", "exit 0"], 200)).await;
        assert!(healthy);
        assert!(log.is_empty(), "{log}");
    }

    fn health_cfg(command: &[&str], timeout_ms: u64) -> HealthConfig {
        HealthConfig {
            command: command.iter().map(ToString::to_string).collect(),
            interval_ms: 1_000,
            timeout_ms,
            stale_secs: None,
        }
    }

    fn unique_pid_file(label: &str) -> PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "keepafloatd-health-{label}-{}-{nonce}.pid",
            std::process::id()
        ))
    }

    async fn read_pid_file(path: &Path) -> u32 {
        for _ in 0..100 {
            if let Ok(contents) = tokio::fs::read_to_string(path).await {
                return contents.trim().parse().unwrap();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("timed out waiting for pid file {}", path.display());
    }

    fn process_exists(pid: u32) -> bool {
        Path::new(&format!("/proc/{pid}")).exists()
    }

    async fn wait_for_processes_to_exit(pids: &[u32]) -> bool {
        // Coverage runs trace child exits and may delay `/proc` disappearance beyond the normal
        // sub-second path. Keep the oracle bounded below the five-second focused-test budget.
        for _ in 0..300 {
            if pids.iter().all(|pid| !process_exists(*pid)) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        false
    }

    async fn cleanup_processes(pids: &[u32]) {
        for pid in pids {
            if process_exists(*pid) {
                let _ = Command::new("/bin/kill")
                    .args(["-KILL", &pid.to_string()])
                    .status()
                    .await;
            }
        }
        let _ = wait_for_processes_to_exit(pids).await;
    }

    #[test]
    fn failure_dampener_keeps_startup_failure_unhealthy() {
        let mut dampener = FailureDampener::new(3);

        assert!(!dampener.observe(false));
        assert!(!dampener.observe(false));
    }

    #[test]
    fn failure_dampener_delays_established_health_failure() {
        let mut dampener = FailureDampener::new(2);

        assert!(dampener.observe(true));
        assert!(dampener.observe(false));
        assert!(dampener.observe(false));
        assert!(!dampener.observe(false));
    }

    #[test]
    fn failure_dampener_zero_delay_fails_immediately() {
        let mut dampener = FailureDampener::new(0);

        assert!(dampener.observe(true));
        assert!(!dampener.observe(false));
    }

    #[test]
    fn failure_dampener_recovery_resets_failure_streak() {
        let mut dampener = FailureDampener::new(2);

        assert!(dampener.observe(true));
        assert!(dampener.observe(false));
        assert!(dampener.observe(true));
        assert!(dampener.observe(false));
        assert!(dampener.observe(false));
        assert!(!dampener.observe(false));
    }

    #[tokio::test]
    async fn health_check_success_exit_zero_is_healthy() {
        assert!(run_health_check(&health_cfg(&["/bin/sh", "-c", "exit 0"], 200)).await);
    }

    #[tokio::test]
    async fn health_check_spawn_failure_is_unhealthy() {
        assert!(
            !run_health_check(&health_cfg(&["/definitely/missing-keepafloatd-probe"], 200)).await
        );
    }

    #[tokio::test]
    async fn health_check_non_zero_exit_is_unhealthy() {
        assert!(
            !run_health_check(&health_cfg(
                &["/bin/sh", "-c", "printf fail >&2; exit 7"],
                200
            ))
            .await
        );
    }

    #[tokio::test]
    async fn health_check_timeout_is_unhealthy() {
        assert!(!run_health_check(&health_cfg(&["/bin/sh", "-c", "echo slow; sleep 1"], 20)).await);
    }

    #[tokio::test]
    async fn cancelling_health_check_kills_direct_child_and_descendant() {
        let parent_file = unique_pid_file("cancel-parent");
        let child_file = unique_pid_file("cancel-child");
        let script = format!(
            "echo $$ > {}; sleep 30 & echo $! > {}; wait",
            parent_file.display(),
            child_file.display()
        );
        let cfg = health_cfg(&["/bin/sh", "-c", &script], 30_000);
        let task = tokio::spawn(async move { run_health_check(&cfg).await });
        let parent_pid = read_pid_file(&parent_file).await;
        let child_pid = read_pid_file(&child_file).await;

        task.abort();
        let _ = task.await;
        let exited = wait_for_processes_to_exit(&[parent_pid, child_pid]).await;
        cleanup_processes(&[parent_pid, child_pid]).await;
        let _ = tokio::fs::remove_file(parent_file).await;
        let _ = tokio::fs::remove_file(child_file).await;

        assert!(
            exited,
            "cancelling a health future must kill and reap its whole process group"
        );
    }

    #[tokio::test]
    async fn successful_probe_kills_background_descendant_before_returning() {
        let child_file = unique_pid_file("success-child");
        let script = format!("sleep 30 & echo $! > {}; exit 0", child_file.display());

        assert!(run_health_check(&health_cfg(&["/bin/sh", "-c", &script], 500)).await);
        let child_pid = read_pid_file(&child_file).await;
        let exited = wait_for_processes_to_exit(&[child_pid]).await;
        cleanup_processes(&[child_pid]).await;
        let _ = tokio::fs::remove_file(child_file).await;

        assert!(
            exited,
            "a successful probe must not leave background descendants running"
        );
    }

    #[tokio::test]
    async fn health_check_returns_when_a_grandchild_holds_the_pipe() {
        // The probe exits 0 immediately but backgrounds a child that inherits stdout, holding the
        // pipe write-end open. Draining must not hang past OUTPUT_DRAIN_TIMEOUT, or the health loop
        // would stall and the node would be fenced as stale forever.
        let start = std::time::Instant::now();
        let healthy =
            run_health_check(&health_cfg(&["/bin/sh", "-c", "sleep 3 & exit 0"], 500)).await;
        assert!(
            healthy,
            "exit 0 is healthy regardless of the lingering grandchild"
        );
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "run_health_check must not block on the inherited pipe (took {:?})",
            start.elapsed()
        );
    }

    #[tokio::test]
    async fn empty_command_is_unhealthy() {
        assert!(!run_health_check(&health_cfg(&[], 200)).await);
    }

    #[tokio::test]
    async fn argv_is_forwarded_to_the_child() {
        // Exit code is driven purely by a forwarded positional arg, proving argv pass-through.
        assert!(
            run_health_check(&health_cfg(
                &["/bin/sh", "-c", "exit \"$1\"", "_", "0"],
                500
            ))
            .await
        );
        assert!(
            !run_health_check(&health_cfg(
                &["/bin/sh", "-c", "exit \"$1\"", "_", "5"],
                500
            ))
            .await
        );
    }

    #[tokio::test]
    async fn completes_well_within_timeout_is_healthy() {
        assert!(
            run_health_check(&health_cfg(&["/bin/sh", "-c", "sleep 0.05; exit 0"], 2000)).await
        );
    }

    #[tokio::test]
    async fn large_output_is_captured_and_nonzero_exit_is_unhealthy() {
        // ~10 KiB of stdout exceeds the 4 KiB capture cap; the probe still resolves cleanly.
        assert!(
            !run_health_check(&health_cfg(
                &[
                    "/bin/sh",
                    "-c",
                    "head -c 10000 /dev/zero | tr '\\0' a; exit 3"
                ],
                2000
            ))
            .await
        );
    }
}
