//! Local script/command health probe.
//!
//! The shared `process` module owns Linux process-group setup and teardown so cancellation can
//! synchronously kill the direct probe and every descendant before Tokio drops the async task.

use crate::config::HealthConfig;
use crate::process::Completion;
use std::process::Stdio;
use std::time::Duration;
use tokio::process::Command;

mod local;
pub(crate) use local::LocalHealth;

#[cfg(test)]
mod test_probe;

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
    use std::sync::{Arc, Mutex};

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

    async fn isolated_diagnostics(case: &str) -> Option<DiagnosticBuffer> {
        let test_name = format!("health::tests::{case}");
        if std::env::var("KEEPAFLOATD_DIAGNOSTIC_TEST").as_deref() != Ok(&test_name) {
            let child = tokio::time::timeout(
                Duration::from_secs(3),
                Command::new(std::env::current_exe().unwrap())
                    .args(["--exact", &test_name, "--nocapture", "--color=never"])
                    .env("KEEPAFLOATD_DIAGNOSTIC_TEST", &test_name)
                    .stdin(Stdio::null())
                    .kill_on_drop(true)
                    .output(),
            )
            .await
            .expect("isolated diagnostic test timed out")
            .unwrap();
            assert!(
                child.status.success(),
                "{test_name}: {}\n{}",
                String::from_utf8_lossy(&child.stdout),
                String::from_utf8_lossy(&child.stderr)
            );
            assert!(
                String::from_utf8_lossy(&child.stdout)
                    .contains(&format!("test {test_name} ... ok")),
                "child did not run {test_name}: {}",
                String::from_utf8_lossy(&child.stdout)
            );
            return None;
        }

        let output = DiagnosticBuffer::default();
        let writer = output.clone();
        // Isolate global callsite registration from the parallel test suite.
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_max_level(tracing::Level::WARN)
            .with_writer(move || writer.clone())
            .finish();
        tracing::subscriber::set_global_default(subscriber).unwrap();
        Some(output)
    }

    async fn recorded_probe(cfg: &HealthConfig, output: &DiagnosticBuffer) -> (bool, String) {
        output.0.lock().unwrap().clear();
        let healthy = run_health_check(cfg).await;
        let text = String::from_utf8(output.0.lock().unwrap().clone()).unwrap();
        (healthy, text)
    }

    #[tokio::test]
    async fn cold_probe_log_is_captured_after_an_unsubscribed_probe() {
        let output = DiagnosticBuffer::default();
        let writer = output.clone();
        let subscriber = tracing::Dispatch::new(
            tracing_subscriber::fmt()
                .without_time()
                .with_ansi(false)
                .with_max_level(tracing::Level::WARN)
                .with_writer(move || writer.clone())
                .finish(),
        );
        let cfg = health_cfg(&["/bin/sh", "-c", "exit 7"], 200);
        assert!(!run_health_check(&cfg).await);
        let Some(output) =
            isolated_diagnostics("cold_probe_log_is_captured_after_an_unsubscribed_probe").await
        else {
            return;
        };
        let (healthy, log) = recorded_probe(&cfg, &output).await;
        assert!(!healthy);
        assert!(
            log.contains("health probe exited non-zero"),
            "captured: {log:?}"
        );
        drop(subscriber);
    }

    #[tokio::test]
    async fn failed_probe_reports_bounded_output_and_exit_code() {
        let Some(output) =
            isolated_diagnostics("failed_probe_reports_bounded_output_and_exit_code").await
        else {
            return;
        };
        let cfg = health_cfg(
            &[
                "/bin/sh",
                "-c",
                "i=0; while [ $i -lt 5000 ]; do printf x; i=$((i+1)); done; printf failure >&2; exit 7",
            ],
            2000,
        );
        let (healthy, log) = recorded_probe(&cfg, &output).await;
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
        let Some(output) =
            isolated_diagnostics("failed_spawn_and_timeout_have_distinct_diagnostics").await
        else {
            return;
        };
        let (healthy, log) = recorded_probe(
            &health_cfg(&["/definitely/missing-keepafloatd-probe"], 200),
            &output,
        )
        .await;
        assert!(!healthy);
        assert!(log.contains("health probe spawn failed"), "{log}");
        assert!(!log.contains("health probe timed out"));

        let (healthy, log) = recorded_probe(
            &health_cfg(&["/bin/sh", "-c", "exec sleep 10"], 200),
            &output,
        )
        .await;
        assert!(!healthy);
        assert!(log.contains("health probe timed out"), "{log}");
        assert!(log.contains("timeout_ms=200"), "{log}");
        assert!(!log.contains("health probe spawn failed"));
    }

    #[tokio::test]
    async fn successful_probe_emits_no_failure_diagnostics() {
        let Some(output) =
            isolated_diagnostics("successful_probe_emits_no_failure_diagnostics").await
        else {
            return;
        };
        let (healthy, log) =
            recorded_probe(&health_cfg(&["/bin/sh", "-c", "exit 0"], 200), &output).await;
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
        let mut probe = test_probe::Probe::start("wait", 30_000).await;
        probe.assert_running();
        probe.cancel().await;
        probe.assert_stopped().await;
    }

    #[tokio::test]
    async fn successful_probe_kills_background_descendant_before_returning() {
        let mut probe = test_probe::Probe::start("exit 0", 500).await;
        assert!(probe.finish().await);
        probe.assert_stopped().await;
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
