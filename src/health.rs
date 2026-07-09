//! Local script/command health probe.

use crate::config::HealthConfig;
use std::io;
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::Command;
use tokio::task::JoinHandle;

/// Bound diagnostic previews so a noisy probe cannot flood logs or memory.
const MAX_CAPTURE_BYTES: usize = 4 * 1024;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct CapturedOutput {
    bytes: Vec<u8>,
    truncated_bytes: usize,
}

impl CapturedOutput {
    fn record_chunk(&mut self, chunk: &[u8], limit: usize) {
        let keep = limit.saturating_sub(self.bytes.len()).min(chunk.len());
        self.bytes.extend_from_slice(&chunk[..keep]);
        self.truncated_bytes += chunk.len().saturating_sub(keep);
    }

    fn preview(&self) -> String {
        String::from_utf8_lossy(&self.bytes).into_owned()
    }
}

async fn read_captured_output<R>(mut reader: R, limit: usize) -> io::Result<CapturedOutput>
where
    R: AsyncRead + Unpin,
{
    let mut captured = CapturedOutput::default();
    let mut buf = [0_u8; 1024];

    loop {
        let n = reader.read(&mut buf).await?;
        if n == 0 {
            return Ok(captured);
        }
        captured.record_chunk(&buf[..n], limit);
    }
}

fn spawn_output_reader<R>(reader: Option<R>) -> Option<JoinHandle<io::Result<CapturedOutput>>>
where
    R: AsyncRead + Unpin + Send + 'static,
{
    reader.map(|reader| {
        // Drain this child pipe until EOF so a chatty probe cannot block on a full buffer.
        tokio::spawn(read_captured_output(reader, MAX_CAPTURE_BYTES))
    })
}

/// Grace period for draining a probe's stdout/stderr after it has exited (or been killed). A probe
/// that forks a background child inheriting the pipe (`curl … &`, `nc -l &`) keeps the write-end
/// open, so the reader never sees EOF; without this bound `run_health_check` would never return and
/// the health-publication loop would stop ticking, fencing the node as stale forever.
const OUTPUT_DRAIN_TIMEOUT: Duration = Duration::from_millis(500);

async fn collect_output(task: Option<JoinHandle<io::Result<CapturedOutput>>>) -> CapturedOutput {
    match task {
        Some(task) => {
            let abort = task.abort_handle();
            match tokio::time::timeout(OUTPUT_DRAIN_TIMEOUT, task).await {
                Ok(Ok(Ok(output))) => output,
                Ok(_) => CapturedOutput::default(),
                Err(_) => {
                    abort.abort();
                    CapturedOutput::default()
                }
            }
        }
        None => CapturedOutput::default(),
    }
}

async fn collect_stdio(
    stdout_task: Option<JoinHandle<io::Result<CapturedOutput>>>,
    stderr_task: Option<JoinHandle<io::Result<CapturedOutput>>>,
) -> (CapturedOutput, CapturedOutput) {
    tokio::join!(collect_output(stdout_task), collect_output(stderr_task))
}

/// Run `command[0]` with `command[1..]` as argv, returning `true` only for exit status 0.
///
/// Failed probes stay unhealthy exactly as before, but now emit structured diagnostics that
/// distinguish spawn failures, non-zero exits, wait errors and wall-clock timeouts.
pub async fn run_health_check(cfg: &HealthConfig) -> bool {
    let (prog, args) = match cfg.command.split_first() {
        Some((p, a)) => (p, a),
        None => return false,
    };

    let mut child = match Command::new(prog)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
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

    let stdout_task = spawn_output_reader(child.stdout.take());
    let stderr_task = spawn_output_reader(child.stderr.take());
    let timeout = Duration::from_millis(cfg.timeout_ms.max(1));

    match tokio::time::timeout(timeout, child.wait()).await {
        Ok(Ok(status)) => {
            let (stdout, stderr) = collect_stdio(stdout_task, stderr_task).await;
            if status.success() {
                return true;
            }

            let stdout_preview = stdout.preview();
            let stderr_preview = stderr.preview();
            tracing::warn!(
                command = ?cfg.command,
                exit_status = %status,
                exit_code = ?status.code(),
                stdout = ?stdout_preview,
                stdout_truncated_bytes = stdout.truncated_bytes,
                stderr = ?stderr_preview,
                stderr_truncated_bytes = stderr.truncated_bytes,
                "health probe exited non-zero"
            );
            false
        }
        Ok(Err(e)) => {
            let _ = child.kill().await;
            let (stdout, stderr) = collect_stdio(stdout_task, stderr_task).await;
            let stdout_preview = stdout.preview();
            let stderr_preview = stderr.preview();
            tracing::warn!(
                command = ?cfg.command,
                error = %e,
                stdout = ?stdout_preview,
                stdout_truncated_bytes = stdout.truncated_bytes,
                stderr = ?stderr_preview,
                stderr_truncated_bytes = stderr.truncated_bytes,
                "health probe wait failed"
            );
            false
        }
        Err(_) => {
            let _ = child.kill().await;
            let (stdout, stderr) = collect_stdio(stdout_task, stderr_task).await;
            let stdout_preview = stdout.preview();
            let stderr_preview = stderr.preview();
            tracing::warn!(
                command = ?cfg.command,
                timeout_ms = cfg.timeout_ms,
                stdout = ?stdout_preview,
                stdout_truncated_bytes = stdout.truncated_bytes,
                stderr = ?stderr_preview,
                stderr_truncated_bytes = stderr.truncated_bytes,
                "health probe timed out"
            );
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn health_cfg(command: &[&str], timeout_ms: u64) -> HealthConfig {
        HealthConfig {
            command: command.iter().map(ToString::to_string).collect(),
            interval_ms: 1_000,
            timeout_ms,
            stale_secs: None,
        }
    }

    #[test]
    fn captured_output_truncates_across_chunks() {
        let mut captured = CapturedOutput::default();
        captured.record_chunk(b"abcd", 5);
        captured.record_chunk(b"efgh", 5);

        assert_eq!(captured.bytes, b"abcde");
        assert_eq!(captured.truncated_bytes, 3);
        assert_eq!(captured.preview(), "abcde");
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

    #[test]
    fn captured_output_limit_zero_truncates_everything() {
        let mut c = CapturedOutput::default();
        c.record_chunk(b"abc", 0);
        assert!(c.bytes.is_empty());
        assert_eq!(c.truncated_bytes, 3);
    }

    #[test]
    fn captured_output_records_up_to_exact_limit() {
        let mut c = CapturedOutput::default();
        c.record_chunk(b"abcde", 5);
        assert_eq!(c.bytes, b"abcde");
        assert_eq!(c.truncated_bytes, 0);
        // Once the limit is reached, a further chunk is fully truncated.
        c.record_chunk(b"fg", 5);
        assert_eq!(c.bytes, b"abcde");
        assert_eq!(c.truncated_bytes, 2);
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
