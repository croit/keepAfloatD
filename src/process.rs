//! Bounded Linux child-process ownership shared by health probes, notify hooks, and VIP effects.
//!
//! Every child starts a new process group. Dropping [`GroupedChild`] synchronously kills that
//! group, so timeout and async-task cancellation cannot leave shell descendants behind (#24).

use std::io;
use std::process::{ExitStatus, Output, Stdio};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::{Child, ChildStderr, ChildStdout, Command};
use tokio::task::JoinHandle;

/// Bound reaping after a timed-out process group has received SIGKILL.
const PROCESS_REAP_TIMEOUT: Duration = Duration::from_millis(500);
/// Grace period for draining a child's stdout/stderr after it has exited (or been killed). A
/// child that forks a background descendant inheriting the pipe (`curl ... &`, `nc -l &`) keeps
/// the write end open, so the reader never sees EOF; without this bound callers would never
/// return.
const OUTPUT_DRAIN_TIMEOUT: Duration = Duration::from_millis(500);
/// Capture bound for strict `wait_output` callers.
const MAX_OUTPUT_BYTES: usize = 64 * 1024;

/// Bounded capture of one child stream: the first `limit` bytes plus a count of what was dropped.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct CapturedOutput {
    bytes: Vec<u8>,
    truncated_bytes: usize,
}

impl CapturedOutput {
    fn record_chunk(&mut self, chunk: &[u8], limit: usize) {
        let keep = limit.saturating_sub(self.bytes.len()).min(chunk.len());
        self.bytes.extend_from_slice(&chunk[..keep]);
        self.truncated_bytes += chunk.len().saturating_sub(keep);
    }

    pub(crate) fn preview(&self) -> String {
        String::from_utf8_lossy(&self.bytes).into_owned()
    }

    #[must_use]
    pub(crate) const fn truncated_bytes(&self) -> usize {
        self.truncated_bytes
    }
}

/// How a captured child finished.
#[derive(Debug)]
pub(crate) enum Completion {
    Exited(ExitStatus),
    WaitFailed(io::Error),
    /// The child outlived its budget; `reap_error` is set when the post-kill wait also failed.
    TimedOut {
        reap_error: Option<String>,
    },
}

/// Lenient result of [`GroupedChild::wait_captured`] for diagnostics-oriented callers.
#[derive(Debug)]
pub(crate) struct CapturedRun {
    pub(crate) completion: Completion,
    /// Failure to terminate the child's process group after it finished.
    pub(crate) cleanup_error: Option<io::Error>,
    pub(crate) stdout: CapturedOutput,
    pub(crate) stderr: CapturedOutput,
}

struct ProcessGroupGuard {
    pgid: i32,
    armed: bool,
}

impl ProcessGroupGuard {
    fn new(pgid: u32) -> io::Result<Self> {
        let pgid = i32::try_from(pgid)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "child pid exceeds i32"))?;
        if pgid <= 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "child process group id must be positive",
            ));
        }
        Ok(Self { pgid, armed: true })
    }

    fn terminate(&mut self) -> io::Result<()> {
        if !self.armed {
            return Ok(());
        }
        // SAFETY: `pgid` is a validated positive child PID created as a new process group. Its
        // negation signals only that child group, never keepafloatd's own process group.
        let result = unsafe { libc::kill(-self.pgid, libc::SIGKILL) };
        if result == 0 {
            self.armed = false;
            return Ok(());
        }
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::ESRCH) {
            self.armed = false;
            return Ok(());
        }
        Err(error)
    }
}

impl Drop for ProcessGroupGuard {
    fn drop(&mut self) {
        if let Err(error) = self.terminate() {
            tracing::error!(
                process_group = self.pgid,
                error = %error,
                "failed to terminate cancelled child process group"
            );
        }
    }
}

/// Direct child plus the kill-on-drop guard for its dedicated process group.
pub(crate) struct GroupedChild {
    child: Child,
    group: ProcessGroupGuard,
}

impl GroupedChild {
    pub(crate) fn take_stdout(&mut self) -> Option<ChildStdout> {
        self.child.stdout.take()
    }

    pub(crate) fn take_stderr(&mut self) -> Option<ChildStderr> {
        self.child.stderr.take()
    }

    pub(crate) async fn wait_status(mut self, timeout: Duration) -> io::Result<ExitStatus> {
        let wait_result = tokio::time::timeout(timeout, self.child.wait()).await;
        let cleanup_result = self.group.terminate();
        match wait_result {
            Ok(Ok(status)) => {
                cleanup_result?;
                Ok(status)
            }
            Ok(Err(error)) => {
                cleanup_result?;
                Err(error)
            }
            Err(_) => {
                cleanup_result?;
                let reap = tokio::time::timeout(PROCESS_REAP_TIMEOUT, self.child.wait())
                    .await
                    .ok();
                Err(timed_out_error(timeout, reap))
            }
        }
    }

    /// Strict capture: a stalled drain or a timed-out child is an error.
    pub(crate) async fn wait_output(mut self, timeout: Duration) -> io::Result<Output> {
        let stdout_task = spawn_capture_reader(self.take_stdout(), MAX_OUTPUT_BYTES);
        let stderr_task = spawn_capture_reader(self.take_stderr(), MAX_OUTPUT_BYTES);
        let status = self.wait_status(timeout).await;
        let (stdout, stderr) =
            tokio::join!(collect_capture(stdout_task), collect_capture(stderr_task));
        Ok(Output {
            status: status?,
            stdout: stdout?.bytes,
            stderr: stderr?.bytes,
        })
    }

    /// Lenient capture for diagnostics: every outcome is reported in the returned value, and a
    /// pipe held open past the drain bound yields an empty capture instead of an error.
    pub(crate) async fn wait_captured(mut self, timeout: Duration, limit: usize) -> CapturedRun {
        let stdout_task = spawn_capture_reader(self.take_stdout(), limit);
        let stderr_task = spawn_capture_reader(self.take_stderr(), limit);
        let wait_result = tokio::time::timeout(timeout, self.child.wait()).await;
        let cleanup_error = self.group.terminate().err();
        let completion = match wait_result {
            Ok(Ok(status)) => Completion::Exited(status),
            Ok(Err(error)) => Completion::WaitFailed(error),
            Err(_) => {
                let reap_error =
                    match tokio::time::timeout(PROCESS_REAP_TIMEOUT, self.child.wait()).await {
                        Ok(Ok(_)) => None,
                        Ok(Err(error)) => Some(error.to_string()),
                        Err(_) => Some("child reap timed out".to_owned()),
                    };
                Completion::TimedOut { reap_error }
            }
        };
        let (stdout, stderr) =
            tokio::join!(collect_capture(stdout_task), collect_capture(stderr_task));
        CapturedRun {
            completion,
            cleanup_error,
            stdout: stdout.unwrap_or_default(),
            stderr: stderr.unwrap_or_default(),
        }
    }
}

/// Error for a child that outlived `timeout`. `reap` is the post-kill wait result, or `None`
/// when reaping itself timed out; the message distinguishes the three outcomes so the operator
/// can tell a slow probe from a stuck kernel wait.
fn timed_out_error(timeout: Duration, reap: Option<io::Result<ExitStatus>>) -> io::Error {
    match reap {
        Some(Ok(_)) => io::Error::new(
            io::ErrorKind::TimedOut,
            format!("child command exceeded {}ms", timeout.as_millis()),
        ),
        Some(Err(error)) => {
            io::Error::other(format!("child command timed out and reap failed: {error}"))
        }
        None => io::Error::other("child command timed out and child reap also timed out"),
    }
}

/// Spawn `command` in a dedicated Linux process group with direct-child kill-on-drop enabled.
pub(crate) fn spawn_grouped(command: &mut Command) -> io::Result<GroupedChild> {
    command.process_group(0).kill_on_drop(true);
    let child = command.spawn()?;
    let pid = child.id().ok_or_else(|| {
        io::Error::other("spawned child did not expose a process-group identifier")
    })?;
    let group = ProcessGroupGuard::new(pid)?;
    Ok(GroupedChild { child, group })
}

pub(crate) async fn run_status(command: &mut Command, timeout: Duration) -> io::Result<ExitStatus> {
    spawn_grouped(command)?.wait_status(timeout).await
}

pub(crate) async fn run_output(command: &mut Command, timeout: Duration) -> io::Result<Output> {
    spawn_grouped(command)?.wait_output(timeout).await
}

async fn read_captured_output<R>(mut reader: R, limit: usize) -> io::Result<CapturedOutput>
where
    R: AsyncRead + Unpin,
{
    let mut captured = CapturedOutput::default();
    let mut buffer = [0_u8; 1024];
    loop {
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
            return Ok(captured);
        }
        captured.record_chunk(&buffer[..read], limit);
    }
}

// Lifetime: bounded by `collect_capture`, which aborts the reader once the drain grace expires.
fn spawn_capture_reader<R>(
    reader: Option<R>,
    limit: usize,
) -> Option<JoinHandle<io::Result<CapturedOutput>>>
where
    R: AsyncRead + Unpin + Send + 'static,
{
    reader.map(|reader| tokio::spawn(read_captured_output(reader, limit)))
}

async fn collect_capture(
    task: Option<JoinHandle<io::Result<CapturedOutput>>>,
) -> io::Result<CapturedOutput> {
    let Some(task) = task else {
        return Ok(CapturedOutput::default());
    };
    let abort = task.abort_handle();
    match tokio::time::timeout(OUTPUT_DRAIN_TIMEOUT, task).await {
        Ok(Ok(result)) => result,
        Ok(Err(error)) => Err(io::Error::other(format!(
            "output reader task failed: {error}"
        ))),
        Err(_) => {
            abort.abort();
            Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "child output drain timed out",
            ))
        }
    }
}

pub(crate) fn null_stdio(command: &mut Command) {
    command.stdout(Stdio::null()).stderr(Stdio::null());
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_ID: AtomicU64 = AtomicU64::new(0);

    fn shell(script: &str) -> Command {
        let mut command = Command::new("/bin/sh");
        command.args(["-c", script]);
        command
    }

    fn unique_pid_file(label: &str) -> PathBuf {
        let id = TEST_ID.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "keepafloatd-process-{label}-{}-{id}.pid",
            std::process::id()
        ))
    }

    async fn read_pid(path: &Path) -> u32 {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match tokio::fs::read_to_string(path).await {
                    Ok(value) if !value.trim().is_empty() => {
                        return value.trim().parse().expect("pid must be numeric");
                    }
                    Ok(_) => tokio::task::yield_now().await,
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {
                        tokio::task::yield_now().await;
                    }
                    Err(error) => panic!("failed to read {}: {error}", path.display()),
                }
            }
        })
        .await
        .expect("child did not write its pid")
    }

    /// A process counts as gone once it is reaped or has become a zombie: a zombie is already
    /// dead and only waits for its parent's `wait`, which coverage tracing can delay by holding
    /// back SIGCHLD delivery. The guarantee under test is that the group was killed.
    fn process_exists(pid: u32) -> bool {
        match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
            Ok(stat) => {
                let state = stat
                    .rsplit(") ")
                    .next()
                    .and_then(|rest| rest.chars().next());
                state != Some('Z')
            }
            Err(_) => false,
        }
    }

    async fn wait_until_gone(pids: &[u32]) -> bool {
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

    async fn force_cleanup(pids: &[u32], files: &[PathBuf]) {
        for pid in pids {
            if process_exists(*pid) {
                let _ = Command::new("/bin/kill")
                    .args(["-KILL", &pid.to_string()])
                    .status()
                    .await;
            }
        }
        for file in files {
            let _ = tokio::fs::remove_file(file).await;
        }
    }

    fn pid_script(parent: &Path, child: &Path) -> String {
        format!(
            "echo $$ > {}; sleep 30 & echo $! > {}; wait",
            parent.display(),
            child.display()
        )
    }

    #[test]
    fn process_group_rejects_zero_and_oversized_pids() {
        for pid in [0, u32::MAX] {
            let error = match ProcessGroupGuard::new(pid) {
                Ok(_) => panic!("{pid} must not be accepted as a process group id"),
                Err(error) => error,
            };
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput, "{pid}");
        }
    }

    #[test]
    fn timed_out_error_reports_every_reap_outcome() {
        use std::os::unix::process::ExitStatusExt;

        let reaped = timed_out_error(
            Duration::from_millis(100),
            Some(Ok(ExitStatus::from_raw(9))),
        );
        assert_eq!(reaped.kind(), io::ErrorKind::TimedOut);
        assert!(reaped.to_string().contains("exceeded 100ms"), "{reaped}");

        let reap_failed = timed_out_error(
            Duration::from_millis(100),
            Some(Err(io::Error::other("no child"))),
        );
        assert!(
            reap_failed.to_string().contains("reap failed: no child"),
            "{reap_failed}"
        );

        let reap_timed_out = timed_out_error(Duration::from_millis(100), None);
        assert!(
            reap_timed_out
                .to_string()
                .contains("child reap also timed out"),
            "{reap_timed_out}"
        );
    }

    #[tokio::test]
    async fn run_status_timeout_kills_and_reaps_the_child_and_descendant() {
        let parent_file = unique_pid_file("timeout-parent");
        let child_file = unique_pid_file("timeout-child");
        let mut command = shell(&pid_script(&parent_file, &child_file));
        null_stdio(&mut command);

        let result = run_status(&mut command, Duration::from_millis(100)).await;
        let parent = read_pid(&parent_file).await;
        let child = read_pid(&child_file).await;
        let exited = wait_until_gone(&[parent, child]).await;
        force_cleanup(&[parent, child], &[parent_file, child_file]).await;

        let error = result.expect_err("a timed-out child must be reported");
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(exited, "timeout must kill and reap the whole process group");
    }

    #[tokio::test]
    async fn dropping_a_grouped_child_kills_the_descendant() {
        let parent_file = unique_pid_file("drop-parent");
        let child_file = unique_pid_file("drop-child");
        let script = pid_script(&parent_file, &child_file);
        let task = tokio::spawn(async move {
            let mut command = shell(&script);
            null_stdio(&mut command);
            run_status(&mut command, Duration::from_secs(30)).await
        });
        let parent = read_pid(&parent_file).await;
        let child = read_pid(&child_file).await;

        task.abort();
        let _ = task.await;
        let exited = wait_until_gone(&[parent, child]).await;
        force_cleanup(&[parent, child], &[parent_file, child_file]).await;

        assert!(
            exited,
            "cancellation must kill and reap the whole process group"
        );
    }

    #[tokio::test]
    async fn captured_output_is_bounded_while_the_pipe_is_drained() {
        let mut command = shell("head -c 100000 /dev/zero | tr '\\0' a; printf err >&2");
        command.stdout(Stdio::piped()).stderr(Stdio::piped());

        let output = run_output(&mut command, Duration::from_secs(2))
            .await
            .unwrap();

        assert!(output.status.success());
        assert_eq!(output.stdout.len(), MAX_OUTPUT_BYTES);
        assert!(output.stdout.iter().all(|byte| *byte == b'a'));
        assert_eq!(output.stderr, b"err");
    }

    #[tokio::test]
    async fn run_output_reports_spawn_failure_and_nonzero_exit() {
        let mut missing = Command::new("/definitely/missing-keepafloatd-child");
        let error = run_output(&mut missing, Duration::from_secs(1))
            .await
            .expect_err("a missing binary must fail to spawn");
        assert_eq!(error.kind(), io::ErrorKind::NotFound);

        let mut nonzero = shell("exit 7");
        null_stdio(&mut nonzero);
        let output = run_output(&mut nonzero, Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(output.status.code(), Some(7));
        assert!(output.stdout.is_empty() && output.stderr.is_empty());
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
    async fn wait_captured_reports_truncation_and_exit_status() {
        let mut command = shell("printf abcdefgh; printf err >&2; exit 3");
        command.stdout(Stdio::piped()).stderr(Stdio::piped());

        let run = spawn_grouped(&mut command)
            .unwrap()
            .wait_captured(Duration::from_secs(2), 5)
            .await;

        let Completion::Exited(status) = run.completion else {
            panic!("expected an exited completion, got {:?}", run.completion);
        };
        assert_eq!(status.code(), Some(3));
        assert!(run.cleanup_error.is_none());
        assert_eq!(run.stdout.preview(), "abcde");
        assert_eq!(run.stdout.truncated_bytes(), 3);
        assert_eq!(run.stderr.preview(), "err");
    }

    #[tokio::test]
    async fn wait_captured_timeout_reports_the_reap_outcome() {
        let mut command = shell("sleep 30");
        command.stdout(Stdio::piped()).stderr(Stdio::piped());

        let run = spawn_grouped(&mut command)
            .unwrap()
            .wait_captured(Duration::from_millis(100), 16)
            .await;

        assert!(
            matches!(run.completion, Completion::TimedOut { reap_error: None }),
            "{:?}",
            run.completion
        );
        assert!(run.cleanup_error.is_none());
        assert!(run.stdout.preview().is_empty());
    }

    #[tokio::test]
    async fn grouped_output_captures_both_streams() {
        let mut command = Command::new("/bin/sh");
        command
            .args(["-c", "printf stdout; printf stderr >&2"])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let output = run_output(&mut command, Duration::from_secs(1))
            .await
            .unwrap();

        assert!(output.status.success());
        assert_eq!(output.stdout, b"stdout");
        assert_eq!(output.stderr, b"stderr");
    }
}
