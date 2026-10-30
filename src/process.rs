//! Bounded Linux child-process ownership shared by health probes, notify hooks, and VIP effects.
//!
//! Every child starts a new process group. Dropping [`GroupedChild`] synchronously kills that
//! group, so timeout and async-task cancellation cannot leave shell descendants behind (#24).

use std::io;
use std::os::unix::process::ExitStatusExt;
use std::process::{ExitStatus, Output, Stdio};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::{Child, ChildStderr, ChildStdout, Command};
use tokio::task::JoinHandle;

/// Bound reaping after a timed-out process group has received SIGKILL.
pub(crate) const PROCESS_REAP_TIMEOUT: Duration = Duration::from_millis(500);
/// Grace period for draining a child's stdout/stderr after it has exited (or been killed). A
/// child that forks a background descendant inheriting the pipe (`curl ... &`, `nc -l &`) keeps
/// the write end open, so the reader never sees EOF; without this bound callers would never
/// return.
pub(crate) const OUTPUT_DRAIN_TIMEOUT: Duration = Duration::from_millis(500);
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
        #[allow(unsafe_code)]
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

    pub(crate) async fn wait_status(self, timeout: Duration) -> io::Result<ExitStatus> {
        self.wait_status_or_cancel(timeout, std::future::pending())
            .await
            .0
    }

    /// [`Self::wait_status`] that `cancelled` cuts short by killing the group and reaping the child.
    /// The flag says whether the child was reaped, after which it can no longer act (#46).
    async fn wait_status_or_cancel(
        mut self,
        timeout: Duration,
        cancelled: impl std::future::Future<Output = ()>,
    ) -> (io::Result<ExitStatus>, bool) {
        let pid = self.group.pgid;
        let started = std::time::Instant::now();
        let wait_result = tokio::select! {
            biased;
            result = tokio::time::timeout(timeout, self.child.wait()) => Some(result),
            () = cancelled => None,
        };
        let wait_elapsed = started.elapsed();
        let cleanup_started = std::time::Instant::now();
        let cleanup_result = self.group.terminate();
        let cleanup_elapsed = cleanup_started.elapsed();
        match wait_result {
            Some(Ok(Ok(status))) => (cleanup_result.map(|()| status), true),
            Some(Ok(Err(error))) => (cleanup_result.and(Err(error)), false),
            Some(Err(_)) => {
                if let Err(error) = cleanup_result {
                    return (Err(error), false);
                }
                let reap_started = std::time::Instant::now();
                let reap = tokio::time::timeout(PROCESS_REAP_TIMEOUT, self.child.wait())
                    .await
                    .ok();
                let reap_elapsed = reap_started.elapsed();
                let status = reap.as_ref().and_then(|result| result.as_ref().ok());
                let child_reaped = status.is_some();
                let exit_code = status.and_then(ExitStatus::code);
                let signal = status.and_then(ExitStatusExt::signal);
                let error = timed_out_error(timeout, reap);
                let error = deadline_missed(
                    error.kind(),
                    format!(
                        "{error}; pid={pid} budget_ms={} wait_ms={} overrun_ms={} cleanup_ms={} reap_ms={} exit_code={exit_code:?} signal={signal:?}",
                        timeout.as_millis(),
                        wait_elapsed.as_millis(),
                        wait_elapsed.saturating_sub(timeout).as_millis(),
                        cleanup_elapsed.as_millis(),
                        reap_elapsed.as_millis(),
                    ),
                );
                (Err(error), child_reaped)
            }
            None => {
                if let Err(error) = cleanup_result {
                    return (Err(error), false);
                }
                let reaped = matches!(
                    tokio::time::timeout(PROCESS_REAP_TIMEOUT, self.child.wait()).await,
                    Ok(Ok(_))
                );
                let error = io::Error::new(
                    io::ErrorKind::Interrupted,
                    format!("command cancelled; pid={pid} reaped={reaped}"),
                );
                (Err(error), reaped)
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

/// Payload of the error for a child that outlived its deadline. The error kind still reports the
/// reap outcome, so callers use [`missed_deadline`] to tell a missed deadline from a failed command
/// (#44).
#[derive(Debug)]
struct DeadlineMissed(String);

impl std::fmt::Display for DeadlineMissed {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for DeadlineMissed {}

pub(crate) fn deadline_missed(kind: io::ErrorKind, message: String) -> io::Error {
    io::Error::new(kind, DeadlineMissed(message))
}

/// Whether `error` means the command missed its deadline, whatever the post-kill reap reported.
pub(crate) fn missed_deadline(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::TimedOut
        || error
            .get_ref()
            .is_some_and(|inner| inner.is::<DeadlineMissed>())
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

/// Spawn a noninteractive command with null stdin in an owned Linux process group.
pub(crate) fn spawn_grouped(command: &mut Command) -> io::Result<GroupedChild> {
    command
        .stdin(Stdio::null())
        .process_group(0)
        .kill_on_drop(true);
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

/// Counts address-changing children not yet proven reaped. One whose reap fails is lost: it keeps
/// the tracker unsettled for the life of the process, because it may still change kernel state,
/// and is reported so the daemon restarts under supervision instead of blocking releases forever.
#[derive(Clone)]
pub(crate) struct ChildTracker {
    state: std::sync::Arc<tokio::sync::watch::Sender<TrackedChildren>>,
}

#[derive(Clone, Copy, Default)]
struct TrackedChildren {
    unsettled: usize,
    lost: bool,
}

impl Default for ChildTracker {
    fn default() -> Self {
        Self {
            state: std::sync::Arc::new(tokio::sync::watch::Sender::new(TrackedChildren::default())),
        }
    }
}

impl ChildTracker {
    pub(crate) fn begin(&self) {
        self.state.send_modify(|state| state.unsettled += 1);
    }

    pub(crate) fn settle(&self) {
        self.state
            .send_modify(|state| state.unsettled = state.unsettled.saturating_sub(1));
    }

    /// The child's reap failed; it stays counted.
    pub(crate) fn lose(&self) {
        self.state.send_modify(|state| state.lost = true);
    }

    pub(crate) fn is_settled(&self) -> bool {
        self.state.borrow().unsettled == 0
    }

    /// Whether every tracked child is reaped within `budget`.
    pub(crate) async fn wait_settled(&self, budget: Duration) -> bool {
        let mut state = self.state.subscribe();
        tokio::time::timeout(budget, state.wait_for(|state| state.unsettled == 0))
            .await
            .is_ok_and(|result| result.is_ok())
    }

    /// Resolves once a tracked child is lost.
    pub(crate) async fn wait_lost(&self) {
        let mut state = self.state.subscribe();
        if state.wait_for(|state| state.lost).await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RunPhase {
    WaitedFor,
    Abandoned,
    Finished,
}

/// Counts a child from the moment its caller stops waiting for it: only then can it outlive the
/// caller, so a child that is still waited for holds no release back (#46). A child whose reap
/// failed stays counted. The lock makes each transition and its count change one step, so the
/// count never reads zero between them.
#[derive(Clone)]
struct TrackedRun {
    tracker: ChildTracker,
    phase: std::sync::Arc<std::sync::Mutex<RunPhase>>,
}

impl TrackedRun {
    fn new(tracker: ChildTracker) -> Self {
        Self {
            tracker,
            phase: std::sync::Arc::new(std::sync::Mutex::new(RunPhase::WaitedFor)),
        }
    }

    fn phase(&self) -> std::sync::MutexGuard<'_, RunPhase> {
        self.phase
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The caller stopped waiting before the child finished.
    fn abandon(&self) {
        let mut phase = self.phase();
        if *phase == RunPhase::WaitedFor {
            self.tracker.begin();
            *phase = RunPhase::Abandoned;
        }
    }

    /// The waiter is done with the child, which is reaped or lost.
    fn finish(&self, reaped: bool) {
        let mut phase = self.phase();
        match (*phase, reaped) {
            (RunPhase::Abandoned, true) => self.tracker.settle(),
            (RunPhase::WaitedFor, false) => self.tracker.begin(),
            _ => {}
        }
        if !reaped {
            self.tracker.lose();
        }
        *phase = RunPhase::Finished;
    }
}

/// Abandons the run, and cancels its child, when the caller is dropped before the result.
struct Abandonment {
    run: TrackedRun,
    cancel: Option<tokio::sync::oneshot::Sender<()>>,
}

impl Drop for Abandonment {
    fn drop(&mut self) {
        if let Some(cancel) = self.cancel.take() {
            self.run.abandon();
            drop(cancel);
        }
    }
}

/// [`run_status`] for a command that changes kernel state. Cancelling the caller kills and reaps
/// the child in the background instead of abandoning it, and `tracker` counts it until then (#46).
pub(crate) async fn run_status_tracked(
    command: &mut Command,
    timeout: Duration,
    tracker: &ChildTracker,
) -> io::Result<ExitStatus> {
    let child = spawn_grouped(command)?;
    let (cancel, cancelled) = tokio::sync::oneshot::channel::<()>();
    let run = TrackedRun::new(tracker.clone());
    let mut abandonment = Abandonment {
        run: run.clone(),
        cancel: Some(cancel),
    };
    // Owns the child until it is reaped or its reap times out, also after the caller is dropped.
    let waiter = tokio::spawn(async move {
        let cancelled = async {
            let _ = cancelled.await;
        };
        let (result, reaped) = child.wait_status_or_cancel(timeout, cancelled).await;
        run.finish(reaped);
        result
    });
    let result = waiter.await;
    abandonment.cancel = None;
    result.unwrap_or_else(|error| Err(io::Error::other(format!("command waiter failed: {error}"))))
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

    async fn assert_stdin_isolated(mode: &str, test_name: &str) {
        use std::io::BufRead;
        use tokio::io::AsyncWriteExt;

        const INPUT: &str = "parent input must remain unread\n";
        if std::env::var("KEEPAFLOATD_STDIN_TEST").as_deref() != Ok(test_name) {
            let mut child = Command::new(std::env::current_exe().unwrap())
                .args(["--exact", test_name, "--nocapture", "--color=never"])
                .env("KEEPAFLOATD_STDIN_TEST", test_name)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .kill_on_drop(true)
                .spawn()
                .unwrap();
            let mut input = child.stdin.take().unwrap();
            input.write_all(INPUT.as_bytes()).await.unwrap();
            let output = tokio::time::timeout(Duration::from_secs(3), child.wait_with_output())
                .await
                .expect("isolated stdin test timed out")
                .unwrap();
            drop(input);
            assert!(
                output.status.success(),
                "{test_name}: {}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(
                String::from_utf8_lossy(&output.stdout)
                    .contains(&format!("test {test_name} ... ok")),
                "isolated child did not execute {test_name}"
            );
            return;
        }

        let mut command =
            shell("if IFS= read -r input; then exit 7; fi; printf stdout; printf stderr >&2");
        let timeout = Duration::from_secs(1);
        match mode {
            "status" => {
                null_stdio(&mut command);
                let status = run_status(&mut command, timeout).await.unwrap();
                assert_eq!(
                    status.code(),
                    Some(0),
                    "child must see EOF, not parent input"
                );
            }
            "output" => {
                command.stdout(Stdio::piped()).stderr(Stdio::piped());
                let output = run_output(&mut command, timeout).await.unwrap();
                assert_eq!(output.status.code(), Some(0), "child must see EOF");
                assert_eq!(output.stdout, b"stdout");
                assert_eq!(output.stderr, b"stderr");
            }
            "captured" => {
                command.stdout(Stdio::piped()).stderr(Stdio::piped());
                let run = spawn_grouped(&mut command)
                    .unwrap()
                    .wait_captured(timeout, 64)
                    .await;
                let Completion::Exited(status) = run.completion else {
                    panic!("child did not exit: {:?}", run.completion);
                };
                assert_eq!(status.code(), Some(0), "child must see EOF");
                assert!(run.cleanup_error.is_none());
                assert_eq!(run.stdout.preview(), "stdout");
                assert_eq!(run.stderr.preview(), "stderr");
            }
            _ => panic!("unknown runner mode: {mode}"),
        }
        let mut remaining = String::new();
        std::io::stdin().lock().read_line(&mut remaining).unwrap();
        assert_eq!(remaining, INPUT, "parent input must stay unread");
    }

    #[tokio::test]
    async fn run_status_does_not_inherit_stdin() {
        assert_stdin_isolated(
            "status",
            "process::tests::run_status_does_not_inherit_stdin",
        )
        .await;
    }

    #[tokio::test]
    async fn run_output_does_not_inherit_stdin() {
        assert_stdin_isolated(
            "output",
            "process::tests::run_output_does_not_inherit_stdin",
        )
        .await;
    }

    #[tokio::test]
    async fn wait_captured_does_not_inherit_stdin() {
        assert_stdin_isolated(
            "captured",
            "process::tests::wait_captured_does_not_inherit_stdin",
        )
        .await;
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
    async fn a_tracked_command_settles_once_its_child_is_reaped() {
        let tracker = ChildTracker::default();
        let mut command = shell("exit 0");
        null_stdio(&mut command);
        let status = run_status_tracked(&mut command, Duration::from_secs(5), &tracker)
            .await
            .unwrap();
        assert!(status.success());
        assert!(tracker.is_settled());
    }

    #[tokio::test]
    async fn a_tracked_command_counts_from_its_abandonment_until_reaped() {
        let tracker = ChildTracker::default();
        let mut command = shell("sleep 30");
        null_stdio(&mut command);
        let mut run = Box::pin(run_status_tracked(
            &mut command,
            Duration::from_secs(30),
            &tracker,
        ));
        assert!(
            tokio::time::timeout(Duration::from_millis(200), &mut run)
                .await
                .is_err()
        );
        assert!(
            tracker.is_settled(),
            "a child its caller still waits for cannot outlive it, so it holds no release back"
        );
        drop(run);
        assert!(
            !tracker.is_settled(),
            "an abandoned child counts until it is reaped"
        );
        assert!(
            tracker.wait_settled(Duration::from_secs(5)).await,
            "the cancelled child must be killed and reaped instead of abandoned"
        );
    }

    #[tokio::test]
    async fn a_timed_out_tracked_command_settles_once_reaped() {
        let tracker = ChildTracker::default();
        let mut command = shell("sleep 30");
        null_stdio(&mut command);
        let error = run_status_tracked(&mut command, Duration::from_millis(20), &tracker)
            .await
            .unwrap_err();
        assert!(missed_deadline(&error));
        assert!(tracker.is_settled());
    }

    #[tokio::test]
    async fn a_lost_child_stays_unsettled_and_is_reported() {
        let tracker = ChildTracker::default();
        let lost = tracker.wait_lost();
        tokio::pin!(lost);
        assert!(futures::poll!(&mut lost).is_pending());
        tracker.begin();
        tracker.lose();
        assert!(!tracker.is_settled());
        assert!(matches!(
            futures::poll!(&mut lost),
            std::task::Poll::Ready(())
        ));
    }

    #[test]
    fn a_child_counts_from_abandonment_until_reaped_in_either_order() {
        for (reaped, abandoned_first, settled) in [
            (true, true, true),
            (true, false, true),
            (false, true, false),
            (false, false, false),
        ] {
            let tracker = ChildTracker::default();
            let run = TrackedRun::new(tracker.clone());
            if abandoned_first {
                run.abandon();
                assert!(!tracker.is_settled(), "an abandoned child counts");
                run.finish(reaped);
            } else {
                run.finish(reaped);
                run.abandon();
            }
            assert_eq!(
                tracker.is_settled(),
                settled,
                "reaped={reaped} abandoned_first={abandoned_first}"
            );
        }
    }

    #[test]
    fn a_child_still_waited_for_is_not_counted() {
        let tracker = ChildTracker::default();
        let run = TrackedRun::new(tracker.clone());
        assert!(tracker.is_settled());
        run.finish(true);
        assert!(tracker.is_settled());
    }

    #[test]
    fn an_unreaped_child_keeps_the_tracker_unsettled() {
        let tracker = ChildTracker::default();
        tracker.begin();
        assert!(!tracker.is_settled());
        tracker.settle();
        assert!(tracker.is_settled());
    }

    #[test]
    fn missed_deadline_recognizes_every_timeout_but_not_a_failed_command() {
        assert!(missed_deadline(&io::Error::new(
            io::ErrorKind::TimedOut,
            "child output drain timed out"
        )));
        for reap in [Some(Err(io::Error::other("no child"))), None] {
            let error = timed_out_error(Duration::from_millis(250), reap);
            assert_ne!(error.kind(), io::ErrorKind::TimedOut);
            assert!(missed_deadline(&deadline_missed(
                error.kind(),
                error.to_string()
            )));
        }
        assert!(!missed_deadline(&io::Error::other(
            "ip addr replace failed"
        )));
        assert!(!missed_deadline(&io::Error::new(
            io::ErrorKind::NotFound,
            "ip not found"
        )));
    }

    #[tokio::test]
    async fn timeout_diagnostics_identify_wait_and_reap_without_command_secrets() {
        let mut command = shell("sleep 30 # secret-argument-sentinel");
        command.env("SECRET_TEST_VALUE", "secret-environment-sentinel");
        null_stdio(&mut command);
        let error = run_status(&mut command, Duration::from_millis(20))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(missed_deadline(&error));
        let diagnostic = error.to_string();
        for field in [
            "pid=",
            "budget_ms=20",
            "wait_ms=",
            "overrun_ms=",
            "cleanup_ms=",
            "reap_ms=",
            "exit_code=",
            "signal=",
        ] {
            assert!(diagnostic.contains(field), "missing {field}: {diagnostic}");
        }
        assert!(!diagnostic.contains("secret-argument-sentinel"));
        assert!(!diagnostic.contains("secret-environment-sentinel"));
        assert!(!diagnostic.contains("sleep"));
    }

    async fn without_clock_advance<F: std::future::Future>(future: F) -> F::Output {
        let watchdog = std::time::Instant::now();
        tokio::pin!(future);
        // Process creation and SIGCHLD delivery use the OS, not Tokio's paused clock.
        loop {
            assert!(
                watchdog.elapsed() < Duration::from_secs(5),
                "paused-clock process fixture exceeded its wall-clock watchdog"
            );
            tokio::select! {
                biased;
                result = &mut future => return result,
                _ = tokio::task::yield_now() => {}
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn run_status_timeout_kills_and_reaps_the_child_and_descendant() {
        let parent_file = unique_pid_file("timeout-parent");
        let child_file = unique_pid_file("timeout-child");
        let mut command = shell(&pid_script(&parent_file, &child_file));
        null_stdio(&mut command);

        let started = tokio::time::Instant::now();
        let run = run_status(&mut command, Duration::from_millis(100));
        tokio::pin!(run);
        let (parent, child) = without_clock_advance(async {
            tokio::select! {
                result = &mut run => panic!("command finished before process readiness: {result:?}"),
                pids = async { (read_pid(&parent_file).await, read_pid(&child_file).await) } => pids,
            }
        })
        .await;
        assert_eq!(started.elapsed(), Duration::ZERO);
        tokio::time::advance(Duration::from_millis(99)).await;
        assert!(futures::poll!(&mut run).is_pending());
        assert!(process_exists(parent) && process_exists(child));
        tokio::time::advance(Duration::from_millis(1)).await;
        let result = without_clock_advance(run).await;
        assert_eq!(started.elapsed(), Duration::from_millis(100));
        tokio::time::resume();
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
