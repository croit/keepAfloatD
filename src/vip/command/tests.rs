use super::{CommandRunner, SystemCommandRunner, scripted::ScriptedRunner};
use futures::FutureExt;
use std::os::unix::process::ExitStatusExt;
use std::panic::AssertUnwindSafe;
use std::process::{ExitStatus, Output, Stdio};
use std::time::Duration;
use tokio::process::Command;

fn command(program: &str, args: &[&str]) -> Command {
    let mut command = Command::new(program);
    command.args(args);
    command
}

#[tokio::test]
async fn scripted_runner_rejects_wrong_program_arguments_timeout_and_mode() {
    for mismatch in 0..4 {
        let runner = ScriptedRunner::default();
        runner.expect_status(
            &command("ip", &["-j", "addr", "show"]),
            Duration::from_millis(250),
            Ok(ExitStatus::from_raw(0)),
        );
        let mut actual = if mismatch == 0 {
            command("other", &["-j", "addr", "show"])
        } else if mismatch == 1 {
            command("ip", &["addr", "show"])
        } else {
            command("ip", &["-j", "addr", "show"])
        };
        let timeout = Duration::from_millis(if mismatch == 2 { 251 } else { 250 });
        let result = AssertUnwindSafe(async {
            if mismatch == 3 {
                runner
                    .output(&mut actual, timeout)
                    .await
                    .map(|output| output.status)
            } else {
                runner.status(&mut actual, timeout).await
            }
        })
        .catch_unwind()
        .await;
        assert!(result.is_err());
    }
}

#[tokio::test]
async fn scripted_runner_matches_concurrent_commands_independently() {
    let runner = ScriptedRunner::default();
    let timeout = Duration::from_millis(250);
    let mut first = command("ip", &["-j", "addr", "show"]);
    let mut second = command("ip", &["-j", "-6", "route", "show"]);
    let (release, wait) = tokio::sync::oneshot::channel();
    runner.expect_output_future(&first, timeout, async move {
        wait.await.unwrap();
        Ok(Output {
            status: ExitStatus::from_raw(0),
            stdout: b"addresses".to_vec(),
            stderr: Vec::new(),
        })
    });
    runner.expect_output(
        &second,
        timeout,
        Ok(Output {
            status: ExitStatus::from_raw(0),
            stdout: b"routes".to_vec(),
            stderr: Vec::new(),
        }),
    );
    let (first, second) = tokio::join!(runner.output(&mut first, timeout), async {
        let output = runner.output(&mut second, timeout).await.unwrap();
        release.send(()).unwrap();
        output
    });
    assert_eq!(first.unwrap().stdout, b"addresses");
    assert_eq!(second.stdout, b"routes");
    runner.assert_finished();
    assert_eq!(runner.calls().len(), 2);
}

#[tokio::test]
async fn system_runner_delegates_status_output_and_spawn_errors() {
    let runner = SystemCommandRunner;
    let timeout = Duration::from_secs(1);
    let mut status = command("/bin/sh", &["-c", "exit 7"]);
    assert_eq!(
        runner.status(&mut status, timeout).await.unwrap().code(),
        Some(7)
    );
    let mut output = command("/bin/sh", &["-c", "printf out; printf err >&2"]);
    output.stdout(Stdio::piped()).stderr(Stdio::piped());
    let output = runner.output(&mut output, timeout).await.unwrap();
    assert_eq!(output.stdout, b"out");
    assert_eq!(output.stderr, b"err");
    let mut missing = Command::new("/no-such-keepafloatd-command");
    assert_eq!(
        runner
            .status(&mut missing, timeout)
            .await
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::NotFound
    );
    assert_eq!(
        runner
            .output(&mut missing, timeout)
            .await
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::NotFound
    );
}

#[tokio::test]
async fn system_runner_keeps_status_and_output_timeouts() {
    let runner = SystemCommandRunner;
    for capture in [false, true] {
        let mut command = command("/bin/sh", &["-c", "sleep 30"]);
        command.stdout(Stdio::piped()).stderr(Stdio::piped());
        let result = if capture {
            runner
                .output(&mut command, Duration::from_millis(20))
                .await
                .map(|output| output.status)
        } else {
            runner.status(&mut command, Duration::from_millis(20)).await
        };
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::TimedOut);
    }
}
