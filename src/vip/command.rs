//! Instance-local execution boundary for VIP address and ownership commands.

use futures::future::BoxFuture;
use std::io;
use std::process::{ExitStatus, Output};
use std::time::Duration;
use tokio::process::Command;

pub(super) trait CommandRunner: Send + Sync {
    fn status<'a>(
        &'a self,
        command: &'a mut Command,
        timeout: Duration,
    ) -> BoxFuture<'a, io::Result<ExitStatus>>;

    fn output<'a>(
        &'a self,
        command: &'a mut Command,
        timeout: Duration,
    ) -> BoxFuture<'a, io::Result<Output>>;
}

pub(super) struct SystemCommandRunner {
    /// Every `status` command changes addresses or markers, so each is tracked until reaped.
    pub(super) children: crate::process::ChildTracker,
}

impl CommandRunner for SystemCommandRunner {
    fn status<'a>(
        &'a self,
        command: &'a mut Command,
        timeout: Duration,
    ) -> BoxFuture<'a, io::Result<ExitStatus>> {
        Box::pin(crate::process::run_status_tracked(
            command,
            timeout,
            &self.children,
        ))
    }

    fn output<'a>(
        &'a self,
        command: &'a mut Command,
        timeout: Duration,
    ) -> BoxFuture<'a, io::Result<Output>> {
        Box::pin(crate::process::run_output(command, timeout))
    }
}

#[cfg(test)]
pub(super) mod scripted;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod callers_tests;
