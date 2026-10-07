use super::LocalVip;
use super::effects::{
    IP_COMMAND_TIMEOUT, bind_command_arguments, delete_command, marker_route_delete_arguments,
    marker_route_probe_arguments, marker_route_replace_arguments, presence_probe_command,
};
use std::io;
use std::net::IpAddr;
use std::os::unix::process::ExitStatusExt;
use std::process::{ExitStatus, Output};
use tokio::process::Command;

pub(crate) type Target<'a> = (&'a str, IpAddr, u8);

fn command(args: impl IntoIterator<Item = impl AsRef<std::ffi::OsStr>>) -> Command {
    let mut command = Command::new("ip");
    command.args(args);
    command
}

fn empty_inventory() -> io::Result<Output> {
    Ok(Output {
        status: ExitStatus::from_raw(0),
        stdout: b"[]".to_vec(),
        stderr: Vec::new(),
    })
}

impl LocalVip {
    pub(crate) async fn force_next_bind_result(
        &self,
        target: Target<'_>,
        result: io::Result<ExitStatus>,
    ) {
        let needs_cleanup = result.as_ref().map_or(true, |status| !status.success());
        self.force_bind_results(target, vec![Ok(ExitStatus::from_raw(0)), result])
            .await;
        if needs_cleanup {
            self.force_next_bind_presence_result(
                target.1,
                Ok(Output {
                    status: ExitStatus::from_raw(0),
                    stdout: Vec::new(),
                    stderr: Vec::new(),
                }),
            )
            .await;
            self.commands.clear_where(|call| {
                call.arguments
                    .windows(2)
                    .any(|args| args == ["route", "del"])
            });
            self.force_marker_delete_results(target.1, vec![Ok(ExitStatus::from_raw(0))])
                .await;
        }
    }

    pub(super) async fn force_bind_results(
        &self,
        target: Target<'_>,
        results: Vec<io::Result<ExitStatus>>,
    ) {
        let mut results = results.into_iter();
        let marker = command(marker_route_replace_arguments(
            target.1,
            self.ownership_marker.protocol(),
        ));
        self.commands.clear(&marker, IP_COMMAND_TIMEOUT, false);
        self.commands.expect_status(
            &marker,
            IP_COMMAND_TIMEOUT,
            results.next().expect("marker result required"),
        );
        let address = command(bind_command_arguments(target.1, target.2, target.0));
        self.commands.clear(&address, IP_COMMAND_TIMEOUT, false);
        if let Some(result) = results.next() {
            self.commands
                .expect_status(&address, IP_COMMAND_TIMEOUT, result);
        }
        assert!(results.next().is_none());
    }

    pub(super) async fn force_unbind_results(
        &self,
        target: Target<'_>,
        results: Vec<io::Result<ExitStatus>>,
    ) {
        let command = delete_command(target.1, target.2, target.0);
        self.commands.clear(&command, IP_COMMAND_TIMEOUT, false);
        for result in results {
            self.commands
                .expect_status(&command, IP_COMMAND_TIMEOUT, result);
        }
    }

    pub(super) async fn force_next_bind_presence_result(
        &self,
        address: IpAddr,
        result: io::Result<Output>,
    ) {
        self.force_unbind_probe_results(address, vec![result]).await;
    }

    pub(super) async fn force_unbind_probe_results(
        &self,
        address: IpAddr,
        results: Vec<io::Result<Output>>,
    ) {
        let command = presence_probe_command(address, 32);
        self.commands.clear(&command, IP_COMMAND_TIMEOUT, true);
        for result in results {
            self.commands
                .expect_output(&command, IP_COMMAND_TIMEOUT, result);
        }
    }

    fn default_discovery(&self) {
        for args in [
            vec!["-N", "-j", "addr", "show"],
            vec!["-N", "-j", "-4", "route", "show", "table", "all"],
            vec!["-N", "-j", "-6", "route", "show", "table", "all"],
        ] {
            let command = command(args);
            if self.commands.remaining(&command, IP_COMMAND_TIMEOUT, true) == 0 {
                self.commands
                    .expect_output(&command, IP_COMMAND_TIMEOUT, empty_inventory());
            }
        }
    }

    pub(crate) async fn force_startup_discovery_result(&self, result: io::Result<Output>) {
        self.default_discovery();
        let command = command(["-N", "-j", "addr", "show"]);
        self.commands.clear(&command, IP_COMMAND_TIMEOUT, true);
        self.commands
            .expect_output(&command, IP_COMMAND_TIMEOUT, result);
    }

    pub(crate) async fn force_startup_marker_discovery_results(
        &self,
        ipv4: io::Result<Output>,
        ipv6: io::Result<Output>,
    ) {
        self.default_discovery();
        for (family, result) in [("-4", ipv4), ("-6", ipv6)] {
            let command = command(["-N", "-j", family, "route", "show", "table", "all"]);
            self.commands.clear(&command, IP_COMMAND_TIMEOUT, true);
            self.commands
                .expect_output(&command, IP_COMMAND_TIMEOUT, result);
        }
    }

    pub(crate) async fn force_next_startup_cleanup_results(
        &self,
        target: Target<'_>,
        delete: io::Result<ExitStatus>,
        probe: Option<io::Result<Output>>,
    ) {
        self.default_discovery();
        self.force_unbind_results(target, vec![delete]).await;
        if let Some(probe) = probe {
            self.force_unbind_probe_results(target.1, vec![probe]).await;
        }
    }

    pub(super) async fn force_marker_delete_results(
        &self,
        address: IpAddr,
        results: Vec<io::Result<ExitStatus>>,
    ) {
        let command = command(marker_route_delete_arguments(
            address,
            self.ownership_marker.protocol(),
        ));
        self.commands.clear(&command, IP_COMMAND_TIMEOUT, false);
        for result in results {
            self.commands
                .expect_status(&command, IP_COMMAND_TIMEOUT, result);
        }
    }

    pub(super) async fn force_marker_delete_probe_results(
        &self,
        address: IpAddr,
        results: Vec<io::Result<Output>>,
    ) {
        let command = command(marker_route_probe_arguments(address));
        for result in results {
            self.commands
                .expect_output(&command, IP_COMMAND_TIMEOUT, result);
        }
    }

    pub(super) async fn remaining_forced_unbind_results(&self) -> usize {
        self.commands.remaining_where(|call| {
            !call.capture
                && call
                    .arguments
                    .windows(2)
                    .any(|args| args == ["addr", "del"])
        })
    }

    pub(super) async fn remaining_marker_delete_results(&self) -> usize {
        self.commands.remaining_where(|call| {
            !call.capture
                && call
                    .arguments
                    .windows(2)
                    .any(|args| args == ["route", "del"])
        })
    }

    pub(super) async fn has_forced_startup_delete_result(&self) -> bool {
        self.remaining_forced_unbind_results().await > 0
    }

    pub(super) fn remaining_address_probes(&self, address: IpAddr) -> usize {
        self.commands.remaining(
            &presence_probe_command(address, 32),
            IP_COMMAND_TIMEOUT,
            true,
        )
    }

    pub(super) fn remaining_bind_results(&self, target: Target<'_>) -> usize {
        self.commands.remaining(
            &command(bind_command_arguments(target.1, target.2, target.0)),
            IP_COMMAND_TIMEOUT,
            false,
        )
    }

    pub(super) fn pause_bind_result(&self, target: Target<'_>) -> tokio::sync::oneshot::Sender<()> {
        self.commands.pause_next(
            &command(bind_command_arguments(target.1, target.2, target.0)),
            IP_COMMAND_TIMEOUT,
            false,
        )
    }

    pub(super) fn pause_address_probe(&self, address: IpAddr) -> tokio::sync::oneshot::Sender<()> {
        let command = presence_probe_command(address, 32);
        self.commands.clear(&command, IP_COMMAND_TIMEOUT, true);
        self.commands.expect_output(
            &command,
            IP_COMMAND_TIMEOUT,
            Err(io::Error::other("cancelled probe must not finish")),
        );
        self.commands.pause_next(&command, IP_COMMAND_TIMEOUT, true)
    }
}
