use crate::config::VipAddr;
use crate::vip::LocalVip;
use std::net::IpAddr;
use std::os::unix::process::ExitStatusExt;
use std::process::{ExitStatus, Output};
use std::time::Duration;
use tokio::process::Command;

const BUDGET: Duration = Duration::from_millis(250);

fn ip(arguments: &str) -> Command {
    let mut command = Command::new("ip");
    command.args(arguments.split_whitespace());
    command
}

fn output(stdout: &[u8]) -> std::io::Result<Output> {
    Ok(Output {
        status: ExitStatus::from_raw(0),
        stdout: stdout.to_vec(),
        stderr: Vec::new(),
    })
}

#[tokio::test]
async fn bind_reassert_and_unbind_use_exact_commands_and_budgets() {
    for (address, prefix, family, suffix, host_prefix) in [
        ("192.0.2.99", 24, "-4", "", 32),
        ("2001:db8::99", 64, "-6", " nodad preferred_lft 0", 128),
    ] {
        let local = LocalVip::new_with_address_protocol(false, 245);
        let marker =
            format!("{family} route replace table 10245 throw {address}/{host_prefix} proto 245");
        let bind = format!("{family} addr replace {address}/{prefix} dev test0.200{suffix}");
        for _ in 0..2 {
            local
                .commands
                .expect_status(&ip(&marker), BUDGET, Ok(ExitStatus::from_raw(0)));
            local
                .commands
                .expect_status(&ip(&bind), BUDGET, Ok(ExitStatus::from_raw(0)));
        }
        let delete = format!("{family} addr del {address}/{prefix} dev test0.200");
        let presence = format!("{family} -o addr show to {address}");
        let marker_delete =
            format!("{family} route del table 10245 throw {address}/{host_prefix} proto 245");
        let marker_probe = format!("-N -j {family} route show table all");
        local
            .commands
            .expect_status(&ip(&delete), BUDGET, Ok(ExitStatus::from_raw(2 << 8)));
        local
            .commands
            .expect_output(&ip(&presence), BUDGET, output(b""));
        local.commands.expect_status(
            &ip(&marker_delete),
            BUDGET,
            Ok(ExitStatus::from_raw(2 << 8)),
        );
        local
            .commands
            .expect_output(&ip(&marker_probe), BUDGET, output(b"[]"));
        let (finish, announcement) = tokio::sync::oneshot::channel();
        *local.next_announcement.lock().await = Some(announcement);
        let address: IpAddr = address.parse().unwrap();
        local.bind("test0.200", address, prefix).await.unwrap();
        local.bind("test0.200", address, prefix).await.unwrap();
        local.unbind("test0.200", address, prefix).await.unwrap();
        drop(finish);
        assert!(local.bound_addrs().await.is_empty());
        local.commands.assert_finished();
        let calls: Vec<_> = local
            .commands
            .calls()
            .into_iter()
            .map(|call| {
                call.arguments
                    .iter()
                    .map(|arg| arg.to_str().unwrap())
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .collect();
        assert_eq!(
            calls,
            [
                marker.clone(),
                bind.clone(),
                marker,
                bind,
                delete,
                presence,
                marker_delete,
                marker_probe
            ]
        );
    }
}

#[tokio::test]
async fn startup_runs_concurrent_inventory_before_ordered_cleanup() {
    let local = LocalVip::new_with_address_protocol(false, 245);
    let (release, wait) = tokio::sync::oneshot::channel::<()>();
    local.commands.expect_output_future(&ip("-N -j addr show"), BUDGET, async move {
        let _ = wait.await;
        output(br#"[{"ifname":"test0","addr_info":[{"family":"inet","local":"192.0.2.99","prefixlen":24}]}]"#)
    });
    local.commands.expect_output(
        &ip("-N -j -4 route show table all"),
        BUDGET,
        output(br#"[{"type":"throw","dst":"192.0.2.99","protocol":245,"table":10245}]"#),
    );
    local
        .commands
        .expect_output(&ip("-N -j -6 route show table all"), BUDGET, output(b"[]"));
    for command in [
        "-4 addr del 192.0.2.98/32 dev test0",
        "-4 addr del 192.0.2.99/24 dev test0",
        "-4 route del table 10245 throw 192.0.2.99/32 proto 245",
    ] {
        local
            .commands
            .expect_status(&ip(command), BUDGET, Ok(ExitStatus::from_raw(0)));
    }
    let table = [(VipAddr::host("192.0.2.98".parse().unwrap()), "test0".into())];
    let mut cleanup = Box::pin(local.startup_cleanup(&table));
    assert!(futures::poll!(&mut cleanup).is_pending());
    let calls = local.commands.calls();
    assert_eq!(
        calls.len(),
        3,
        "all inventories must start before waiting on the first"
    );
    assert!(calls.iter().all(|call| call.capture));
    drop(release);
    cleanup.await.unwrap();
    local.commands.assert_finished();
    let calls = local.commands.calls();
    assert_eq!(calls.len(), 6);
    assert_eq!(
        calls[5].arguments,
        ip("-4 route del table 10245 throw 192.0.2.99/32 proto 245")
            .as_std()
            .get_args()
            .map(std::ffi::OsString::from)
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn runner_expectations_are_instance_local() {
    let first = LocalVip::new(false);
    let second = LocalVip::new(false);
    let address = "192.0.2.99".parse().unwrap();
    for (local, success) in [(&first, true), (&second, false)] {
        local.bound.write().await.insert(address);
        local.commands.expect_status(
            &ip("-4 addr del 192.0.2.99/32 dev test0"),
            BUDGET,
            if success {
                Ok(ExitStatus::from_raw(0))
            } else {
                Err(std::io::Error::other("delete denied"))
            },
        );
    }
    first.commands.expect_status(
        &ip("-4 route del table 10246 throw 192.0.2.99/32 proto 246"),
        BUDGET,
        Ok(ExitStatus::from_raw(0)),
    );
    let (first_result, second_result) = tokio::join!(
        first.unbind("test0", address, 32),
        second.unbind("test0", address, 32)
    );
    first_result.unwrap();
    assert!(second_result.is_err());
    assert!(first.bound_addrs().await.is_empty());
    assert_eq!(second.bound_addrs().await, [address]);
    first.commands.assert_finished();
    second.commands.assert_finished();
}
