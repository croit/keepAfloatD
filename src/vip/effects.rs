//! Bounded Linux `ip` command construction and result verification.

use std::future::Future;
use std::net::IpAddr;
use tokio::process::Command;

pub(super) const IP_COMMAND_TIMEOUT: tokio::time::Duration =
    tokio::time::Duration::from_millis(250);
pub(super) const IP_OUTPUT_BUDGET: tokio::time::Duration = IP_COMMAND_TIMEOUT
    .saturating_add(crate::process::PROCESS_REAP_TIMEOUT)
    .saturating_add(crate::process::OUTPUT_DRAIN_TIMEOUT);
/// A delete followed by a failed-delete presence query, including child cleanup.
pub(super) const DELETE_BUDGET: tokio::time::Duration = IP_COMMAND_TIMEOUT
    .saturating_add(crate::process::PROCESS_REAP_TIMEOUT)
    .saturating_add(IP_OUTPUT_BUDGET);
pub(super) const VIP_MARKER_ROUTE_TABLE_BASE: u16 = 10_000;

pub(super) fn marker_route_table(address_protocol: u8) -> String {
    (VIP_MARKER_ROUTE_TABLE_BASE + u16::from(address_protocol)).to_string()
}

/// `ip` address-family selector for an address. The configured prefix defaults to a host route
/// but may be widened through the VIP CIDR suffix.
pub(super) const fn ip_family(ip: IpAddr) -> &'static str {
    match ip {
        IpAddr::V4(_) => "-4",
        IpAddr::V6(_) => "-6",
    }
}

/// The `ip` arguments that add one VIP to `interface`.
///
/// #34: an IPv6 VIP is also added `preferred_lft 0`, which marks it deprecated. Every global
/// address on an interface is a source-address candidate (RFC 6724), so a node holding a VIP can
/// source its own outbound connections from it, and a peer that authorizes by source address then
/// refuses the call (ceph-iscsi answers 403 and the gateway reads as UNKNOWN). A deprecated
/// address still serves clients; it just stops winning that choice.
///
/// IPv6 only, and the gate is required rather than stylistic: IPv4 accepts the flag and reports
/// the address `deprecated`, then ignores it when choosing a source, so setting it there would be
/// a flag `ip addr` confirms and nothing acts on.
///
/// This relies on deprecation being a preference and not a prohibition: with no non-deprecated
/// global address left the kernel sources from a deprecated one anyway, so the node's own address
/// must never carry the flag. It does not, because only VIPs pass through here. The same builder
/// serves the first bind and every reconcile-tick reassert, so the flag survives re-asserts.
pub(super) fn bind_command_arguments(ip: IpAddr, prefix: u8, interface: &str) -> Vec<String> {
    let mut arguments = vec![
        ip_family(ip).into(),
        "addr".into(),
        "replace".into(),
        format!("{ip}/{prefix}"),
        "dev".into(),
        interface.into(),
    ];
    if ip.is_ipv6() {
        // Ownership fencing handles duplicates; DAD would delay a ready-to-serve VIP.
        arguments.push("nodad".into());
        arguments.push("preferred_lft".into());
        arguments.push("0".into());
    }
    arguments
}

pub(super) fn delete_command(ip: IpAddr, prefix: u8, interface: &str) -> Command {
    let mut command = Command::new("ip");
    command.args([
        ip_family(ip),
        "addr",
        "del",
        &format!("{ip}/{prefix}"),
        "dev",
        interface,
    ]);
    command
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum DeleteOutcome {
    Deleted,
    AlreadyAbsent,
}

pub(super) async fn verify_delete_result(
    status: std::process::ExitStatus,
    probe: impl Future<Output = std::io::Result<std::process::Output>>,
) -> anyhow::Result<DeleteOutcome> {
    if status.success() {
        return Ok(DeleteOutcome::Deleted);
    }
    ensure_failed_delete_is_absent(status, probe.await)?;
    Ok(DeleteOutcome::AlreadyAbsent)
}

fn marker_route_arguments(operation: &str, ip: IpAddr, address_protocol: u8) -> [String; 9] {
    let host_prefix = if ip.is_ipv4() { 32 } else { 128 };
    [
        ip_family(ip).into(),
        "route".into(),
        operation.into(),
        "table".into(),
        marker_route_table(address_protocol),
        "throw".into(),
        format!("{ip}/{host_prefix}"),
        "proto".into(),
        address_protocol.to_string(),
    ]
}

pub(super) fn marker_route_replace_arguments(ip: IpAddr, address_protocol: u8) -> [String; 9] {
    marker_route_arguments("replace", ip, address_protocol)
}

pub(super) fn marker_route_delete_arguments(ip: IpAddr, address_protocol: u8) -> [String; 9] {
    marker_route_arguments("del", ip, address_protocol)
}

pub(super) fn marker_route_probe_arguments(ip: IpAddr) -> [String; 7] {
    [
        "-N".into(),
        "-j".into(),
        ip_family(ip).into(),
        "route".into(),
        "show".into(),
        "table".into(),
        "all".into(),
    ]
}

pub(super) fn ensure_failed_delete_is_absent(
    delete_status: std::process::ExitStatus,
    probe: std::io::Result<std::process::Output>,
) -> anyhow::Result<()> {
    let probe = probe.map_err(|error| {
        anyhow::anyhow!(
            "ip addr del failed ({delete_status}) and presence verification could not run: {error}"
        )
    })?;
    anyhow::ensure!(
        probe.status.success(),
        "ip addr del failed ({delete_status}) and presence verification failed ({})",
        probe.status
    );
    anyhow::ensure!(
        probe.stdout.is_empty(),
        "ip addr del failed ({delete_status}) and the address is still present"
    );
    Ok(())
}

pub(super) fn presence_probe_command(ip: IpAddr, configured_prefix: u8) -> Command {
    let probe_target = presence_probe_target(ip, configured_prefix);
    let mut command = Command::new("ip");
    // A device may disappear or be renamed while its VIP must still be proven absent.
    command
        .args([ip_family(ip), "-o", "addr", "show", "to", &probe_target])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true);
    command
}

/// `ip addr show to` interprets a CIDR as a subnet filter. Presence verification must therefore
/// use the bare host address even when the configured bind/delete prefix is wider (#26).
pub(super) fn presence_probe_target(ip: IpAddr, _configured_prefix: u8) -> String {
    ip.to_string()
}

#[cfg(test)]
mod tests {
    use super::{
        DeleteOutcome, VIP_MARKER_ROUTE_TABLE_BASE, bind_command_arguments, delete_command,
        marker_route_delete_arguments, marker_route_probe_arguments,
        marker_route_replace_arguments, presence_probe_command, verify_delete_result,
    };
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
    use std::os::unix::process::ExitStatusExt;
    use std::process::{ExitStatus, Output};

    #[test]
    fn delete_command_preserves_family_prefix_and_interface() {
        for (address, prefix, family) in [("192.0.2.30", 24, "-4"), ("2001:db8::30", 64, "-6")] {
            let command = delete_command(address.parse().unwrap(), prefix, "test0.200");
            assert_eq!(command.as_std().get_program(), "ip");
            let arguments: Vec<_> = command.as_std().get_args().collect();
            assert_eq!(
                arguments,
                [
                    family,
                    "addr",
                    "del",
                    &format!("{address}/{prefix}"),
                    "dev",
                    "test0.200"
                ]
            );
        }
    }

    #[tokio::test]
    async fn delete_result_success_does_not_poll_the_probe() {
        let outcome = verify_delete_result(ExitStatus::from_raw(0), async {
            panic!("successful deletion must not poll the probe");
        })
        .await
        .unwrap();
        assert_eq!(outcome, DeleteOutcome::Deleted);
    }

    #[tokio::test]
    async fn delete_result_distinguishes_verified_absence() {
        let outcome = verify_delete_result(ExitStatus::from_raw(2 << 8), async {
            Ok(Output {
                status: ExitStatus::from_raw(0),
                stdout: Vec::new(),
                stderr: Vec::new(),
            })
        })
        .await
        .unwrap();
        assert_eq!(outcome, DeleteOutcome::AlreadyAbsent);
    }

    #[test]
    fn ipv4_presence_probe_checks_the_exact_host_without_requiring_an_interface() {
        let command = presence_probe_command("192.0.2.30".parse().unwrap(), 24);
        let arguments: Vec<_> = command.as_std().get_args().collect();
        assert_eq!(arguments, ["-4", "-o", "addr", "show", "to", "192.0.2.30"]);
    }

    #[test]
    fn ipv6_presence_probe_checks_the_exact_host_without_requiring_an_interface() {
        let command = presence_probe_command("2001:db8::30".parse().unwrap(), 64);
        let arguments: Vec<_> = command.as_std().get_args().collect();
        assert_eq!(
            arguments,
            ["-6", "-o", "addr", "show", "to", "2001:db8::30"]
        );
    }

    /// #34: the IPv4 arguments are exactly what they were before the IPv6 flag existed, so
    /// the v6 flag is an addition and not a change to the family that already worked.
    #[test]
    fn ipv4_bind_args_are_unchanged() {
        assert_eq!(
            bind_command_arguments("192.0.2.30".parse().unwrap(), 24, "eth0.200"),
            vec!["-4", "addr", "replace", "192.0.2.30/24", "dev", "eth0.200"]
        );
    }

    /// #34: an IPv6 VIP is deprecated, so the node stops sourcing its own connections from
    /// it and a peer that authorizes by source address accepts the call.
    #[test]
    fn ipv6_bind_args_deprecate_the_vip() {
        assert_eq!(
            bind_command_arguments(
                IpAddr::V6(Ipv6Addr::new(0xfd00, 0x5290, 0, 0, 0, 0, 0, 0x100)),
                128,
                "eth0"
            ),
            vec![
                "-6",
                "addr",
                "replace",
                "fd00:5290::100/128",
                "dev",
                "eth0",
                "nodad",
                "preferred_lft",
                "0"
            ]
        );
    }

    #[test]
    fn marker_routes_are_host_routes_in_the_inert_table() {
        assert_eq!(VIP_MARKER_ROUTE_TABLE_BASE, 10_000);
        assert_eq!(
            marker_route_replace_arguments(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 30)), 245,),
            [
                "-4",
                "route",
                "replace",
                "table",
                "10245",
                "throw",
                "192.0.2.30/32",
                "proto",
                "245",
            ]
        );
        assert_eq!(
            marker_route_delete_arguments(IpAddr::V6(Ipv6Addr::LOCALHOST), 246,),
            [
                "-6", "route", "del", "table", "10246", "throw", "::1/128", "proto", "246",
            ]
        );
        assert_eq!(
            marker_route_probe_arguments(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            ["-N", "-j", "-4", "route", "show", "table", "all"]
        );
    }
}
