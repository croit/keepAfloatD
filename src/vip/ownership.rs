//! Diskless kernel ownership markers for crash-time VIP discovery.

use super::command::CommandRunner;
use crate::config::VipAddr;
use anyhow::Context;
use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeMap;
use std::net::IpAddr;
use std::process::Output;
use std::sync::Arc;
use tokio::process::Command;

use super::effects::{IP_COMMAND_TIMEOUT, presence_probe_command};
use super::effects::{
    VIP_MARKER_ROUTE_TABLE_BASE, marker_route_delete_arguments, marker_route_probe_arguments,
    marker_route_replace_arguments, marker_route_table,
};

pub(super) struct OwnershipMarker {
    protocol: u8,
    runner: Arc<dyn CommandRunner>,
}

impl OwnershipMarker {
    pub(super) fn new(protocol: u8, runner: Arc<dyn CommandRunner>) -> Self {
        Self { protocol, runner }
    }

    pub(super) const fn protocol(&self) -> u8 {
        self.protocol
    }

    pub(super) async fn replace(&self, address: IpAddr) -> anyhow::Result<()> {
        let mut command = Command::new("ip");
        command
            .args(marker_route_replace_arguments(address, self.protocol))
            .kill_on_drop(true);
        let status = self
            .runner
            .status(&mut command, IP_COMMAND_TIMEOUT)
            .await
            .context("VIP operation=marker_replace")?;
        anyhow::ensure!(status.success(), "ip route marker replace failed: {status}");
        Ok(())
    }

    pub(super) async fn delete(&self, address: IpAddr) -> anyhow::Result<()> {
        let mut command = Command::new("ip");
        command
            .args(marker_route_delete_arguments(address, self.protocol))
            .kill_on_drop(true);
        let status = self.runner.status(&mut command, IP_COMMAND_TIMEOUT).await?;
        if status.success() {
            return Ok(());
        }
        let mut probe_command = Command::new("ip");
        probe_command
            .args(marker_route_probe_arguments(address))
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        let probe = self
            .runner
            .output(&mut probe_command, IP_COMMAND_TIMEOUT)
            .await?;
        let absent = failed_marker_delete_is_absent(probe, address, self.protocol).map_err(|error| {
            anyhow::anyhow!(
                "ip route marker del failed ({status}) and presence verification failed: {error}"
            )
        })?;
        anyhow::ensure!(
            absent,
            "ip route marker del failed ({status}) and the marker is still present"
        );
        Ok(())
    }

    pub(super) async fn remove_after_failed_first_bind(
        &self,
        address: IpAddr,
        prefix: u8,
    ) -> anyhow::Result<()> {
        let mut probe_command = presence_probe_command(address, prefix);
        let probe = self
            .runner
            .output(&mut probe_command, IP_COMMAND_TIMEOUT)
            .await;
        let address_absent = matches!(
            probe,
            Ok(ref output) if output.status.success() && output.stdout.is_empty()
        );
        anyhow::ensure!(
            address_absent,
            "address absence could not be proven, ownership marker retained"
        );
        self.delete(address).await.map_err(|error| {
            anyhow::anyhow!("address is absent but ownership marker cleanup failed: {error}")
        })
    }
}

#[derive(Deserialize)]
struct InterfaceAddresses {
    ifname: String,
    addr_info: Vec<AddressInfo>,
}

#[derive(Deserialize)]
struct AddressInfo {
    family: Option<String>,
    local: Option<String>,
    prefixlen: Option<u8>,
    protocol: Option<Value>,
}

#[derive(Deserialize)]
struct MarkerRouteInfo {
    dst: Option<String>,
    table: Option<Value>,
    protocol: Option<Value>,
    #[serde(rename = "type")]
    route_type: Option<Value>,
}

#[derive(Debug, PartialEq)]
pub(super) struct OwnershipDiscovery {
    pub(super) addresses: Vec<(VipAddr, String)>,
    pub(super) marker_routes: Vec<IpAddr>,
}

type AddressInventory = BTreeMap<(IpAddr, String), (VipAddr, String)>;

#[cfg(test)]
pub(super) fn parse_owned_addresses(
    json: &[u8],
    address_protocol: u8,
) -> anyhow::Result<Vec<(VipAddr, String)>> {
    let (_, owned) = parse_address_inventory(json, address_protocol)?;
    Ok(owned.into_values().collect())
}

fn parse_address_inventory(
    json: &[u8],
    address_protocol: u8,
) -> anyhow::Result<(AddressInventory, AddressInventory)> {
    let interfaces: Vec<InterfaceAddresses> = serde_json::from_slice(json)
        .map_err(|error| anyhow::anyhow!("parse `ip -j addr show`: {error}"))?;
    let mut addresses = BTreeMap::new();
    let mut owned = BTreeMap::new();
    for interface in interfaces {
        anyhow::ensure!(
            !interface.ifname.is_empty(),
            "kernel address has an empty interface name"
        );
        for info in interface.addr_info {
            let marked = is_keepafloatd_protocol(info.protocol.as_ref(), address_protocol)?;
            let (Some(local), Some(family), Some(prefixlen)) =
                (info.local, info.family, info.prefixlen)
            else {
                anyhow::ensure!(!marked, "marked address is missing identity fields");
                continue;
            };
            let address: IpAddr = local.parse().map_err(|error| {
                anyhow::anyhow!("kernel address {local} is not a valid IP: {error}")
            })?;
            let family_matches = matches!(
                (family.as_str(), address),
                ("inet", IpAddr::V4(_)) | ("inet6", IpAddr::V6(_))
            );
            anyhow::ensure!(
                family_matches,
                "kernel address {} has inconsistent family {}",
                local,
                family
            );
            let max_prefix = if address.is_ipv4() { 32 } else { 128 };
            anyhow::ensure!(
                prefixlen <= max_prefix,
                "kernel address {}/{} has an invalid prefix",
                local,
                prefixlen
            );
            let target = (
                VipAddr {
                    addr: address,
                    prefix: prefixlen,
                },
                interface.ifname.clone(),
            );
            let key = (address, interface.ifname.clone());
            addresses.insert(key.clone(), target.clone());
            if marked {
                owned.insert(key, target);
            }
        }
    }
    Ok((addresses, owned))
}

pub(super) fn parse_marker_routes(
    json: &[u8],
    address_protocol: u8,
    ipv6: bool,
) -> anyhow::Result<Vec<IpAddr>> {
    let routes: Vec<MarkerRouteInfo> = serde_json::from_slice(json)
        .map_err(|error| anyhow::anyhow!("parse ownership marker routes: {error}"))?;
    let mut marked = BTreeMap::new();
    let expected_table =
        u64::from(VIP_MARKER_ROUTE_TABLE_BASE).saturating_add(u64::from(address_protocol));
    for route in routes {
        if numeric_kernel_value(route.table.as_ref(), "route table")? != Some(expected_table) {
            continue;
        }
        if !is_keepafloatd_protocol(route.protocol.as_ref(), address_protocol)? {
            continue;
        }
        let destination = route
            .dst
            .ok_or_else(|| anyhow::anyhow!("marked route is missing its destination"))?;
        anyhow::ensure!(
            is_throw_route(route.route_type.as_ref()),
            "marked route {destination} is not an inert throw route"
        );
        let (address_text, prefix) = destination
            .split_once('/')
            .map_or((destination.as_str(), None), |(address, prefix)| {
                (address, Some(prefix))
            });
        let address: IpAddr = address_text.parse().map_err(|error| {
            anyhow::anyhow!("marked route {destination} is not a valid IP: {error}")
        })?;
        anyhow::ensure!(
            address.is_ipv6() == ipv6,
            "marked route {destination} has the wrong address family"
        );
        if let Some(prefix) = prefix {
            let prefix: u8 = prefix.parse().map_err(|error| {
                anyhow::anyhow!("marked route {destination} has an invalid prefix: {error}")
            })?;
            let expected = if ipv6 { 128 } else { 32 };
            anyhow::ensure!(
                prefix == expected,
                "marked route {destination} is not a host route"
            );
        }
        marked.insert(address, address);
    }
    Ok(marked.into_values().collect())
}

pub(super) fn parse_ownership_discovery(
    address_json: &[u8],
    ipv4_route_json: &[u8],
    ipv6_route_json: &[u8],
    address_protocol: u8,
) -> anyhow::Result<OwnershipDiscovery> {
    let (addresses, mut owned) = parse_address_inventory(address_json, address_protocol)?;
    let mut marker_routes = parse_marker_routes(ipv4_route_json, address_protocol, false)?;
    marker_routes.extend(parse_marker_routes(
        ipv6_route_json,
        address_protocol,
        true,
    )?);
    marker_routes.sort_unstable();
    marker_routes.dedup();
    for address in &marker_routes {
        let mut matches = addresses
            .iter()
            .filter(|((candidate, _), _)| candidate == address);
        if let Some((key, target)) = matches.next() {
            anyhow::ensure!(
                matches.next().is_none(),
                "marked address {address} is attached to multiple interfaces"
            );
            owned.insert(key.clone(), target.clone());
        }
    }
    Ok(OwnershipDiscovery {
        addresses: owned.into_values().collect(),
        marker_routes,
    })
}

#[cfg(test)]
pub(super) fn parse_discovery_output(
    output: Output,
    address_protocol: u8,
) -> anyhow::Result<Vec<(VipAddr, String)>> {
    anyhow::ensure!(
        output.status.success(),
        "`ip -j addr show` failed ({}): {}",
        output.status,
        String::from_utf8_lossy(&output.stderr).trim()
    );
    parse_owned_addresses(&output.stdout, address_protocol)
}

pub(super) fn parse_discovery_outputs(
    addresses: Output,
    ipv4_routes: Output,
    ipv6_routes: Output,
    address_protocol: u8,
) -> anyhow::Result<OwnershipDiscovery> {
    let marker_table = marker_route_table(address_protocol);
    ensure_discovery_succeeded(&addresses, "ip -j addr show")?;
    ensure_discovery_succeeded(
        &ipv4_routes,
        &format!("ip -j -4 route show table {marker_table}"),
    )?;
    ensure_discovery_succeeded(
        &ipv6_routes,
        &format!("ip -j -6 route show table {marker_table}"),
    )?;
    parse_ownership_discovery(
        &addresses.stdout,
        &ipv4_routes.stdout,
        &ipv6_routes.stdout,
        address_protocol,
    )
}

pub(super) fn failed_marker_delete_is_absent(
    output: Output,
    address: IpAddr,
    address_protocol: u8,
) -> anyhow::Result<bool> {
    ensure_discovery_succeeded(
        &output,
        &format!(
            "ip -j route show table {}",
            marker_route_table(address_protocol)
        ),
    )?;
    let markers = parse_marker_routes(&output.stdout, address_protocol, address.is_ipv6())?;
    Ok(!markers.contains(&address))
}

fn ensure_discovery_succeeded(output: &Output, operation: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        output.status.success(),
        "`{operation}` failed ({}): {}",
        output.status,
        String::from_utf8_lossy(&output.stderr).trim()
    );
    Ok(())
}

fn discovery_command_arguments() -> [String; 4] {
    ["-N".into(), "-j".into(), "addr".into(), "show".into()]
}

fn marker_discovery_command_arguments(ipv6: bool) -> [String; 7] {
    [
        "-N".into(),
        "-j".into(),
        if ipv6 { "-6" } else { "-4" }.into(),
        "route".into(),
        "show".into(),
        "table".into(),
        "all".into(),
    ]
}

pub(super) async fn discover_owned_addresses(
    runner: &dyn CommandRunner,
    address_protocol: u8,
) -> anyhow::Result<OwnershipDiscovery> {
    let address_arguments = discovery_command_arguments();
    let ipv4_arguments = marker_discovery_command_arguments(false);
    let ipv6_arguments = marker_discovery_command_arguments(true);
    let (addresses, ipv4_routes, ipv6_routes) = tokio::try_join!(
        run_discovery_command(runner, &address_arguments, "addresses"),
        run_discovery_command(runner, &ipv4_arguments, "IPv4 marker routes"),
        run_discovery_command(runner, &ipv6_arguments, "IPv6 marker routes"),
    )?;
    parse_discovery_outputs(addresses, ipv4_routes, ipv6_routes, address_protocol)
}

async fn run_discovery_command(
    runner: &dyn CommandRunner,
    arguments: &[String],
    subject: &str,
) -> anyhow::Result<Output> {
    let mut command = Command::new("ip");
    command
        .args(arguments)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    runner
        .output(&mut command, IP_COMMAND_TIMEOUT)
        .await
        .map_err(|error| anyhow::anyhow!("discover keepafloatd-owned {subject}: {error}"))
}

fn is_keepafloatd_protocol(protocol: Option<&Value>, address_protocol: u8) -> anyhow::Result<bool> {
    let Some(protocol) = protocol else {
        return Ok(false);
    };
    match protocol {
        Value::Number(number) => {
            let value = number
                .as_u64()
                .filter(|value| *value <= u64::from(u8::MAX))
                .ok_or_else(|| anyhow::anyhow!("kernel address protocol is not a u8: {number}"))?;
            Ok(value == u64::from(address_protocol))
        }
        Value::String(value) => {
            let parsed = value
                .strip_prefix("0x")
                .and_then(|hex| u8::from_str_radix(hex, 16).ok())
                .or_else(|| value.parse::<u8>().ok());
            Ok(parsed == Some(address_protocol))
        }
        _ => anyhow::bail!("kernel address protocol has an unsupported JSON type: {protocol}"),
    }
}

fn numeric_kernel_value(value: Option<&Value>, subject: &str) -> anyhow::Result<Option<u64>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let parsed = match value {
        Value::Number(number) => number.as_u64(),
        Value::String(value) => value
            .strip_prefix("0x")
            .and_then(|hex| u64::from_str_radix(hex, 16).ok())
            .or_else(|| value.parse::<u64>().ok()),
        _ => anyhow::bail!("kernel {subject} has an unsupported JSON type: {value}"),
    };
    parsed
        .map(Some)
        .ok_or_else(|| anyhow::anyhow!("kernel {subject} is not an unsigned integer: {value}"))
}

fn is_throw_route(route_type: Option<&Value>) -> bool {
    matches!(route_type, Some(Value::Number(value)) if value.as_u64() == Some(9))
        || matches!(route_type, Some(Value::String(value)) if value == "throw" || value == "9")
}

pub(super) fn merge_cleanup_targets(
    configured: &[(VipAddr, String)],
    discovered: Vec<(VipAddr, String)>,
) -> Vec<(VipAddr, String)> {
    let mut targets = BTreeMap::new();
    for (vip, interface) in configured.iter().cloned() {
        targets.insert((vip.addr, interface.clone()), (vip, interface));
    }
    // Kernel discovery is authoritative for the prefix currently attached to an interface. If
    // YAML changed the prefix while the daemon was down, trying the new prefix first would make
    // `ip addr del` fail and startup would abort before reaching the actual crash orphan.
    for (vip, interface) in discovered {
        targets.insert((vip.addr, interface.clone()), (vip, interface));
    }
    targets.into_values().collect()
}

#[cfg(test)]
mod tests {
    use super::{
        discovery_command_arguments, failed_marker_delete_is_absent,
        marker_discovery_command_arguments, merge_cleanup_targets, parse_discovery_output,
        parse_discovery_outputs, parse_marker_routes, parse_owned_addresses,
        parse_ownership_discovery,
    };
    use crate::config::{DEFAULT_VIP_ADDRESS_PROTOCOL, VipAddr};
    use std::net::IpAddr;
    use std::os::unix::process::ExitStatusExt;
    use std::process::{ExitStatus, Output};

    #[test]
    fn discovery_forces_numeric_protocol_json() {
        assert_eq!(discovery_command_arguments(), ["-N", "-j", "addr", "show"]);
        assert_eq!(
            marker_discovery_command_arguments(false),
            ["-N", "-j", "-4", "route", "show", "table", "all"]
        );
        assert_eq!(
            marker_discovery_command_arguments(true),
            ["-N", "-j", "-6", "route", "show", "table", "all"]
        );
    }

    #[test]
    fn route_marker_parser_accepts_numeric_protocols_for_both_families() {
        let ipv4 = br#"[
          {"type":"9","dst":"192.0.2.30","table":"10245","protocol":"245"},
          {"type":"9","dst":"192.0.2.31","table":"10246","protocol":"246"},
          {"type":"9","dst":"192.0.2.32","table":"10246","protocol":"245"}
        ]"#;
        let ipv6 = br#"[
          {"type":"throw","dst":"2001:db8::30","table":10245,"protocol":245},
          {"type":"throw","dst":"2001:db8::31","table":10246,"protocol":246}
        ]"#;

        assert_eq!(
            parse_marker_routes(ipv4, 245, false).unwrap(),
            vec!["192.0.2.30".parse::<IpAddr>().unwrap()]
        );
        assert_eq!(
            parse_marker_routes(ipv6, 245, true).unwrap(),
            vec!["2001:db8::30".parse::<IpAddr>().unwrap()]
        );
    }

    #[test]
    fn route_markers_select_addresses_and_retain_marker_only_crash_state() {
        let addresses = br#"[
          {"ifname":"eth0","addr_info":[
            {"family":"inet","local":"192.0.2.30","prefixlen":24},
            {"family":"inet","local":"192.0.2.31","prefixlen":24}
          ]},
          {"ifname":"eth0.200","addr_info":[
            {"family":"inet6","local":"2001:db8::30","prefixlen":64}
          ]}
        ]"#;
        let ipv4_routes = br#"[
          {"type":"9","dst":"192.0.2.30","table":"10245","protocol":"245"},
          {"type":"9","dst":"192.0.2.31","table":"10246","protocol":"246"},
          {"type":"9","dst":"192.0.2.99","table":"10245","protocol":245}
        ]"#;
        let ipv6_routes = br#"[
          {"type":"9","dst":"2001:db8::30","table":"10245","protocol":"0xf5"}
        ]"#;

        let discovered =
            parse_ownership_discovery(addresses, ipv4_routes, ipv6_routes, 245).unwrap();

        assert_eq!(
            discovered.addresses,
            vec![
                ("192.0.2.30/24".parse::<VipAddr>().unwrap(), "eth0".into()),
                (
                    "2001:db8::30/64".parse::<VipAddr>().unwrap(),
                    "eth0.200".into()
                ),
            ]
        );
        assert_eq!(
            discovered.marker_routes,
            vec![
                "192.0.2.30".parse::<IpAddr>().unwrap(),
                "192.0.2.99".parse().unwrap(),
                "2001:db8::30".parse().unwrap(),
            ]
        );
    }

    #[test]
    fn parser_selects_only_marked_ipv4_and_ipv6_addresses() {
        let json = br#"[
          {"ifname":"eth0","addr_info":[
            {"family":"inet","local":"192.0.2.10","prefixlen":24,"protocol":"0xf6"},
            {"family":"inet","local":"192.0.2.11","prefixlen":24},
            {"family":"inet","local":"192.0.2.12","prefixlen":24,"protocol":18},
            {"family":"inet","local":"192.0.2.13","prefixlen":24,"protocol":245}
          ]},
          {"ifname":"eth0.200","addr_info":[
            {"family":"inet6","local":"2001:db8::10","prefixlen":64,"protocol":246},
            {"family":"inet6","local":"2001:db8::11","prefixlen":64,"protocol":"kernel_lo"}
          ]}
        ]"#;

        assert_eq!(DEFAULT_VIP_ADDRESS_PROTOCOL, 246);
        assert_eq!(
            parse_owned_addresses(json, DEFAULT_VIP_ADDRESS_PROTOCOL).unwrap(),
            vec![
                ("192.0.2.10/24".parse::<VipAddr>().unwrap(), "eth0".into()),
                (
                    "2001:db8::10/64".parse::<VipAddr>().unwrap(),
                    "eth0.200".into()
                ),
            ]
        );
    }

    /// Deprecated IPv6 VIPs report extra lifetime fields; discovery must keep seeing them as
    /// owned addresses so crash cleanup still reclaims them.
    #[test]
    fn parser_tolerates_deprecated_lifetime_fields_on_marked_addresses() {
        let json = br#"[{"ifname":"eth0","addr_info":[
            {"family":"inet6","local":"fd00:5290::100","prefixlen":128,"scope":"global",
             "deprecated":true,"protocol":246,
             "valid_life_time":4294967295,"preferred_life_time":0}
        ]}]"#;

        assert_eq!(
            parse_owned_addresses(json, DEFAULT_VIP_ADDRESS_PROTOCOL).unwrap(),
            vec![(
                "fd00:5290::100/128".parse::<VipAddr>().unwrap(),
                "eth0".into()
            )]
        );
    }

    #[test]
    fn parser_ignores_numeric_mode_placeholders_without_a_protocol() {
        let json = br#"[{"ifname":"lo","addr_info":[{},
            {"family":"inet","local":"192.0.2.40","prefixlen":32,"protocol":"0xf6"}
        ]}]"#;

        assert_eq!(
            parse_owned_addresses(json, DEFAULT_VIP_ADDRESS_PROTOCOL).unwrap(),
            vec![("192.0.2.40/32".parse::<VipAddr>().unwrap(), "lo".into())]
        );
    }

    #[test]
    fn parser_rejects_malformed_or_inconsistent_kernel_output() {
        assert!(
            parse_owned_addresses(br#"{"ifname":"eth0"}"#, DEFAULT_VIP_ADDRESS_PROTOCOL).is_err()
        );
        assert!(
            parse_owned_addresses(
                br#"[{"ifname":"eth0","addr_info":[{"family":"inet","local":"2001:db8::1","prefixlen":64,"protocol":"0xf6"}]}]"#,
                DEFAULT_VIP_ADDRESS_PROTOCOL,
            )
            .is_err()
        );
        assert!(
            parse_owned_addresses(
                br#"[{"ifname":"eth0","addr_info":[{"family":"inet","local":"192.0.2.1","prefixlen":24,"protocol":true}]}]"#,
                DEFAULT_VIP_ADDRESS_PROTOCOL,
            )
            .is_err()
        );
        assert!(
            parse_marker_routes(
                br#"[{"type":"1","dst":"192.0.2.1","table":"10246","protocol":246}]"#,
                DEFAULT_VIP_ADDRESS_PROTOCOL,
                false,
            )
            .is_err()
        );
        assert!(
            parse_ownership_discovery(
                br#"[
                  {"ifname":"eth0","addr_info":[
                    {"family":"inet","local":"192.0.2.1","prefixlen":32}
                  ]},
                  {"ifname":"eth1","addr_info":[
                    {"family":"inet","local":"192.0.2.1","prefixlen":32}
                  ]}
                ]"#,
                br#"[{"type":"9","dst":"192.0.2.1","table":"10246","protocol":246}]"#,
                br#"[]"#,
                DEFAULT_VIP_ADDRESS_PROTOCOL,
            )
            .is_err()
        );
    }

    #[test]
    fn cleanup_targets_include_current_config_and_removed_marked_vips_only_once() {
        let configured = vec![("192.0.2.10/24".parse::<VipAddr>().unwrap(), "eth0".into())];
        let discovered = vec![
            (
                "2001:db8::10/64".parse::<VipAddr>().unwrap(),
                "eth0.200".into(),
            ),
            configured[0].clone(),
        ];

        assert_eq!(
            merge_cleanup_targets(&configured, discovered),
            vec![
                configured[0].clone(),
                (
                    "2001:db8::10/64".parse::<VipAddr>().unwrap(),
                    "eth0.200".into()
                ),
            ]
        );
    }

    #[test]
    fn discovered_kernel_prefix_overrides_changed_config_prefix() {
        let configured = vec![("192.0.2.10/24".parse::<VipAddr>().unwrap(), "eth0".into())];
        let discovered = vec![("192.0.2.10/32".parse::<VipAddr>().unwrap(), "eth0".into())];

        assert_eq!(
            merge_cleanup_targets(&configured, discovered.clone()),
            discovered
        );
    }

    #[test]
    fn discovery_output_must_be_successful_and_valid_json() {
        let failed = Output {
            status: ExitStatus::from_raw(2 << 8),
            stdout: Vec::new(),
            stderr: b"permission denied".to_vec(),
        };
        assert!(
            parse_discovery_output(failed, DEFAULT_VIP_ADDRESS_PROTOCOL)
                .unwrap_err()
                .to_string()
                .contains("permission denied")
        );

        let malformed = Output {
            status: ExitStatus::from_raw(0),
            stdout: b"not json".to_vec(),
            stderr: Vec::new(),
        };
        assert!(parse_discovery_output(malformed, DEFAULT_VIP_ADDRESS_PROTOCOL).is_err());

        let empty = || Output {
            status: ExitStatus::from_raw(0),
            stdout: b"[]".to_vec(),
            stderr: Vec::new(),
        };
        let failed_route = Output {
            status: ExitStatus::from_raw(2 << 8),
            stdout: Vec::new(),
            stderr: b"route denied".to_vec(),
        };
        assert!(
            parse_discovery_outputs(empty(), failed_route, empty(), DEFAULT_VIP_ADDRESS_PROTOCOL,)
                .unwrap_err()
                .to_string()
                .contains("route denied")
        );

        let absent = Output {
            status: ExitStatus::from_raw(0),
            stdout: br#"[{"type":"9","dst":"192.0.2.31","table":"10246","protocol":246}]"#.to_vec(),
            stderr: Vec::new(),
        };
        assert!(
            failed_marker_delete_is_absent(
                absent,
                "192.0.2.30".parse().unwrap(),
                DEFAULT_VIP_ADDRESS_PROTOCOL,
            )
            .unwrap()
        );
        let same_destination_from_foreign_instance = Output {
            status: ExitStatus::from_raw(0),
            stdout: br#"[{"type":"9","dst":"192.0.2.30","table":"10245","protocol":245}]"#.to_vec(),
            stderr: Vec::new(),
        };
        assert!(
            failed_marker_delete_is_absent(
                same_destination_from_foreign_instance,
                "192.0.2.30".parse().unwrap(),
                DEFAULT_VIP_ADDRESS_PROTOCOL,
            )
            .unwrap()
        );
        let present = Output {
            status: ExitStatus::from_raw(0),
            stdout: br#"[{"type":"9","dst":"192.0.2.30","table":"10246","protocol":246}]"#.to_vec(),
            stderr: Vec::new(),
        };
        assert!(
            !failed_marker_delete_is_absent(
                present,
                "192.0.2.30".parse().unwrap(),
                DEFAULT_VIP_ADDRESS_PROTOCOL,
            )
            .unwrap()
        );
    }
}
