use super::{Config, VipConfig};
use std::collections::BTreeMap;
use std::net::IpAddr;

impl VipConfig {
    pub(super) fn effective_interface(&self) -> String {
        match self.vlan {
            Some(vlan) => format!("{}.{vlan}", self.interface),
            None => self.interface.clone(),
        }
    }
}

impl Config {
    pub(super) fn normalize_vips(&mut self) -> anyhow::Result<()> {
        let mut bindings = BTreeMap::new();
        for (i, vip) in self.vips.iter_mut().enumerate() {
            if let IpAddr::V6(address) = vip.address.addr
                && let Some(ipv4) = address.to_ipv4_mapped()
            {
                anyhow::ensure!(
                    (97..=128).contains(&vip.address.prefix),
                    "vips[{i}]: mapped IPv4 prefix must be /97 to /128"
                );
                vip.address.addr = IpAddr::V4(ipv4);
                vip.address.prefix -= 96;
            }
            let address = vip.address.addr;
            anyhow::ensure!(
                !address.is_unspecified() && !address.is_multicast() && !address.is_loopback(),
                "vips[{i}]: VIP must be a non-loopback unicast host address"
            );
            let max = if address.is_ipv4() { 32 } else { 128 };
            anyhow::ensure!(
                (1..=max).contains(&vip.address.prefix),
                "vips[{i}]: VIP prefix must be between 1 and {max}"
            );
            if let IpAddr::V4(address) = address {
                anyhow::ensure!(
                    !address.is_broadcast(),
                    "vips[{i}]: broadcast is not a VIP host address"
                );
                if vip.address.prefix < 31 {
                    let host_mask = u32::MAX >> vip.address.prefix;
                    let host = u32::from(address) & host_mask;
                    anyhow::ensure!(
                        host != 0 && host != host_mask,
                        "vips[{i}]: VIP must not be the subnet network or broadcast address"
                    );
                }
            }
            validate_interface(&vip.interface, i)?;
            if let Some(vlan) = vip.vlan {
                anyhow::ensure!(
                    (1..=4094).contains(&vlan),
                    "vips[{i}] ({address}): vlan {vlan} is out of range: IEEE 802.1Q allows 1-4094"
                );
                anyhow::ensure!(
                    !vip.interface.contains('.'),
                    "vips[{i}]: interface {:?} must not contain a dot when vlan is set",
                    vip.interface
                );
            }
            let interface = vip.effective_interface();
            validate_interface(&interface, i)?;
            let binding = (vip.address.prefix, interface);
            if let Some(previous) = bindings.insert(address, binding.clone()) {
                anyhow::ensure!(
                    previous == binding,
                    "conflicting VIP {address}: prefix and effective interface must agree"
                );
            }
        }
        self.vips.sort_by_key(|vip| vip.address.addr);
        self.vips.dedup_by_key(|vip| vip.address.addr);
        Ok(())
    }
}

fn validate_interface(name: &str, index: usize) -> anyhow::Result<()> {
    anyhow::ensure!(
        !name.is_empty()
            && name.len() <= 15
            && ![".", ".."].contains(&name)
            && !name
                .chars()
                .any(|c| c.is_whitespace() || c.is_control() || matches!(c, '/' | ':')),
        "vips[{index}]: interface must be a Linux name of 1 to 15 bytes without whitespace, controls, '/', ':' or dot-only names"
    );
    Ok(())
}
