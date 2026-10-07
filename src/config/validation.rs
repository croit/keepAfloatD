use super::{Config, MAX_RAFT_FRAME_BYTES, parse_socket_addr};
use anyhow::Context;
use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;

impl Config {
    pub(super) fn normalize(&mut self) -> anyhow::Result<()> {
        anyhow::ensure!(!self.peers.is_empty(), "peers must be non-empty");
        anyhow::ensure!(!self.vips.is_empty(), "vips must be non-empty");
        self.validate_secret()?;
        anyhow::ensure!(
            !self.health.command.is_empty(),
            "health.command must be non-empty"
        );
        anyhow::ensure!(
            self.health.interval_ms > 0,
            "health.interval_ms must be > 0"
        );
        anyhow::ensure!(self.health.timeout_ms > 0, "health.timeout_ms must be > 0");
        anyhow::ensure!(
            self.health.timeout_ms <= self.health.interval_ms.saturating_mul(10),
            "health.timeout_ms suspiciously large vs interval_ms"
        );

        // Validate Raft timing at load. OpenRaft validates the same relations, but only inside
        // start_raft, after startup cleanup has already touched host addresses; failing here
        // keeps a bad raft block from mutating the host at all.
        anyhow::ensure!(
            self.raft.election_timeout_min_ms > 0,
            "raft.election_timeout_min_ms must be > 0"
        );
        anyhow::ensure!(
            self.raft.election_timeout_max_ms > 0,
            "raft.election_timeout_max_ms must be > 0"
        );
        anyhow::ensure!(
            self.raft.heartbeat_interval_ms > 0,
            "raft.heartbeat_interval_ms must be > 0"
        );
        anyhow::ensure!(
            self.raft.heartbeat_interval_ms < self.raft.election_timeout_min_ms,
            "raft.heartbeat_interval_ms ({}) must be < raft.election_timeout_min_ms ({})",
            self.raft.heartbeat_interval_ms,
            self.raft.election_timeout_min_ms
        );
        anyhow::ensure!(
            self.raft.election_timeout_min_ms < self.raft.election_timeout_max_ms,
            "raft.election_timeout_min_ms ({}) must be < raft.election_timeout_max_ms ({})",
            self.raft.election_timeout_min_ms,
            self.raft.election_timeout_max_ms
        );

        let stale = self.health.effective_stale_secs();
        let interval_secs = self.health.interval_ms.div_ceil(1_000).max(1);
        anyhow::ensure!(
            stale >= interval_secs,
            "health.stale_secs ({stale}) must be >= ceil(interval_ms/1000) ({interval_secs})"
        );
        let stale_ms = self
            .health
            .interval_ms
            .saturating_mul(self.health.effective_stale_missed_probes());
        anyhow::ensure!(
            self.health.timeout_ms < stale_ms,
            "health.timeout_ms ({} ms) must be less than the effective stale window ({stale_ms} ms); \
             reduce health.timeout_ms or increase health.stale_secs",
            self.health.timeout_ms
        );

        let ids: BTreeSet<u64> = self.peers.iter().map(|p| p.id).collect();
        anyhow::ensure!(ids.len() == self.peers.len(), "duplicate peer id");
        anyhow::ensure!(
            self.peers.iter().any(|p| p.id == self.node_id),
            "node_id must appear in peers"
        );
        let raft_listen = parse_socket_addr("raft_listen", &self.raft_listen)?;
        let client_submit_listen =
            parse_socket_addr("client_submit_listen", &self.client_submit_listen)?;
        self.raft_listen = raft_listen.to_string();
        self.client_submit_listen = client_submit_listen.to_string();
        let mut endpoint_owners: BTreeMap<SocketAddr, (u64, &'static str)> = BTreeMap::new();
        let mut raft_roster_is_ipv4 = None;
        let mut submit_roster_is_ipv4 = None;
        for peer in &mut self.peers {
            let raft_address = parse_socket_addr(
                &format!("peers[{}].raft_address", peer.id),
                &peer.raft_address,
            )?;
            let client_submit_address = parse_socket_addr(
                &format!("peers[{}].client_submit_address", peer.id),
                &peer.client_submit_address,
            )?;
            peer.raft_address = raft_address.to_string();
            peer.client_submit_address = client_submit_address.to_string();
            if let Some(expected) = raft_roster_is_ipv4 {
                anyhow::ensure!(
                    expected == raft_address.is_ipv4(),
                    "raft_address roster must use one IP family"
                );
            } else {
                raft_roster_is_ipv4 = Some(raft_address.is_ipv4());
            }
            if let Some(expected) = submit_roster_is_ipv4 {
                anyhow::ensure!(
                    expected == client_submit_address.is_ipv4(),
                    "client_submit_address roster must use one IP family"
                );
            } else {
                submit_roster_is_ipv4 = Some(client_submit_address.is_ipv4());
            }
            anyhow::ensure!(
                !raft_address.ip().is_unspecified(),
                "peers[{}].raft_address must not use an unspecified IP ({})",
                peer.id,
                peer.raft_address
            );
            anyhow::ensure!(
                !client_submit_address.ip().is_unspecified(),
                "peers[{}].client_submit_address must not use an unspecified IP ({})",
                peer.id,
                peer.client_submit_address
            );
            anyhow::ensure!(
                raft_address != client_submit_address,
                "peer {} raft_address and client_submit_address must differ",
                peer.id
            );
            for (kind, address) in [
                ("raft_address", raft_address),
                ("client_submit_address", client_submit_address),
            ] {
                if let Some((other_id, other_kind)) = endpoint_owners.get(&address) {
                    anyhow::bail!(
                        "peer endpoint {address} is used by peer {other_id} {other_kind} and peer {} {kind}",
                        peer.id
                    );
                }
                endpoint_owners.insert(address, (peer.id, kind));
            }
        }

        let local_peer = self
            .get_peer(self.node_id)
            .context("node_id must appear in peers after validation")?;
        let local_raft_address = parse_socket_addr(
            &format!("peers[{}].raft_address", local_peer.id),
            &local_peer.raft_address,
        )?;
        let local_client_submit_address = parse_socket_addr(
            &format!("peers[{}].client_submit_address", local_peer.id),
            &local_peer.client_submit_address,
        )?;
        anyhow::ensure!(
            raft_listen == local_raft_address,
            "raft_listen {} must match peers[{}].raft_address {}",
            self.raft_listen,
            self.node_id,
            local_peer.raft_address
        );
        anyhow::ensure!(
            client_submit_listen == local_client_submit_address,
            "client_submit_listen {} must match peers[{}].client_submit_address {}",
            self.client_submit_listen,
            self.node_id,
            local_peer.client_submit_address
        );

        anyhow::ensure!(
            self.max_frame_bytes >= 64 * 1024,
            "max_frame_bytes must be >= 64 KiB to fit Raft heartbeats"
        );
        anyhow::ensure!(
            self.max_frame_bytes <= MAX_RAFT_FRAME_BYTES,
            "max_frame_bytes must be <= 16 MiB"
        );
        anyhow::ensure!(self.submit_timeout_ms > 0, "submit_timeout_ms must be > 0");
        anyhow::ensure!(
            self.address_protocol > 0,
            "address_protocol must be between 1 and 255"
        );

        self.normalize_vips()?;
        Ok(())
    }
}
