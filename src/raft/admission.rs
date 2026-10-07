//! Exact volatile replica identities and immutable admission context.

mod controller;
pub use controller::*;
mod membership;
pub use membership::*;
pub(crate) mod runtime;

use crate::config::ClusterConfigFingerprint;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::sync::Arc;
use std::{fmt, io, str::FromStr};

/// A configured physical member and one exact volatile runtime, without compression.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReplicaId {
    pub physical_id: u64,
    pub boot_nonce: [u8; 32],
}

impl TryFrom<u64> for ReplicaId {
    type Error = AdmissionDenied;
    fn try_from(_: u64) -> Result<Self, Self::Error> {
        Err(AdmissionDenied(
            "physical-only Raft identity has no boot authority",
        ))
    }
}

impl ReplicaId {
    pub fn fresh(physical_id: u64) -> io::Result<Self> {
        let mut boot_nonce = [0; 32];
        getrandom::fill(&mut boot_nonce)
            .map_err(|_| io::Error::other("replica boot entropy unavailable"))?;
        Ok(Self {
            physical_id,
            boot_nonce,
        })
    }
}

impl fmt::Display for ReplicaId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}:", self.physical_id)?;
        for byte in self.boot_nonce {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl FromStr for ReplicaId {
    type Err = AdmissionDenied;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let bytes = value.as_bytes();
        if bytes.len() != 81
            || bytes[16] != b':'
            || bytes
                .iter()
                .enumerate()
                .any(|(i, byte)| i != 16 && !matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
        {
            return Err(AdmissionDenied("noncanonical replica identity"));
        }
        let physical_id = u64::from_str_radix(&value[..16], 16)
            .map_err(|_| AdmissionDenied("invalid physical identity"))?;
        let mut boot_nonce = [0; 32];
        for (index, byte) in boot_nonce.iter_mut().enumerate() {
            let offset = 17 + index * 2;
            *byte = u8::from_str_radix(&value[offset..offset + 2], 16)
                .map_err(|_| AdmissionDenied("invalid boot identity"))?;
        }
        Ok(Self {
            physical_id,
            boot_nonce,
        })
    }
}

impl Serialize for ReplicaId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for ReplicaId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

/// The exact consenting cold roster, not a discovery response or an admission grant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Genesis {
    pub config: ClusterConfigFingerprint,
    pub epoch: u128,
    pub voters: BTreeSet<ReplicaId>,
}

impl Genesis {
    pub fn digest(&self) -> [u8; 32] {
        let mut digest = Sha256::new();
        digest.update(b"keepafloatd-admission-genesis\0");
        digest.update([3, 1, self.config.version]);
        digest.update(self.config.digest);
        digest.update(self.epoch.to_be_bytes());
        digest.update((self.voters.len() as u64).to_be_bytes());
        for replica in &self.voters {
            digest.update(replica.physical_id.to_be_bytes());
            digest.update(replica.boot_nonce);
        }
        digest.finalize().into()
    }

    pub fn validate_roster(&self, configured: &BTreeSet<u64>) -> Result<(), AdmissionDenied> {
        let physical: BTreeSet<_> = self.voters.iter().map(|id| id.physical_id).collect();
        if physical.len() != self.voters.len()
            || !physical.is_subset(configured)
            || physical.len() <= configured.len() / 2
        {
            return Err(AdmissionDenied(
                "genesis is not a distinct configured physical majority",
            ));
        }
        Ok(())
    }
}

/// Immutable context selected by admission; possession alone grants no permission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdmissionContext {
    pub local_replica: ReplicaId,
    pub genesis: Genesis,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdmissionDenied(pub &'static str);

impl fmt::Display for AdmissionDenied {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for AdmissionDenied {}

/// The composition root owns terminal revocation and timing; consensus only checks it.
pub trait AdmissionFence: Send + Sync {
    fn local_replica(&self) -> ReplicaId;
    fn check(&self, expected: &AdmissionContext) -> Result<tokio::time::Instant, AdmissionDenied>;
}

/// Local authority handle, deliberately absent from every snapshot and wire record.
#[derive(Clone)]
pub struct AdmissionSession {
    context: AdmissionContext,
    fence: Arc<dyn AdmissionFence>,
}

impl AdmissionSession {
    pub fn new(
        context: AdmissionContext,
        fence: Arc<dyn AdmissionFence>,
    ) -> Result<Self, AdmissionDenied> {
        if fence.local_replica() != context.local_replica {
            return Err(AdmissionDenied(
                "runtime boot differs from admission context",
            ));
        }
        let session = Self { context, fence };
        session.check()?;
        Ok(session)
    }

    pub fn context(&self) -> &AdmissionContext {
        &self.context
    }

    pub fn check(&self) -> Result<tokio::time::Instant, AdmissionDenied> {
        let deadline = self.fence.check(&self.context)?;
        if tokio::time::Instant::now() >= deadline {
            return Err(AdmissionDenied("admission deadline expired"));
        }
        Ok(deadline)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppliedHealthProgress {
    pub request: HealthProgress,
    pub log_id: openraft::alias::LogIdOf<super::TypeConfig>,
}

/// Fresh consumer challenge carried unchanged through the committed state machine.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HealthProgress {
    pub node_id: u64,
    /// None renews runtime admission without publishing a new service probe.
    pub healthy: Option<bool>,
    pub replica: ReplicaId,
    pub epoch: u128,
    pub request_nonce: [u8; 32],
    pub genesis: Genesis,
}

impl HealthProgress {
    pub fn validate(&self) -> Result<(), AdmissionDenied> {
        if self.node_id != self.replica.physical_id || self.epoch != self.genesis.epoch {
            return Err(AdmissionDenied("progress identity or epoch mismatch"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn replica_identity_is_lossless_and_canonical_in_json_map_keys() {
        let first = ReplicaId {
            physical_id: u64::MAX,
            boot_nonce: [255; 32],
        };
        let mut second = first;
        second.boot_nonce[31] = 254;
        let map = BTreeMap::from([(first, 1), (second, 2)]);
        let bytes = serde_json::to_vec(&map).unwrap();
        let restored: BTreeMap<ReplicaId, u8> = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(restored, map);
        assert_eq!(
            first.to_string(),
            format!("ffffffffffffffff:{}", "ff".repeat(32))
        );
        assert_ne!(first, second);
        for invalid in [
            "1".to_owned(),
            first.to_string().to_uppercase(),
            format!("1:{}", "ff".repeat(32)),
        ] {
            assert!(invalid.parse::<ReplicaId>().is_err());
        }
        assert!(serde_json::from_str::<ReplicaId>("1").is_err());
    }

    #[test]
    fn genesis_requires_distinct_configured_physical_majority() {
        let roster = BTreeSet::from([1, 2, 3, 4, 5]);
        let mut genesis = Genesis {
            config: ClusterConfigFingerprint {
                version: 1,
                digest: [1; 32],
            },
            epoch: u128::MAX,
            voters: [3, 4, 5]
                .map(|physical_id| ReplicaId {
                    physical_id,
                    boot_nonce: [9; 32],
                })
                .into(),
        };
        assert!(genesis.validate_roster(&roster).is_ok());
        genesis.voters.remove(&ReplicaId {
            physical_id: 3,
            boot_nonce: [9; 32],
        });
        assert!(genesis.validate_roster(&roster).is_err());
        genesis.voters.insert(ReplicaId {
            physical_id: 4,
            boot_nonce: [8; 32],
        });
        assert!(genesis.validate_roster(&roster).is_err());
        genesis.voters.insert(ReplicaId {
            physical_id: 99,
            boot_nonce: [8; 32],
        });
        assert!(genesis.validate_roster(&roster).is_err());
    }

    #[test]
    fn fresh_boots_do_not_inherit_identity() {
        let first = ReplicaId::fresh(u64::MAX).unwrap();
        let second = ReplicaId::fresh(u64::MAX).unwrap();
        assert_eq!(first.physical_id, second.physical_id);
        assert_ne!(first.boot_nonce, second.boot_nonce);
    }
}
