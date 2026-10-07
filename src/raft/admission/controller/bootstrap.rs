//! Quorum-attested exact JOIN plan binding, separate from replicated state.

use super::super::{JoinPlan, PreparedJoin};
use crate::raft::TypeConfig;
use openraft::alias::LogIdOf;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JoinBinding {
    pub log_id: LogIdOf<TypeConfig>,
    pub plan_digest: [u8; 32],
}

impl JoinBinding {
    pub fn from_prepared(prepared: &PreparedJoin) -> Self {
        Self {
            log_id: prepared.prepared,
            plan_digest: plan_digest(&prepared.plan),
        }
    }
}

fn plan_digest(plan: &JoinPlan) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"keepafloatd-admission-join-plan-v1\0");
    hash.update(plan.genesis.digest());
    hash.update(plan.consumer.physical_id.to_be_bytes());
    hash.update(plan.consumer.boot_nonce);
    hash.update(plan.request_nonce);
    for voters in [&plan.previous_voters, &plan.next_voters] {
        hash.update((voters.len() as u64).to_be_bytes());
        for voter in voters {
            hash.update(voter.physical_id.to_be_bytes());
            hash.update(voter.boot_nonce);
        }
    }
    hash.finalize().into()
}

/// Only the controller's successful quorum verification can construct this capability.
#[derive(Clone)]
pub struct VerifiedJoinBootstrap {
    pub(super) prepared: PreparedJoin,
}

impl VerifiedJoinBootstrap {
    pub fn prepared(&self) -> &PreparedJoin {
        &self.prepared
    }
}
