//! Replicated, resumable learner admission operations.

use super::{AdmissionDenied, Genesis, ReplicaId};
use crate::raft::TypeConfig;
use openraft::alias::LogIdOf;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AdmissionCommand {
    PrepareJoin(JoinPlan),
    CancelJoin {
        plan: JoinPlan,
        prepared: LogIdOf<TypeConfig>,
    },
    LearnerApplied {
        consumer: ReplicaId,
        /// The committed operation nonce, not a later admission challenge.
        request_nonce: [u8; 32],
        prepared: LogIdOf<TypeConfig>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JoinPlan {
    pub genesis: Genesis,
    pub consumer: ReplicaId,
    /// Immutable operation nonce, independent of each short admission challenge.
    pub request_nonce: [u8; 32],
    pub previous_voters: BTreeSet<ReplicaId>,
    pub next_voters: BTreeSet<ReplicaId>,
}

impl JoinPlan {
    pub fn validate(&self) -> Result<(), AdmissionDenied> {
        let mut expected = self.previous_voters.clone();
        expected.retain(|replica| replica.physical_id != self.consumer.physical_id);
        expected.insert(self.consumer);
        let physical: BTreeSet<_> = self
            .previous_voters
            .iter()
            .map(|id| id.physical_id)
            .collect();
        if self.previous_voters.is_empty()
            || physical.len() != self.previous_voters.len()
            || self.previous_voters.contains(&self.consumer)
            || self.next_voters != expected
        {
            return Err(AdmissionDenied(
                "join plan is not one exact boot replacement",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreparedJoin {
    pub plan: JoinPlan,
    pub prepared: LogIdOf<TypeConfig>,
    pub learner_applied: Option<LogIdOf<TypeConfig>>,
}

/// Replicated completion evidence survives a lost management reply and snapshot compaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompletedJoin {
    pub prepared: PreparedJoin,
    pub promoted: LogIdOf<TypeConfig>,
}
