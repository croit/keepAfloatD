//! Strict operation envelopes for protocol-three Raft and admission frames.

use super::SnapshotTransfer;
use crate::raft::probe::ClusterStatusRequest;
use crate::raft::types::TypeConfig;
use openraft::network::RPCTypes;
use openraft::raft::{AppendEntriesRequest, VoteRequest};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum RaftRequest {
    Admission(super::admission_rpc::SignedAdmissionRecord),
    AdmissionControl(
        crate::raft::admission::SignedAdmission<
            super::authorization::management::ManagementRequest,
        >,
    ),
    Status(ClusterStatusRequest),
    PreVote(VoteRequest<TypeConfig>),
    AppendEntries(AppendEntriesRequest<TypeConfig>),
    #[serde(rename = "install_snapshot")]
    Snapshot(SnapshotTransfer),
    Vote(VoteRequest<TypeConfig>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Operation {
    Admission,
    AdmissionControl,
    Status,
    PreVote,
    AppendEntries,
    InstallSnapshot,
    Vote,
}

impl TryFrom<RPCTypes> for Operation {
    type Error = &'static str;

    fn try_from(value: RPCTypes) -> Result<Self, Self::Error> {
        Ok(match value {
            RPCTypes::AppendEntries => Self::AppendEntries,
            RPCTypes::InstallSnapshot => Self::InstallSnapshot,
            RPCTypes::Vote => Self::Vote,
            RPCTypes::TransferLeader => return Err("leader transfer has no wire operation"),
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Envelope<T> {
    Admission(T),
    AdmissionControl(T),
    Status(T),
    PreVote(T),
    AppendEntries(T),
    InstallSnapshot(T),
    Vote(T),
}

pub(super) fn encode<T: Serialize>(
    operation: Operation,
    payload: &T,
) -> serde_json::Result<Vec<u8>> {
    let envelope = match operation {
        Operation::Admission => Envelope::Admission(payload),
        Operation::AdmissionControl => Envelope::AdmissionControl(payload),
        Operation::Status => Envelope::Status(payload),
        Operation::PreVote => Envelope::PreVote(payload),
        Operation::AppendEntries => Envelope::AppendEntries(payload),
        Operation::InstallSnapshot => Envelope::InstallSnapshot(payload),
        Operation::Vote => Envelope::Vote(payload),
    };
    serde_json::to_vec(&envelope)
}

pub(super) fn decode_payload<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
    expected: Operation,
) -> anyhow::Result<T> {
    // Decode directly into the concrete payload so u128 identifiers never pass through JSON Value.
    let (operation, payload) = match serde_json::from_slice::<Envelope<T>>(bytes)? {
        Envelope::Admission(payload) => (Operation::Admission, payload),
        Envelope::AdmissionControl(payload) => (Operation::AdmissionControl, payload),
        Envelope::Status(payload) => (Operation::Status, payload),
        Envelope::PreVote(payload) => (Operation::PreVote, payload),
        Envelope::AppendEntries(payload) => (Operation::AppendEntries, payload),
        Envelope::InstallSnapshot(payload) => (Operation::InstallSnapshot, payload),
        Envelope::Vote(payload) => (Operation::Vote, payload),
    };
    anyhow::ensure!(
        operation == expected,
        "unexpected RPC operation {operation:?}; expected {expected:?}"
    );
    Ok(payload)
}

pub(super) fn decode(buf: &[u8]) -> anyhow::Result<RaftRequest> {
    Ok(serde_json::from_slice(buf)?)
}

impl RaftRequest {
    pub(super) fn status(&self) -> Option<&ClusterStatusRequest> {
        match self {
            Self::Status(request) => Some(request),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::types::{FailoverSemantics, KafRequest};
    use openraft::alias::{CommittedLeaderIdOf, EntryOf, LogIdOf, VoteOf};
    use openraft::vote::RaftLeaderId;

    #[test]
    fn management_control_discovery_is_a_distinct_tag() {
        #[derive(Serialize)]
        struct Discovery {
            nonce: [u8; 32],
            action: &'static str,
        }
        let record = crate::raft::admission::SignedAdmission::sign(
            Some("network-test-secret"),
            12,
            crate::raft::types::test_replica(1),
            crate::raft::types::test_replica(2),
            [4; 32],
            Discovery {
                nonce: [3; 32],
                action: "Discover",
            },
        )
        .unwrap();
        let payload = serde_json::to_string(&record).unwrap();
        assert!(decode(format!("{{\"admission_control\":{payload}}}").as_bytes()).is_ok());
        assert!(decode(format!("{{\"admission\":{payload}}}").as_bytes()).is_err());
    }

    #[test]
    fn tagged_request_golden_vectors_cover_every_operation() {
        let vote = || {
            VoteRequest::<TypeConfig>::new(
                VoteOf::<TypeConfig>::new(7, crate::raft::types::test_replica(2)),
                None,
            )
        };
        let cases = [
            (
                RaftRequest::Status(ClusterStatusRequest {
                    probe_from: 2,
                    ..Default::default()
                }),
                r#"{"status":{"probe_from":2,"config_fingerprint":null,"supports_cancellation_safe_rpc_v1":false}}"#,
            ),
            (
                RaftRequest::Vote(vote()),
                r#"{"vote":{"vote":{"leader_id":{"term":7,"node_id":"0000000000000002:2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a"},"committed":false},"last_log_id":null,"leadership_transfer":false}}"#,
            ),
            (
                RaftRequest::PreVote(vote()),
                r#"{"pre_vote":{"vote":{"leader_id":{"term":7,"node_id":"0000000000000002:2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a"},"committed":false},"last_log_id":null,"leadership_transfer":false}}"#,
            ),
            (
                RaftRequest::AppendEntries(AppendEntriesRequest::<TypeConfig> {
                    vote: VoteOf::<TypeConfig>::new(7, crate::raft::types::test_replica(2)),
                    prev_log_id: None,
                    entries: vec![],
                    leader_commit: None,
                }),
                r#"{"append_entries":{"vote":{"leader_id":{"term":7,"node_id":"0000000000000002:2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a"},"committed":false},"prev_log_id":null,"entries":[],"leader_commit":null}}"#,
            ),
            (
                RaftRequest::Snapshot(SnapshotTransfer {
                    vote: VoteOf::<TypeConfig>::new(7, crate::raft::types::test_replica(2)),
                    meta: openraft::SnapshotMeta {
                        snapshot_id: "sample".into(),
                        ..Default::default()
                    },
                    data: vec![0, 255],
                }),
                r#"{"install_snapshot":{"vote":{"leader_id":{"term":7,"node_id":"0000000000000002:2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a"},"committed":false},"meta":{"last_log_id":null,"last_membership":{"log_id":null,"membership":{"configs":[],"nodes":{}}},"snapshot_id":"sample"},"data":[0,255]}}"#,
            ),
        ];
        for (request, expected) in cases {
            let encoded = match &request {
                RaftRequest::Admission(value) => encode(Operation::Admission, value),
                RaftRequest::AdmissionControl(value) => encode(Operation::AdmissionControl, value),
                RaftRequest::Status(value) => encode(Operation::Status, value),
                RaftRequest::PreVote(value) => encode(Operation::PreVote, value),
                RaftRequest::AppendEntries(value) => encode(Operation::AppendEntries, value),
                RaftRequest::Snapshot(value) => encode(Operation::InstallSnapshot, value),
                RaftRequest::Vote(value) => encode(Operation::Vote, value),
            }
            .unwrap();
            assert_eq!(String::from_utf8(encoded).unwrap(), expected);
            let decoded = decode(expected.as_bytes()).unwrap();
            assert_eq!(serde_json::to_string(&decoded).unwrap(), expected);
        }
        assert!(matches!(
            decode(br#"{"status":{"probe_from":2}}"#).unwrap(),
            RaftRequest::Status(_)
        ));
    }

    fn response_golden<T: Serialize + serde::de::DeserializeOwned>(
        operation: Operation,
        payload: T,
        expected: &str,
    ) {
        assert_eq!(
            String::from_utf8(encode(operation, &payload).unwrap()).unwrap(),
            expected
        );
        let decoded: T = decode_payload(expected.as_bytes(), operation).unwrap();
        assert_eq!(
            serde_json::to_vec(&decoded).unwrap(),
            serde_json::to_vec(&payload).unwrap()
        );
        for other in [
            Operation::Admission,
            Operation::AdmissionControl,
            Operation::Status,
            Operation::PreVote,
            Operation::Vote,
            Operation::AppendEntries,
            Operation::InstallSnapshot,
        ] {
            if other != operation {
                assert!(decode_payload::<T>(expected.as_bytes(), other).is_err());
            }
        }
    }

    #[test]
    fn tagged_response_golden_vectors_cover_every_operation() {
        use crate::raft::probe::ClusterStatusResponse;
        use openraft::raft::{AppendEntriesResponse, SnapshotResponse, VoteResponse};
        response_golden(
            Operation::Status,
            ClusterStatusResponse::default(),
            r#"{"status":{"supports_pre_vote":false,"initialized":false,"current_leader":null,"member_count":0,"cluster_epoch":null,"supports_failover_semantics_v2":false,"config_fingerprint":null,"supports_config_identity_v1":false,"config_identity_enforced":false}}"#,
        );
        response_golden(
            Operation::Vote,
            VoteResponse::<TypeConfig>::new(
                VoteOf::<TypeConfig>::new(7, crate::raft::types::test_replica(2)),
                None,
                true,
            ),
            r#"{"vote":{"vote":{"leader_id":{"term":7,"node_id":"0000000000000002:2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a"},"committed":false},"vote_granted":true,"last_log_id":null}}"#,
        );
        response_golden(
            Operation::PreVote,
            VoteResponse::<TypeConfig>::new(
                VoteOf::<TypeConfig>::new(7, crate::raft::types::test_replica(2)),
                None,
                false,
            ),
            r#"{"pre_vote":{"vote":{"leader_id":{"term":7,"node_id":"0000000000000002:2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a"},"committed":false},"vote_granted":false,"last_log_id":null}}"#,
        );
        response_golden(
            Operation::InstallSnapshot,
            SnapshotResponse::<TypeConfig>::new(VoteOf::<TypeConfig>::new(
                7,
                crate::raft::types::test_replica(2),
            )),
            r#"{"install_snapshot":{"vote":{"leader_id":{"term":7,"node_id":"0000000000000002:2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a"},"committed":false}}}"#,
        );
        for (payload, expected) in [
            (
                AppendEntriesResponse::<TypeConfig>::Success,
                r#"{"append_entries":"Success"}"#,
            ),
            (
                AppendEntriesResponse::<TypeConfig>::Conflict,
                r#"{"append_entries":"Conflict"}"#,
            ),
            (
                AppendEntriesResponse::<TypeConfig>::PartialSuccess(None),
                r#"{"append_entries":{"PartialSuccess":null}}"#,
            ),
            (
                AppendEntriesResponse::<TypeConfig>::HigherVote(VoteOf::<TypeConfig>::new(
                    7,
                    crate::raft::types::test_replica(2),
                )),
                r#"{"append_entries":{"HigherVote":{"leader_id":{"term":7,"node_id":"0000000000000002:2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a"},"committed":false}}}"#,
            ),
        ] {
            response_golden(Operation::AppendEntries, payload, expected);
        }
    }

    #[test]
    fn tagged_status_response_preserves_maximum_epoch_and_leader() {
        let response = crate::raft::probe::ClusterStatusResponse {
            cluster_epoch: Some(u128::MAX),
            current_leader: Some(crate::raft::types::test_replica(u64::MAX)),
            ..Default::default()
        };
        let bytes = encode(Operation::Status, &response).unwrap();
        let decoded: crate::raft::probe::ClusterStatusResponse =
            decode_payload(&bytes, Operation::Status).unwrap();
        assert_eq!(decoded, response);
    }

    #[test]
    fn tagged_request_rejects_malformed_payload_for_each_selected_operation() {
        for operation in [
            "admission",
            "admission_control",
            "status",
            "pre_vote",
            "vote",
            "append_entries",
            "install_snapshot",
        ] {
            for payload in ["null", "false", "[]", "{}", "{\"probe_from\":false}"] {
                let bytes = format!("{{\"{operation}\":{payload}}}");
                assert!(decode(bytes.as_bytes()).is_err(), "accepted {bytes}");
            }
        }
    }

    #[test]
    fn rpc_actions_map_only_to_implemented_operations() {
        assert_eq!(
            Operation::try_from(RPCTypes::Vote).unwrap(),
            Operation::Vote
        );
        assert_eq!(
            Operation::try_from(RPCTypes::AppendEntries).unwrap(),
            Operation::AppendEntries
        );
        assert_eq!(
            Operation::try_from(RPCTypes::InstallSnapshot).unwrap(),
            Operation::InstallSnapshot
        );
        assert!(Operation::try_from(RPCTypes::TransferLeader).is_err());
    }

    #[test]
    fn tagged_wire_accepts_explicit_vote_operation() {
        let vote = VoteRequest::<TypeConfig>::new(
            VoteOf::<TypeConfig>::new(7, crate::raft::types::test_replica(2)),
            None,
        );
        let payload = serde_json::to_string(&vote).unwrap();
        let bytes = format!("{{\"vote\":{payload}}}");
        assert!(matches!(decode(bytes.as_bytes()), Ok(RaftRequest::Vote(_))));
    }

    #[test]
    fn tagged_wire_rejects_legacy_and_multiple_operations() {
        let vote = VoteRequest::<TypeConfig>::new(
            VoteOf::<TypeConfig>::new(7, crate::raft::types::test_replica(2)),
            None,
        );
        let payload = serde_json::to_string(&vote).unwrap();
        for bytes in [
            payload.clone(),
            format!("{{\"unknown\":{payload}}}"),
            format!("{{\"vote\":{payload},\"vote\":{payload}}}"),
            format!("{{\"vote\":{payload},\"pre_vote\":{payload}}}"),
            format!("{{\"append_entries\":false,\"vote\":{payload}}}"),
            "{\"vote\":null}".to_owned(),
        ] {
            assert!(decode(bytes.as_bytes()).is_err(), "accepted {bytes}");
        }
    }

    #[test]
    fn decoded_append_keeps_full_width_integers_and_exact_wire_bytes() {
        let request = AppendEntriesRequest::<TypeConfig> {
            vote: VoteOf::<TypeConfig>::new_committed(7, crate::raft::types::test_replica(2)),
            prev_log_id: None,
            entries: vec![EntryOf::<TypeConfig> {
                log_id: LogIdOf::<TypeConfig>::new(
                    CommittedLeaderIdOf::<TypeConfig>::new(7, crate::raft::types::test_replica(2)),
                    1,
                ),
                payload: openraft::EntryPayload::Normal(KafRequest::ClusterFormed {
                    cluster_id: u128::MAX,
                    failover_semantics: FailoverSemantics::V2,
                    config_identity_enforced: true,
                }),
            }],
            leader_commit: None,
        };
        let bytes = encode(Operation::AppendEntries, &request).unwrap();
        let RaftRequest::AppendEntries(decoded) = decode(&bytes).unwrap() else {
            panic!("append decoded as a different request");
        };
        assert_eq!(encode(Operation::AppendEntries, &decoded).unwrap(), bytes);
        assert!(matches!(
            decoded.entries[0].payload,
            openraft::EntryPayload::Normal(KafRequest::ClusterFormed {
                cluster_id: u128::MAX,
                ..
            })
        ));
    }

    #[test]
    fn decoded_vote_and_pre_vote_keep_their_exact_wire_bytes() {
        let vote = VoteRequest::<TypeConfig>::new(
            VoteOf::<TypeConfig>::new(7, crate::raft::types::test_replica(2)),
            None,
        );
        let bytes = encode(Operation::Vote, &vote).unwrap();
        let RaftRequest::Vote(decoded) = decode(&bytes).unwrap() else {
            panic!("vote decoded as a different request");
        };
        assert_eq!(encode(Operation::Vote, &decoded).unwrap(), bytes);

        let bytes = encode(Operation::PreVote, &vote).unwrap();
        let RaftRequest::PreVote(decoded) = decode(&bytes).unwrap() else {
            panic!("pre-vote decoded as a different request");
        };
        assert_eq!(encode(Operation::PreVote, &decoded).unwrap(), bytes);
    }
}
