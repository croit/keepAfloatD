use super::*;
use crate::config::Config;
use crate::raft::admission::{AdmissionDenied, Genesis, ReplicaId};
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

#[test]
fn received_grant_must_match_the_actual_authenticated_channel() {
    let cfg = config();
    let request = AdmissionRequest {
        consumer: replica(3),
        genesis: genesis(&cfg),
        request_nonce: [8; 32],
        mode: AdmissionMode::Cold,
    };
    let record = SignedAdmission::sign(
        cfg.cluster_secret.as_deref(),
        11,
        replica(2),
        replica(3),
        [7; 32],
        request.clone(),
    )
    .unwrap();
    assert!(
        ReceivedGrant::authenticate(
            cfg.cluster_secret.as_deref(),
            replica(2),
            replica(3),
            [6; 32],
            &request,
            record.clone()
        )
        .is_err()
    );
    assert!(
        ReceivedGrant::authenticate(
            cfg.cluster_secret.as_deref(),
            replica(1),
            replica(3),
            [7; 32],
            &request,
            record.clone()
        )
        .is_err()
    );
    assert!(
        ReceivedGrant::authenticate(
            cfg.cluster_secret.as_deref(),
            replica(2),
            replica(3),
            [7; 32],
            &request,
            record
        )
        .is_ok()
    );
}

struct Timing;

#[tokio::test(start_paused = true)]
async fn renewal_requires_validated_progress_not_a_caller_supplied_log_id() {
    use openraft::alias::{CommittedLeaderIdOf, LogIdOf};
    use openraft::vote::RaftLeaderId;
    let cfg = config();
    let controller = AdmissionController::new(
        cfg.clone(),
        replica(3),
        Instant::now() - Duration::from_secs(60),
        Arc::new(Timing),
    )
    .unwrap();
    let log = LogIdOf::<crate::raft::TypeConfig>::new(
        CommittedLeaderIdOf::<crate::raft::TypeConfig>::new(1, replica(3)),
        9,
    );
    for mode in [
        AdmissionMode::Join {
            prepared: Some(JoinBinding {
                log_id: log,
                plan_digest: [0; 32],
            }),
        },
        AdmissionMode::Renew {
            progress: Some(log),
        },
    ] {
        assert!(
            controller.begin(genesis(&cfg), mode).is_err(),
            "caller-supplied evidence must not bypass the round's nonce check"
        );
    }
}

struct ActiveFence {
    context: super::super::AdmissionContext,
    deadline: Instant,
}

impl super::super::AdmissionFence for ActiveFence {
    fn local_replica(&self) -> ReplicaId {
        self.context.local_replica
    }
    fn check(&self, expected: &super::super::AdmissionContext) -> Result<Instant, AdmissionDenied> {
        if expected != &self.context {
            return Err(AdmissionDenied("fixture context mismatch"));
        }
        Ok(self.deadline)
    }
}

#[tokio::test(start_paused = true)]
async fn original_cold_cohort_can_finish_after_one_member_activates() {
    use super::super::{AdmissionContext, AdmissionSession};
    let cfg = config();
    let local = replica(3);
    let mut issuer = AdmissionController::new(
        cfg.clone(),
        local,
        Instant::now() - Duration::from_secs(60),
        Arc::new(Timing),
    )
    .unwrap();
    let descriptor = genesis(&cfg);
    let request = AdmissionRequest {
        consumer: replica(2),
        genesis: descriptor.clone(),
        request_nonce: [8; 32],
        mode: AdmissionMode::Cold,
    };
    let signed = SignedAdmission::sign(
        cfg.cluster_secret.as_deref(),
        10,
        replica(2),
        local,
        [7; 32],
        request,
    )
    .unwrap();
    issuer.reserve(&signed, [7; 32], None).unwrap();
    let (_, _, state) = crate::raft::store::new_store(Arc::new(vec![]), 3, true, 0);
    let mut state = state.write().await;
    let context = AdmissionContext {
        local_replica: local,
        genesis: descriptor.clone(),
    };
    let fence = Arc::new(ActiveFence {
        context: context.clone(),
        deadline: Instant::now() + Duration::from_secs(5),
    });
    state
        .bind_admission(AdmissionSession::new(context, fence).unwrap())
        .unwrap();
    state.genesis = Some(descriptor);
    assert!(
        issuer.reserve(&signed, [7; 32], Some(&state)).is_ok(),
        "activation of one original member must not prevent the rest consenting to the same genesis"
    );
    tokio::time::advance(Duration::from_secs(5)).await;
    assert!(issuer.reserve(&signed, [7; 32], Some(&state)).is_err());
}

impl AdmissionTiming for Timing {
    fn consumer_deadline(&self, start: Instant) -> Result<Instant, AdmissionDenied> {
        Ok(start + Duration::from_secs(10))
    }
    fn reservation_deadline(&self, start: Instant) -> Result<Instant, AdmissionDenied> {
        Ok(start + Duration::from_secs(30))
    }
    fn quarantine_deadline(&self, start: Instant) -> Result<Instant, AdmissionDenied> {
        Ok(start + Duration::from_secs(60))
    }
}

fn replica(physical_id: u64) -> ReplicaId {
    ReplicaId {
        physical_id,
        boot_nonce: [physical_id as u8; 32],
    }
}

fn received(cfg: &Config, record: SignedAdmission<AdmissionRequest>) -> ReceivedGrant {
    ReceivedGrant::authenticate(
        cfg.cluster_secret.as_deref(),
        record.sender,
        record.recipient,
        [7; 32],
        &record.payload.clone(),
        record,
    )
    .unwrap()
}

fn config() -> Arc<Config> {
    Arc::new(serde_yaml::from_str(
        "node_id: 3\nraft_listen: '127.0.0.3:1000'\nclient_submit_listen: '127.0.0.3:2000'\npeers:\n  - {id: 1, raft_address: '127.0.0.1:1000', client_submit_address: '127.0.0.1:2000'}\n  - {id: 2, raft_address: '127.0.0.2:1000', client_submit_address: '127.0.0.2:2000'}\n  - {id: 3, raft_address: '127.0.0.3:1000', client_submit_address: '127.0.0.3:2000'}\nvips: []\nhealth: {command: [/bin/true], interval_ms: 1000, timeout_ms: 500}\ncluster_secret: admission-unit-test-secret-0123456789\ndry_run: true\n"
    ).unwrap())
}

fn genesis(cfg: &Config) -> Genesis {
    Genesis {
        config: cfg.cluster_config_fingerprint().unwrap(),
        epoch: u128::MAX,
        voters: BTreeSet::from([replica(2), replica(3)]),
    }
}

#[tokio::test(start_paused = true)]
async fn quarantine_and_reservation_precede_signed_cold_grant() {
    let cfg = config();
    let boot = Instant::now();
    let mut issuer =
        AdmissionController::new(cfg.clone(), replica(3), boot, Arc::new(Timing)).unwrap();
    let request = AdmissionRequest {
        consumer: replica(2),
        genesis: genesis(&cfg),
        request_nonce: [8; 32],
        mode: AdmissionMode::Cold,
    };
    let signed = SignedAdmission::sign(
        cfg.cluster_secret.as_deref(),
        10,
        replica(2),
        replica(3),
        [7; 32],
        request.clone(),
    )
    .unwrap();
    assert!(issuer.reserve(&signed, [7; 32], None).is_err());
    tokio::time::advance(Duration::from_secs(60)).await;
    let grant = issuer.reserve(&signed, [7; 32], None).unwrap();
    grant
        .verify(
            cfg.cluster_secret.as_deref(),
            11,
            replica(3),
            replica(2),
            [7; 32],
        )
        .unwrap();
    assert_eq!(
        issuer.reservation.as_ref().unwrap().genesis,
        request.genesis
    );
    assert_eq!(
        issuer.reservation.as_ref().unwrap().until,
        Instant::now() + Duration::from_secs(30)
    );
    let mut foreign = request;
    foreign.genesis.epoch -= 1;
    let foreign = SignedAdmission::sign(
        cfg.cluster_secret.as_deref(),
        10,
        replica(2),
        replica(3),
        [7; 32],
        foreign,
    )
    .unwrap();
    assert!(issuer.reserve(&foreign, [7; 32], None).is_err());
}

#[tokio::test(start_paused = true)]
async fn altered_or_replayed_channel_request_cannot_reserve() {
    let cfg = config();
    let mut issuer = AdmissionController::new(
        cfg.clone(),
        replica(3),
        Instant::now() - Duration::from_secs(60),
        Arc::new(Timing),
    )
    .unwrap();
    let request = AdmissionRequest {
        consumer: replica(2),
        genesis: genesis(&cfg),
        request_nonce: [8; 32],
        mode: AdmissionMode::Cold,
    };
    let mut signed = SignedAdmission::sign(
        cfg.cluster_secret.as_deref(),
        10,
        replica(2),
        replica(3),
        [7; 32],
        request,
    )
    .unwrap();
    assert!(issuer.reserve(&signed, [6; 32], None).is_err());
    signed.payload.request_nonce[31] ^= 1;
    assert!(issuer.reserve(&signed, [7; 32], None).is_err());
    assert!(issuer.reservation.is_none());
}

#[tokio::test(start_paused = true)]
async fn collected_quorum_is_physical_and_deadline_stays_at_request_start() {
    let cfg = config();
    let consumer = AdmissionController::new(
        cfg.clone(),
        replica(3),
        Instant::now() - Duration::from_secs(60),
        Arc::new(Timing),
    )
    .unwrap();
    let mut round = consumer.begin(genesis(&cfg), AdmissionMode::Cold).unwrap();
    let start = round.request_started();
    let grants: Vec<_> = [replica(2), replica(3)]
        .into_iter()
        .map(|issuer| {
            received(
                &cfg,
                SignedAdmission::sign(
                    cfg.cluster_secret.as_deref(),
                    11,
                    issuer,
                    replica(3),
                    [7; 32],
                    round.request().clone(),
                )
                .unwrap(),
            )
        })
        .collect();
    assert!(
        consumer
            .complete(&mut round, &[grants[0].clone(), grants[0].clone()])
            .is_err()
    );
    tokio::time::advance(Duration::from_secs(3)).await;
    let verified = consumer.complete(&mut round, &grants).unwrap();
    assert_eq!(verified.deadline(), start + Duration::from_secs(10));
    assert!(consumer.complete(&mut round, &grants).is_err());
    let mut late = consumer.begin(genesis(&cfg), AdmissionMode::Cold).unwrap();
    tokio::time::advance(Duration::from_secs(10)).await;
    assert!(consumer.complete(&mut late, &grants).is_err());
}

#[tokio::test(start_paused = true)]
async fn renewal_round_binds_commit_after_nonce_creation_and_rejects_foreign_context() {
    use crate::raft::admission::{AppliedHealthProgress, HealthProgress};
    let cfg = config();
    let consumer = AdmissionController::new(
        cfg.clone(),
        replica(3),
        Instant::now() - Duration::from_secs(60),
        Arc::new(Timing),
    )
    .unwrap();
    let mut round = consumer
        .begin(genesis(&cfg), AdmissionMode::Renew { progress: None })
        .unwrap();
    let started = round.request_started();
    let request = round.request().clone();
    let applied = AppliedHealthProgress {
        request: HealthProgress {
            node_id: 3,
            healthy: Some(true),
            replica: replica(3),
            epoch: request.genesis.epoch,
            request_nonce: request.request_nonce,
            genesis: request.genesis,
        },
        log_id: openraft::testing::log_id::<crate::raft::TypeConfig>(2, replica(3), 20),
    };
    let mut foreign = applied.clone();
    foreign.request.request_nonce[31] ^= 1;
    assert!(round.bind_progress(&foreign).is_err());
    foreign = applied.clone();
    foreign.request.genesis.epoch -= 1;
    foreign.request.epoch -= 1;
    assert!(round.bind_progress(&foreign).is_err());
    tokio::time::advance(Duration::from_secs(2)).await;
    round.bind_progress(&applied).unwrap();
    assert_eq!(round.request_started(), started);
    assert_eq!(round.request().request_nonce, applied.request.request_nonce);
    assert!(round.bind_progress(&applied).is_err());
    let encoded = serde_json::to_vec(round.request()).unwrap();
    let decoded: AdmissionRequest = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(decoded, *round.request());
    let signed = SignedAdmission::sign(
        cfg.cluster_secret.as_deref(),
        10,
        replica(3),
        replica(2),
        [7; 32],
        decoded,
    )
    .unwrap();
    let bytes = serde_json::to_vec(&signed).unwrap();
    let decoded: SignedAdmission<AdmissionRequest> = serde_json::from_slice(&bytes).unwrap();
    decoded
        .verify(
            cfg.cluster_secret.as_deref(),
            10,
            replica(3),
            replica(2),
            [7; 32],
        )
        .unwrap();
    assert_eq!(decoded.payload.genesis.epoch, u128::MAX);
}

#[tokio::test(start_paused = true)]
async fn unsigned_evidence_cannot_complete_an_unprepared_join_or_renewal() {
    let cfg = config();
    let consumer = AdmissionController::new(
        cfg.clone(),
        replica(3),
        Instant::now() - Duration::from_secs(60),
        Arc::new(Timing),
    )
    .unwrap();
    for mode in [
        AdmissionMode::Join { prepared: None },
        AdmissionMode::Renew { progress: None },
    ] {
        let mut round = consumer.begin(genesis(&cfg), mode).unwrap();
        let grants: Vec<_> = [replica(2), replica(3)]
            .into_iter()
            .map(|issuer| {
                received(
                    &cfg,
                    SignedAdmission::sign(
                        cfg.cluster_secret.as_deref(),
                        11,
                        issuer,
                        replica(3),
                        [7; 32],
                        round.request().clone(),
                    )
                    .unwrap(),
                )
            })
            .collect();
        assert!(
            consumer.complete(&mut round, &grants).is_err(),
            "missing committed evidence granted permission"
        );
    }
}

fn prepared_join(cfg: &Config) -> super::super::PreparedJoin {
    let mut previous_voters = genesis(cfg).voters;
    previous_voters.remove(&replica(3));
    previous_voters.insert(replica(1));
    super::super::PreparedJoin {
        plan: super::super::JoinPlan {
            genesis: genesis(cfg),
            consumer: replica(3),
            request_nonce: [19; 32],
            previous_voters,
            next_voters: [replica(1), replica(2), replica(3)].into(),
        },
        prepared: openraft::testing::log_id::<crate::raft::TypeConfig>(2, replica(2), 20),
        learner_applied: None,
    }
}

#[tokio::test(start_paused = true)]
async fn fresh_join_challenges_bind_the_same_immutable_operation() {
    let cfg = config();
    let controller = AdmissionController::new(
        cfg.clone(),
        replica(3),
        Instant::now() - Duration::from_secs(60),
        Arc::new(Timing),
    )
    .unwrap();
    let prepared = prepared_join(&cfg);
    let mut first = controller
        .begin(genesis(&cfg), AdmissionMode::Join { prepared: None })
        .unwrap();
    let first_nonce = first.request().request_nonce;
    assert_ne!(first_nonce, prepared.plan.request_nonce);
    first
        .bind_prepared(&prepared)
        .expect("fresh challenge must bind a committed operation nonce");
    let grants_for = |round: &AdmissionRound| {
        [replica(1), replica(2)].map(|issuer| {
            received(
                &cfg,
                SignedAdmission::sign(
                    cfg.cluster_secret.as_deref(),
                    11,
                    issuer,
                    replica(3),
                    [7; 32],
                    round.request().clone(),
                )
                .unwrap(),
            )
        })
    };
    let old_grants = grants_for(&first);
    tokio::time::advance(Duration::from_secs(10)).await;
    assert!(controller.complete(&mut first, &[]).is_err());
    let mut retry = controller
        .begin(genesis(&cfg), AdmissionMode::Join { prepared: None })
        .unwrap();
    assert_ne!(retry.request().request_nonce, first_nonce);
    retry.bind_prepared(&prepared).unwrap();
    assert_eq!(retry.request().mode, first.request().mode);
    assert!(controller.complete(&mut retry, &old_grants).is_err());
    let fresh_grants = grants_for(&retry);
    let verified = controller.complete(&mut retry, &fresh_grants).unwrap();
    assert_eq!(verified.request_nonce(), retry.request().request_nonce);
    assert_eq!(
        verified.deadline(),
        retry.request_started() + Duration::from_secs(10)
    );
    for change in ["consumer", "genesis", "voters"] {
        let mut wrong = prepared.clone();
        match change {
            "consumer" => wrong.plan.consumer.boot_nonce[0] ^= 1,
            "genesis" => wrong.plan.genesis.epoch -= 1,
            _ => {
                wrong.plan.next_voters.remove(&replica(1));
            }
        }
        let mut round = controller
            .begin(genesis(&cfg), AdmissionMode::Join { prepared: None })
            .unwrap();
        assert!(round.bind_prepared(&wrong).is_err(), "{change}");
    }
}

#[tokio::test(start_paused = true)]
async fn join_grant_uses_operation_barrier_not_challenge_nonce_and_rejects_voters() {
    use super::super::{AdmissionContext, AdmissionSession};
    let cfg = config();
    let mut issuer = AdmissionController::new(
        cfg.clone(),
        replica(3),
        Instant::now() - Duration::from_secs(60),
        Arc::new(Timing),
    )
    .unwrap();
    let mut operation = prepared_join(&cfg);
    operation.plan.consumer = replica(1);
    operation.plan.previous_voters = [replica(2), replica(3)].into();
    let request = AdmissionRequest {
        consumer: replica(1),
        genesis: genesis(&cfg),
        request_nonce: [22; 32],
        mode: AdmissionMode::Join {
            prepared: Some(JoinBinding::from_prepared(&operation)),
        },
    };
    let sign = |request| {
        SignedAdmission::sign(
            cfg.cluster_secret.as_deref(),
            10,
            replica(1),
            replica(3),
            [7; 32],
            request,
        )
        .unwrap()
    };
    let signed = sign(request.clone());
    let (_, _, state) = crate::raft::store::new_store(Arc::new(vec![]), 3, true, 0);
    let mut state = state.write().await;
    let context = AdmissionContext {
        local_replica: replica(3),
        genesis: genesis(&cfg),
    };
    let fence = Arc::new(ActiveFence {
        context: context.clone(),
        deadline: Instant::now() + Duration::from_secs(30),
    });
    state
        .bind_admission(AdmissionSession::new(context, fence).unwrap())
        .unwrap();
    state.genesis = Some(genesis(&cfg));
    state.prepared_join = Some(operation.clone());
    state.last_membership = openraft::StoredMembership::new(
        Some(operation.prepared),
        openraft::Membership::new_with_defaults(
            vec![operation.plan.previous_voters.clone()],
            operation.plan.next_voters.clone(),
        ),
    );
    assert!(issuer.reserve(&signed, [7; 32], Some(&state)).is_ok());
    let mut substituted = operation.clone();
    substituted.plan.request_nonce[0] ^= 1;
    state.prepared_join = Some(substituted);
    assert!(
        issuer.reserve(&signed, [7; 32], Some(&state)).is_err(),
        "same log id must not authorize a different committed operation plan"
    );
    state.prepared_join = Some(operation.clone());
    let mut stale = request.clone();
    let mut other_log = operation.prepared;
    other_log.index += 1;
    stale.mode = AdmissionMode::Join {
        prepared: Some(JoinBinding {
            log_id: other_log,
            ..JoinBinding::from_prepared(&operation)
        }),
    };
    assert!(issuer.reserve(&sign(stale), [7; 32], Some(&state)).is_err());
    state.last_membership = openraft::StoredMembership::new(
        Some(other_log),
        openraft::Membership::new_with_defaults(
            vec![[replica(2)].into()],
            operation.plan.next_voters.clone(),
        ),
    );
    assert!(issuer.reserve(&signed, [7; 32], Some(&state)).is_err());
    state.last_membership = openraft::StoredMembership::new(
        Some(other_log),
        openraft::Membership::new_with_defaults(
            vec![operation.plan.next_voters.clone()],
            operation.plan.next_voters.clone(),
        ),
    );
    assert!(issuer.reserve(&signed, [7; 32], Some(&state)).is_err());
    state.prepared_join = None;
    assert!(issuer.reserve(&signed, [7; 32], Some(&state)).is_err());
}
