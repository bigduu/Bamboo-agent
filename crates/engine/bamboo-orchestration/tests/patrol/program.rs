//! Reuse the existing controlled ports; no filesystem lock or provider runtime.
use super::*;
use bamboo_orchestration::patrol::{checkpoint, program::ProgramPatrol};

fn program() -> ProgramPatrol {
    ProgramPatrol::new(policy()).unwrap()
}

fn pass(checkpoints: &mut FakeHost, observations: &mut FakeHost, now: u64) -> Report {
    program()
        .tick(&scope(), &FakeClock(now), checkpoints, observations)
        .unwrap()
}

fn only_program_calls(checkpoints: &FakeHost, observations: &FakeHost) {
    assert!(checkpoints
        .durable
        .borrow()
        .calls
        .iter()
        .all(|call| *call == "save"));
    assert!(observations
        .durable
        .borrow()
        .calls
        .iter()
        .all(|call| *call == "observe"));
    for host in [checkpoints, observations] {
        let durable = host.durable.borrow();
        assert!(durable.charges.is_empty());
        assert!(durable.queue.is_empty());
    }
}

#[test]
fn healthy_program_needs_no_semantic_capabilities() {
    let mut checkpoints = FakeHost::new();
    let mut observations = FakeHost::new();
    observations.observation.progress.as_mut().unwrap().at_ms = 95;
    assert_eq!(
        pass(&mut checkpoints, &mut observations, 100).status,
        Status::Healthy
    );
    assert!(checkpoints.checkpoint().incident.is_none());
    only_program_calls(&checkpoints, &observations);
}

#[test]
fn expected_user_wait_uses_the_validated_deadline() {
    let mut checkpoints = FakeHost::new();
    let mut observations = FakeHost::new();
    observations.observation.expected_wait = Some(ExpectedWait {
        kind: WaitKind::User,
        issued_at_ms: 90,
        until_ms: 105,
        progress_sequence: 1,
        evidence_ref: "canonical-user-wait".into(),
    });
    let report = pass(&mut checkpoints, &mut observations, 100);
    assert_eq!(report.status, Status::ExpectedWait);
    assert_eq!(report.next_check_ms, 105);
    only_program_calls(&checkpoints, &observations);
}

#[test]
fn missing_measurements_keep_one_pending_undelivered_incident() {
    let mut checkpoints = FakeHost::new();
    let mut observations = FakeHost::new();
    observations.observation.started_at_ms = None;
    observations.observation.heartbeat_at_ms = None;
    observations.observation.progress = None;
    assert_eq!(
        pass(&mut checkpoints, &mut observations, 100).status,
        Status::Incident
    );
    let id = checkpoints.incident().id;
    for now in [101, 200, 1000] {
        assert_eq!(
            pass(&mut checkpoints, &mut observations, now).status,
            Status::NeedsSupervisor
        );
        let incident = checkpoints.incident();
        assert_eq!(incident.id, id);
        assert_eq!(incident.reason, Reason::EvidenceUnknown);
        assert_eq!(incident.attempts, 0);
        let escalation = incident.escalation.unwrap();
        assert_eq!(escalation.operation.incident, id);
        assert_eq!(escalation.reason, SupervisorReason::DeterministicAnomaly);
        assert_eq!(escalation.deliveries, 0);
        assert!(!escalation.queued);
    }
    only_program_calls(&checkpoints, &observations);
}

#[test]
fn fresh_heartbeat_does_not_count_as_progress_or_recovery_ack() {
    let mut checkpoints = FakeHost::new();
    let mut observations = FakeHost::new();
    pass(&mut checkpoints, &mut observations, 100);
    let id = checkpoints.incident().id;
    for now in [101, 102, 103] {
        observations.fresh_heartbeat(now);
        assert_eq!(
            pass(&mut checkpoints, &mut observations, now).status,
            Status::NeedsSupervisor
        );
        assert_eq!(checkpoints.incident().id, id);
        assert_eq!(checkpoints.incident().reason, Reason::NoProgress);
        assert!(!matches!(
            checkpoints.incident().phase,
            Phase::Resolved { .. }
        ));
    }
    only_program_calls(&checkpoints, &observations);
}

#[test]
fn unknown_execution_requests_original_receipt_reconciliation_without_delivery() {
    let mut checkpoints = FakeHost::new();
    let mut observations = FakeHost::new();
    observations.observation.execution_unknown = true;
    pass(&mut checkpoints, &mut observations, 100);
    pass(&mut checkpoints, &mut observations, 101);
    let incident = checkpoints.incident();
    assert_eq!(incident.phase, Phase::Blocked(Block::UnknownUsage));
    let escalation = incident.escalation.unwrap();
    assert_eq!(escalation.reason, SupervisorReason::ReconcileReceipt);
    assert_eq!(escalation.deliveries, 0);
    assert!(!escalation.queued);
    only_program_calls(&checkpoints, &observations);
}

#[test]
fn meaningful_progress_resolves_program_incident_without_ticket_mutation() {
    let mut checkpoints = FakeHost::new();
    let mut observations = FakeHost::new();
    pass(&mut checkpoints, &mut observations, 100);
    pass(&mut checkpoints, &mut observations, 101);
    observations.observation.progress = Some(Progress {
        sequence: 2,
        at_ms: 102,
    });
    observations.fresh_heartbeat(102);
    assert_eq!(
        pass(&mut checkpoints, &mut observations, 102).status,
        Status::Resolved
    );
    assert!(matches!(
        checkpoints.incident().phase,
        Phase::Resolved { at_ms: 102, .. }
    ));
    assert_eq!(checkpoints.incident().escalation.unwrap().deliveries, 0);
    only_program_calls(&checkpoints, &observations);
}

#[test]
fn progress_while_execution_is_unknown_does_not_resolve_incident() {
    let mut checkpoints = FakeHost::new();
    let mut observations = FakeHost::new();
    observations.observation.execution_unknown = true;
    pass(&mut checkpoints, &mut observations, 100);
    observations.observation.progress = Some(Progress {
        sequence: 2,
        at_ms: 101,
    });
    pass(&mut checkpoints, &mut observations, 101);
    assert!(!matches!(
        checkpoints.incident().phase,
        Phase::Resolved { .. }
    ));
    only_program_calls(&checkpoints, &observations);
}

#[test]
fn event_gap_requests_snapshot_without_replacing_the_original_cursor() {
    let mut checkpoints = FakeHost::new();
    let mut observations = FakeHost::new();
    pass(&mut checkpoints, &mut observations, 100);
    let before = checkpoints.checkpoint();
    observations.observation.kind = ObservationKind::Delta { previous_cursor: 9 };
    observations.observation.cursor = 12;
    observations.observation.source_revision = 12;
    assert_eq!(
        pass(&mut checkpoints, &mut observations, 101).status,
        Status::ResyncRequired
    );
    let after = checkpoints.checkpoint();
    assert_eq!(after.cursor, before.cursor);
    assert_eq!(after.incident, before.incident);
    only_program_calls(&checkpoints, &observations);
}

#[test]
fn rejected_current_authority_never_publishes_a_checkpoint() {
    let mut checkpoints = FakeHost::new();
    let mut observations = FakeHost::new();
    observations.denied = true;
    assert_eq!(
        program().tick(
            &scope(),
            &FakeClock(100),
            &mut checkpoints,
            &mut observations
        ),
        Err(Error::Port(PortError::Denied))
    );
    assert!(checkpoints.durable.borrow().checkpoints.is_empty());
    assert_eq!(checkpoints.durable.borrow().save_count, 0);
}

#[test]
fn stale_generation_or_project_observation_is_not_a_recovered_grant() {
    for changed in [0, 1] {
        let mut checkpoints = FakeHost::new();
        let mut observations = FakeHost::new();
        if changed == 0 {
            observations.observation.scope.generation += 1;
        } else {
            observations.observation.scope.project_id = "foreign-project".into();
        }
        assert_eq!(
            program().tick(
                &scope(),
                &FakeClock(100),
                &mut checkpoints,
                &mut observations
            ),
            Err(Error::InvalidObservation)
        );
        assert!(checkpoints.durable.borrow().checkpoints.is_empty());
    }
}

#[test]
fn conflicted_cas_has_no_delivery_or_attempt_side_effect() {
    let mut checkpoints = FakeHost::new();
    let mut observations = FakeHost::new();
    pass(&mut checkpoints, &mut observations, 100);
    let before = checkpoints.incident();
    checkpoints.fail_save = Some(checkpoints.durable.borrow().save_count + 1);
    assert_eq!(
        program().tick(
            &scope(),
            &FakeClock(101),
            &mut checkpoints,
            &mut observations
        ),
        Err(Error::Port(PortError::Conflict))
    );
    assert_eq!(checkpoints.incident(), before);
    checkpoints.fail_save = None;
    pass(&mut checkpoints, &mut observations, 102);
    assert_eq!(checkpoints.incident().id, before.id);
    assert_eq!(checkpoints.incident().escalation.unwrap().deliveries, 0);
    only_program_calls(&checkpoints, &observations);
}

#[test]
fn restarted_diagnostic_retains_exact_request_charge_and_operation() {
    let mut checkpoints = FakeHost::new();
    let mut provider = ControlledProvider::new(&checkpoints);
    tick(&configured(), &mut checkpoints, &mut provider, 100);
    tick(&configured(), &mut checkpoints, &mut provider, 101);
    let before = checkpoints.checkpoint();
    assert!(matches!(
        before.incident.as_ref().unwrap().phase,
        Phase::Diagnosing { .. }
    ));
    assert_eq!(checkpoints.usage(), 120);
    let stored = checkpoints.durable.borrow().checkpoints[&scope()].clone();
    let recovered = checkpoint::decode(
        &scope(),
        &checkpoint::encode(stored.version, &stored.checkpoint).unwrap(),
    )
    .unwrap();
    checkpoints
        .durable
        .borrow_mut()
        .checkpoints
        .insert(scope(), recovered);
    let mut observations = FakeHost::new();
    observations.observation.progress = Some(Progress {
        sequence: 2,
        at_ms: 500,
    });
    observations.fresh_heartbeat(500);
    assert_eq!(
        pass(&mut checkpoints, &mut observations, 500).status,
        Status::NeedsSupervisor
    );
    let after = checkpoints.checkpoint();
    assert_eq!(after.incident, before.incident);
    checkpoint::validate_update(&before, &after).unwrap();
    assert_eq!(checkpoints.usage(), 120);
    assert_eq!(provider.submits.len(), 1);
    assert!(provider.polls.is_empty());
}

#[test]
fn unavailable_settlement_adapter_keeps_known_usage_and_original_reservation() {
    let mut checkpoints = FakeHost::new();
    let mut provider = ControlledProvider::new(&checkpoints);
    tick(&configured(), &mut checkpoints, &mut provider, 100);
    tick(&configured(), &mut checkpoints, &mut provider, 101);
    checkpoints.fail_settle = true;
    provider.finish(Advice::Observe);
    tick(&configured(), &mut checkpoints, &mut provider, 102);
    let before = checkpoints.checkpoint();
    assert_eq!(
        before.incident.as_ref().unwrap().pending_settlement_usage,
        Some(Usage::Known {
            input_tokens: 40,
            output_tokens: 10
        })
    );
    let charges = checkpoints.usage();
    let original_calls = checkpoints.durable.borrow().calls.clone();
    let mut observations = FakeHost::new();
    assert_eq!(
        pass(&mut checkpoints, &mut observations, 200).status,
        Status::NeedsSupervisor
    );
    let after = checkpoints.checkpoint();
    assert_eq!(after.incident, before.incident);
    checkpoint::validate_update(&before, &after).unwrap();
    assert_eq!(checkpoints.usage(), charges);
    assert_eq!(
        &checkpoints.durable.borrow().calls[..original_calls.len()],
        original_calls.as_slice()
    );
    assert!(checkpoints.durable.borrow().calls[original_calls.len()..]
        .iter()
        .all(|call| *call == "save"));
}

#[test]
fn program_does_not_consume_a_previous_failed_delivery_or_forge_an_ack() {
    let mut checkpoints = FakeHost::new();
    let mut observations = FakeHost::new();
    pass(&mut checkpoints, &mut observations, 100);
    pass(&mut checkpoints, &mut observations, 101);
    {
        let mut state = checkpoints.durable.borrow_mut();
        let incident = state
            .checkpoints
            .get_mut(&scope())
            .unwrap()
            .checkpoint
            .incident
            .as_mut()
            .unwrap();
        let escalation = incident.escalation.as_mut().unwrap();
        escalation.deliveries = 1;
        escalation.next_delivery_ms = 110;
    }
    let before = checkpoints.incident().escalation;
    pass(&mut checkpoints, &mut observations, 200);
    assert_eq!(checkpoints.incident().escalation, before);
    only_program_calls(&checkpoints, &observations);
}
