use bamboo_orchestration::patrol::{delivery, *};

struct Time(u64);
impl Clock for Time {
    fn now_ms(&self) -> u64 {
        self.0
    }
}

struct Journal {
    stored: StoredCheckpoint,
    reject_save: bool,
}
impl CheckpointPort for Journal {
    fn load(&mut self, _: &Scope) -> Result<Option<StoredCheckpoint>, PortError> {
        Ok(Some(self.stored.clone()))
    }
    fn save(&mut self, version: Option<u64>, next: &Checkpoint) -> Result<u64, PortError> {
        if self.reject_save || version != Some(self.stored.version) {
            return Err(PortError::Conflict);
        }
        checkpoint::validate_update(&self.stored.checkpoint, next)?;
        self.stored = StoredCheckpoint {
            version: self.stored.version + 1,
            checkpoint: next.clone(),
        };
        Ok(self.stored.version)
    }
}

fn fixture() -> (Scope, Policy, Journal) {
    let scope = Scope {
        project_id: "p".into(),
        work_id: "w".into(),
        assignment_id: "a".into(),
        generation: 1,
        authority_epoch: 1,
    };
    let id = IncidentId {
        scope: scope.clone(),
        progress_sequence: 0,
    };
    let operation = OperationId {
        incident: id.clone(),
        kind: OperationKind::Escalation,
    };
    let cp = Checkpoint {
        scope: scope.clone(),
        cursor: 3,
        source_revision: 3,
        last_check_ms: 100,
        next_check_ms: 200,
        incident: Some(Incident {
            id,
            reason: Reason::ExecutionUnknown,
            opened_at_ms: 100,
            opened_cursor: 3,
            source_revision: 3,
            attempts: 0,
            last_diagnostic_cursor: 3,
            last_diagnostic_evidence_revision: None,
            last_diagnosis: None,
            pending_settlement: None,
            pending_settlement_usage: None,
            phase: Phase::Blocked(Block::UnknownUsage),
            escalation: Some(Escalation {
                operation,
                reason: SupervisorReason::ReconcileReceipt,
                source_revision: 3,
                cursor: 3,
                deliveries: 0,
                queued: false,
                next_delivery_ms: 100,
            }),
        }),
    };
    let policy = Policy {
        cooldown_ms: 100,
        max_escalation_deliveries: 2,
        ..Policy::default()
    };
    (
        scope,
        policy,
        Journal {
            stored: StoredCheckpoint {
                version: 1,
                checkpoint: cp,
            },
            reject_save: false,
        },
    )
}

#[test]
fn attempts_are_durable_before_delivery_and_cooldown_preserves_original_identity() {
    let (scope, policy, mut journal) = fixture();
    let first = delivery::prepare(&scope, &policy, &Time(100), &mut journal)
        .unwrap()
        .unwrap();
    assert!(first.may_enqueue);
    assert_eq!(
        journal
            .stored
            .checkpoint
            .incident
            .as_ref()
            .unwrap()
            .escalation
            .as_ref()
            .unwrap()
            .deliveries,
        1
    );
    let operation = first.escalation.operation.clone();
    // Simulate disappearance before enqueue or ACK: reopen the exact bytes.
    journal.stored = serde_json::from_slice(&serde_json::to_vec(&journal.stored).unwrap()).unwrap();
    let cooldown = delivery::prepare(&scope, &policy, &Time(101), &mut journal)
        .unwrap()
        .unwrap();
    assert!(!cooldown.may_enqueue);
    assert_eq!(cooldown.escalation.operation, operation);
    let retry = delivery::prepare(&scope, &policy, &Time(200), &mut journal)
        .unwrap()
        .unwrap();
    assert!(retry.may_enqueue);
    assert_eq!(retry.escalation.operation, operation);
    let exhausted = delivery::prepare(&scope, &policy, &Time(300), &mut journal)
        .unwrap()
        .unwrap();
    assert!(!exhausted.may_enqueue);
    assert_eq!(exhausted.escalation.deliveries, 2);
    // A receipt from the last enqueue can still be reconciled after exhaustion.
    exhausted.acknowledge(&mut journal).unwrap();
    assert!(delivery::prepare(&scope, &policy, &Time(301), &mut journal)
        .unwrap()
        .is_none());
}

#[test]
fn failed_cas_cannot_authorize_delivery_or_acknowledge_a_replaced_checkpoint() {
    let (scope, policy, mut journal) = fixture();
    journal.reject_save = true;
    assert!(delivery::prepare(&scope, &policy, &Time(100), &mut journal).is_err());
    assert_eq!(journal.stored.version, 1);
    journal.reject_save = false;
    let pending = delivery::prepare(&scope, &policy, &Time(100), &mut journal)
        .unwrap()
        .unwrap();
    journal.stored.version += 1;
    assert!(pending.acknowledge(&mut journal).is_err());
    assert!(
        !journal
            .stored
            .checkpoint
            .incident
            .as_ref()
            .unwrap()
            .escalation
            .as_ref()
            .unwrap()
            .queued
    );
}

#[test]
fn resolved_or_acked_incidents_never_enqueue_and_clock_regression_is_rejected() {
    let (scope, policy, mut journal) = fixture();
    assert!(matches!(
        delivery::prepare(&scope, &policy, &Time(99), &mut journal),
        Err(Error::ClockRegression)
    ));
    journal.stored.checkpoint.incident.as_mut().unwrap().phase = Phase::Resolved {
        at_ms: 100,
        cursor: 3,
    };
    assert!(delivery::prepare(&scope, &policy, &Time(100), &mut journal)
        .unwrap()
        .is_none());
    journal.stored.checkpoint.incident.as_mut().unwrap().phase = Phase::Open;
    delivery::prepare(&scope, &policy, &Time(100), &mut journal)
        .unwrap()
        .unwrap()
        .acknowledge(&mut journal)
        .unwrap();
    assert!(
        delivery::prepare(&scope, &policy, &Time(1000), &mut journal)
            .unwrap()
            .is_none()
    );
}
