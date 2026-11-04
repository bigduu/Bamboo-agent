use super::*;
use bamboo_orchestration::patrol::{
    Advice, Checkpoint, Diagnosis, DiagnosticRecord, Incident, IncidentId, OperationId,
    OperationKind, Reason,
};

fn stored() -> StoredCheckpoint {
    let scope = Scope {
        project_id: "project".into(),
        work_id: "work".into(),
        assignment_id: "assignment".into(),
        generation: 1,
        authority_epoch: 1,
    };
    let id = IncidentId {
        scope: scope.clone(),
        progress_sequence: 0,
    };
    StoredCheckpoint {
        version: 1,
        checkpoint: Checkpoint {
            scope,
            cursor: 4,
            source_revision: 4,
            last_check_ms: 100,
            next_check_ms: 200,
            incident: Some(Incident {
                id: id.clone(),
                reason: Reason::EvidenceUnknown,
                opened_at_ms: 100,
                opened_cursor: 4,
                source_revision: 4,
                attempts: 1,
                last_diagnostic_cursor: 4,
                last_diagnostic_evidence_revision: Some(4),
                pending_settlement: None,
                pending_settlement_usage: None,
                phase: Phase::Open,
                escalation: None,
                last_diagnosis: Some(DiagnosticRecord {
                    operation: OperationId {
                        incident: id,
                        kind: OperationKind::Diagnostic(1),
                    },
                    source_revision: 4,
                    through_cursor: 4,
                    at_ms: 99,
                    diagnosis: Diagnosis {
                        advice: Advice::Observe,
                        evidence: vec!["PRIVATE_EVIDENCE_BODY".into()],
                        uncertain: true,
                        note: "PRIVATE_MODEL_NOTE".into(),
                    },
                }),
            }),
        },
    }
}

#[test]
fn saved_summary_uses_execution_facts_and_never_serializes_diagnosis_bodies() {
    let (state, value) = summary(stored(), 5, 4).unwrap();
    assert_eq!(state, "available");
    assert_eq!(value["execution_facts_match"], true);
    assert_eq!(value["incident"]["last_diagnosis_at_ms"], 99);
    let encoded = value.to_string();
    assert!(!encoded.contains("PRIVATE_"));
    assert_eq!(summary(stored(), 5, 5).unwrap().0, "stale");
    let mut blocked = stored();
    blocked.checkpoint.incident.as_mut().unwrap().phase =
        Phase::Blocked(bamboo_orchestration::patrol::Block::Budget(
            bamboo_orchestration::patrol::BudgetBlock::Unavailable,
        ));
    let (_, blocked) = summary(blocked, 4, 4).unwrap();
    assert_eq!(
        blocked["incident"]["blocked_reason"],
        serde_json::json!({"budget":"unavailable"})
    );
    assert!(matches!(
        summary(stored(), 3, 4),
        Err(Error::RevisionConflict)
    ));
    assert!(matches!(
        summary(stored(), 5, 3),
        Err(Error::RevisionConflict)
    ));
    let mut empty = stored();
    empty.checkpoint.incident = None;
    assert_eq!(
        summary(empty, 4, 4).unwrap().0,
        "available",
        "no incident is not a runtime Healthy claim"
    );
}

#[test]
fn diagnosing_phase_is_a_label_without_request_or_reservation_serialization() {
    use bamboo_orchestration::patrol::{DiagnosticInput, DiagnosticRequest, Model, Reservation};
    let mut stored = stored();
    let incident = stored.checkpoint.incident.as_mut().unwrap();
    let operation = OperationId {
        incident: incident.id.clone(),
        kind: OperationKind::Diagnostic(1),
    };
    let request = DiagnosticRequest {
        operation: operation.clone(),
        model: Model {
            provider: "PRIVATE_PROVIDER".into(),
            model: "PRIVATE_MODEL".into(),
            max_input_tokens: 10,
            max_output_tokens: 10,
            max_evidence_bytes: 100,
            max_evidence_refs: 1,
        },
        input: DiagnosticInput {
            scope: stored.checkpoint.scope.clone(),
            source_revision: 4,
            since_cursor: 4,
            through_cursor: 4,
            summary: "PRIVATE_REQUEST".into(),
            references: vec![],
            input_tokens: 1,
        },
        deadline_ms: 150,
    };
    let reservation = Reservation {
        operation,
        request: Box::new(request.clone()),
        receipt: "PRIVATE_RECEIPT".into(),
    };
    incident.pending_settlement = Some(reservation.clone());
    incident.phase = Phase::Diagnosing {
        request: Box::new(request),
        reservation,
    };
    let (_, value) = summary(stored, 4, 4).unwrap();
    assert_eq!(value["incident"]["phase"], "diagnosing");
    assert_eq!(value["incident"]["accounting_pending"], true);
    assert!(!value.to_string().contains("PRIVATE_"));
}
