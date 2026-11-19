//! Private v2 extension of the existing checkpoint file, lock and publication.
//! Public Checkpoint/CheckpointPort and the exact legacy v1 codec stay intact.
#[cfg(unix)]
use bamboo_orchestration::patrol::{checkpoint, Scope, StoredCheckpoint};
use bamboo_orchestration::patrol::{Checkpoint, Escalation, OperationId, PortError};
use bamboo_tickets::TicketService;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NoticeSource {
    pub operation: OperationId,
    pub commit: String,
    pub cursor: u64,
    pub execution_facts_revision: u64,
}

impl NoticeSource {
    pub(super) fn validate(&self, checkpoint: &Checkpoint) -> Result<(), PortError> {
        let escalation = checkpoint
            .incident
            .as_ref()
            .and_then(|incident| incident.escalation.as_ref())
            .ok_or(PortError::Conflict)?;
        self.matches(escalation)?;
        if self.operation.incident.scope != checkpoint.scope {
            return Err(PortError::Denied);
        }
        Ok(())
    }

    pub(super) fn matches(&self, escalation: &Escalation) -> Result<(), PortError> {
        if self.operation != escalation.operation
            || self.cursor != escalation.cursor
            || self.execution_facts_revision != escalation.source_revision
            || self.cursor == 0
            || self.execution_facts_revision == 0
            || self.execution_facts_revision > self.cursor
            || self.commit.len() != 64
            || !self.commit.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(PortError::Conflict);
        }
        Ok(())
    }

    /// Only the writer observing the new escalation transition calls this.
    /// A same-facts budget commit still changes seq/HEAD and must not substitute
    /// its commit for the observation that opened this exact escalation.
    pub(super) fn capture(
        service: &TicketService,
        checkpoint: &Checkpoint,
    ) -> Result<Self, PortError> {
        let escalation = checkpoint
            .incident
            .as_ref()
            .and_then(|incident| incident.escalation.as_ref())
            .ok_or(PortError::Conflict)?;
        let (commit, snapshot) = service.published().map_err(|_| PortError::Unavailable)?;
        if snapshot.seq != escalation.cursor
            || snapshot.execution_facts_revision() != escalation.source_revision
        {
            return Err(PortError::Conflict);
        }
        let source = Self {
            operation: escalation.operation.clone(),
            commit,
            cursor: snapshot.seq,
            execution_facts_revision: snapshot.execution_facts_revision(),
        };
        source.validate(checkpoint)?;
        Ok(source)
    }
}

pub(super) fn new_escalation(previous: Option<&Checkpoint>, next: &Checkpoint) -> bool {
    let operation = |checkpoint: &Checkpoint| {
        checkpoint
            .incident
            .as_ref()
            .and_then(|incident| incident.escalation.as_ref())
            .map(|escalation| escalation.operation.clone())
    };
    operation(next).is_some() && previous.and_then(operation) != operation(next)
}

#[cfg(unix)]
pub(super) struct Record {
    pub stored: StoredCheckpoint,
    pub source: Option<NoticeSource>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg(unix)]
struct Envelope {
    format: String,
    schema: u32,
    record: StoredCheckpoint,
    source: NoticeSource,
    sha256: String,
}

#[cfg(unix)]
fn digest(record: &StoredCheckpoint, source: &NoticeSource) -> Result<String, PortError> {
    Ok(bamboo_tickets::content_hash(
        &bamboo_tickets::canonical_bytes(&serde_json::json!({
            "record": record, "source": source
        }))
        .map_err(|_| PortError::Unavailable)?,
    ))
}

#[cfg(unix)]
pub(super) fn encode(record: &Record) -> Result<Vec<u8>, PortError> {
    // Reuse all original public checkpoint validation and size limits.
    let legacy = checkpoint::encode(record.stored.version, &record.stored.checkpoint)?;
    let Some(source) = &record.source else {
        return Ok(legacy);
    };
    source.validate(&record.stored.checkpoint)?;
    let envelope = Envelope {
        format: "bamboo/patrol-checkpoint".into(),
        schema: 2,
        record: record.stored.clone(),
        source: source.clone(),
        sha256: digest(&record.stored, source)?,
    };
    let bytes = bamboo_tickets::canonical_bytes(&envelope).map_err(|_| PortError::Unavailable)?;
    if bytes.len() > checkpoint::MAX_CHECKPOINT_BYTES {
        return Err(PortError::Unavailable);
    }
    Ok(bytes)
}

#[cfg(unix)]
pub(super) fn decode(scope: &Scope, bytes: &[u8]) -> Result<Record, PortError> {
    if bytes.is_empty() || bytes.len() > checkpoint::MAX_CHECKPOINT_BYTES {
        return Err(PortError::Unavailable);
    }
    let value: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|_| PortError::Unavailable)?;
    match value["schema"].as_u64() {
        Some(1) => Ok(Record {
            stored: checkpoint::decode(scope, bytes)?,
            source: None,
        }),
        Some(2) => {
            let envelope: Envelope =
                serde_json::from_value(value).map_err(|_| PortError::Unavailable)?;
            if envelope.format != "bamboo/patrol-checkpoint"
                || &envelope.record.checkpoint.scope != scope
                || digest(&envelope.record, &envelope.source)? != envelope.sha256
            {
                return Err(PortError::Unavailable);
            }
            checkpoint::encode(envelope.record.version, &envelope.record.checkpoint)?;
            envelope.source.validate(&envelope.record.checkpoint)?;
            Ok(Record {
                stored: envelope.record,
                source: Some(envelope.source),
            })
        }
        _ => Err(PortError::Unavailable),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_orchestration::patrol::{
        Incident, IncidentId, OperationKind, Phase, Reason, Scope, SupervisorReason,
    };
    use bamboo_tickets::{
        Authority, Contract, Operation, Principal, ScopeBinding, TicketKind, WorkTokenLimits,
    };
    use std::collections::{BTreeMap, BTreeSet};

    #[test]
    fn budget_only_head_change_cannot_replace_the_observation_commit() {
        let root = tempfile::tempdir().unwrap();
        let binding = ScopeBinding {
            scope_id: "scope".into(),
            supervisor_session_id: "supervisor".into(),
            binding_revision: 1,
        };
        let service = TicketService::open(root.path(), binding.clone()).unwrap();
        let user = Authority::from_verified_host(
            binding,
            Principal::User {
                user_id: "owner".into(),
            },
        );
        let command = service
            .prepare_command(
                &user,
                "create",
                vec![Operation::Create {
                    temp_id: "work".into(),
                    kind: TicketKind::Work,
                    parent: None,
                    contract: Contract {
                        title: "Budget fixture".into(),
                        objective: "One bounded result".into(),
                        constraints: vec![],
                        acceptance: vec!["Review".into()],
                        user_acceptance_required: true,
                        allowed_tools: BTreeSet::from(["Task".into()]),
                    },
                    depends_on: BTreeSet::new(),
                }],
            )
            .unwrap();
        let work = service.execute(&user, &command).unwrap().ids["work"].clone();
        let (head, snapshot) = service.published().unwrap();
        let scope = Scope {
            project_id: "project".into(),
            work_id: work.clone(),
            assignment_id: "assignment".into(),
            generation: 1,
            authority_epoch: 1,
        };
        let id = IncidentId {
            scope: scope.clone(),
            progress_sequence: 0,
        };
        let cp = Checkpoint {
            scope,
            cursor: snapshot.seq,
            source_revision: snapshot.execution_facts_revision(),
            last_check_ms: 100,
            next_check_ms: 200,
            incident: Some(Incident {
                id: id.clone(),
                reason: Reason::EvidenceUnknown,
                opened_at_ms: 100,
                opened_cursor: snapshot.seq,
                source_revision: snapshot.execution_facts_revision(),
                attempts: 0,
                last_diagnostic_cursor: snapshot.seq,
                last_diagnostic_evidence_revision: None,
                last_diagnosis: None,
                pending_settlement: None,
                pending_settlement_usage: None,
                phase: Phase::Open,
                escalation: Some(Escalation {
                    operation: OperationId {
                        incident: id,
                        kind: OperationKind::Escalation,
                    },
                    reason: SupervisorReason::DeterministicAnomaly,
                    source_revision: snapshot.execution_facts_revision(),
                    cursor: snapshot.seq,
                    deliveries: 0,
                    queued: false,
                    next_delivery_ms: 100,
                }),
            }),
        };
        assert_eq!(NoticeSource::capture(&service, &cp).unwrap().commit, head);
        service
            .configure_work_token_budget(
                &user,
                0,
                WorkTokenLimits {
                    work_tokens: BTreeMap::from([(work, 100)]),
                    utc_day_tokens: 100,
                    total_tokens: 100,
                },
            )
            .unwrap();
        let (changed, after) = service.published().unwrap();
        assert_ne!(changed, head);
        assert!(after.seq > snapshot.seq);
        assert_eq!(
            after.execution_facts_revision(),
            snapshot.execution_facts_revision()
        );
        assert_eq!(
            NoticeSource::capture(&service, &cp),
            Err(PortError::Conflict)
        );
    }
}
