//! Bounded historical Ticket facts. No transcript, runtime read or metering.
use super::checkpoint_record::NoticeSource;
use bamboo_orchestration::patrol::{host, Escalation, PortError};
use bamboo_tickets::{
    Authority, EffectState, InspectOptions, RequestKind, Snapshot, TicketKind, TicketService,
};
use serde_json::{json, Value};
use std::collections::BTreeSet;

pub(super) const MAX_EVIDENCE_BYTES: usize = 4096;

fn hash(bytes: &[u8]) -> String {
    bamboo_tickets::content_hash(bytes)
}

fn bounded(value: &Value) -> Result<Vec<u8>, PortError> {
    let bytes = bamboo_tickets::canonical_bytes(value).map_err(|_| PortError::Unavailable)?;
    if bytes.len() > MAX_EVIDENCE_BYTES {
        return Err(PortError::Unavailable);
    }
    Ok(bytes)
}

/// Fresh current authority/Scope/fence checks belong before AND after this
/// blocking historical read. The source is a selector, never a recovered grant.
pub(super) fn read(
    service: &TicketService,
    authority: &Authority,
    source: &NoticeSource,
    escalation: &Escalation,
) -> Result<Value, PortError> {
    source.matches(escalation)?;
    let scope = &source.operation.incident.scope;
    host::read_observation(service, authority, scope)?;
    let (head, current) = service.published().map_err(|_| PortError::Unavailable)?;
    // Explicit depth zero and empty history sections: only the selected Ticket
    // is authorized/read through this bounded public contract.
    service
        .work_inspect_sections(
            authority,
            std::slice::from_ref(&scope.work_id),
            &InspectOptions {
                sections: BTreeSet::new(),
                depth: 0,
                budget_bytes: 65_536,
                fixed_commit: Some(source.commit.clone()),
            },
        )
        .map_err(|_| PortError::Unavailable)?;
    let snapshot = service
        .fixed_snapshot(&source.commit)
        .map_err(|_| PortError::Unavailable)?;
    let value = project(source, escalation, &current, &snapshot)?;
    host::read_observation(service, authority, scope)?;
    if service.published().map_err(|_| PortError::Unavailable)?.0 != head {
        return Err(PortError::Conflict);
    }
    Ok(value)
}

// Shared by the real authorized reader and source-validation negatives.
pub(super) fn project(
    source: &NoticeSource,
    escalation: &Escalation,
    current: &Snapshot,
    snapshot: &Snapshot,
) -> Result<Value, PortError> {
    source.matches(escalation)?;
    let scope = &source.operation.incident.scope;
    let current_assignment = current
        .assignments
        .get(&scope.assignment_id)
        .ok_or(PortError::Denied)?;
    let work = snapshot
        .tickets
        .get(&scope.work_id)
        .ok_or(PortError::Denied)?;
    let assignment = snapshot
        .assignments
        .get(&scope.assignment_id)
        .ok_or(PortError::Denied)?;
    let intent = snapshot
        .intents
        .get(&assignment.dispatch_key)
        .ok_or(PortError::Denied)?;
    let spec = &intent.immutable_spec;
    let spec_bytes = bamboo_tickets::canonical_bytes(spec).map_err(|_| PortError::Unavailable)?;
    if spec_bytes.len() > 65_536 {
        return Err(PortError::Unavailable);
    }
    let current_intent = current
        .intents
        .get(&current_assignment.dispatch_key)
        .ok_or(PortError::Denied)?;
    if snapshot.seq != source.cursor
        || snapshot.execution_facts_revision() != source.execution_facts_revision
        || snapshot.binding != current.binding
        || work.kind != TicketKind::Work
        || work.generation != scope.generation
        || assignment.work_id != work.id
        || assignment.generation != scope.generation
        || assignment.contract_revision != work.contract_revision
        || assignment.contract_revision != current_assignment.contract_revision
        || snapshot.project.links.get(&work.id).map(|id| id.as_str()) != Some(&scope.project_id)
        || intent.assignment_id != assignment.id
        || intent.dispatch_key != assignment.dispatch_key
        || spec.assignment_id != assignment.id
        || spec.work_id != work.id
        || spec.generation != scope.generation
        || spec.contract_revision != work.contract_revision
        || spec.authority_epoch != scope.authority_epoch
        || spec.authority_epoch > snapshot.authority_epoch
        || assignment.authority_epoch < spec.authority_epoch
        || assignment.authority_epoch > snapshot.authority_epoch
        || spec.binding != snapshot.binding
        || assignment.runtime != intent.receipt
        || assignment.runtime != current_assignment.runtime
        || assignment.dispatch_key != current_assignment.dispatch_key
        || intent.spec_hash != current_intent.spec_hash
        || hash(&spec_bytes) != intent.spec_hash
    {
        return Err(PortError::Denied);
    }
    let mut references = Vec::new();
    for (kind, id, revision, value) in [
        (
            "work",
            &work.id,
            work.updated_seq,
            serde_json::to_value(work),
        ),
        (
            "assignment",
            &assignment.id,
            assignment.updated_seq,
            serde_json::to_value(assignment),
        ),
    ] {
        let value = value.map_err(|_| PortError::Unavailable)?;
        // Refuse unbounded source values before hashing, rather than pretending
        // a clipped record is the exact canonical evidence.
        let bytes = bamboo_tickets::canonical_bytes(&value).map_err(|_| PortError::Unavailable)?;
        if id.len() > 256
            || revision == 0
            || revision > source.execution_facts_revision
            || bytes.len() > 65_536
        {
            return Err(PortError::Unavailable);
        }
        references.push(json!({"kind":kind,"id":id,"revision":revision,"sha256":hash(&bytes)}));
    }
    let request = assignment.awaiting_request.as_ref().map(|id| {
        let request = snapshot.requests.get(id).ok_or(PortError::Denied)?;
        if id.len() > 256
            || request.work_id != work.id
            || request.assignment_id.as_deref() != Some(&assignment.id)
            || request.generation != scope.generation
            || request.contract_revision != work.contract_revision
        {
            return Err(PortError::Denied);
        }
        Ok(json!({"id":id,"kind":match request.kind {RequestKind::Question => "question", RequestKind::Approval {..} => "approval"},
            "status":request.status,"prompt_revision":request.prompt_revision}))
    }).transpose()?;
    let workspace = spec.workspace.as_ref().map(|workspace| {
        let bytes = bamboo_tickets::canonical_bytes(workspace).map_err(|_| PortError::Unavailable)?;
        if bytes.len() > 16_384 || workspace.write_roots.len() > 256 || workspace.claims.len() > 256 {
            return Err(PortError::Unavailable);
        }
        Ok(json!({"sha256":hash(&bytes),"write_roots":workspace.write_roots.len(),"claims":workspace.claims.len()}))
    }).transpose()?;
    let value = json!({
        "format":"ticket_patrol_evidence_v1", "historical":true,
        "source":source, "scope":scope,
        "work":{"state":work.state,"contract_revision":work.contract_revision},
        "assignment":{"state":assignment.state,"process_stopped":assignment.process_stopped,
            "runtime_receipt":assignment.runtime,"effects":assignment.effects.len(),
            "unknown_effects":assignment.effects.values().filter(|effect|matches!(effect.state,EffectState::Started|EffectState::OutcomeUnknown)).count()},
        "awaiting_request":request, "workspace_declaration":workspace,
        "measurements":"start/heartbeat/progress/tool/error/resource availability unknown",
        "references":references,
    });
    bounded(&value)?;
    Ok(value)
}
