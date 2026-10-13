//! Explicit User-only observation of an uncertain local file replacement.
//! No provider retry, workspace mutation or invented process-stop proof.
use crate::{
    service::{validate_authority, validate_snapshot},
    workspace_files::observed_hash,
    *,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileReconcileRequest {
    pub operation_id: String,
    pub binding: ScopeBinding,
    pub expected_commit: String,
    pub expected_seq: u64,
    pub expected_epoch: u64,
    pub assignment_id: String,
    pub expected_assignment_revision: u64,
    pub effect_id: String,
    pub expected_request_hash: String,
    pub intended_sha256: String,
    pub evidence: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct FileReconciliationPlan {
    pub snapshot: String,
    pub assignment_id: String,
    pub effect_id: String,
    pub file_path: Option<String>,
    pub intended_sha256: Option<String>,
    pub observed_sha256: Option<String>,
    pub process_stopped: bool,
    pub reason: String,
    pub request: Option<FileReconcileRequest>,
}

fn request_budget(operation_id: &str, evidence: &str) -> Result<()> {
    if operation_id.is_empty()
        || operation_id.len() > 128
        || operation_id.starts_with("worker-file/")
        || evidence.trim().is_empty()
        || evidence.len() > 4096
    {
        return Err(Error::InvalidTransition(
            "invalid file reconciliation request".into(),
        ));
    }
    Ok(())
}

fn verified_offline(store: &crate::store::FileStore) -> bool {
    store.offline_original_writable
        && matches!(&store.health, Health::ReadOnly { reason }
            if reason == "offline inspection; no runtime permission")
}

fn write_intent(assignment: &Assignment, effect_id: &str) -> Result<FileOperation> {
    if assignment.workspace.is_none() {
        return Err(Error::InvalidTransition(
            "file intent lacks an isolated workspace".into(),
        ));
    }
    let effect = assignment
        .effects
        .get(effect_id)
        .ok_or_else(|| Error::InvalidTransition("file effect missing".into()))?;
    let intent = effect.file_intent.as_ref().ok_or_else(|| {
        Error::ResourceBlocked("legacy file effect lacks a frozen request".into())
    })?;
    let runtime = assignment
        .runtime
        .as_ref()
        .ok_or_else(|| Error::InvalidTransition("file intent lacks Runtime receipt".into()))?;
    let principal = format!(
        "worker:{}:{}:{}:{}",
        assignment.id, assignment.generation, runtime.run_id, runtime.session_id
    );
    let op: FileOperation = serde_json::from_str(&intent.canonical_request)?;
    let FileOperation::Write { content, .. } = &op else {
        return Err(Error::InvalidTransition(
            "file intent is not a write".into(),
        ));
    };
    if !effect_id.starts_with("worker-file/")
        || intent.principal != principal
        || intent.canonical_request.len() > 32768
        || content.len() > FILE_BYTES_LIMIT
        || canonical_bytes(&op)? != intent.canonical_request.as_bytes()
        || content_hash(intent.canonical_request.as_bytes()) != effect.action_fingerprint
        || effect
            .artifact
            .as_ref()
            .is_none_or(|a| a.sha256 != content_hash(content.as_bytes()))
    {
        return Err(Error::InvalidTransition(
            "invalid immutable file intent".into(),
        ));
    }
    Ok(op)
}

pub(crate) fn validate_file_intents(snapshot: &Snapshot) -> Result<()> {
    for assignment in snapshot.assignments.values() {
        for (id, effect) in &assignment.effects {
            if effect.file_intent.is_some() {
                if snapshot.schema < 4 {
                    return Err(Error::AuthorityUnavailable(
                        "frozen file intent requires schema 4".into(),
                    ));
                }
                write_intent(assignment, id)?;
            }
        }
    }
    Ok(())
}

impl TicketService {
    /// The embedding Host supplies explicit User authority. Not an LLM tool.
    /// Preview observes bytes only, using the same no-follow file boundary.
    pub fn file_reconciliation_plan(
        &self,
        authority: &Authority,
        assignment_id: &str,
        effect_id: &str,
        operation_id: &str,
        evidence: &str,
    ) -> Result<FileReconciliationPlan> {
        let store = self.inner.lock().expect("store mutex");
        let (commit, snapshot) = store
            .published
            .as_ref()
            .ok_or_else(|| Error::AuthorityUnavailable("no verified snapshot".into()))?;
        validate_authority(authority, snapshot)?;
        authority.user()?;
        request_budget(operation_id, evidence)?;
        let assignment = snapshot
            .assignments
            .get(assignment_id)
            .ok_or_else(|| Error::InvalidTransition("Assignment missing".into()))?;
        let effect = assignment
            .effects
            .get(effect_id)
            .ok_or_else(|| Error::InvalidTransition("file effect missing".into()))?;
        let mut plan = FileReconciliationPlan {
            snapshot: commit.clone(),
            assignment_id: assignment_id.into(),
            effect_id: effect_id.into(),
            file_path: None,
            intended_sha256: effect.artifact.as_ref().map(|a| a.sha256.clone()),
            observed_sha256: None,
            process_stopped: assignment.process_stopped,
            reason: "legacy file effect lacks a frozen request; keep quarantined".into(),
            request: None,
        };
        if effect.file_intent.is_none() {
            return Ok(plan);
        }
        let FileOperation::Write { file_path, .. } = write_intent(assignment, effect_id)? else {
            unreachable!()
        };
        let workspace = assignment
            .workspace
            .as_ref()
            .ok_or_else(|| Error::ScopeDenied("file effect has no workspace".into()))?;
        plan.observed_sha256 = observed_hash(&store, workspace, &file_path)?;
        plan.file_path = Some(file_path);
        plan.reason = if store.health != Health::Writable && !verified_offline(&store) {
            "authority unavailable; reopen and verify before reconciliation"
        } else if !assignment.process_stopped || assignment.state != AssignmentState::OutcomeUnknown
        {
            "a durable actual stopped Run is required"
        } else if !matches!(
            effect.state,
            EffectState::Started | EffectState::OutcomeUnknown
        ) {
            "file effect is already resolved"
        } else if plan.observed_sha256 != plan.intended_sha256 {
            "file bytes do not match immutable intended content; keep quarantined"
        } else {
            plan.request = Some(FileReconcileRequest {
                operation_id: operation_id.into(),
                binding: snapshot.binding.clone(),
                expected_commit: commit.clone(),
                expected_seq: snapshot.seq,
                expected_epoch: snapshot.authority_epoch,
                assignment_id: assignment_id.into(),
                expected_assignment_revision: assignment.record_revision,
                effect_id: effect_id.into(),
                expected_request_hash: effect.action_fingerprint.clone(),
                intended_sha256: plan.intended_sha256.clone().expect("validated artifact"),
                evidence: evidence.into(),
            });
            "ready for explicit User acknowledgement of observed current content"
        }
        .into();
        Ok(plan)
    }

    /// Consume an exact reviewed request; record observation, never write/retry.
    /// Scope/subject and duplicate receipt precede CAS and physical inspection.
    pub fn reconcile_file_effect(
        &self,
        authority: &Authority,
        request: &FileReconcileRequest,
    ) -> Result<OperationReceipt> {
        let mut store = self.inner.lock().expect("store mutex");
        let (commit, snapshot) = store
            .published
            .as_ref()
            .ok_or_else(|| Error::AuthorityUnavailable("no verified snapshot".into()))?;
        validate_authority(authority, snapshot)?;
        authority.user()?;
        if request.binding != snapshot.binding {
            return Err(Error::ScopeDenied("reconciliation scope binding".into()));
        }
        request_budget(&request.operation_id, &request.evidence)?;
        let canonical = canonical_bytes(request)?;
        if canonical.len() > 32768 {
            return Err(Error::InvalidTransition(
                "file reconciliation request exceeds budget".into(),
            ));
        }
        let hash = content_hash(&canonical);
        if let Some(receipt) = snapshot.receipts.get(&request.operation_id) {
            if receipt.principal != authority.identity() {
                return Err(Error::ScopeDenied("receipt subject".into()));
            }
            return if receipt.request_hash == hash {
                Ok(receipt.clone())
            } else {
                Err(Error::IdempotencyConflict)
            };
        }
        let offline = verified_offline(&store);
        if store.health != Health::Writable && !offline {
            return Err(Error::AuthorityUnavailable(format!("{:?}", store.health)));
        }
        if request.expected_commit != *commit
            || request.expected_seq != snapshot.seq
            || request.expected_epoch != snapshot.authority_epoch
        {
            return Err(Error::RevisionConflict);
        }
        let assignment = snapshot
            .assignments
            .get(&request.assignment_id)
            .ok_or_else(|| Error::InvalidTransition("Assignment missing".into()))?;
        if request.expected_assignment_revision != assignment.record_revision {
            return Err(Error::RevisionConflict);
        }
        if !assignment.process_stopped || assignment.state != AssignmentState::OutcomeUnknown {
            return Err(Error::ResourceBlocked("actual stopped Run required".into()));
        }
        let FileOperation::Write { file_path, .. } = write_intent(assignment, &request.effect_id)?
        else {
            unreachable!()
        };
        let effect = &assignment.effects[&request.effect_id];
        let artifact = effect.artifact.as_ref().expect("validated artifact");
        if !matches!(
            effect.state,
            EffectState::Started | EffectState::OutcomeUnknown
        ) || effect.action_fingerprint != request.expected_request_hash
            || artifact.sha256 != request.intended_sha256
        {
            return Err(Error::InvalidTransition(
                "file effect identity changed".into(),
            ));
        }
        if observed_hash(
            &store,
            assignment.workspace.as_ref().expect("file workspace"),
            &file_path,
        )? != Some(request.intended_sha256.clone())
        {
            return Err(Error::RevisionConflict);
        }
        if snapshot.receipts.contains_key(&request.effect_id) {
            return Err(Error::InvalidTransition(
                "unresolved effect already has a receipt".into(),
            ));
        }
        let intent = effect.file_intent.as_ref().expect("frozen intent").clone();
        let artifact = artifact.clone();
        let original_hash = effect.action_fingerprint.clone();
        let mut next = snapshot.clone();
        next.seq += 1;
        let a = next
            .assignments
            .get_mut(&request.assignment_id)
            .expect("Assignment");
        let effect = a.effects.get_mut(&request.effect_id).expect("effect");
        effect.state = EffectState::Succeeded;
        // This states observed bytes, not exactly-once physical execution.
        effect.provider_receipt = Some(format!("local-file-observed:{}", artifact.sha256));
        a.record_revision += 1;
        a.updated_seq = next.seq;
        let released = !a
            .effects
            .values()
            .any(|e| matches!(e.state, EffectState::Started | EffectState::OutcomeUnknown));
        if released {
            let work = next.tickets.get_mut(&a.work_id).expect("Work");
            a.state = if work.state == WorkState::Cancelled {
                AssignmentState::Cancelled
            } else {
                AssignmentState::Failed
            };
            if work.active_assignment.as_deref() == Some(a.id.as_str()) {
                work.active_assignment = None;
                if work.state != WorkState::Cancelled {
                    work.state = WorkState::Blocked;
                    work.paused = true;
                    work.blocked = Some(BlockReason {
                        reason: "file effects reconciled; explicit retry required".into(),
                        resume_state: WorkState::Ready,
                    });
                }
                work.record_revision += 1;
                work.updated_seq = next.seq;
            }
        }
        next.receipts.insert(
            request.effect_id.clone(),
            OperationReceipt {
                operation_id: request.effect_id.clone(),
                principal: intent.principal,
                request_hash: original_hash,
                canonical_request: intent.canonical_request,
                committed_seq: next.seq,
                ids: BTreeMap::from([
                    ("sha256".into(), artifact.sha256.clone()),
                    ("artifact".into(), artifact.uri.clone()),
                ]),
            },
        );
        let receipt = OperationReceipt {
            operation_id: request.operation_id.clone(),
            principal: authority.identity(),
            request_hash: hash,
            canonical_request: String::from_utf8(canonical).expect("JSON UTF-8"),
            committed_seq: next.seq,
            ids: BTreeMap::from([
                ("assignment_id".into(), request.assignment_id.clone()),
                ("effect_id".into(), request.effect_id.clone()),
                ("sha256".into(), artifact.sha256),
                ("resource_released".into(), released.to_string()),
            ]),
        };
        next.receipts
            .insert(request.operation_id.clone(), receipt.clone());
        validate_snapshot(&next)?;
        // Offline inspection still denies ordinary commands and Run permits.
        // Only this exact User operation temporarily enables publication. An
        // uncertain HEAD/directory failure keeps its read-only health intact.
        if offline {
            store.health = Health::Writable;
        }
        #[cfg(feature = "test-utils")]
        let result = store.publish_operation(next, &request.operation_id);
        #[cfg(not(feature = "test-utils"))]
        let result = store.publish(next);
        if offline {
            if store.health == Health::Writable {
                store.health = Health::ReadOnly {
                    reason: "offline inspection; no runtime permission".into(),
                };
            } else {
                store.offline_original_writable = false;
            }
        }
        result?;
        Ok(receipt)
    }
}
