//! Pure bounded, unmetered Ticket evidence. No native store, provider or I/O.
//! Generic payloads are adapter-owned data; a plan never grants read authority.
use super::Scope;
use bamboo_domain::{ProjectManifest, ProjectStatus, WorkspaceBinding};
use bamboo_tickets::{
    AssignmentState, Contract, EffectState, Error, ProjectContextSnapshot, RequestKind,
    RequestStatus, Result, RuntimeReceipt, Snapshot, WorkState,
};
use serde::Serialize;

pub const MAX_WORK_PATROL_EVIDENCE_BYTES: usize = 16 * 1024;
pub const MAX_WORK_PATROL_EVIDENCE_REFS: usize = 8;
const MAX_SOURCE_BYTES: usize = 8 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkPatrolEvidenceCoverage {
    BoundedSnapshot,
}

/// An explicitly unmetered snapshot; since/through select a window, not deltas.
/// Native bodies are untrusted context data and confer no execution permission.
#[derive(Clone, Debug, Serialize)]
pub struct UnmeteredWorkPatrolEvidence<R, S> {
    pub scope: Scope,
    pub source_commit: String,
    pub since_cursor: u64,
    pub through_cursor: u64,
    pub execution_facts_revision: u64,
    pub adopted_context_hash: String,
    pub adopted_context: WorkPatrolAdoptedContextEvidence,
    pub coverage: WorkPatrolEvidenceCoverage,
    /// Includes the Ticket snapshot and adopted context, plus source outcomes.
    pub reference_count: usize,
    /// False for no sources, unsupported or any unresolved/over-budget source.
    /// This is content consistency at reads, not truth or runtime completeness.
    pub all_sources_matched: bool,
    pub ticket: WorkPatrolTicketEvidence,
    pub sources: Vec<WorkPatrolSourceEvidence<R, S>>,
}

// Keep room for every slot's largest aggregate-overflow marker. A short
// Unavailable/Missing outcome must not let earlier content consume that room.
fn reserved_bytes<R: Serialize, S: Serialize>(
    evidence: &UnmeteredWorkPatrolEvidence<R, S>,
) -> Result<usize> {
    let marker: WorkPatrolSourceOutcome<R, S> = WorkPatrolSourceOutcome::Oversized {
        kind: WorkPatrolEvidenceLimit::AggregateBytes,
        required: usize::MAX,
        limit: MAX_WORK_PATROL_EVIDENCE_BYTES,
    };
    let floor = serde_json::to_vec(&marker)?.len();
    let mut bytes = serde_json::to_vec(evidence)?.len();
    for source in &evidence.sources {
        let current = serde_json::to_vec(&source.outcome)?.len();
        bytes = bytes
            .checked_add(floor.saturating_sub(current))
            .ok_or(Error::ContextBudgetExceeded)?;
    }
    Ok(bytes)
}

#[derive(Clone, Debug, Serialize)]
pub struct WorkPatrolAdoptedContextEvidence {
    pub project_revision: u64,
    pub resource_revision: u64,
    pub workspace_binding: WorkspaceBinding,
}

#[derive(Clone, Debug, Serialize)]
pub struct WorkPatrolTicketEvidence {
    pub work_state: WorkState,
    pub assignment_state: AssignmentState,
    pub contract_revision: u64,
    pub contract: Contract,
    pub runtime_receipt: Option<RuntimeReceipt>,
    pub process_stopped: bool,
    pub unknown_effects: usize,
    pub awaiting_request: Option<WorkPatrolRequestEvidence>,
    /// Hash of declared scope, never measured filesystem availability.
    pub workspace_declaration_hash: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct WorkPatrolRequestEvidence {
    pub id: String,
    pub kind: &'static str,
    pub status: RequestStatus,
    pub prompt_revision: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct WorkPatrolSourceEvidence<R, S> {
    pub source_key: String,
    pub expected_version: String,
    pub outcome: WorkPatrolSourceOutcome<R, S>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkPatrolEvidenceLimit {
    NativeCharacters,
    SourceBytes,
    AggregateBytes,
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum WorkPatrolSourceOutcome<R, S> {
    VersionMatched {
        reference: R,
        title: String,
        content: String,
    },
    VersionMismatch {
        observed_version: String,
    },
    Missing,
    Inactive {
        status: S,
    },
    Oversized {
        kind: WorkPatrolEvidenceLimit,
        required: usize,
        limit: usize,
    },
    Unavailable,
    Unsupported,
}

/// A bounded snapshot plan only. The Host must independently authorize its exact
/// commit/Scope before and after every actual read. This is not a stored grant.
pub struct EvidencePlan<R, S> {
    evidence: UnmeteredWorkPatrolEvidence<R, S>,
    context: ProjectContextSnapshot,
}

impl<R: Serialize, S: Serialize> EvidencePlan<R, S> {
    pub fn prepare(snapshot: &Snapshot, scope: &Scope, commit: &str, since: u64) -> Result<Self> {
        super::host::project(snapshot, scope)
            .map_err(|_| Error::ScopeDenied("invalid evidence snapshot scope".into()))?;
        if super::host::current_assignment_id(snapshot, &scope.work_id)
            .map_err(|_| Error::RevisionConflict)?
            .as_deref()
            != Some(scope.assignment_id.as_str())
        {
            return Err(Error::ScopeDenied(
                "current evidence attempt required".into(),
            ));
        }
        if since > snapshot.seq {
            return Err(Error::RevisionConflict);
        }
        let assignment = snapshot
            .assignments
            .get(&scope.assignment_id)
            .ok_or_else(|| Error::ScopeDenied("evidence Assignment missing".into()))?;
        let spec = &snapshot
            .intents
            .get(&assignment.dispatch_key)
            .ok_or(Error::IdempotencyConflict)?
            .immutable_spec;
        let hash = spec.project_context_hash.as_ref().ok_or_else(|| {
            Error::ResourceBlocked("Assignment has no adopted Project context".into())
        })?;
        let context = snapshot
            .project
            .contexts
            .get(hash)
            .ok_or(Error::IdempotencyConflict)?;
        if context.sources.len() + 2 > MAX_WORK_PATROL_EVIDENCE_REFS
            || context
                .sources
                .iter()
                .any(|(key, version)| key.len() > 512 || version.len() > 256)
        {
            return Err(Error::ContextBudgetExceeded);
        }
        let context_bytes = bamboo_tickets::canonical_bytes(context)?;
        if context_bytes.len() > bamboo_tickets::PROJECT_CONTEXT_BYTES_LIMIT {
            return Err(Error::ContextBudgetExceeded);
        }
        // Immutable Assignment adoption; current resource validation is separate.
        if snapshot.project.assignment_contexts.get(&assignment.id) != Some(hash)
            || snapshot.project.links.get(&spec.work_id) != Some(&context.project_id)
            || context.project_id.as_str() != scope.project_id
            || context.contract_revision != spec.contract_revision
            || context.contract != spec.contract
            || bamboo_tickets::content_hash(&context_bytes) != *hash
        {
            return Err(Error::IdempotencyConflict);
        }
        let mut sources: Vec<WorkPatrolSourceEvidence<R, S>> = Vec::new();
        for (key, version) in &context.sources {
            sources.push(WorkPatrolSourceEvidence {
                source_key: key.clone(),
                expected_version: version.clone(),
                outcome: WorkPatrolSourceOutcome::Unavailable,
            });
        }
        let awaiting_request = assignment
            .awaiting_request
            .as_ref()
            .map(|id| {
                let request = snapshot
                    .requests
                    .get(id)
                    .ok_or(Error::IdempotencyConflict)?;
                if request.work_id != scope.work_id
                    || request.assignment_id.as_ref() != Some(&scope.assignment_id)
                    || request.generation != scope.generation
                    || request.contract_revision != spec.contract_revision
                {
                    return Err(Error::IdempotencyConflict);
                }
                Ok(WorkPatrolRequestEvidence {
                    id: id.clone(),
                    kind: match request.kind {
                        RequestKind::Question => "question",
                        RequestKind::Approval { .. } => "approval",
                    },
                    status: request.status,
                    prompt_revision: request.prompt_revision,
                })
            })
            .transpose()?;
        let workspace_declaration_hash = spec
            .workspace
            .as_ref()
            .map(|workspace| {
                let bytes = bamboo_tickets::canonical_bytes(workspace)?;
                if bytes.len() > 16 * 1024 {
                    return Err(Error::ContextBudgetExceeded);
                }
                Ok(bamboo_tickets::content_hash(&bytes))
            })
            .transpose()?;
        let evidence: UnmeteredWorkPatrolEvidence<R, S> = UnmeteredWorkPatrolEvidence {
            scope: scope.clone(),
            source_commit: commit.into(),
            since_cursor: since,
            through_cursor: snapshot.seq,
            execution_facts_revision: snapshot.execution_facts_revision(),
            adopted_context_hash: hash.clone(),
            adopted_context: WorkPatrolAdoptedContextEvidence {
                project_revision: context.project_revision,
                resource_revision: context.resource_revision,
                workspace_binding: context.workspace_binding.clone(),
            },
            coverage: WorkPatrolEvidenceCoverage::BoundedSnapshot,
            reference_count: 2 + sources.len(),
            all_sources_matched: false,
            ticket: WorkPatrolTicketEvidence {
                work_state: snapshot.tickets[&scope.work_id].state,
                assignment_state: assignment.state,
                contract_revision: spec.contract_revision,
                contract: spec.contract.clone(),
                runtime_receipt: assignment.runtime.clone(),
                process_stopped: assignment.process_stopped,
                unknown_effects: assignment
                    .effects
                    .values()
                    .filter(|effect| {
                        matches!(
                            effect.state,
                            EffectState::Started | EffectState::OutcomeUnknown
                        )
                    })
                    .count(),
                awaiting_request,
                workspace_declaration_hash,
            },
            sources,
        };
        if reserved_bytes(&evidence)? > MAX_WORK_PATROL_EVIDENCE_BYTES {
            return Err(Error::ContextBudgetExceeded);
        }
        Ok(Self {
            evidence,
            context: context.clone(),
        })
    }

    pub fn context(&self) -> &ProjectContextSnapshot {
        &self.context
    }
    /// Opaque adopted selectors; the adapter owns native validation/resolution.
    pub fn sources(&self) -> &[WorkPatrolSourceEvidence<R, S>] {
        &self.evidence.sources
    }
    pub fn validate_project(&self, manifest: &ProjectManifest) -> Result<()> {
        resource(&self.context, manifest)
    }
    pub fn record_source(
        &mut self,
        index: usize,
        outcome: WorkPatrolSourceOutcome<R, S>,
    ) -> Result<()> {
        let source = self
            .evidence
            .sources
            .get_mut(index)
            .ok_or_else(|| Error::ScopeDenied("evidence source index out of range".into()))?;
        source.outcome = bound_source(outcome);
        let bytes = reserved_bytes(&self.evidence)?;
        if bytes > MAX_WORK_PATROL_EVIDENCE_BYTES {
            self.evidence.sources[index].outcome = WorkPatrolSourceOutcome::Oversized {
                kind: WorkPatrolEvidenceLimit::AggregateBytes,
                required: bytes,
                limit: MAX_WORK_PATROL_EVIDENCE_BYTES,
            };
        }
        if reserved_bytes(&self.evidence)? > MAX_WORK_PATROL_EVIDENCE_BYTES {
            return Err(Error::ContextBudgetExceeded);
        }
        Ok(())
    }
    pub fn finish(&self) -> Result<UnmeteredWorkPatrolEvidence<R, S>>
    where
        R: Clone,
        S: Clone,
    {
        let mut evidence = self.evidence.clone();
        evidence.all_sources_matched = !evidence.sources.is_empty()
            && evidence.sources.iter().all(|source| {
                matches!(
                    source.outcome,
                    WorkPatrolSourceOutcome::VersionMatched { .. }
                )
            });
        if serde_json::to_vec(&evidence)?.len() > MAX_WORK_PATROL_EVIDENCE_BYTES {
            return Err(Error::ContextBudgetExceeded);
        }
        Ok(evidence)
    }
}

/// Apply the per-source serialized bound to adapter-owned, identity-checked data.
pub fn bound_source<R: Serialize, S: Serialize>(
    outcome: WorkPatrolSourceOutcome<R, S>,
) -> WorkPatrolSourceOutcome<R, S> {
    match serde_json::to_vec(&outcome) {
        Ok(bytes) if bytes.len() <= MAX_SOURCE_BYTES => outcome,
        Ok(bytes) => WorkPatrolSourceOutcome::Oversized {
            kind: WorkPatrolEvidenceLimit::SourceBytes,
            required: bytes.len(),
            limit: MAX_SOURCE_BYTES,
        },
        Err(_) => WorkPatrolSourceOutcome::Unavailable,
    }
}
fn resource(context: &ProjectContextSnapshot, manifest: &ProjectManifest) -> Result<()> {
    if manifest.id != context.project_id || manifest.status != ProjectStatus::Active {
        return Err(Error::ResourceBlocked("adopted Project unavailable".into()));
    }
    if context.project_revision > manifest.revision
        || context.resource_revision != manifest.resource_revision
        || !manifest
            .workspace_roots()
            .any(|path| path == context.workspace_binding.path)
    {
        return Err(Error::RevisionConflict);
    }
    if let Some(binding) = manifest
        .workspace_bindings
        .iter()
        .find(|binding| binding.path == context.workspace_binding.path)
    {
        if binding != &context.workspace_binding {
            return Err(Error::RevisionConflict);
        }
    } else if context.workspace_binding.git_common_dir.is_some()
        || context.workspace_binding.label.is_some()
    {
        return Err(Error::RevisionConflict);
    }
    Ok(())
}
