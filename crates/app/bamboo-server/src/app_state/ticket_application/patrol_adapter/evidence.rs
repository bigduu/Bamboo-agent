//! Host-selected, unmetered context data. Never a DiagnosticInput or a grant.
use super::super::{Error, Principal, Result};
use super::patrol_error;
use bamboo_domain::{ProjectManifest, ProjectStatus, WorkspaceBinding};
use bamboo_memory::memory_store::{DurableMemoryStatus, ProjectMemoryRead, ProjectMemoryRef};
use bamboo_orchestration::patrol::host;
use bamboo_orchestration::patrol::Scope;
use bamboo_tickets::{AssignmentState, Contract, RequestStatus, RuntimeReceipt, WorkState};
use bamboo_tickets::{
    Authority, EffectState, ProjectContextSnapshot, RequestKind, Snapshot, TicketService,
};
use serde::Serialize;
use std::{future::Future, sync::Arc};

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
pub struct UnmeteredWorkPatrolEvidence {
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
    pub sources: Vec<WorkPatrolSourceEvidence>,
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
pub struct WorkPatrolSourceEvidence {
    pub source_key: String,
    pub expected_version: String,
    pub outcome: WorkPatrolSourceOutcome,
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
pub enum WorkPatrolSourceOutcome {
    VersionMatched {
        reference: ProjectMemoryRef,
        title: String,
        content: String,
    },
    VersionMismatch {
        observed_version: String,
    },
    Missing,
    Inactive {
        status: DurableMemoryStatus,
    },
    Oversized {
        kind: WorkPatrolEvidenceLimit,
        required: usize,
        limit: usize,
    },
    Unavailable,
    Unsupported,
}

fn resolve(
    expected: &ProjectMemoryRef,
    read: std::io::Result<ProjectMemoryRead>,
) -> WorkPatrolSourceOutcome {
    match read {
        Ok(ProjectMemoryRead::VersionMatched {
            reference,
            title,
            content,
            ..
        }) if &reference == expected => {
            let outcome = WorkPatrolSourceOutcome::VersionMatched {
                reference,
                title,
                content,
            };
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
        Ok(ProjectMemoryRead::VersionMismatch { observed })
            if observed.project_id() == expected.project_id()
                && observed.memory_id() == expected.memory_id() =>
        {
            WorkPatrolSourceOutcome::VersionMismatch {
                observed_version: observed.content_version().into(),
            }
        }
        Ok(ProjectMemoryRead::Missing) => WorkPatrolSourceOutcome::Missing,
        Ok(ProjectMemoryRead::Inactive { status }) => WorkPatrolSourceOutcome::Inactive { status },
        Ok(ProjectMemoryRead::OverBudget {
            required_chars,
            max_chars,
        }) => WorkPatrolSourceOutcome::Oversized {
            kind: WorkPatrolEvidenceLimit::NativeCharacters,
            required: required_chars,
            limit: max_chars,
        },
        _ => WorkPatrolSourceOutcome::Unavailable,
    }
}

struct Prepared {
    evidence: UnmeteredWorkPatrolEvidence,
    context: ProjectContextSnapshot,
    pins: Vec<Option<ProjectMemoryRef>>,
}

fn authorized(
    service: &TicketService,
    authority: &Authority,
    scope: &Scope,
    expected: &str,
) -> Result<Snapshot> {
    let (commit, snapshot) = service.published()?;
    if commit != expected {
        return Err(Error::RevisionConflict);
    }
    let current = host::scope_for_assignment(
        service,
        authority,
        &scope.project_id,
        &scope.work_id,
        &scope.assignment_id,
    )
    .map_err(patrol_error)?;
    if current != *scope
        || host::current_assignment_id(&snapshot, &scope.work_id)
            .map_err(patrol_error)?
            .as_deref()
            != Some(&scope.assignment_id)
    {
        return Err(Error::ScopeDenied(
            "current patrol evidence scope required".into(),
        ));
    }
    if service.published()?.0 != expected {
        return Err(Error::RevisionConflict);
    }
    Ok(snapshot)
}

fn prepare(snapshot: &Snapshot, scope: &Scope, commit: &str, since: u64) -> Result<Prepared> {
    if since > snapshot.seq {
        return Err(Error::RevisionConflict);
    }
    let assignment = &snapshot.assignments[&scope.assignment_id];
    let spec = &snapshot.intents[&assignment.dispatch_key].immutable_spec;
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
    // Equivalent immutable DispatchSpec/context proof, without the old helper's
    // repairing ProjectStore::open/get path. Resource proof below is pure read.
    if snapshot.project.assignment_contexts.get(&assignment.id) != Some(hash)
        || snapshot.project.links.get(&spec.work_id) != Some(&context.project_id)
        || context.project_id.as_str() != scope.project_id
        || context.contract_revision != spec.contract_revision
        || context.contract != spec.contract
        || bamboo_tickets::content_hash(&context_bytes) != *hash
    {
        return Err(Error::IdempotencyConflict);
    }
    let mut pins = Vec::new();
    let mut sources = Vec::new();
    for (key, version) in &context.sources {
        let pin = if key.starts_with("jiandu:") {
            let prefix = format!("jiandu:project:{}:memory:", context.project_id);
            let id = key
                .strip_prefix(&prefix)
                .filter(|id| !id.is_empty() && id.len() <= 256)
                .ok_or_else(|| Error::ScopeDenied("foreign or invalid adopted source".into()))?;
            let pin: ProjectMemoryRef = serde_json::from_value(serde_json::json!({"project_id":context.project_id,"memory_id":id,"content_version":version}))
                .map_err(|_| Error::ScopeDenied("invalid adopted Jiandu source".into()))?;
            if pin.source_key() != *key {
                return Err(Error::ScopeDenied("invalid adopted source key".into()));
            }
            Some(pin)
        } else {
            None
        };
        sources.push(WorkPatrolSourceEvidence {
            source_key: key.clone(),
            expected_version: version.clone(),
            outcome: if pin.is_some() {
                WorkPatrolSourceOutcome::Unavailable
            } else {
                WorkPatrolSourceOutcome::Unsupported
            },
        });
        pins.push(pin);
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
    let evidence = UnmeteredWorkPatrolEvidence {
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
    if serde_json::to_vec(&evidence)?.len() > MAX_WORK_PATROL_EVIDENCE_BYTES {
        return Err(Error::ContextBudgetExceeded);
    }
    Ok(Prepared {
        evidence,
        context: context.clone(),
        pins,
    })
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

fn fit_source(
    evidence: &mut UnmeteredWorkPatrolEvidence,
    index: usize,
    outcome: WorkPatrolSourceOutcome,
) -> Result<()> {
    evidence.sources[index].outcome = outcome;
    let bytes = serde_json::to_vec(evidence)?.len();
    if bytes > MAX_WORK_PATROL_EVIDENCE_BYTES {
        evidence.sources[index].outcome = WorkPatrolSourceOutcome::Oversized {
            kind: WorkPatrolEvidenceLimit::AggregateBytes,
            required: bytes,
            limit: MAX_WORK_PATROL_EVIDENCE_BYTES,
        };
    }
    Ok(())
}

impl crate::app_state::AppState {
    /// Bounded, unmetered Host read of this Assignment's exact adopted sources.
    /// No provider activation, token estimate, source discovery or read grant.
    pub async fn read_work_patrol_evidence(
        &self,
        scope: &Scope,
        expected_ticket_commit: &str,
        since_cursor: u64,
    ) -> Result<UnmeteredWorkPatrolEvidence> {
        let memory = self.memory_store.clone();
        let project: bamboo_domain::ProjectId = scope
            .project_id
            .parse()
            .map_err(|_| Error::ScopeDenied("invalid Project selector".into()))?;
        self.read_evidence_with(scope, expected_ticket_commit, since_cursor, move |pin| {
            let memory = memory.clone();
            let project = project.clone();
            async move {
                memory
                    .read_project_source_read_only(&project, pin.memory_id(), pin.content_version())
                    .await
            }
        })
        .await
    }

    async fn project_evidence_manifest(
        &self,
        context: &ProjectContextSnapshot,
    ) -> Result<ProjectManifest> {
        let projects = self.project_store.clone();
        let root = self.tickets.root.clone();
        let project = context.project_id.clone();
        tokio::task::spawn_blocking(move || {
            if projects.paths().data_dir().canonicalize()? != root.canonicalize()? {
                return Err(Error::ScopeDenied(
                    "Project store is outside the current Host".into(),
                ));
            }
            projects.get_read_only(&project).map_err(|_| {
                Error::AuthorityUnavailable("Project read-only proof unavailable".into())
            })
        })
        .await
        .map_err(|_| Error::AuthorityUnavailable("Project evidence reader stopped".into()))?
    }

    async fn evidence_fence(
        &self,
        service: &Arc<TicketService>,
        scope: &Scope,
        expected: &str,
        context: &ProjectContextSnapshot,
        manifest: &ProjectManifest,
    ) -> Result<()> {
        let (fresh, authority) = self.tickets.authority(Principal::Runtime).await?;
        if !Arc::ptr_eq(&fresh, service) {
            return Err(Error::RevisionConflict);
        }
        authorized(&fresh, &authority, scope, expected)?;
        let current = self.project_evidence_manifest(context).await?;
        resource(context, &current)?;
        if &current != manifest {
            return Err(Error::RevisionConflict);
        }
        let (fresh, authority) = self.tickets.authority(Principal::Runtime).await?;
        if !Arc::ptr_eq(&fresh, service) {
            return Err(Error::RevisionConflict);
        }
        authorized(&fresh, &authority, scope, expected)?;
        Ok(())
    }

    // Private per-call reader seam: tests may interleave real source reads with
    // authority changes; no environment hook or production fault capability.
    async fn read_evidence_with<R, F>(
        &self,
        scope: &Scope,
        expected: &str,
        since: u64,
        mut read: R,
    ) -> Result<UnmeteredWorkPatrolEvidence>
    where
        R: FnMut(ProjectMemoryRef) -> F,
        F: Future<Output = std::io::Result<ProjectMemoryRead>>,
    {
        let (service, authority) = self.tickets.authority(Principal::Runtime).await?;
        let snapshot = authorized(&service, &authority, scope, expected)?;
        let Prepared {
            mut evidence,
            context,
            pins,
        } = prepare(&snapshot, scope, expected, since)?;
        let manifest = self.project_evidence_manifest(&context).await?;
        resource(&context, &manifest)?;
        self.evidence_fence(&service, scope, expected, &context, &manifest)
            .await?;
        for (index, pin) in pins.into_iter().enumerate() {
            if let Some(pin) = pin {
                let outcome = resolve(&pin, read(pin.clone()).await);
                self.evidence_fence(&service, scope, expected, &context, &manifest)
                    .await?;
                fit_source(&mut evidence, index, outcome)?;
            }
        }
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
        self.evidence_fence(&service, scope, expected, &context, &manifest)
            .await?;
        Ok(evidence)
    }
}

#[cfg(all(test, unix))]
mod native_tests;
#[cfg(test)]
mod tests;
