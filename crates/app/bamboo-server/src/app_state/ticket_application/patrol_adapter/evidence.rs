//! Host/native I/O adapter. Pure evidence policy belongs to bamboo-orchestration.
use super::super::{Error, Principal, Result, TicketApplication};
use super::patrol_error;
use bamboo_config::{
    Config, LiveSection, ModelLimitsSection, PatrolModelConfig, PatrolModelProtocol,
};
use bamboo_domain::{Message, ProjectManifest};
use bamboo_llm::protocol::llama_cpp::GptOssPrompt;
use bamboo_memory::memory_store::{
    DurableMemoryStatus, MemoryStore, ProjectMemoryRead, ProjectMemoryRef,
};
use bamboo_orchestration::patrol::{
    diagnostic_input::{
        diagnostic_prompt, prepare_diagnostic_input, prepare_diagnostic_input_with_ceiling,
    },
    evidence as core, host, DiagnosticInput, EvidenceRef, Model, Observation, PortError, Scope,
};
use bamboo_projects::ProjectStore;
use bamboo_tickets::{Authority, ProjectContextSnapshot, Snapshot, TicketService};
pub use core::{
    WorkPatrolAdoptedContextEvidence, WorkPatrolEvidenceCoverage, WorkPatrolEvidenceLimit,
    WorkPatrolRequestEvidence, WorkPatrolTicketEvidence, MAX_WORK_PATROL_EVIDENCE_BYTES,
    MAX_WORK_PATROL_EVIDENCE_REFS,
};
use std::{
    future::{ready, Future},
    sync::Arc,
};
pub type UnmeteredWorkPatrolEvidence =
    core::UnmeteredWorkPatrolEvidence<ProjectMemoryRef, DurableMemoryStatus>;
pub type WorkPatrolSourceEvidence =
    core::WorkPatrolSourceEvidence<ProjectMemoryRef, DurableMemoryStatus>;
pub type WorkPatrolSourceOutcome =
    core::WorkPatrolSourceOutcome<ProjectMemoryRef, DurableMemoryStatus>;
type Plan = core::EvidencePlan<ProjectMemoryRef, DurableMemoryStatus>;

/// The selected model and exact raw prompt whose count is in the diagnostic input.
pub struct PreparedPatrolDiagnostic {
    model: Model,
    input: DiagnosticInput,
    prompt: GptOssPrompt,
}

impl PreparedPatrolDiagnostic {
    pub fn model(&self) -> &Model {
        &self.model
    }

    pub fn input(&self) -> &DiagnosticInput {
        &self.input
    }

    pub fn prompt_tokens(&self) -> &[u32] {
        self.prompt.tokens()
    }

    pub(super) fn prompt(&self) -> &GptOssPrompt {
        &self.prompt
    }
}

pub(in crate::app_state::ticket_application) struct EvidenceSources {
    pub(super) memory: MemoryStore,
    pub(super) projects: Arc<ProjectStore>,
    pub(super) providers: Arc<bamboo_llm::ProviderRegistry>,
    pub(super) config_io_lock: Arc<tokio::sync::Mutex<()>>,
    pub(super) model_limits: Option<Arc<LiveSection<ModelLimitsSection>>>,
}

pub(super) struct EvidenceReader<'a> {
    pub(super) tickets: &'a TicketApplication,
    pub(super) config: &'a tokio::sync::RwLock<Config>,
    pub(super) memory_store: &'a MemoryStore,
    pub(super) project_store: &'a Arc<ProjectStore>,
}

fn prepare_configured_diagnostic(
    evidence: UnmeteredWorkPatrolEvidence,
    expected_commit: &str,
    since_cursor: u64,
    through: &Observation,
    selection: PatrolModelConfig,
) -> Result<PreparedPatrolDiagnostic> {
    let model = configured_model(&selection);
    let mut prompt = None;
    let input = prepare_diagnostic_input(
        &evidence,
        expected_commit,
        since_cursor,
        through,
        &model,
        |_, summary, references| {
            let (instructions, evidence) = diagnostic_prompt(summary, references)?;
            let encoded = GptOssPrompt::encode(&instructions, &evidence)
                .map_err(|_| PortError::Unavailable)?;
            let tokens = u32::try_from(encoded.tokens().len()).map_err(|_| PortError::Denied)?;
            prompt = Some(encoded);
            Ok(tokens)
        },
    )
    .map_err(patrol_error)?;
    Ok(PreparedPatrolDiagnostic {
        model,
        input,
        prompt: prompt.ok_or_else(|| patrol_error(PortError::Unavailable))?,
    })
}

pub(super) fn configured_model(selection: &PatrolModelConfig) -> Model {
    Model {
        provider: selection.provider.clone(),
        model: selection.model.clone(),
        max_input_tokens: selection.max_input_tokens.get(),
        max_output_tokens: selection.max_output_tokens.get(),
        max_evidence_bytes: selection.max_evidence_bytes.get(),
        max_evidence_refs: selection.max_evidence_refs.get(),
    }
}

fn resolve(
    expected: &ProjectMemoryRef,
    read: std::io::Result<ProjectMemoryRead>,
) -> WorkPatrolSourceOutcome {
    core::bound_source(match read {
        Ok(ProjectMemoryRead::VersionMatched {
            reference,
            title,
            content,
            ..
        }) if &reference == expected => WorkPatrolSourceOutcome::VersionMatched {
            reference,
            title,
            content,
        },
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
    })
}

fn native_pins(plan: &Plan) -> Result<Vec<Option<ProjectMemoryRef>>> {
    plan.sources()
        .iter()
        .map(|source| {
            native_pin(
                &plan.context().project_id,
                &source.source_key,
                &source.expected_version,
            )
        })
        .collect()
}

fn native_pin(
    project: &bamboo_domain::ProjectId,
    key: &str,
    version: &str,
) -> Result<Option<ProjectMemoryRef>> {
    let pin = if key.starts_with("jiandu:") {
        let prefix = format!("jiandu:project:{project}:memory:");
        let id = key
            .strip_prefix(&prefix)
            .filter(|id| !id.is_empty() && id.len() <= 256)
            .ok_or_else(|| Error::ScopeDenied("foreign or invalid adopted source".into()))?;
        let pin: ProjectMemoryRef = serde_json::from_value(
            serde_json::json!({"project_id":project,"memory_id":id,"content_version":version}),
        )
        .map_err(|_| Error::ScopeDenied("invalid adopted Jiandu source".into()))?;
        if pin.source_key() != key {
            return Err(Error::ScopeDenied("invalid adopted source key".into()));
        }
        Some(pin)
    } else {
        None
    };
    Ok(pin)
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

impl crate::app_state::AppState {
    fn patrol_evidence_reader(&self) -> EvidenceReader<'_> {
        EvidenceReader {
            tickets: &self.tickets,
            config: &self.config,
            memory_store: &self.memory_store,
            project_store: &self.project_store,
        }
    }

    /// Bounded, unmetered Host read of this Assignment's exact adopted sources.
    /// No provider activation, token estimate, source discovery or read grant.
    pub async fn read_work_patrol_evidence(
        &self,
        scope: &Scope,
        expected_ticket_commit: &str,
        since_cursor: u64,
    ) -> Result<UnmeteredWorkPatrolEvidence> {
        self.patrol_evidence_reader()
            .with_work_patrol_evidence(scope, expected_ticket_commit, since_cursor, |evidence| {
                ready(Ok(evidence))
            })
            .await
    }

    /// Prepare input for the selected diagnostic model from an exact Host read.
    /// The trusted local meter accounts for the protocol's full model-visible
    /// envelope without submitting a request. The final Host fence runs after
    /// metering. The returned input is a read, with no provider admission.
    pub async fn read_work_patrol_diagnostic_input(
        &self,
        through: &Observation,
        expected_ticket_commit: &str,
        since_cursor: u64,
        model: &Model,
        meter: impl FnOnce(&Model, &str, &[EvidenceRef]) -> std::result::Result<u32, PortError>,
    ) -> Result<DiagnosticInput> {
        self.patrol_evidence_reader()
            .with_work_patrol_evidence(
                &through.scope,
                expected_ticket_commit,
                since_cursor,
                |evidence| {
                    ready(
                        prepare_diagnostic_input(
                            &evidence,
                            expected_ticket_commit,
                            since_cursor,
                            through,
                            model,
                            meter,
                        )
                        .map_err(patrol_error),
                    )
                },
            )
            .await
    }

    /// Read and meter the configured patrol prompt. The numeric prompt must be
    /// sent unchanged if a future provider adapter admits this preparation.
    pub async fn read_configured_work_patrol_diagnostic_input(
        &self,
        through: &Observation,
        expected_ticket_commit: &str,
        since_cursor: u64,
    ) -> Result<Option<PreparedPatrolDiagnostic>> {
        self.patrol_evidence_reader()
            .read_configured_work_patrol_diagnostic_input(
                through,
                expected_ticket_commit,
                since_cursor,
            )
            .await
    }
}

impl EvidenceReader<'_> {
    pub(super) async fn read_configured_work_patrol_diagnostic_input(
        &self,
        through: &Observation,
        expected_ticket_commit: &str,
        since_cursor: u64,
    ) -> Result<Option<PreparedPatrolDiagnostic>> {
        let selection = self.config.read().await.patrol_model.clone();
        let Some(selection) =
            selection.filter(|selection| selection.protocol == PatrolModelProtocol::LlamaCppRawV1)
        else {
            return Ok(None);
        };
        self.read_patrol_diagnostic_for_selection(
            through,
            expected_ticket_commit,
            since_cursor,
            selection,
        )
        .await
        .map(Some)
    }

    pub(super) async fn read_patrol_diagnostic_for_selection(
        &self,
        through: &Observation,
        expected_ticket_commit: &str,
        since_cursor: u64,
        selection: PatrolModelConfig,
    ) -> Result<PreparedPatrolDiagnostic> {
        selection.validate().map_err(Error::AuthorityUnavailable)?;
        if selection.protocol != PatrolModelProtocol::LlamaCppRawV1 {
            return Err(patrol_error(PortError::Denied));
        }
        let prepared = self
            .with_work_patrol_evidence(
                &through.scope,
                expected_ticket_commit,
                since_cursor,
                |evidence| {
                    let selection = selection.clone();
                    let through = through.clone();
                    let expected = expected_ticket_commit.to_owned();
                    async move {
                        tokio::task::spawn_blocking(move || {
                            prepare_configured_diagnostic(
                                evidence,
                                &expected,
                                since_cursor,
                                &through,
                                selection,
                            )
                        })
                        .await
                        .map_err(|_| {
                            Error::AuthorityUnavailable("patrol prompt meter stopped".into())
                        })?
                    }
                },
            )
            .await?;
        // The evidence read just fenced Ticket and Project again after metering.
        // Avoid another suspension point before checking the selected profile.
        let config = self
            .config
            .try_read()
            .map_err(|_| Error::RevisionConflict)?;
        if config.patrol_model.as_ref() != Some(&selection) {
            return Err(Error::RevisionConflict);
        }
        Ok(prepared)
    }

    pub(super) async fn read_registered_diagnostic(
        &self,
        through: &Observation,
        expected_commit: &str,
        since_cursor: u64,
        model: &Model,
        input_ceiling: u32,
    ) -> Result<(DiagnosticInput, [Message; 2])> {
        self.with_work_patrol_evidence(&through.scope, expected_commit, since_cursor, |evidence| {
            ready((|| {
                let input = prepare_diagnostic_input_with_ceiling(
                    &evidence,
                    expected_commit,
                    since_cursor,
                    through,
                    model,
                    input_ceiling,
                )
                .map_err(patrol_error)?;
                let (instructions, evidence) =
                    diagnostic_prompt(&input.summary, &input.references).map_err(patrol_error)?;
                Ok((
                    input,
                    [Message::system(instructions), Message::user(evidence)],
                ))
            })())
        })
        .await
    }

    async fn with_work_patrol_evidence<T, F>(
        &self,
        scope: &Scope,
        expected_ticket_commit: &str,
        since_cursor: u64,
        finish: impl FnOnce(UnmeteredWorkPatrolEvidence) -> F,
    ) -> Result<T>
    where
        F: Future<Output = Result<T>>,
    {
        let memory = self.memory_store.clone();
        let project: bamboo_domain::ProjectId = scope
            .project_id
            .parse()
            .map_err(|_| Error::ScopeDenied("invalid Project selector".into()))?;
        self.read_evidence_with(
            scope,
            expected_ticket_commit,
            since_cursor,
            move |pin| {
                let memory = memory.clone();
                let project = project.clone();
                async move {
                    memory
                        .read_project_source_read_only(
                            &project,
                            pin.memory_id(),
                            pin.content_version(),
                        )
                        .await
                }
            },
            finish,
        )
        .await
    }

    async fn project_evidence_manifest(
        &self,
        context: &ProjectContextSnapshot,
    ) -> Result<ProjectManifest> {
        read_project(
            self.project_store.clone(),
            self.tickets.root.clone(),
            context.project_id.clone(),
        )
        .await
    }

    async fn evidence_fence(
        &self,
        service: &Arc<TicketService>,
        scope: &Scope,
        expected: &str,
        plan: &Plan,
        manifest: &ProjectManifest,
    ) -> Result<()> {
        let (fresh, authority) = self.tickets.authority(Principal::Runtime).await?;
        if !Arc::ptr_eq(&fresh, service) {
            return Err(Error::RevisionConflict);
        }
        authorized(&fresh, &authority, scope, expected)?;
        let current = self.project_evidence_manifest(plan.context()).await?;
        plan.validate_project(&current)?;
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
    async fn read_evidence_with<R, F, T, P>(
        &self,
        scope: &Scope,
        expected: &str,
        since: u64,
        mut read: R,
        finish: impl FnOnce(UnmeteredWorkPatrolEvidence) -> P,
    ) -> Result<T>
    where
        R: FnMut(ProjectMemoryRef) -> F,
        F: Future<Output = std::io::Result<ProjectMemoryRead>>,
        P: Future<Output = Result<T>>,
    {
        let (service, authority) = self.tickets.authority(Principal::Runtime).await?;
        let snapshot = authorized(&service, &authority, scope, expected)?;
        let mut plan = Plan::prepare(&snapshot, scope, expected, since)?;
        let pins = native_pins(&plan)?;
        let manifest = self.project_evidence_manifest(plan.context()).await?;
        plan.validate_project(&manifest)?;
        self.evidence_fence(&service, scope, expected, &plan, &manifest)
            .await?;
        for (index, pin) in pins.into_iter().enumerate() {
            if let Some(pin) = pin {
                let outcome = resolve(&pin, read(pin.clone()).await);
                self.evidence_fence(&service, scope, expected, &plan, &manifest)
                    .await?;
                plan.record_source(index, outcome)?;
            } else {
                plan.record_source(index, WorkPatrolSourceOutcome::Unsupported)?;
            }
        }
        let result = finish(plan.finish()?).await?;
        self.evidence_fence(&service, scope, expected, &plan, &manifest)
            .await?;
        Ok(result)
    }
}

async fn read_project(
    projects: Arc<ProjectStore>,
    root: std::path::PathBuf,
    project: bamboo_domain::ProjectId,
) -> Result<ProjectManifest> {
    tokio::task::spawn_blocking(move || read_project_manifest(&projects, &root, &project))
        .await
        .map_err(|_| Error::AuthorityUnavailable("Project evidence reader stopped".into()))?
}

pub(super) fn read_project_manifest(
    projects: &ProjectStore,
    root: &std::path::Path,
    project: &bamboo_domain::ProjectId,
) -> Result<ProjectManifest> {
    if projects.paths().data_dir().canonicalize()? != root.canonicalize()? {
        return Err(Error::ScopeDenied(
            "Project store is outside the current Host".into(),
        ));
    }
    projects
        .get_read_only(project)
        .map_err(|_| Error::AuthorityUnavailable("Project read-only proof unavailable".into()))
}

pub(super) fn current_project_manifest(
    projects: &ProjectStore,
    root: &std::path::Path,
    service: &TicketService,
    authority: &Authority,
    scope: &Scope,
    source_revision: u64,
) -> Result<ProjectManifest> {
    let (commit, _) = service.published()?;
    let snapshot = authorized(service, authority, scope, &commit)?;
    if snapshot.execution_facts_revision() != source_revision {
        return Err(Error::RevisionConflict);
    }
    let plan = Plan::prepare(&snapshot, scope, &commit, 0)?;
    let manifest = read_project_manifest(projects, root, &plan.context().project_id)?;
    plan.validate_project(&manifest)?;
    if service.published()?.1.execution_facts_revision() != source_revision {
        return Err(Error::RevisionConflict);
    }
    Ok(manifest)
}

#[cfg(all(test, unix))]
mod native_tests;
#[cfg(test)]
mod tests;
