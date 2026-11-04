//! Concrete evidence, transport and authority I/O for the orchestration pass.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use bamboo_config::{
    Config, LiveSection, ModelLimitsSection, PatrolModelConfig, PatrolModelProtocol,
};
use bamboo_domain::{
    Message, ProjectManifest, ProviderModelRef, Storage, DEFAULT_SUPERVISOR_SESSION_ID,
};
use bamboo_llm::protocol::llama_cpp::{GptOssLocalClient, PreparedCompletion};
use bamboo_llm::{
    work_text::{self, WorkTextReply},
    LLMProvider, ProviderRegistry,
};
use bamboo_orchestration::{
    patrol::{
        budget::TicketBudget,
        calls::{CallIo, ProviderReply},
        staged::{InputDemand, PreparedInput},
        BudgetBlock, BudgetPort, Checkpoint, CheckpointPort, DiagnosticInput, DiagnosticRequest,
        Escalation, EscalationPort, Observation, ObservationPort, PortError, Reservation, Scope,
        StoredCheckpoint, Usage,
    },
    ReadyAdmissionFence,
};
use bamboo_projects::ProjectStore;
use bamboo_tickets::{Authority, Principal, TicketService};
use tokio::sync::RwLock;

use super::{
    evidence::{current_project_manifest, read_project_manifest, EvidenceReader, EvidenceSources},
    patrol_error,
    program::SystemClock,
    Error, Result, TicketApplication,
};

pub(super) struct Ports<'a> {
    pub(super) journal: &'a mut dyn CheckpointPort,
    pub(super) observation: &'a mut dyn ObservationPort,
    pub(super) budget: TicketBudget<'a>,
}

impl CheckpointPort for Ports<'_> {
    fn load(&mut self, scope: &Scope) -> std::result::Result<Option<StoredCheckpoint>, PortError> {
        self.journal.load(scope)
    }
    fn save(
        &mut self,
        version: Option<u64>,
        checkpoint: &Checkpoint,
    ) -> std::result::Result<u64, PortError> {
        self.journal.save(version, checkpoint)
    }
}

impl ObservationPort for Ports<'_> {
    fn observation(
        &mut self,
        scope: &Scope,
        cursor: u64,
    ) -> std::result::Result<Observation, PortError> {
        self.observation.observation(scope, cursor)
    }
    fn diagnostic_input(
        &mut self,
        _: &Scope,
        _: u64,
        _: &Observation,
    ) -> std::result::Result<DiagnosticInput, PortError> {
        Err(PortError::Denied)
    }
}

impl BudgetPort for Ports<'_> {
    fn reserve(
        &mut self,
        request: &DiagnosticRequest,
    ) -> std::result::Result<Reservation, BudgetBlock> {
        self.budget.reserve(request)
    }
    fn settle(
        &mut self,
        reservation: &Reservation,
        usage: Usage,
    ) -> std::result::Result<(), PortError> {
        self.budget.settle(reservation, usage)
    }
}

impl EscalationPort for Ports<'_> {
    fn queue_escalation(&mut self, _: &Escalation) -> std::result::Result<(), PortError> {
        // The existing separate Inbox delivery pass owns this capability.
        Err(PortError::Denied)
    }
}

pub(super) struct PreparedCall {
    pub(super) input: PreparedInput,
    pub(super) io: ReadyPatrolIo,
}

enum Completion {
    Raw(Option<PreparedCompletion>),
    Registered(Box<RegisteredCompletion>),
}

struct RegisteredCompletion {
    route: RegisteredRoute,
    messages: Option<[Message; 2]>,
}

struct RegisteredRoute {
    registry: Arc<ProviderRegistry>,
    provider: Arc<dyn LLMProvider>,
    target: ProviderModelRef,
    model_limits: Option<Arc<LiveSection<ModelLimitsSection>>>,
    model_limits_revision: Option<u64>,
    legacy_model_limits: Option<serde_json::Value>,
}

impl RegisteredRoute {
    fn capture(
        sources: &EvidenceSources,
        config: &Config,
        selection: &PatrolModelConfig,
    ) -> Result<Self> {
        let target = ProviderModelRef::new(&selection.provider, &selection.model);
        let (provider, managed) = sources
            .providers
            .provider_for_configured_model_with_origin(config, &target)
            .map_err(|_| unavailable())?;
        if !managed || provider.known_pre_dispatch_error().is_some() {
            return Err(unavailable());
        }
        let model_limits = sources.model_limits.clone();
        let model_limits_revision = model_limits
            .as_ref()
            .map(|section| section.snapshot().revision);
        Ok(Self {
            registry: sources.providers.clone(),
            provider,
            target,
            model_limits,
            model_limits_revision,
            legacy_model_limits: config.extra.get("model_limits").cloned(),
        })
    }

    fn check(&self, config: &Config) -> std::result::Result<(), PortError> {
        let (current, managed) = self
            .registry
            .provider_for_configured_model_with_origin(config, &self.target)
            .map_err(|_| PortError::Denied)?;
        if !managed
            || !Arc::ptr_eq(&current, &self.provider)
            || self
                .model_limits
                .as_ref()
                .map(|section| section.snapshot().revision)
                != self.model_limits_revision
            || config.extra.get("model_limits") != self.legacy_model_limits.as_ref()
        {
            return Err(PortError::Conflict);
        }
        Ok(())
    }
}

impl TicketApplication {
    pub(in crate::app_state::ticket_application) async fn reconcile_semantic_accounting(
        &self,
        service: &Arc<TicketService>,
        authority: &Authority,
    ) -> Result<()> {
        if !self.patrol_calls.needs_accounting() {
            return Ok(());
        }
        let calls = self.patrol_calls.clone();
        let service = service.clone();
        let authority = authority.clone();
        tokio::task::spawn_blocking(move || {
            let clock = SystemClock::current()?;
            let mut budget = TicketBudget {
                service: &service,
                authority: &authority,
                clock: &clock,
            };
            calls
                .reconcile_accounting(&mut budget)
                .map(|_| ())
                .map_err(patrol_error)
        })
        .await
        .map_err(|_| unavailable())?
    }

    pub(super) async fn prepare_semantic_call(
        &self,
        service: &Arc<TicketService>,
        authority: &Authority,
        demand: InputDemand,
        selection: PatrolModelConfig,
        fence: ReadyAdmissionFence,
    ) -> Result<PreparedCall> {
        fence.check()?;
        let sources = self.patrol_sources.get().ok_or_else(unavailable)?;
        selection.validate().map_err(Error::AuthorityUnavailable)?;
        let (commit, snapshot) = service.published()?;
        fence.check_semantic_scope(&snapshot, &demand.through.scope)?;
        if snapshot.seq != demand.through.cursor
            || snapshot.execution_facts_revision() != demand.through.source_revision
        {
            return Err(Error::RevisionConflict);
        }
        let project = {
            let service = service.clone();
            let authority = authority.clone();
            let projects = sources.projects.clone();
            let root = self.root.clone();
            let through = demand.through.clone();
            tokio::task::spawn_blocking(move || {
                current_project_manifest(
                    &projects,
                    &root,
                    &service,
                    &authority,
                    &through.scope,
                    through.source_revision,
                )
            })
            .await
            .map_err(|_| unavailable())??
        };
        let reader = EvidenceReader {
            tickets: self,
            config: &self.config,
            memory_store: &sources.memory,
            project_store: &sources.projects,
        };
        let (input, completion) = match selection.protocol {
            PatrolModelProtocol::LlamaCppRawV1 => {
                let endpoint = selection.endpoint.ok_or_else(unavailable)?;
                let evidence = reader
                    .read_patrol_diagnostic_for_selection(
                        &demand.through,
                        &commit,
                        demand.since_cursor,
                        selection.clone(),
                    )
                    .await?;
                if evidence.model() != &demand.model {
                    return Err(Error::RevisionConflict);
                }
                let completion = GptOssLocalClient::new(endpoint)
                    .map_err(|_| unavailable())?
                    .prepare(
                        evidence.prompt(),
                        demand.model.max_output_tokens,
                        Duration::from_secs(2),
                    )
                    .await
                    .map_err(|_| unavailable())?;
                (evidence.input().clone(), Completion::Raw(Some(completion)))
            }
            PatrolModelProtocol::RegisteredProviderV1 => {
                let route = {
                    let config = self.config.read().await;
                    if config.patrol_model.as_ref() != Some(&selection) {
                        return Err(Error::RevisionConflict);
                    }
                    RegisteredRoute::capture(sources, &config, &selection)?
                };
                let (input_ceiling, output_ceiling) =
                    bamboo_engine::ticket_token_budget::known_model_ceilings(
                        &self.root,
                        route.model_limits.clone(),
                        route.legacy_model_limits.clone(),
                        &route.target.model,
                        route.provider.as_ref(),
                    )
                    .await
                    .ok_or_else(unavailable)?;
                if input_ceiling > demand.model.max_input_tokens
                    || output_ceiling < demand.model.max_output_tokens
                {
                    return Err(Error::ResourceBlocked(
                        "registered patrol model exceeds the configured token ceilings".into(),
                    ));
                }
                let (input, messages) = reader
                    .read_registered_diagnostic(
                        &demand.through,
                        &commit,
                        demand.since_cursor,
                        &demand.model,
                        input_ceiling,
                    )
                    .await?;
                {
                    let config = self
                        .config
                        .try_read()
                        .map_err(|_| Error::RevisionConflict)?;
                    route.check(&config).map_err(patrol_error)?;
                }
                (
                    input,
                    Completion::Registered(Box::new(RegisteredCompletion {
                        route,
                        messages: Some(messages),
                    })),
                )
            }
        };
        fence.check()?;
        Ok(PreparedCall {
            input: PreparedInput {
                demand,
                result: Ok(input),
            },
            io: ReadyPatrolIo {
                service: service.clone(),
                storage: self.storage.clone(),
                config: self.config.clone(),
                projects: sources.projects.clone(),
                root: self.root.clone(),
                selection,
                project,
                fence,
                completion,
            },
        })
    }
}

fn unavailable() -> Error {
    Error::AuthorityUnavailable("configured patrol provider unavailable".into())
}

pub(super) struct ReadyPatrolIo {
    service: Arc<TicketService>,
    storage: Arc<dyn Storage>,
    config: Arc<RwLock<Config>>,
    projects: Arc<ProjectStore>,
    root: PathBuf,
    selection: PatrolModelConfig,
    project: ProjectManifest,
    fence: ReadyAdmissionFence,
    completion: Completion,
}

impl ReadyPatrolIo {
    async fn authority(&self) -> std::result::Result<Authority, PortError> {
        let (binding, _) = bamboo_engine::ticket_runtime::verified_scope_binding_for_service(
            self.storage.as_ref(),
            &self.service,
            DEFAULT_SUPERVISOR_SESSION_ID,
        )
        .await
        .map_err(|_| PortError::Denied)?;
        Ok(Authority::from_verified_host(binding, Principal::Runtime))
    }

    fn require_selection(&self) -> std::result::Result<(), PortError> {
        self.fence.check().map_err(|_| PortError::Denied)?;
        let current = self.config.try_read().map_err(|_| PortError::Conflict)?;
        if !current.features.ticket_mutation || !current.features.ticket_dispatch {
            return Err(PortError::Denied);
        }
        if current.patrol_model.as_ref() != Some(&self.selection) {
            return Err(PortError::Conflict);
        }
        if let Completion::Registered(completion) = &self.completion {
            completion.route.check(&current)?;
        }
        Ok(())
    }
}

fn check_project_after_ticket_proof(
    projects: &ProjectStore,
    root: &Path,
    expected: &ProjectManifest,
    ticket_proof: impl FnOnce() -> std::result::Result<(), PortError>,
) -> std::result::Result<(), PortError> {
    ticket_proof()?;
    let project =
        read_project_manifest(projects, root, &expected.id).map_err(|_| PortError::Conflict)?;
    if project != *expected {
        return Err(PortError::Conflict);
    }
    Ok(())
}

#[async_trait::async_trait]
impl CallIo for ReadyPatrolIo {
    async fn admit(&self, reservation: &Reservation) -> std::result::Result<(), PortError> {
        self.require_selection()?;
        let authority = self.authority().await?;
        let service = self.service.clone();
        let projects = self.projects.clone();
        let root = self.root.clone();
        let expected_project = self.project.clone();
        let reservation = reservation.clone();
        let fence = self.fence.clone();
        tokio::task::spawn_blocking(move || {
            let scope = &reservation.operation.incident.scope;
            let manifest = current_project_manifest(
                &projects,
                &root,
                &service,
                &authority,
                scope,
                reservation.request.input.source_revision,
            )
            .map_err(|_| PortError::Conflict)?;
            if manifest != expected_project {
                return Err(PortError::Conflict);
            }
            check_project_after_ticket_proof(&projects, &root, &expected_project, || {
                let clock = SystemClock::current().map_err(|_| PortError::Unavailable)?;
                TicketBudget {
                    service: &service,
                    authority: &authority,
                    clock: &clock,
                }
                .confirm_submission(&reservation)?;
                let (_, snapshot) = service.published().map_err(|_| PortError::Unavailable)?;
                if snapshot.execution_facts_revision() != reservation.request.input.source_revision
                {
                    return Err(PortError::Conflict);
                }
                fence
                    .check_semantic_scope(&snapshot, scope)
                    .map_err(|_| PortError::Denied)
            })
        })
        .await
        .map_err(|_| PortError::Unavailable)??;
        self.require_selection()
    }

    async fn complete(
        &mut self,
        remaining: Duration,
    ) -> std::result::Result<ProviderReply, PortError> {
        self.require_selection()?;
        let remaining = remaining.min(Duration::from_secs(60));
        let mut reply = match &mut self.completion {
            Completion::Raw(completion) => {
                let reply = completion
                    .take()
                    .ok_or(PortError::Denied)?
                    .complete(remaining)
                    .await
                    .map_err(|_| PortError::OutcomeUnknown)?;
                ProviderReply {
                    diagnosis: reply.final_text,
                    exact_usage: reply.usage.known(),
                    observed_input_tokens: reply.usage.input_tokens,
                    observed_output_tokens: reply.usage.output_tokens,
                    observed_total_tokens: None,
                }
            }
            Completion::Registered(completion) => {
                let messages = completion.messages.take().ok_or(PortError::Denied)?;
                match work_text::complete(
                    completion.route.provider.as_ref(),
                    &completion.route.target.model,
                    &messages,
                    self.selection.max_input_tokens.get(),
                    self.selection.max_output_tokens.get(),
                    remaining,
                )
                .await
                {
                    WorkTextReply::Complete { text, exact_usage } => ProviderReply {
                        diagnosis: text,
                        exact_usage,
                        observed_input_tokens: None,
                        observed_output_tokens: None,
                        observed_total_tokens: None,
                    },
                    WorkTextReply::OverCap(usage) => ProviderReply {
                        diagnosis: None,
                        exact_usage: None,
                        observed_input_tokens: usage.input_tokens,
                        observed_output_tokens: usage.output_tokens,
                        observed_total_tokens: usage.total_tokens,
                    },
                    WorkTextReply::Unknown => return Err(PortError::OutcomeUnknown),
                }
            }
        };
        if self.require_selection().is_err() {
            reply.diagnosis = None;
        }
        Ok(reply)
    }

    async fn settle(
        &self,
        reservation: &Reservation,
        usage: Usage,
    ) -> std::result::Result<(), PortError> {
        let authority = self.authority().await?;
        let service = self.service.clone();
        let reservation = reservation.clone();
        tokio::task::spawn_blocking(move || {
            let clock = SystemClock::current().map_err(|_| PortError::Unavailable)?;
            TicketBudget {
                service: &service,
                authority: &authority,
                clock: &clock,
            }
            .settle(&reservation, usage)
        })
        .await
        .map_err(|_| PortError::Unavailable)?
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn final_project_proof_rejects_change_during_ticket_proof() {
        let root = tempfile::tempdir().unwrap();
        let root = root.path().canonicalize().unwrap();
        let store = ProjectStore::open(&root).unwrap();
        let original = store.create("Current Project", None).unwrap();
        let result = check_project_after_ticket_proof(&store, &root, &original, || {
            store
                .update(&original.id, original.revision, |manifest| {
                    manifest.name = "Changed during Ticket proof".into();
                    Ok(())
                })
                .unwrap();
            Ok(())
        });
        assert_eq!(result, Err(PortError::Conflict));
    }
}
