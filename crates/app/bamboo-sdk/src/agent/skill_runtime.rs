//! Finite Skills authority for the SDK's known, assembled default surface.
//!
//! The real SDK future owns the run guard. Scoped Q is input data, and output
//! caps are budget data; neither creates the host identity held here.

use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, RwLock as SyncRwLock,
};

use async_trait::async_trait;
use bamboo_agent_core::tools::{
    observed_tool_output_cap, ToolCall, ToolCtx, ToolError, ToolExecutionContext, ToolExecutor,
    ToolOutcome, ToolResult, ToolSchema,
};
use bamboo_agent_core::{AgentError, Message, Role, Session, ToolMutability};
use bamboo_domain::{SessionMessageKind, SessionMessageSource, SessionSkillRequest};
use bamboo_engine::config::{SdkSkillExecutionHost, UntrustedExecutionInputs};
use bamboo_engine::runtime::managers::lifecycle::{
    with_scoped_input_request_data, BoundedInputRequestBatch, ProjectedInputRequest,
};
use bamboo_engine::session_app::skill_input::PreparedSkillInput;
use bamboo_engine::{DirectExecutionLease, SessionRepository};
use bamboo_llm::Config;
use bamboo_projects::ProjectStore;
use bamboo_server_tools::skill_runtime::{
    skill_response_byte_budget, SkillCatalogCaller, SkillCatalogCallerResolver,
    SkillCatalogInvocation, SkillInputFactory, SkillInputSession, SkillsListTool, SkillsReadTool,
};
use bamboo_server_tools::OverlayToolExecutor;
use bamboo_skills::progressive::{render_skill_usage_instructions, SkillMetadataBudget};
use bamboo_skills::{SkillManager, WorkflowSelection};
use bamboo_storage::session_merge::SessionLockGuard;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

use super::{Agent, SdkError, SdkSkillInput};

const METADATA_TOKENS: usize = 2_000;
const RESPONSE_BYTES: usize = 512 * 1024;
const MAX_ID_BYTES: usize = 512;

#[derive(Clone)]
pub(super) struct DefaultSdkSkills {
    manager: Arc<SkillManager>,
    config: Arc<RwLock<Config>>,
    sessions: SessionRepository,
    projects: Arc<ProjectStore>,
    // Host policy is separate from the typed input's selections. Only known
    // default assembly constructs this owner; None is not an unknown caller.
    ceiling: Option<BTreeSet<String>>,
    mode: Option<String>,
}

impl DefaultSdkSkills {
    pub(super) fn new(
        manager: Arc<SkillManager>,
        config: Arc<RwLock<Config>>,
        sessions: SessionRepository,
        projects: Arc<ProjectStore>,
    ) -> Self {
        Self {
            manager,
            config,
            sessions,
            projects,
            ceiling: None,
            mode: None,
        }
    }

    pub(super) fn begin_run(
        &self,
        session: &Session,
        execution_id: &str,
        input: &Message,
        request: Option<&SessionSkillRequest>,
        cancel: CancellationToken,
    ) -> Result<SdkSkillRunGuard, SdkError> {
        if !valid_id(&session.id)
            || !valid_id(execution_id)
            || !valid_id(&input.id)
            || input.role != Role::User
        {
            return Err(SdkError::Unsupported(
                "SDK Skill submission identity is invalid".into(),
            ));
        }
        if let Some(request) = request {
            request
                .validate()
                .map_err(|error| SdkError::Unsupported(error.to_string()))?;
            if request.mode != self.mode {
                return Err(SdkError::Unsupported(
                    "SDK Skill request mode differs from host mode".into(),
                ));
            }
        }
        Ok(self.run_guard(
            session,
            Some(execution_id.to_owned()),
            Some(input.id.clone()),
            request.cloned(),
            cancel,
        ))
    }

    pub(super) fn begin_without_input(
        &self,
        session: &Session,
        cancel: CancellationToken,
    ) -> SdkSkillRunGuard {
        self.run_guard(session, None, None, None, cancel)
    }

    fn run_guard(
        &self,
        session: &Session,
        execution_id: Option<String>,
        input_id: Option<String>,
        request: Option<SessionSkillRequest>,
        cancel: CancellationToken,
    ) -> SdkSkillRunGuard {
        SdkSkillRunGuard {
            run: Arc::new(SdkSkillRun {
                defaults: self.clone(),
                session_id: session.id.clone(),
                submitted_input_id: input_id,
                submitted_request: request,
                cancel,
                live: AtomicBool::new(true),
                epoch: AtomicU64::new(0),
                explicit_prepared: AtomicBool::new(false),
                state: SyncRwLock::new(RunState {
                    execution_id,
                    current_input_id: None,
                    epoch: 0,
                }),
            }),
        }
    }

    /// Own the new typed entry's real preappend transaction. The returned lease
    /// is transferred to the existing execution path, never acquired twice.
    pub(super) async fn prepare_typed_run(
        &self,
        agent: &Agent,
        session: &mut Session,
        input: SdkSkillInput,
        cancel: CancellationToken,
    ) -> Result<
        (
            SdkSkillRunGuard,
            UntrustedExecutionInputs,
            DirectExecutionLease,
        ),
        SdkError,
    > {
        let lease = agent.inner.begin_direct_execution(&session.id).await?;
        let preparation = async {
            if cancel.is_cancelled() {
                return Err(SdkError::Unsupported(
                    "SDK Skill submission was cancelled".into(),
                ));
            }
            let user = if input.parts.is_empty() {
                Message::user(input.content)
            } else {
                Message::user_with_parts(input.content, input.parts)
            };
            let request = if input.selections.is_empty() {
                None
            } else {
                // This is bounded intent only. F checks current source/revision,
                // arguments/schema and policy before any ordinary User append.
                let selections = input
                    .selections
                    .iter()
                    .map(|selection| {
                        let source = serde_json::to_value(selection.source)
                            .map_err(|error| SdkError::Unsupported(error.to_string()))?;
                        Ok(bamboo_domain::SessionSkillSelection {
                            id: selection.id.clone(),
                            source: source
                                .as_str()
                                .ok_or_else(|| {
                                    SdkError::Unsupported("SDK Skill source is invalid".into())
                                })?
                                .to_owned(),
                            revision: selection.revision,
                            args: selection.args.clone(),
                        })
                    })
                    .collect::<Result<Vec<_>, SdkError>>()?;
                let request = SessionSkillRequest {
                    selections,
                    mode: self.mode.clone(),
                };
                request
                    .validate()
                    .map_err(|error| SdkError::Unsupported(error.to_string()))?;
                Some(request)
            };
            let owner = self.sessions.persistence().acquire_lock(&session.id).await;
            let checkpoint = self.sessions.storage().load_session(&session.id).await?;
            let mut candidate = session.clone();
            let is_new = checkpoint.is_none();
            if let Some(checkpoint) = checkpoint.as_ref() {
                if session_json(checkpoint)? != session_json(&candidate)? {
                    return Err(SdkError::Unsupported("Typed SDK Skill input requires the exact durable Session; unsaved or staged changes are unsupported".into()));
                }
                if agent.project_id.as_ref().is_some_and(|project| {
                    candidate.project_id_meta().is_none()
                        || candidate.project_id_meta().as_deref() != Some(project.as_str())
                }) {
                    return Err(SdkError::Unsupported("Typed SDK Skill input requires its existing Project assignment to be committed first".into()));
                }
            } else {
                // The actual owned empty Session is the New provenance. An
                // unsaved transcript cannot be relabelled New from absence.
                if !candidate.messages.is_empty()
                    || self.sessions.cache().get(&candidate.id).is_some()
                {
                    return Err(SdkError::Unsupported("Typed SDK Skill input requires a real new empty Session or an exact durable Session".into()));
                }
                if candidate.project_id_meta().is_none() {
                    if let Some(project) = agent.project_id.as_ref() {
                        candidate.set_project_id_meta(project.to_string());
                    }
                }
                if candidate.project_id_meta().is_some() {
                    agent
                        .inner
                        .prepare_external_project_assignment_read_only(&mut candidate)
                        .await?;
                }
            }
            let app_data_dir = agent
                .session_store
                .as_ref()
                .map(|store| store.bamboo_home_dir().to_path_buf());
            let receipt = agent
                .inner
                .precheck_sdk_user_input(&mut candidate, &user, app_data_dir)
                .await?;
            let run = self.begin_run(
                &candidate,
                receipt.execution_id(),
                receipt.user(),
                request.as_ref(),
                cancel.clone(),
            )?;
            let host = if is_new {
                SkillInputSession::New(&candidate)
            } else {
                SkillInputSession::Existing
            };
            let prepared = run
                .prepare_input(
                    receipt.user(),
                    host,
                    &input.selections,
                    &owner,
                    checkpoint.as_ref(),
                    &candidate,
                )
                .await
                .map_err(|error| SdkError::Agent(AgentError::Tool(error.to_string())))?;
            if cancel.is_cancelled() {
                return Err(SdkError::Unsupported(
                    "SDK Skill submission was cancelled".into(),
                ));
            }
            let inputs = agent.inner.append_sdk_user_input(
                &mut candidate,
                prepared.message,
                Some(receipt),
                request,
            )?;
            // F's synchronous final acceptance does not grant later reads. The
            // Engine checkpoints and seals this exact User under its measured
            // UUID, publishing the same repository cache before metadata.
            *session = candidate;
            drop(owner);
            Ok((run, inputs))
        };
        let result = tokio::select! {
            biased;
            _ = cancel.cancelled() => Err(SdkError::Unsupported("SDK Skill submission was cancelled".into())),
            result = preparation => result,
        };
        match result {
            Ok((run, inputs)) => Ok((run, inputs, lease)),
            Err(error) => {
                lease.abandon().await;
                Err(error)
            }
        }
    }
}

fn session_json(session: &Session) -> Result<serde_json::Value, SdkError> {
    serde_json::to_value(session).map_err(|error| SdkError::Unsupported(error.to_string()))
}
fn valid_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= MAX_ID_BYTES && !id.chars().any(char::is_control)
}
fn denied(reason: &str) -> ToolError {
    ToolError::Execution(reason.to_owned())
}
fn engine_error(error: ToolError) -> AgentError {
    AgentError::Tool(error.to_string())
}

struct RunState {
    execution_id: Option<String>,
    current_input_id: Option<String>,
    epoch: u64,
}
struct SdkSkillRun {
    defaults: DefaultSdkSkills,
    session_id: String,
    submitted_input_id: Option<String>,
    submitted_request: Option<SessionSkillRequest>,
    cancel: CancellationToken,
    live: AtomicBool,
    epoch: AtomicU64,
    explicit_prepared: AtomicBool,
    state: SyncRwLock<RunState>,
}

/// This guard belongs to the actual SDK execution future. Host/tool Arc clones
/// cannot extend its authority after that future finishes, drops or unwinds.
pub(super) struct SdkSkillRunGuard {
    run: Arc<SdkSkillRun>,
}
impl Drop for SdkSkillRunGuard {
    fn drop(&mut self) {
        self.run.revoke();
    }
}
impl SdkSkillRunGuard {
    pub(super) fn host(&self) -> Arc<dyn SdkSkillExecutionHost> {
        Arc::new(SdkSkillHost {
            run: self.run.clone(),
        })
    }
    pub(super) fn tools(&self, base: Arc<dyn ToolExecutor>) -> Arc<dyn ToolExecutor> {
        let catalog = self.run.catalog();
        let read = SkillsReadTool::new(self.run.catalog());
        let with_list: Arc<dyn ToolExecutor> =
            Arc::new(OverlayToolExecutor::new(base, Arc::new(catalog)));
        let tools: Arc<dyn ToolExecutor> =
            Arc::new(OverlayToolExecutor::new(with_list, Arc::new(read)));
        Arc::new(ScopedSdkSkillExecutor {
            run: self.run.clone(),
            tools,
        })
    }
    pub(super) async fn prepare_input(
        &self,
        user: &Message,
        host: SkillInputSession<'_>,
        selections: &[WorkflowSelection],
        owner: &SessionLockGuard,
        durable_checkpoint: Option<&Session>,
        candidate: &Session,
    ) -> Result<PreparedSkillInput, ToolError> {
        let mut ctx = ToolCtx::none(format!("sdk-skill-preappend:{}", self.run.execution_id()?));
        ctx.session_id = Some(self.run.session_id.clone().into());
        let factory = SkillInputFactory::new(
            self.run.defaults.manager.clone(),
            self.run.defaults.config.clone(),
            self.run.defaults.sessions.clone(),
            Arc::new(SdkSkillCallerResolver {
                run: self.run.clone(),
            }),
        )
        .with_project_store(self.run.defaults.projects.clone());
        let result = SDK_SKILL_PHASE
            .scope(
                PhaseBinding {
                    run: self.run.clone(),
                    purpose: Purpose::Preappend,
                    call_id: ctx.tool_call_id.clone(),
                },
                factory.prepare_input_with_owner(
                    &ctx,
                    user,
                    host,
                    selections,
                    owner,
                    durable_checkpoint,
                    candidate,
                ),
            )
            .await?;
        self.run.explicit_prepared.store(true, Ordering::Release);
        Ok(result)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Purpose {
    Preappend,
    RoundMetadata,
    ActualDispatch,
}
struct PhaseBinding {
    run: Arc<SdkSkillRun>,
    purpose: Purpose,
    call_id: Arc<str>,
}
tokio::task_local! { static SDK_SKILL_PHASE: PhaseBinding; }

impl SdkSkillRun {
    fn revoke(&self) {
        self.live.store(false, Ordering::Release);
        self.epoch.fetch_add(1, Ordering::AcqRel);
    }
    fn check_live(&self) -> Result<u64, ToolError> {
        if !self.live.load(Ordering::Acquire) || self.cancel.is_cancelled() {
            return Err(denied("SDK Skill caller is no longer active"));
        }
        Ok(self.epoch.load(Ordering::Acquire))
    }
    fn execution_id(&self) -> Result<String, ToolError> {
        self.state
            .try_read()
            .map_err(|_| denied("SDK Skill caller owner is busy"))?
            .execution_id
            .clone()
            .ok_or_else(|| denied("SDK Skill execution identity is unavailable"))
    }
    fn catalog(self: &Arc<Self>) -> SkillsListTool {
        SkillsListTool::new(
            self.defaults.manager.clone(),
            self.defaults.config.clone(),
            self.defaults.sessions.clone(),
            Arc::new(SdkSkillCallerResolver { run: self.clone() }),
        )
        .with_project_store(self.defaults.projects.clone())
    }
    fn phase(&self, ctx: &ToolCtx) -> Result<Purpose, ToolError> {
        SDK_SKILL_PHASE
            .try_with(|binding| {
                if !std::ptr::eq(self, binding.run.as_ref())
                    || binding.call_id.as_ref() != ctx.tool_call_id.as_ref()
                    || ctx.session_id() != Some(self.session_id.as_str())
                {
                    Err(denied(
                        "SDK Skill caller is outside its actual input or dispatch",
                    ))
                } else {
                    Ok(binding.purpose)
                }
            })
            .map_err(|_| denied("SDK Skill caller phase is unavailable"))?
    }
    fn caller(&self, ctx: &ToolCtx, preappend_only: bool) -> Result<SkillCatalogCaller, ToolError> {
        let epoch = self.check_live()?;
        let purpose = self.phase(ctx)?;
        if preappend_only && purpose != Purpose::Preappend {
            return Err(denied(
                "SDK Skill caller is not a real preappend submission",
            ));
        }
        let (execution_id, current_id) = {
            let state = self
                .state
                .try_read()
                .map_err(|_| denied("SDK Skill caller owner is busy"))?;
            if state.epoch != epoch {
                return Err(denied("SDK Skill caller owner update is unavailable"));
            }
            (
                state
                    .execution_id
                    .clone()
                    .ok_or_else(|| denied("SDK Skill execution identity is unavailable"))?,
                state.current_input_id.clone(),
            )
        };
        let (input_id, invocation) = if purpose == Purpose::Preappend {
            let input_id = self
                .submitted_input_id
                .clone()
                .ok_or_else(|| denied("SDK Skill submission input is unavailable"))?;
            let invocation =
                self.submitted_request
                    .as_ref()
                    .map(|request| SkillCatalogInvocation {
                        input_id: input_id.clone(),
                        skills: request
                            .selections
                            .iter()
                            .map(|selection| selection.id.clone())
                            .collect(),
                    });
            (input_id, invocation)
        } else {
            let current_id = current_id
                .ok_or_else(|| denied("SDK Skill accepted current User is unavailable"))?;
            with_scoped_input_request_data(&self.session_id, &execution_id, |batch| {
                let record = batch
                    .records()
                    .last()
                    .ok_or_else(|| denied("SDK Skill current input batch is empty"))?;
                if record.input_id != current_id || !is_user(record) {
                    return Err(denied("SDK Skill current User binding changed"));
                }
                let invocation = if record.request.is_some() {
                    // Only the actual typed producer completed preappend F.
                    // A request on another input cannot reuse that acceptance.
                    if self.submitted_input_id.as_deref() != Some(record.input_id.as_str())
                        || !self.explicit_prepared.load(Ordering::Acquire)
                        || self.submitted_request.as_ref() != record.request.as_ref()
                    {
                        return Err(denied(
                            "SDK explicit Skill input was not prepared by this submission",
                        ));
                    }
                    record
                        .request
                        .as_ref()
                        .map(|request| SkillCatalogInvocation {
                            input_id: record.input_id.clone(),
                            skills: request
                                .selections
                                .iter()
                                .map(|selection| selection.id.clone())
                                .collect(),
                        })
                } else {
                    None
                };
                Ok((record.input_id.clone(), invocation))
            })
            .ok_or_else(|| denied("SDK Skill current input scope is unavailable"))??
        };
        let response_bytes = if purpose == Purpose::ActualDispatch {
            skill_response_byte_budget(RESPONSE_BYTES, observed_tool_output_cap(ctx))?
        } else {
            RESPONSE_BYTES
        };
        let usage =
            render_skill_usage_instructions(SkillMetadataBudget::Tokens(METADATA_TOKENS))
                .ok_or_else(|| denied("SDK Skill usage guidance does not fit its host budget"))?;
        let catalog_tokens = METADATA_TOKENS.saturating_sub(
            SkillMetadataBudget::Tokens(METADATA_TOKENS)
                .cost(usage)
                .saturating_add(1),
        );
        let caller = SkillCatalogCaller {
            caller_id: format!("sdk-default:{execution_id}"),
            session_id: self.session_id.clone(),
            input_id,
            ceiling: self.defaults.ceiling.clone(),
            invocation,
            mode: self.defaults.mode.clone(),
            context_window: None,
            metadata_tokens: NonZeroUsize::new(catalog_tokens),
            response_bytes,
        };
        if self.check_live()? != epoch {
            return Err(denied("SDK Skill caller changed during resolution"));
        }
        Ok(caller)
    }
}
fn is_user(record: &ProjectedInputRequest) -> bool {
    record.kind == SessionMessageKind::UserInput
        && matches!(record.source, SessionMessageSource::User)
        && record.wrapper.is_none()
        && valid_id(&record.input_id)
}

struct SdkSkillCallerResolver {
    run: Arc<SdkSkillRun>,
}
#[async_trait]
impl SkillCatalogCallerResolver for SdkSkillCallerResolver {
    async fn resolve(&self, ctx: &ToolCtx) -> Result<SkillCatalogCaller, ToolError> {
        self.run.caller(ctx, false)
    }
    fn resolve_preappend(&self, ctx: &ToolCtx) -> Result<SkillCatalogCaller, ToolError> {
        self.run.caller(ctx, true)
    }
    fn validate_current(
        &self,
        ctx: &ToolCtx,
        expected: &SkillCatalogCaller,
    ) -> Result<(), ToolError> {
        let current = self.run.caller(ctx, true)?;
        let json = |caller: &SkillCatalogCaller| {
            serde_json::to_value(caller).map_err(|error| denied(&error.to_string()))
        };
        if json(&current)? != json(expected)? {
            return Err(denied("SDK Skill preappend caller changed"));
        }
        Ok(())
    }
}

struct SdkSkillHost {
    run: Arc<SdkSkillRun>,
}
#[async_trait]
impl SdkSkillExecutionHost for SdkSkillHost {
    fn observe_current_inputs(
        &self,
        session_id: &str,
        execution_id: &str,
        current: Option<&BoundedInputRequestBatch>,
    ) -> Result<(), AgentError> {
        // Invalidate before a nonblocking update, including contention/failure.
        let epoch = self.run.epoch.fetch_add(1, Ordering::AcqRel) + 1;
        let mut state = self
            .run
            .state
            .try_write()
            .map_err(|_| engine_error(denied("SDK Skill caller owner is busy")))?;
        state.current_input_id = None;
        state.epoch = epoch;
        if !self.run.live.load(Ordering::Acquire) || self.run.cancel.is_cancelled() {
            return Ok(());
        }
        if session_id != self.run.session_id
            || !valid_id(execution_id)
            || state
                .execution_id
                .as_deref()
                .is_some_and(|expected| expected != execution_id)
        {
            self.run.revoke();
            return Err(engine_error(denied(
                "SDK Skill actual execution identity changed",
            )));
        }
        // An unbound session-only owner learns the measured identity from this
        // genuine Engine callback, never from a transcript or public request.
        state.execution_id = Some(execution_id.to_owned());
        if let Some(batch) = current {
            if batch.session_id() != session_id || batch.execution_id() != execution_id {
                self.run.revoke();
                return Err(engine_error(denied("SDK Skill Q identity changed")));
            }
            state.current_input_id = batch
                .records()
                .last()
                .filter(|record| is_user(record))
                .map(|record| record.input_id.clone());
        }
        Ok(())
    }
    async fn render_skill_prompt(
        &self,
        session: &Session,
        execution_id: &str,
    ) -> Result<String, AgentError> {
        if session.id != self.run.session_id
            || self.run.execution_id().map_err(engine_error)? != execution_id
        {
            return Err(engine_error(denied(
                "SDK Skill metadata execution identity changed",
            )));
        }
        if session.root_orchestration_only_enabled() || self.run.cancel.is_cancelled() {
            return Ok(String::new());
        }
        let current = self
            .run
            .state
            .try_read()
            .map_err(|_| engine_error(denied("SDK Skill caller owner is busy")))?
            .current_input_id
            .is_some();
        if !current {
            return Ok(String::new());
        }
        let mut ctx = ToolCtx::none(format!("sdk-skill-metadata:{execution_id}"));
        ctx.session_id = Some(self.run.session_id.clone().into());
        SDK_SKILL_PHASE
            .scope(
                PhaseBinding {
                    run: self.run.clone(),
                    purpose: Purpose::RoundMetadata,
                    call_id: ctx.tool_call_id.clone(),
                },
                async {
                    let render = self
                        .run
                        .catalog()
                        .render_catalog(&ctx)
                        .await
                        .map_err(engine_error)?;
                    if render.included_count == 0 {
                        return Ok(String::new());
                    }
                    let usage = render_skill_usage_instructions(SkillMetadataBudget::Tokens(
                        METADATA_TOKENS,
                    ))
                    .ok_or_else(|| engine_error(denied("SDK Skill usage guidance does not fit")))?;
                    Ok(format!("{}\n\n{usage}", render.text))
                },
            )
            .await
    }
    fn finish(&self, _session_id: &str, _execution_id: &str) {
        self.run.revoke();
    }
}

struct ScopedSdkSkillExecutor {
    run: Arc<SdkSkillRun>,
    tools: Arc<dyn ToolExecutor>,
}
impl ScopedSdkSkillExecutor {
    fn is_skill(name: &str) -> bool {
        matches!(name, "skills_list" | "skills_read")
    }
    async fn dispatch(
        &self,
        call: &ToolCall,
        name: &str,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolOutcome, ToolError> {
        if !Self::is_skill(name) {
            return self
                .tools
                .execute_exact_with_context_outcome(call, name, ctx)
                .await;
        }
        if ctx.session_id != Some(self.run.session_id.as_str()) || ctx.tool_call_id != call.id {
            return Err(denied("SDK Skill actual dispatch identity is unavailable"));
        }
        self.run.check_live()?;
        SDK_SKILL_PHASE
            .scope(
                PhaseBinding {
                    run: self.run.clone(),
                    purpose: Purpose::ActualDispatch,
                    call_id: call.id.clone().into(),
                },
                self.tools
                    .execute_exact_with_context_outcome(call, name, ctx),
            )
            .await
    }
}
#[async_trait]
impl ToolExecutor for ScopedSdkSkillExecutor {
    async fn execute(&self, call: &ToolCall) -> Result<ToolResult, ToolError> {
        self.execute_with_context(call, ToolExecutionContext::none(&call.id))
            .await
    }
    async fn execute_with_context(
        &self,
        call: &ToolCall,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolResult, ToolError> {
        self.execute_with_context_outcome(call, ctx)
            .await
            .map(ToolOutcome::into_tool_result)
    }
    async fn execute_with_context_outcome(
        &self,
        call: &ToolCall,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolOutcome, ToolError> {
        let name = bamboo_domain::resolve_tool_reference_name(&call.function.name, |name| {
            self.tools.owns_exact_tool(name)
        });
        match name {
            Some(name) => self.dispatch(call, &name, ctx).await,
            None => self.tools.execute_with_context_outcome(call, ctx).await,
        }
    }
    async fn execute_exact_with_context_outcome(
        &self,
        call: &ToolCall,
        name: &str,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolOutcome, ToolError> {
        self.dispatch(call, name, ctx).await
    }
    async fn check_permissions_for(
        &self,
        call: &ToolCall,
        ctx: &ToolExecutionContext<'_>,
    ) -> Result<Option<ToolOutcome>, ToolError> {
        self.tools.check_permissions_for(call, ctx).await
    }
    async fn check_permissions_for_exact(
        &self,
        call: &ToolCall,
        name: &str,
        ctx: &ToolExecutionContext<'_>,
    ) -> Result<Option<ToolOutcome>, ToolError> {
        self.tools
            .check_permissions_for_exact(call, name, ctx)
            .await
    }
    async fn check_permissions_for_resolved(
        &self,
        call: &ToolCall,
        name: &str,
        args: &serde_json::Value,
        ctx: &ToolExecutionContext<'_>,
    ) -> Result<Option<ToolOutcome>, ToolError> {
        self.tools
            .check_permissions_for_resolved(call, name, args, ctx)
            .await
    }
    fn list_tools(&self) -> Vec<ToolSchema> {
        self.tools.list_tools()
    }
    fn owns_exact_tool(&self, name: &str) -> bool {
        self.tools.owns_exact_tool(name)
    }
    fn exact_tool_owner(&self, name: &str) -> Option<&dyn ToolExecutor> {
        self.tools.exact_tool_owner(name)
    }
    fn tool_guidance(&self) -> Option<String> {
        self.tools.tool_guidance()
    }
    fn tool_mutability(&self, name: &str) -> ToolMutability {
        self.tools.tool_mutability(name)
    }
    fn call_mutability(&self, call: &ToolCall) -> ToolMutability {
        self.tools.call_mutability(call)
    }
    fn tool_concurrency_safe(&self, name: &str) -> bool {
        self.tools.tool_concurrency_safe(name)
    }
    fn call_concurrency_safe(&self, call: &ToolCall) -> bool {
        self.tools.call_concurrency_safe(call)
    }
    fn call_parallel_classification(&self, call: &ToolCall) -> (ToolMutability, bool) {
        self.tools.call_parallel_classification(call)
    }
}

#[cfg(test)]
#[path = "skill_runtime_tests.rs"]
mod tests;
