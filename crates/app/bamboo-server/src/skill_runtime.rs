//! Native Main adapter over Server's existing read-only Skill surface.
//! Q and transport selections are data. Only this registered Server producer
//! and a genuine execution reservation create the finite caller below.

use crate::{tools::ToolSurface, AppState};
use actix_web::web;
use async_trait::async_trait;
use bamboo_agent_core::tools::{
    observed_tool_output_cap, ToolCall, ToolCtx, ToolError, ToolExecutionContext, ToolExecutor,
    ToolOutcome, ToolResult, ToolSchema,
};
use bamboo_agent_core::{AgentError, Message, Session, ToolMutability};
use bamboo_domain::{
    SessionMessageBody, SessionMessageEnvelope, SessionMessageKind, SessionMessageSource,
    SessionPermissionMode,
};
use bamboo_engine::config::SkillExecutionHost;
use bamboo_engine::execution::SessionExecutionReservation;
use bamboo_engine::runtime::managers::lifecycle::{
    with_scoped_input_request_data, BoundedInputRequestBatch, ProjectedInputRequest,
};
use bamboo_server_tools::skill_runtime::{
    skill_response_byte_budget, SkillCatalogCaller, SkillCatalogCallerResolver,
    SkillCatalogInvocation, SkillInputFactory, SkillInputSession, SkillsListTool, SkillsReadTool,
};
use bamboo_server_tools::OverlayToolExecutor;
use bamboo_skills::progressive::{render_skill_usage_instructions, SkillMetadataBudget};
use bamboo_storage::session_merge::SessionLockGuard;
use bamboo_tools::permission::PermissionConfig;
use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, RwLock as SyncRwLock,
};
use tokio_util::sync::CancellationToken;

const METADATA_TOKENS: usize = 2_000;
// Producer page ceiling, further narrowed by the actual dispatch cap.
const RESPONSE_BYTES: usize = 4 * 1024;

fn denied(reason: &str) -> ToolError {
    ToolError::Execution(reason.into())
}
fn engine_error(error: ToolError) -> AgentError {
    AgentError::Tool(error.to_string())
}

pub(crate) fn ordinary_main(session: &Session) -> bool {
    session.kind == bamboo_domain::SessionKind::Root
        && session.parent_session_id.is_none()
        && session.root_session_id == session.id
        && session.spawn_depth == 0
        && session.authority_identity.is_ordinary()
        && !session.root_orchestration_only_enabled()
}

// Capture the resolved directory once. Re-resolving the original alias on
// every page would let a redirected symlink silently retarget an active run.
fn workspace_identity(session: &Session) -> Result<Option<std::path::PathBuf>, ToolError> {
    session
        .workspace_path_meta()
        .map(|path| {
            let canonical = std::fs::canonicalize(path)
                .map_err(|_| denied("Native canonical workspace is unavailable"))?;
            if !canonical.is_dir() {
                return Err(denied("Native canonical workspace is not a directory"));
            }
            Ok(canonical)
        })
        .transpose()
}

fn ceiling(session: &Session) -> Result<Option<BTreeSet<String>>, ToolError> {
    session
        .metadata
        .get("selected_skill_ids")
        .map(|raw| {
            bamboo_skills::selection::parse_selected_skill_ids_metadata(raw)
                .map(|ids| ids.into_iter().collect())
                .ok_or_else(|| denied("Native Skill restriction is invalid"))
        })
        .transpose()
}
fn mode(session: &Session) -> Option<String> {
    session
        .metadata
        .get("skill_mode")
        .or_else(|| session.metadata.get("mode"))
        .map(|mode| mode.trim())
        .filter(|mode| !mode.is_empty())
        .map(str::to_owned)
}
fn permission_mode(session: &Session) -> SessionPermissionMode {
    session
        .agent_runtime_state
        .as_ref()
        .map(|state| state.effective_permission_mode())
        .unwrap_or_default()
}

#[derive(Clone)]
pub(crate) struct ServerMainSkillProducer {
    skill_manager: Arc<bamboo_skills::SkillManager>,
    config: Arc<tokio::sync::RwLock<bamboo_llm::Config>>,
    session_repo: bamboo_engine::SessionRepository,
    project_store: Arc<bamboo_projects::ProjectStore>,
    storage: Arc<dyn bamboo_agent_core::Storage>,
    permission_checker: Arc<dyn bamboo_tools::permission::PermissionChecker>,
    tool_factory: crate::tools::ToolSurfaceFactory,
    runners:
        Option<Arc<tokio::sync::RwLock<std::collections::HashMap<String, crate::AgentRunner>>>>,
}
impl ServerMainSkillProducer {
    pub(crate) fn new(
        skill_manager: Arc<bamboo_skills::SkillManager>,
        config: Arc<tokio::sync::RwLock<bamboo_llm::Config>>,
        session_repo: bamboo_engine::SessionRepository,
        project_store: Arc<bamboo_projects::ProjectStore>,
        storage: Arc<dyn bamboo_agent_core::Storage>,
        permission_checker: Arc<dyn bamboo_tools::permission::PermissionChecker>,
        tool_factory: crate::tools::ToolSurfaceFactory,
    ) -> Self {
        Self {
            skill_manager,
            config,
            session_repo,
            project_store,
            storage,
            permission_checker,
            tool_factory,
            runners: None,
        }
    }
    pub(crate) fn with_runners(
        mut self,
        runners: Arc<tokio::sync::RwLock<std::collections::HashMap<String, crate::AgentRunner>>>,
    ) -> Self {
        self.runners = Some(runners);
        self
    }
    fn from_state(state: &AppState) -> Self {
        Self::new(
            state.skill_manager.clone(),
            state.config.clone(),
            state.session_repo.clone(),
            state.project_store.clone(),
            state.storage.clone(),
            state.permission_checker.clone(),
            state.tool_factory.clone(),
        )
        .with_runners(state.agent_runners.clone())
    }
    fn tools_for(&self, surface: ToolSurface) -> Arc<dyn ToolExecutor> {
        self.tool_factory.get(surface)
    }
    pub(crate) fn bind(
        &self,
        agent: Arc<bamboo_engine::Agent>,
        session: &Session,
        reservation: &SessionExecutionReservation,
        base: Arc<dyn ToolExecutor>,
    ) -> Result<NativeExecutionBinding, ToolError> {
        bind_registered_execution(Arc::new(self.clone()), agent, session, reservation, base)
    }
}

struct Policy {
    state: Arc<ServerMainSkillProducer>,
    base: Arc<dyn ToolExecutor>,
    permission: Arc<PermissionConfig>,
    revision: u64,
    enabled: bool,
    configured_mode: bamboo_domain::PermissionMode,
    session_id: String,
    root_revision: u64,
    permission_mode: SessionPermissionMode,
    plan: bool,
    workspace: Option<std::path::PathBuf>,
    project: Option<String>,
    ceiling: Option<BTreeSet<String>>,
    mode: Option<String>,
}
impl Policy {
    fn new(state: Arc<ServerMainSkillProducer>, session: &Session) -> Result<Self, ToolError> {
        if !ordinary_main(session) {
            return Err(denied("Native Skills require ordinary Main"));
        }
        let base = state.tools_for(ToolSurface::Root);
        if !base.owns_exact_tool("load_skill") || !base.owns_exact_tool("read_skill_resource") {
            return Err(denied("Registered Server Skill surface is unavailable"));
        }
        let permission = state
            .permission_checker
            .permission_config()
            .ok_or_else(|| denied("Registered Server permission policy is unavailable"))?;
        // This is a revocation fence, not a ReadFile/Source grant. Existing
        // Server read-only and Plan/name policy stays on the actual base tools.
        Ok(Self {
            revision: permission.policy_revision(),
            enabled: permission.is_enabled(),
            configured_mode: permission.mode(),
            permission,
            base,
            session_id: session.id.clone(),
            root_revision: session.root_tool_authority_revision,
            permission_mode: permission_mode(session),
            plan: session
                .agent_runtime_state
                .as_ref()
                .is_some_and(|s| s.plan_mode.is_some()),
            workspace: workspace_identity(session)?,
            project: session.project_id_meta(),
            ceiling: ceiling(session)?,
            mode: mode(session),
            state,
        })
    }
    fn check(&self) -> Result<(), ToolError> {
        let permission = self
            .state
            .permission_checker
            .permission_config()
            .ok_or_else(|| denied("Server permission policy disappeared"))?;
        if !Arc::ptr_eq(&permission, &self.permission)
            || permission.policy_revision() != self.revision
            || permission.is_enabled() != self.enabled
            || permission.mode() != self.configured_mode
            || !Arc::ptr_eq(&self.base, &self.state.tools_for(ToolSurface::Root))
        {
            return Err(denied("Native Skill policy changed"));
        }
        Ok(())
    }
    fn check_execution_session(&self, session: &Session) -> Result<(), ToolError> {
        self.check()?;
        if session.id != self.session_id || !ordinary_main(session) {
            return Err(denied("Native Main identity changed"));
        }
        if session.root_tool_authority_revision != self.root_revision {
            return Err(denied("Native Root tool authority revision changed"));
        }
        if permission_mode(session) != self.permission_mode
            || session
                .agent_runtime_state
                .as_ref()
                .is_some_and(|s| s.plan_mode.is_some())
                != self.plan
        {
            return Err(denied("Native Session permission policy changed"));
        }
        if ceiling(session)? != self.ceiling || mode(session) != self.mode {
            return Err(denied("Native Session Skill restriction changed"));
        }
        Ok(())
    }
    fn check_session(&self, session: &Session) -> Result<(), ToolError> {
        self.check_execution_session(session)?;
        if workspace_identity(session)? != self.workspace
            || session.project_id_meta() != self.project
        {
            return Err(denied("Native canonical Session Source scope changed"));
        }
        Ok(())
    }
    fn catalog(&self, resolver: Arc<dyn SkillCatalogCallerResolver>) -> SkillsListTool {
        SkillsListTool::new(
            self.state.skill_manager.clone(),
            self.state.config.clone(),
            self.state.session_repo.clone(),
            resolver,
        )
        .with_project_store(self.state.project_store.clone())
    }
}

struct Submission {
    policy: Policy,
    caller: SkillCatalogCaller,
}
#[async_trait]
impl SkillCatalogCallerResolver for Submission {
    async fn resolve(&self, _: &ToolCtx) -> Result<SkillCatalogCaller, ToolError> {
        Err(denied("Preappend producer is not a Reader caller"))
    }
    fn resolve_preappend(&self, ctx: &ToolCtx) -> Result<SkillCatalogCaller, ToolError> {
        self.policy.check()?;
        if ctx.session_id() != Some(self.policy.session_id.as_str())
            || ctx.tool_call_id.as_ref() != self.caller.caller_id
        {
            return Err(denied("Native preappend identity changed"));
        }
        Ok(self.caller.clone())
    }
    fn validate_current(
        &self,
        ctx: &ToolCtx,
        expected: &SkillCatalogCaller,
    ) -> Result<(), ToolError> {
        let current = self.resolve_preappend(ctx)?;
        if serde_json::to_value(current).map_err(|e| denied(&e.to_string()))?
            != serde_json::to_value(expected).map_err(|e| denied(&e.to_string()))?
        {
            return Err(denied("Native preappend caller changed"));
        }
        Ok(())
    }
}

pub(crate) async fn prepare_envelope(
    state: web::Data<AppState>,
    candidate: &Session,
    checkpoint: &Session,
    owner: &SessionLockGuard,
    envelope: &mut SessionMessageEnvelope,
    selection: Option<&bamboo_skills::WorkflowSelection>,
) -> Result<(), ToolError> {
    let Some(selection) = selection else {
        return Ok(());
    };
    let policy = Policy::new(
        Arc::new(ServerMainSkillProducer::from_state(&state)),
        candidate,
    )?;
    let user = envelope
        .to_provider_message()
        .map_err(|e| denied(&e.to_string()))?;
    let call_id = format!("native-submit:{}", user.id);
    let caller = SkillCatalogCaller {
        caller_id: call_id.clone(),
        session_id: candidate.id.clone(),
        input_id: user.id.clone(),
        // The existing Server producer may replace a UI selection. That UI
        // restriction is not its authority to select a legal scoped Skill.
        ceiling: None,
        invocation: Some(SkillCatalogInvocation {
            input_id: user.id.clone(),
            skills: [selection.id.clone()].into_iter().collect(),
        }),
        mode: policy.mode.clone(),
        context_window: None,
        metadata_tokens: NonZeroUsize::new(METADATA_TOKENS),
        response_bytes: RESPONSE_BYTES,
    };
    let resolver = Arc::new(Submission { policy, caller });
    let mut ctx = ToolCtx::none(call_id);
    ctx.session_id = Some(candidate.id.clone().into());
    let prepared = SkillInputFactory::new(
        state.skill_manager.clone(),
        state.config.clone(),
        state.session_repo.clone(),
        resolver,
    )
    .with_project_store(state.project_store.clone())
    .prepare_input_with_owner(
        &ctx,
        &user,
        SkillInputSession::Existing,
        std::slice::from_ref(selection),
        owner,
        Some(checkpoint),
        candidate,
    )
    .await?;
    let SessionMessageBody::Content(content) = &mut envelope.body else {
        return Err(denied("Native Skill input is not ordinary content"));
    };
    content.text = prepared.message.content;
    content.parts = prepared.message.content_parts.unwrap_or_default();
    envelope.validate().map_err(|e| denied(&e.to_string()))
}

struct RunState {
    execution_id: Option<String>,
    current_input_id: Option<String>,
    epoch: u64,
}
struct NativeRun {
    policy: Policy,
    reservation_id: String,
    cancel: CancellationToken,
    live: AtomicBool,
    epoch: AtomicU64,
    current: SyncRwLock<RunState>,
}
impl NativeRun {
    fn revoke(&self) {
        self.live.store(false, Ordering::Release);
        self.epoch.fetch_add(1, Ordering::AcqRel);
    }
    fn check_live(&self) -> Result<u64, ToolError> {
        self.policy.check()?;
        if !self.live.load(Ordering::Acquire) || self.cancel.is_cancelled() {
            return Err(denied("Native Skill run is no longer active"));
        }
        // Sync callbacks validate this exact token and host. Async metadata and
        // dispatch also check the actual registry owner, without try-lock denial
        // or synchronous reentry into a busy shared registry.
        Ok(self.epoch.load(Ordering::Acquire))
    }
    async fn check_owner(&self) -> Result<u64, ToolError> {
        let epoch = self.check_live()?;
        let registry = self
            .policy
            .state
            .runners
            .as_ref()
            .ok_or_else(|| denied("Registered Main runner registry is unavailable"))?;
        let runners = tokio::select! { biased;
            _ = self.cancel.cancelled() => return Err(denied("Native runner check was cancelled")),
            runners = registry.read() => runners,
        };
        if !runners.get(&self.policy.session_id).is_some_and(|runner| {
            runner.run_id == self.reservation_id
                && matches!(
                    runner.status,
                    crate::AgentStatus::Pending | crate::AgentStatus::Running
                )
        }) {
            return Err(denied("Native runner identity changed"));
        }
        if self.check_live()? != epoch {
            return Err(denied("Native owner changed during validation"));
        }
        Ok(epoch)
    }
    fn caller(&self, ctx: &ToolCtx) -> Result<SkillCatalogCaller, ToolError> {
        let epoch = self.check_live()?;
        let purpose = NATIVE_PHASE
            .try_with(|phase| {
                if !std::ptr::eq(self, phase.run.as_ref())
                    || phase.call_id.as_ref() != ctx.tool_call_id.as_ref()
                    || ctx.session_id() != Some(self.policy.session_id.as_str())
                {
                    return Err(denied("Native Skill call is outside its actual dispatch"));
                }
                Ok(phase.purpose)
            })
            .map_err(|_| denied("Native Skill dispatch scope is unavailable"))??;
        let (execution_id, input_id) = {
            let current = self
                .current
                .try_read()
                .map_err(|_| denied("Native current input owner is busy"))?;
            if current.epoch != epoch {
                return Err(denied("Native input owner changed"));
            }
            (
                current
                    .execution_id
                    .clone()
                    .ok_or_else(|| denied("Native execution is unavailable"))?,
                current
                    .current_input_id
                    .clone()
                    .ok_or_else(|| denied("Native current User is unavailable"))?,
            )
        };
        let invocation =
            with_scoped_input_request_data(&self.policy.session_id, &execution_id, |batch| {
                let record = batch
                    .records()
                    .last()
                    .ok_or_else(|| denied("Native current input batch is empty"))?;
                if record.input_id != input_id || !is_user(record) {
                    return Err(denied("Native current User binding changed"));
                }
                if record
                    .request
                    .as_ref()
                    .is_some_and(|request| request.mode != self.policy.mode)
                {
                    return Err(denied("Native Skill mode differs from its host"));
                }
                Ok(record
                    .request
                    .as_ref()
                    .map(|request| SkillCatalogInvocation {
                        input_id: input_id.clone(),
                        skills: request.selections.iter().map(|s| s.id.clone()).collect(),
                    }))
            })
            .ok_or_else(|| denied("Native Q scope is unavailable"))??;
        let response_bytes = if purpose == Purpose::Dispatch {
            skill_response_byte_budget(RESPONSE_BYTES, observed_tool_output_cap(ctx))?
        } else {
            RESPONSE_BYTES
        };
        let usage = render_skill_usage_instructions(SkillMetadataBudget::Tokens(METADATA_TOKENS))
            .ok_or_else(|| denied("Native Skill usage does not fit"))?;
        let tokens = METADATA_TOKENS.saturating_sub(
            SkillMetadataBudget::Tokens(METADATA_TOKENS)
                .cost(usage)
                .saturating_add(1),
        );
        let caller = SkillCatalogCaller {
            caller_id: format!("native-main:{}:{execution_id}", self.reservation_id),
            session_id: self.policy.session_id.clone(),
            input_id,
            ceiling: self.policy.ceiling.clone(),
            invocation,
            mode: self.policy.mode.clone(),
            context_window: None,
            metadata_tokens: NonZeroUsize::new(tokens),
            response_bytes,
        };
        if self.check_live()? != epoch {
            return Err(denied("Native caller changed during resolution"));
        }
        Ok(caller)
    }
    fn catalog(self: &Arc<Self>) -> SkillsListTool {
        self.policy
            .catalog(Arc::new(NativeResolver { run: self.clone() }))
    }
}
fn is_user(record: &ProjectedInputRequest) -> bool {
    (record.kind == SessionMessageKind::UserInput
        && matches!(record.source, SessionMessageSource::User)
        && record.wrapper.is_none())
        || (record.kind == SessionMessageKind::RuntimeInstruction
            && matches!(&record.source, SessionMessageSource::Runtime { subsystem } if subsystem == "chat")
            && record.wrapper.as_deref() == Some("root_chat_turn_v1"))
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum Purpose {
    Metadata,
    Dispatch,
}
struct Phase {
    run: Arc<NativeRun>,
    purpose: Purpose,
    call_id: Arc<str>,
}
tokio::task_local! { static NATIVE_PHASE: Phase; }

struct NativeResolver {
    run: Arc<NativeRun>,
}
#[async_trait]
impl SkillCatalogCallerResolver for NativeResolver {
    async fn resolve(&self, ctx: &ToolCtx) -> Result<SkillCatalogCaller, ToolError> {
        self.run.check_owner().await?;
        let caller = self.run.caller(ctx)?;
        // Direct durable read, never cache/owner reentry. The Reader also
        // validates current canonical Source scope and Config for each page.
        let session = self
            .run
            .policy
            .state
            .storage
            .load_session(&caller.session_id)
            .await
            .map_err(|e| denied(&e.to_string()))?
            .ok_or_else(|| denied("Native Session disappeared"))?;
        self.run.policy.check_session(&session)?;
        self.run.check_owner().await?;
        let current = self.run.caller(ctx)?;
        if current.caller_id != caller.caller_id || current.input_id != caller.input_id {
            return Err(denied("Native input changed during Session validation"));
        }
        Ok(current)
    }
}

struct NativeHost {
    run: Arc<NativeRun>,
}
#[async_trait]
impl SkillExecutionHost for NativeHost {
    fn observe_current_inputs(
        &self,
        session_id: &str,
        execution_id: &str,
        batch: Option<&BoundedInputRequestBatch>,
    ) -> Result<(), AgentError> {
        let epoch = self.run.epoch.fetch_add(1, Ordering::AcqRel) + 1;
        let mut current = self
            .run
            .current
            .try_write()
            .map_err(|_| engine_error(denied("Native input owner is busy")))?;
        current.current_input_id = None;
        current.epoch = epoch;
        self.run.check_live().map_err(engine_error)?;
        if session_id != self.run.policy.session_id
            || execution_id.is_empty()
            || current
                .execution_id
                .as_deref()
                .is_some_and(|id| id != execution_id)
            || batch.is_some_and(|batch| {
                batch.session_id() != session_id || batch.execution_id() != execution_id
            })
        {
            self.run.revoke();
            return Err(engine_error(denied("Native execution/Q identity changed")));
        }
        current.execution_id = Some(execution_id.into());
        current.current_input_id = batch
            .and_then(|batch| batch.records().last())
            .filter(|r| is_user(r))
            .map(|r| r.input_id.clone());
        Ok(())
    }
    async fn render_skill_prompt(
        &self,
        session: &Session,
        execution_id: &str,
    ) -> Result<String, AgentError> {
        self.run
            .policy
            .check_execution_session(session)
            .map_err(engine_error)?;
        let has_input = {
            let current = self
                .run
                .current
                .try_read()
                .map_err(|_| engine_error(denied("Native input owner is busy")))?;
            if current.execution_id.as_deref() != Some(execution_id) {
                return Err(engine_error(denied("Native metadata execution changed")));
            }
            current.current_input_id.is_some()
        };
        if !has_input {
            return Ok(String::new());
        }
        let mut ctx = ToolCtx::none(format!("native-metadata:{execution_id}"));
        ctx.session_id = Some(session.id.clone().into());
        NATIVE_PHASE
            .scope(
                Phase {
                    run: self.run.clone(),
                    purpose: Purpose::Metadata,
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
                    .ok_or_else(|| engine_error(denied("Native Skill usage does not fit")))?;
                    Ok(format!("{}\n\n{usage}", render.text))
                },
            )
            .await
    }
    fn finish(&self, _: &str, _: &str) {
        self.run.revoke();
    }
}

pub(crate) type NativeExecutionBinding = (Arc<bamboo_engine::Agent>, Arc<dyn ToolExecutor>);

pub(crate) fn bind_execution(
    state: web::Data<AppState>,
    session: &Session,
    reservation: &SessionExecutionReservation,
    base: Arc<dyn ToolExecutor>,
) -> Result<NativeExecutionBinding, ToolError> {
    if reservation.session_id() != session.id {
        return Err(denied("Native reservation target mismatch"));
    }
    ServerMainSkillProducer::from_state(&state).bind(
        state.agent.clone(),
        session,
        reservation,
        base,
    )
}

fn bind_registered_execution(
    state: Arc<ServerMainSkillProducer>,
    agent: Arc<bamboo_engine::Agent>,
    session: &Session,
    reservation: &SessionExecutionReservation,
    base: Arc<dyn ToolExecutor>,
) -> Result<NativeExecutionBinding, ToolError> {
    if reservation.session_id() != session.id {
        return Err(denied("Native reservation target mismatch"));
    }
    let policy = Policy::new(state, session)?;
    if !Arc::ptr_eq(&base, &policy.base) {
        return Err(denied("Native execution surface changed"));
    }
    let run = Arc::new(NativeRun {
        policy,
        reservation_id: reservation.run_id().into(),
        cancel: reservation.cancel_token().clone(),
        live: AtomicBool::new(true),
        epoch: AtomicU64::new(0),
        current: SyncRwLock::new(RunState {
            execution_id: None,
            current_input_id: None,
            epoch: 0,
        }),
    });
    run.check_live()?;
    let tools =
        Arc::new(OverlayToolExecutor::new(base, Arc::new(run.catalog()))) as Arc<dyn ToolExecutor>;
    let tools = Arc::new(OverlayToolExecutor::new(
        tools,
        Arc::new(SkillsReadTool::new(run.catalog())),
    )) as Arc<dyn ToolExecutor>;
    let agent =
        Arc::new(agent.with_skill_execution_host(Arc::new(NativeHost { run: run.clone() })));
    let tools = Arc::new(NativeExecutor { run, tools });
    #[cfg(test)]
    tests::observe_bound(&tools);
    Ok((agent, tools))
}

struct NativeExecutor {
    run: Arc<NativeRun>,
    tools: Arc<dyn ToolExecutor>,
}
impl NativeExecutor {
    async fn dispatch(
        &self,
        call: &ToolCall,
        name: &str,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolOutcome, ToolError> {
        if matches!(name, "load_skill" | "read_skill_resource") {
            return Err(denied("Native finite Skills use the scoped Reader"));
        }
        if !matches!(name, "skills_list" | "skills_read") {
            return self
                .tools
                .execute_exact_with_context_outcome(call, name, ctx)
                .await;
        }
        if ctx.session_id != Some(self.run.policy.session_id.as_str())
            || ctx.tool_call_id != call.id
        {
            return Err(denied("Native actual dispatch identity is unavailable"));
        }
        let epoch = self.run.check_owner().await?;
        let result = tokio::select! { biased;
            _ = self.run.cancel.cancelled() => Err(denied("Native Skill dispatch was cancelled")),
            result = NATIVE_PHASE.scope(Phase { run: self.run.clone(), purpose: Purpose::Dispatch, call_id: call.id.clone().into() },
                self.tools.execute_exact_with_context_outcome(call, name, ctx)) => result,
        };
        if self.run.check_owner().await? != epoch {
            return Err(denied("Native Skill dispatch owner changed"));
        }
        result
    }
}
#[async_trait]
impl ToolExecutor for NativeExecutor {
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
        match bamboo_domain::resolve_tool_reference_name(&call.function.name, |name| {
            self.tools.owns_exact_tool(name)
        }) {
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
        self.tools
            .list_tools()
            .into_iter()
            .filter(|tool| {
                !matches!(
                    tool.function.name.as_str(),
                    "load_skill" | "read_skill_resource"
                )
            })
            .collect()
    }
    fn owns_exact_tool(&self, name: &str) -> bool {
        !matches!(name, "load_skill" | "read_skill_resource") && self.tools.owns_exact_tool(name)
    }
    fn exact_tool_owner(&self, name: &str) -> Option<&dyn ToolExecutor> {
        if matches!(name, "skills_list" | "skills_read") {
            Some(self)
        } else if self.owns_exact_tool(name) {
            self.tools.exact_tool_owner(name)
        } else {
            None
        }
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
