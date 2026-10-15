//! Server services bound to the actual same-process ordinary Child execution.
use super::skill_runtime::{ceiling, mode, permission_mode, workspace_identity};
use async_trait::async_trait;
use bamboo_agent_core::tools::{
    observed_tool_output_cap, ToolCall, ToolCtx, ToolError, ToolExecutionContext, ToolExecutor,
    ToolOutcome, ToolResult, ToolSchema,
};
use bamboo_agent_core::{AgentError, Session, ToolMutability};
use bamboo_domain::{SessionMessageKind, SessionMessageSource, SessionPermissionMode};
use bamboo_engine::config::SkillExecutionHost;
use bamboo_engine::external_agents::local_task::{
    LocalChildSkillAuthority, LocalChildSkillBinding, LocalChildSkillFactory,
};
use bamboo_engine::runtime::managers::lifecycle::{
    with_scoped_input_request_data, BoundedInputRequestBatch, ProjectedInputRequest,
};
use bamboo_server_tools::skill_runtime::{
    skill_response_byte_budget, SkillCatalogCaller, SkillCatalogCallerResolver, SkillsListTool,
    SkillsReadTool,
};
use bamboo_server_tools::{OverlayToolExecutor, ToolSurface, ToolSurfaceFactory};
use bamboo_skills::progressive::{render_skill_usage_instructions, SkillMetadataBudget};
use bamboo_tools::permission::{PermissionChecker, PermissionConfig};
use std::{
    collections::BTreeSet,
    num::NonZeroUsize,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, RwLock,
    },
};

const METADATA_TOKENS: usize = 2000;
const RESPONSE_BYTES: usize = 4096;
fn deny(reason: &str) -> ToolError {
    ToolError::Execution(reason.into())
}
fn agent_error(error: impl std::fmt::Display) -> AgentError {
    AgentError::Tool(error.to_string())
}

pub(crate) struct ServerChildSkillFactory {
    manager: Arc<bamboo_skills::SkillManager>,
    config: Arc<tokio::sync::RwLock<bamboo_llm::Config>>,
    sessions: bamboo_engine::SessionRepository,
    projects: Arc<bamboo_projects::ProjectStore>,
    permission: Arc<dyn PermissionChecker>,
    surfaces: ToolSurfaceFactory,
}
impl ServerChildSkillFactory {
    pub(crate) fn new(
        manager: Arc<bamboo_skills::SkillManager>,
        config: Arc<tokio::sync::RwLock<bamboo_llm::Config>>,
        sessions: bamboo_engine::SessionRepository,
        projects: Arc<bamboo_projects::ProjectStore>,
        permission: Arc<dyn PermissionChecker>,
        surfaces: ToolSurfaceFactory,
    ) -> Self {
        Self {
            manager,
            config,
            sessions,
            projects,
            permission,
            surfaces,
        }
    }
}
#[async_trait]
impl LocalChildSkillFactory for ServerChildSkillFactory {
    async fn bind(
        &self,
        authority: Arc<LocalChildSkillAuthority>,
        session: &Session,
        base: Arc<dyn ToolExecutor>,
    ) -> Result<LocalChildSkillBinding, AgentError> {
        authority.check_session(session)?;
        let registered = self.surfaces.get(ToolSurface::Child);
        if !Arc::ptr_eq(&base, &registered)
            || !base.owns_exact_tool("load_skill")
            || !base.owns_exact_tool("read_skill_resource")
        {
            return Err(agent_error("registered local Child base changed"));
        }
        let permission = self
            .permission
            .permission_config()
            .ok_or_else(|| agent_error("registered Child permission policy unavailable"))?;
        let run = Arc::new(ChildRun {
            authority,
            base,
            surfaces: self.surfaces.clone(),
            permission_checker: self.permission.clone(),
            policy_revision: permission.policy_revision(),
            policy_enabled: permission.is_enabled(),
            policy_mode: permission.mode(),
            permission,
            session_mode: permission_mode(session),
            read_only: readonly(session),
            workspace: workspace_identity(session).map_err(agent_error)?,
            project: session.project_id_meta(),
            ceiling: child_ceiling(session).map_err(agent_error)?,
            mode: mode(session),
            manager: self.manager.clone(),
            config: self.config.clone(),
            sessions: self.sessions.clone(),
            projects: self.projects.clone(),
            live: AtomicBool::new(true),
            epoch: AtomicU64::new(0),
            current: RwLock::new(Current::default()),
        });
        run.check_owner().await.map_err(agent_error)?;
        let listed: Arc<dyn ToolExecutor> = Arc::new(OverlayToolExecutor::new(
            run.base.clone(),
            Arc::new(run.catalog()),
        ));
        let tools = Arc::new(ChildExecutor {
            run: run.clone(),
            tools: Arc::new(OverlayToolExecutor::new(
                listed,
                Arc::new(SkillsReadTool::new(run.catalog())),
            )),
        });
        #[cfg(test)]
        tests::observe_bound(&tools);
        Ok(LocalChildSkillBinding {
            host: Arc::new(ChildHost { run }),
            tools,
        })
    }
}
// The legacy selection parser deliberately collapses []. A finite host
// ceiling must preserve Some(empty), without changing that old public parser.
fn child_ceiling(session: &Session) -> Result<Option<BTreeSet<String>>, ToolError> {
    if session
        .metadata
        .get("selected_skill_ids")
        .is_some_and(|raw| serde_json::from_str::<Vec<String>>(raw).is_ok_and(|ids| ids.is_empty()))
    {
        return Ok(Some(BTreeSet::new()));
    }
    ceiling(session)
}
fn readonly(session: &Session) -> bool {
    session
        .agent_runtime_state
        .as_ref()
        .is_some_and(|r| r.read_only || r.plan_mode.is_some())
}
#[derive(Default)]
struct Current {
    execution: Option<String>,
    input: Option<String>,
    epoch: u64,
}
struct ChildRun {
    authority: Arc<LocalChildSkillAuthority>,
    base: Arc<dyn ToolExecutor>,
    surfaces: ToolSurfaceFactory,
    permission_checker: Arc<dyn PermissionChecker>,
    permission: Arc<PermissionConfig>,
    policy_revision: u64,
    policy_enabled: bool,
    policy_mode: bamboo_domain::PermissionMode,
    session_mode: SessionPermissionMode,
    read_only: bool,
    workspace: Option<PathBuf>,
    project: Option<String>,
    ceiling: Option<BTreeSet<String>>,
    mode: Option<String>,
    manager: Arc<bamboo_skills::SkillManager>,
    config: Arc<tokio::sync::RwLock<bamboo_llm::Config>>,
    sessions: bamboo_engine::SessionRepository,
    projects: Arc<bamboo_projects::ProjectStore>,
    live: AtomicBool,
    epoch: AtomicU64,
    current: RwLock<Current>,
}
impl ChildRun {
    fn revoke(&self) {
        self.live.store(false, Ordering::Release);
        self.epoch.fetch_add(1, Ordering::AcqRel);
    }
    fn check_live(&self) -> Result<u64, ToolError> {
        if !self.live.load(Ordering::Acquire) || self.authority.cancel_token().is_cancelled() {
            return Err(deny("local Child Skills execution ended"));
        }
        let policy = self
            .permission_checker
            .permission_config()
            .ok_or_else(|| deny("Child permission policy disappeared"))?;
        if !Arc::ptr_eq(&policy, &self.permission)
            || policy.policy_revision() != self.policy_revision
            || policy.is_enabled() != self.policy_enabled
            || policy.mode() != self.policy_mode
            || !Arc::ptr_eq(&self.base, &self.surfaces.get(ToolSurface::Child))
        {
            return Err(deny("local Child Skills policy changed"));
        }
        Ok(self.epoch.load(Ordering::Acquire))
    }
    fn check_session(&self, session: &Session) -> Result<(), ToolError> {
        self.authority
            .check_session(session)
            .map_err(|e| deny(&e.to_string()))?;
        if workspace_identity(session)? != self.workspace
            || session.project_id_meta() != self.project
            || child_ceiling(session)? != self.ceiling
            || mode(session) != self.mode
            || permission_mode(session) != self.session_mode
            || readonly(session) != self.read_only
        {
            return Err(deny("local Child Skills scope/restriction changed"));
        }
        Ok(())
    }
    async fn check_owner(&self) -> Result<u64, ToolError> {
        let epoch = self.check_live()?;
        let session = self
            .authority
            .current_session()
            .await
            .map_err(|e| deny(&e.to_string()))?;
        self.check_session(&session)?;
        if self.check_live()? != epoch {
            return Err(deny("local Child Skills owner changed"));
        }
        Ok(epoch)
    }
    fn catalog(self: &Arc<Self>) -> SkillsListTool {
        SkillsListTool::new(
            self.manager.clone(),
            self.config.clone(),
            self.sessions.clone(),
            Arc::new(Resolver { run: self.clone() }),
        )
        .with_project_store(self.projects.clone())
    }
    fn caller(&self, ctx: &ToolCtx) -> Result<SkillCatalogCaller, ToolError> {
        let epoch = self.check_live()?;
        if self.read_only || !self.authority.reader_callable() {
            return Err(deny("local readonly Child has no Skill callable ceiling"));
        }
        let dispatch = CHILD_PHASE
            .try_with(|phase| {
                if !std::ptr::eq(self, phase.run.as_ref())
                    || ctx.session_id() != Some(self.authority.session_id())
                    || ctx.tool_call_id != phase.call
                {
                    return Err(deny("Child Skill call is outside actual dispatch"));
                }
                Ok(phase.dispatch)
            })
            .map_err(|_| deny("Child Skill phase unavailable"))??;
        let (execution, input) = {
            let current = self
                .current
                .try_read()
                .map_err(|_| deny("Child input owner busy"))?;
            if current.epoch != epoch {
                return Err(deny("Child input owner changed"));
            }
            (
                current
                    .execution
                    .clone()
                    .ok_or_else(|| deny("Child execution unavailable"))?,
                current
                    .input
                    .clone()
                    .ok_or_else(|| deny("Child current User unavailable"))?,
            )
        };
        with_scoped_input_request_data(self.authority.session_id(), &execution, |batch| {
            let record = batch
                .records()
                .last()
                .ok_or_else(|| deny("Child input batch empty"))?;
            if record.input_id != input || !ordinary_user(record) {
                return Err(deny("Child current User changed"));
            }
            // Child has no typed preappend producer in this slice. Neither
            // persisted selections nor a request bypasses Source/F validation.
            if record.request.is_some() {
                return Err(deny("Child explicit Skill input was not prepared"));
            }
            Ok(())
        })
        .ok_or_else(|| deny("Child current input scope unavailable"))??;
        let usage = render_skill_usage_instructions(SkillMetadataBudget::Tokens(METADATA_TOKENS))
            .ok_or_else(|| deny("Child usage budget unavailable"))?;
        let tokens = METADATA_TOKENS.saturating_sub(
            SkillMetadataBudget::Tokens(METADATA_TOKENS)
                .cost(usage)
                .saturating_add(1),
        );
        let caller = SkillCatalogCaller {
            caller_id: format!("native-child:{execution}"),
            session_id: self.authority.session_id().into(),
            input_id: input,
            ceiling: self.ceiling.clone(),
            invocation: None,
            mode: self.mode.clone(),
            context_window: None,
            metadata_tokens: NonZeroUsize::new(tokens),
            response_bytes: if dispatch {
                skill_response_byte_budget(RESPONSE_BYTES, observed_tool_output_cap(ctx))?
            } else {
                RESPONSE_BYTES
            },
        };
        if self.check_live()? != epoch {
            return Err(deny("Child caller changed during resolution"));
        }
        Ok(caller)
    }
}
fn ordinary_user(record: &ProjectedInputRequest) -> bool {
    record.kind == SessionMessageKind::UserInput
        && matches!(record.source, SessionMessageSource::User)
        && record.wrapper.is_none()
}
struct Phase {
    run: Arc<ChildRun>,
    call: Arc<str>,
    dispatch: bool,
}
tokio::task_local! { static CHILD_PHASE: Phase; }
struct Resolver {
    run: Arc<ChildRun>,
}
#[async_trait]
impl SkillCatalogCallerResolver for Resolver {
    async fn resolve(&self, ctx: &ToolCtx) -> Result<SkillCatalogCaller, ToolError> {
        self.run.check_owner().await?;
        self.run.caller(ctx)
    }
}
struct ChildHost {
    run: Arc<ChildRun>,
}
#[async_trait]
impl SkillExecutionHost for ChildHost {
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
            .map_err(|_| agent_error("Child input owner busy"))?;
        current.input = None;
        current.epoch = epoch;
        self.run.check_live().map_err(agent_error)?;
        if session_id != self.run.authority.session_id()
            || execution_id.is_empty()
            || current
                .execution
                .as_deref()
                .is_some_and(|id| id != execution_id)
            || batch
                .is_some_and(|b| b.session_id() != session_id || b.execution_id() != execution_id)
        {
            self.run.revoke();
            return Err(agent_error("Child current input identity changed"));
        }
        current.execution = Some(execution_id.into());
        current.input = batch
            .and_then(|b| b.records().last())
            .filter(|r| ordinary_user(r))
            .map(|r| r.input_id.clone());
        Ok(())
    }
    async fn render_skill_prompt(
        &self,
        session: &Session,
        execution_id: &str,
    ) -> Result<String, AgentError> {
        self.run.check_owner().await.map_err(agent_error)?;
        self.run.check_session(session).map_err(agent_error)?;
        let present = {
            let current = self
                .run
                .current
                .try_read()
                .map_err(|_| agent_error("Child input owner busy"))?;
            if current.execution.as_deref() != Some(execution_id) {
                return Err(agent_error("Child metadata execution changed"));
            }
            current.input.is_some()
        };
        if !present || self.run.read_only || !self.run.authority.reader_callable() {
            return Ok(String::new());
        }
        let mut ctx = ToolCtx::none(format!("child-metadata:{execution_id}"));
        ctx.session_id = Some(session.id.clone().into());
        let epoch = self.run.check_live().map_err(agent_error)?;
        let result = CHILD_PHASE
            .scope(
                Phase {
                    run: self.run.clone(),
                    call: ctx.tool_call_id.clone(),
                    dispatch: false,
                },
                async {
                    let render = self
                        .run
                        .catalog()
                        .render_catalog(&ctx)
                        .await
                        .map_err(agent_error)?;
                    if render.included_count == 0 {
                        return Ok(String::new());
                    }
                    let usage = render_skill_usage_instructions(SkillMetadataBudget::Tokens(
                        METADATA_TOKENS,
                    ))
                    .ok_or_else(|| agent_error("Child Skill usage budget unavailable"))?;
                    Ok(format!("{}\n\n{usage}", render.text))
                },
            )
            .await;
        if self.run.check_owner().await.map_err(agent_error)? != epoch {
            return Err(agent_error("Child metadata owner changed"));
        }
        result
    }
    fn finish(&self, _: &str, _: &str) {
        self.run.revoke();
    }
}
struct ChildExecutor {
    run: Arc<ChildRun>,
    tools: Arc<dyn ToolExecutor>,
}
impl ChildExecutor {
    fn check_call(
        &self,
        call: &ToolCall,
        name: &str,
        ctx: &ToolExecutionContext<'_>,
    ) -> Result<(), ToolError> {
        if ctx.session_id != Some(self.run.authority.session_id())
            || ctx.tool_call_id != call.id
            || bamboo_domain::resolve_tool_reference_name(&call.function.name, |n| {
                self.owns_exact_tool(n)
            })
            .as_deref()
                != Some(name)
        {
            return Err(deny("local Child exact call identity changed"));
        }
        Ok(())
    }
    fn legacy(name: &str) -> bool {
        matches!(name, "load_skill" | "read_skill_resource")
    }
    async fn dispatch(
        &self,
        call: &ToolCall,
        name: &str,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolOutcome, ToolError> {
        self.check_call(call, name, &ctx)?;
        if Self::legacy(name) {
            return Err(deny("finite Child uses the scoped Reader"));
        }
        if !matches!(name, "skills_list" | "skills_read") {
            return self
                .tools
                .execute_exact_with_context_outcome(call, name, ctx)
                .await;
        }
        if ctx.session_id != Some(self.run.authority.session_id()) || ctx.tool_call_id != call.id {
            return Err(deny("Child actual dispatch changed"));
        }
        let epoch = self.run.check_owner().await?;
        let result = tokio::select! { biased;
            _ = self.run.authority.cancel_token().cancelled() => Err(deny("Child Skill dispatch cancelled")),
            result = CHILD_PHASE.scope(Phase { run: self.run.clone(), call: call.id.clone().into(), dispatch: true }, self.tools.execute_exact_with_context_outcome(call, name, ctx)) => result,
        };
        if self.run.check_owner().await? != epoch {
            return Err(deny("Child dispatch owner changed"));
        }
        result
    }
}
#[async_trait]
impl ToolExecutor for ChildExecutor {
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
            self.owns_exact_tool(name)
        })
        .ok_or_else(|| ToolError::NotFound(call.function.name.clone()))?;
        self.dispatch(call, &name, ctx).await
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
        let name = bamboo_domain::resolve_tool_reference_name(&call.function.name, |name| {
            self.owns_exact_tool(name)
        })
        .ok_or_else(|| ToolError::NotFound(call.function.name.clone()))?;
        self.check_permissions_for_exact(call, &name, ctx).await
    }
    async fn check_permissions_for_exact(
        &self,
        call: &ToolCall,
        name: &str,
        ctx: &ToolExecutionContext<'_>,
    ) -> Result<Option<ToolOutcome>, ToolError> {
        if Self::legacy(name) {
            return Err(ToolError::NotFound(name.into()));
        }
        // The outer local ceiling supplies canonical overlays such as SubAgent.
        // Keep their existing base permission handoff; only Skills use this
        // adapter's registered owner and Source caller checks.
        if !matches!(name, "skills_list" | "skills_read") {
            return self
                .tools
                .check_permissions_for_exact(call, name, ctx)
                .await;
        }
        self.check_call(call, name, ctx)?;
        self.run.check_owner().await?;
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
        if Self::legacy(name) {
            return Err(ToolError::NotFound(name.into()));
        }
        // The outer local ceiling supplies canonical overlays such as SubAgent.
        // Keep their existing base permission handoff; only Skills use this
        // adapter's registered owner and Source caller checks.
        if !matches!(name, "skills_list" | "skills_read") {
            return self
                .tools
                .check_permissions_for_resolved(call, name, args, ctx)
                .await;
        }
        self.check_call(call, name, ctx)?;
        self.run.check_owner().await?;
        self.tools
            .check_permissions_for_resolved(call, name, args, ctx)
            .await
    }
    fn list_tools(&self) -> Vec<ToolSchema> {
        self.tools
            .list_tools()
            .into_iter()
            .filter(|s| !Self::legacy(&s.function.name))
            .collect()
    }
    fn owns_exact_tool(&self, name: &str) -> bool {
        !Self::legacy(name) && self.tools.owns_exact_tool(name)
    }
    fn exact_tool_owner(&self, name: &str) -> Option<&dyn ToolExecutor> {
        if !self.owns_exact_tool(name) {
            None
        } else if matches!(name, "skills_list" | "skills_read") {
            Some(self)
        } else {
            self.tools.exact_tool_owner(name)
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
#[path = "local_child_skills_tests.rs"]
mod tests;
