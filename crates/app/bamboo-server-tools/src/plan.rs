use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

use bamboo_agent_core::tools::{Tool, ToolCtx, ToolError, ToolOutcome};
use bamboo_engine::session_app::child_session::{
    self, ChildSessionError, ChildSessionPort, CreateChildInput, SubagentResolutionPort,
};

use crate::sub_agent::{waiting_for_children_tool_result, DEFAULT_MAX_SPAWN_DEPTH};

const PLANNER_ROLE: &str = "planner";
const MAX_CONTEXT_FORK_MESSAGES: usize = 12;
const PLANNER_RESPONSIBILITY: &str =
    "Investigate the requested work and return an evidence-grounded implementation plan.";
const PLANNER_BRIEF: &str = r#"Work only as the planning child. Inspect the current workspace and relevant primary sources with the dedicated Read, Glob, Grep, and GetFileInfo tools, but do not implement, edit files, invoke Bash or another shell, run builds or tests, execute repository programs, or change external state. Ambient command and executable resolution is outside the hard read-only boundary. Do not ask the user an interactive question.

Return a concrete plan to the parent that includes:
- verified current-state evidence and the important entry points;
- the recommended design and ordered implementation steps;
- affected files or systems;
- tests and acceptance verification;
- risks, assumptions, and anything the parent must clarify.

If the request is ambiguous, make the smallest reasonable assumption and state it in the result. Stop after reporting the plan."#;

#[derive(Debug, Deserialize)]
struct PlanArgs {
    /// Self-contained planning request. The child does not implicitly inherit
    /// the root transcript.
    task: String,
    #[serde(default)]
    title: Option<String>,
    /// Optional child workspace. Defaults through the same Project-aware path
    /// as `SubAgent.create`.
    #[serde(default)]
    workspace: Option<String>,
    /// Optional bounded parent-context fork for details that are awkward to
    /// repeat in `task`.
    #[serde(default)]
    fork_last_messages: Option<usize>,
}

/// Delegate planning to one runtime-enforced read-only child while the root
/// session remains the normal orchestrator.
pub struct PlanTool {
    sessions: Arc<dyn ChildSessionPort>,
    resolver: Arc<dyn SubagentResolutionPort>,
}

impl PlanTool {
    pub fn new(
        sessions: Arc<dyn ChildSessionPort>,
        resolver: Arc<dyn SubagentResolutionPort>,
    ) -> Self {
        Self { sessions, resolver }
    }
}

fn child_error(error: ChildSessionError) -> ToolError {
    match error {
        ChildSessionError::NotFound(id) => ToolError::Execution(format!("session not found: {id}")),
        ChildSessionError::NotRootSession(id) => {
            ToolError::Execution(format!("session is not a root session: {id}"))
        }
        ChildSessionError::InvalidArguments(message) => ToolError::InvalidArguments(message),
        ChildSessionError::Execution(message) => ToolError::Execution(message),
        other => ToolError::Execution(other.to_string()),
    }
}

fn normalize_task(task: String) -> Result<String, ToolError> {
    let task = task.trim();
    if task.is_empty() {
        return Err(ToolError::InvalidArguments(
            "task must be non-empty".to_string(),
        ));
    }
    Ok(task.to_string())
}

fn plan_title(explicit: Option<String>, task: &str) -> String {
    if let Some(title) = explicit
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
    {
        return title;
    }

    let first_line = task.lines().next().unwrap_or("implementation").trim();
    let mut summary: String = first_line.chars().take(72).collect();
    if first_line.chars().count() > 72 {
        summary.push('…');
    }
    format!("Plan: {summary}")
}

fn planner_assignment(task: &str) -> String {
    format!("{PLANNER_BRIEF}\n\n## Planning request\n{task}")
}

async fn cleanup_planner_child(
    sessions: &dyn ChildSessionPort,
    parent_session_id: &str,
    child_session_id: &str,
    rollback_wait: bool,
) -> Vec<String> {
    let mut failures = Vec::new();
    if rollback_wait {
        if let Err(error) = sessions
            .rollback_parent_wait_for_child(parent_session_id, child_session_id)
            .await
        {
            failures.push(format!("parent-wait rollback failed: {error}"));
        }
    }
    match sessions
        .delete_child_session(parent_session_id, child_session_id)
        .await
    {
        Ok(result) if !result.deleted => {
            failures.push("planner child cleanup reported that no session was deleted".to_string());
        }
        Ok(_) => {}
        Err(error) => failures.push(format!("planner child cleanup failed: {error}")),
    }
    failures
}

fn planner_launch_error(
    stage: &str,
    error: ChildSessionError,
    cleanup_failures: Vec<String>,
) -> ToolError {
    let cleanup = if cleanup_failures.is_empty() {
        String::new()
    } else {
        format!("; cleanup incomplete: {}", cleanup_failures.join("; "))
    };
    ToolError::Execution(format!("failed to {stage} planner child: {error}{cleanup}"))
}

/// Planner authority is currently enforceable only over the typed actor
/// provisioning protocol used by BambooRuntime, Claude Code, and Codex worker
/// executors. Never send a read-only claim to an A2A peer that cannot attest
/// this capability.
fn require_actor_runtime(
    metadata: HashMap<String, String>,
) -> Result<HashMap<String, String>, ToolError> {
    let runtime_kind = metadata.get("runtime.kind").map(String::as_str);
    let protocol = metadata.get("external.protocol").map(String::as_str);
    if runtime_kind == Some("external") && protocol == Some("actor") {
        return Ok(metadata);
    }

    Err(ToolError::Execution(format!(
        "configured planner runtime cannot enforce Bamboo's typed read-only capability (runtime.kind={}, external.protocol={}); configure the planner role with an actor Bamboo/Claude Code/Codex worker",
        runtime_kind.unwrap_or("missing"),
        protocol.unwrap_or("missing")
    )))
}

#[async_trait]
impl Tool for PlanTool {
    fn name(&self) -> &str {
        "Plan"
    }

    fn description(&self) -> &str {
        "Delegate a complex planning request to one runtime-enforced read-only planner child. The parent stays in normal orchestration mode, waits without asking the user for a mode switch, and resumes automatically with the planner's result. Use when the user asks for a plan or when isolated codebase exploration is needed before implementation."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "task": {
                    "type": "string",
                    "description": "Self-contained planning request, including scope, constraints, and expected decision or deliverable."
                },
                "title": {
                    "type": "string",
                    "description": "Optional short title for the planner child shown in the Sub-agents panel."
                },
                "workspace": {
                    "type": "string",
                    "description": "Optional absolute workspace path. Defaults to the parent's validated Project/workspace."
                },
                "fork_last_messages": {
                    "type": "integer",
                    "minimum": 0,
                    "maximum": MAX_CONTEXT_FORK_MESSAGES,
                    "description": "Optional bounded recent parent-context fork. Prefer a self-contained task; use this only when recent discussion is essential."
                }
            },
            "required": ["task"],
            "additionalProperties": false
        })
    }

    async fn invoke(
        &self,
        args: serde_json::Value,
        ctx: ToolCtx,
    ) -> Result<ToolOutcome, ToolError> {
        let parent_session_id = ctx.session_id().ok_or_else(|| {
            ToolError::Execution("Plan requires a session_id in tool context".to_string())
        })?;
        let parsed: PlanArgs = serde_json::from_value(args)
            .map_err(|error| ToolError::InvalidArguments(format!("Invalid Plan args: {error}")))?;
        let task = normalize_task(parsed.task)?;
        let title = plan_title(parsed.title, &task);
        if parsed
            .fork_last_messages
            .is_some_and(|count| count > MAX_CONTEXT_FORK_MESSAGES)
        {
            return Err(ToolError::InvalidArguments(format!(
                "fork_last_messages must be at most {MAX_CONTEXT_FORK_MESSAGES}"
            )));
        }

        let parent = self
            .sessions
            .load_root_session(parent_session_id)
            .await
            .map_err(child_error)?;
        if parent.spawn_depth >= DEFAULT_MAX_SPAWN_DEPTH {
            return Err(ToolError::InvalidArguments(format!(
                "spawn depth limit ({DEFAULT_MAX_SPAWN_DEPTH}) reached: this agent is at depth {} and cannot create a planner child",
                parent.spawn_depth
            )));
        }
        if parent.model.trim().is_empty() {
            return Err(ToolError::Execution(
                "parent session model is empty".to_string(),
            ));
        }

        let explicit_workspace = parsed
            .workspace
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        let workspace_was_explicit = explicit_workspace.is_some();
        let parent_workspace_is_project_default = parent
            .metadata
            .get(bamboo_engine::project_context::WORKSPACE_SOURCE_METADATA_KEY)
            .map(String::as_str)
            == Some(bamboo_engine::project_context::WorkspaceSource::ProjectDefault.as_str());
        let requested_workspace = explicit_workspace
            .or_else(|| {
                (!parent_workspace_is_project_default)
                    .then(|| parent.workspace.clone())
                    .flatten()
            })
            .unwrap_or_default();
        let parent_project_id =
            match bamboo_engine::project_context::ProjectContextResolver::session_project_identity(
                &parent,
            ) {
                bamboo_engine::project_context::SessionProjectIdentity::Assigned(project_id) => {
                    Some(project_id)
                }
                bamboo_engine::project_context::SessionProjectIdentity::Unassigned => None,
                bamboo_engine::project_context::SessionProjectIdentity::Invalid {
                    raw,
                    message,
                } => {
                    return Err(ToolError::InvalidArguments(format!(
                        "parent session carries an invalid Project identity '{raw}': {message}"
                    )));
                }
            };
        let workspace_source = if workspace_was_explicit {
            bamboo_engine::project_context::WorkspaceSource::Explicit
        } else if parent_workspace_is_project_default
            || (requested_workspace.is_empty() && parent_project_id.is_some())
        {
            bamboo_engine::project_context::WorkspaceSource::ProjectDefault
        } else {
            bamboo_engine::project_context::WorkspaceSource::Session
        };
        let workspace = self
            .sessions
            .validate_child_workspace(parent_project_id.as_ref(), &requested_workspace)
            .await
            .map_err(child_error)?;

        let model_ref_override = self.resolver.resolve_subagent_model(PLANNER_ROLE).await;
        let model_override = model_ref_override
            .as_ref()
            .map(|model_ref| model_ref.model.clone());
        let runtime_metadata =
            require_actor_runtime(self.resolver.resolve_runtime_metadata(PLANNER_ROLE).await)?;
        let child_id = Uuid::new_v4().to_string();
        let result = child_session::create_child_action(
            self.sessions.as_ref(),
            CreateChildInput {
                parent_session: parent.clone(),
                child_id: child_id.clone(),
                title: title.clone(),
                responsibility: PLANNER_RESPONSIBILITY.to_string(),
                assignment_prompt: planner_assignment(&task),
                subagent_type: PLANNER_ROLE.to_string(),
                workspace,
                workspace_source,
                model_override,
                model_ref_override,
                runtime_metadata,
                read_only: true,
                // Plan must arm the durable parent wait before the scheduler
                // can run a fast child to completion. Enqueue explicitly below.
                auto_run: false,
                reasoning_effort: None,
                lifecycle: None,
                resident_name: None,
                resident_context: None,
                // Host provisioning derives the authoritative denylist from
                // typed runtime state. Leaving this empty proves Plan does not
                // rely on a cooperative caller to request its own restriction.
                disabled_tools: None,
                context_fork: parsed.fork_last_messages.filter(|count| *count > 0),
            },
        )
        .await
        .map_err(child_error)?;

        self.sessions.ensure_child_indexed(&child_id).await;
        let child = match self
            .sessions
            .load_child_for_parent(&parent.id, &child_id)
            .await
        {
            Ok(child) => child,
            Err(error) => {
                let cleanup =
                    cleanup_planner_child(self.sessions.as_ref(), &parent.id, &child_id, false)
                        .await;
                return Err(planner_launch_error("reload", error, cleanup));
            }
        };
        if let Err(error) = self
            .sessions
            .register_parent_wait_for_child(&parent.id, &child_id, Some(ctx.tool_call_id.as_ref()))
            .await
        {
            let cleanup =
                cleanup_planner_child(self.sessions.as_ref(), &parent.id, &child_id, true).await;
            return Err(planner_launch_error(
                "register the parent wait for",
                error,
                cleanup,
            ));
        }
        if let Err(error) = self.sessions.enqueue_child_run(&parent, &child).await {
            let cleanup =
                cleanup_planner_child(self.sessions.as_ref(), &parent.id, &child_id, true).await;
            return Err(planner_launch_error("enqueue", error, cleanup));
        }
        ctx.emit_tool_token(format!("Delegated plan to read-only child: {child_id}"))
            .await;

        let payload = json!({
            "status": "waiting_for_planner",
            "parent_session_id": parent.id,
            "child_session_id": child_id,
            "title": title,
            "subagent_type": PLANNER_ROLE,
            "model": result.model,
            "read_only": true
        });
        waiting_for_children_tool_result(payload).map(ToolOutcome::Completed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_agent_core::tools::ToolExecutionContext;
    use bamboo_domain::session::runtime_state::ChildWaitPolicy;
    use bamboo_domain::Session;
    use bamboo_engine::session_app::child_session::{
        ChildRunnerInfo, ChildSessionEntry, DeleteChildResult,
    };
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Mutex;

    struct RecordingPlanPort {
        parent: Session,
        child: Mutex<Option<Session>>,
        calls: Mutex<Vec<&'static str>>,
        wait_armed: AtomicBool,
        fail_enqueue: bool,
    }

    impl RecordingPlanPort {
        fn new(workspace: &str, fail_enqueue: bool) -> Self {
            let mut parent = Session::new("plan-parent", "parent-model");
            parent.workspace = Some(workspace.to_string());
            Self {
                parent,
                child: Mutex::new(None),
                calls: Mutex::new(Vec::new()),
                wait_armed: AtomicBool::new(false),
                fail_enqueue,
            }
        }

        fn record(&self, call: &'static str) {
            self.calls.lock().expect("calls lock").push(call);
        }

        fn calls(&self) -> Vec<&'static str> {
            self.calls.lock().expect("calls lock").clone()
        }
    }

    #[async_trait]
    impl ChildSessionPort for RecordingPlanPort {
        async fn validate_child_workspace(
            &self,
            _project_id: Option<&bamboo_domain::ProjectId>,
            requested_workspace: &str,
        ) -> Result<String, ChildSessionError> {
            Ok(requested_workspace.to_string())
        }

        fn publish_child_workspace(
            &self,
            _session_id: &str,
            workspace: std::path::PathBuf,
            _source: &str,
        ) -> std::path::PathBuf {
            workspace
        }

        async fn load_root_session(&self, _root_id: &str) -> Result<Session, ChildSessionError> {
            self.record("load_parent");
            Ok(self.parent.clone())
        }

        async fn load_child_for_parent(
            &self,
            _parent_id: &str,
            _child_id: &str,
        ) -> Result<Session, ChildSessionError> {
            self.record("load_child");
            self.child
                .lock()
                .expect("child lock")
                .clone()
                .ok_or_else(|| ChildSessionError::NotFound("planner child".to_string()))
        }

        async fn save_child_session(&self, child: &mut Session) -> Result<(), ChildSessionError> {
            self.record("save_child");
            *self.child.lock().expect("child lock") = Some(child.clone());
            Ok(())
        }

        async fn save_child_session_authoritative_flags(
            &self,
            _child: &mut Session,
        ) -> Result<(), ChildSessionError> {
            unreachable!("new planner creation uses the ordinary save")
        }

        async fn is_child_running(&self, _child_id: &str) -> bool {
            false
        }

        async fn list_children(&self, _parent_id: &str) -> Vec<ChildSessionEntry> {
            Vec::new()
        }

        async fn enqueue_child_run(
            &self,
            _parent: &Session,
            _child: &Session,
        ) -> Result<(), ChildSessionError> {
            self.record("enqueue_child");
            if !self.wait_armed.load(Ordering::SeqCst) {
                return Err(ChildSessionError::Execution(
                    "planner was enqueued before its parent wait was armed".to_string(),
                ));
            }
            if self.fail_enqueue {
                return Err(ChildSessionError::Execution(
                    "injected scheduler failure".to_string(),
                ));
            }
            Ok(())
        }

        async fn cancel_child_run_and_wait(
            &self,
            _child_id: &str,
        ) -> Result<(), ChildSessionError> {
            unreachable!("an unqueued planner is deleted directly")
        }

        async fn delete_child_session(
            &self,
            _parent_id: &str,
            _child_id: &str,
        ) -> Result<DeleteChildResult, ChildSessionError> {
            self.record("delete_child");
            let deleted = self.child.lock().expect("child lock").take().is_some();
            Ok(DeleteChildResult {
                deleted,
                cancelled_running_child: false,
            })
        }

        async fn get_child_runner_info(&self, _child_id: &str) -> Option<ChildRunnerInfo> {
            None
        }

        async fn register_parent_wait_for_child(
            &self,
            _parent_session_id: &str,
            _child_session_id: &str,
            _tool_call_id: Option<&str>,
        ) -> Result<(), ChildSessionError> {
            self.record("register_wait");
            self.wait_armed.store(true, Ordering::SeqCst);
            Ok(())
        }

        async fn rollback_parent_wait_for_child(
            &self,
            _parent_session_id: &str,
            _child_session_id: &str,
        ) -> Result<(), ChildSessionError> {
            self.record("rollback_wait");
            self.wait_armed.store(false, Ordering::SeqCst);
            Ok(())
        }

        async fn register_parent_wait_for_children(
            &self,
            _parent_session_id: &str,
            _child_session_ids: &[String],
            _policy: ChildWaitPolicy,
        ) -> Result<usize, ChildSessionError> {
            unreachable!("Plan registers one child")
        }

        async fn active_child_ids(&self, _parent_session_id: &str) -> Vec<String> {
            Vec::new()
        }

        async fn find_resident_child(
            &self,
            _root_session_id: &str,
            _resident_name: &str,
        ) -> Option<String> {
            None
        }

        async fn ensure_child_indexed(&self, _child_session_id: &str) {
            self.record("index_child");
        }
    }

    struct ActorPlannerResolver;

    #[async_trait]
    impl SubagentResolutionPort for ActorPlannerResolver {
        async fn resolve_subagent_model(
            &self,
            _subagent_type: &str,
        ) -> Option<bamboo_domain::ProviderModelRef> {
            Some(bamboo_domain::ProviderModelRef::new(
                "openai",
                "planner-model",
            ))
        }

        async fn resolve_runtime_metadata(&self, _subagent_type: &str) -> HashMap<String, String> {
            HashMap::from([
                ("runtime.kind".to_string(), "external".to_string()),
                ("external.protocol".to_string(), "actor".to_string()),
                ("external.agent_id".to_string(), "planner".to_string()),
            ])
        }
    }

    fn plan_test_ctx() -> ToolCtx {
        ToolExecutionContext {
            executing_supervisor: None,
            session_id: Some("plan-parent"),
            root_session_id: None,
            tool_call_id: "plan-call",
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
            auto_approve_permissions: false,
            plan_read_only: false,
            can_async_resume: false,
            bash_completion_sink: None,
            pre_parsed_args: None,
        }
        .to_tool_ctx()
    }

    async fn invoke_recording_plan(port: Arc<RecordingPlanPort>) -> Result<ToolOutcome, ToolError> {
        let tool = PlanTool::new(port, Arc::new(ActorPlannerResolver));
        tool.invoke(
            json!({
                "task": "Inspect the implementation and return a plan.",
                "workspace": std::env::temp_dir().to_string_lossy()
            }),
            plan_test_ctx(),
        )
        .await
    }

    #[test]
    fn title_defaults_to_a_bounded_task_summary() {
        let title = plan_title(None, &"x".repeat(100));
        assert!(title.starts_with("Plan: "));
        assert!(title.ends_with('…'));
        assert!(title.chars().count() <= 79);
    }

    #[test]
    fn assignment_forbids_implementation_and_interactive_questions() {
        let assignment = planner_assignment("Design the migration");
        let normalized = assignment.to_ascii_lowercase();
        assert!(normalized.contains("do not implement"));
        assert!(normalized.contains("do not implement, edit files, invoke bash"));
        assert!(assignment.contains("Read, Glob, Grep, and GetFileInfo"));
        assert!(normalized.contains("ambient command and executable resolution"));
        assert!(normalized.contains("do not ask the user an interactive question"));
        assert!(assignment.contains("Design the migration"));
    }

    #[test]
    fn actor_runtime_is_required_for_typed_read_only_enforcement() {
        let actor = HashMap::from([
            ("runtime.kind".to_string(), "external".to_string()),
            ("external.protocol".to_string(), "actor".to_string()),
            (
                "external.agent_id".to_string(),
                "configured-codex".to_string(),
            ),
        ]);
        assert_eq!(
            require_actor_runtime(actor.clone())
                .unwrap()
                .get("external.agent_id"),
            actor.get("external.agent_id")
        );

        let unsafe_a2a = HashMap::from([
            ("runtime.kind".to_string(), "external".to_string()),
            ("external.protocol".to_string(), "a2a_jsonrpc".to_string()),
        ]);
        assert!(require_actor_runtime(unsafe_a2a).is_err());
    }

    #[tokio::test]
    async fn planner_wait_is_armed_before_the_child_is_enqueued() {
        let workspace = std::env::temp_dir();
        let port = Arc::new(RecordingPlanPort::new(&workspace.to_string_lossy(), false));

        let outcome = invoke_recording_plan(port.clone())
            .await
            .expect("planner launch should succeed when its wait is armed first");
        assert!(matches!(outcome, ToolOutcome::Completed(_)));
        assert!(port.wait_armed.load(Ordering::SeqCst));

        let calls = port.calls();
        let wait = calls
            .iter()
            .position(|call| *call == "register_wait")
            .expect("wait registration call");
        let enqueue = calls
            .iter()
            .position(|call| *call == "enqueue_child")
            .expect("enqueue call");
        assert!(wait < enqueue, "call order was {calls:?}");
    }

    #[tokio::test]
    async fn planner_enqueue_failure_rolls_back_wait_and_deletes_unstarted_child() {
        let workspace = std::env::temp_dir();
        let port = Arc::new(RecordingPlanPort::new(&workspace.to_string_lossy(), true));

        let error = invoke_recording_plan(port.clone())
            .await
            .expect_err("injected scheduler failure must fail the Plan call");
        assert!(error
            .to_string()
            .contains("failed to enqueue planner child"));
        assert!(!port.wait_armed.load(Ordering::SeqCst));
        assert!(port.child.lock().expect("child lock").is_none());

        let calls = port.calls();
        let tail = &calls[calls.len() - 4..];
        assert_eq!(
            tail,
            [
                "register_wait",
                "enqueue_child",
                "rollback_wait",
                "delete_child"
            ]
        );
    }
}
