use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use bamboo_agent_core::tools::{Tool, ToolClass, ToolCtx, ToolError, ToolOutcome, ToolResult};
use bamboo_domain::{
    StartWorkflowRun, WorkflowBudgets, WorkflowDefinitionBundle, WorkflowProgress,
    WorkflowRunDefinition, WorkflowRunSnapshot,
};
use bamboo_engine::{
    AgentStepPort, AgentStepResult, FileWorkflowRunRepository, NamedAgentSpec, PermissionDecision,
    WorkflowDefinitionPort, WorkflowPolicyPort, WorkflowPolicyTarget, WorkflowRunEngine,
    WorkflowRunError, WorkflowSecretMaterial, WorkflowSecretResolverPort,
};
use bamboo_skills::SkillManager;
use serde::Deserialize;
use serde_json::{json, Value};

const MAX_CONCURRENCY: usize = 8;
const MAX_AGENTS: u32 = 16;
const MAX_STEPS: u32 = 512;
const MAX_RETRIES: u32 = 16;
const MAX_NESTING_DEPTH: u32 = 8;
const MAX_WALL_TIME_MS: u64 = 60 * 60 * 1000;
const MAX_TOKENS: u64 = 2_000_000;
const MAX_COST_MICROS: u64 = 100_000_000;
const MAX_PINNED_DEFINITIONS_PER_RUN: usize = 32;
const MAX_PINNED_BUNDLE_BYTES_PER_RUN: usize = 512 * 1024;
const SAFE_UNTRUSTED_WORKFLOW_TOOLS: &[&str] = &[
    "Read",
    "read_file",
    "GetFileInfo",
    "Glob",
    "list_directory",
    "Grep",
];

/// Server-owned access boundary for workflow runs. Session, workspace trust and
/// capabilities are derived here rather than accepted from HTTP/tool callers.
#[derive(Clone)]
pub struct WorkflowRunAccess {
    engine: Arc<WorkflowRunEngine>,
    skills: Arc<SkillManager>,
    sessions: bamboo_engine::SessionRepository,
}

impl WorkflowRunAccess {
    pub async fn new(
        data_dir: &Path,
        tools: Arc<dyn bamboo_agent_core::tools::ToolExecutor>,
        skills: Arc<SkillManager>,
        sessions: bamboo_engine::SessionRepository,
    ) -> Result<Self, String> {
        let repository = Arc::new(
            FileWorkflowRunRepository::new(data_dir.join("workflow-runs"))
                .map_err(|error| format!("failed to initialize workflow journal: {error}"))?,
        );
        let engine = WorkflowRunEngine::new(
            repository,
            tools,
            Arc::new(UnavailableAgentPort),
            Arc::new(ExternallyPinnedDefinitions),
            Arc::new(ServerWorkflowPolicy),
            Arc::new(UnavailableSecretResolver),
            WorkflowBudgets {
                max_concurrency: MAX_CONCURRENCY,
                max_agents: MAX_AGENTS,
                max_steps: MAX_STEPS,
                max_retries: MAX_RETRIES,
                max_nesting_depth: MAX_NESTING_DEPTH,
                wall_time_ms: MAX_WALL_TIME_MS,
                max_tokens: Some(MAX_TOKENS),
                max_cost_micros: Some(MAX_COST_MICROS),
            },
        );
        engine
            .recover()
            .await
            .map_err(|error| format!("failed to recover workflow journal: {error}"))?;
        Ok(Self {
            engine,
            skills,
            sessions,
        })
    }

    async fn session_context(
        &self,
        session_id: &str,
    ) -> Result<(Option<PathBuf>, bool), WorkflowRunError> {
        let session =
            self.sessions.try_load(session_id).await.map_err(|_| {
                WorkflowRunError::Preflight("session state is unavailable".to_string())
            })?;
        let session = session.ok_or_else(|| {
            WorkflowRunError::Preflight("workflow session does not exist".to_string())
        })?;
        let preferred = session.workspace.map(PathBuf::from);
        let workspace =
            bamboo_agent_core::workspace_state::ensure_session_workspace(session_id, preferred)
                .or_else(|| {
                    // Server bootstrap registers the workspace-root provider, so this
                    // yields a persistent session-scoped directory under data_dir when
                    // the session has no explicit/configured workspace (#217).
                    Some(
                        bamboo_agent_core::workspace_state::workspace_or_process_cwd(Some(
                            session_id,
                        )),
                    )
                });
        // Path resolution and workspace trust are separate authorities. Until
        // #601 provides an explicit server-owned trust decision, never infer
        // trust merely because the resolved path is absolute. Read-only runs
        // remain available through `ServerWorkflowPolicy`; every stronger
        // capability stays fail-closed.
        Ok((workspace, false))
    }

    pub async fn start(
        &self,
        session_id: &str,
        workflow_id: &str,
        revision: u64,
        args: Value,
    ) -> Result<WorkflowRunSnapshot, WorkflowRunError> {
        let (workspace, workspace_trusted) = self.session_context(session_id).await?;
        let bundle = self
            .skills
            .pin_workflow_definition_bundle(workspace.as_deref(), workflow_id, revision)
            .await
            .map_err(|_| WorkflowRunError::Preflight("workflow catalog pin failed".to_string()))?;
        let bundle_bytes = serde_json::to_vec(&bundle)
            .map_err(|_| WorkflowRunError::Preflight("workflow bundle is invalid".to_string()))?
            .len();
        enforce_pinned_bundle_limits(bundle.definitions.len(), bundle_bytes)?;
        let definition = bundle.root().cloned().ok_or_else(|| {
            WorkflowRunError::Preflight("pinned workflow root is missing".to_string())
        })?;
        self.engine
            .start_pinned(
                StartWorkflowRun {
                    definition,
                    args,
                    session_id: session_id.to_string(),
                    workspace_trusted,
                    // #601 is not a production capability authority yet. Grant
                    // only the server-owned read-only class needed by the review
                    // dogfood workflow; the concrete base ToolExecutor still
                    // performs its normal per-tool/per-resource permission gate.
                    allowed_capabilities: vec!["read".to_string()],
                },
                bundle,
            )
            .await
    }

    pub async fn progress_for_session(
        &self,
        session_id: &str,
        run_id: &str,
        since: u64,
    ) -> Result<WorkflowProgress, WorkflowRunError> {
        let progress = self.engine.progress(run_id, since).await?;
        if progress.snapshot.session_id != session_id {
            return Err(WorkflowRunError::NotFound);
        }
        Ok(progress)
    }

    pub async fn cancel_for_session(
        &self,
        session_id: &str,
        run_id: &str,
    ) -> Result<WorkflowRunSnapshot, WorkflowRunError> {
        self.progress_for_session(session_id, run_id, u64::MAX)
            .await?;
        self.engine.cancel(run_id).await
    }

    pub async fn restart_for_session(
        &self,
        session_id: &str,
        run_id: &str,
    ) -> Result<WorkflowRunSnapshot, WorkflowRunError> {
        self.progress_for_session(session_id, run_id, u64::MAX)
            .await?;
        let (_, workspace_trusted) = self.session_context(session_id).await?;
        self.engine
            .restart(run_id, workspace_trusted, vec!["read".to_string()])
            .await
    }
}

fn enforce_pinned_bundle_limits(
    definition_count: usize,
    serialized_bytes: usize,
) -> Result<(), WorkflowRunError> {
    if definition_count > MAX_PINNED_DEFINITIONS_PER_RUN {
        return Err(WorkflowRunError::Preflight(
            "workflow dependency count exceeds the server limit".to_string(),
        ));
    }
    if serialized_bytes > MAX_PINNED_BUNDLE_BYTES_PER_RUN {
        return Err(WorkflowRunError::Preflight(
            "workflow definition bundle exceeds the server size limit".to_string(),
        ));
    }
    Ok(())
}

/// The durable snapshot keeps the complete pinned bundle for deterministic
/// restart, but clients need only the root definition, bundle identity/hash,
/// step tree and events. Avoid echoing every nested definition over HTTP/tools.
pub(crate) fn public_workflow_snapshot(mut snapshot: WorkflowRunSnapshot) -> WorkflowRunSnapshot {
    snapshot.definition_bundle.definitions.clear();
    snapshot
}

/// The catalog adapter above always calls `start_pinned`. Generic engine starts
/// remain unavailable so no future server call site can accidentally re-read a
/// live definition or mix publications.
struct ExternallyPinnedDefinitions;

#[async_trait]
impl WorkflowDefinitionPort for ExternallyPinnedDefinitions {
    async fn pin_bundle(
        &self,
        _root: &WorkflowRunDefinition,
    ) -> Result<WorkflowDefinitionBundle, String> {
        Err("server workflows must be pinned through SkillManager".to_string())
    }
}

/// #563 named-agent registry integration is not complete. Unknown and named
/// agents therefore fail preflight rather than falling back to a dynamic agent.
struct UnavailableAgentPort;

#[async_trait]
impl AgentStepPort for UnavailableAgentPort {
    async fn resolve(&self, _name: &str) -> Result<Option<NamedAgentSpec>, String> {
        Ok(None)
    }

    async fn execute(
        &self,
        _spec: &NamedAgentSpec,
        _prompt: Value,
        _model: Option<&str>,
        _effort: Option<&str>,
        _capabilities: &BTreeSet<String>,
        _session_id: &str,
    ) -> Result<AgentStepResult, String> {
        Err("named-agent execution is not available".to_string())
    }
}

struct ServerWorkflowPolicy;

#[async_trait]
impl WorkflowPolicyPort for ServerWorkflowPolicy {
    async fn authorize(
        &self,
        _session_id: &str,
        target: &WorkflowPolicyTarget,
        requested: &BTreeSet<String>,
        _workspace_trusted: bool,
    ) -> PermissionDecision {
        // Definition-declared capabilities are descriptive input, not an
        // authority. Until #601 provides a server-owned capability registry,
        // bind authorization to this strict server-owned target allowlist too.
        // Unknown/MCP/network/script/write targets therefore fail closed even
        // when hostile YAML claims `capabilities: []` or `[read]`.
        match target {
            // The reference itself belongs to the same immutable, validated
            // bundle. Every concrete step in the nested definition is still
            // authorized independently during this preflight walk.
            WorkflowPolicyTarget::Workflow { .. } if requested.is_empty() => {
                PermissionDecision::Allow
            }
            WorkflowPolicyTarget::Tool(name) => {
                let safe_target = SAFE_UNTRUSTED_WORKFLOW_TOOLS
                    .iter()
                    .any(|candidate| candidate.eq_ignore_ascii_case(name));
                if safe_target && requested.iter().all(|capability| capability == "read") {
                    PermissionDecision::Allow
                } else {
                    PermissionDecision::Deny(
                        "workflow capability authority is not available".to_string(),
                    )
                }
            }
            WorkflowPolicyTarget::Agent(_) | WorkflowPolicyTarget::Workflow { .. } => {
                PermissionDecision::Deny(
                    "workflow capability authority is not available".to_string(),
                )
            }
        }
    }
}

struct UnavailableSecretResolver;

#[async_trait]
impl WorkflowSecretResolverPort for UnavailableSecretResolver {
    async fn resolve(
        &self,
        _session_id: &str,
        _capability: &str,
    ) -> Result<WorkflowSecretMaterial, String> {
        Err("workflow secret capability resolver is not available".to_string())
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
enum WorkflowToolInput {
    Start {
        workflow_id: String,
        revision: u64,
        #[serde(default)]
        args: Value,
    },
    Get {
        run_id: String,
    },
    Events {
        run_id: String,
        #[serde(default)]
        since: u64,
    },
    Cancel {
        run_id: String,
    },
    Restart {
        run_id: String,
    },
}

pub struct WorkflowRunTool {
    access: WorkflowRunAccess,
}

impl WorkflowRunTool {
    pub fn new(access: WorkflowRunAccess) -> Self {
        Self { access }
    }
}

#[async_trait]
impl Tool for WorkflowRunTool {
    fn name(&self) -> &str {
        "workflow_run"
    }

    fn description(&self) -> &str {
        "Start, inspect, cancel, or safely restart a catalog-pinned workflow run"
    }

    fn parameters_schema(&self) -> Value {
        json!({
            "type": "object",
            "oneOf": [
                {"properties": {"action": {"const": "start"}, "workflow_id": {"type": "string"}, "revision": {"type": "integer", "minimum": 0}, "args": {}}, "required": ["action", "workflow_id", "revision"], "additionalProperties": false},
                {"properties": {"action": {"const": "get"}, "run_id": {"type": "string"}}, "required": ["action", "run_id"], "additionalProperties": false},
                {"properties": {"action": {"const": "events"}, "run_id": {"type": "string"}, "since": {"type": "integer", "minimum": 0}}, "required": ["action", "run_id"], "additionalProperties": false},
                {"properties": {"action": {"const": "cancel"}, "run_id": {"type": "string"}}, "required": ["action", "run_id"], "additionalProperties": false},
                {"properties": {"action": {"const": "restart"}, "run_id": {"type": "string"}}, "required": ["action", "run_id"], "additionalProperties": false}
            ]
        })
    }

    fn classify(&self, args: &Value) -> ToolClass {
        match args.get("action").and_then(Value::as_str) {
            Some("get" | "events") => ToolClass::READONLY_PARALLEL,
            _ => ToolClass::MUTATING_SERIAL,
        }
    }

    async fn invoke(&self, args: Value, ctx: ToolCtx) -> Result<ToolOutcome, ToolError> {
        let input: WorkflowToolInput = serde_json::from_value(args)
            .map_err(|error| ToolError::InvalidArguments(error.to_string()))?;
        let session_id = ctx.session_id().ok_or_else(|| {
            ToolError::InvalidArguments("workflow_run requires a session".to_string())
        })?;
        let result = match input {
            WorkflowToolInput::Start {
                workflow_id,
                revision,
                args,
            } => serde_json::to_value(public_workflow_snapshot(
                self.access
                    .start(session_id, &workflow_id, revision, args)
                    .await
                    .map_err(workflow_tool_error)?,
            )),
            WorkflowToolInput::Get { run_id } => {
                let progress = self
                    .access
                    .progress_for_session(session_id, &run_id, u64::MAX)
                    .await
                    .map_err(workflow_tool_error)?;
                serde_json::to_value(public_workflow_snapshot(progress.snapshot))
            }
            WorkflowToolInput::Events { run_id, since } => {
                let progress = self
                    .access
                    .progress_for_session(session_id, &run_id, since)
                    .await
                    .map_err(workflow_tool_error)?;
                serde_json::to_value(progress.events)
            }
            WorkflowToolInput::Cancel { run_id } => serde_json::to_value(public_workflow_snapshot(
                self.access
                    .cancel_for_session(session_id, &run_id)
                    .await
                    .map_err(workflow_tool_error)?,
            )),
            WorkflowToolInput::Restart { run_id } => {
                serde_json::to_value(public_workflow_snapshot(
                    self.access
                        .restart_for_session(session_id, &run_id)
                        .await
                        .map_err(workflow_tool_error)?,
                ))
            }
        }
        .map_err(|error| ToolError::Execution(error.to_string()))?;
        Ok(ToolOutcome::Completed(ToolResult::text(
            true,
            serde_json::to_string(&result)
                .map_err(|error| ToolError::Execution(error.to_string()))?,
        )))
    }
}

fn workflow_tool_error(error: WorkflowRunError) -> ToolError {
    match error {
        WorkflowRunError::InvalidInput(message) => ToolError::InvalidArguments(message),
        WorkflowRunError::Compile(error) => ToolError::InvalidArguments(error.to_string()),
        other => ToolError::Execution(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tool_input_rejects_security_context_spoofing() {
        let error = serde_json::from_value::<WorkflowToolInput>(json!({
            "action": "start",
            "workflow_id": "safe",
            "revision": 1,
            "workspace_trusted": true
        }))
        .unwrap_err();
        assert!(error.to_string().contains("unknown field"));
    }

    #[tokio::test]
    async fn production_policy_allows_read_without_fabricating_workspace_trust() {
        let read = BTreeSet::from(["read".to_string()]);
        assert_eq!(
            ServerWorkflowPolicy
                .authorize(
                    "session",
                    &WorkflowPolicyTarget::Tool("read_file".to_string()),
                    &read,
                    false,
                )
                .await,
            PermissionDecision::Allow
        );

        let write = BTreeSet::from(["write".to_string()]);
        assert!(matches!(
            ServerWorkflowPolicy
                .authorize(
                    "session",
                    &WorkflowPolicyTarget::Tool("write_file".to_string()),
                    &write,
                    false,
                )
                .await,
            PermissionDecision::Deny(_)
        ));

        for hostile_target in [
            "Write",
            "write_file",
            "WebFetch",
            "mcp::remote_tool",
            "Bash",
        ] {
            for claimed in [BTreeSet::new(), read.clone()] {
                assert!(matches!(
                    ServerWorkflowPolicy
                        .authorize(
                            "session",
                            &WorkflowPolicyTarget::Tool(hostile_target.to_string()),
                            &claimed,
                            false,
                        )
                        .await,
                    PermissionDecision::Deny(_)
                ));
            }
        }

        assert_eq!(
            ServerWorkflowPolicy
                .authorize(
                    "session",
                    &WorkflowPolicyTarget::Workflow {
                        id: "nested-review".to_string(),
                        revision: 1,
                    },
                    &BTreeSet::new(),
                    false,
                )
                .await,
            PermissionDecision::Allow
        );
    }

    #[test]
    fn pinned_bundle_limits_reject_oversized_runs_before_engine_start() {
        assert!(enforce_pinned_bundle_limits(
            MAX_PINNED_DEFINITIONS_PER_RUN,
            MAX_PINNED_BUNDLE_BYTES_PER_RUN
        )
        .is_ok());
        assert!(enforce_pinned_bundle_limits(
            MAX_PINNED_DEFINITIONS_PER_RUN + 1,
            MAX_PINNED_BUNDLE_BYTES_PER_RUN
        )
        .is_err());
        assert!(enforce_pinned_bundle_limits(
            MAX_PINNED_DEFINITIONS_PER_RUN,
            MAX_PINNED_BUNDLE_BYTES_PER_RUN + 1
        )
        .is_err());
    }
}
