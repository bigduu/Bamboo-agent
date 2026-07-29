use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
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
const MAX_WORKFLOW_RUN_IDS_PER_SESSION: usize = 256;
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
        budget: Option<WorkflowBudgets>,
    ) -> Result<WorkflowRunSnapshot, WorkflowRunError> {
        self.start_for_invoker(session_id, workflow_id, revision, args, budget, false)
            .await
    }

    pub async fn start_from_tool(
        &self,
        session_id: &str,
        workflow_id: &str,
        revision: u64,
        args: Value,
        budget: Option<WorkflowBudgets>,
    ) -> Result<WorkflowRunSnapshot, WorkflowRunError> {
        self.start_for_invoker(session_id, workflow_id, revision, args, budget, true)
            .await
    }

    async fn start_for_invoker(
        &self,
        session_id: &str,
        workflow_id: &str,
        revision: u64,
        args: Value,
        budget: Option<WorkflowBudgets>,
        model_started: bool,
    ) -> Result<WorkflowRunSnapshot, WorkflowRunError> {
        self.ensure_run_index_capacity(session_id).await?;
        let (workspace, workspace_trusted) = self.session_context(session_id).await?;
        let store = self
            .skills
            .store_for_workspace(workspace.as_deref())
            .await
            .map_err(|_| {
                WorkflowRunError::Preflight("workflow catalog is unavailable".to_string())
            })?;
        let catalog = store.workflow_catalog_snapshot().await;
        let entry = catalog
            .entries
            .iter()
            .find(|entry| {
                entry.winner
                    && entry.id == workflow_id
                    && entry.revision == revision
                    && entry.status == bamboo_skills::WorkflowStatus::Valid
            })
            .ok_or_else(|| {
                WorkflowRunError::Preflight(
                    "requested workflow revision is unavailable".to_string(),
                )
            })?;
        if entry.kind != bamboo_skills::WorkflowKind::Orchestration {
            return Err(WorkflowRunError::Preflight(
                "instruction workflows must be activated with load_skill".to_string(),
            ));
        }
        if model_started {
            let session = self
                .sessions
                .try_load(session_id)
                .await
                .map_err(|_| {
                    WorkflowRunError::Preflight("session state is unavailable".to_string())
                })?
                .ok_or(WorkflowRunError::NotFound)?;
            let opted_in = session
                .metadata
                .get(bamboo_skills::WORKFLOW_ORCHESTRATION_OPT_IN_METADATA_KEY)
                .is_some_and(|value| value.eq_ignore_ascii_case("true"));
            if !opted_in {
                return Err(WorkflowRunError::Preflight(
                    "model-started orchestration requires explicit session opt-in".to_string(),
                ));
            }
        }
        let mut bundle = self
            .skills
            .pin_workflow_definition_bundle(workspace.as_deref(), workflow_id, revision)
            .await
            .map_err(|_| WorkflowRunError::Preflight("workflow catalog pin failed".to_string()))?;
        let policy = if model_started {
            "automatic"
        } else {
            "explicit"
        };
        if bundle.root_invocation_policy[policy].as_bool() != Some(true) {
            return Err(WorkflowRunError::Preflight(format!(
                "pinned workflow invocation policy denies {policy} start"
            )));
        }
        let bundle_bytes = serde_json::to_vec(&bundle)
            .map_err(|_| WorkflowRunError::Preflight("workflow bundle is invalid".to_string()))?
            .len();
        enforce_pinned_bundle_limits(bundle.definitions.len(), bundle_bytes)?;
        let mut definition = bundle.root().cloned().ok_or_else(|| {
            WorkflowRunError::Preflight("pinned workflow root is missing".to_string())
        })?;
        if let Some(requested) = budget {
            validate_requested_budget(&requested)?;
            definition.budgets = tighten_workflow_budget(&definition.budgets, &requested);
            let root_key = WorkflowDefinitionBundle::key(&definition.id, definition.revision);
            bundle.definitions.insert(root_key, definition.clone());
        }
        let snapshot = self
            .engine
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
            .await?;
        if let Err(error) = self.remember_run_id(session_id, &snapshot.run_id).await {
            return match self.engine.cancel(&snapshot.run_id).await {
                Ok(cancelled) if cancelled.status.is_terminal() => Err(WorkflowRunError::Storage(
                    format!(
                        "run index persistence failed; run {} reached terminal {:?}: {error}",
                        snapshot.run_id, cancelled.status
                    ),
                )),
                Ok(cancelled) => Err(WorkflowRunError::Storage(format!(
                    "run index persistence failed; orphan run {} remains {:?}; repair with this run id: {error}",
                    snapshot.run_id, cancelled.status
                ))),
                Err(cancel_error) => Err(WorkflowRunError::Storage(format!(
                    "run index persistence failed; orphan run {} could not be cancelled ({cancel_error}); repair with this run id: {error}",
                    snapshot.run_id
                ))),
            };
        }
        Ok(snapshot)
    }

    async fn ensure_run_index_capacity(&self, session_id: &str) -> Result<(), WorkflowRunError> {
        let session = self
            .sessions
            .try_load(session_id)
            .await
            .map_err(|_| WorkflowRunError::Storage("session state unavailable".to_string()))?
            .ok_or(WorkflowRunError::NotFound)?;
        let ids = session
            .metadata
            .get(bamboo_skills::WORKFLOW_RUN_IDS_METADATA_KEY)
            .and_then(|raw| serde_json::from_str::<Vec<String>>(raw).ok())
            .unwrap_or_default();
        if ids.len() < MAX_WORKFLOW_RUN_IDS_PER_SESSION {
            return Ok(());
        }
        let mut evictable = BTreeSet::new();
        for run_id in &ids {
            match self.engine.progress(run_id, u64::MAX).await {
                Ok(progress) if progress.snapshot.status.is_terminal() => {
                    evictable.insert(run_id.clone());
                }
                Err(WorkflowRunError::NotFound) => {
                    evictable.insert(run_id.clone());
                }
                Ok(_) => {}
                Err(error) => return Err(error),
            }
        }
        if !evictable.is_empty() {
            self.sessions
                .update_runtime_session(
                    session_id,
                    &[bamboo_skills::WORKFLOW_RUN_IDS_METADATA_KEY],
                    move |session| {
                        let mut ids = session
                            .metadata
                            .get(bamboo_skills::WORKFLOW_RUN_IDS_METADATA_KEY)
                            .and_then(|raw| serde_json::from_str::<Vec<String>>(raw).ok())
                            .unwrap_or_default();
                        ids.retain(|id| !evictable.contains(id));
                        session.metadata.insert(
                            bamboo_skills::WORKFLOW_RUN_IDS_METADATA_KEY.to_string(),
                            serde_json::to_string(&ids)
                                .expect("string vector serialization cannot fail"),
                        );
                    },
                )
                .await
                .map_err(|_| WorkflowRunError::Storage("run index pruning failed".to_string()))?
                .ok_or(WorkflowRunError::NotFound)?;
        }
        let remaining = self
            .sessions
            .try_load(session_id)
            .await
            .map_err(|_| WorkflowRunError::Storage("session state unavailable".to_string()))?
            .and_then(|session| {
                session
                    .metadata
                    .get(bamboo_skills::WORKFLOW_RUN_IDS_METADATA_KEY)
                    .and_then(|raw| serde_json::from_str::<Vec<String>>(raw).ok())
            })
            .unwrap_or_default()
            .len();
        if remaining >= MAX_WORKFLOW_RUN_IDS_PER_SESSION {
            return Err(WorkflowRunError::Preflight(
                "workflow run index is full of active runs".to_string(),
            ));
        }
        Ok(())
    }

    async fn remember_run_id(
        &self,
        session_id: &str,
        run_id: &str,
    ) -> Result<(), WorkflowRunError> {
        let run_id = run_id.to_string();
        let index_full = Arc::new(AtomicBool::new(false));
        let index_full_in_transaction = index_full.clone();
        self.sessions
            .update_runtime_session(
                session_id,
                &[bamboo_skills::WORKFLOW_RUN_IDS_METADATA_KEY],
                move |session| {
                    let mut ids = session
                        .metadata
                        .get(bamboo_skills::WORKFLOW_RUN_IDS_METADATA_KEY)
                        .and_then(|raw| serde_json::from_str::<Vec<String>>(raw).ok())
                        .unwrap_or_default();
                    ids.retain(|existing| existing != &run_id);
                    if ids.len() >= MAX_WORKFLOW_RUN_IDS_PER_SESSION {
                        index_full_in_transaction.store(true, Ordering::SeqCst);
                        return;
                    }
                    ids.push(run_id);
                    session.metadata.insert(
                        bamboo_skills::WORKFLOW_RUN_IDS_METADATA_KEY.to_string(),
                        serde_json::to_string(&ids)
                            .expect("string vector serialization cannot fail"),
                    );
                },
            )
            .await
            .map_err(|_| WorkflowRunError::Storage("run index persistence failed".to_string()))?
            .ok_or(WorkflowRunError::NotFound)
            .and_then(|_| {
                if index_full.load(Ordering::SeqCst) {
                    Err(WorkflowRunError::Storage(
                        "workflow run index reached its active-run capacity".to_string(),
                    ))
                } else {
                    Ok(())
                }
            })
    }

    pub async fn list_for_session(
        &self,
        session_id: &str,
    ) -> Result<Vec<WorkflowRunSnapshot>, WorkflowRunError> {
        self.session_context(session_id).await?;
        let session = self
            .sessions
            .try_load(session_id)
            .await
            .map_err(|_| WorkflowRunError::Storage("session state unavailable".to_string()))?
            .ok_or(WorkflowRunError::NotFound)?;
        let run_ids = session
            .metadata
            .get(bamboo_skills::WORKFLOW_RUN_IDS_METADATA_KEY)
            .and_then(|raw| serde_json::from_str::<Vec<String>>(raw).ok())
            .unwrap_or_default();
        let mut snapshots = Vec::new();
        let mut stale = BTreeSet::new();
        for run_id in run_ids {
            match self.engine.progress(&run_id, u64::MAX).await {
                Ok(progress) if progress.snapshot.session_id == session_id => {
                    snapshots.push(progress.snapshot);
                }
                Ok(_) | Err(WorkflowRunError::NotFound) => {
                    stale.insert(run_id);
                }
                Err(error) => return Err(error),
            }
        }
        if !stale.is_empty() {
            let _ = self
                .sessions
                .update_runtime_session(
                    session_id,
                    &[bamboo_skills::WORKFLOW_RUN_IDS_METADATA_KEY],
                    move |session| {
                        let mut ids = session
                            .metadata
                            .get(bamboo_skills::WORKFLOW_RUN_IDS_METADATA_KEY)
                            .and_then(|raw| serde_json::from_str::<Vec<String>>(raw).ok())
                            .unwrap_or_default();
                        ids.retain(|id| !stale.contains(id));
                        session.metadata.insert(
                            bamboo_skills::WORKFLOW_RUN_IDS_METADATA_KEY.to_string(),
                            serde_json::to_string(&ids)
                                .expect("string vector serialization cannot fail"),
                        );
                    },
                )
                .await;
        }
        snapshots.sort_by_key(|snapshot| std::cmp::Reverse(snapshot.created_at));
        Ok(snapshots)
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

    pub async fn restart_from_tool(
        &self,
        session_id: &str,
        run_id: &str,
    ) -> Result<WorkflowRunSnapshot, WorkflowRunError> {
        let progress = self
            .progress_for_session(session_id, run_id, u64::MAX)
            .await?;
        if progress.snapshot.definition_bundle.root_invocation_policy["automatic"].as_bool()
            != Some(true)
        {
            return Err(WorkflowRunError::Preflight(
                "pinned workflow invocation policy denies automatic restart".to_string(),
            ));
        }
        let session = self
            .sessions
            .try_load(session_id)
            .await
            .map_err(|_| WorkflowRunError::Preflight("session state is unavailable".to_string()))?
            .ok_or(WorkflowRunError::NotFound)?;
        let opted_in = session
            .metadata
            .get(bamboo_skills::WORKFLOW_ORCHESTRATION_OPT_IN_METADATA_KEY)
            .is_some_and(|value| value.eq_ignore_ascii_case("true"));
        if !opted_in {
            return Err(WorkflowRunError::Preflight(
                "model-started orchestration restart requires explicit session opt-in".to_string(),
            ));
        }
        self.restart_for_session(session_id, run_id).await
    }
}

fn tighten_workflow_budget(
    definition: &WorkflowBudgets,
    requested: &WorkflowBudgets,
) -> WorkflowBudgets {
    WorkflowBudgets {
        max_concurrency: definition.max_concurrency.min(requested.max_concurrency),
        max_agents: definition.max_agents.min(requested.max_agents),
        max_steps: definition.max_steps.min(requested.max_steps),
        max_retries: definition.max_retries.min(requested.max_retries),
        max_nesting_depth: definition
            .max_nesting_depth
            .min(requested.max_nesting_depth),
        wall_time_ms: definition.wall_time_ms.min(requested.wall_time_ms),
        max_tokens: match (definition.max_tokens, requested.max_tokens) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (Some(value), None) | (None, Some(value)) => Some(value),
            (None, None) => None,
        },
        max_cost_micros: match (definition.max_cost_micros, requested.max_cost_micros) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (Some(value), None) | (None, Some(value)) => Some(value),
            (None, None) => None,
        },
    }
}

fn validate_requested_budget(requested: &WorkflowBudgets) -> Result<(), WorkflowRunError> {
    if requested.max_concurrency == 0
        || requested.max_steps == 0
        || requested.max_nesting_depth == 0
        || requested.wall_time_ms == 0
    {
        return Err(WorkflowRunError::InvalidInput(
            "workflow execution limits must be positive".to_string(),
        ));
    }
    Ok(())
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
        #[serde(default = "empty_object")]
        args: Value,
        #[serde(default)]
        budget: Option<WorkflowBudgets>,
    },
    List {},
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

fn empty_object() -> Value {
    json!({})
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
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["start", "list", "get", "events", "cancel", "restart"]
                },
                "workflow_id": {"type": "string", "minLength": 1},
                "revision": {"type": "integer", "minimum": 1},
                "args": {"type": "object", "default": {}},
                "budget": {
                    "type": "object",
                    "properties": {
                        "max_concurrency": {"type": "integer", "minimum": 1},
                        "max_agents": {"type": "integer", "minimum": 0},
                        "max_steps": {"type": "integer", "minimum": 1},
                        "max_retries": {"type": "integer", "minimum": 0},
                        "max_nesting_depth": {"type": "integer", "minimum": 1},
                        "wall_time_ms": {"type": "integer", "minimum": 1},
                        "max_tokens": {"type": "integer", "minimum": 0},
                        "max_cost_micros": {"type": "integer", "minimum": 0}
                    },
                    "required": [
                        "max_concurrency",
                        "max_agents",
                        "max_steps",
                        "max_retries",
                        "max_nesting_depth",
                        "wall_time_ms"
                    ],
                    "additionalProperties": false
                },
                "run_id": {"type": "string"},
                "since": {"type": "integer", "minimum": 0}
            },
            "required": ["action"],
            "additionalProperties": false
        })
    }

    fn classify(&self, args: &Value) -> ToolClass {
        match args.get("action").and_then(Value::as_str) {
            Some("get" | "list" | "events") => ToolClass::READONLY_PARALLEL,
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
                budget,
            } => serde_json::to_value(public_workflow_snapshot(
                self.access
                    .start_from_tool(session_id, &workflow_id, revision, args, budget)
                    .await
                    .map_err(workflow_tool_error)?,
            )),
            WorkflowToolInput::List {} => serde_json::to_value(
                self.access
                    .list_for_session(session_id)
                    .await
                    .map_err(workflow_tool_error)?
                    .into_iter()
                    .map(public_workflow_snapshot)
                    .collect::<Vec<_>>(),
            ),
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
                        .restart_from_tool(session_id, &run_id)
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
    use bamboo_agent_core::storage::Storage;
    use bamboo_agent_core::tools::{
        FunctionSchema, ToolCall, ToolExecutor, ToolResult, ToolSchema,
    };
    use bamboo_agent_core::Session;
    use bamboo_llm::protocol::{gemini::GeminiTool, ToProvider};
    use std::collections::HashMap;
    use tokio::sync::RwLock;

    #[derive(Default)]
    struct WorkflowTestStorage {
        sessions: RwLock<HashMap<String, Session>>,
    }

    #[async_trait]
    impl Storage for WorkflowTestStorage {
        async fn save_session(&self, session: &Session) -> std::io::Result<()> {
            self.sessions
                .write()
                .await
                .insert(session.id.clone(), session.clone());
            Ok(())
        }

        async fn load_session(&self, session_id: &str) -> std::io::Result<Option<Session>> {
            Ok(self.sessions.read().await.get(session_id).cloned())
        }

        async fn delete_session(&self, session_id: &str) -> std::io::Result<bool> {
            Ok(self.sessions.write().await.remove(session_id).is_some())
        }
    }

    struct WorkflowReadTool;

    #[async_trait]
    impl ToolExecutor for WorkflowReadTool {
        async fn execute(
            &self,
            _call: &ToolCall,
        ) -> bamboo_agent_core::tools::executor::Result<ToolResult> {
            Ok(ToolResult::text(true, r#"{"ok":true}"#))
        }

        fn list_tools(&self) -> Vec<ToolSchema> {
            vec![ToolSchema {
                schema_type: "function".to_string(),
                function: FunctionSchema {
                    name: "Read".to_string(),
                    description: "read".to_string(),
                    parameters: serde_json::json!({"type":"object"}),
                },
            }]
        }
    }

    async fn workflow_test_access() -> (
        WorkflowRunAccess,
        bamboo_engine::SessionRepository,
        tempfile::TempDir,
    ) {
        let directory = tempfile::tempdir().expect("tempdir");
        let skills_dir = directory.path().join("skills");
        let root = skills_dir.join("review-flow");
        std::fs::create_dir_all(&root).expect("workflow dir");
        std::fs::write(
            root.join("SKILL.md"),
            "---\nname: review-flow\ndescription: Review flow\n---\nRun review flow.\n",
        )
        .expect("skill");
        std::fs::write(
            root.join("workflow.yaml"),
            "workflow_schema: 1\nid: review-flow\nrevision: 42\ninvocation_policy: {explicit: true, automatic: true}\ninput_schema:\n  type: object\n  additionalProperties: true\nsteps:\n  - id: inspect\n    type: tool\n    tool: Read\n    args: {}\n    capabilities: [read]\n    output_schema:\n      type: object\n      additionalProperties: true\nplan:\n  type: step\n  step: inspect\nbudgets:\n  max_concurrency: 2\n  max_agents: 1\n  max_steps: 4\n  max_retries: 2\n  max_nesting_depth: 2\n  wall_time_ms: 10000\n  max_tokens: 1000\n  max_cost_micros: 1000\n",
        )
        .expect("workflow");
        let skills = Arc::new(SkillManager::with_config(bamboo_skills::SkillStoreConfig {
            skills_dir,
            ..Default::default()
        }));
        skills.initialize().await.expect("skills initialize");
        let storage: Arc<dyn Storage> = Arc::new(WorkflowTestStorage::default());
        let persistence = Arc::new(bamboo_storage::LockedSessionStore::new(storage.clone()));
        let cache = Arc::new(dashmap::DashMap::new());
        let repo = bamboo_engine::SessionRepository::new(cache, storage, persistence);
        let access = WorkflowRunAccess::new(
            directory.path(),
            Arc::new(WorkflowReadTool),
            skills,
            repo.clone(),
        )
        .await
        .expect("workflow access");
        (access, repo, directory)
    }

    fn canonical_workflow_run_schema() -> Value {
        json!({
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["start", "list", "get", "events", "cancel", "restart"]
                },
                "workflow_id": {"type": "string", "minLength": 1},
                "revision": {"type": "integer", "minimum": 1},
                "args": {"type": "object", "default": {}},
                "budget": {
                    "type": "object",
                    "properties": {
                        "max_concurrency": {"type": "integer", "minimum": 1},
                        "max_agents": {"type": "integer", "minimum": 0},
                        "max_steps": {"type": "integer", "minimum": 1},
                        "max_retries": {"type": "integer", "minimum": 0},
                        "max_nesting_depth": {"type": "integer", "minimum": 1},
                        "wall_time_ms": {"type": "integer", "minimum": 1},
                        "max_tokens": {"type": "integer", "minimum": 0},
                        "max_cost_micros": {"type": "integer", "minimum": 0}
                    },
                    "required": [
                        "max_concurrency",
                        "max_agents",
                        "max_steps",
                        "max_retries",
                        "max_nesting_depth",
                        "wall_time_ms"
                    ],
                    "additionalProperties": false
                },
                "run_id": {"type": "string"},
                "since": {"type": "integer", "minimum": 0}
            },
            "required": ["action"],
            "additionalProperties": false
        })
    }

    #[tokio::test]
    async fn workflow_run_schema_is_flat_complete_and_canonical() {
        let (access, _, _) = workflow_test_access().await;
        let schema = WorkflowRunTool::new(access).parameters_schema();

        for combinator in ["oneOf", "anyOf", "allOf"] {
            assert!(
                schema.get(combinator).is_none(),
                "workflow_run must not advertise root {combinator}"
            );
        }
        assert_eq!(schema, canonical_workflow_run_schema());
    }

    #[tokio::test]
    async fn workflow_run_schema_survives_openai_sanitization_with_all_properties() {
        let (access, _, _) = workflow_test_access().await;
        let schema = WorkflowRunTool::new(access).parameters_schema();
        let sanitized =
            bamboo_llm::providers::common::tool_schema::sanitize_openai_function_parameters_schema(
                &schema,
            );

        let properties = sanitized["properties"]
            .as_object()
            .expect("sanitized workflow_run properties");
        assert!(!properties.is_empty());
        assert_eq!(properties.len(), 7);
        assert_eq!(sanitized, canonical_workflow_run_schema());
    }

    #[tokio::test]
    async fn workflow_run_schema_reaches_gemini_unchanged() {
        let (access, _, _) = workflow_test_access().await;
        let direct = WorkflowRunTool::new(access).to_schema();
        let gemini: GeminiTool = direct.to_provider().expect("Gemini tool conversion");
        let declaration = gemini
            .function_declarations
            .first()
            .expect("workflow_run declaration");

        assert_eq!(declaration.name, "workflow_run");
        assert_eq!(
            declaration.parameters_json_schema.as_ref(),
            Some(&canonical_workflow_run_schema())
        );
        assert!(declaration.parameters.is_none());
    }

    #[tokio::test]
    async fn workflow_run_enforces_opt_in_tightens_budget_lists_and_isolates_sessions() {
        let (access, repo, directory) = workflow_test_access().await;
        let workspace = directory.path().join("workspace");
        std::fs::create_dir_all(&workspace).expect("workspace");
        let mut session = Session::new("workflow-session", "model");
        session.workspace = Some(workspace.to_string_lossy().into_owned());
        repo.save(&mut session).await.expect("save session");

        let denied = access
            .start_from_tool("workflow-session", "review-flow", 42, json!({}), None)
            .await
            .expect_err("model start defaults off without session opt-in");
        assert!(denied.to_string().contains("opt-in"));

        session.metadata.insert(
            bamboo_skills::WORKFLOW_ORCHESTRATION_OPT_IN_METADATA_KEY.to_string(),
            "true".to_string(),
        );
        repo.save(&mut session).await.expect("save opt-in");
        let requested = WorkflowBudgets {
            max_concurrency: 1,
            max_agents: 0,
            max_steps: 2,
            max_retries: 0,
            max_nesting_depth: 1,
            wall_time_ms: 5_000,
            max_tokens: Some(500),
            max_cost_micros: Some(500),
        };
        let started = access
            .start_from_tool(
                "workflow-session",
                "review-flow",
                42,
                json!({}),
                Some(requested.clone()),
            )
            .await
            .expect("opted-in model start");
        assert_eq!(started.definition.budgets, requested);
        let listed = access
            .list_for_session("workflow-session")
            .await
            .expect("session run list");
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].run_id, started.run_id);
        let progress = access
            .progress_for_session("workflow-session", &started.run_id, 0)
            .await
            .expect("run events");
        assert!(progress
            .events
            .first()
            .is_some_and(|event| event.kind == bamboo_domain::WorkflowRunEventKind::RunQueued));
        let completed = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let progress = access
                    .progress_for_session("workflow-session", &started.run_id, 0)
                    .await
                    .expect("terminal run progress");
                if progress.snapshot.status.is_terminal() {
                    break progress;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("workflow reaches terminal state");
        assert_eq!(
            completed.snapshot.status,
            bamboo_domain::WorkflowRunStatus::Succeeded
        );
        assert_eq!(
            completed
                .events
                .iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>(),
            (1..=7).collect::<Vec<_>>()
        );
        assert!(matches!(
            completed.events.as_slice(),
            [
                bamboo_domain::WorkflowRunEvent {
                    kind: bamboo_domain::WorkflowRunEventKind::RunQueued,
                    ..
                },
                bamboo_domain::WorkflowRunEvent {
                    kind: bamboo_domain::WorkflowRunEventKind::RunStarted,
                    ..
                },
                bamboo_domain::WorkflowRunEvent {
                    kind: bamboo_domain::WorkflowRunEventKind::Phase { ref name },
                    ..
                },
                bamboo_domain::WorkflowRunEvent {
                    kind: bamboo_domain::WorkflowRunEventKind::StepQueued,
                    ..
                },
                bamboo_domain::WorkflowRunEvent {
                    kind: bamboo_domain::WorkflowRunEventKind::StepStarted,
                    ..
                },
                bamboo_domain::WorkflowRunEvent {
                    kind: bamboo_domain::WorkflowRunEventKind::StepCompleted { .. },
                    ..
                },
                bamboo_domain::WorkflowRunEvent {
                    kind: bamboo_domain::WorkflowRunEventKind::RunSucceeded { .. },
                    ..
                }
            ] if name == "step_reserved"
        ));
        assert_eq!(
            completed.snapshot.last_sequence,
            completed.events.last().expect("terminal event").sequence
        );

        let invalid_budget = WorkflowBudgets {
            max_steps: 0,
            ..requested.clone()
        };
        assert!(matches!(
            access
                .start_from_tool(
                    "workflow-session",
                    "review-flow",
                    42,
                    json!({}),
                    Some(invalid_budget),
                )
                .await,
            Err(WorkflowRunError::InvalidInput(_))
        ));

        // Restart authority is the original immutable publication, not the
        // current catalog policy.
        let workflow_path = directory.path().join("skills/review-flow/workflow.yaml");
        let original_workflow = std::fs::read_to_string(&workflow_path).expect("workflow yaml");
        std::fs::write(
            &workflow_path,
            original_workflow.replace(
                "invocation_policy: {explicit: true, automatic: true}",
                "invocation_policy: {explicit: true, automatic: false}",
            ),
        )
        .expect("disable automatic live policy");
        access.skills.store().reload().await.expect("reload policy");
        let restart = access
            .restart_from_tool("workflow-session", &started.run_id)
            .await
            .expect_err("succeeded runs are terminal");
        assert!(matches!(restart, WorkflowRunError::Terminal));

        let mut other = Session::new("other-session", "model");
        other.workspace = Some(workspace.to_string_lossy().into_owned());
        repo.save(&mut other).await.expect("save other session");
        assert!(access
            .list_for_session("other-session")
            .await
            .expect("isolated list")
            .is_empty());
        assert!(matches!(
            access
                .progress_for_session("other-session", &started.run_id, 0)
                .await,
            Err(WorkflowRunError::NotFound)
        ));
    }

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

    #[test]
    fn tool_input_rejects_fields_from_other_actions() {
        let invalid = [
            json!({"action": "list", "run_id": "run-1"}),
            json!({"action": "get", "run_id": "run-1", "since": 1}),
            json!({"action": "events", "run_id": "run-1", "workflow_id": "flow"}),
            json!({"action": "cancel", "run_id": "run-1", "revision": 1}),
            json!({"action": "restart", "run_id": "run-1", "budget": {}}),
            json!({
                "action": "start",
                "workflow_id": "flow",
                "revision": 1,
                "run_id": "run-1"
            }),
        ];

        for input in invalid {
            let error = serde_json::from_value::<WorkflowToolInput>(input.clone())
                .expect_err("action-specific fields must remain authoritative at runtime");
            assert!(
                error.to_string().contains("unknown field"),
                "unexpected error for {input}: {error}"
            );
        }

        assert!(matches!(
            serde_json::from_value::<WorkflowToolInput>(json!({"action": "list"}))
                .expect("fieldless list action"),
            WorkflowToolInput::List {}
        ));
    }

    #[tokio::test]
    async fn omitted_start_args_and_zero_budgets_match_schema() {
        let WorkflowToolInput::Start { args, .. } =
            serde_json::from_value::<WorkflowToolInput>(json!({
                "action": "start",
                "workflow_id": "safe",
                "revision": 1
            }))
            .expect("tool args default")
        else {
            panic!("start input")
        };
        assert_eq!(args, json!({}));
        let http: crate::handlers::workflow_runs::StartWorkflowRunRequest =
            serde_json::from_value(json!({"workflow_id":"safe", "revision":1}))
                .expect("http args default");
        assert_eq!(http.args, json!({}));

        let (access, _, _) = workflow_test_access().await;
        let schema = WorkflowRunTool { access }.parameters_schema();
        let properties = &schema["properties"];
        assert_eq!(properties["args"]["default"], json!({}));
        assert_eq!(
            properties["budget"]["properties"]["max_agents"]["minimum"],
            0
        );
        assert_eq!(
            properties["budget"]["properties"]["max_retries"]["minimum"],
            0
        );
        assert_eq!(
            properties["budget"]["properties"]["max_tokens"]["minimum"],
            0
        );
        assert_eq!(
            properties["budget"]["properties"]["max_cost_micros"]["minimum"],
            0
        );
    }

    #[tokio::test]
    async fn run_index_updates_are_concurrent_and_never_evict_at_active_capacity() {
        let (access, repo, _) = workflow_test_access().await;
        let mut session = Session::new("run-index", "model");
        repo.save(&mut session).await.expect("seed session");
        let results = futures::future::join_all((0..32).map(|index| {
            let access = access.clone();
            async move {
                let run_id = format!("run-{index}");
                access.remember_run_id("run-index", &run_id).await
            }
        }))
        .await;
        assert!(results.into_iter().all(|result| result.is_ok()));
        let concurrent = repo
            .try_load("run-index")
            .await
            .expect("load")
            .expect("session");
        let ids = serde_json::from_str::<Vec<String>>(
            concurrent
                .metadata
                .get(bamboo_skills::WORKFLOW_RUN_IDS_METADATA_KEY)
                .expect("run ids"),
        )
        .expect("ids json");
        assert_eq!(ids.len(), 32);
        assert_eq!(ids.iter().collect::<BTreeSet<_>>().len(), 32);

        let capacity_ids = (0..MAX_WORKFLOW_RUN_IDS_PER_SESSION)
            .map(|index| format!("active-{index}"))
            .collect::<Vec<_>>();
        repo.update_runtime_session(
            "run-index",
            &[bamboo_skills::WORKFLOW_RUN_IDS_METADATA_KEY],
            {
                let capacity_ids = capacity_ids.clone();
                move |session| {
                    session.metadata.insert(
                        bamboo_skills::WORKFLOW_RUN_IDS_METADATA_KEY.to_string(),
                        serde_json::to_string(&capacity_ids).expect("ids json"),
                    );
                }
            },
        )
        .await
        .expect("fill index")
        .expect("session");
        assert!(matches!(
            access.remember_run_id("run-index", "new-run").await,
            Err(WorkflowRunError::Storage(_))
        ));
        let retained = repo
            .try_load("run-index")
            .await
            .expect("load")
            .expect("session");
        let retained = serde_json::from_str::<Vec<String>>(
            retained
                .metadata
                .get(bamboo_skills::WORKFLOW_RUN_IDS_METADATA_KEY)
                .expect("run ids"),
        )
        .expect("ids json");
        assert_eq!(
            retained, capacity_ids,
            "oldest active id must not be evicted"
        );
    }

    #[tokio::test]
    async fn real_model_workflow_run_index_survives_tool_result_and_final_session_save() {
        use bamboo_agent_core::storage::AttachmentReader;
        use bamboo_engine::{Agent, ExecuteRequestBuilder};
        use bamboo_llm::{LLMChunk, LLMProvider, LLMStream};
        use futures::stream;
        use tokio::sync::Mutex;
        use tokio_util::sync::CancellationToken;

        struct NoAttachments;
        #[async_trait]
        impl AttachmentReader for NoAttachments {
            async fn read_attachment(
                &self,
                _session_id: &str,
                _attachment_id: &str,
            ) -> std::io::Result<Option<(Vec<u8>, String)>> {
                Ok(None)
            }
        }
        struct QueueProvider {
            queue: Mutex<Vec<Vec<bamboo_llm::provider::Result<LLMChunk>>>>,
        }
        #[async_trait]
        impl LLMProvider for QueueProvider {
            async fn chat_stream(
                &self,
                _messages: &[bamboo_agent_core::Message],
                _tools: &[ToolSchema],
                _max_output_tokens: Option<u32>,
                _model: &str,
            ) -> bamboo_llm::provider::Result<LLMStream> {
                Ok(Box::pin(stream::iter(self.queue.lock().await.remove(0))))
            }
        }

        let (access, repo, directory) = workflow_test_access().await;
        let session_id = "real-model-workflow-run";
        let mut session = Session::new(session_id, "test-model");
        session.metadata.insert(
            bamboo_skills::WORKFLOW_ORCHESTRATION_OPT_IN_METADATA_KEY.to_string(),
            "true".to_string(),
        );
        session
            .metadata
            .insert("external.metadata".to_string(), "preserve".to_string());
        session.add_message(bamboo_agent_core::Message::system("system"));
        session.add_message(bamboo_agent_core::Message::user("run review workflow"));
        repo.save(&mut session).await.expect("seed session");
        let call = ToolCall {
            id: "call-workflow-run".to_string(),
            tool_type: "function".to_string(),
            function: bamboo_agent_core::tools::FunctionCall {
                name: "workflow_run".to_string(),
                arguments: json!({
                    "action":"start",
                    "workflow_id":"review-flow",
                    "revision":42
                })
                .to_string(),
            },
        };
        let provider = Arc::new(QueueProvider {
            queue: Mutex::new(vec![
                vec![Ok(LLMChunk::ToolCalls(vec![call])), Ok(LLMChunk::Done)],
                vec![Ok(LLMChunk::Token("done".to_string())), Ok(LLMChunk::Done)],
            ]),
        });
        let tools = Arc::new(
            bamboo_tools::BuiltinToolExecutorBuilder::new()
                .with_tool(WorkflowRunTool::new(access.clone()))
                .expect("workflow tool")
                .build(),
        );
        let metrics = bamboo_metrics::MetricsCollector::spawn(
            Arc::new(bamboo_metrics::SqliteMetricsStorage::new(
                directory.path().join("runner-metrics.db"),
            )),
            7,
        );
        let agent = Agent::builder()
            .storage(repo.storage().clone())
            .persistence(Arc::new(repo.clone()))
            .attachment_reader(Arc::new(NoAttachments))
            .skill_manager(access.skills.clone())
            .metrics_collector(metrics)
            .config(Arc::new(RwLock::new(bamboo_llm::Config::default())))
            .provider(provider)
            .default_tools(tools)
            .build()
            .expect("agent");
        let (event_tx, _event_rx) = tokio::sync::mpsc::channel(128);
        agent
            .execute(
                &mut session,
                ExecuteRequestBuilder::new(
                    "run review workflow",
                    event_tx,
                    CancellationToken::new(),
                )
                .model("test-model")
                .build(),
            )
            .await
            .expect("real model workflow run");

        let saved = repo
            .storage()
            .load_session(session_id)
            .await
            .expect("load")
            .expect("saved");
        assert_eq!(
            saved.metadata.get("external.metadata").map(String::as_str),
            Some("preserve")
        );
        assert!(saved
            .metadata
            .contains_key(bamboo_skills::WORKFLOW_RUN_IDS_METADATA_KEY));
        let listed = access
            .list_for_session(session_id)
            .await
            .expect("list after final save");
        assert_eq!(listed.len(), 1);
        assert!(saved.messages.iter().any(|message| {
            message.tool_calls.as_ref().is_some_and(|calls| {
                calls
                    .iter()
                    .any(|call| call.function.name == "workflow_run")
            })
        }));
    }

    #[tokio::test]
    async fn http_start_survives_concurrent_stale_runner_final_save_and_server_restart() {
        use bamboo_agent_core::storage::AttachmentReader;
        use bamboo_engine::{Agent, ExecuteRequestBuilder};
        use bamboo_llm::{LLMChunk, LLMProvider, LLMStream};
        use futures::stream;
        use tokio::sync::{oneshot, Mutex};
        use tokio_util::sync::CancellationToken;

        struct NoAttachments;
        #[async_trait]
        impl AttachmentReader for NoAttachments {
            async fn read_attachment(
                &self,
                _session_id: &str,
                _attachment_id: &str,
            ) -> std::io::Result<Option<(Vec<u8>, String)>> {
                Ok(None)
            }
        }

        struct PausingProvider {
            entered: Mutex<Option<oneshot::Sender<()>>>,
            resume: Mutex<Option<oneshot::Receiver<()>>>,
        }
        #[async_trait]
        impl LLMProvider for PausingProvider {
            async fn chat_stream(
                &self,
                _messages: &[bamboo_agent_core::Message],
                _tools: &[ToolSchema],
                _max_output_tokens: Option<u32>,
                _model: &str,
            ) -> bamboo_llm::provider::Result<LLMStream> {
                if let Some(entered) = self.entered.lock().await.take() {
                    let _ = entered.send(());
                }
                if let Some(resume) = self.resume.lock().await.take() {
                    let _ = resume.await;
                }
                Ok(Box::pin(stream::iter(vec![
                    Ok(LLMChunk::Token("done".to_string())),
                    Ok(LLMChunk::Done),
                ])))
            }
        }

        let (access, repo, directory) = workflow_test_access().await;
        let session_id = "http-start-concurrent-runner-save";
        let mut session = Session::new(session_id, "test-model");
        session.add_message(bamboo_agent_core::Message::system("system"));
        session.add_message(bamboo_agent_core::Message::user("keep running"));
        repo.save(&mut session).await.expect("seed session");
        let mut runner_session = repo
            .try_load(session_id)
            .await
            .expect("load runner session")
            .expect("runner session");

        let (entered_tx, entered_rx) = oneshot::channel();
        let (resume_tx, resume_rx) = oneshot::channel();
        let provider = Arc::new(PausingProvider {
            entered: Mutex::new(Some(entered_tx)),
            resume: Mutex::new(Some(resume_rx)),
        });
        let metrics = bamboo_metrics::MetricsCollector::spawn(
            Arc::new(bamboo_metrics::SqliteMetricsStorage::new(
                directory.path().join("http-runner-metrics.db"),
            )),
            7,
        );
        let agent = Agent::builder()
            .storage(repo.storage().clone())
            .persistence(Arc::new(repo.clone()))
            .attachment_reader(Arc::new(NoAttachments))
            .skill_manager(access.skills.clone())
            .metrics_collector(metrics)
            .config(Arc::new(RwLock::new(bamboo_llm::Config::default())))
            .provider(provider)
            .default_tools(Arc::new(
                bamboo_tools::BuiltinToolExecutorBuilder::new().build(),
            ))
            .build()
            .expect("agent");
        let (event_tx, _event_rx) = tokio::sync::mpsc::channel(128);
        let runner = tokio::spawn(async move {
            agent
                .execute(
                    &mut runner_session,
                    ExecuteRequestBuilder::new("keep running", event_tx, CancellationToken::new())
                        .model("test-model")
                        .build(),
                )
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(2), entered_rx)
            .await
            .expect("runner enters model round")
            .expect("runner entry signal");

        let started = access
            .start(session_id, "review-flow", 42, json!({}), None)
            .await
            .expect("HTTP-equivalent explicit start");
        let durable_during_round = repo
            .storage()
            .load_session(session_id)
            .await
            .expect("load during round")
            .expect("session during round");
        assert!(durable_during_round
            .metadata
            .get(bamboo_skills::WORKFLOW_RUN_IDS_METADATA_KEY)
            .is_some_and(|raw| raw.contains(&started.run_id)));

        resume_tx.send(()).expect("resume runner");
        tokio::time::timeout(std::time::Duration::from_secs(2), runner)
            .await
            .expect("runner completes")
            .expect("runner task")
            .expect("runner execution");

        let durable_after_final_save = repo
            .storage()
            .load_session(session_id)
            .await
            .expect("load after final save")
            .expect("saved session");
        assert!(durable_after_final_save
            .metadata
            .get(bamboo_skills::WORKFLOW_RUN_IDS_METADATA_KEY)
            .is_some_and(|raw| raw.contains(&started.run_id)));

        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let progress = access
                    .progress_for_session(session_id, &started.run_id, u64::MAX)
                    .await
                    .expect("workflow progress before restart");
                if progress.snapshot.status.is_terminal()
                    && !access.engine.is_run_active(&started.run_id)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("workflow reaches terminal state before restart");

        let skills = access.skills.clone();
        drop(access);
        let restarted =
            WorkflowRunAccess::new(directory.path(), Arc::new(WorkflowReadTool), skills, repo)
                .await
                .expect("restart workflow access");
        let listed = restarted
            .list_for_session(session_id)
            .await
            .expect("list after restart");
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].run_id, started.run_id);
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
