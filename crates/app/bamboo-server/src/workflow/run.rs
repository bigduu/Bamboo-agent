use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bamboo_agent_core::tools::{
    Tool, ToolClass, ToolCtx, ToolError, ToolExecutionSessionFlags, ToolOutcome, ToolResult,
};
use bamboo_domain::{
    StartWorkflowRun, WorkflowBudgetUsage, WorkflowBudgets, WorkflowDefinitionBundle,
    WorkflowFailure, WorkflowFailureCode, WorkflowPlan, WorkflowProgress, WorkflowRunDefinition,
    WorkflowRunEvent, WorkflowRunEventKind, WorkflowRunSnapshot, WorkflowRunStatus,
    WorkflowStepKind, WorkflowStepSnapshot, WorkflowStepStatus, WorkflowSuspensionContext,
};
use bamboo_engine::{
    AgentStepPort, AgentStepResult, FileWorkflowRunRepository, NamedAgentSpec, PermissionDecision,
    WorkflowDefinitionPort, WorkflowPolicyPort, WorkflowPolicyTarget, WorkflowRunEngine,
    WorkflowRunError, WorkflowSecretMaterial, WorkflowSecretResolverPort,
    WorkflowSessionPermissionPort,
};
use bamboo_skills::SkillManager;
use serde::{Deserialize, Serialize};
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

struct ServerWorkflowSessionPermissions {
    sessions: bamboo_engine::SessionRepository,
    permission_config: Arc<bamboo_tools::permission::PermissionConfig>,
}

#[async_trait]
impl WorkflowSessionPermissionPort for ServerWorkflowSessionPermissions {
    async fn flags_for_session(
        &self,
        session_id: &str,
    ) -> Result<ToolExecutionSessionFlags, String> {
        let session = self
            .sessions
            .try_load(session_id)
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("workflow session '{session_id}' does not exist"))?;
        let configured = if session
            .agent_runtime_state
            .as_ref()
            .is_some_and(|state| state.plan_mode.is_some())
        {
            bamboo_domain::PermissionMode::Plan
        } else {
            self.permission_config.mode()
        };
        Ok(ToolExecutionSessionFlags::from_session_and_configured_mode(
            &session, configured,
        ))
    }
}

impl WorkflowRunAccess {
    pub async fn new(
        data_dir: &Path,
        tools: Arc<dyn bamboo_agent_core::tools::ToolExecutor>,
        skills: Arc<SkillManager>,
        sessions: bamboo_engine::SessionRepository,
    ) -> Result<Self, String> {
        Self::new_with_permission_config(data_dir, tools, skills, sessions, None).await
    }

    pub async fn new_with_permission_config(
        data_dir: &Path,
        tools: Arc<dyn bamboo_agent_core::tools::ToolExecutor>,
        skills: Arc<SkillManager>,
        sessions: bamboo_engine::SessionRepository,
        permission_config: Option<Arc<bamboo_tools::permission::PermissionConfig>>,
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
        if let Some(permission_config) = permission_config {
            engine.set_session_permission_port(Arc::new(ServerWorkflowSessionPermissions {
                sessions: sessions.clone(),
                permission_config,
            }));
        }
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
        self.index_started_run_or_compensate(session_id, snapshot)
            .await
    }

    async fn index_started_run_or_compensate(
        &self,
        session_id: &str,
        snapshot: WorkflowRunSnapshot,
    ) -> Result<WorkflowRunSnapshot, WorkflowRunError> {
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
        let progress = self
            .progress_for_session(session_id, run_id, u64::MAX)
            .await?;
        ensure_workflow_cancel_allowed(&progress.snapshot)?;
        self.engine.cancel(run_id).await
    }

    pub async fn restart_for_session(
        &self,
        session_id: &str,
        run_id: &str,
    ) -> Result<WorkflowRunSnapshot, WorkflowRunError> {
        let progress = self
            .progress_for_session(session_id, run_id, u64::MAX)
            .await?;
        ensure_workflow_restart_as_new_run_allowed(&progress.snapshot)?;
        self.ensure_run_index_capacity(session_id).await?;
        let (_, workspace_trusted) = self.session_context(session_id).await?;
        let snapshot = self
            .engine
            .restart(run_id, workspace_trusted, vec!["read".to_string()])
            .await?;
        self.index_started_run_or_compensate(session_id, snapshot)
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

fn ensure_workflow_cancel_allowed(snapshot: &WorkflowRunSnapshot) -> Result<(), WorkflowRunError> {
    match snapshot.status {
        WorkflowRunStatus::Succeeded | WorkflowRunStatus::Failed => Err(WorkflowRunError::Terminal),
        WorkflowRunStatus::Queued
        | WorkflowRunStatus::Running
        | WorkflowRunStatus::Suspended
        | WorkflowRunStatus::Cancelled => Ok(()),
    }
}

fn ensure_workflow_restart_as_new_run_allowed(
    snapshot: &WorkflowRunSnapshot,
) -> Result<(), WorkflowRunError> {
    match (snapshot.status, snapshot.suspension.as_ref()) {
        (WorkflowRunStatus::Suspended, Some(WorkflowSuspensionContext::Recovery { .. })) => Ok(()),
        (status, _) if status.is_terminal() => Err(WorkflowRunError::Terminal),
        _ => Err(WorkflowRunError::Preflight(
            "only recovery-suspended workflows can restart as a new run".to_string(),
        )),
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

/// Stable metadata-only projection of a durable workflow run.
///
/// The internal snapshot deliberately persists the complete definition bundle,
/// validated arguments, input hashes, tool outputs, and diagnostic strings for
/// deterministic recovery. None of those values are public API data. HTTP and
/// model-facing tools must serialize this projection instead of the durable
/// snapshot itself.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub(crate) struct PublicWorkflowRunSnapshot {
    pub run_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_step_id: Option<String>,
    pub session_id: String,
    pub workflow_id: String,
    pub workflow_revision: u64,
    pub definition_bundle_hash: String,
    pub status: WorkflowRunStatus,
    pub can_cancel: bool,
    pub can_restart_as_new_run: bool,
    pub planned_steps: BTreeMap<String, PublicWorkflowPlannedStep>,
    pub plan: PublicWorkflowPlan,
    pub steps: BTreeMap<String, PublicWorkflowStepSnapshot>,
    pub budget: WorkflowBudgets,
    pub usage: WorkflowBudgetUsage,
    pub child_agent_count: u32,
    pub last_sequence: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure: Option<PublicWorkflowFailure>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suspension: Option<PublicWorkflowSuspension>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub(crate) struct PublicWorkflowStepSnapshot {
    pub id: String,
    pub status: WorkflowStepStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure: Option<PublicWorkflowFailure>,
    pub attempts: u32,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct PublicWorkflowPlannedStep {
    pub id: String,
    pub kind: PublicWorkflowPlannedStepKind,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PublicWorkflowPlannedStepKind {
    Tool,
    Agent,
    Workflow,
}

/// Safe plan topology. Data bindings, map item names, delays, tool arguments,
/// prompts, schemas, and capabilities remain internal.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum PublicWorkflowPlan {
    Step {
        step: String,
    },
    Sequence {
        nodes: Vec<PublicWorkflowPlan>,
    },
    Parallel {
        nodes: Vec<PublicWorkflowPlan>,
    },
    Map {
        body: Box<PublicWorkflowPlan>,
    },
    Retry {
        node: Box<PublicWorkflowPlan>,
        max_attempts: u32,
    },
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct PublicWorkflowFailure {
    pub code: WorkflowFailureCode,
    pub message: &'static str,
    pub retryable: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum PublicWorkflowSuspension {
    ToolApproval { step_id: String },
    ToolRunning { step_id: String, killed: bool },
    Recovery,
}

/// Durable reconnect events use the same metadata-only boundary as snapshots.
/// Raw step/run outputs, suspension reasons, and backend diagnostics are never
/// serialized to HTTP, SSE consumers, or model-facing tools.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub(crate) struct PublicWorkflowRunEvent {
    pub run_id: String,
    pub sequence: u64,
    pub at: chrono::DateTime<chrono::Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub step_id: Option<String>,
    #[serde(flatten)]
    pub kind: PublicWorkflowRunEventKind,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum PublicWorkflowRunEventKind {
    RunQueued,
    RunStarted,
    Phase { name: &'static str },
    StepQueued,
    StepStarted,
    StepSuspended,
    StepCompleted,
    StepFailed { failure: PublicWorkflowFailure },
    StepCancelled,
    StepSkipped,
    RunSuspended,
    RunSucceeded,
    RunFailed { failure: PublicWorkflowFailure },
    RunCancelled,
}

fn public_workflow_plan(plan: &WorkflowPlan) -> PublicWorkflowPlan {
    match plan {
        WorkflowPlan::Step { step } => PublicWorkflowPlan::Step { step: step.clone() },
        WorkflowPlan::Sequence { nodes } => PublicWorkflowPlan::Sequence {
            nodes: nodes.iter().map(public_workflow_plan).collect(),
        },
        WorkflowPlan::Parallel { nodes } => PublicWorkflowPlan::Parallel {
            nodes: nodes.iter().map(public_workflow_plan).collect(),
        },
        WorkflowPlan::Map { body, .. } => PublicWorkflowPlan::Map {
            body: Box::new(public_workflow_plan(body)),
        },
        WorkflowPlan::Retry {
            node, max_attempts, ..
        } => PublicWorkflowPlan::Retry {
            node: Box::new(public_workflow_plan(node)),
            max_attempts: *max_attempts,
        },
    }
}

fn public_planned_steps(
    definition: &WorkflowRunDefinition,
) -> BTreeMap<String, PublicWorkflowPlannedStep> {
    definition
        .steps
        .iter()
        .map(|step| {
            let kind = match &step.kind {
                WorkflowStepKind::Tool { .. } => PublicWorkflowPlannedStepKind::Tool,
                WorkflowStepKind::Agent { .. } => PublicWorkflowPlannedStepKind::Agent,
                WorkflowStepKind::Workflow { .. } => PublicWorkflowPlannedStepKind::Workflow,
            };
            (
                step.id.clone(),
                PublicWorkflowPlannedStep {
                    id: step.id.clone(),
                    kind,
                },
            )
        })
        .collect()
}

fn public_workflow_failure(failure: WorkflowFailure) -> PublicWorkflowFailure {
    let message = match failure.code {
        WorkflowFailureCode::InvalidDefinition => "Workflow definition is invalid",
        WorkflowFailureCode::InvalidInput => "Workflow input is invalid",
        WorkflowFailureCode::InvalidOutput => "Workflow output is invalid",
        WorkflowFailureCode::UnknownReference => "Workflow reference is unavailable",
        WorkflowFailureCode::PermissionDenied => "Workflow permission was denied",
        WorkflowFailureCode::UntrustedWorkspace => "Workflow workspace is not trusted",
        WorkflowFailureCode::BudgetExceeded => "Workflow execution budget was exceeded",
        WorkflowFailureCode::RetryExhausted => "Workflow retry budget was exhausted",
        WorkflowFailureCode::ExecutionFailed => "Workflow execution failed",
        WorkflowFailureCode::Cancelled => "Workflow execution was cancelled",
        WorkflowFailureCode::RecoverySuspended => "Workflow recovery requires attention",
        WorkflowFailureCode::Suspended => "Workflow execution is suspended",
        WorkflowFailureCode::DependencySkipped => "Workflow dependency was skipped",
        WorkflowFailureCode::Storage => "Workflow storage is unavailable",
    };
    PublicWorkflowFailure {
        code: failure.code,
        message,
        retryable: failure.retryable,
    }
}

fn public_workflow_step(step: WorkflowStepSnapshot) -> PublicWorkflowStepSnapshot {
    PublicWorkflowStepSnapshot {
        id: step.id,
        status: step.status,
        failure: step.failure.map(public_workflow_failure),
        attempts: step.attempts,
    }
}

fn public_workflow_suspension(suspension: WorkflowSuspensionContext) -> PublicWorkflowSuspension {
    match suspension {
        WorkflowSuspensionContext::ToolApproval { step_id, .. } => {
            PublicWorkflowSuspension::ToolApproval { step_id }
        }
        WorkflowSuspensionContext::ToolRunning {
            step_id, killed, ..
        } => PublicWorkflowSuspension::ToolRunning { step_id, killed },
        WorkflowSuspensionContext::Recovery { .. } => PublicWorkflowSuspension::Recovery,
    }
}

fn public_workflow_phase(name: &str) -> &'static str {
    match name {
        "retry_reserved" => "retry_reserved",
        "step_reserved" => "step_reserved",
        "agent_reserved" => "agent_reserved",
        "agent_usage_recorded" => "agent_usage_recorded",
        "suspension_context_persisted" => "suspension_context_persisted",
        _ => "workflow_progressed",
    }
}

pub(crate) fn public_workflow_snapshot(snapshot: WorkflowRunSnapshot) -> PublicWorkflowRunSnapshot {
    let can_cancel = ensure_workflow_cancel_allowed(&snapshot).is_ok();
    let can_restart_as_new_run = ensure_workflow_restart_as_new_run_allowed(&snapshot).is_ok();
    let planned_steps = public_planned_steps(&snapshot.definition);
    let plan = public_workflow_plan(&snapshot.definition.plan);
    let child_agent_count = snapshot.usage.agents;
    PublicWorkflowRunSnapshot {
        run_id: snapshot.run_id,
        parent_run_id: snapshot.parent_run_id,
        parent_step_id: snapshot.parent_step_id,
        session_id: snapshot.session_id,
        workflow_id: snapshot.definition.id,
        workflow_revision: snapshot.definition.revision,
        definition_bundle_hash: snapshot.definition_bundle_hash,
        status: snapshot.status,
        can_cancel,
        can_restart_as_new_run,
        planned_steps,
        plan,
        steps: snapshot
            .steps
            .into_iter()
            .map(|(id, step)| (id, public_workflow_step(step)))
            .collect(),
        budget: snapshot.definition.budgets,
        usage: snapshot.usage,
        child_agent_count,
        last_sequence: snapshot.last_sequence,
        failure: snapshot.failure.map(public_workflow_failure),
        suspension: snapshot.suspension.map(public_workflow_suspension),
        created_at: snapshot.created_at,
        updated_at: snapshot.updated_at,
    }
}

pub(crate) fn public_workflow_event(event: WorkflowRunEvent) -> PublicWorkflowRunEvent {
    let kind = match event.kind {
        WorkflowRunEventKind::RunQueued => PublicWorkflowRunEventKind::RunQueued,
        WorkflowRunEventKind::RunStarted => PublicWorkflowRunEventKind::RunStarted,
        WorkflowRunEventKind::Phase { name } => PublicWorkflowRunEventKind::Phase {
            name: public_workflow_phase(&name),
        },
        WorkflowRunEventKind::StepQueued => PublicWorkflowRunEventKind::StepQueued,
        WorkflowRunEventKind::StepStarted => PublicWorkflowRunEventKind::StepStarted,
        WorkflowRunEventKind::StepSuspended { .. } => PublicWorkflowRunEventKind::StepSuspended,
        WorkflowRunEventKind::StepCompleted { .. } => PublicWorkflowRunEventKind::StepCompleted,
        WorkflowRunEventKind::StepFailed { failure } => PublicWorkflowRunEventKind::StepFailed {
            failure: public_workflow_failure(failure),
        },
        WorkflowRunEventKind::StepCancelled => PublicWorkflowRunEventKind::StepCancelled,
        WorkflowRunEventKind::StepSkipped { .. } => PublicWorkflowRunEventKind::StepSkipped,
        WorkflowRunEventKind::RunSuspended { .. } => PublicWorkflowRunEventKind::RunSuspended,
        WorkflowRunEventKind::RunSucceeded { .. } => PublicWorkflowRunEventKind::RunSucceeded,
        WorkflowRunEventKind::RunFailed { failure } => PublicWorkflowRunEventKind::RunFailed {
            failure: public_workflow_failure(failure),
        },
        WorkflowRunEventKind::RunCancelled => PublicWorkflowRunEventKind::RunCancelled,
    };
    PublicWorkflowRunEvent {
        run_id: event.run_id,
        sequence: event.sequence,
        at: event.at,
        step_id: event.step_id,
        kind,
    }
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
                serde_json::to_value(
                    progress
                        .events
                        .into_iter()
                        .map(public_workflow_event)
                        .collect::<Vec<_>>(),
                )
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
        WorkflowRunError::InvalidInput(_) => {
            ToolError::InvalidArguments("workflow input is invalid".to_string())
        }
        WorkflowRunError::Compile(_) => {
            ToolError::InvalidArguments("workflow definition is invalid".to_string())
        }
        WorkflowRunError::Preflight(_) => {
            ToolError::InvalidArguments("workflow preflight failed".to_string())
        }
        WorkflowRunError::Storage(details) => {
            tracing::error!(%details, "workflow tool storage unavailable");
            ToolError::Execution("workflow storage unavailable".to_string())
        }
        WorkflowRunError::NotFound => ToolError::Execution("workflow run not found".to_string()),
        WorkflowRunError::Terminal => {
            ToolError::Execution("workflow run is already terminal".to_string())
        }
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
    use bamboo_engine::WorkflowRunRepository;
    use bamboo_llm::protocol::{gemini::GeminiTool, ToProvider};
    use std::collections::HashMap;
    use tokio::sync::{RwLock, Semaphore};

    #[derive(Default)]
    struct WorkflowTestStorage {
        sessions: RwLock<HashMap<String, Session>>,
        fail_saves: AtomicBool,
    }

    impl WorkflowTestStorage {
        fn set_fail_saves(&self, fail: bool) {
            self.fail_saves.store(fail, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl Storage for WorkflowTestStorage {
        async fn save_session(&self, session: &Session) -> std::io::Result<()> {
            if self.fail_saves.load(Ordering::SeqCst) {
                return Err(std::io::Error::other(
                    "injected workflow session persistence failure",
                ));
            }
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

    struct BlockingWorkflowReadTool {
        entered: Arc<Semaphore>,
    }

    #[async_trait]
    impl ToolExecutor for BlockingWorkflowReadTool {
        async fn execute(
            &self,
            _call: &ToolCall,
        ) -> bamboo_agent_core::tools::executor::Result<ToolResult> {
            self.entered.add_permits(1);
            std::future::pending().await
        }

        fn list_tools(&self) -> Vec<ToolSchema> {
            WorkflowReadTool.list_tools()
        }
    }

    async fn workflow_test_access_with_tools(
        tools: Arc<dyn ToolExecutor>,
    ) -> (
        WorkflowRunAccess,
        bamboo_engine::SessionRepository,
        tempfile::TempDir,
    ) {
        let (access, repo, directory, _) = workflow_test_access_with_tools_and_storage(tools).await;
        (access, repo, directory)
    }

    async fn workflow_test_access_with_tools_and_storage(
        tools: Arc<dyn ToolExecutor>,
    ) -> (
        WorkflowRunAccess,
        bamboo_engine::SessionRepository,
        tempfile::TempDir,
        Arc<WorkflowTestStorage>,
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
        let storage = Arc::new(WorkflowTestStorage::default());
        let storage_port: Arc<dyn Storage> = storage.clone();
        let persistence = Arc::new(bamboo_storage::LockedSessionStore::new(
            storage_port.clone(),
        ));
        let cache = Arc::default();
        let repo = bamboo_engine::SessionRepository::new(cache, storage_port, persistence);
        let access = WorkflowRunAccess::new(directory.path(), tools, skills, repo.clone())
            .await
            .expect("workflow access");
        (access, repo, directory, storage)
    }

    async fn workflow_test_access() -> (
        WorkflowRunAccess,
        bamboo_engine::SessionRepository,
        tempfile::TempDir,
    ) {
        workflow_test_access_with_tools(Arc::new(WorkflowReadTool)).await
    }

    async fn seed_durable_workflow_run(
        access: &WorkflowRunAccess,
        directory: &Path,
        workspace: &Path,
        session_id: &str,
        run_id: &str,
        status: WorkflowRunStatus,
        suspension: Option<WorkflowSuspensionContext>,
    ) -> WorkflowRunSnapshot {
        let bundle = access
            .skills
            .pin_workflow_definition_bundle(Some(workspace), "review-flow", 42)
            .await
            .expect("pin workflow test bundle");
        let definition = bundle.root().cloned().expect("pinned root definition");
        let step_status = match status {
            WorkflowRunStatus::Queued => WorkflowStepStatus::Queued,
            WorkflowRunStatus::Running => WorkflowStepStatus::Running,
            WorkflowRunStatus::Suspended => WorkflowStepStatus::Suspended,
            WorkflowRunStatus::Succeeded => WorkflowStepStatus::Succeeded,
            WorkflowRunStatus::Failed => WorkflowStepStatus::Failed,
            WorkflowRunStatus::Cancelled => WorkflowStepStatus::Cancelled,
        };
        let now = chrono::Utc::now();
        let failure = (status == WorkflowRunStatus::Failed).then(|| WorkflowFailure {
            code: WorkflowFailureCode::ExecutionFailed,
            message: "seeded workflow failure".to_string(),
            retryable: false,
        });
        let snapshot = WorkflowRunSnapshot {
            run_id: run_id.to_string(),
            parent_run_id: None,
            parent_step_id: None,
            session_id: session_id.to_string(),
            definition: definition.clone(),
            definition_bundle: bundle,
            definition_bundle_hash: "seeded-public-bundle-hash".to_string(),
            validated_args: json!({}),
            status,
            steps: definition
                .steps
                .iter()
                .map(|step| {
                    (
                        step.id.clone(),
                        WorkflowStepSnapshot {
                            id: step.id.clone(),
                            status: step_status,
                            input_hash: "seeded-input-hash".to_string(),
                            output: None,
                            failure: failure.clone(),
                            attempts: 0,
                        },
                    )
                })
                .collect(),
            usage: WorkflowBudgetUsage::default(),
            last_sequence: 1,
            output: None,
            failure: failure.clone(),
            suspension,
            created_at: now,
            updated_at: now,
        };
        persist_durable_workflow_run(directory, &snapshot).await;
        snapshot
    }

    async fn persist_durable_workflow_run(directory: &Path, snapshot: &WorkflowRunSnapshot) {
        let kind = match snapshot.status {
            WorkflowRunStatus::Queued => WorkflowRunEventKind::RunQueued,
            WorkflowRunStatus::Running => WorkflowRunEventKind::RunStarted,
            WorkflowRunStatus::Suspended => WorkflowRunEventKind::RunSuspended {
                reason: "seeded suspension".to_string(),
            },
            WorkflowRunStatus::Succeeded => WorkflowRunEventKind::RunSucceeded {
                output: Value::Null,
            },
            WorkflowRunStatus::Failed => WorkflowRunEventKind::RunFailed {
                failure: snapshot.failure.clone().expect("failed run has failure"),
            },
            WorkflowRunStatus::Cancelled => WorkflowRunEventKind::RunCancelled,
        };
        let repository = FileWorkflowRunRepository::new(directory.join("workflow-runs"))
            .expect("open workflow test repository");
        repository
            .create(
                snapshot,
                &WorkflowRunEvent {
                    run_id: snapshot.run_id.clone(),
                    sequence: snapshot.last_sequence,
                    at: snapshot.updated_at,
                    step_id: None,
                    kind,
                },
            )
            .await
            .expect("seed durable workflow run");
    }

    async fn replace_workflow_run_index(
        repo: &bamboo_engine::SessionRepository,
        session_id: &str,
        run_ids: Vec<String>,
    ) {
        repo.update_runtime_session(
            session_id,
            &[bamboo_skills::WORKFLOW_RUN_IDS_METADATA_KEY],
            move |session| {
                session.metadata.insert(
                    bamboo_skills::WORKFLOW_RUN_IDS_METADATA_KEY.to_string(),
                    serde_json::to_string(&run_ids).expect("run index json"),
                );
            },
        )
        .await
        .expect("replace workflow run index")
        .expect("workflow session");
    }

    async fn wait_for_workflow_run_to_settle(
        access: &WorkflowRunAccess,
        run_id: &str,
    ) -> WorkflowRunSnapshot {
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let snapshot = access
                    .engine
                    .progress(run_id, u64::MAX)
                    .await
                    .expect("workflow progress")
                    .snapshot;
                if snapshot.status.is_terminal() && !access.engine.is_run_active(run_id) {
                    break snapshot;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("workflow run settles")
    }

    fn private_workflow_snapshot(status: WorkflowRunStatus) -> WorkflowRunSnapshot {
        let definition = WorkflowRunDefinition {
            workflow_schema: 1,
            id: "review-flow".to_string(),
            revision: 42,
            input_schema: json!({"private_schema": "PRIVATE-SCHEMA-SENTINEL"}),
            output_schema: Some(json!({"private_output": "PRIVATE-SCHEMA-SENTINEL"})),
            steps: vec![bamboo_domain::WorkflowStepDefinition {
                id: "inspect".to_string(),
                kind: WorkflowStepKind::Tool {
                    tool: "PRIVATE-TOOL-SENTINEL".to_string(),
                    args: json!({"credential": "PRIVATE-ARG-SENTINEL"}),
                    capabilities: vec!["PRIVATE-CAPABILITY-SENTINEL".to_string()],
                },
                failure: bamboo_domain::FailurePolicy::FailFast,
                output_schema: Some(json!({"private": "PRIVATE-STEP-SCHEMA-SENTINEL"})),
            }],
            plan: WorkflowPlan::Retry {
                node: Box::new(WorkflowPlan::Map {
                    source: bamboo_domain::ValueRef::Literal {
                        value: json!("PRIVATE-BINDING-SENTINEL"),
                    },
                    item: "PRIVATE-ITEM-SENTINEL".to_string(),
                    body: Box::new(WorkflowPlan::Step {
                        step: "inspect".to_string(),
                    }),
                }),
                max_attempts: 3,
                delay_ms: 987_654,
            },
            budgets: WorkflowBudgets {
                max_concurrency: 2,
                max_agents: 4,
                max_steps: 8,
                max_retries: 3,
                max_nesting_depth: 2,
                wall_time_ms: 10_000,
                max_tokens: Some(1_000),
                max_cost_micros: Some(2_000),
            },
        };
        let definition_bundle = WorkflowDefinitionBundle {
            publication_revision: 7,
            root_id: definition.id.clone(),
            root_revision: definition.revision,
            root_invocation_policy: json!({"private": "PRIVATE-POLICY-SENTINEL"}),
            definitions: BTreeMap::from([(
                WorkflowDefinitionBundle::key(&definition.id, definition.revision),
                definition.clone(),
            )]),
        };
        let now = chrono::Utc::now();
        WorkflowRunSnapshot {
            run_id: "public-run".to_string(),
            parent_run_id: Some("public-parent-run".to_string()),
            parent_step_id: Some("public-parent-step".to_string()),
            session_id: "public-session".to_string(),
            definition,
            definition_bundle,
            definition_bundle_hash: "public-bundle-hash".to_string(),
            validated_args: json!({"password": "PRIVATE-VALIDATED-ARG-SENTINEL"}),
            status,
            steps: BTreeMap::from([(
                "inspect".to_string(),
                WorkflowStepSnapshot {
                    id: "inspect".to_string(),
                    status: WorkflowStepStatus::Failed,
                    input_hash: "PRIVATE-INPUT-HASH-SENTINEL".to_string(),
                    output: Some(json!({"raw_tool_output": "PRIVATE-OUTPUT-SENTINEL"})),
                    failure: Some(WorkflowFailure {
                        code: WorkflowFailureCode::Storage,
                        message: "/private/workspace/PRIVATE-DIAGNOSTIC-SENTINEL".to_string(),
                        retryable: true,
                    }),
                    attempts: 2,
                },
            )]),
            usage: WorkflowBudgetUsage {
                steps: 1,
                retries: 1,
                agents: 3,
                tokens: 40,
                cost_micros: 50,
            },
            last_sequence: 9,
            output: Some(json!({"raw_run_output": "PRIVATE-RUN-OUTPUT-SENTINEL"})),
            failure: Some(WorkflowFailure {
                code: WorkflowFailureCode::ExecutionFailed,
                message: "credential PRIVATE-RUN-FAILURE-SENTINEL".to_string(),
                retryable: false,
            }),
            suspension: Some(WorkflowSuspensionContext::ToolApproval {
                step_id: "inspect".to_string(),
                tool: "PRIVATE-SUSPENSION-TOOL-SENTINEL".to_string(),
                tool_call_id: "PRIVATE-TOOL-CALL-SENTINEL".to_string(),
            }),
            created_at: now,
            updated_at: now,
        }
    }

    #[test]
    fn public_workflow_snapshot_is_stable_metadata_only_for_every_status() {
        let statuses = [
            (WorkflowRunStatus::Queued, "queued", true),
            (WorkflowRunStatus::Running, "running", true),
            (WorkflowRunStatus::Suspended, "suspended", true),
            (WorkflowRunStatus::Succeeded, "succeeded", false),
            (WorkflowRunStatus::Failed, "failed", false),
            (WorkflowRunStatus::Cancelled, "cancelled", true),
        ];

        for (status, wire_status, can_cancel) in statuses {
            let public =
                serde_json::to_value(public_workflow_snapshot(private_workflow_snapshot(status)))
                    .expect("public snapshot serializes");
            let text = public.to_string();
            assert_eq!(public["status"], wire_status);
            assert_eq!(public["can_cancel"], can_cancel);
            assert_eq!(public["can_restart_as_new_run"], false);
            assert_eq!(public["workflow_id"], "review-flow");
            assert_eq!(public["workflow_revision"], 42);
            assert_eq!(public["definition_bundle_hash"], "public-bundle-hash");
            assert_eq!(public["planned_steps"]["inspect"]["kind"], "tool");
            assert_eq!(public["plan"]["type"], "retry");
            assert_eq!(public["steps"]["inspect"]["attempts"], 2);
            assert_eq!(public["budget"]["max_steps"], 8);
            assert_eq!(public["usage"]["agents"], 3);
            assert_eq!(public["child_agent_count"], 3);
            assert_eq!(public["last_sequence"], 9);
            assert_eq!(public["failure"]["message"], "Workflow execution failed");
            assert_eq!(public["suspension"]["type"], "tool_approval");
            for internal_field in [
                "definition",
                "definition_bundle",
                "validated_args",
                "output",
            ] {
                assert!(
                    public.get(internal_field).is_none(),
                    "public snapshot exposed internal field {internal_field}: {text}"
                );
            }

            for private in [
                "PRIVATE-",
                "validated_args",
                "input_hash",
                "output_schema",
                "root_invocation_policy",
                "delay_ms",
                "tool_call_id",
                "raw_tool_output",
                "raw_run_output",
            ] {
                assert!(
                    !text.contains(private),
                    "public snapshot leaked {private}: {text}"
                );
            }
        }

        let mut recovery = private_workflow_snapshot(WorkflowRunStatus::Suspended);
        recovery.suspension = Some(WorkflowSuspensionContext::Recovery {
            reason: "PRIVATE-RECOVERY-REASON-SENTINEL".to_string(),
        });
        let public = serde_json::to_value(public_workflow_snapshot(recovery))
            .expect("public recovery snapshot serializes");
        let text = public.to_string();
        assert_eq!(public["can_cancel"], true);
        assert_eq!(public["can_restart_as_new_run"], true);
        assert_eq!(public["suspension"]["type"], "recovery");
        assert!(!text.contains("PRIVATE-RECOVERY-REASON-SENTINEL"));
    }

    #[tokio::test]
    async fn public_workflow_actions_match_cancel_and_restart_endpoint_acceptance() {
        let (access, repo, directory) = workflow_test_access().await;
        let workspace = directory.path().join("action-workspace");
        std::fs::create_dir_all(&workspace).expect("action workspace");
        let session_id = "workflow-action-matrix";
        let mut session = Session::new(session_id, "model");
        session.workspace = Some(workspace.to_string_lossy().into_owned());
        repo.save(&mut session).await.expect("save action session");

        let cases = vec![
            ("queued", WorkflowRunStatus::Queued, None, true, false),
            ("running", WorkflowRunStatus::Running, None, true, false),
            (
                "suspended_without_context",
                WorkflowRunStatus::Suspended,
                None,
                true,
                false,
            ),
            (
                "tool_approval",
                WorkflowRunStatus::Suspended,
                Some(WorkflowSuspensionContext::ToolApproval {
                    step_id: "inspect".to_string(),
                    tool: "Read".to_string(),
                    tool_call_id: "approval-call".to_string(),
                }),
                true,
                false,
            ),
            (
                "tool_running",
                WorkflowRunStatus::Suspended,
                Some(WorkflowSuspensionContext::ToolRunning {
                    step_id: "inspect".to_string(),
                    tool: "Read".to_string(),
                    tool_call_id: "running-call".to_string(),
                    killed: true,
                }),
                true,
                false,
            ),
            (
                "recovery",
                WorkflowRunStatus::Suspended,
                Some(WorkflowSuspensionContext::Recovery {
                    reason: "process restarted".to_string(),
                }),
                true,
                true,
            ),
            (
                "succeeded",
                WorkflowRunStatus::Succeeded,
                None,
                false,
                false,
            ),
            ("failed", WorkflowRunStatus::Failed, None, false, false),
            ("cancelled", WorkflowRunStatus::Cancelled, None, true, false),
        ];

        for (name, status, suspension, can_cancel, can_restart_as_new_run) in cases {
            let cancel_run_id = format!("action-cancel-{name}");
            let cancel_snapshot = seed_durable_workflow_run(
                &access,
                directory.path(),
                &workspace,
                session_id,
                &cancel_run_id,
                status,
                suspension.clone(),
            )
            .await;
            let cancel_public = public_workflow_snapshot(cancel_snapshot);
            assert_eq!(
                cancel_public.can_cancel, can_cancel,
                "cancel projection mismatch for {name}"
            );
            assert_eq!(
                access
                    .cancel_for_session(session_id, &cancel_run_id)
                    .await
                    .is_ok(),
                can_cancel,
                "cancel endpoint mismatch for {name}"
            );

            let restart_run_id = format!("action-restart-{name}");
            let restart_snapshot = seed_durable_workflow_run(
                &access,
                directory.path(),
                &workspace,
                session_id,
                &restart_run_id,
                status,
                suspension,
            )
            .await;
            let restart_public = public_workflow_snapshot(restart_snapshot);
            assert_eq!(
                restart_public.can_restart_as_new_run, can_restart_as_new_run,
                "restart projection mismatch for {name}"
            );
            let restarted = access
                .restart_for_session(session_id, &restart_run_id)
                .await;
            assert_eq!(
                restarted.is_ok(),
                can_restart_as_new_run,
                "restart endpoint mismatch for {name}: {restarted:?}"
            );
            if let Ok(restarted) = restarted {
                let _ = access
                    .cancel_for_session(session_id, &restarted.run_id)
                    .await;
            }
        }
    }

    #[tokio::test]
    async fn restart_as_new_run_is_indexed_isolated_and_survives_reconstruction() {
        let (access, repo, directory, storage) =
            workflow_test_access_with_tools_and_storage(Arc::new(WorkflowReadTool)).await;
        let workspace = directory.path().join("restart-workspace");
        std::fs::create_dir_all(&workspace).expect("restart workspace");
        let session_id = "restart-owner";
        let mut session = Session::new(session_id, "model");
        session.workspace = Some(workspace.to_string_lossy().into_owned());
        repo.save(&mut session).await.expect("save restart owner");
        let mut other = Session::new("restart-other", "model");
        other.workspace = Some(workspace.to_string_lossy().into_owned());
        repo.save(&mut other).await.expect("save other session");

        let original = seed_durable_workflow_run(
            &access,
            directory.path(),
            &workspace,
            session_id,
            "recovery-original",
            WorkflowRunStatus::Suspended,
            Some(WorkflowSuspensionContext::Recovery {
                reason: "process restarted".to_string(),
            }),
        )
        .await;
        replace_workflow_run_index(&repo, session_id, vec![original.run_id.clone()]).await;

        let restarted = access
            .restart_for_session(session_id, &original.run_id)
            .await
            .expect("restart recovery suspension as a new run");
        assert_ne!(restarted.run_id, original.run_id);
        assert_eq!(restarted.session_id, session_id);
        assert_eq!(
            access
                .progress_for_session(session_id, &original.run_id, u64::MAX)
                .await
                .expect("original progress")
                .snapshot,
            original,
            "restart-as-new must not mutate the original suspended run"
        );

        let immediate = access
            .list_for_session(session_id)
            .await
            .expect("immediate owner list");
        let immediate_ids = immediate
            .iter()
            .map(|snapshot| snapshot.run_id.as_str())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            immediate_ids,
            BTreeSet::from([original.run_id.as_str(), restarted.run_id.as_str()])
        );
        assert!(access
            .list_for_session("restart-other")
            .await
            .expect("isolated other list")
            .is_empty());
        assert!(matches!(
            access
                .progress_for_session("restart-other", &restarted.run_id, u64::MAX)
                .await,
            Err(WorkflowRunError::NotFound)
        ));
        assert!(matches!(
            access
                .restart_for_session("restart-other", &original.run_id)
                .await,
            Err(WorkflowRunError::NotFound)
        ));

        let settled = wait_for_workflow_run_to_settle(&access, &restarted.run_id).await;
        assert_eq!(settled.status, WorkflowRunStatus::Succeeded);
        let skills = access.skills.clone();
        drop(access);
        drop(repo);

        let storage_port: Arc<dyn Storage> = storage;
        let reopened_repo = bamboo_engine::SessionRepository::new(
            Arc::default(),
            storage_port.clone(),
            Arc::new(bamboo_storage::LockedSessionStore::new(storage_port)),
        );
        let reopened = WorkflowRunAccess::new(
            directory.path(),
            Arc::new(WorkflowReadTool),
            skills,
            reopened_repo,
        )
        .await
        .expect("reconstruct workflow access");
        let reconstructed = reopened
            .list_for_session(session_id)
            .await
            .expect("list after reconstruction");
        let reconstructed_ids = reconstructed
            .iter()
            .map(|snapshot| snapshot.run_id.as_str())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            reconstructed_ids,
            BTreeSet::from([original.run_id.as_str(), restarted.run_id.as_str()])
        );
        assert_eq!(
            reopened
                .progress_for_session(session_id, &original.run_id, u64::MAX)
                .await
                .expect("reconstructed original")
                .snapshot,
            original
        );
    }

    #[tokio::test]
    async fn restart_capacity_preflight_creates_no_new_run_or_active_orphan() {
        let (access, repo, directory) = workflow_test_access().await;
        let workspace = directory.path().join("restart-capacity-workspace");
        std::fs::create_dir_all(&workspace).expect("restart capacity workspace");
        let session_id = "restart-capacity";
        let mut session = Session::new(session_id, "model");
        session.workspace = Some(workspace.to_string_lossy().into_owned());
        repo.save(&mut session)
            .await
            .expect("save restart capacity session");

        let original = seed_durable_workflow_run(
            &access,
            directory.path(),
            &workspace,
            session_id,
            "capacity-run-0",
            WorkflowRunStatus::Suspended,
            Some(WorkflowSuspensionContext::Recovery {
                reason: "process restarted".to_string(),
            }),
        )
        .await;
        let mut run_ids = vec![original.run_id.clone()];
        for index in 1..MAX_WORKFLOW_RUN_IDS_PER_SESSION {
            let mut snapshot = original.clone();
            snapshot.run_id = format!("capacity-run-{index}");
            snapshot.created_at = chrono::Utc::now();
            snapshot.updated_at = snapshot.created_at;
            persist_durable_workflow_run(directory.path(), &snapshot).await;
            run_ids.push(snapshot.run_id);
        }
        replace_workflow_run_index(&repo, session_id, run_ids).await;
        let before = access
            .engine
            .list_run_ids()
            .await
            .expect("run ids before capacity rejection")
            .into_iter()
            .collect::<BTreeSet<_>>();

        assert!(matches!(
            access
                .restart_for_session(session_id, &original.run_id)
                .await,
            Err(WorkflowRunError::Preflight(message))
                if message == "workflow run index is full of active runs"
        ));
        let after = access
            .engine
            .list_run_ids()
            .await
            .expect("run ids after capacity rejection")
            .into_iter()
            .collect::<BTreeSet<_>>();
        assert_eq!(
            after, before,
            "capacity rejection must precede run creation"
        );
        assert!(after
            .iter()
            .all(|run_id| !access.engine.is_run_active(run_id)));
        assert_eq!(
            access
                .progress_for_session(session_id, &original.run_id, u64::MAX)
                .await
                .expect("original after capacity rejection")
                .snapshot,
            original
        );
    }

    #[tokio::test]
    async fn restart_index_persistence_failure_cancels_the_unindexed_new_run() {
        let (access, repo, directory, storage) =
            workflow_test_access_with_tools_and_storage(Arc::new(BlockingWorkflowReadTool {
                entered: Arc::new(Semaphore::new(0)),
            }))
            .await;
        let workspace = directory.path().join("restart-failure-workspace");
        std::fs::create_dir_all(&workspace).expect("restart failure workspace");
        let session_id = "restart-index-failure";
        let mut session = Session::new(session_id, "model");
        session.workspace = Some(workspace.to_string_lossy().into_owned());
        repo.save(&mut session)
            .await
            .expect("save restart failure session");
        let original = seed_durable_workflow_run(
            &access,
            directory.path(),
            &workspace,
            session_id,
            "failure-original",
            WorkflowRunStatus::Suspended,
            Some(WorkflowSuspensionContext::Recovery {
                reason: "process restarted".to_string(),
            }),
        )
        .await;
        replace_workflow_run_index(&repo, session_id, vec![original.run_id.clone()]).await;
        let before = access
            .engine
            .list_run_ids()
            .await
            .expect("run ids before injected failure")
            .into_iter()
            .collect::<BTreeSet<_>>();

        storage.set_fail_saves(true);
        let error = access
            .restart_for_session(session_id, &original.run_id)
            .await
            .expect_err("injected index persistence failure");
        storage.set_fail_saves(false);
        assert!(matches!(error, WorkflowRunError::Storage(_)));

        let after = access
            .engine
            .list_run_ids()
            .await
            .expect("run ids after injected failure")
            .into_iter()
            .collect::<BTreeSet<_>>();
        let created = after.difference(&before).cloned().collect::<Vec<_>>();
        assert_eq!(created.len(), 1, "restart should have created one new run");
        let unindexed_run_id = &created[0];
        let compensated = wait_for_workflow_run_to_settle(&access, unindexed_run_id).await;
        assert_eq!(compensated.status, WorkflowRunStatus::Cancelled);
        assert!(!access.engine.is_run_active(unindexed_run_id));
        assert_eq!(
            access
                .progress_for_session(session_id, &original.run_id, u64::MAX)
                .await
                .expect("original after index failure")
                .snapshot,
            original
        );
        let listed = access
            .list_for_session(session_id)
            .await
            .expect("owner list after index failure");
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].run_id, original.run_id);
        let durable = storage
            .load_session(session_id)
            .await
            .expect("load durable owner")
            .expect("durable owner");
        let durable_ids = durable
            .metadata
            .get(bamboo_skills::WORKFLOW_RUN_IDS_METADATA_KEY)
            .and_then(|raw| serde_json::from_str::<Vec<String>>(raw).ok())
            .expect("durable run index");
        assert_eq!(durable_ids, vec![original.run_id]);
        assert!(!durable_ids.contains(unindexed_run_id));
    }

    #[test]
    fn public_workflow_events_preserve_sequence_and_drop_private_payloads() {
        let private = "PRIVATE-EVENT-SENTINEL";
        let failure = WorkflowFailure {
            code: WorkflowFailureCode::ExecutionFailed,
            message: format!("/private/workspace/{private}"),
            retryable: true,
        };
        let kinds = vec![
            WorkflowRunEventKind::RunQueued,
            WorkflowRunEventKind::RunStarted,
            WorkflowRunEventKind::Phase {
                name: private.to_string(),
            },
            WorkflowRunEventKind::StepQueued,
            WorkflowRunEventKind::StepStarted,
            WorkflowRunEventKind::StepSuspended {
                reason: private.to_string(),
            },
            WorkflowRunEventKind::StepCompleted {
                output: json!({"raw": private}),
            },
            WorkflowRunEventKind::StepFailed {
                failure: failure.clone(),
            },
            WorkflowRunEventKind::StepCancelled,
            WorkflowRunEventKind::StepSkipped {
                reason: private.to_string(),
            },
            WorkflowRunEventKind::RunSuspended {
                reason: private.to_string(),
            },
            WorkflowRunEventKind::RunSucceeded {
                output: json!({"raw": private}),
            },
            WorkflowRunEventKind::RunFailed { failure },
            WorkflowRunEventKind::RunCancelled,
        ];
        let at = chrono::Utc::now();
        let events = kinds
            .into_iter()
            .enumerate()
            .map(|(index, kind)| {
                public_workflow_event(WorkflowRunEvent {
                    run_id: "public-run".to_string(),
                    sequence: index as u64 + 1,
                    at,
                    step_id: Some("inspect".to_string()),
                    kind,
                })
            })
            .collect::<Vec<_>>();
        let public = serde_json::to_value(&events).expect("public events serialize");
        let text = public.to_string();

        assert!(!text.contains(private), "public events leaked: {text}");
        assert_eq!(public[2]["name"], "workflow_progressed");
        assert_eq!(public[6]["type"], "step_completed");
        assert!(public[6].get("output").is_none());
        assert_eq!(public[7]["failure"]["message"], "Workflow execution failed");
        assert_eq!(public[11]["type"], "run_succeeded");
        assert!(public[11].get("output").is_none());
        assert_eq!(
            events
                .iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>(),
            (1..=14).collect::<Vec<_>>()
        );
    }

    #[test]
    fn workflow_tool_errors_never_expose_backend_diagnostics() {
        let sentinel = "/private/workspace/credentials-PRIVATE-SENTINEL";
        let cases = [
            WorkflowRunError::Storage(sentinel.to_string()),
            WorkflowRunError::InvalidInput(sentinel.to_string()),
            WorkflowRunError::Preflight(sentinel.to_string()),
        ];
        for error in cases {
            assert!(!workflow_tool_error(error).to_string().contains(sentinel));
        }
        assert!(!workflow_tool_error(WorkflowRunError::Compile(
            bamboo_domain::WorkflowCompileError::InvalidSchema(sentinel.to_string())
        ))
        .to_string()
        .contains(sentinel));
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

        // Recreate the adapter over the same durable journal, then combine the
        // snapshot with only the tail after sequence 4. This is the reconnect
        // contract used by Lotus after a process or transport restart.
        let run_id = started.run_id.clone();
        let skills = access.skills.clone();
        drop(access);
        let reopened = WorkflowRunAccess::new(
            directory.path(),
            Arc::new(WorkflowReadTool),
            skills,
            repo.clone(),
        )
        .await
        .expect("reopen workflow adapter");
        let reconnected = reopened
            .progress_for_session("workflow-session", &run_id, 4)
            .await
            .expect("durable reconnect");
        assert_eq!(reconnected.snapshot.status, WorkflowRunStatus::Succeeded);
        assert_eq!(
            reconnected
                .events
                .iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>(),
            vec![5, 6, 7]
        );
        assert_eq!(
            public_workflow_snapshot(reconnected.snapshot).last_sequence,
            7
        );
        assert!(matches!(
            public_workflow_event(reconnected.events.last().cloned().expect("tail event")).kind,
            PublicWorkflowRunEventKind::RunSucceeded
        ));
        assert_eq!(
            reopened
                .list_for_session("workflow-session")
                .await
                .expect("reconnected list")
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn workflow_cancel_is_idempotent_and_reconnects_to_one_terminal_event() {
        let entered = Arc::new(Semaphore::new(0));
        let (access, repo, directory) =
            workflow_test_access_with_tools(Arc::new(BlockingWorkflowReadTool {
                entered: entered.clone(),
            }))
            .await;
        let workspace = directory.path().join("workspace");
        std::fs::create_dir_all(&workspace).expect("workspace");
        let mut session = Session::new("cancel-session", "model");
        session.workspace = Some(workspace.to_string_lossy().into_owned());
        repo.save(&mut session).await.expect("save session");

        let started = access
            .start("cancel-session", "review-flow", 42, json!({}), None)
            .await
            .expect("start blocking run");
        let _entered = tokio::time::timeout(std::time::Duration::from_secs(2), entered.acquire())
            .await
            .expect("blocking step entered")
            .expect("semaphore open");

        let first = access
            .cancel_for_session("cancel-session", &started.run_id)
            .await
            .expect("first cancel");
        let second = access
            .cancel_for_session("cancel-session", &started.run_id)
            .await
            .expect("idempotent cancel");
        assert_eq!(first.status, WorkflowRunStatus::Cancelled);
        assert_eq!(second.status, WorkflowRunStatus::Cancelled);
        assert_eq!(first.last_sequence, second.last_sequence);

        let progress = access
            .progress_for_session("cancel-session", &started.run_id, 0)
            .await
            .expect("cancelled journal");
        assert_eq!(progress.snapshot.status, WorkflowRunStatus::Cancelled);
        assert_eq!(progress.snapshot.last_sequence, first.last_sequence);
        assert_eq!(
            progress
                .events
                .iter()
                .filter(|event| matches!(event.kind, WorkflowRunEventKind::RunCancelled))
                .count(),
            1
        );
        assert!(!progress
            .events
            .iter()
            .any(|event| matches!(event.kind, WorkflowRunEventKind::RunSucceeded { .. })));
        assert_eq!(
            progress
                .events
                .iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>(),
            (1..=progress.snapshot.last_sequence).collect::<Vec<_>>()
        );
        let tail = access
            .progress_for_session(
                "cancel-session",
                &started.run_id,
                progress.snapshot.last_sequence,
            )
            .await
            .expect("terminal reconnect tail");
        assert!(tail.events.is_empty());
        assert!(matches!(
            public_workflow_snapshot(second).status,
            WorkflowRunStatus::Cancelled
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
