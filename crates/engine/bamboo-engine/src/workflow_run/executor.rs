use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Weak,
};
use std::time::Duration;

use async_trait::async_trait;
use bamboo_agent_core::tools::{
    FunctionCall, ToolCall, ToolExecutionContext, ToolExecutionSessionFlags, ToolExecutor,
    ToolOutcome, ToolResult,
};
use bamboo_domain::{
    validate_schema, CompiledWorkflow, FailurePolicy, StartWorkflowRun, ValueRef,
    WorkflowBudgetUsage, WorkflowBudgets, WorkflowCompileError, WorkflowDefinitionBundle,
    WorkflowFailure, WorkflowFailureCode, WorkflowPlan, WorkflowProgress, WorkflowRunDefinition,
    WorkflowRunEvent, WorkflowRunEventKind, WorkflowRunSnapshot, WorkflowRunStatus,
    WorkflowStepDefinition, WorkflowStepKind, WorkflowStepSnapshot, WorkflowStepStatus,
    WorkflowSuspensionContext,
};
use chrono::Utc;
use dashmap::{mapref::entry::Entry, DashMap};
use futures::{future::join_all, stream::FuturesUnordered, StreamExt};
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::sync::{broadcast, Mutex, Semaphore};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::repository::WorkflowRunRepository;

type SecretResolutionFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(Value, Vec<String>), WorkflowFailure>> + Send + 'a>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamedAgentSpec {
    pub name: String,
    pub allowed_capabilities: BTreeSet<String>,
    pub profile:
        Option<Arc<crate::session_app::child_session::named_profile::ResolvedChildProfile>>,
    pub cost_supported: bool,
}

#[derive(Debug, Clone)]
pub struct AgentStepResult {
    pub output: Value,
    pub tokens: u64,
    pub cost_micros: Option<u64>,
    /// A completed attempt may fail after consuming tokens; its usage still counts.
    pub failure: Option<WorkflowFailure>,
    /// Process-local ownership handoff; absent for ports without retained results.
    pub attempt_id: Option<String>,
}

#[async_trait]
pub trait AgentStepPort: Send + Sync {
    /// #563 seam. Unknown names must return `Ok(None)` and fail preflight.
    async fn resolve(&self, name: &str, session_id: &str)
        -> Result<Option<NamedAgentSpec>, String>;
    async fn execute(
        &self,
        spec: &NamedAgentSpec,
        prompt: Value,
        model: Option<&str>,
        effort: Option<&str>,
        capabilities: &BTreeSet<String>,
        session_id: &str,
        root_run_id: &str,
        cancellation: CancellationToken,
    ) -> Result<AgentStepResult, String>;
    /// Consume a retained result while the caller owns the usage ledger lock.
    fn acknowledge_result(&self, _attempt_id: &str) -> bool {
        true
    }
    /// Stop and drain cancelled executions owned by this root run.
    async fn drain_cancelled(
        &self,
        _root_run_id: &str,
    ) -> (Vec<Result<AgentStepResult, String>>, Option<String>) {
        (Vec::new(), None)
    }
}

#[async_trait]
pub trait WorkflowDefinitionPort: Send + Sync {
    /// Pin the root and every transitively referenced nested definition from one
    /// immutable catalog publication. Implementations must never re-read a live
    /// source while constructing the returned bundle.
    async fn pin_bundle(
        &self,
        root: &WorkflowRunDefinition,
    ) -> Result<WorkflowDefinitionBundle, String>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PermissionDecision {
    Allow,
    Deny(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkflowPolicyTarget {
    Tool(String),
    Agent(String),
    Workflow { id: String, revision: u64 },
}

#[async_trait]
pub trait WorkflowPolicyPort: Send + Sync {
    async fn authorize(
        &self,
        session_id: &str,
        target: &WorkflowPolicyTarget,
        requested: &BTreeSet<String>,
        workspace_trusted: bool,
    ) -> PermissionDecision;
}

#[async_trait]
pub trait WorkflowSessionPermissionPort: Send + Sync {
    async fn flags_for_session(
        &self,
        session_id: &str,
    ) -> Result<ToolExecutionSessionFlags, String>;
}

/// Resolves a typed, persisted-safe capability handle to ephemeral secret
/// material. Implementations own access control; raw values are never returned
/// in snapshots/events/errors.
pub struct WorkflowSecretMaterial(String);

impl WorkflowSecretMaterial {
    pub fn new(value: String) -> Self {
        Self(value)
    }

    fn into_exposed(self) -> String {
        self.0
    }
}

#[async_trait]
pub trait WorkflowSecretResolverPort: Send + Sync {
    async fn resolve(
        &self,
        session_id: &str,
        capability: &str,
    ) -> Result<WorkflowSecretMaterial, String>;
}

#[derive(Debug, Error)]
pub enum WorkflowRunError {
    #[error(transparent)]
    Compile(#[from] WorkflowCompileError),
    #[error("invalid workflow input: {0}")]
    InvalidInput(String),
    #[error("workflow preflight failed: {0}")]
    Preflight(String),
    #[error("named agents without monetary measurement do not support a finite monetary budget")]
    UnsupportedMonetaryBudget,
    #[error("workflow storage failed: {0}")]
    Storage(String),
    #[error("workflow run not found")]
    NotFound,
    #[error("workflow run is already terminal")]
    Terminal,
}

pub struct WorkflowRunEngine {
    repository: Arc<dyn WorkflowRunRepository>,
    tools: Arc<dyn ToolExecutor>,
    agents: Arc<dyn AgentStepPort>,
    definitions: Arc<dyn WorkflowDefinitionPort>,
    policy: Arc<dyn WorkflowPolicyPort>,
    secrets: Arc<dyn WorkflowSecretResolverPort>,
    ceilings: WorkflowBudgets,
    active: DashMap<String, Arc<ActiveRun>>,
    events: DashMap<String, broadcast::Sender<WorkflowRunEvent>>,
    session_permissions: std::sync::RwLock<Option<Arc<dyn WorkflowSessionPermissionPort>>>,
}

struct ActiveRun {
    cancellation: CancellationToken,
    snapshot: Arc<Mutex<WorkflowRunSnapshot>>,
    root_run_id: String,
    ledger: Arc<Mutex<WorkflowBudgetUsage>>,
    drain_lock: Arc<Mutex<()>>,
    cleanup_pending: AtomicBool,
    has_agents: bool,
}

struct RuntimeRegistration {
    engine: Weak<WorkflowRunEngine>,
    run_id: String,
}

impl Drop for RuntimeRegistration {
    fn drop(&mut self) {
        if let Some(engine) = self.engine.upgrade() {
            let pending = engine
                .active
                .get(&self.run_id)
                .is_some_and(|run| run.cleanup_pending.load(Ordering::Acquire));
            if !pending {
                engine.active.remove(&self.run_id);
                engine.events.remove(&self.run_id);
            }
        }
    }
}

struct RunContext {
    engine: Arc<WorkflowRunEngine>,
    compiled: Arc<CompiledWorkflow>,
    bundle: Arc<WorkflowDefinitionBundle>,
    pinned_agents: Arc<HashMap<String, NamedAgentSpec>>,
    snapshot: Arc<Mutex<WorkflowRunSnapshot>>,
    cancellation: CancellationToken,
    branch_cancellation: CancellationToken,
    allowed_capabilities: BTreeSet<String>,
    workspace_trusted: bool,
    semaphore: Arc<Semaphore>,
    items: HashMap<String, Value>,
    scope: String,
    depth: u32,
    ledger: Arc<Mutex<WorkflowBudgetUsage>>,
    root_limits: WorkflowBudgets,
    root_run_id: String,
    drain_lock: Arc<Mutex<()>>,
}

impl Clone for RunContext {
    fn clone(&self) -> Self {
        Self {
            engine: self.engine.clone(),
            compiled: self.compiled.clone(),
            bundle: self.bundle.clone(),
            pinned_agents: self.pinned_agents.clone(),
            snapshot: self.snapshot.clone(),
            cancellation: self.cancellation.clone(),
            branch_cancellation: self.branch_cancellation.clone(),
            allowed_capabilities: self.allowed_capabilities.clone(),
            workspace_trusted: self.workspace_trusted,
            semaphore: self.semaphore.clone(),
            items: self.items.clone(),
            scope: self.scope.clone(),
            depth: self.depth,
            ledger: self.ledger.clone(),
            root_limits: self.root_limits.clone(),
            root_run_id: self.root_run_id.clone(),
            drain_lock: self.drain_lock.clone(),
        }
    }
}

type NodeFuture<'a> = Pin<Box<dyn Future<Output = Result<Value, WorkflowFailure>> + Send + 'a>>;
type StartSignal =
    Arc<Mutex<Option<tokio::sync::oneshot::Sender<Result<WorkflowRunSnapshot, WorkflowRunError>>>>>;

impl WorkflowRunEngine {
    pub fn new(
        repository: Arc<dyn WorkflowRunRepository>,
        tools: Arc<dyn ToolExecutor>,
        agents: Arc<dyn AgentStepPort>,
        definitions: Arc<dyn WorkflowDefinitionPort>,
        policy: Arc<dyn WorkflowPolicyPort>,
        secrets: Arc<dyn WorkflowSecretResolverPort>,
        ceilings: WorkflowBudgets,
    ) -> Arc<Self> {
        Arc::new(Self {
            repository,
            tools,
            agents,
            definitions,
            policy,
            secrets,
            ceilings,
            active: DashMap::new(),
            events: DashMap::new(),
            session_permissions: std::sync::RwLock::new(None),
        })
    }

    pub fn set_session_permission_port(&self, port: Arc<dyn WorkflowSessionPermissionPort>) {
        *self
            .session_permissions
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(port);
    }

    pub async fn run(
        self: &Arc<Self>,
        request: StartWorkflowRun,
    ) -> Result<WorkflowRunSnapshot, WorkflowRunError> {
        let bundle = self.pin_and_validate_bundle(&request.definition).await?;
        self.run_pinned(request, bundle).await
    }

    pub async fn run_pinned(
        self: &Arc<Self>,
        request: StartWorkflowRun,
        bundle: WorkflowDefinitionBundle,
    ) -> Result<WorkflowRunSnapshot, WorkflowRunError> {
        self.validate_bundle(&request.definition, &bundle)?;
        let cancellation = CancellationToken::new();
        let ledger = Arc::new(Mutex::new(WorkflowBudgetUsage::default()));
        let limits = effective_limits(&request.definition.budgets, &self.ceilings);
        let semaphore = Arc::new(Semaphore::new(limits.max_concurrency));
        let pinned_agents = Arc::new(
            self.preflight_bundle(
                &bundle,
                &request.session_id,
                &request.allowed_capabilities.iter().cloned().collect(),
                request.workspace_trusted,
                &limits,
            )
            .await?,
        );
        self.run_internal(
            request,
            Arc::new(bundle),
            pinned_agents,
            None,
            None,
            0,
            cancellation,
            ledger,
            limits,
            semaphore,
            None,
            None,
        )
        .await
    }

    /// Start in the background and return only after the running snapshot is
    /// durable. HTTP and tool adapters use this non-blocking entrypoint.
    pub async fn start(
        self: &Arc<Self>,
        request: StartWorkflowRun,
    ) -> Result<WorkflowRunSnapshot, WorkflowRunError> {
        let bundle = self.pin_and_validate_bundle(&request.definition).await?;
        self.start_pinned(request, bundle).await
    }

    pub async fn start_pinned(
        self: &Arc<Self>,
        request: StartWorkflowRun,
        bundle: WorkflowDefinitionBundle,
    ) -> Result<WorkflowRunSnapshot, WorkflowRunError> {
        self.validate_bundle(&request.definition, &bundle)?;
        let cancellation = CancellationToken::new();
        let ledger = Arc::new(Mutex::new(WorkflowBudgetUsage::default()));
        let limits = effective_limits(&request.definition.budgets, &self.ceilings);
        let semaphore = Arc::new(Semaphore::new(limits.max_concurrency));
        let pinned_agents = Arc::new(
            self.preflight_bundle(
                &bundle,
                &request.session_id,
                &request.allowed_capabilities.iter().cloned().collect(),
                request.workspace_trusted,
                &limits,
            )
            .await?,
        );
        let (tx, rx) = tokio::sync::oneshot::channel();
        let signal = Arc::new(Mutex::new(Some(tx)));
        let engine = self.clone();
        tokio::spawn(async move {
            let result = engine
                .run_internal(
                    request,
                    Arc::new(bundle),
                    pinned_agents,
                    None,
                    None,
                    0,
                    cancellation,
                    ledger,
                    limits,
                    semaphore,
                    Some(signal.clone()),
                    None,
                )
                .await;
            if let Err(error) = result {
                if let Some(sender) = signal.lock().await.take() {
                    let _ = sender.send(Err(error));
                } else {
                    tracing::error!("background workflow run failed after start");
                }
            } else if signal.lock().await.is_some() {
                tracing::error!("workflow task completed without publishing a start snapshot");
            }
        });
        rx.await.map_err(|_| {
            WorkflowRunError::Storage("workflow task exited before durable start".to_string())
        })?
    }

    /// Phase-1 safe restart starts a fresh run from the suspended run's pinned
    /// definition snapshot. Entered-step replay and script resume remain out of scope.
    pub async fn restart(
        self: &Arc<Self>,
        run_id: &str,
        workspace_trusted: bool,
        allowed_capabilities: Vec<String>,
    ) -> Result<WorkflowRunSnapshot, WorkflowRunError> {
        let previous = self
            .repository
            .load(run_id)
            .await
            .map_err(storage)?
            .ok_or(WorkflowRunError::NotFound)?;
        if previous.status != WorkflowRunStatus::Suspended {
            return Err(if previous.status.is_terminal() {
                WorkflowRunError::Terminal
            } else {
                WorkflowRunError::Preflight("only suspended workflows can restart".to_string())
            });
        }
        if matches!(
            previous.suspension,
            Some(
                WorkflowSuspensionContext::ToolApproval { .. }
                    | WorkflowSuspensionContext::ToolRunning { .. }
            )
        ) {
            return Err(WorkflowRunError::Preflight(
                "workflow has durable suspension context and requires explicit resume handling"
                    .to_string(),
            ));
        }
        let bundle = previous.definition_bundle;
        self.start_pinned(
            StartWorkflowRun {
                definition: previous.definition,
                args: previous.validated_args,
                session_id: previous.session_id,
                workspace_trusted,
                allowed_capabilities,
            },
            bundle,
        )
        .await
    }

    /// Validate an unentered suffix using only the durable checkpoint. This is
    /// also the metadata predicate; invocation authority is checked separately.
    pub fn completed_prefix(snapshot: &WorkflowRunSnapshot) -> Result<usize, WorkflowRunError> {
        let refuse = || {
            WorkflowRunError::Preflight("workflow has no safe completed-prefix checkpoint".into())
        };
        if snapshot.status != WorkflowRunStatus::Suspended
            || !matches!(
                snapshot.suspension,
                Some(WorkflowSuspensionContext::Recovery { .. })
            )
            || snapshot.parent_run_id.is_some()
            || snapshot.parent_step_id.is_some()
            || snapshot.output.is_some()
            || snapshot.failure.is_some()
        {
            return Err(refuse());
        }
        let bundle = &snapshot.definition_bundle;
        if bundle.definitions.len() != 1
            || bundle.root_id != snapshot.definition.id
            || bundle.root_revision != snapshot.definition.revision
            || bundle.root() != Some(&snapshot.definition)
            || definition_bundle_hash(bundle)? != snapshot.definition_bundle_hash
        {
            return Err(refuse());
        }
        let compiled = CompiledWorkflow::compile(snapshot.definition.clone())?;
        compiled
            .validate_input(&snapshot.validated_args)
            .map_err(WorkflowRunError::InvalidInput)?;
        reject_secret_material(&snapshot.validated_args).map_err(WorkflowRunError::InvalidInput)?;
        let serialized = serde_json::to_value(bundle)
            .map_err(|error| WorkflowRunError::Preflight(error.to_string()))?;
        reject_secret_material_in_definition(&serialized).map_err(WorkflowRunError::Preflight)?;
        let WorkflowPlan::Sequence { nodes } = &snapshot.definition.plan else {
            return Err(refuse());
        };
        let ids = nodes
            .iter()
            .map(|node| match node {
                WorkflowPlan::Step { step } => Ok(step.as_str()),
                _ => Err(refuse()),
            })
            .collect::<Result<Vec<_>, _>>()?;
        if ids.is_empty()
            || ids.len() != compiled.steps.len()
            || ids.iter().copied().collect::<BTreeSet<_>>().len() != ids.len()
        {
            return Err(refuse());
        }
        let mut outputs = BTreeMap::new();
        let mut prefix = 0;
        let mut suffix = false;
        for id in ids.iter().copied() {
            let step = compiled.steps.get(id).ok_or_else(refuse)?;
            let WorkflowStepKind::Tool {
                args, capabilities, ..
            } = &step.kind
            else {
                return Err(refuse());
            };
            if capabilities.iter().any(|capability| capability != "read")
                || contains_secret_handle(args)
            {
                return Err(refuse());
            }
            let Some(state) = snapshot.steps.get(id) else {
                suffix = true;
                continue;
            };
            if suffix
                || state.id != id
                || state.status != WorkflowStepStatus::Succeeded
                || state.attempts != 1
                || state.failure.is_some()
            {
                return Err(refuse());
            }
            let input = resolve_checkpoint_template(args, &snapshot.validated_args, &outputs)?;
            let hash = hex::encode(Sha256::digest(
                serde_json::to_vec(&input)
                    .map_err(|error| WorkflowRunError::InvalidInput(error.to_string()))?,
            ));
            let output = state.output.as_ref().ok_or_else(refuse)?;
            if hash != state.input_hash {
                return Err(refuse());
            }
            if let Some(schema) = &step.output_schema {
                validate_schema(schema, output).map_err(|_| refuse())?;
            }
            reject_secret_material(output).map_err(|_| refuse())?;
            outputs.insert(id.to_string(), output.clone());
            prefix += 1;
        }
        let usage = &snapshot.usage;
        let budget = &snapshot.definition.budgets;
        if prefix == 0
            || snapshot.steps.len() != prefix
            || usage.steps as usize != prefix
            || usage.agents != 0
            || usage.retries != 0
            || ids.len() > budget.max_steps as usize
            || budget.max_tokens.is_some_and(|limit| usage.tokens > limit)
            || budget
                .max_cost_micros
                .is_some_and(|limit| usage.cost_micros.is_none_or(|cost| cost > limit))
            || remaining_wall_time(snapshot) == 0
        {
            return Err(refuse());
        }
        Ok(prefix)
    }

    /// Explicitly continue the same readonly run; no entered step is replayed.
    /// The original created_at + wall_time_ms deadline includes time offline.
    pub async fn continue_completed_prefix(
        self: &Arc<Self>,
        run_id: &str,
        session_id: &str,
        workspace_trusted: bool,
        allowed_capabilities: Vec<String>,
    ) -> Result<WorkflowRunSnapshot, WorkflowRunError> {
        let previous = self
            .repository
            .load(run_id)
            .await
            .map_err(storage)?
            .ok_or(WorkflowRunError::NotFound)?;
        if previous.run_id != run_id || previous.session_id != session_id {
            return Err(WorkflowRunError::NotFound);
        }
        Self::completed_prefix(&previous)?;
        let snapshot = Arc::new(Mutex::new(previous.clone()));
        let cancellation = CancellationToken::new();
        let ledger = Arc::new(Mutex::new(previous.usage.clone()));
        let drain_lock = Arc::new(Mutex::new(()));
        let active = Arc::new(ActiveRun {
            cancellation: cancellation.clone(),
            snapshot: snapshot.clone(),
            root_run_id: run_id.to_string(),
            ledger: ledger.clone(),
            drain_lock: drain_lock.clone(),
            cleanup_pending: AtomicBool::new(false),
            has_agents: false,
        });
        match self.active.entry(run_id.to_string()) {
            Entry::Occupied(_) => {
                return Err(WorkflowRunError::Preflight(
                    "workflow continuation is already active".into(),
                ))
            }
            Entry::Vacant(entry) => {
                entry.insert(active);
            }
        }
        let registration = RuntimeRegistration {
            engine: Arc::downgrade(self),
            run_id: run_id.to_string(),
        };
        // A concurrent cancellation may have committed while admission was
        // loading the checkpoint. Re-read after acquiring the existing owner.
        let current = self
            .repository
            .load(run_id)
            .await
            .map_err(storage)?
            .ok_or(WorkflowRunError::NotFound)?;
        let prefix = Self::completed_prefix(&current)?;
        if current != previous {
            return Err(WorkflowRunError::Preflight(
                "workflow checkpoint changed during continuation admission".into(),
            ));
        }
        self.validate_bundle(&current.definition, &current.definition_bundle)?;
        self.enforce_ceilings(&current.definition.budgets)?;
        let allowed_capabilities = allowed_capabilities.into_iter().collect::<BTreeSet<_>>();
        let root_limits = effective_limits(&current.definition.budgets, &self.ceilings);
        self.preflight_bundle(
            &current.definition_bundle,
            session_id,
            &allowed_capabilities,
            workspace_trusted,
            &root_limits,
        )
        .await?;
        let permission_port = self
            .session_permissions
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(port) = permission_port {
            port.flags_for_session(session_id).await.map_err(|_| {
                WorkflowRunError::Preflight(
                    "workflow session permission posture is unavailable".into(),
                )
            })?;
        }
        Self::completed_prefix(&current)?;
        let compiled = Arc::new(CompiledWorkflow::compile(current.definition.clone())?);
        let context = RunContext {
            engine: self.clone(),
            compiled,
            bundle: Arc::new(current.definition_bundle),
            pinned_agents: Arc::new(HashMap::new()),
            snapshot: snapshot.clone(),
            cancellation: cancellation.clone(),
            branch_cancellation: cancellation.child_token(),
            allowed_capabilities,
            workspace_trusted,
            semaphore: Arc::new(Semaphore::new(root_limits.max_concurrency)),
            items: HashMap::new(),
            scope: "root".into(),
            depth: 0,
            ledger,
            root_limits,
            root_run_id: run_id.to_string(),
            drain_lock,
        };
        let (sender, _) = broadcast::channel(256);
        self.events.insert(run_id.to_string(), sender);
        let (tx, rx) = tokio::sync::oneshot::channel();
        let engine = self.clone();
        tokio::spawn(async move {
            let _registration = registration;
            let start = {
                let mut snapshot = snapshot.lock().await;
                if cancellation.is_cancelled() {
                    Err(WorkflowRunError::Preflight(
                        "workflow continuation was cancelled during admission".into(),
                    ))
                } else {
                    engine
                        .transition(
                            &mut snapshot,
                            None,
                            WorkflowRunEventKind::RunStarted,
                            |snapshot| {
                                snapshot.status = WorkflowRunStatus::Running;
                                snapshot.suspension = None;
                            },
                        )
                        .await
                        .map(|()| snapshot.clone())
                }
            };
            match start {
                Ok(started) => {
                    let timeout_ms = remaining_wall_time(&started);
                    let _ = tx.send(Ok(started));
                    if engine
                        .execute_context(context, timeout_ms, prefix)
                        .await
                        .is_err()
                    {
                        tracing::error!(
                            "background workflow continuation failed after durable start"
                        );
                    }
                }
                Err(error) => {
                    let _ = tx.send(Err(error));
                }
            }
        });
        rx.await.map_err(|_| {
            WorkflowRunError::Storage("workflow continuation exited before durable start".into())
        })?
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_internal(
        self: &Arc<Self>,
        request: StartWorkflowRun,
        bundle: Arc<WorkflowDefinitionBundle>,
        pinned_agents: Arc<HashMap<String, NamedAgentSpec>>,
        parent_run_id: Option<String>,
        parent_step_id: Option<String>,
        depth: u32,
        cancellation: CancellationToken,
        ledger: Arc<Mutex<WorkflowBudgetUsage>>,
        root_limits: WorkflowBudgets,
        semaphore: Arc<Semaphore>,
        started: Option<StartSignal>,
        root_run_id: Option<String>,
    ) -> Result<WorkflowRunSnapshot, WorkflowRunError> {
        if request.definition.steps.len() > self.ceilings.max_steps as usize {
            return Err(WorkflowRunError::Preflight(
                "workflow definition exceeds server step-count ceiling".to_string(),
            ));
        }
        let compiled = Arc::new(CompiledWorkflow::compile(request.definition)?);
        self.enforce_ceilings(&compiled.definition.budgets)?;
        let definition_value = serde_json::to_value(&compiled.definition)
            .map_err(|error| WorkflowRunError::Preflight(error.to_string()))?;
        reject_secret_material_in_definition(&definition_value)
            .map_err(WorkflowRunError::Preflight)?;
        compiled
            .validate_input(&request.args)
            .map_err(WorkflowRunError::InvalidInput)?;
        reject_secret_material(&request.args).map_err(WorkflowRunError::InvalidInput)?;
        let allowed_capabilities = request
            .allowed_capabilities
            .into_iter()
            .collect::<BTreeSet<_>>();
        enforce_budget_within(&compiled.definition.budgets, &root_limits).map_err(|message| {
            WorkflowRunError::Preflight(format!("nested workflow budget expands root: {message}"))
        })?;

        let run_id = Uuid::new_v4().to_string();
        let root_run_id = root_run_id.unwrap_or_else(|| run_id.clone());
        let now = Utc::now();
        let snapshot = WorkflowRunSnapshot {
            run_id: run_id.clone(),
            parent_run_id,
            parent_step_id,
            session_id: request.session_id,
            definition: compiled.definition.clone(),
            definition_bundle: bundle.as_ref().clone(),
            definition_bundle_hash: definition_bundle_hash(&bundle)?,
            validated_args: request.args,
            status: WorkflowRunStatus::Queued,
            steps: BTreeMap::new(),
            usage: WorkflowBudgetUsage::default(),
            last_sequence: 1,
            output: None,
            failure: None,
            suspension: None,
            created_at: now,
            updated_at: now,
        };
        let queued = event(&snapshot, None, WorkflowRunEventKind::RunQueued);
        self.repository
            .create(&snapshot, &queued)
            .await
            .map_err(storage)?;
        let (sender, _) = broadcast::channel(256);
        self.events.insert(run_id.clone(), sender);
        self.publish(&queued);
        let snapshot = Arc::new(Mutex::new(snapshot));
        let drain_lock = Arc::new(Mutex::new(()));
        self.active.insert(
            run_id.clone(),
            Arc::new(ActiveRun {
                cancellation: cancellation.clone(),
                snapshot: snapshot.clone(),
                root_run_id: root_run_id.clone(),
                ledger: ledger.clone(),
                drain_lock: drain_lock.clone(),
                cleanup_pending: AtomicBool::new(false),
                has_agents: !pinned_agents.is_empty(),
            }),
        );
        let _registration = RuntimeRegistration {
            engine: Arc::downgrade(self),
            run_id: run_id.clone(),
        };
        {
            let mut shared = snapshot.lock().await;
            let start_result = if cancellation.is_cancelled() {
                self.finish_cancelled(&mut shared).await
            } else {
                self.transition(
                    &mut shared,
                    None,
                    WorkflowRunEventKind::RunStarted,
                    |snapshot| {
                        snapshot.status = WorkflowRunStatus::Running;
                    },
                )
                .await
            };
            start_result?;
            if let Some(started) = started {
                if let Some(sender) = started.lock().await.take() {
                    let _ = sender.send(Ok(shared.clone()));
                }
            }
        }
        let context = RunContext {
            engine: self.clone(),
            compiled: compiled.clone(),
            bundle,
            pinned_agents,
            snapshot: snapshot.clone(),
            cancellation: cancellation.clone(),
            branch_cancellation: cancellation.child_token(),
            allowed_capabilities,
            workspace_trusted: request.workspace_trusted,
            semaphore,
            items: HashMap::new(),
            scope: "root".to_string(),
            depth,
            ledger,
            root_limits,
            root_run_id: root_run_id.clone(),
            drain_lock,
        };
        self.execute_context(context, compiled.definition.budgets.wall_time_ms, 0)
            .await
    }

    async fn execute_context(
        self: &Arc<Self>,
        context: RunContext,
        timeout_ms: u64,
        completed_prefix: usize,
    ) -> Result<WorkflowRunSnapshot, WorkflowRunError> {
        let compiled = &context.compiled;
        let snapshot = &context.snapshot;
        let cancellation = &context.cancellation;
        let run_id = snapshot.lock().await.run_id.clone();
        let result = tokio::time::timeout(
            Duration::from_millis(timeout_ms),
            context.execute_root(completed_prefix),
        )
        .await;
        if result.is_err() {
            cancellation.cancel();
        }
        let _drain_guard = context.drain_lock.lock().await;
        {
            let snapshot = snapshot.lock().await;
            if snapshot.status.is_terminal() {
                return Ok(snapshot.clone());
            }
        }
        if result.is_err() {
            if let Some(durable) = self.repository.load(&run_id).await.map_err(storage)? {
                *snapshot.lock().await = durable;
            }
        }
        let (usage_unavailable, drain_error) = context.drain_agents().await;
        // A node can be dropped after its ledger handoff but before checkpoint.
        if context.ledger.lock().await.agents > 0 {
            context
                .checkpoint_agent_usage("AgentDrained")
                .await
                .map_err(|e| WorkflowRunError::Storage(e.message))?;
        }
        if drain_error.is_some() {
            if let Some(active) = self.active.get(&run_id) {
                active.cleanup_pending.store(true, Ordering::Release);
            }
            return Err(WorkflowRunError::Preflight(
                "Workflow children have not confirmed stop; cancellation can be retried".into(),
            ));
        }
        let mut final_snapshot = snapshot.lock().await;
        if usage_unavailable && !final_snapshot.status.is_terminal() {
            self.finish_failed(&mut final_snapshot, failure(WorkflowFailureCode::ExecutionFailed,
                "workflow Agent stopped but cumulative token observation unavailable; usage is not measured", false)).await?;
            return Ok(final_snapshot.clone());
        }
        if final_snapshot.status.is_terminal() {
            return Ok(final_snapshot.clone());
        }
        match result {
            Ok(Ok(output)) if cancellation.is_cancelled() => {
                let _ = output;
                self.finish_cancelled(&mut final_snapshot).await?;
            }
            Ok(Ok(output)) => {
                if let Some(schema) = &compiled.definition.output_schema {
                    if let Err(message) = validate_schema(schema, &output) {
                        let failure = failure(WorkflowFailureCode::InvalidOutput, message, false);
                        self.finish_failed(&mut final_snapshot, failure).await?;
                    } else {
                        self.finish_succeeded(&mut final_snapshot, output).await?;
                    }
                } else {
                    self.finish_succeeded(&mut final_snapshot, output).await?;
                }
            }
            Ok(Err(error)) if error.code == WorkflowFailureCode::Cancelled => {
                self.finish_cancelled(&mut final_snapshot).await?;
            }
            Ok(Err(error)) if error.code == WorkflowFailureCode::Suspended => {
                self.finish_suspended(&mut final_snapshot, error.message)
                    .await?;
            }
            Ok(Err(error)) => self.finish_failed(&mut final_snapshot, error).await?,
            Err(_) => {
                cancellation.cancel();
                // `timeout` cancels the node future at an arbitrary await,
                // including a repository commit. Reconcile the in-memory copy
                // from the journal/temp recovery protocol before allocating the
                // next sequence, so a partially committed StepStarted cannot
                // make the timeout terminal transition skip a sequence.
                if let Some(durable) = self.repository.load(&run_id).await.map_err(storage)? {
                    *final_snapshot = durable;
                }
                let step_failure = failure(
                    WorkflowFailureCode::BudgetExceeded,
                    "workflow wall-time budget exceeded",
                    false,
                );
                self.fail_timeout_frontier(
                    &mut final_snapshot,
                    &compiled.definition.plan,
                    step_failure.clone(),
                )
                .await?;
                self.fail_active_steps(&mut final_snapshot, step_failure.clone())
                    .await?;
                self.finish_failed(&mut final_snapshot, step_failure)
                    .await?;
            }
        }
        Ok(final_snapshot.clone())
    }

    pub async fn progress(
        &self,
        run_id: &str,
        since: u64,
    ) -> Result<WorkflowProgress, WorkflowRunError> {
        let snapshot = self
            .repository
            .load(run_id)
            .await
            .map_err(storage)?
            .ok_or(WorkflowRunError::NotFound)?;
        let events = self
            .repository
            .events_since(run_id, since)
            .await
            .map_err(storage)?;
        Ok(WorkflowProgress { snapshot, events })
    }

    pub async fn list_run_ids(&self) -> Result<Vec<String>, WorkflowRunError> {
        self.repository.list_run_ids().await.map_err(storage)
    }

    /// Whether the in-process worker for `run_id` is still executing.
    ///
    /// A terminal durable snapshot can become visible just before the worker's
    /// final registration guard is released. Shutdown/restart coordination can
    /// use this boundary to avoid opening a second repository owner while the
    /// old worker is still finishing its journal commit.
    pub fn is_run_active(&self, run_id: &str) -> bool {
        self.active.contains_key(run_id)
    }

    #[cfg(test)]
    pub(super) fn test_active_ledger(
        &self,
        run_id: &str,
    ) -> (Arc<Mutex<WorkflowBudgetUsage>>, CancellationToken) {
        let active = self.active.get(run_id).unwrap();
        (active.ledger.clone(), active.cancellation.clone())
    }

    pub async fn cancel(
        self: &Arc<Self>,
        run_id: &str,
    ) -> Result<WorkflowRunSnapshot, WorkflowRunError> {
        let snapshot = self
            .repository
            .load(run_id)
            .await
            .map_err(storage)?
            .ok_or(WorkflowRunError::NotFound)?;
        if snapshot.status == WorkflowRunStatus::Cancelled {
            return Ok(snapshot);
        }
        if snapshot.status.is_terminal() {
            return Err(WorkflowRunError::Terminal);
        }
        // Inactive cancellation must own the same entry as continuation.
        // Otherwise it can commit a terminal outcome after a new worker has
        // registered without cancelling that worker's token.
        let (active, _registration) = match self.active.entry(run_id.to_string()) {
            Entry::Occupied(entry) => (entry.get().clone(), None),
            Entry::Vacant(entry) => {
                let active = Arc::new(ActiveRun {
                    cancellation: CancellationToken::new(),
                    snapshot: Arc::new(Mutex::new(snapshot)),
                    root_run_id: run_id.to_string(),
                    ledger: Arc::new(Mutex::new(WorkflowBudgetUsage::default())),
                    drain_lock: Arc::new(Mutex::new(())),
                    cleanup_pending: AtomicBool::new(false),
                    has_agents: false,
                });
                entry.insert(active.clone());
                (
                    active,
                    Some(RuntimeRegistration {
                        engine: Arc::downgrade(self),
                        run_id: run_id.to_string(),
                    }),
                )
            }
        };
        {
            active.cancellation.cancel();
            // Tool-only runs keep their existing cancellation/repository path.
            if !active.has_agents {
                let mut shared = active.snapshot.lock().await;
                if !shared.status.is_terminal() {
                    self.finish_cancelled(&mut shared).await?;
                }
                return Ok(shared.clone());
            }
            let _drain_guard = active.drain_lock.lock().await;
            {
                let snapshot = active.snapshot.lock().await;
                if snapshot.status.is_terminal() {
                    return Ok(snapshot.clone());
                }
            }
            let mut usage_unavailable = false;
            let mut ledger = active.ledger.lock().await;
            let (drained, drain_error) = self.agents.drain_cancelled(&active.root_run_id).await;
            let usage = {
                let usage = &mut *ledger;
                for result in drained {
                    match result {
                        Ok(result) => {
                            usage.tokens = usage.tokens.saturating_add(result.tokens);
                            usage.cost_micros = usage
                                .cost_micros
                                .zip(result.cost_micros)
                                .map(|(a, b)| a.saturating_add(b));
                        }
                        Err(_) => {
                            usage_unavailable = true;
                            usage.cost_micros = None;
                        }
                    }
                }
                usage.clone()
            };
            drop(ledger);
            {
                let mut shared = active.snapshot.lock().await;
                self.transition(
                    &mut shared,
                    None,
                    WorkflowRunEventKind::Phase {
                        name: "AgentDrained".into(),
                    },
                    |snapshot| {
                        snapshot.usage.tokens = usage.tokens;
                        snapshot.usage.cost_micros = usage.cost_micros;
                    },
                )
                .await?;
            }
            if drain_error.is_some() {
                active.cleanup_pending.store(true, Ordering::Release);
                return Err(WorkflowRunError::Preflight(
                    "Workflow children have not confirmed stop; cancellation can be retried".into(),
                ));
            }
            let mut shared = active.snapshot.lock().await;
            if !shared.status.is_terminal() {
                if usage_unavailable {
                    self.finish_failed(&mut shared, failure(WorkflowFailureCode::ExecutionFailed,
                        "workflow Agent stopped but cumulative token observation unavailable; usage is not measured", false)).await?;
                } else {
                    self.finish_cancelled(&mut shared).await?;
                }
            }
            let result = shared.clone();
            if active.cleanup_pending.swap(false, Ordering::AcqRel) {
                self.active.remove(run_id);
                self.events.remove(run_id);
            }
            return Ok(result);
        }
    }

    pub async fn recover(&self) -> Result<Vec<WorkflowRunSnapshot>, WorkflowRunError> {
        let mut recovered = Vec::new();
        for run_id in self.repository.list_run_ids().await.map_err(storage)? {
            let Some(mut snapshot) = self.repository.load(&run_id).await.map_err(storage)? else {
                continue;
            };
            if matches!(
                snapshot.status,
                WorkflowRunStatus::Queued | WorkflowRunStatus::Running
            ) {
                let reason = "process restarted; explicit safe restart is required".to_string();
                let active_steps = snapshot
                    .steps
                    .iter()
                    .filter(|(_, step)| {
                        matches!(
                            step.status,
                            WorkflowStepStatus::Queued | WorkflowStepStatus::Running
                        )
                    })
                    .map(|(id, _)| id.clone())
                    .collect::<Vec<_>>();
                for step_id in active_steps {
                    let state_id = step_id.clone();
                    let step_reason = reason.clone();
                    self.transition(
                        &mut snapshot,
                        Some(step_id),
                        WorkflowRunEventKind::StepSuspended {
                            reason: reason.clone(),
                        },
                        move |snapshot| {
                            if let Some(step) = snapshot.steps.get_mut(&state_id) {
                                step.status = WorkflowStepStatus::Suspended;
                                step.failure = Some(failure(
                                    WorkflowFailureCode::RecoverySuspended,
                                    step_reason,
                                    true,
                                ));
                            }
                        },
                    )
                    .await?;
                }
                self.transition(
                    &mut snapshot,
                    None,
                    WorkflowRunEventKind::RunSuspended {
                        reason: reason.clone(),
                    },
                    move |snapshot| {
                        snapshot.status = WorkflowRunStatus::Suspended;
                        snapshot.suspension = Some(WorkflowSuspensionContext::Recovery {
                            reason: reason.clone(),
                        });
                    },
                )
                .await?;
                recovered.push(snapshot);
            }
        }
        Ok(recovered)
    }

    pub fn subscribe(&self, run_id: &str) -> Option<broadcast::Receiver<WorkflowRunEvent>> {
        self.events.get(run_id).map(|sender| sender.subscribe())
    }

    #[cfg(test)]
    pub(crate) fn runtime_resource_counts(&self) -> (usize, usize) {
        (self.active.len(), self.events.len())
    }

    async fn pin_and_validate_bundle(
        &self,
        root: &WorkflowRunDefinition,
    ) -> Result<WorkflowDefinitionBundle, WorkflowRunError> {
        let bundle =
            self.definitions.pin_bundle(root).await.map_err(|_| {
                WorkflowRunError::Preflight("workflow bundle pin failed".to_string())
            })?;
        self.validate_bundle(root, &bundle)?;
        Ok(bundle)
    }

    fn validate_bundle(
        &self,
        root: &WorkflowRunDefinition,
        bundle: &WorkflowDefinitionBundle,
    ) -> Result<(), WorkflowRunError> {
        if bundle.root_id != root.id
            || bundle.root_revision != root.revision
            || bundle.root() != Some(root)
        {
            return Err(WorkflowRunError::Preflight(
                "pinned bundle root identity/content mismatch".to_string(),
            ));
        }
        let serialized = serde_json::to_value(bundle).map_err(|_| {
            WorkflowRunError::Preflight("workflow bundle is not serializable".into())
        })?;
        reject_secret_material_in_definition(&serialized).map_err(WorkflowRunError::Preflight)?;
        let mut stack = vec![(root.id.clone(), root.revision, Vec::<String>::new())];
        let mut visited = BTreeSet::new();
        while let Some((id, revision, path)) = stack.pop() {
            let key = WorkflowDefinitionBundle::key(&id, revision);
            if path.contains(&key) {
                return Err(WorkflowRunError::Preflight(format!(
                    "nested workflow cycle includes {key}"
                )));
            }
            if !visited.insert(key.clone()) {
                continue;
            }
            let definition = bundle.get(&id, revision).ok_or_else(|| {
                WorkflowRunError::Preflight(format!("pinned bundle is missing {key}"))
            })?;
            if definition.id != id || definition.revision != revision {
                return Err(WorkflowRunError::Preflight(
                    "pinned bundle definition identity mismatch".to_string(),
                ));
            }
            let compiled = CompiledWorkflow::compile(definition.clone())?;
            let mut nested_path = path;
            nested_path.push(key);
            for step in compiled.steps.values() {
                if let WorkflowStepKind::Workflow {
                    workflow_id,
                    revision,
                    args,
                } = &step.kind
                {
                    let nested = bundle.get(workflow_id, *revision).ok_or_else(|| {
                        WorkflowRunError::Preflight(format!(
                            "pinned bundle is missing {workflow_id}@{revision}"
                        ))
                    })?;
                    validate_nested_input_contract(args, &nested.input_schema, &compiled)
                        .map_err(WorkflowRunError::Preflight)?;
                    stack.push((workflow_id.clone(), *revision, nested_path.clone()));
                }
            }
        }
        Ok(())
    }

    async fn preflight_bundle(
        &self,
        bundle: &WorkflowDefinitionBundle,
        session_id: &str,
        allowed: &BTreeSet<String>,
        trusted: bool,
        root_limits: &WorkflowBudgets,
    ) -> Result<HashMap<String, NamedAgentSpec>, WorkflowRunError> {
        let mut pinned_agents = HashMap::<String, NamedAgentSpec>::new();
        let mut stack = vec![(bundle.root_id.clone(), bundle.root_revision, 0_u32)];
        let mut visited = BTreeSet::new();
        while let Some((id, revision, depth)) = stack.pop() {
            if depth >= root_limits.max_nesting_depth {
                return Err(WorkflowRunError::Preflight(
                    "nested workflow depth exceeded shared root limit".to_string(),
                ));
            }
            if !visited.insert(WorkflowDefinitionBundle::key(&id, revision)) {
                continue;
            }
            let definition = bundle.get(&id, revision).ok_or_else(|| {
                WorkflowRunError::Preflight("pinned workflow definition missing".to_string())
            })?;
            enforce_budget_within(&definition.budgets, root_limits).map_err(|message| {
                WorkflowRunError::Preflight(format!(
                    "nested workflow budget expands root: {message}"
                ))
            })?;
            let compiled = CompiledWorkflow::compile(definition.clone())?;
            for step in compiled.steps.values() {
                let (target, capabilities) = match &step.kind {
                    WorkflowStepKind::Tool {
                        tool, capabilities, ..
                    } => (WorkflowPolicyTarget::Tool(tool.clone()), capabilities),
                    WorkflowStepKind::Agent {
                        agent,
                        capabilities,
                        ..
                    } => {
                        let spec = if let Some(spec) = pinned_agents.get(agent) {
                            spec.clone()
                        } else {
                            let spec = self
                                .agents
                                .resolve(agent, session_id)
                                .await
                                .map_err(|_| {
                                    WorkflowRunError::Preflight(
                                        "named agent resolution failed".to_string(),
                                    )
                                })?
                                .ok_or_else(|| {
                                    WorkflowRunError::Preflight(format!(
                                        "unknown named agent '{agent}'"
                                    ))
                                })?;
                            if spec.name != *agent {
                                return Err(WorkflowRunError::Preflight(
                                    "named agent resolver returned mismatched identity".to_string(),
                                ));
                            }
                            pinned_agents.insert(agent.clone(), spec.clone());
                            spec
                        };
                        if (root_limits.max_cost_micros.is_some()
                            || compiled.definition.budgets.max_cost_micros.is_some())
                            && !spec.cost_supported
                        {
                            return Err(WorkflowRunError::UnsupportedMonetaryBudget);
                        }
                        if !capabilities
                            .iter()
                            .all(|capability| spec.allowed_capabilities.contains(capability))
                        {
                            return Err(WorkflowRunError::Preflight(format!(
                                "agent '{agent}' capability expansion denied"
                            )));
                        }
                        (WorkflowPolicyTarget::Agent(agent.clone()), capabilities)
                    }
                    WorkflowStepKind::Workflow {
                        workflow_id,
                        revision,
                        args,
                    } => {
                        let nested = bundle.get(workflow_id, *revision).ok_or_else(|| {
                            WorkflowRunError::Preflight(format!(
                                "missing pinned workflow {workflow_id}@{revision}"
                            ))
                        })?;
                        validate_nested_input_contract(args, &nested.input_schema, &compiled)
                            .map_err(WorkflowRunError::Preflight)?;
                        stack.push((workflow_id.clone(), *revision, depth + 1));
                        (
                            WorkflowPolicyTarget::Workflow {
                                id: workflow_id.clone(),
                                revision: *revision,
                            },
                            &Vec::new(),
                        )
                    }
                };
                let requested = capabilities.iter().cloned().collect::<BTreeSet<_>>();
                if !requested.is_subset(allowed) {
                    return Err(WorkflowRunError::Preflight(format!(
                        "step '{}' exceeds root capabilities",
                        step.id
                    )));
                }
                if let PermissionDecision::Deny(_reason) = self
                    .policy
                    .authorize(session_id, &target, &requested, trusted)
                    .await
                {
                    return Err(WorkflowRunError::Preflight(
                        "workflow policy denied this step".to_string(),
                    ));
                }
            }
        }
        Ok(pinned_agents)
    }

    fn enforce_ceilings(&self, budget: &WorkflowBudgets) -> Result<(), WorkflowRunError> {
        if budget.max_concurrency > self.ceilings.max_concurrency
            || budget.max_agents > self.ceilings.max_agents
            || budget.max_steps > self.ceilings.max_steps
            || budget.max_retries > self.ceilings.max_retries
            || budget.max_nesting_depth > self.ceilings.max_nesting_depth
            || budget.wall_time_ms > self.ceilings.wall_time_ms
            || exceeds_optional(budget.max_tokens, self.ceilings.max_tokens)
            || exceeds_optional(budget.max_cost_micros, self.ceilings.max_cost_micros)
        {
            return Err(WorkflowRunError::Preflight(
                "definition exceeds server workflow budget ceilings".to_string(),
            ));
        }
        Ok(())
    }

    async fn transition(
        &self,
        snapshot: &mut WorkflowRunSnapshot,
        step_id: Option<String>,
        kind: WorkflowRunEventKind,
        mutate: impl FnOnce(&mut WorkflowRunSnapshot),
    ) -> Result<(), WorkflowRunError> {
        // Always derive a candidate from durable state. The commit itself runs
        // in an owned task, so dropping this caller at a timeout/cancel boundary
        // cannot interrupt rename/fsync halfway through. In-memory state advances
        // only after that task confirms the durable commit.
        let mut candidate = self
            .repository
            .load(&snapshot.run_id)
            .await
            .map_err(storage)?
            .ok_or_else(|| WorkflowRunError::Storage("workflow snapshot missing".to_string()))?;
        mutate(&mut candidate);
        candidate.last_sequence += 1;
        candidate.updated_at = Utc::now();
        let event = event(&candidate, step_id, kind);
        let repository = self.repository.clone();
        let durable_candidate = candidate.clone();
        let durable_event = event.clone();
        let commit =
            tokio::spawn(
                async move { repository.commit(&durable_candidate, &durable_event).await },
            );
        commit
            .await
            .map_err(|error| {
                WorkflowRunError::Storage(format!("workflow commit task failed: {error}"))
            })?
            .map_err(storage)?;
        *snapshot = candidate;
        self.publish(&event);
        Ok(())
    }

    fn publish(&self, event: &WorkflowRunEvent) {
        if let Some(sender) = self.events.get(&event.run_id) {
            let _ = sender.send(event.clone());
        }
    }

    async fn finish_succeeded(
        &self,
        snapshot: &mut WorkflowRunSnapshot,
        output: Value,
    ) -> Result<(), WorkflowRunError> {
        if snapshot.status.is_terminal() {
            return Ok(());
        }
        let copy = output.clone();
        self.transition(
            snapshot,
            None,
            WorkflowRunEventKind::RunSucceeded { output },
            move |snapshot| {
                snapshot.status = WorkflowRunStatus::Succeeded;
                snapshot.output = Some(copy);
            },
        )
        .await
    }
    async fn finish_failed(
        &self,
        snapshot: &mut WorkflowRunSnapshot,
        error: WorkflowFailure,
    ) -> Result<(), WorkflowRunError> {
        if snapshot.status.is_terminal() {
            return Ok(());
        }
        let copy = error.clone();
        self.transition(
            snapshot,
            None,
            WorkflowRunEventKind::RunFailed { failure: error },
            move |snapshot| {
                snapshot.status = WorkflowRunStatus::Failed;
                snapshot.failure = Some(copy);
            },
        )
        .await
    }
    async fn finish_cancelled(
        &self,
        snapshot: &mut WorkflowRunSnapshot,
    ) -> Result<(), WorkflowRunError> {
        if snapshot.status == WorkflowRunStatus::Cancelled {
            return Ok(());
        }
        let active_steps = snapshot
            .steps
            .iter()
            .filter(|(_, step)| {
                matches!(
                    step.status,
                    WorkflowStepStatus::Queued | WorkflowStepStatus::Running
                )
            })
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        for step_id in active_steps {
            let state_id = step_id.clone();
            self.transition(
                snapshot,
                Some(step_id),
                WorkflowRunEventKind::StepCancelled,
                move |snapshot| {
                    if let Some(step) = snapshot.steps.get_mut(&state_id) {
                        step.status = WorkflowStepStatus::Cancelled;
                        step.failure = Some(failure(
                            WorkflowFailureCode::Cancelled,
                            "workflow cancelled",
                            false,
                        ));
                    }
                },
            )
            .await?;
        }
        self.transition(
            snapshot,
            None,
            WorkflowRunEventKind::RunCancelled,
            |snapshot| {
                snapshot.status = WorkflowRunStatus::Cancelled;
                snapshot.failure = Some(failure(
                    WorkflowFailureCode::Cancelled,
                    "workflow cancelled",
                    false,
                ));
            },
        )
        .await
    }

    async fn finish_suspended(
        &self,
        snapshot: &mut WorkflowRunSnapshot,
        reason: String,
    ) -> Result<(), WorkflowRunError> {
        if snapshot.status.is_terminal() {
            return Ok(());
        }
        self.transition(
            snapshot,
            None,
            WorkflowRunEventKind::RunSuspended { reason },
            |snapshot| {
                snapshot.status = WorkflowRunStatus::Suspended;
            },
        )
        .await
    }

    async fn fail_active_steps(
        &self,
        snapshot: &mut WorkflowRunSnapshot,
        error: WorkflowFailure,
    ) -> Result<(), WorkflowRunError> {
        let active_steps = snapshot
            .steps
            .iter()
            .filter(|(_, step)| {
                matches!(
                    step.status,
                    WorkflowStepStatus::Queued | WorkflowStepStatus::Running
                )
            })
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        for step_id in active_steps {
            let state_id = step_id.clone();
            let copy = error.clone();
            self.transition(
                snapshot,
                Some(step_id),
                WorkflowRunEventKind::StepFailed {
                    failure: error.clone(),
                },
                move |snapshot| {
                    if let Some(step) = snapshot.steps.get_mut(&state_id) {
                        step.status = WorkflowStepStatus::Failed;
                        step.failure = Some(copy);
                    }
                },
            )
            .await?;
        }
        Ok(())
    }

    async fn fail_timeout_frontier(
        &self,
        snapshot: &mut WorkflowRunSnapshot,
        plan: &WorkflowPlan,
        error: WorkflowFailure,
    ) -> Result<(), WorkflowRunError> {
        if snapshot.steps.values().any(|step| {
            matches!(
                step.status,
                WorkflowStepStatus::Queued | WorkflowStepStatus::Running
            )
        }) {
            return Ok(());
        }
        for step_id in plan_frontier(plan) {
            if snapshot.steps.contains_key(&step_id) {
                continue;
            }
            let state_id = step_id.clone();
            let state_error = error.clone();
            self.transition(
                snapshot,
                Some(step_id),
                WorkflowRunEventKind::StepFailed {
                    failure: error.clone(),
                },
                move |snapshot| {
                    snapshot.steps.insert(
                        state_id.clone(),
                        WorkflowStepSnapshot {
                            id: state_id,
                            status: WorkflowStepStatus::Failed,
                            input_hash: String::new(),
                            output: None,
                            failure: Some(state_error),
                            attempts: 0,
                        },
                    );
                },
            )
            .await?;
        }
        Ok(())
    }
}

impl RunContext {
    async fn execute_root(&self, completed_prefix: usize) -> Result<Value, WorkflowFailure> {
        if completed_prefix == 0 {
            return self
                .execute_node(&self.compiled.definition.plan, "root")
                .await;
        }
        let WorkflowPlan::Sequence { nodes } = &self.compiled.definition.plan else {
            unreachable!("validated flat sequence");
        };
        let WorkflowPlan::Step { step } = &nodes[completed_prefix - 1] else {
            unreachable!("validated tool leaf");
        };
        let mut result = self.snapshot.lock().await.steps[step]
            .output
            .clone()
            .expect("validated completed output");
        for (index, node) in nodes.iter().enumerate().skip(completed_prefix) {
            result = self.execute_node(node, &format!("root.{index}")).await?;
        }
        Ok(result)
    }

    fn execute_node<'a>(&'a self, plan: &'a WorkflowPlan, path: &'a str) -> NodeFuture<'a> {
        Box::pin(async move {
            self.check_cancelled()?;
            match plan {
                WorkflowPlan::Step { step } => self.execute_step(step, path).await,
                WorkflowPlan::Sequence { nodes } => {
                    let mut result = Value::Null;
                    for (index, node) in nodes.iter().enumerate() {
                        match self.execute_node(node, &format!("{path}.{index}")).await {
                            Ok(value) => result = value,
                            Err(error) => {
                                if error.code == WorkflowFailureCode::DependencySkipped {
                                    for remaining in &nodes[index + 1..] {
                                        self.skip_plan(
                                            remaining,
                                            "dependency requested skip_dependents",
                                        )
                                        .await?;
                                    }
                                }
                                return Err(error);
                            }
                        }
                    }
                    Ok(result)
                }
                WorkflowPlan::Parallel { nodes } => {
                    let parallel_cancellation = self.branch_cancellation.child_token();
                    let mut futures = FuturesUnordered::new();
                    for (index, node) in nodes.iter().enumerate() {
                        let mut child = self.clone();
                        child.branch_cancellation = parallel_cancellation.clone();
                        futures.push(async move {
                            (
                                index,
                                child.execute_node(node, &format!("{path}.{index}")).await,
                            )
                        });
                    }
                    let mut output = vec![Value::Null; nodes.len()];
                    while let Some((index, result)) = futures.next().await {
                        match result {
                            Ok(value) => output[index] = value,
                            Err(mut error) => {
                                parallel_cancellation.cancel();
                                // Drop sibling futures before awaiting durable
                                // cancellation transitions. A sibling may hold
                                // the snapshot mutex across its shielded commit;
                                // leaving it parked inside FuturesUnordered would
                                // deadlock this reconciliation.
                                drop(futures);
                                if !self.pinned_agents.is_empty() {
                                    let _drain_guard = self.drain_lock.lock().await;
                                    let (usage_unavailable, drain_error) =
                                        self.drain_agents().await;
                                    self.checkpoint_agent_usage("AgentDrained").await?;
                                    if usage_unavailable {
                                        return Err(failure(WorkflowFailureCode::ExecutionFailed, "workflow Agent cumulative token observation unavailable; usage is not measured", false));
                                    }
                                    if drain_error.is_some() {
                                        return Err(failure(
                                            WorkflowFailureCode::ExecutionFailed,
                                            "Workflow children have not confirmed stop",
                                            false,
                                        ));
                                    }
                                }
                                self.cancel_active_parallel_steps(nodes).await?;
                                error.message =
                                    format!("parallel branch[{index}] failed: {}", error.message);
                                return Err(error);
                            }
                        }
                    }
                    Ok(Value::Array(output))
                }
                WorkflowPlan::Choice {
                    condition,
                    then_branch,
                    else_branch,
                } => {
                    let condition = self.resolve_ref(condition).await?;
                    let selected = condition.as_bool().ok_or_else(|| {
                        failure(
                            WorkflowFailureCode::InvalidInput,
                            "choice condition must be a boolean",
                            false,
                        )
                    })?;
                    let (chosen, unchosen, branch) = if selected {
                        (then_branch, else_branch, "then")
                    } else {
                        (else_branch, then_branch, "else")
                    };
                    self.skip_plan(unchosen, "conditional branch not selected")
                        .await?;
                    self.execute_node(chosen, &format!("{path}.{branch}")).await
                }
                WorkflowPlan::Map { source, item, body } => {
                    let source = self.resolve_ref(source).await?;
                    let values = source.as_array().ok_or_else(|| {
                        failure(
                            WorkflowFailureCode::InvalidInput,
                            "map source must be an array",
                            false,
                        )
                    })?;
                    let used = self.ledger.lock().await.steps as usize;
                    let remaining = (self.root_limits.max_steps as usize).saturating_sub(used);
                    let per_item = plan_leaf_count(body).max(1);
                    if values
                        .len()
                        .checked_mul(per_item)
                        .is_none_or(|required| required > remaining)
                    {
                        return Err(failure(
                            WorkflowFailureCode::BudgetExceeded,
                            "map cardinality exceeds remaining workflow step budget",
                            false,
                        ));
                    }
                    let futures = values.iter().cloned().enumerate().map(|(index, value)| {
                        let mut child = self.clone();
                        child.items.insert(item.clone(), value);
                        // Scope identifies the logical map item, not a retry
                        // attempt's diagnostic path. This keeps durable attempts
                        // cumulative when Retry wraps Map and gives nested
                        // Parallel invocations an item-local cancellation domain.
                        child.scope = format!("{}[{index}]", self.scope);
                        async move { child.execute_node(body, &format!("{path}[{index}]")).await }
                    });
                    let results = join_all(futures).await;
                    let mut values = Vec::with_capacity(results.len());
                    let mut failures = Vec::new();
                    for (index, result) in results.into_iter().enumerate() {
                        match result {
                            Ok(value) => values.push(value),
                            Err(error) => failures.push((index, error)),
                        }
                    }
                    if failures.is_empty() {
                        Ok(Value::Array(values))
                    } else {
                        let retryable = failures.iter().any(|(_, error)| error.retryable);
                        let first_code = failures[0].1.code;
                        let code = if failures
                            .iter()
                            .any(|(_, error)| error.code == WorkflowFailureCode::DependencySkipped)
                        {
                            WorkflowFailureCode::DependencySkipped
                        } else if failures.iter().all(|(_, error)| error.code == first_code) {
                            first_code
                        } else {
                            WorkflowFailureCode::ExecutionFailed
                        };
                        let diagnostics = failures
                            .into_iter()
                            .map(|(index, error)| format!("item[{index}]: {}", error.message))
                            .collect::<Vec<_>>()
                            .join("; ");
                        Err(failure(
                            code,
                            format!("map items failed: {diagnostics}"),
                            retryable,
                        ))
                    }
                }
                WorkflowPlan::Retry {
                    node,
                    max_attempts,
                    delay_ms,
                } => {
                    let limit =
                        (*max_attempts).min(self.compiled.definition.budgets.max_retries + 1);
                    let mut last = None;
                    for attempt in 0..limit {
                        match self
                            .execute_node(node, &format!("{path}.retry{attempt}"))
                            .await
                        {
                            Ok(value) => return Ok(value),
                            Err(error) if error.retryable => {
                                last = Some(error);
                                if attempt + 1 < limit {
                                    self.reserve_retry().await?;
                                    self.checkpoint_usage("retry_reserved").await?;
                                    tokio::select! {
                                        _ = self.cancellation.cancelled() => return Err(failure(WorkflowFailureCode::Cancelled, "workflow cancelled", false)),
                                        _ = self.branch_cancellation.cancelled() => return Err(failure(WorkflowFailureCode::Cancelled, "workflow branch cancelled", false)),
                                        _ = tokio::time::sleep(Duration::from_millis(*delay_ms)) => {}
                                    }
                                } else {
                                    break;
                                }
                            }
                            Err(error) => return Err(error),
                        }
                    }
                    Err(failure(
                        WorkflowFailureCode::RetryExhausted,
                        last.map_or_else(|| "retry exhausted".to_string(), |error| error.message),
                        false,
                    ))
                }
            }
        })
    }

    async fn execute_step(&self, step_id: &str, _path: &str) -> Result<Value, WorkflowFailure> {
        self.check_cancelled()?;
        let step = self.compiled.steps.get(step_id).cloned().ok_or_else(|| {
            failure(
                WorkflowFailureCode::UnknownReference,
                format!("unknown step {step_id}"),
                false,
            )
        })?;
        let instance_id = if self.scope == "root" {
            step_id.to_string()
        } else {
            format!("{step_id}@{}", self.scope)
        };
        let input = match &step.kind {
            WorkflowStepKind::Tool { args, .. } | WorkflowStepKind::Workflow { args, .. } => {
                self.resolve_template(args).await?
            }
            WorkflowStepKind::Agent { prompt, .. } => self.resolve_template(prompt).await?,
        };
        let input_hash = hex::encode(Sha256::digest(
            serde_json::to_vec(&input).unwrap_or_default(),
        ));
        self.reserve_step().await?;
        self.checkpoint_usage("step_reserved").await?;
        self.step_transition(&instance_id, WorkflowRunEventKind::StepQueued, |snapshot| {
            let state = snapshot
                .steps
                .entry(instance_id.clone())
                .or_insert_with(|| WorkflowStepSnapshot {
                    id: instance_id.clone(),
                    status: WorkflowStepStatus::Queued,
                    input_hash: input_hash.clone(),
                    output: None,
                    failure: None,
                    attempts: 0,
                });
            state.status = WorkflowStepStatus::Queued;
            state.input_hash = input_hash;
            state.output = None;
            state.failure = None;
        })
        .await?;
        let _permit = if matches!(&step.kind, WorkflowStepKind::Workflow { .. }) {
            None
        } else {
            Some(tokio::select! {
                _ = self.cancellation.cancelled() => {
                    let cancelled_id = instance_id.clone();
                    self.step_transition(&instance_id, WorkflowRunEventKind::StepCancelled, move |snapshot| {
                        if let Some(state) = snapshot.steps.get_mut(&cancelled_id) { state.status = WorkflowStepStatus::Cancelled; }
                    }).await?;
                    return Err(failure(WorkflowFailureCode::Cancelled, "workflow cancelled", false));
                }
                _ = self.branch_cancellation.cancelled() => {
                    let cancelled_id = instance_id.clone();
                    self.step_transition(&instance_id, WorkflowRunEventKind::StepCancelled, move |snapshot| {
                        if let Some(state) = snapshot.steps.get_mut(&cancelled_id) {
                            state.status = WorkflowStepStatus::Cancelled;
                        }
                    }).await?;
                    return Err(failure(WorkflowFailureCode::Cancelled, "workflow branch cancelled", false));
                }
                permit = self.semaphore.acquire() => permit.map_err(|_| failure(WorkflowFailureCode::ExecutionFailed, "workflow semaphore closed", false))?,
            })
        };
        let started_id = instance_id.clone();
        self.step_transition(
            &instance_id,
            WorkflowRunEventKind::StepStarted,
            move |snapshot| {
                if let Some(state) = snapshot.steps.get_mut(&started_id) {
                    state.status = WorkflowStepStatus::Running;
                    state.attempts += 1;
                }
            },
        )
        .await?;
        let result = self.dispatch(&step, input, &instance_id).await;
        let result = match result {
            Ok(output) => {
                if let Some(schema) = &step.output_schema {
                    validate_schema(schema, &output)
                        .map(|()| output)
                        .map_err(|message| {
                            failure(WorkflowFailureCode::InvalidOutput, message, false)
                        })
                } else {
                    Ok(output)
                }
            }
            Err(error) => Err(error),
        };
        let result = result.and_then(|output| {
            reject_secret_material(&output)
                .map(|()| output)
                .map_err(|message| failure(WorkflowFailureCode::InvalidOutput, message, false))
        });
        match result {
            Ok(output) => {
                let copy = output.clone();
                let completed_id = instance_id.clone();
                self.step_transition(
                    &instance_id,
                    WorkflowRunEventKind::StepCompleted {
                        output: output.clone(),
                    },
                    move |snapshot| {
                        if let Some(state) = snapshot.steps.get_mut(&completed_id) {
                            state.status = WorkflowStepStatus::Succeeded;
                            state.output = Some(copy);
                        }
                    },
                )
                .await?;
                Ok(output)
            }
            Err(error) => {
                if error.code == WorkflowFailureCode::Cancelled {
                    let cancelled_id = instance_id.clone();
                    self.step_transition(
                        &instance_id,
                        WorkflowRunEventKind::StepCancelled,
                        move |snapshot| {
                            if let Some(state) = snapshot.steps.get_mut(&cancelled_id) {
                                state.status = WorkflowStepStatus::Cancelled;
                                state.failure = Some(failure(
                                    WorkflowFailureCode::Cancelled,
                                    "workflow branch cancelled",
                                    false,
                                ));
                            }
                        },
                    )
                    .await?;
                    return Err(error);
                }
                if error.code == WorkflowFailureCode::Suspended {
                    let reason = error.message.clone();
                    let suspended_id = instance_id.clone();
                    self.step_transition(
                        &instance_id,
                        WorkflowRunEventKind::StepSuspended {
                            reason: reason.clone(),
                        },
                        move |snapshot| {
                            if let Some(state) = snapshot.steps.get_mut(&suspended_id) {
                                state.status = WorkflowStepStatus::Suspended;
                                state.failure =
                                    Some(failure(WorkflowFailureCode::Suspended, reason, true));
                            }
                        },
                    )
                    .await?;
                    return Err(error);
                }
                let copy = error.clone();
                let failed_id = instance_id.clone();
                self.step_transition(
                    &instance_id,
                    WorkflowRunEventKind::StepFailed {
                        failure: error.clone(),
                    },
                    move |snapshot| {
                        if let Some(state) = snapshot.steps.get_mut(&failed_id) {
                            state.status = WorkflowStepStatus::Failed;
                            state.failure = Some(copy);
                        }
                    },
                )
                .await?;
                match step.failure {
                    FailurePolicy::ContinueWithError => Ok(serde_json::json!({"error": error})),
                    FailurePolicy::SkipDependents => Err(failure(
                        WorkflowFailureCode::DependencySkipped,
                        format!("{} (dependents skipped)", error.message),
                        false,
                    )),
                    FailurePolicy::FailFast => Err(error),
                }
            }
        }
    }

    async fn dispatch(
        &self,
        step: &WorkflowStepDefinition,
        input: Value,
        instance_id: &str,
    ) -> Result<Value, WorkflowFailure> {
        let session_id = { self.snapshot.lock().await.session_id.clone() };
        match &step.kind {
            WorkflowStepKind::Tool {
                tool, capabilities, ..
            } => {
                self.authorize(
                    &session_id,
                    WorkflowPolicyTarget::Tool(tool.clone()),
                    capabilities,
                )
                .await?;
                let (resolved_input, resolved_secrets) =
                    self.resolve_secret_handles(&input, &session_id).await?;
                let arguments = serde_json::to_string(&resolved_input).map_err(|error| {
                    failure(WorkflowFailureCode::InvalidInput, error.to_string(), false)
                })?;
                let call = ToolCall {
                    id: format!("workflow-{}", Uuid::new_v4()),
                    tool_type: "function".to_string(),
                    function: FunctionCall {
                        name: tool.clone(),
                        arguments,
                    },
                };
                let permission_port = self
                    .engine
                    .session_permissions
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone();
                let session_flags = match permission_port {
                    Some(port) => port.flags_for_session(&session_id).await.map_err(|error| {
                        failure(
                            WorkflowFailureCode::ExecutionFailed,
                            format!("workflow session permission posture is unavailable: {error}"),
                            true,
                        )
                    })?,
                    None => ToolExecutionSessionFlags::default(),
                };
                let context = ToolExecutionContext {
                    executing_supervisor: None,
                    session_id: Some(&session_id),
                    // WorkflowRunSnapshot does not persist root-session identity.
                    // Fail closed for tool events instead of treating a child
                    // session id as authoritative root metadata.
                    root_session_id: None,
                    tool_call_id: &call.id,
                    event_tx: None,
                    available_tool_schemas: None,
                    bypass_permissions: session_flags.bypass_permissions,
                    auto_approve_permissions: session_flags.auto_approve_permissions,
                    plan_read_only: session_flags.plan_read_only,
                    can_async_resume: false,
                    bash_completion_sink: None,
                    pre_parsed_args: Some(&resolved_input),
                };
                if session_flags.plan_read_only
                    && !bamboo_tools::orchestrator::plan_mode_allows_tool(&call.function.name)
                {
                    return Err(failure(
                        WorkflowFailureCode::ExecutionFailed,
                        format!("Plan mode: {} operation blocked", call.function.name),
                        false,
                    ));
                }
                let outcome = self
                    .engine
                    .tools
                    .execute_with_context_outcome(&call, context)
                    .await
                    .map_err(|error| {
                        let (code, message, retryable) = match error {
                            bamboo_agent_core::tools::ToolError::NotFound(_) => (
                                WorkflowFailureCode::UnknownReference,
                                "workflow tool is not available",
                                false,
                            ),
                            bamboo_agent_core::tools::ToolError::InvalidArguments(_) => (
                                WorkflowFailureCode::InvalidInput,
                                "workflow tool arguments were rejected",
                                false,
                            ),
                            bamboo_agent_core::tools::ToolError::Execution(_) => (
                                WorkflowFailureCode::ExecutionFailed,
                                "workflow tool execution was denied or failed",
                                true,
                            ),
                        };
                        failure(code, message, retryable)
                    })?;
                match outcome {
                    ToolOutcome::Completed(result) => {
                        let output = parse_tool_result(result)?;
                        if contains_any_secret_material(&output, &resolved_secrets) {
                            return Err(failure(
                                WorkflowFailureCode::InvalidOutput,
                                "workflow tool output contained resolved secret material",
                                false,
                            ));
                        }
                        Ok(output)
                    }
                    ToolOutcome::NeedsHuman { question, .. } => {
                        self.persist_suspension(WorkflowSuspensionContext::ToolApproval {
                            step_id: instance_id.to_string(),
                            tool: tool.clone(),
                            tool_call_id: question.tool_call_id,
                        })
                        .await?;
                        Err(failure(
                            WorkflowFailureCode::Suspended,
                            "workflow tool requires human approval",
                            true,
                        ))
                    }
                    ToolOutcome::Running(handle) => {
                        let tool_call_id = handle.tool_call_id.clone();
                        (handle.kill)();
                        self.persist_suspension(WorkflowSuspensionContext::ToolRunning {
                            step_id: instance_id.to_string(),
                            tool: tool.clone(),
                            tool_call_id,
                            killed: true,
                        })
                        .await?;
                        Err(failure(
                            WorkflowFailureCode::Suspended,
                            "workflow tool is running without a durable workflow resume handle",
                            true,
                        ))
                    }
                }
            }
            WorkflowStepKind::Agent {
                agent,
                model,
                effort,
                capabilities,
                structured_output_attempts,
                ..
            } => {
                if contains_secret_handle(&input) {
                    return Err(failure(
                        WorkflowFailureCode::PermissionDenied,
                        "secret capability handles are supported only for tool arguments",
                        false,
                    ));
                }
                self.authorize(
                    &session_id,
                    WorkflowPolicyTarget::Agent(agent.clone()),
                    capabilities,
                )
                .await?;
                let spec = self.pinned_agents.get(agent).cloned().ok_or_else(|| {
                    failure(
                        WorkflowFailureCode::PermissionDenied,
                        "named agent was not pinned during preflight",
                        false,
                    )
                })?;
                let requested = capabilities.iter().cloned().collect::<BTreeSet<_>>();
                if !requested.is_subset(&spec.allowed_capabilities) {
                    return Err(failure(
                        WorkflowFailureCode::PermissionDenied,
                        "named agent capability intersection changed",
                        false,
                    ));
                }
                let mut last_error = None;
                for _ in 0..*structured_output_attempts {
                    self.ensure_agent_usage_budget_available().await?;
                    self.reserve_agent().await?;
                    if !spec.cost_supported {
                        self.ledger.lock().await.cost_micros = None;
                    }
                    self.checkpoint_usage("agent_reserved").await?;
                    match self
                        .engine
                        .agents
                        .execute(
                            &spec,
                            input.clone(),
                            model.as_deref(),
                            effort.as_deref(),
                            &requested,
                            &session_id,
                            &self.root_run_id,
                            self.branch_cancellation.child_token(),
                        )
                        .await
                    {
                        Ok(result) => {
                            let _drain_guard = self.drain_lock.lock().await;
                            let mut usage = self.ledger.lock().await;
                            if result
                                .attempt_id
                                .as_ref()
                                .is_some_and(|id| !self.engine.agents.acknowledge_result(id))
                            {
                                self.check_cancelled()?;
                                return Err(failure(
                                    WorkflowFailureCode::Cancelled,
                                    "workflow Agent result already drained",
                                    false,
                                ));
                            }
                            // No await between ownership transfer and accounting.
                            let exceeded =
                                self.record_usage(&mut usage, result.tokens, result.cost_micros);
                            drop(usage);
                            self.checkpoint_usage("agent_usage_recorded").await?;
                            self.check_cancelled()?;
                            if let Some(error) = result.failure {
                                return Err(error);
                            }
                            if let Some(error) = exceeded {
                                return Err(error);
                            }
                            if let Some(schema) = &step.output_schema {
                                if let Err(error) = validate_schema(schema, &result.output) {
                                    last_error = Some(error);
                                    continue;
                                }
                            }
                            return Ok(result.output);
                        }
                        Err(_error) => {
                            self.check_cancelled()?;
                            return Err(failure(
                                WorkflowFailureCode::ExecutionFailed,
                                "named agent execution or usage observation unavailable",
                                false,
                            ));
                        }
                    }
                }
                Err(failure(
                    WorkflowFailureCode::InvalidOutput,
                    last_error.unwrap_or_else(|| "agent structured output exhausted".to_string()),
                    false,
                ))
            }
            WorkflowStepKind::Workflow {
                workflow_id,
                revision,
                ..
            } => {
                if self.depth + 1 >= self.root_limits.max_nesting_depth {
                    return Err(failure(
                        WorkflowFailureCode::BudgetExceeded,
                        "nested workflow depth exceeded",
                        false,
                    ));
                }
                let definition = self
                    .bundle
                    .get(workflow_id, *revision)
                    .cloned()
                    .ok_or_else(|| {
                        failure(
                            WorkflowFailureCode::UnknownReference,
                            format!("persisted bundle missing workflow {workflow_id}@{revision}"),
                            false,
                        )
                    })?;
                let nested = StartWorkflowRun {
                    definition,
                    args: input,
                    session_id,
                    workspace_trusted: self.workspace_trusted,
                    allowed_capabilities: self.allowed_capabilities.iter().cloned().collect(),
                };
                let parent_run_id = self.snapshot.lock().await.run_id.clone();
                let result = Box::pin(self.engine.run_internal(
                    nested,
                    self.bundle.clone(),
                    self.pinned_agents.clone(),
                    Some(parent_run_id),
                    Some(instance_id.to_string()),
                    self.depth + 1,
                    self.branch_cancellation.clone(),
                    self.ledger.clone(),
                    self.root_limits.clone(),
                    self.semaphore.clone(),
                    None,
                    Some(self.root_run_id.clone()),
                ))
                .await
                .map_err(|_error| {
                    failure(
                        WorkflowFailureCode::ExecutionFailed,
                        "nested workflow execution failed",
                        false,
                    )
                })?;
                result.output.ok_or_else(|| {
                    result.failure.unwrap_or_else(|| {
                        failure(
                            WorkflowFailureCode::ExecutionFailed,
                            "nested workflow returned no output",
                            false,
                        )
                    })
                })
            }
        }
    }

    async fn authorize(
        &self,
        session_id: &str,
        target: WorkflowPolicyTarget,
        capabilities: &[String],
    ) -> Result<(), WorkflowFailure> {
        let requested = capabilities.iter().cloned().collect::<BTreeSet<_>>();
        if !requested.is_subset(&self.allowed_capabilities) {
            return Err(failure(
                WorkflowFailureCode::PermissionDenied,
                "step capability exceeds root policy",
                false,
            ));
        }
        match self
            .engine
            .policy
            .authorize(session_id, &target, &requested, self.workspace_trusted)
            .await
        {
            PermissionDecision::Allow => Ok(()),
            PermissionDecision::Deny(_reason) => Err(failure(
                if self.workspace_trusted {
                    WorkflowFailureCode::PermissionDenied
                } else {
                    WorkflowFailureCode::UntrustedWorkspace
                },
                "workflow policy denied this step",
                false,
            )),
        }
    }

    async fn step_transition(
        &self,
        step_id: &str,
        kind: WorkflowRunEventKind,
        mutate: impl FnOnce(&mut WorkflowRunSnapshot),
    ) -> Result<(), WorkflowFailure> {
        let usage = self.ledger.lock().await.clone();
        let mut snapshot = self.snapshot.lock().await;
        snapshot.usage = usage;
        self.engine
            .transition(&mut snapshot, Some(step_id.to_string()), kind, mutate)
            .await
            .map_err(|error| failure(WorkflowFailureCode::Storage, error.to_string(), false))
    }

    async fn reserve_step(&self) -> Result<(), WorkflowFailure> {
        let mut usage = self.ledger.lock().await;
        if usage.steps >= self.root_limits.max_steps {
            return Err(failure(
                WorkflowFailureCode::BudgetExceeded,
                "workflow step budget exceeded",
                false,
            ));
        }
        usage.steps += 1;
        Ok(())
    }

    async fn reserve_retry(&self) -> Result<(), WorkflowFailure> {
        let mut usage = self.ledger.lock().await;
        if usage.retries >= self.root_limits.max_retries {
            return Err(failure(
                WorkflowFailureCode::BudgetExceeded,
                "workflow retry budget exceeded",
                false,
            ));
        }
        usage.retries += 1;
        Ok(())
    }

    async fn reserve_agent(&self) -> Result<(), WorkflowFailure> {
        let mut usage = self.ledger.lock().await;
        if usage.agents >= self.root_limits.max_agents {
            return Err(failure(
                WorkflowFailureCode::BudgetExceeded,
                "workflow agent budget exceeded",
                false,
            ));
        }
        usage.agents += 1;
        Ok(())
    }

    fn record_usage(
        &self,
        usage: &mut WorkflowBudgetUsage,
        tokens: u64,
        cost_micros: Option<u64>,
    ) -> Option<WorkflowFailure> {
        let next_tokens = usage.tokens.saturating_add(tokens);
        let next_cost = usage
            .cost_micros
            .zip(cost_micros)
            .map(|(a, b)| a.saturating_add(b));
        usage.tokens = next_tokens;
        usage.cost_micros = next_cost;
        if self
            .root_limits
            .max_tokens
            .is_some_and(|limit| next_tokens > limit)
            || self
                .root_limits
                .max_cost_micros
                .is_some_and(|limit| next_cost.is_none_or(|cost| cost > limit))
        {
            return Some(failure(
                WorkflowFailureCode::BudgetExceeded,
                "workflow token/cost budget exceeded",
                false,
            ));
        }
        None
    }

    async fn drain_agents(&self) -> (bool, Option<String>) {
        // Acquire before the port transfers results; after it returns there is
        // no await until all rows are accounted. Provider execution stays parallel.
        let mut ledger = self.ledger.lock().await;
        let (results, error) = self.engine.agents.drain_cancelled(&self.root_run_id).await;
        let mut unavailable = false;
        for result in results {
            match result {
                Ok(result) => {
                    self.record_usage(&mut ledger, result.tokens, result.cost_micros);
                }
                Err(_) => {
                    unavailable = true;
                    ledger.cost_micros = None;
                }
            }
        }
        (unavailable, error)
    }

    async fn ensure_agent_usage_budget_available(&self) -> Result<(), WorkflowFailure> {
        let usage = self.ledger.lock().await;
        if self
            .root_limits
            .max_tokens
            .is_some_and(|limit| usage.tokens >= limit)
            || self
                .root_limits
                .max_cost_micros
                .is_some_and(|limit| usage.cost_micros.is_none_or(|cost| cost >= limit))
        {
            return Err(failure(
                WorkflowFailureCode::BudgetExceeded,
                "workflow token/cost budget exhausted before agent dispatch",
                false,
            ));
        }
        Ok(())
    }

    async fn checkpoint_agent_usage(&self, name: &str) -> Result<(), WorkflowFailure> {
        let usage = self.ledger.lock().await.clone();
        let mut snapshot = self.snapshot.lock().await;
        self.engine
            .transition(
                &mut snapshot,
                None,
                WorkflowRunEventKind::Phase {
                    name: name.to_string(),
                },
                move |snapshot| {
                    // Only completed Agent usage is reconciled here. Other counters
                    // retain their existing durable reservation/checkpoint semantics.
                    snapshot.usage.tokens = usage.tokens;
                    snapshot.usage.cost_micros = usage.cost_micros;
                },
            )
            .await
            .map_err(|e| failure(WorkflowFailureCode::Storage, e.to_string(), false))
    }

    async fn checkpoint_usage(&self, name: &str) -> Result<(), WorkflowFailure> {
        let usage = self.ledger.lock().await.clone();
        let mut snapshot = self.snapshot.lock().await;
        self.engine
            .transition(
                &mut snapshot,
                None,
                WorkflowRunEventKind::Phase {
                    name: name.to_string(),
                },
                move |snapshot| snapshot.usage = usage,
            )
            .await
            .map_err(|error| failure(WorkflowFailureCode::Storage, error.to_string(), false))
    }

    async fn persist_suspension(
        &self,
        context: WorkflowSuspensionContext,
    ) -> Result<(), WorkflowFailure> {
        let mut snapshot = self.snapshot.lock().await;
        self.engine
            .transition(
                &mut snapshot,
                None,
                WorkflowRunEventKind::Phase {
                    name: "suspension_context_persisted".to_string(),
                },
                move |snapshot| snapshot.suspension = Some(context),
            )
            .await
            .map_err(|error| failure(WorkflowFailureCode::Storage, error.to_string(), false))
    }

    async fn cancel_active_parallel_steps(
        &self,
        nodes: &[WorkflowPlan],
    ) -> Result<(), WorkflowFailure> {
        let sibling_steps = nodes
            .iter()
            .flat_map(plan_step_ids)
            .collect::<BTreeSet<_>>();
        let active = {
            let snapshot = self.snapshot.lock().await;
            snapshot
                .steps
                .iter()
                .filter(|(id, step)| {
                    matches!(
                        step.status,
                        WorkflowStepStatus::Queued | WorkflowStepStatus::Running
                    ) && sibling_steps
                        .iter()
                        .any(|step_id| instance_is_in_scope(id, step_id, &self.scope))
                })
                .map(|(id, _)| id.clone())
                .collect::<Vec<_>>()
        };
        for step_id in active {
            let state_id = step_id.clone();
            self.step_transition(
                &step_id,
                WorkflowRunEventKind::StepCancelled,
                move |snapshot| {
                    if let Some(step) = snapshot.steps.get_mut(&state_id) {
                        step.status = WorkflowStepStatus::Cancelled;
                        step.failure = Some(failure(
                            WorkflowFailureCode::Cancelled,
                            "parallel sibling cancelled by fail_fast",
                            false,
                        ));
                    }
                },
            )
            .await?;
        }
        Ok(())
    }

    async fn resolve_secret_handles(
        &self,
        value: &Value,
        session_id: &str,
    ) -> Result<(Value, Vec<String>), WorkflowFailure> {
        fn walk<'a>(
            context: &'a RunContext,
            value: &'a Value,
            session_id: &'a str,
        ) -> SecretResolutionFuture<'a> {
            Box::pin(async move {
                match value {
                    Value::Object(object) if object.contains_key("$secret") => {
                        let handle: bamboo_domain::WorkflowSecretHandle =
                            serde_json::from_value(value.clone()).map_err(|_| {
                                failure(
                                    WorkflowFailureCode::InvalidInput,
                                    "malformed secret capability handle",
                                    false,
                                )
                            })?;
                        let material = context
                            .engine
                            .secrets
                            .resolve(session_id, &handle.capability)
                            .await
                            .map_err(|_| {
                                failure(
                                    WorkflowFailureCode::PermissionDenied,
                                    "secret capability resolution denied",
                                    false,
                                )
                            })?;
                        let material = material.into_exposed();
                        Ok((Value::String(material.clone()), vec![material]))
                    }
                    Value::Object(object) => {
                        let mut resolved = serde_json::Map::new();
                        let mut secrets = Vec::new();
                        for (key, child) in object {
                            let (child, mut child_secrets) =
                                walk(context, child, session_id).await?;
                            resolved.insert(key.clone(), child);
                            secrets.append(&mut child_secrets);
                        }
                        Ok((Value::Object(resolved), secrets))
                    }
                    Value::Array(array) => {
                        let mut resolved = Vec::with_capacity(array.len());
                        let mut secrets = Vec::new();
                        for child in array {
                            let (child, mut child_secrets) =
                                walk(context, child, session_id).await?;
                            resolved.push(child);
                            secrets.append(&mut child_secrets);
                        }
                        Ok((Value::Array(resolved), secrets))
                    }
                    value => Ok((value.clone(), Vec::new())),
                }
            })
        }
        walk(self, value, session_id).await
    }

    fn skip_plan<'a>(&'a self, plan: &'a WorkflowPlan, reason: &'a str) -> NodeFuture<'a> {
        Box::pin(async move {
            match plan {
                WorkflowPlan::Step { step } => {
                    let instance_id = if self.scope == "root" {
                        step.clone()
                    } else {
                        format!("{step}@{}", self.scope)
                    };
                    // A previous Retry attempt may have materialized Map items
                    // in this branch. Close only instances in the current scope.
                    let mut instances = {
                        let snapshot = self.snapshot.lock().await;
                        snapshot
                            .steps
                            .keys()
                            .filter(|id| instance_is_in_scope(id, step, &self.scope))
                            .cloned()
                            .collect::<Vec<_>>()
                    };
                    if instances.is_empty() {
                        instances.push(instance_id);
                    }
                    for instance_id in instances {
                        let reason_owned = reason.to_string();
                        let state_id = instance_id.clone();
                        self.step_transition(
                            &instance_id,
                            WorkflowRunEventKind::StepSkipped {
                                reason: reason.to_string(),
                            },
                            move |snapshot| {
                                let state = snapshot.steps.entry(state_id.clone()).or_insert(
                                    WorkflowStepSnapshot {
                                        id: state_id,
                                        status: WorkflowStepStatus::Skipped,
                                        input_hash: String::new(),
                                        output: None,
                                        failure: None,
                                        attempts: 0,
                                    },
                                );
                                state.status = WorkflowStepStatus::Skipped;
                                state.output = None;
                                state.failure = Some(failure(
                                    WorkflowFailureCode::DependencySkipped,
                                    reason_owned,
                                    false,
                                ));
                            },
                        )
                        .await?;
                    }
                }
                WorkflowPlan::Sequence { nodes } | WorkflowPlan::Parallel { nodes } => {
                    for node in nodes {
                        self.skip_plan(node, reason).await?;
                    }
                }
                WorkflowPlan::Choice {
                    then_branch,
                    else_branch,
                    ..
                } => {
                    self.skip_plan(then_branch, reason).await?;
                    self.skip_plan(else_branch, reason).await?;
                }
                WorkflowPlan::Map { body, .. } | WorkflowPlan::Retry { node: body, .. } => {
                    self.skip_plan(body, reason).await?;
                }
            }
            Ok(Value::Null)
        })
    }

    async fn resolve_template(&self, value: &Value) -> Result<Value, WorkflowFailure> {
        Box::pin(self.resolve_template_inner(value)).await
    }

    fn resolve_template_inner<'a>(
        &'a self,
        value: &'a Value,
    ) -> Pin<Box<dyn Future<Output = Result<Value, WorkflowFailure>> + Send + 'a>> {
        Box::pin(async move {
            match value {
                Value::Object(object) if object.get("from").is_some() => {
                    let reference: ValueRef =
                        serde_json::from_value(value.clone()).map_err(|error| {
                            failure(
                                WorkflowFailureCode::InvalidInput,
                                format!("malformed value reference: {error}"),
                                false,
                            )
                        })?;
                    self.resolve_ref(&reference).await
                }
                Value::Object(object) => {
                    let mut resolved = serde_json::Map::new();
                    for (key, child) in object {
                        resolved.insert(key.clone(), self.resolve_template_inner(child).await?);
                    }
                    Ok(Value::Object(resolved))
                }
                Value::Array(array) => {
                    let mut resolved = Vec::with_capacity(array.len());
                    for child in array {
                        resolved.push(self.resolve_template_inner(child).await?);
                    }
                    Ok(Value::Array(resolved))
                }
                value => Ok(value.clone()),
            }
        })
    }

    async fn resolve_ref(&self, reference: &ValueRef) -> Result<Value, WorkflowFailure> {
        let (root, pointer) = match reference {
            ValueRef::Args { pointer } => (
                self.snapshot.lock().await.validated_args.clone(),
                pointer.as_str(),
            ),
            ValueRef::Step { step, pointer } => {
                let snapshot = self.snapshot.lock().await;
                let exact = format!("{step}@{}", self.scope);
                let output = snapshot
                    .steps
                    .get(&exact)
                    .or_else(|| snapshot.steps.get(step))
                    .and_then(|state| state.output.clone())
                    .ok_or_else(|| {
                        failure(
                            WorkflowFailureCode::UnknownReference,
                            format!("step output '{step}' unavailable in execution scope"),
                            false,
                        )
                    })?;
                (output, pointer.as_str())
            }
            ValueRef::Item { name, pointer } => (
                self.items.get(name).cloned().ok_or_else(|| {
                    failure(
                        WorkflowFailureCode::UnknownReference,
                        format!("map item '{name}' unavailable"),
                        false,
                    )
                })?,
                pointer.as_str(),
            ),
            ValueRef::Literal { value } => return Ok(value.clone()),
        };
        if pointer.is_empty() {
            Ok(root)
        } else {
            root.pointer(pointer).cloned().ok_or_else(|| {
                failure(
                    WorkflowFailureCode::UnknownReference,
                    format!("JSON pointer '{pointer}' not found"),
                    false,
                )
            })
        }
    }

    fn check_cancelled(&self) -> Result<(), WorkflowFailure> {
        if self.cancellation.is_cancelled() || self.branch_cancellation.is_cancelled() {
            Err(failure(
                WorkflowFailureCode::Cancelled,
                "workflow cancelled",
                false,
            ))
        } else {
            Ok(())
        }
    }
}

fn event(
    snapshot: &WorkflowRunSnapshot,
    step_id: Option<String>,
    kind: WorkflowRunEventKind,
) -> WorkflowRunEvent {
    WorkflowRunEvent {
        run_id: snapshot.run_id.clone(),
        sequence: snapshot.last_sequence,
        at: Utc::now(),
        step_id,
        kind,
    }
}
fn failure(
    code: WorkflowFailureCode,
    message: impl Into<String>,
    retryable: bool,
) -> WorkflowFailure {
    WorkflowFailure {
        code,
        message: message.into(),
        retryable,
    }
}
fn storage(error: std::io::Error) -> WorkflowRunError {
    WorkflowRunError::Storage(error.to_string())
}
fn exceeds_optional(requested: Option<u64>, ceiling: Option<u64>) -> bool {
    match (requested, ceiling) {
        (Some(requested), Some(ceiling)) => requested > ceiling,
        _ => false,
    }
}

fn enforce_budget_within(
    requested: &WorkflowBudgets,
    ceiling: &WorkflowBudgets,
) -> Result<(), &'static str> {
    if requested.max_concurrency > ceiling.max_concurrency {
        Err("max_concurrency")
    } else if requested.max_agents > ceiling.max_agents {
        Err("max_agents")
    } else if requested.max_steps > ceiling.max_steps {
        Err("max_steps")
    } else if requested.max_retries > ceiling.max_retries {
        Err("max_retries")
    } else if requested.max_nesting_depth > ceiling.max_nesting_depth {
        Err("max_nesting_depth")
    } else if requested.wall_time_ms > ceiling.wall_time_ms {
        Err("wall_time_ms")
    } else if exceeds_optional(requested.max_tokens, ceiling.max_tokens) {
        Err("max_tokens")
    } else if exceeds_optional(requested.max_cost_micros, ceiling.max_cost_micros) {
        Err("max_cost_micros")
    } else {
        Ok(())
    }
}

fn remaining_wall_time(snapshot: &WorkflowRunSnapshot) -> u64 {
    let elapsed = Utc::now()
        .signed_duration_since(snapshot.created_at)
        .num_milliseconds()
        .max(0) as u64;
    snapshot
        .definition
        .budgets
        .wall_time_ms
        .saturating_sub(elapsed)
}

fn resolve_checkpoint_template(
    value: &Value,
    args: &Value,
    outputs: &BTreeMap<String, Value>,
) -> Result<Value, WorkflowRunError> {
    match value {
        Value::Object(object) if object.contains_key("from") => {
            let reference: ValueRef = serde_json::from_value(value.clone())
                .map_err(|_| WorkflowRunError::Preflight("invalid checkpoint reference".into()))?;
            let (root, pointer) = match &reference {
                ValueRef::Args { pointer } => (args, pointer),
                ValueRef::Step { step, pointer } => (
                    outputs.get(step).ok_or_else(|| {
                        WorkflowRunError::Preflight(
                            "checkpoint reference is not in the completed prefix".into(),
                        )
                    })?,
                    pointer,
                ),
                ValueRef::Literal { value } => return Ok(value.clone()),
                ValueRef::Item { .. } => {
                    return Err(WorkflowRunError::Preflight(
                        "map references are unsupported for continuation".into(),
                    ))
                }
            };
            if pointer.is_empty() {
                Ok(root.clone())
            } else {
                root.pointer(pointer).cloned().ok_or_else(|| {
                    WorkflowRunError::Preflight("checkpoint reference pointer is missing".into())
                })
            }
        }
        Value::Object(object) => object
            .iter()
            .map(|(key, value)| {
                Ok((
                    key.clone(),
                    resolve_checkpoint_template(value, args, outputs)?,
                ))
            })
            .collect::<Result<serde_json::Map<_, _>, _>>()
            .map(Value::Object),
        Value::Array(values) => values
            .iter()
            .map(|value| resolve_checkpoint_template(value, args, outputs))
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array),
        value => Ok(value.clone()),
    }
}

fn definition_bundle_hash(bundle: &WorkflowDefinitionBundle) -> Result<String, WorkflowRunError> {
    let bytes = serde_json::to_vec(bundle)
        .map_err(|_| WorkflowRunError::Preflight("workflow bundle hashing failed".to_string()))?;
    Ok(hex::encode(Sha256::digest(bytes)))
}
fn parse_tool_result(result: ToolResult) -> Result<Value, WorkflowFailure> {
    if !result.success {
        return Err(failure(
            WorkflowFailureCode::ExecutionFailed,
            "workflow tool reported failure",
            true,
        ));
    }
    Ok(serde_json::from_str(&result.result).unwrap_or(Value::String(result.result)))
}
fn reject_secret_material(value: &Value) -> Result<(), String> {
    reject_secret_material_inner(value, false)
}

fn reject_secret_material_in_definition(value: &Value) -> Result<(), String> {
    reject_secret_material_inner(value, true)
}

fn reject_secret_material_inner(value: &Value, allow_bindings: bool) -> Result<(), String> {
    fn walk(value: &Value, key: Option<&str>, allow_bindings: bool) -> Result<(), String> {
        if value.as_object().is_some_and(|object| {
            object.len() == 1
                && object
                    .get("$secret")
                    .and_then(Value::as_str)
                    .is_some_and(|handle| !handle.trim().is_empty())
        }) {
            return Ok(());
        }
        let safe_binding = allow_bindings
            && serde_json::from_value::<ValueRef>(value.clone())
                .is_ok_and(|reference| !matches!(reference, ValueRef::Literal { .. }));
        if key.is_some_and(|key| {
            let normalized = key
                .chars()
                .filter(|character| character.is_ascii_alphanumeric())
                .flat_map(char::to_lowercase)
                .collect::<String>();
            matches!(
                normalized.as_str(),
                "secret"
                    | "token"
                    | "password"
                    | "credential"
                    | "credentials"
                    | "apikey"
                    | "accesskey"
                    | "accesstoken"
                    | "secretkey"
                    | "privatekey"
            )
        }) && !safe_binding
        {
            return Err("secret-bearing fields are not accepted by workflow runs".to_string());
        }
        if value.as_str().is_some_and(|value| {
            let trimmed = value.trim();
            trimmed.starts_with("capability://")
                || trimmed.starts_with("Bearer ")
                || trimmed.starts_with("sk-")
                || trimmed.starts_with("ghp_")
                || trimmed.starts_with("github_pat_")
        }) {
            // No production capability resolver is part of #578. Treating an
            // arbitrary caller string as an opaque handle would be an injection
            // channel, so handles and common raw credential forms fail closed.
            return Err("opaque credential handles are not enabled for workflows".to_string());
        }
        match value {
            Value::Object(object) => {
                for (key, value) in object {
                    if key == "properties" {
                        let properties = value.as_object().ok_or_else(|| {
                            "workflow schema properties must be an object".to_string()
                        })?;
                        for schema in properties.values() {
                            walk(schema, None, allow_bindings)?;
                        }
                    } else {
                        walk(value, Some(key), allow_bindings)?;
                    }
                }
            }
            Value::Array(array) => {
                for value in array {
                    walk(value, None, allow_bindings)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    walk(value, None, allow_bindings)
}

fn contains_secret_handle(value: &Value) -> bool {
    match value {
        Value::Object(object) => {
            object.contains_key("$secret") || object.values().any(contains_secret_handle)
        }
        Value::Array(array) => array.iter().any(contains_secret_handle),
        _ => false,
    }
}

fn contains_any_secret_material(value: &Value, secrets: &[String]) -> bool {
    let matches = |candidate: &str| {
        secrets
            .iter()
            .any(|secret| !secret.is_empty() && candidate.contains(secret))
    };
    fn walk(value: &Value, matches: &impl Fn(&str) -> bool) -> bool {
        match value {
            Value::String(value) => matches(value),
            Value::Object(object) => object
                .iter()
                .any(|(key, value)| matches(key) || walk(value, matches)),
            Value::Array(array) => array.iter().any(|value| walk(value, matches)),
            _ => false,
        }
    }
    walk(value, &matches)
}

fn plan_leaf_count(plan: &WorkflowPlan) -> usize {
    match plan {
        WorkflowPlan::Step { .. } => 1,
        WorkflowPlan::Sequence { nodes } | WorkflowPlan::Parallel { nodes } => {
            nodes.iter().fold(0usize, |total, node| {
                total.saturating_add(plan_leaf_count(node))
            })
        }
        WorkflowPlan::Choice {
            then_branch,
            else_branch,
            ..
        } => plan_leaf_count(then_branch).max(plan_leaf_count(else_branch)),
        WorkflowPlan::Map { body, .. } | WorkflowPlan::Retry { node: body, .. } => {
            plan_leaf_count(body)
        }
    }
}

fn plan_step_ids(plan: &WorkflowPlan) -> Vec<String> {
    match plan {
        WorkflowPlan::Step { step } => vec![step.clone()],
        WorkflowPlan::Sequence { nodes } | WorkflowPlan::Parallel { nodes } => {
            nodes.iter().flat_map(plan_step_ids).collect()
        }
        WorkflowPlan::Choice {
            then_branch,
            else_branch,
            ..
        } => {
            let mut result = plan_step_ids(then_branch);
            result.extend(plan_step_ids(else_branch));
            result
        }
        WorkflowPlan::Map { body, .. } | WorkflowPlan::Retry { node: body, .. } => {
            plan_step_ids(body)
        }
    }
}

fn instance_is_in_scope(instance_id: &str, step_id: &str, scope: &str) -> bool {
    if scope == "root" {
        instance_id == step_id
            || instance_id
                .strip_prefix(&format!("{step_id}@root"))
                .is_some_and(|suffix| suffix.starts_with('['))
    } else {
        let exact = format!("{step_id}@{scope}");
        instance_id == exact
            || instance_id
                .strip_prefix(&exact)
                .is_some_and(|suffix| suffix.starts_with('['))
    }
}

fn plan_frontier(plan: &WorkflowPlan) -> Vec<String> {
    match plan {
        WorkflowPlan::Step { step } => vec![step.clone()],
        WorkflowPlan::Sequence { nodes } => nodes.first().map_or_else(Vec::new, plan_frontier),
        WorkflowPlan::Parallel { nodes } => nodes.iter().flat_map(plan_frontier).collect(),
        WorkflowPlan::Choice {
            then_branch,
            else_branch,
            ..
        } => {
            let mut result = plan_frontier(then_branch);
            result.extend(plan_frontier(else_branch));
            result
        }
        WorkflowPlan::Map { body, .. } | WorkflowPlan::Retry { node: body, .. } => {
            plan_frontier(body)
        }
    }
}

fn validate_nested_input_contract(
    template: &Value,
    target_schema: &Value,
    compiled: &CompiledWorkflow,
) -> Result<(), String> {
    fn contains_ref(value: &Value) -> bool {
        match value {
            Value::Object(object) => {
                object.contains_key("from") || object.values().any(contains_ref)
            }
            Value::Array(array) => array.iter().any(contains_ref),
            _ => false,
        }
    }
    if !contains_ref(template) {
        return validate_schema(target_schema, template)
            .map_err(|error| format!("nested workflow input is incompatible: {error}"));
    }
    let reference: ValueRef = serde_json::from_value(template.clone()).map_err(|_| {
        "nested dynamic input schema cannot be proven compatible in phase 1".to_string()
    })?;
    let source_schema = match reference {
        ValueRef::Args { pointer } => {
            schema_at_pointer(&compiled.definition.input_schema, &pointer)
        }
        ValueRef::Step { step, pointer } => compiled
            .steps
            .get(&step)
            .and_then(|step| step.output_schema.as_ref())
            .and_then(|schema| schema_at_pointer(schema, &pointer)),
        ValueRef::Literal { value } => {
            return validate_schema(target_schema, &value)
                .map_err(|error| format!("nested workflow input is incompatible: {error}"));
        }
        ValueRef::Item { .. } => None,
    }
    .ok_or_else(|| {
        "nested dynamic input source schema is missing or pointer is invalid".to_string()
    })?;
    if schema_compatible(source_schema, target_schema) {
        Ok(())
    } else {
        Err("nested workflow input schema is not compatible with its pinned target".to_string())
    }
}

fn schema_at_pointer<'a>(schema: &'a Value, pointer: &str) -> Option<&'a Value> {
    if pointer.is_empty() {
        return Some(schema);
    }
    let mut current = schema;
    for token in pointer.strip_prefix('/')?.split('/') {
        let token = token.replace("~1", "/").replace("~0", "~");
        current = if token.parse::<usize>().is_ok() {
            current.get("items")?
        } else {
            current.get("properties")?.get(&token)?
        };
    }
    Some(current)
}

fn schema_compatible(source: &Value, target: &Value) -> bool {
    if source == target {
        return true;
    }
    let source_type = source.get("type").and_then(Value::as_str);
    let target_type = target.get("type").and_then(Value::as_str);
    source_type.is_some() && source_type == target_type && target_type != Some("object")
}

fn effective_limits(requested: &WorkflowBudgets, ceilings: &WorkflowBudgets) -> WorkflowBudgets {
    WorkflowBudgets {
        max_concurrency: requested.max_concurrency.min(ceilings.max_concurrency),
        max_agents: requested.max_agents.min(ceilings.max_agents),
        max_steps: requested.max_steps.min(ceilings.max_steps),
        max_retries: requested.max_retries.min(ceilings.max_retries),
        max_nesting_depth: requested.max_nesting_depth.min(ceilings.max_nesting_depth),
        wall_time_ms: requested.wall_time_ms.min(ceilings.wall_time_ms),
        max_tokens: match (requested.max_tokens, ceilings.max_tokens) {
            (Some(requested), Some(ceiling)) => Some(requested.min(ceiling)),
            (Some(requested), None) => Some(requested),
            (None, ceiling) => ceiling,
        },
        max_cost_micros: match (requested.max_cost_micros, ceilings.max_cost_micros) {
            (Some(requested), Some(ceiling)) => Some(requested.min(ceiling)),
            (Some(requested), None) => Some(requested),
            (None, ceiling) => ceiling,
        },
    }
}
