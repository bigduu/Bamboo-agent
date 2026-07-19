use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Weak};
use std::time::Duration;

use async_trait::async_trait;
use bamboo_agent_core::tools::{
    FunctionCall, ToolCall, ToolExecutionContext, ToolExecutor, ToolOutcome, ToolResult,
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
use dashmap::DashMap;
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
}

#[derive(Debug, Clone)]
pub struct AgentStepResult {
    pub output: Value,
    pub tokens: u64,
    pub cost_micros: u64,
}

#[async_trait]
pub trait AgentStepPort: Send + Sync {
    /// #563 seam. Unknown names must return `Ok(None)` and fail preflight.
    async fn resolve(&self, name: &str) -> Result<Option<NamedAgentSpec>, String>;
    async fn execute(
        &self,
        spec: &NamedAgentSpec,
        prompt: Value,
        model: Option<&str>,
        effort: Option<&str>,
        capabilities: &BTreeSet<String>,
        session_id: &str,
    ) -> Result<AgentStepResult, String>;
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
}

struct ActiveRun {
    cancellation: CancellationToken,
    snapshot: Arc<Mutex<WorkflowRunSnapshot>>,
}

struct RuntimeRegistration {
    engine: Weak<WorkflowRunEngine>,
    run_id: String,
}

impl Drop for RuntimeRegistration {
    fn drop(&mut self) {
        if let Some(engine) = self.engine.upgrade() {
            engine.active.remove(&self.run_id);
            engine.events.remove(&self.run_id);
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
        })
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
    /// definition snapshot. Prefix/script resume remains explicitly out of scope (#581).
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
        self.active.insert(
            run_id.clone(),
            Arc::new(ActiveRun {
                cancellation: cancellation.clone(),
                snapshot: snapshot.clone(),
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
        };
        let result = tokio::time::timeout(
            Duration::from_millis(compiled.definition.budgets.wall_time_ms),
            context.execute_node(&compiled.definition.plan, "root"),
        )
        .await;
        let mut final_snapshot = snapshot.lock().await;
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

    pub async fn cancel(&self, run_id: &str) -> Result<WorkflowRunSnapshot, WorkflowRunError> {
        let mut snapshot = self
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
        if let Some(active) = self.active.get(run_id).map(|active| active.clone()) {
            active.cancellation.cancel();
            let mut shared = active.snapshot.lock().await;
            if !shared.status.is_terminal() {
                self.finish_cancelled(&mut shared).await?;
            }
            return Ok(shared.clone());
        }
        self.finish_cancelled(&mut snapshot).await?;
        Ok(snapshot)
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
                                .resolve(agent)
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
                                self.cancel_active_parallel_steps(nodes).await?;
                                error.message =
                                    format!("parallel branch[{index}] failed: {}", error.message);
                                return Err(error);
                            }
                        }
                    }
                    Ok(Value::Array(output))
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
                let context = ToolExecutionContext {
                    session_id: Some(&session_id),
                    tool_call_id: &call.id,
                    event_tx: None,
                    available_tool_schemas: None,
                    bypass_permissions: false,
                    can_async_resume: false,
                    bash_completion_sink: None,
                    pre_parsed_args: Some(&resolved_input),
                };
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
                        )
                        .await
                    {
                        Ok(result) => {
                            let exceeded =
                                self.record_usage(result.tokens, result.cost_micros).await;
                            self.checkpoint_usage("agent_usage_recorded").await?;
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
                            last_error = Some("named agent execution failed".to_string())
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

    async fn record_usage(&self, tokens: u64, cost_micros: u64) -> Option<WorkflowFailure> {
        let mut usage = self.ledger.lock().await;
        let next_tokens = usage.tokens.saturating_add(tokens);
        let next_cost = usage.cost_micros.saturating_add(cost_micros);
        usage.tokens = next_tokens;
        usage.cost_micros = next_cost;
        if self
            .root_limits
            .max_tokens
            .is_some_and(|limit| next_tokens > limit)
            || self
                .root_limits
                .max_cost_micros
                .is_some_and(|limit| next_cost > limit)
        {
            return Some(failure(
                WorkflowFailureCode::BudgetExceeded,
                "workflow token/cost budget exceeded",
                false,
            ));
        }
        None
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
                .is_some_and(|limit| usage.cost_micros >= limit)
        {
            return Err(failure(
                WorkflowFailureCode::BudgetExceeded,
                "workflow token/cost budget exhausted before agent dispatch",
                false,
            ));
        }
        Ok(())
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
                                    failure: Some(failure(
                                        WorkflowFailureCode::DependencySkipped,
                                        reason_owned.clone(),
                                        false,
                                    )),
                                    attempts: 0,
                                },
                            );
                            state.status = WorkflowStepStatus::Skipped;
                            state.failure = Some(failure(
                                WorkflowFailureCode::DependencySkipped,
                                reason_owned,
                                false,
                            ));
                        },
                    )
                    .await?;
                }
                WorkflowPlan::Sequence { nodes } | WorkflowPlan::Parallel { nodes } => {
                    for node in nodes {
                        self.skip_plan(node, reason).await?;
                    }
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
