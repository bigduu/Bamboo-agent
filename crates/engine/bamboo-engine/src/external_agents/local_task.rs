//! Native Child execution in the Host process. Identity, input, history and
//! terminal publication still belong to the canonical Actor/Session stores.
//! There is no broker, Worker IPC, or remote fallback in this backend.
use super::runtime::NativeToolCeilingSource;
use crate::execution::spawn::{ExternalChildRunner, SessionInboxRuntimeBinding, SpawnJob};
use crate::runtime::{Agent, ExecuteRequestBuilder};
use async_trait::async_trait;
use bamboo_agent_core::tools::ToolExecutor;
use bamboo_agent_core::{AgentError, AgentEvent, Session};
use bamboo_domain::{
    ActorActivationClaim, ActorActivationFinish, ActorDirectoryPort, ChildActorRuntimeWrite,
    RuntimeSessionPersistence, SessionKind, Storage,
};
use bamboo_llm::{Config, ProviderModelRouter};
use futures::FutureExt;
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::{mpsc, Semaphore};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct LocalTaskRuntime {
    pub agent: Arc<Agent>,
    /// The Host's complete permission-checking base chain, without the Root role fence.
    pub tools: Arc<dyn ToolExecutor>,
    pub provider_router: Option<Arc<ProviderModelRouter>>,
    pub app_data_dir: Option<PathBuf>,
    pub sessions_cache: crate::SessionCache,
}

#[derive(Default)]
struct Bindings {
    runtime: Option<LocalTaskRuntime>,
    inbox: Option<SessionInboxRuntimeBinding>,
    store: Option<Arc<bamboo_storage::SessionStoreV2>>,
    tickets: Option<Arc<bamboo_tickets::TicketService>>,
    subagent: Option<Arc<dyn bamboo_agent_core::tools::Tool>>,
    observer: Option<Arc<dyn super::actor_event_stream::ActorEventObserver>>,
}

/// Constructed only after the actual Agent task was joined and its owned
/// resources drained. A request to cancel, a PID, and a durable status cannot
/// manufacture this value.
#[derive(Clone, Debug)]
pub struct ConfirmedTaskStop {
    session_id: String,
    child_birth: chrono::DateTime<chrono::Utc>,
    run_id: String,
    instance_id: uuid::Uuid,
}
impl ConfirmedTaskStop {
    pub(crate) fn session_id(&self) -> &str {
        &self.session_id
    }
    pub(crate) fn child_birth(&self) -> chrono::DateTime<chrono::Utc> {
        self.child_birth
    }
    pub(crate) fn run_id(&self) -> &str {
        &self.run_id
    }
    pub(crate) fn instance_id(&self) -> uuid::Uuid {
        self.instance_id
    }
}

struct ExecutionOutput {
    session: Session,
    result: crate::runtime::runner::Result<()>,
    resources: std::io::Result<bamboo_tools::DrainedLocalTaskResources>,
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum LaunchState {
    Queued,
    Preparing,
    Running,
    Stopping,
    Stopped,
}

struct Execution {
    owner: ChildActorRuntimeWrite,
    persistence: Arc<dyn RuntimeSessionPersistence>,
    cancel: CancellationToken,
    resources: bamboo_tools::LocalTaskResources,
    task: tokio::sync::Mutex<Option<tokio::task::JoinHandle<ExecutionOutput>>>,
    stop: Mutex<Option<ConfirmedTaskStop>>,
    launch: Mutex<LaunchState>,
    admission: Mutex<Option<tokio::sync::OwnedSemaphorePermit>>,
    slot: Mutex<Option<tokio::sync::OwnedSemaphorePermit>>,
    launch_handoff: tokio::sync::watch::Sender<bool>,
    heartbeat_stop: CancellationToken,
    heartbeat: tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl Execution {
    async fn wait_for_handoff(&self) {
        let mut handoff = self.launch_handoff.subscribe();
        while !*handoff.borrow_and_update() {
            // This Execution owns the sender, so cancellation of any other
            // waiter cannot close it or consume another waiter's wakeup.
            handoff
                .changed()
                .await
                .expect("execution retains handoff sender");
        }
    }
}

pub struct LocalTaskChildRunner {
    bindings: Mutex<Bindings>,
    executions: Mutex<HashMap<String, Arc<Execution>>>,
    coordinators: Mutex<Vec<tokio::task::JoinHandle<Result<(), String>>>>,
    coordinator_errors: Mutex<Vec<String>>,
    concurrency: Arc<Semaphore>,
    admission: Arc<Semaphore>,
    preparations: Arc<Semaphore>,
    closed: AtomicBool,
    ceiling: Option<Arc<dyn NativeToolCeilingSource>>,
    permission: Option<Arc<bamboo_tools::permission::PermissionConfig>>,
    remote_roles: Vec<String>,
    approval_registry: Option<super::approval_registry::SharedApprovalRegistry>,
    approval_reviewer: Option<Arc<dyn super::actor_adapter::ChildApprovalReviewer>>,
}

fn failure(error: impl std::fmt::Display) -> AgentError {
    AgentError::LLM(format!("local task backend: {error}"))
}
fn record_stop_metadata(session: &mut Session, proof: &ConfirmedTaskStop) {
    session
        .metadata
        .insert("runtime.local_task.state".into(), "stopped".into());
    session.metadata.insert(
        "runtime.local_task.instance".into(),
        proof.instance_id.to_string(),
    );
    session
        .metadata
        .insert("runtime.local_task.run_id".into(), proof.run_id.clone());
    if session
        .metadata
        .get(bamboo_subagent::proto::WORKFLOW_USAGE_REQUESTED_KEY)
        .is_some_and(|v| v == "true")
    {
        session.metadata.insert(
            bamboo_subagent::proto::WORKFLOW_TERMINAL_OBSERVATION_KEY.into(),
            proof.run_id.clone(),
        );
    }
}

fn reconcile_question_handoff(session: &mut Session, previous_request: Option<&str>) {
    let issued_here = session
        .metadata
        .get(bamboo_domain::PARENT_QUESTION_REQUEST_KEY)
        .is_some_and(|request| Some(request.as_str()) != previous_request);
    if issued_here {
        // Preserve this run's handoff even if a fast parent answer already
        // cleared pending_question before the Agent returned.
        session.metadata.insert(
            "runtime.actor_parent_question_handoff".into(),
            "true".into(),
        );
    } else {
        session
            .metadata
            .remove("runtime.actor_parent_question_handoff");
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

impl LocalTaskChildRunner {
    pub fn new(
        config: &Config,
        ceiling: Option<Arc<dyn NativeToolCeilingSource>>,
        permission: Option<Arc<bamboo_tools::permission::PermissionConfig>>,
    ) -> Self {
        Self {
            bindings: Mutex::new(Bindings::default()),
            executions: Mutex::new(HashMap::new()),
            coordinators: Mutex::new(Vec::new()),
            coordinator_errors: Mutex::new(Vec::new()),
            concurrency: Arc::new(Semaphore::new(
                config
                    .subagents()
                    .max_concurrent
                    .unwrap_or(super::actor_adapter::DEFAULT_MAX_CONCURRENT_ACTORS)
                    .max(1),
            )),
            admission: Arc::new(Semaphore::new(
                config
                    .subagents()
                    .max_concurrent
                    .unwrap_or(super::actor_adapter::DEFAULT_MAX_CONCURRENT_ACTORS)
                    .max(1)
                    .saturating_add(128),
            )),
            preparations: Arc::new(Semaphore::new(128)),
            closed: AtomicBool::new(false),
            ceiling,
            permission,
            approval_registry: None,
            approval_reviewer: None,
            remote_roles: config
                .subagents()
                .remote_placements
                .iter()
                .map(|p| p.role.clone())
                .chain(
                    config
                        .subagents()
                        .schedulable_placements
                        .iter()
                        .map(|p| p.role.clone()),
                )
                .collect(),
        }
    }
    pub fn with_approval(
        mut self,
        registry: Option<super::approval_registry::SharedApprovalRegistry>,
        reviewer: Option<Arc<dyn super::actor_adapter::ChildApprovalReviewer>>,
    ) -> Self {
        self.approval_registry = registry;
        self.approval_reviewer = reviewer;
        self
    }
    async fn stop_unstarted(
        &self,
        execution: &Execution,
        session: &mut Session,
        run_id: &str,
    ) -> Result<(), String> {
        {
            let mut state = lock(&execution.launch);
            match *state {
                LaunchState::Queued | LaunchState::Preparing => *state = LaunchState::Stopping,
                LaunchState::Stopped => return Ok(()),
                _ => return Err("local execution launch or stop is already in progress".into()),
            }
        }
        let drained = execution
            .resources
            .close_and_drain()
            .await
            .map_err(|e| e.to_string())?;
        let identity = drained.identity();
        if identity.session_id != session.id
            || identity.run_id != run_id
            || identity.child_birth != session.created_at
        {
            return Err("local resource identity changed during rollback".into());
        }
        let proof = ConfirmedTaskStop {
            session_id: session.id.clone(),
            child_birth: session.created_at,
            run_id: run_id.into(),
            instance_id: identity.owner_id,
        };
        record_stop_metadata(session, &proof);
        *lock(&execution.stop) = Some(proof);
        *lock(&execution.launch) = LaunchState::Stopped;
        Ok(())
    }
    fn execution(&self, session: &Session, run_id: &str) -> Result<Arc<Execution>, String> {
        let execution = lock(&self.executions)
            .get(&session.id)
            .cloned()
            .ok_or("local execution ownership is missing")?;
        if execution.owner.created_at != session.created_at
            || execution.owner.fence.run_id != run_id
        {
            return Err("local execution birth or generation changed".into());
        }
        Ok(execution)
    }
    fn runtime(&self) -> Result<LocalTaskRuntime, String> {
        lock(&self.bindings)
            .runtime
            .clone()
            .ok_or("Host local task runtime is unavailable".into())
    }
}

#[async_trait]
impl ExternalChildRunner for LocalTaskChildRunner {
    fn set_local_task_runtime(&self, runtime: Option<LocalTaskRuntime>) {
        lock(&self.bindings).runtime = runtime;
    }
    fn set_session_inbox_runtime(&self, inbox: Option<SessionInboxRuntimeBinding>) {
        lock(&self.bindings).inbox = inbox;
    }
    fn set_actor_directory_store(&self, store: Option<Arc<bamboo_storage::SessionStoreV2>>) {
        lock(&self.bindings).store = store;
    }
    fn set_ticket_service(&self, service: Option<Arc<bamboo_tickets::TicketService>>) {
        lock(&self.bindings).tickets = service;
    }
    fn set_canonical_subagent_tool(&self, tool: Option<Arc<dyn bamboo_agent_core::tools::Tool>>) {
        lock(&self.bindings).subagent = tool;
    }
    fn set_actor_event_observer(
        &self,
        observer: Option<Arc<dyn super::actor_event_stream::ActorEventObserver>>,
    ) {
        lock(&self.bindings).observer = observer;
    }

    async fn should_handle(&self, session: &Session) -> bool {
        session.kind == SessionKind::Child
            && session
                .metadata
                .get("external.agent_id")
                .is_none_or(|id| id == super::config::LOCAL_ACTOR_AGENT_ID)
            && !self
                .remote_roles
                .iter()
                .any(|role| session.metadata.get("subagent_type") == Some(role))
    }
    async fn validate_required_child_context_route(&self, session: &Session) -> Result<(), String> {
        // Admission probes use a synthetic Child before its durable birth.
        // Check only its configured route here; execution separately proves
        // the canonical Child, lineage and activation ownership.
        if session
            .metadata
            .get("external.agent_id")
            .is_some_and(|id| id != super::config::LOCAL_ACTOR_AGENT_ID)
            || self
                .remote_roles
                .iter()
                .any(|role| session.metadata.get("subagent_type") == Some(role))
            || self.ceiling.is_none()
        {
            return Err(
                "required_child_context_unsupported: local native ceiling unavailable".into(),
            );
        }
        self.runtime()?;
        let bindings = lock(&self.bindings);
        if bindings.store.is_none() || bindings.inbox.is_none() {
            return Err("canonical local Child storage is unavailable".into());
        }
        Ok(())
    }

    async fn prepare_child_execution(
        &self,
        session: &Session,
        run_id: &str,
        cancel: CancellationToken,
    ) -> Result<Option<Arc<dyn RuntimeSessionPersistence>>, String> {
        let _preparation = self
            .preparations
            .clone()
            .try_acquire_owned()
            .map_err(|_| "local Child preparation capacity exhausted")?;
        if self.closed.load(Ordering::Acquire) {
            return Err("local task backend is shutting down".into());
        }
        // Keep scheduler backpressure: an accepted setup waits for bounded
        // capacity instead of detaching an unbounded pending coordinator.
        let admission = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err("local Child cancelled before admission".into()),
            permit = self.admission.clone().acquire_owned() => permit.map_err(|_| "local task backend is shutting down")?,
        };
        let runtime = self.runtime()?;
        let (store, inbox) = {
            let bindings = lock(&self.bindings);
            (
                bindings
                    .store
                    .clone()
                    .ok_or("canonical local Actor store unavailable")?,
                bindings
                    .inbox
                    .clone()
                    .ok_or("canonical local Inbox unavailable")?,
            )
        };
        if !inbox.router.owns_run(&session.id, run_id).await {
            return Err("local Child activation is not owned by this Host run".into());
        }
        if lock(&self.executions).contains_key(&session.id) {
            return Err("previous local execution has not been finalized".into());
        }
        let durable = store
            .load_session(&session.id)
            .await
            .map_err(|e| e.to_string())?
            .ok_or("canonical Child is missing")?;
        if durable.created_at != session.created_at
            || durable.parent_created_at != session.parent_created_at
            || durable.parent_session_id != session.parent_session_id
            || durable.root_session_id != session.root_session_id
            || durable.project_id_meta() != session.project_id_meta()
        {
            return Err("canonical Child birth or lineage changed".into());
        }
        if durable
            .metadata
            .get("runtime.local_task.state")
            .is_some_and(|state| state != "stopped")
        {
            return Err("previous local task has no confirmed stop; recovery is blocked".into());
        }
        store
            .ensure_actor(&session.id)
            .await
            .map_err(|e| e.to_string())?;
        store
            .bind_control_plane(
                &session.id,
                bamboo_domain::ActorControlPlaneBinding {
                    policy_revision: self.permission.as_ref().map(|p| p.policy_revision()),
                    placement_intent: Some(bamboo_domain::ActorPlacementIntent::Local),
                },
            )
            .await
            .map_err(|e| e.to_string())?;
        let locked = inbox
            .parent_question_lock
            .as_ref()
            .ok_or("canonical local write coordinator unavailable")?;
        let now = chrono::Utc::now();
        let claimed = store
            .claim_activation(&ActorActivationClaim {
                actor_id: session.id.clone(),
                run_id: run_id.into(),
                lease_owner: format!("host-task-{}", uuid::Uuid::new_v4()),
                lease_expires_at: now + chrono::Duration::seconds(30),
                inbox_generation: session
                    .session_inbox_admission()
                    .map_or(0, |state| state.last_admitted_sequence),
                placement_ref: None,
                now,
            })
            .await
            .map_err(|e| e.to_string())?;
        let setup = async {
            let owner = ChildActorRuntimeWrite::from_session(claimed.fence(), session)
                .map_err(|e| e.to_string())?;
            let cache = runtime.sessions_cache.clone();
            let persistence: Arc<dyn RuntimeSessionPersistence> = Arc::new(
                locked
                    .bind_child_actor_writer(
                        owner.clone(),
                        store.clone(),
                        Arc::new(move |saved| {
                            cache.insert(
                                saved.id.clone(),
                                Arc::new(crate::SessionSnapshot::new(saved.clone())),
                            );
                        }),
                    )
                    .map_err(|e| e.to_string())?,
            );
            store
                .start_activation(&owner.fence, now)
                .await
                .map_err(|e| e.to_string())?;
            Ok::<_, String>((owner, persistence))
        }
        .await;
        let (owner, persistence) = match setup {
            Ok(bound) => bound,
            Err(error) => {
                return match store
                    .finish_activation(
                        &claimed.fence(),
                        chrono::Utc::now(),
                        ActorActivationFinish::Failed,
                    )
                    .await
                {
                    Ok(_) => Err(error),
                    Err(rollback) => Err(format!(
                        "{error}; exact claim rollback unconfirmed: {rollback}"
                    )),
                };
            }
        };
        let heartbeat_stop = CancellationToken::new();
        let heartbeat = {
            let store = store.clone();
            let fence = owner.fence.clone();
            let done = heartbeat_stop.clone();
            let cancel = cancel.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(5));
                interval.tick().await;
                loop {
                    tokio::select! { _ = done.cancelled() => break, _ = interval.tick() => {} }
                    let now = chrono::Utc::now();
                    if store
                        .renew_activation(&fence, now, now + chrono::Duration::seconds(30))
                        .await
                        .is_err()
                    {
                        cancel.cancel();
                        break;
                    }
                }
            })
        };
        let execution = Arc::new(Execution {
            owner,
            persistence: persistence.clone(),
            cancel,
            resources: bamboo_tools::LocalTaskResources::new(
                &session.id,
                run_id,
                session.created_at,
            ),
            task: tokio::sync::Mutex::new(None),
            stop: Mutex::new(None),
            launch: Mutex::new(LaunchState::Queued),
            admission: Mutex::new(Some(admission)),
            slot: Mutex::new(None),
            launch_handoff: tokio::sync::watch::channel(false).0,
            heartbeat_stop,
            heartbeat: tokio::sync::Mutex::new(Some(heartbeat)),
        });
        lock(&self.executions).insert(session.id.clone(), execution);
        Ok(Some(persistence))
    }

    async fn execute_external_child(
        &self,
        session: &mut Session,
        job: &SpawnJob,
        event_tx: mpsc::Sender<AgentEvent>,
        cancel_token: CancellationToken,
    ) -> crate::runtime::runner::Result<()> {
        let runtime = self.runtime().map_err(failure)?;
        let (binding, tickets) = {
            let b = lock(&self.bindings);
            (
                b.inbox
                    .clone()
                    .ok_or_else(|| failure("Inbox unavailable"))?,
                b.tickets.clone(),
            )
        };
        let run_id = binding
            .router
            .current_run_id(&session.id)
            .await
            .ok_or_else(|| failure("activation missing"))?;
        let execution = self.execution(session, &run_id).map_err(failure)?;
        {
            let mut state = lock(&execution.launch);
            if *state != LaunchState::Queued {
                return Err(failure("duplicate or closed local task launch"));
            }
            *state = LaunchState::Preparing;
        }
        let result = async {
        let slot = tokio::select! { biased; _ = cancel_token.cancelled() => return Err(AgentError::Cancelled), slot = self.concurrency.clone().acquire_owned() => slot.map_err(failure)? };
        *lock(&execution.slot) = Some(slot);
        crate::session_app::child_session::named_profile::validate_named_profile(session)
            .map_err(failure)?;
        if let (Some(policy), Some(workspace)) = (&self.permission, &session.workspace) {
            policy.register_session_workspace(session.id.clone(), workspace);
        }
        let required =
            bamboo_domain::ChildContextBinding::from_session(session).map_err(failure)?;
        let strict = required.is_some() || crate::session_app::child_session::named_profile::has_named_profile(session)
            || session.metadata.contains_key(crate::ticket_worker_plan::TICKET_LOCAL_PLAN_KEY);
        let names = if strict { match &self.ceiling {
            Some(source) => Some(source.observe(session).await.map_err(failure)?),
            None => return Err(failure("strict native tool ceiling unavailable")),
        }} else { None };
        let mut ticket_plan = None;
        if session.metadata.contains_key(crate::ticket_worker_plan::TICKET_LOCAL_PLAN_KEY) {
            let service = tickets.clone().ok_or_else(||failure("Ticket authority unavailable"))?;
            crate::ticket_runtime::admit_ticket_run(&service, &binding, session, &run_id).await.map_err(failure)?;
            let assignment = session.metadata[crate::ticket_worker_plan::TICKET_LOCAL_PLAN_KEY].clone();
            let plan = Arc::new(crate::ticket_worker_plan::TicketWorkerPlan::from_runtime_receipt(service, &assignment).map_err(failure)?);
            session.agent_runtime_state.get_or_insert_with(Default::default).run_id = run_id.clone();
            plan.bind_session(session).map_err(failure)?;
            ticket_plan = Some(plan);
        }
        let mut tools: Arc<dyn ToolExecutor> = runtime.tools.clone();
        let names = names.unwrap_or_else(||tools.list_tools().into_iter().map(|s|s.function.name).collect());
        let store = lock(&self.bindings).store.clone().ok_or_else(||failure("local store unavailable"))?;
        tools = Arc::new(tool_policy::LocalChildTools::new(tools, names.clone(), session, store, execution.owner.clone(), job.disabled_tools.as_deref())?.with_canonical_subagent(lock(&self.bindings).subagent.clone()));
        if session.metadata.contains_key(crate::ticket_worker_plan::TICKET_LOCAL_PLAN_KEY) {
            tools = Arc::new(crate::ticket_worker_plan::files::LocalFileExecutor::new(
                tools, tickets.clone().ok_or_else(||failure("Ticket authority unavailable"))?, session.clone(), run_id.clone(), &names
            ).map_err(failure)?);
        }
        let (local_event_tx, local_event_rx) = mpsc::channel(256);
        let mut request = ExecuteRequestBuilder::new("", local_event_tx.clone(), cancel_token.clone())
            .tools(tools)
            .model(job.model.clone());
        if strict { request = request.selected_skill_ids(Vec::new()); }
        else {
            if let Some(ids) = session.metadata.get("selected_skill_ids").and_then(|raw|bamboo_skills::selection::parse_selected_skill_ids_metadata(raw)) { request = request.selected_skill_ids(ids); }
            if let Some(mode) = session.metadata.get("skill_mode").or_else(||session.metadata.get("mode")).map(|v|v.trim()).filter(|v|!v.is_empty()) { request = request.selected_skill_mode(mode); }
        }
        if let Some(data_dir) = runtime.app_data_dir.clone() {
            request = request.app_data_dir(data_dir);
        }
        if let Some(disabled) = &job.disabled_tools {
            request = request.disabled_tools(disabled.iter().cloned().collect());
        }
        if let Some(reference) = crate::session_app::provider_model::session_effective_model_ref(session) {
            if let Some(router) = &runtime.provider_router {
                request = request.model(reference.model.clone())
                    .provider_override(router.route(&reference).map_err(failure)?)
                    .provider_name(reference.provider.clone());
                if let Some(kind) = router.provider_type_for(&reference) {
                    request = request.provider_type(kind);
                }
            }
        }
        if let Some(effort) = session.reasoning_effort {
            request = request.reasoning_effort(effort);
        }
        if let Some(plan) = ticket_plan { request = request.ticket_worker_plan(plan); }
        let mut owned_session = session.clone();
        // The question fact survives its answer for replay/deduplication. The
        // handoff is only for the run that issued that fact, not every later
        // run that happens to retain it in canonical history.
        let parent_question_at_start = owned_session.metadata.get(bamboo_domain::PARENT_QUESTION_REQUEST_KEY).cloned();
        owned_session.metadata.remove("runtime.actor_parent_question_handoff");
        owned_session.metadata.remove(bamboo_subagent::proto::WORKFLOW_TERMINAL_OBSERVATION_KEY);
        owned_session.metadata.remove(bamboo_subagent::proto::WORKFLOW_USAGE_OBSERVATION_KEY);
        owned_session
            .metadata
            .insert("runtime.local_task.state".into(), "running".into());
        execution
            .persistence
            .checkpoint_runtime_session(&mut owned_session)
            .await
            .map_err(failure)?;
        let agent = runtime
            .agent
            .with_execution_persistence(execution.persistence.clone());
        let resources = execution.resources.clone();
        let task_cancel = cancel_token.clone();
        let mut handle = execution.task.lock().await;
        {
            let mut state = lock(&execution.launch);
            if *state != LaunchState::Preparing { return Err(failure("local launch gate is closed")); }
            if cancel_token.is_cancelled() || self.closed.load(Ordering::Acquire) { return Err(AgentError::Cancelled); }
            *state = LaunchState::Running;
        }
        let approval_proxy: Option<Arc<dyn bamboo_tools::ApprovalProxy>> = match (&self.permission, &self.approval_reviewer) {
            (Some(policy), Some(reviewer)) => Some(Arc::new(approval::LocalApprovalProxy::new(approval::LocalApprovalContext {
                store: lock(&self.bindings).store.clone().expect("prepared store"), owner: execution.owner.clone(), router: binding.router.clone(), cancel: cancel_token.clone(), policy: policy.clone(), reviewer: reviewer.clone(), registry: self.approval_registry.clone(),
            }))),
            _ => None,
        };
        let (event_store, event_observer) = { let bindings=lock(&self.bindings); (bindings.store.clone().expect("prepared store"), bindings.observer.clone()) };
        let event_owner = execution.owner.clone();
        let event_cancel = cancel_token.clone();
        let task = tokio::spawn(async move {
            let forward = tokio::spawn(events::forward(event_store, event_owner, event_observer, local_event_rx, event_tx, event_cancel));
            let result = std::panic::AssertUnwindSafe(
                resources.scope(bamboo_tools::with_approval_proxy(approval_proxy, async {
                    if let Some(audit) = bamboo_domain::PermissionAuditSnapshot::from_metadata(&owned_session.metadata) {
                        local_event_tx.send(AgentEvent::PermissionPostureActivated { session_id: owned_session.id.clone(), policy_revision: audit.policy_revision, requested_mode: audit.resolution.requested.as_str().to_owned(), effective_mode: audit.resolution.effective.as_str().to_owned(), executor_mapping: audit.executor_mapping }).await.map_err(failure)?;
                    }
                    drop(local_event_tx);
                    // This owned execution has now taken over the admitted
                    // resume. Mirror the normal Root execution seam: retained
                    // answer/retry handoff markers are not future pending work.
                    // Do not consume them while queued or during preparation.
                    crate::session_app::execute::consume_pending_clarification_resume(&mut owned_session);
                    agent.execute(&mut owned_session, request.build()).await
                })),
            )
            .catch_unwind()
            .await;
            // This is a private execution token. Finalization cancels remaining
            // safe auxiliary requests even when the main loop returned normally.
            task_cancel.cancel();
            if result.is_err() { resources.record_unconfirmed("Agent task panicked; physical effects remain unconfirmed"); }
            let mut drained = resources.close_and_drain().await;
            let mut result = match result {
                Ok(result) => result,
                Err(_) => {
                    // The loop itself may have unwound a Tokio filesystem or
                    // custom-tool future outside registered builtin resources.
                    // Joining the task proves exit, not those physical effects.
                    drained = Err(std::io::Error::other("Agent task panicked; physical effects remain unconfirmed"));
                    Err(failure("Agent task panicked"))
                },
            };
            match forward.await {
                Ok(Ok(())) => {},
                Ok(Err(error)) => result = Err(failure(format!("local event publication failed: {error}"))),
                Err(error) => { drained = Err(std::io::Error::other(format!("local event owner panicked: {error}"))); result = Err(failure(error)); },
            }
            reconcile_question_handoff(&mut owned_session, parent_question_at_start.as_deref());
            ExecutionOutput {
                session: owned_session,
                result,
                resources: drained,
            }
        });
        *handle = Some(task);
        let joined = handle.as_mut().expect("owned task").await;
        handle.take();
        let output = joined.map_err(|error| failure(format!("Agent task join failed: {error}")))?;
        *session = output.session;
        let drained = output
            .resources
            .map_err(|error| failure(format!("resource stop unconfirmed: {error}")))?;
        let identity = drained.identity();
        if identity.session_id != session.id
            || identity.run_id != run_id
            || identity.child_birth != session.created_at
        {
            return Err(failure("resource owner changed"));
        }
        let proof = ConfirmedTaskStop {
            session_id: session.id.clone(),
            child_birth: session.created_at,
            run_id: run_id.clone(),
            instance_id: identity.owner_id,
        };
        record_stop_metadata(session, &proof);
        if session.metadata.get(bamboo_subagent::proto::WORKFLOW_USAGE_REQUESTED_KEY).is_some_and(|v|v == "true") {
            if let Some(runtime) = session.agent_runtime_state.as_ref().filter(|runtime|runtime.run_id == run_id) {
                let usage = bamboo_subagent::proto::WorkflowAgentUsage { kind: bamboo_subagent::proto::WorkflowAgentUsage::TYPE.into(), activation_run_id: run_id.clone(), child_session_id: session.id.clone(), child_created_at: session.created_at, prompt_tokens: runtime.round.total_prompt_tokens, completion_tokens: runtime.round.total_completion_tokens };
                session.metadata.insert(bamboo_subagent::proto::WORKFLOW_USAGE_OBSERVATION_KEY.into(), serde_json::to_string(&usage).map_err(failure)?);
            }
        }
        *lock(&execution.stop) = Some(proof.clone());
        *lock(&execution.launch) = LaunchState::Stopped;
        let stopped_tickets = lock(&self.bindings).tickets.clone();
        if let Some(tickets) = stopped_tickets {
            if session
                .metadata
                .contains_key(crate::ticket_worker_plan::TICKET_LOCAL_PLAN_KEY)
            {
                crate::ticket_runtime::checkpoint_owned_task_stop(
                    &tickets,
                    &binding,
                    session,
                    &run_id,
                    &proof,
                    output.result.is_ok(),
                )
                .await
                .map_err(failure)?;
            }
        }
        output.result
        }.await;
        // No-work stop is proved by permanently closing the launch gate and
        // draining the resource domain, never by inventing a PID or a task join.
        let unstarted = *lock(&execution.launch) == LaunchState::Preparing;
        if unstarted {
            self.stop_unstarted(&execution, session, &run_id)
                .await
                .map_err(failure)?;
        }
        if unstarted
            && crate::ticket_runtime::read_dispatch(session)
                .ok()
                .flatten()
                .is_some_and(|d| d.receipt.is_some())
        {
            let proof = lock(&execution.stop)
                .clone()
                .ok_or_else(|| failure("local stop proof missing"))?;
            if let Some(tickets) = tickets {
                crate::ticket_runtime::checkpoint_owned_task_stop(
                    &tickets, &binding, session, &run_id, &proof, false,
                )
                .await
                .map_err(failure)?;
            }
        }
        result
    }

    async fn abandon_child_execution(&self, session: &Session, run_id: &str) -> Result<(), String> {
        let execution = self.execution(session, run_id)?;
        execution.cancel.cancel();
        execution.launch_handoff.send_replace(true);
        let mut stopped = session.clone();
        self.stop_unstarted(&execution, &mut stopped, run_id)
            .await?;
        stopped.set_last_run_status("error");
        stopped.set_last_run_error("initial local Child checkpoint failed");
        execution
            .persistence
            .checkpoint_runtime_session(&mut stopped)
            .await
            .map_err(|e| e.to_string())?;
        self.finish_child_execution(&stopped, run_id, true).await
    }

    fn quarantine_child_execution(&self, session: &Session, run_id: &str) {
        if let Ok(execution) = self.execution(session, run_id) {
            execution.cancel.cancel();
            let mut state = lock(&execution.launch);
            if matches!(*state, LaunchState::Queued | LaunchState::Preparing) {
                *state = LaunchState::Stopping;
            }
            execution.launch_handoff.send_replace(true);
        }
    }

    fn track_child_task(
        &self,
        session_id: &str,
        run_id: &str,
        handle: tokio::task::JoinHandle<()>,
    ) -> Option<tokio::task::JoinHandle<()>> {
        let execution = lock(&self.executions).get(session_id).cloned();
        if let Some(execution) = execution.filter(|e| e.owner.fence.run_id == run_id) {
            // Admission remains occupied through the SDK publication tail.
            // The retained supervisor owns and joins the actual coordinator;
            // completed permits are returned without waiting for a later spawn.
            let permit = lock(&execution.admission).take();
            let supervisor = tokio::spawn(async move {
                let _permit = permit;
                handle.await.map_err(|error| error.to_string())
            });
            let mut handles = lock(&self.coordinators);
            let mut pending = Vec::with_capacity(handles.len() + 1);
            for mut handle in handles.drain(..) {
                if handle.is_finished() {
                    match (&mut handle).now_or_never() {
                        Some(Err(error)) => lock(&self.coordinator_errors).push(error.to_string()),
                        Some(Ok(Err(error))) => lock(&self.coordinator_errors).push(error),
                        Some(Ok(Ok(()))) => {}
                        None => pending.push(handle),
                    }
                } else {
                    pending.push(handle);
                }
            }
            pending.push(supervisor);
            *handles = pending;
            execution.launch_handoff.send_replace(true);
            None
        } else {
            Some(handle)
        }
    }

    async fn prepare_durable_child_delivery(
        &self,
        session: &Session,
        run_id: &str,
    ) -> Result<bool, String> {
        if !self.should_handle(session).await {
            return Ok(false);
        }
        let execution = self.execution(session, run_id)?;
        if lock(&execution.stop).is_none() {
            return Err("local execution stop is unconfirmed; terminal publication blocked".into());
        }
        Ok(true)
    }
    async fn commit_durable_child_delivery(
        &self,
        session: &Session,
        run_id: &str,
    ) -> Result<(), String> {
        if !self.should_handle(session).await {
            return Ok(());
        }
        let execution = self.execution(session, run_id)?;
        if lock(&execution.stop).is_none() {
            return Err("local task stop receipt missing".into());
        }
        let mut final_session = session.clone();
        crate::execution::ChildCompletionSource::publish_after_durable_delivery(
            &mut final_session,
            run_id,
        )?;
        execution
            .persistence
            .checkpoint_runtime_session(&mut final_session)
            .await
            .map_err(|e| e.to_string())?;
        let store = lock(&self.bindings)
            .store
            .clone()
            .ok_or("local store unavailable")?;
        let readback = store
            .load_session(&session.id)
            .await
            .map_err(|e| e.to_string())?
            .ok_or("Child missing after terminal checkpoint")?;
        if readback.created_at != session.created_at
            || readback
                .metadata
                .get("runtime.local_task.state")
                .map(String::as_str)
                != Some("stopped")
            || readback.metadata.get("runtime.local_task.instance")
                != final_session.metadata.get("runtime.local_task.instance")
            || readback.metadata.get("runtime.local_task.run_id") != Some(&run_id.to_string())
            || crate::execution::ChildCompletionSource::from_committed_session(&readback)
                != crate::execution::ChildCompletionSource::from_committed_session(&final_session)
        {
            return Err("local terminal checkpoint readback differs".into());
        }
        let tickets = lock(&self.bindings).tickets.clone();
        if let Some(tickets) = tickets.filter(|_| {
            crate::ticket_runtime::read_dispatch(&readback)
                .ok()
                .flatten()
                .is_some_and(|d| d.receipt.is_some())
        }) {
            crate::ticket_runtime::checkpoint_ticket_result(
                &tickets,
                store.as_ref(),
                &readback,
                run_id,
            )
            .await
            .map_err(|e| e.to_string())?;
        }
        Ok(())
    }
    async fn finish_child_execution(
        &self,
        session: &Session,
        run_id: &str,
        history_committed: bool,
    ) -> Result<(), String> {
        let execution = self.execution(session, run_id)?;
        // SDK registers the real coordinator immediately after spawning it.
        // Keep this record visible until its handle has another strong owner.
        execution.wait_for_handoff().await;
        if !history_committed || lock(&execution.stop).is_none() {
            return Err("local Child finalization remains unconfirmed".into());
        }
        execution.heartbeat_stop.cancel();
        if let Some(handle) = execution.heartbeat.lock().await.take() {
            handle.await.map_err(|e| e.to_string())?;
        }
        let store = lock(&self.bindings)
            .store
            .clone()
            .ok_or("local store unavailable")?;
        let outcome = match session.last_run_status().as_deref() {
            Some("completed" | "suspended") => ActorActivationFinish::Succeeded,
            Some("cancelled") => ActorActivationFinish::Cancelled,
            _ => ActorActivationFinish::Failed,
        };
        store
            .finish_activation(&execution.owner.fence, chrono::Utc::now(), outcome)
            .await
            .map_err(|e| e.to_string())?;
        lock(&execution.slot).take();
        lock(&self.executions).remove(&session.id);
        Ok(())
    }
    async fn shutdown(&self) -> Result<(), String> {
        self.closed.store(true, Ordering::Release);
        self.admission.close();
        // Preparations take a permit before observing closed. Holding all of
        // them here closes the snapshot race with an already admitted claim.
        let _preparations = self
            .preparations
            .acquire_many(128)
            .await
            .map_err(|e| e.to_string())?;
        let executions: Vec<_> = lock(&self.executions).values().cloned().collect();
        for execution in &executions {
            execution.cancel.cancel();
        }
        let mut errors = Vec::new();
        for execution in &executions {
            execution.wait_for_handoff().await;
        }
        let coordinators = std::mem::take(&mut *lock(&self.coordinators));
        for coordinator in coordinators {
            match coordinator.await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => errors.push(error),
                Err(error) => errors.push(error.to_string()),
            }
        }
        for execution in executions {
            execution.wait_for_handoff().await;
            execution.heartbeat_stop.cancel();
            if let Some(heartbeat) = execution.heartbeat.lock().await.take() {
                if let Err(error) = heartbeat.await {
                    errors.push(error.to_string());
                }
            }
            if let Some(task) = execution.task.lock().await.take() {
                if let Err(error) = task.await {
                    errors.push(error.to_string());
                }
            }
            if let Err(error) = execution.resources.close_and_drain().await {
                errors.push(error.to_string());
            }
            if lock(&execution.stop).is_none() {
                errors.push(format!(
                    "{} local stop remains unconfirmed",
                    execution.owner.fence.actor_id
                ));
            }
        }
        // A setup accepted before shutdown may harvest an older coordinator
        // while handing off. Read this queue only after all handoffs/joins, so
        // its retained error cannot be lost between the two snapshots.
        errors.extend(std::mem::take(&mut *lock(&self.coordinator_errors)));
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("; "))
        }
    }
}

mod approval;
mod events;
mod tool_policy;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod question_handoff_tests {
    use super::*;

    #[test]
    fn answered_question_fact_does_not_suspend_successor_run() {
        let mut session = Session::new("child", "model");
        session.metadata.insert(
            bamboo_domain::PARENT_QUESTION_REQUEST_KEY.into(),
            "durable-question".into(),
        );
        session.metadata.insert(
            "runtime.actor_parent_question_handoff".into(),
            "true".into(),
        );
        reconcile_question_handoff(&mut session, Some("durable-question"));
        assert!(!session
            .metadata
            .contains_key("runtime.actor_parent_question_handoff"));
    }

    #[test]
    fn newly_issued_question_keeps_handoff_after_fast_answer() {
        let mut session = Session::new("child", "model");
        session.metadata.insert(
            bamboo_domain::PARENT_QUESTION_REQUEST_KEY.into(),
            "durable-question".into(),
        );
        assert!(session.pending_question.is_none());
        reconcile_question_handoff(&mut session, None);
        assert_eq!(
            session
                .metadata
                .get("runtime.actor_parent_question_handoff")
                .map(String::as_str),
            Some("true")
        );
    }
}
