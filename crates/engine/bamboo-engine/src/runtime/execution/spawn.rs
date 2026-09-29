//! Sub-session spawn scheduler.
//!
//! Provides a background queue for spawning child sessions. Spawn is async
//! (tool returns immediately), but the UI can observe child progress via
//! events forwarded to the parent session stream.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use dashmap::DashMap;
use futures::future::BoxFuture;
use tokio::sync::{broadcast, mpsc, oneshot, Mutex, OwnedMutexGuard, RwLock};
use tokio_util::sync::CancellationToken;

use bamboo_agent_core::storage::Storage;
use bamboo_agent_core::tools::ToolExecutor;
use bamboo_agent_core::{AgentEvent, Session};
use bamboo_domain::{AdmissionCommit, AdmissionGate, RuntimeSessionPersistence, SessionInboxPort};
use bamboo_llm::ProviderModelRouter;

use crate::runtime::Agent;

use super::agent_spawn::SessionExecutionReservation;
use super::child_completion::{ChildCompletion, ChildCompletionHandler};
use super::runner_state::AgentRunner;

#[derive(Debug, Clone)]
pub struct SpawnJob {
    pub parent_session_id: String,
    pub child_session_id: String,
    pub model: String,
    /// Tool names to hide from the LLM schema for this child session.
    /// Computed from the child's `subagent_type` profile policy.
    pub disabled_tools: Option<Vec<String>>,
}

struct QueuedSpawnJob {
    job: SpawnJob,
    title: Option<String>,
    launch_generation: Option<u64>,
    announcement_ready: oneshot::Receiver<()>,
    start_published: Arc<AtomicBool>,
}

/// Serializes a queued launch's durable eligibility check and runner
/// reservation against parent cancellation of that same logical child.
#[derive(Clone, Default)]
struct ChildLaunchLocks {
    locks: Arc<DashMap<String, Arc<Mutex<()>>>>,
}

pub struct ChildLaunchGuard {
    guard: Option<OwnedMutexGuard<()>>,
    locks: ChildLaunchLocks,
    child_id: String,
}

impl Drop for ChildLaunchGuard {
    fn drop(&mut self) {
        self.guard.take();
        self.locks
            .locks
            .remove_if(&self.child_id, |_, lock| Arc::strong_count(lock) == 1);
    }
}

impl ChildLaunchLocks {
    async fn acquire(&self, child_id: &str) -> ChildLaunchGuard {
        let lock = self
            .locks
            .entry(child_id.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        let mut guard = ChildLaunchGuard {
            guard: None,
            locks: self.clone(),
            child_id: child_id.to_string(),
        };
        guard.guard = Some(lock.lock_owned().await);
        guard
    }
}

/// Optional application-layer preparation for a child run launched through
/// the canonical scheduler.
///
/// The engine owns runner reservation and execution. Applications may use
/// this synchronous, no-fail hook to attach observers to the already-created
/// child event sender (for example, the server's always-on notification
/// relay) without introducing an engine dependency on application services.
pub trait ChildRunLaunchHook: Send + Sync {
    fn before_child_launch(&self, job: &SpawnJob, child_events: broadcast::Sender<AgentEvent>);

    /// Optional queue boundary observer. The production notification hook is
    /// a no-op; deterministic scheduler tests can hold dequeue before the
    /// cancellation fence is acquired.
    fn before_queued_dequeue(&self, _job: &SpawnJob) -> BoxFuture<'static, ()> {
        Box::pin(async {})
    }

    /// Optional observer inside the cancellation fence, immediately before
    /// runner reservation. It must never make a model decision or perform
    /// unbounded work while this guard is held.
    fn before_queued_reservation(&self, _job: &SpawnJob) -> BoxFuture<'static, ()> {
        Box::pin(async {})
    }
}

/// Runtime-scoped durable inbox resources used by external actor drivers.
///
/// The host store remains canonical. A worker confirmation is only permission
/// for the driver to checkpoint that canonical logical Session and then ack the
/// exact claim; it never turns transport state into authority.
#[derive(Clone)]
pub struct SessionInboxRuntimeBinding {
    pub router: Arc<crate::SessionActivationRouter>,
    pub inbox: Arc<dyn SessionInboxPort>,
    pub storage: Arc<dyn Storage>,
    pub persistence: Arc<dyn RuntimeSessionPersistence>,
    /// The production Host's exact SessionRepository write coordinator.
    /// Canonical Worker checkpoints fail closed when this is absent.
    pub parent_question_lock: Option<Arc<bamboo_storage::LockedSessionStore>>,
}

/// Trait for external child session runtimes (e.g. A2A, CLI adapters).
///
/// Implementors are responsible for emitting AgentEvents via `event_tx`
/// and respecting the `cancel_token`.
#[async_trait::async_trait]
pub trait ExternalChildRunner: Send + Sync {
    /// A narrow, pre-persistence compatibility check on the actual registered
    /// runner. Unknown/custom routes fail closed for required one-shot packets.
    async fn validate_required_child_context_route(
        &self,
        _session: &Session,
    ) -> Result<(), String> {
        Err("required_child_context_unsupported: no supported registered worker route".into())
    }
    /// Returns true if this runner should handle the given child session.
    async fn should_handle(&self, session: &Session) -> bool;

    /// Execute the child session using an external runtime.
    async fn execute_external_child(
        &self,
        session: &mut Session,
        job: &SpawnJob,
        event_tx: tokio::sync::mpsc::Sender<AgentEvent>,
        cancel_token: CancellationToken,
    ) -> crate::runtime::runner::Result<()>;

    /// Persist a Host-only broker receipt before the final Child checkpoint.
    /// `Ok(true)` selects the append-safe final checkpoint. An unprepared
    /// broker terminal must never be ACKed after a merely ordinary save.
    async fn prepare_durable_child_delivery(
        &self,
        _session: &Session,
        _activation_run_id: &str,
    ) -> Result<bool, String> {
        Ok(false)
    }

    /// Called only after the Host's final Child Session save returns. The
    /// exact Child birth and activation run identify a pending broker terminal
    /// receipt; a failed save leaves the mailbox unacknowledged.
    async fn confirm_durable_child_delivery(
        &self,
        _session: &Session,
        _activation_run_id: &str,
        _save_succeeded: bool,
    ) -> Result<(), String> {
        Ok(())
    }

    /// Bind this runner's per-run escalation host bridge (#68). A nested worker's
    /// `run()` installs its OWN host bridge here so the runner can hand it to each
    /// grandchild's `drive()` AT SPAWN time (captured into the drive task, not read
    /// later), letting the grandchild re-proxy a non-bypass approval request UP to
    /// its parent run for its whole lifetime — even when it outlives the run that
    /// spawned it. Default no-op for runners that don't escalate (e.g. A2A).
    fn set_escalation_bridge(&self, _bridge: Option<bamboo_subagent::executor::HostBridge>) {}

    /// Bind the owning runtime's canonical SessionInbox resources. Actor
    /// runners use this to bridge active local/remote/warm workers without a
    /// process-global live-session registry.
    fn set_session_inbox_runtime(&self, _binding: Option<SessionInboxRuntimeBinding>) {}

    /// Optional actual host Store for the local zero-tool named-profile route.
    /// This does not enable owned Inbox claims or grant authority to custom runners.
    fn set_actor_directory_store(&self, _store: Option<Arc<bamboo_storage::SessionStoreV2>>) {}

    /// Bind the application-owned logical SubAgent tool after its canonical
    /// scheduler/adapter exist. Actor runners use it only for fenced child RPCs.
    fn set_canonical_subagent_tool(&self, _tool: Option<Arc<dyn bamboo_agent_core::tools::Tool>>) {}

    /// Optional host-owned, redacted canonical Actor event observer.
    fn set_actor_event_observer(
        &self,
        _observer: Option<Arc<dyn crate::external_agents::actor_event_stream::ActorEventObserver>>,
    ) {
    }
}

#[derive(Clone)]
pub struct SpawnContext {
    pub agent: Arc<Agent>,
    pub tools: Arc<dyn ToolExecutor>,
    pub sessions_cache: crate::SessionCache,
    pub agent_runners: Arc<RwLock<HashMap<String, AgentRunner>>>,
    pub session_event_senders: Arc<RwLock<HashMap<String, broadcast::Sender<AgentEvent>>>>,
    pub external_child_runner: Arc<dyn ExternalChildRunner>,
    pub provider_router: Option<Arc<ProviderModelRouter>>,
    pub app_data_dir: Option<std::path::PathBuf>,
    /// Optional application-layer completion hook. The engine still emits
    /// `SubAgentCompleted` to the parent stream itself; this hook lets the
    /// server persist parent wait state and resume the parent runner without
    /// introducing an engine -> AppState dependency.
    pub completion_handler: Option<Arc<dyn ChildCompletionHandler>>,
    /// Optional application observer setup shared by queued tool launches and
    /// reserved idle SessionInbox activation.
    pub child_run_launch_hook: Option<Arc<dyn ChildRunLaunchHook>>,
    /// Optional inbox to the account-wide change feed. When present, durable
    /// change events from child-session execution are mirrored onto the feed
    /// for resumable multi-client sync.
    pub account_feed_inbox: Option<super::event_forwarder::AccountFeedInbox>,
}

impl SpawnContext {
    pub(crate) fn replayable_event_publisher(
        &self,
    ) -> super::session_events::ReplayableSessionEventPublisher {
        super::session_events::ReplayableSessionEventPublisher::new(
            self.agent_runners.clone(),
            self.session_event_senders.clone(),
            self.account_feed_inbox.clone(),
        )
    }
}

#[derive(Clone)]
pub struct SpawnScheduler {
    tx: mpsc::Sender<QueuedSpawnJob>,
    ctx: SpawnContext,
    launch_locks: ChildLaunchLocks,
}

impl SpawnScheduler {
    pub async fn validate_required_child_context_route(
        &self,
        metadata: &HashMap<String, String>,
        role: &str,
    ) -> Result<(), String> {
        let mut candidate = Session::new("required-context-preflight", "");
        candidate.metadata = metadata.clone();
        candidate
            .metadata
            .insert("subagent_type".into(), role.into());
        self.ctx
            .external_child_runner
            .validate_required_child_context_route(&candidate)
            .await
    }

    pub fn new(ctx: SpawnContext) -> Self {
        let (tx, mut rx) = mpsc::channel::<QueuedSpawnJob>(128);
        let worker_ctx = ctx.clone();
        let launch_locks = ChildLaunchLocks::default();
        let worker_launch_locks = launch_locks.clone();

        // The worker loop is a single point of failure for ALL child spawning:
        // if it unwinds, queued jobs are dropped with no completion published
        // and every later enqueue fails with "spawn scheduler is not running"
        // for the rest of the process lifetime. Run each job on its own task
        // and await the JoinHandle — a panicking job is isolated, keeps the
        // worker alive, and still publishes a terminal error completion so the
        // waiting parent is woken instead of stranded.
        tokio::spawn(async move {
            while let Some(QueuedSpawnJob {
                job,
                title,
                launch_generation,
                announcement_ready,
                start_published,
            }) = rx.recv().await
            {
                let job_ctx = worker_ctx.clone();
                let job_for_panic = job.clone();
                let job_launch_locks = worker_launch_locks.clone();
                let handle = tokio::spawn(async move {
                    // The producer publishes Start before returning to the
                    // tool. If it is aborted after queue admission, the worker
                    // publishes Start before any child execution/completion.
                    let fallback_start =
                        needs_fallback_start(announcement_ready, &start_published).await;
                    if let Some(hook) = job_ctx.child_run_launch_hook.as_ref() {
                        hook.before_queued_dequeue(&job).await;
                    }
                    let launch_guard = if let Some(generation) = launch_generation {
                        let guard = job_launch_locks.acquire(&job.child_session_id).await;
                        if queued_launch_state(&job_ctx, &job, generation).await?
                            != QueuedLaunchState::Eligible
                        {
                            return Ok(());
                        }
                        Some(guard)
                    } else {
                        None
                    };
                    if let Some(hook) = job_ctx.child_run_launch_hook.as_ref() {
                        hook.before_queued_reservation(&job).await;
                    }
                    if fallback_start {
                        Self::prepare_child_launch(&job_ctx, &job).await;
                        job_ctx
                            .replayable_event_publisher()
                            .publish(
                                &job.parent_session_id,
                                AgentEvent::SubAgentStarted {
                                    parent_session_id: job.parent_session_id.clone(),
                                    child_session_id: job.child_session_id.clone(),
                                    title,
                                },
                            )
                            .await;
                    }
                    if let Err(err) = run_spawn_job(job_ctx, job, launch_guard).await {
                        tracing::warn!("spawn job failed: {}", err);
                    }
                    Ok(())
                });
                let failure = match handle.await {
                    Ok(Ok(())) => continue,
                    Ok(Err(error)) => error,
                    Err(error) => format!("child spawn panicked: {error}"),
                };
                {
                    tracing::error!(
                        parent_session_id = %job_for_panic.parent_session_id,
                        child_session_id = %job_for_panic.child_session_id,
                        error = %failure,
                        "admitted spawn job failed; publishing terminal error completion"
                    );
                    let publisher = worker_ctx.replayable_event_publisher();
                    publish_child_completion_parts(
                        &publisher,
                        worker_ctx.completion_handler.clone(),
                        job_for_panic.parent_session_id.clone(),
                        job_for_panic.child_session_id.clone(),
                        "error".to_string(),
                        Some(failure),
                    )
                    .await;
                }
            }
        });

        Self {
            tx,
            ctx,
            launch_locks,
        }
    }

    async fn prepare_child_launch(ctx: &SpawnContext, job: &SpawnJob) {
        let child_tx = super::session_events::get_or_create_event_sender(
            &ctx.session_event_senders,
            &job.child_session_id,
        )
        .await;
        invoke_child_run_launch_hook(ctx.child_run_launch_hook.as_ref(), job, child_tx);
    }

    pub async fn enqueue(&self, job: SpawnJob) -> Result<(), String> {
        self.enqueue_announced(job, None).await
    }

    /// Reserve queue capacity, publish the observable child Start, and only
    /// then release the job to the worker. This preserves S→C ordering even
    /// when child loading/execution fails immediately.
    pub async fn enqueue_announced(
        &self,
        job: SpawnJob,
        title: Option<String>,
    ) -> Result<(), String> {
        match self.enqueue_announced_with_gate(job, title, None).await? {
            AdmissionCommit::Committed(()) | AdmissionCommit::AlreadyCommitted => Ok(()),
            AdmissionCommit::Cancelled => unreachable!("an ungated launch cannot be cancelled"),
        }
    }

    pub async fn enqueue_announced_with_gate(
        &self,
        job: SpawnJob,
        title: Option<String>,
        gate: Option<&AdmissionGate>,
    ) -> Result<AdmissionCommit<()>, String> {
        self.enqueue_announced_for_generation(job, title, gate, None)
            .await
    }

    /// A child action supplies the generation it durably prepared before
    /// queue admission. Replayed or cancelled generations cannot borrow a
    /// newer explicit retry's pending status.
    pub async fn enqueue_announced_for_generation(
        &self,
        job: SpawnJob,
        title: Option<String>,
        gate: Option<&AdmissionGate>,
        launch_generation: Option<u64>,
    ) -> Result<AdmissionCommit<()>, String> {
        let preparation_job = job.clone();
        match reserve_and_send(&self.tx, job, title.clone(), gate, launch_generation).await? {
            AdmissionCommit::Committed((announce_ready, start_published)) => {
                let launch_guard = if launch_generation.is_some() {
                    Some(
                        self.launch_locks
                            .acquire(&preparation_job.child_session_id)
                            .await,
                    )
                } else {
                    None
                };
                let state = match launch_generation {
                    Some(generation) => {
                        queued_launch_state(&self.ctx, &preparation_job, generation).await?
                    }
                    None => QueuedLaunchState::Eligible,
                };
                if state == QueuedLaunchState::Eligible {
                    Self::prepare_child_launch(&self.ctx, &preparation_job).await;
                    self.ctx
                        .replayable_event_publisher()
                        .publish(
                            &preparation_job.parent_session_id,
                            AgentEvent::SubAgentStarted {
                                parent_session_id: preparation_job.parent_session_id.clone(),
                                child_session_id: preparation_job.child_session_id.clone(),
                                title,
                            },
                        )
                        .await;
                }
                drop(launch_guard);
                // From publication through the acknowledgement there is no
                // await. A dropped producer after this point must not make
                // the worker publish a duplicate Start.
                start_published.store(true, Ordering::SeqCst);
                let _ = announce_ready.send(());
                match state {
                    QueuedLaunchState::Eligible => Ok(AdmissionCommit::Committed(())),
                    QueuedLaunchState::AlreadyStarted => Ok(AdmissionCommit::AlreadyCommitted),
                    QueuedLaunchState::Stale => Ok(AdmissionCommit::Cancelled),
                }
            }
            AdmissionCommit::AlreadyCommitted => Ok(AdmissionCommit::AlreadyCommitted),
            AdmissionCommit::Cancelled => Ok(AdmissionCommit::Cancelled),
        }
    }

    /// Publish replayable parent-session state through the scheduler's shared
    /// runner/cache/account/broadcast boundary.
    pub async fn publish_parent_replayable_event(
        &self,
        parent_session_id: &str,
        event: AgentEvent,
    ) {
        self.ctx
            .replayable_event_publisher()
            .publish(parent_session_id, event)
            .await;
    }

    pub async fn lock_child_launch(&self, child_id: &str) -> ChildLaunchGuard {
        self.launch_locks.acquire(child_id).await
    }

    pub async fn publish_queued_child_cancellation(&self, parent_id: &str, child_id: &str) {
        let publisher = self.ctx.replayable_event_publisher();
        publish_child_completion_parts(
            &publisher,
            self.ctx.completion_handler.clone(),
            parent_id.to_string(),
            child_id.to_string(),
            "cancelled".to_string(),
            Some("Cancelled by parent before activation".to_string()),
        )
        .await;
    }

    /// Launch through the canonical child core using a runner slot already
    /// reserved by SessionInbox activation. This bypasses only queue mechanics;
    /// placement and execution still flow through `run_child_spawn`.
    pub(crate) fn launch_reserved(
        &self,
        job: SpawnJob,
        reservation: SessionExecutionReservation,
    ) -> tokio::task::JoinHandle<()> {
        let ctx = self.ctx.clone();
        tokio::spawn(async move {
            Self::prepare_child_launch(&ctx, &job).await;
            ctx.replayable_event_publisher()
                .publish(
                    &job.parent_session_id,
                    AgentEvent::SubAgentStarted {
                        parent_session_id: job.parent_session_id.clone(),
                        child_session_id: job.child_session_id.clone(),
                        title: None,
                    },
                )
                .await;
            if let Err(error) =
                crate::sdk::spawn::run_child_spawn_reserved(ctx, job.clone(), reservation).await
            {
                tracing::warn!(
                    parent_session_id = %job.parent_session_id,
                    child_session_id = %job.child_session_id,
                    %error,
                    "reserved child activation failed"
                );
            }
        })
    }
}

async fn needs_fallback_start(
    announcement_ready: oneshot::Receiver<()>,
    start_published: &AtomicBool,
) -> bool {
    announcement_ready.await.is_err() && !start_published.load(Ordering::SeqCst)
}

fn invoke_child_run_launch_hook(
    hook: Option<&Arc<dyn ChildRunLaunchHook>>,
    job: &SpawnJob,
    child_tx: broadcast::Sender<AgentEvent>,
) {
    let Some(hook) = hook else {
        return;
    };
    let invoked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        hook.before_child_launch(job, child_tx);
    }));
    if invoked.is_err() {
        tracing::error!(
            parent_session_id = %job.parent_session_id,
            child_session_id = %job.child_session_id,
            "child launch hook panicked; continuing with canonical execution"
        );
    }
}

/// The queue send is the admission commit. The cancellation fence and send run
/// in one synchronous critical section after capacity is reserved. Observer
/// setup and `SubAgentStarted` run only for a committed queue entry, before
/// the child execution starts.
async fn reserve_and_send(
    tx: &mpsc::Sender<QueuedSpawnJob>,
    job: SpawnJob,
    title: Option<String>,
    gate: Option<&AdmissionGate>,
    launch_generation: Option<u64>,
) -> Result<AdmissionCommit<(oneshot::Sender<()>, Arc<AtomicBool>)>, String> {
    if let Some(gate) = gate {
        if gate.is_committed() {
            return Ok(AdmissionCommit::AlreadyCommitted);
        }
        if gate.is_cancelled() {
            return Ok(AdmissionCommit::Cancelled);
        }
    }
    let permit = match gate {
        Some(gate) => tokio::select! {
            biased;
            _ = gate.cancelled() => return Ok(AdmissionCommit::Cancelled),
            permit = tx.reserve() => permit,
        },
        None => tx.reserve().await,
    }
    .map_err(|_| "spawn scheduler is not running".to_string())?;
    let (announce_ready, announcement_ready) = oneshot::channel();
    let start_published = Arc::new(AtomicBool::new(false));
    let queued = QueuedSpawnJob {
        job,
        title,
        launch_generation,
        announcement_ready,
        start_published: start_published.clone(),
    };
    match gate {
        Some(gate) => gate.commit(|| {
            permit.send(queued);
            Ok((announce_ready, start_published))
        }),
        None => {
            permit.send(queued);
            Ok(AdmissionCommit::Committed((
                announce_ready,
                start_published,
            )))
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ChildWatchdogPolicy {
    check_interval_secs: i64,
    // pub(crate): the child-wait watchdog (#546) reads these limits to decide
    // when a Running runner entry whose task died (frozen last_event_at) is
    // stale beyond what the per-child liveness watchdog could still act on.
    pub(crate) max_total_secs: i64,
    pub(crate) max_idle_secs: i64,
}

impl Default for ChildWatchdogPolicy {
    fn default() -> Self {
        Self {
            check_interval_secs: 15,
            // Parent waits may be longer, but child execution owns its own
            // liveness. A one hour total cap avoids indefinitely orphaned
            // sub-session runners.
            max_total_secs: 60 * 60,
            // No child event for 15 minutes is considered stalled.
            max_idle_secs: 15 * 60,
        }
    }
}

fn metadata_i64(session: &Session, key: &str) -> Option<i64> {
    session
        .metadata
        .get(key)
        .and_then(|value| value.trim().parse::<i64>().ok())
        .filter(|value| *value > 0)
}

pub(crate) fn watchdog_policy_for_session(session: &Session) -> ChildWatchdogPolicy {
    let mut policy = ChildWatchdogPolicy::default();
    if let Some(value) = metadata_i64(session, "child_watchdog.max_total_secs") {
        policy.max_total_secs = value;
    }
    if let Some(value) = metadata_i64(session, "child_watchdog.max_idle_secs") {
        policy.max_idle_secs = value;
    }
    if let Some(value) = metadata_i64(session, "child_watchdog.check_interval_secs") {
        policy.check_interval_secs = value;
    }
    policy
}

async fn publish_child_completion(
    publisher: &super::session_events::ReplayableSessionEventPublisher,
    completion_handler: Option<Arc<dyn ChildCompletionHandler>>,
    completion: ChildCompletion,
) {
    publisher
        .publish(
            &completion.parent_session_id,
            AgentEvent::SubAgentCompleted {
                parent_session_id: completion.parent_session_id.clone(),
                child_session_id: completion.child_session_id.clone(),
                status: completion.status.clone(),
                error: completion.error.clone(),
            },
        )
        .await;

    if let Some(handler) = completion_handler {
        // Contain a panicking handler: this call frequently runs on the caller's
        // only liveness-critical task (the child's terminal block, or the spawn
        // scheduler worker for early failures). Unwinding here would kill that
        // task after the child already looks terminal everywhere — the classic
        // stranded-parent signature. The child-wait watchdog backstops the wake
        // that a panicked handler failed to deliver.
        use futures::FutureExt;
        let parent_session_id = completion.parent_session_id.clone();
        let child_session_id = completion.child_session_id.clone();
        if std::panic::AssertUnwindSafe(handler.on_child_completed(completion))
            .catch_unwind()
            .await
            .is_err()
        {
            tracing::error!(
                %parent_session_id,
                %child_session_id,
                "child completion handler panicked; child-wait watchdog will backstop the parent wake"
            );
        }
    }
}

pub(crate) async fn publish_child_completion_parts(
    publisher: &super::session_events::ReplayableSessionEventPublisher,
    completion_handler: Option<Arc<dyn ChildCompletionHandler>>,
    parent_session_id: String,
    child_session_id: String,
    status: String,
    error: Option<String>,
) {
    publish_child_completion(
        publisher,
        completion_handler,
        ChildCompletion {
            parent_session_id,
            child_session_id,
            status,
            error,
            completed_at: Utc::now(),
        },
    )
    .await;
}

pub(crate) async fn watch_child_liveness(
    parent_session_id: String,
    child_session_id: String,
    runners: Arc<RwLock<HashMap<String, AgentRunner>>>,
    cancel_token: CancellationToken,
    timeout_reason: Arc<RwLock<Option<String>>>,
    done: CancellationToken,
    policy: ChildWatchdogPolicy,
) {
    let mut ticker =
        tokio::time::interval(Duration::from_secs(policy.check_interval_secs.max(1) as u64));
    // Skip the immediate tick.
    ticker.tick().await;

    loop {
        tokio::select! {
            _ = done.cancelled() => return,
            _ = ticker.tick() => {
                if cancel_token.is_cancelled() {
                    return;
                }

                let snapshot = {
                    let guard = runners.read().await;
                    guard.get(&child_session_id).cloned()
                };
                let Some(runner) = snapshot else {
                    return;
                };
                if !matches!(runner.status, super::runner_state::AgentStatus::Running) {
                    return;
                }

                let now = Utc::now();
                let total_secs = now.signed_duration_since(runner.started_at).num_seconds();
                if total_secs >= policy.max_total_secs {
                    let reason = format!(
                        "Child session timed out after {} seconds (max_total_secs={})",
                        total_secs, policy.max_total_secs
                    );
                    tracing::warn!(
                        parent_session_id = %parent_session_id,
                        child_session_id = %child_session_id,
                        reason = %reason,
                        "child session total timeout; cancelling child runner"
                    );
                    *timeout_reason.write().await = Some(reason);
                    cancel_token.cancel();
                    return;
                }

                let last_activity_at = runner.last_activity_at().unwrap_or(runner.started_at);
                let idle_secs = now.signed_duration_since(last_activity_at).num_seconds();
                if idle_secs >= policy.max_idle_secs {
                    let reason = format!(
                        "Child session idle timeout after {} seconds without events (max_idle_secs={})",
                        idle_secs, policy.max_idle_secs
                    );
                    tracing::warn!(
                        parent_session_id = %parent_session_id,
                        child_session_id = %child_session_id,
                        reason = %reason,
                        last_tool_name = ?runner.last_tool_name,
                        last_tool_phase = ?runner.last_tool_phase,
                        round_count = runner.round_count,
                        "child session idle timeout; cancelling child runner"
                    );
                    *timeout_reason.write().await = Some(reason);
                    cancel_token.cancel();
                    return;
                }
            }
        }
    }
}

/// Drive a single queued spawn job through the canonical child-spawn path.
///
/// ANTI-FORK: this is a 1-line delegator to [`crate::sdk::spawn::run_child_spawn`],
/// which is the single implementation of the spawn/execute/finalize logic. The
/// `SpawnScheduler` queue mechanics (above) remain here; the body lives in the SDK
/// core so both the scheduler and the ergonomic `ChildRunner` funnel into it.
async fn run_spawn_job(
    ctx: SpawnContext,
    job: SpawnJob,
    launch_guard: Option<ChildLaunchGuard>,
) -> Result<(), String> {
    match launch_guard {
        Some(guard) => crate::sdk::spawn::run_child_spawn_fenced(ctx, job, guard).await,
        None => crate::sdk::spawn::run_child_spawn(ctx, job).await,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum QueuedLaunchState {
    Eligible,
    AlreadyStarted,
    Stale,
}

async fn queued_launch_state(
    ctx: &SpawnContext,
    job: &SpawnJob,
    generation: u64,
) -> Result<QueuedLaunchState, String> {
    let child = ctx
        .agent
        .storage()
        .load_session(&job.child_session_id)
        .await
        .map_err(|error| format!("load queued child launch fence: {error}"))?;
    let Some(child) = child else {
        return Ok(QueuedLaunchState::Stale);
    };
    if child.kind != bamboo_domain::SessionKind::Child
        || child.parent_session_id.as_deref() != Some(job.parent_session_id.as_str())
        || child.child_launch_generation() != generation
        || child.is_child_launch_cancelled(generation)
    {
        return Ok(QueuedLaunchState::Stale);
    }
    match child.last_run_status().as_deref() {
        Some("pending") => Ok(QueuedLaunchState::Eligible),
        Some("running" | "completed" | "error" | "timeout" | "skipped" | "suspended") => {
            Ok(QueuedLaunchState::AlreadyStarted)
        }
        _ => Ok(QueuedLaunchState::Stale),
    }
}

#[cfg(test)]
mod launch_hook_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn job() -> SpawnJob {
        SpawnJob {
            parent_session_id: "parent".to_string(),
            child_session_id: "child".to_string(),
            model: "test".to_string(),
            disabled_tools: None,
        }
    }

    struct PanickingHook {
        calls: AtomicUsize,
    }

    impl ChildRunLaunchHook for PanickingHook {
        fn before_child_launch(
            &self,
            _job: &SpawnJob,
            _child_events: broadcast::Sender<AgentEvent>,
        ) {
            self.calls.fetch_add(1, Ordering::SeqCst);
            panic!("injected launch hook panic");
        }
    }

    #[test]
    fn launch_hook_panic_is_contained() {
        let hook = Arc::new(PanickingHook {
            calls: AtomicUsize::new(0),
        });
        let hook_port: Arc<dyn ChildRunLaunchHook> = hook.clone();
        let (child_tx, _child_rx) = broadcast::channel(1);

        invoke_child_run_launch_hook(Some(&hook_port), &job(), child_tx);

        assert_eq!(hook.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn closed_scheduler_does_not_prepare_phantom_launch() {
        let (tx, rx) = mpsc::channel(1);
        drop(rx);
        let result = reserve_and_send(&tx, job(), None, None, None).await;

        assert_eq!(result.unwrap_err(), "spawn scheduler is not running");
    }

    #[tokio::test]
    async fn cancellation_while_waiting_for_queue_capacity_never_sends() {
        let (tx, mut rx) = mpsc::channel(1);
        let (_, announcement_ready) = oneshot::channel();
        tx.send(QueuedSpawnJob {
            job: job(),
            title: None,
            launch_generation: None,
            announcement_ready,
            start_published: Arc::new(AtomicBool::new(false)),
        })
        .await
        .unwrap();
        let gate = Arc::new(AdmissionGate::default());
        let waiting = {
            let tx = tx.clone();
            let gate = gate.clone();
            tokio::spawn(async move { reserve_and_send(&tx, job(), None, Some(&gate), None).await })
        };
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        gate.cancel_if_pending();
        assert!(matches!(
            tokio::time::timeout(Duration::from_millis(200), waiting)
                .await
                .expect("cancellation must wake without freeing queue capacity")
                .unwrap()
                .unwrap(),
            AdmissionCommit::Cancelled
        ));
        rx.recv().await.unwrap();
        assert!(matches!(
            rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test]
    async fn committed_queue_gate_does_not_enqueue_a_duplicate_job() {
        let (tx, mut rx) = mpsc::channel(1);
        let gate = AdmissionGate::default();
        assert!(matches!(
            reserve_and_send(&tx, job(), None, Some(&gate), None)
                .await
                .unwrap(),
            AdmissionCommit::Committed(_)
        ));
        assert!(matches!(
            reserve_and_send(&tx, job(), None, Some(&gate), None)
                .await
                .unwrap(),
            AdmissionCommit::AlreadyCommitted
        ));
        rx.recv().await.unwrap();
        assert!(matches!(
            rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test]
    async fn producer_drop_after_start_publication_does_not_require_fallback_start() {
        let (announce_ready, announcement_ready) = oneshot::channel::<()>();
        let start_published = Arc::new(AtomicBool::new(false));
        let runners = Arc::new(RwLock::new(HashMap::new()));
        let senders = Arc::new(RwLock::new(HashMap::new()));
        let (sender, mut receiver) = broadcast::channel(4);
        senders.write().await.insert("parent".to_string(), sender);
        let publisher = super::super::session_events::ReplayableSessionEventPublisher::new(
            runners, senders, None,
        );
        // Reproduce the precise post-publish, pre-ack producer loss: the
        // worker observes a closed announcement channel, but Start is already
        // in the replayable event stream.
        publisher
            .publish(
                "parent",
                AgentEvent::SubAgentStarted {
                    parent_session_id: "parent".to_string(),
                    child_session_id: "child".to_string(),
                    title: None,
                },
            )
            .await;
        start_published.store(true, Ordering::SeqCst);
        drop(announce_ready);
        assert!(!needs_fallback_start(announcement_ready, &start_published).await);
        assert!(matches!(
            receiver.try_recv(),
            Ok(AgentEvent::SubAgentStarted { .. })
        ));
        assert!(matches!(
            receiver.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));

        let (announce_ready, announcement_ready) = oneshot::channel::<()>();
        let start_published = AtomicBool::new(false);
        drop(announce_ready);
        assert!(needs_fallback_start(announcement_ready, &start_published).await);
    }
}
