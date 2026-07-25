//! Sub-session spawn scheduler.
//!
//! Provides a background queue for spawning child sessions. Spawn is async
//! (tool returns immediately), but the UI can observe child progress via
//! events forwarded to the parent session stream.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tokio::sync::{broadcast, mpsc, RwLock};
use tokio_util::sync::CancellationToken;

use bamboo_agent_core::storage::Storage;
use bamboo_agent_core::tools::ToolExecutor;
use bamboo_agent_core::{AgentEvent, Session};
use bamboo_domain::{RuntimeSessionPersistence, SessionInboxPort};
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

/// Optional application-layer preparation for a child run launched through
/// the canonical scheduler.
///
/// The engine owns runner reservation and execution. Applications may use
/// this synchronous, no-fail hook to attach observers to the already-created
/// child event sender (for example, the server's always-on notification
/// relay) without introducing an engine dependency on application services.
pub trait ChildRunLaunchHook: Send + Sync {
    fn before_child_launch(&self, job: &SpawnJob, child_events: broadcast::Sender<AgentEvent>);
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
}

/// Trait for external child session runtimes (e.g. A2A, CLI adapters).
///
/// Implementors are responsible for emitting AgentEvents via `event_tx`
/// and respecting the `cancel_token`.
#[async_trait::async_trait]
pub trait ExternalChildRunner: Send + Sync {
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

#[derive(Clone)]
pub struct SpawnScheduler {
    tx: mpsc::Sender<SpawnJob>,
    ctx: SpawnContext,
}

impl SpawnScheduler {
    pub fn new(ctx: SpawnContext) -> Self {
        let (tx, mut rx) = mpsc::channel::<SpawnJob>(128);
        let worker_ctx = ctx.clone();

        // The worker loop is a single point of failure for ALL child spawning:
        // if it unwinds, queued jobs are dropped with no completion published
        // and every later enqueue fails with "spawn scheduler is not running"
        // for the rest of the process lifetime. Run each job on its own task
        // and await the JoinHandle — a panicking job is isolated, keeps the
        // worker alive, and still publishes a terminal error completion so the
        // waiting parent is woken instead of stranded.
        tokio::spawn(async move {
            while let Some(job) = rx.recv().await {
                let job_ctx = worker_ctx.clone();
                let job_for_panic = job.clone();
                let handle = tokio::spawn(async move {
                    if let Err(err) = run_spawn_job(job_ctx, job).await {
                        tracing::warn!("spawn job failed: {}", err);
                    }
                });
                if let Err(join_error) = handle.await {
                    tracing::error!(
                        parent_session_id = %job_for_panic.parent_session_id,
                        child_session_id = %job_for_panic.child_session_id,
                        error = %join_error,
                        "spawn job panicked; publishing terminal error completion"
                    );
                    let parent_tx = super::session_events::get_or_create_event_sender(
                        &worker_ctx.session_event_senders,
                        &job_for_panic.parent_session_id,
                    )
                    .await;
                    publish_child_completion_parts(
                        &parent_tx,
                        worker_ctx.completion_handler.clone(),
                        job_for_panic.parent_session_id.clone(),
                        job_for_panic.child_session_id.clone(),
                        "error".to_string(),
                        Some(format!("child spawn panicked: {join_error}")),
                    )
                    .await;
                }
            }
        });

        Self { tx, ctx }
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
        let ctx = self.ctx.clone();
        let preparation_job = job.clone();
        reserve_prepare_and_send(&self.tx, job, async move {
            Self::prepare_child_launch(&ctx, &preparation_job).await;
        })
        .await
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
            let parent_tx = super::session_events::get_or_create_event_sender(
                &ctx.session_event_senders,
                &job.parent_session_id,
            )
            .await;
            let _ = parent_tx.send(AgentEvent::SubAgentStarted {
                parent_session_id: job.parent_session_id.clone(),
                child_session_id: job.child_session_id.clone(),
                title: None,
            });
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

/// Reserve queue capacity before polling observer setup. A closed scheduler
/// therefore cannot start a relay/observer for a child that will never launch.
async fn reserve_prepare_and_send(
    tx: &mpsc::Sender<SpawnJob>,
    job: SpawnJob,
    preparation: impl std::future::Future<Output = ()>,
) -> Result<(), String> {
    let permit = tx
        .reserve()
        .await
        .map_err(|_| "spawn scheduler is not running".to_string())?;
    preparation.await;
    permit.send(job);
    Ok(())
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
    parent_tx: &broadcast::Sender<AgentEvent>,
    completion_handler: Option<Arc<dyn ChildCompletionHandler>>,
    completion: ChildCompletion,
) {
    let _ = parent_tx.send(AgentEvent::SubAgentCompleted {
        parent_session_id: completion.parent_session_id.clone(),
        child_session_id: completion.child_session_id.clone(),
        status: completion.status.clone(),
        error: completion.error.clone(),
    });

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
    parent_tx: &broadcast::Sender<AgentEvent>,
    completion_handler: Option<Arc<dyn ChildCompletionHandler>>,
    parent_session_id: String,
    child_session_id: String,
    status: String,
    error: Option<String>,
) {
    publish_child_completion(
        parent_tx,
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

                let last_activity_at = runner.last_event_at.unwrap_or(runner.started_at);
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
async fn run_spawn_job(ctx: SpawnContext, job: SpawnJob) -> Result<(), String> {
    crate::sdk::spawn::run_child_spawn(ctx, job).await
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
        let preparations = Arc::new(AtomicUsize::new(0));
        let preparations_for_future = preparations.clone();

        let result = reserve_prepare_and_send(&tx, job(), async move {
            preparations_for_future.fetch_add(1, Ordering::SeqCst);
        })
        .await;

        assert_eq!(result.unwrap_err(), "spawn scheduler is not running");
        assert_eq!(preparations.load(Ordering::SeqCst), 0);
    }
}
