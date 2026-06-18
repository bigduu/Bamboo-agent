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

use bamboo_agent_core::tools::ToolExecutor;
use bamboo_agent_core::{AgentEvent, Session};
use bamboo_llm::ProviderModelRouter;

use crate::runtime::Agent;

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
    /// Optional inbox to the account-wide change feed. When present, durable
    /// change events from child-session execution are mirrored onto the feed
    /// for resumable multi-client sync.
    pub account_feed_inbox: Option<super::event_forwarder::AccountFeedInbox>,
}

#[derive(Clone)]
pub struct SpawnScheduler {
    tx: mpsc::Sender<SpawnJob>,
}

impl SpawnScheduler {
    pub fn new(ctx: SpawnContext) -> Self {
        let (tx, mut rx) = mpsc::channel::<SpawnJob>(128);

        tokio::spawn(async move {
            while let Some(job) = rx.recv().await {
                if let Err(err) = run_spawn_job(ctx.clone(), job).await {
                    tracing::warn!("spawn job failed: {}", err);
                }
            }
        });

        Self { tx }
    }

    pub async fn enqueue(&self, job: SpawnJob) -> Result<(), String> {
        self.tx
            .send(job)
            .await
            .map_err(|_| "spawn scheduler is not running".to_string())
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ChildWatchdogPolicy {
    check_interval_secs: i64,
    max_total_secs: i64,
    max_idle_secs: i64,
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
        handler.on_child_completed(completion).await;
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
