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
use bamboo_agent_core::{AgentEvent, Role, Session, SessionKind};
use bamboo_domain::ProviderModelRef;
use bamboo_infrastructure::{LLMProvider, ProviderModelRouter};

use crate::runtime::Agent;
use crate::runtime::ExecuteRequest;

use super::child_completion::{ChildCompletion, ChildCompletionHandler};
use super::event_forwarder::create_event_forwarder;
use super::runner_lifecycle::{finalize_runner, try_reserve_runner, RunnerReservation};
use super::runner_state::AgentRunner;
use super::session_events::get_or_create_event_sender;

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
}

#[derive(Clone)]
pub struct SpawnContext {
    pub agent: Arc<Agent>,
    pub tools: Arc<dyn ToolExecutor>,
    pub sessions_cache: Arc<RwLock<HashMap<String, Session>>>,
    pub agent_runners: Arc<RwLock<HashMap<String, AgentRunner>>>,
    pub session_event_senders: Arc<RwLock<HashMap<String, broadcast::Sender<AgentEvent>>>>,
    pub external_child_runner: Option<Arc<dyn ExternalChildRunner>>,
    pub provider_router: Option<Arc<ProviderModelRouter>>,
    pub app_data_dir: Option<std::path::PathBuf>,
    /// Optional application-layer completion hook. The engine still emits
    /// `SubAgentCompleted` to the parent stream itself; this hook lets the
    /// server persist parent wait state and resume the parent runner without
    /// introducing an engine -> AppState dependency.
    pub completion_handler: Option<Arc<dyn ChildCompletionHandler>>,
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

fn child_model_ref(session: &Session, model: &str) -> Option<ProviderModelRef> {
    if let Some(model_ref) = session.model_ref.clone() {
        let provider = model_ref.provider.trim();
        let model_name = model_ref.model.trim();
        if !provider.is_empty() && !model_name.is_empty() {
            return Some(ProviderModelRef::new(provider, model_name));
        }
    }

    let provider = session
        .metadata
        .get("provider_name")
        .map(String::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())?;
    let model_name = model.trim();
    if model_name.is_empty() {
        return None;
    }
    Some(ProviderModelRef::new(provider, model_name))
}

#[derive(Debug, Clone, Copy)]
struct ChildWatchdogPolicy {
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

fn watchdog_policy_for_session(session: &Session) -> ChildWatchdogPolicy {
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

async fn publish_child_completion_parts(
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

async fn watch_child_liveness(
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

fn resolve_child_provider_override(
    router: Option<&Arc<ProviderModelRouter>>,
    session: &Session,
    model: &str,
) -> (Option<Arc<dyn LLMProvider>>, Option<String>, Option<String>) {
    let model_ref = child_model_ref(session, model);
    let provider_name = model_ref
        .as_ref()
        .map(|model_ref| model_ref.provider.clone());
    let provider_type = if let (Some(router), Some(model_ref)) = (router, model_ref.as_ref()) {
        router.provider_type_for(model_ref)
    } else {
        provider_name.clone()
    };
    let provider = router.and_then(|router| {
        let model_ref = model_ref.as_ref()?;
        match router.route(model_ref) {
            Ok(provider) => Some(provider),
            Err(error) => {
                tracing::warn!(
                    session_id = %session.id,
                    provider = %model_ref.provider,
                    model = %model_ref.model,
                    error = %error,
                    "failed to resolve provider override for child session; falling back to runtime provider"
                );
                None
            }
        }
    });
    (provider, provider_name, provider_type)
}

async fn run_spawn_job(ctx: SpawnContext, job: SpawnJob) -> Result<(), String> {
    // Ensure both session event streams exist.
    let parent_tx =
        get_or_create_event_sender(&ctx.session_event_senders, &job.parent_session_id).await;
    let child_tx =
        get_or_create_event_sender(&ctx.session_event_senders, &job.child_session_id).await;

    // Load child session.
    let mut session = match ctx
        .agent
        .storage()
        .load_session(&job.child_session_id)
        .await
    {
        Ok(Some(s)) => s,
        Ok(None) => {
            let error = "child session not found".to_string();
            publish_child_completion_parts(
                &parent_tx,
                ctx.completion_handler.clone(),
                job.parent_session_id.clone(),
                job.child_session_id.clone(),
                "error".to_string(),
                Some(error.clone()),
            )
            .await;
            return Err(error);
        }
        Err(e) => {
            let error = format!("failed to load child session: {e}");
            publish_child_completion_parts(
                &parent_tx,
                ctx.completion_handler.clone(),
                job.parent_session_id.clone(),
                job.child_session_id.clone(),
                "error".to_string(),
                Some(error.clone()),
            )
            .await;
            return Err(error);
        }
    };

    // Register the child's workspace in the global state so tools
    // (Read, Glob, Grep, Bash, etc.) can resolve relative paths.
    if let Some(ref ws) = session.workspace {
        bamboo_agent_core::workspace_state::set_workspace(
            &session.id,
            std::path::PathBuf::from(ws),
        );
    }

    if session.kind != SessionKind::Child {
        let error = "spawn job child session is not kind=child".to_string();
        publish_child_completion_parts(
            &parent_tx,
            ctx.completion_handler.clone(),
            job.parent_session_id.clone(),
            job.child_session_id.clone(),
            "error".to_string(),
            Some(error.clone()),
        )
        .await;
        return Err(error);
    }

    // Ensure last message is user (otherwise nothing to do).
    let last_is_user = session
        .messages
        .last()
        .map(|m| matches!(m.role, Role::User))
        .unwrap_or(false);
    if !last_is_user {
        session
            .metadata
            .insert("last_run_status".to_string(), "skipped".to_string());
        session.metadata.insert(
            "last_run_error".to_string(),
            "No pending message to execute".to_string(),
        );
        let _ = ctx
            .agent
            .persistence()
            .save_runtime_session(&mut session)
            .await;
        {
            let mut sessions = ctx.sessions_cache.write().await;
            sessions.insert(job.child_session_id.clone(), session);
        }
        publish_child_completion_parts(
            &parent_tx,
            ctx.completion_handler.clone(),
            job.parent_session_id.clone(),
            job.child_session_id.clone(),
            "skipped".to_string(),
            Some("No pending message to execute".to_string()),
        )
        .await;
        return Ok(());
    }

    // Persist a running marker early so list_sessions can reconstruct status.
    session
        .metadata
        .insert("last_run_status".to_string(), "running".to_string());
    session.metadata.remove("last_run_error");
    let _ = ctx
        .agent
        .persistence()
        .save_runtime_session(&mut session)
        .await;

    // Insert runner status.
    let Some(RunnerReservation { cancel_token, .. }) =
        try_reserve_runner(&ctx.agent_runners, &job.child_session_id, &child_tx).await
    else {
        return Ok(());
    };

    // Forward ALL child events to parent.
    let forwarder_done = CancellationToken::new();
    {
        let mut rx = child_tx.subscribe();
        let parent_tx = parent_tx.clone();
        let job_clone = job.clone();
        let done = forwarder_done.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = done.cancelled() => break,
                    evt = rx.recv() => {
                        match evt {
                            Ok(event) => {
                                let _ = parent_tx.send(AgentEvent::SubAgentEvent {
                                    parent_session_id: job_clone.parent_session_id.clone(),
                                    child_session_id: job_clone.child_session_id.clone(),
                                    event: Box::new(event),
                                });
                            }
                            Err(broadcast::error::RecvError::Lagged(_)) => {
                                continue;
                            }
                            Err(_) => break,
                        }
                    }
                }
            }
        });
    }
    {
        let parent_tx = parent_tx.clone();
        let job_clone = job.clone();
        let done = forwarder_done.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(5));
            loop {
                tokio::select! {
                    _ = done.cancelled() => break,
                    _ = ticker.tick() => {
                        let _ = parent_tx.send(AgentEvent::SubAgentHeartbeat {
                            parent_session_id: job_clone.parent_session_id.clone(),
                            child_session_id: job_clone.child_session_id.clone(),
                            timestamp: Utc::now(),
                        });
                    }
                }
            }
        });
    }

    // Create mpsc channel for agent loop → session events sender.
    let (mpsc_tx, _forwarder_handle) = create_event_forwarder(
        job.child_session_id.clone(),
        child_tx.clone(),
        ctx.agent_runners.clone(),
    );

    // Child liveness is owned by the child runner. The parent wait state can
    // have a longer lease, but it should not poll or terminate children.
    let timeout_reason = Arc::new(RwLock::new(None::<String>));
    let watchdog_policy = watchdog_policy_for_session(&session);
    tokio::spawn(watch_child_liveness(
        job.parent_session_id.clone(),
        job.child_session_id.clone(),
        ctx.agent_runners.clone(),
        cancel_token.clone(),
        timeout_reason.clone(),
        forwarder_done.clone(),
        watchdog_policy,
    ));

    // Run child loop via unified spawn_session_execution.
    let model = job.model.clone();
    let session_id_clone = job.child_session_id.clone();
    let agent_runners_for_status = ctx.agent_runners.clone();
    let sessions_cache = ctx.sessions_cache.clone();
    let agent = ctx.agent.clone();
    let tools = ctx.tools.clone();
    let external_runner = ctx.external_child_runner.clone();
    let done = forwarder_done.clone();
    let parent_tx_for_done = parent_tx.clone();
    let parent_id_for_done = job.parent_session_id.clone();
    let child_id_for_done = job.child_session_id.clone();
    let session_event_senders = ctx.session_event_senders.clone();
    let provider_router = ctx.provider_router.clone();
    let completion_handler = ctx.completion_handler.clone();

    tokio::spawn(async move {
        session.model = model.clone();

        let wants_external = session
            .metadata
            .get("runtime.kind")
            .is_some_and(|v| v == "external");

        let result: crate::runtime::runner::Result<()> = if wants_external {
            if let Some(runner) = external_runner {
                if runner.should_handle(&session).await {
                    runner
                        .execute_external_child(&mut session, &job, mpsc_tx, cancel_token.clone())
                        .await
                } else {
                    Err(bamboo_agent_core::AgentError::LLM(format!(
                        "No external runner matched child session runtime metadata: agent_id={:?}, protocol={:?}",
                        session.metadata.get("external.agent_id"),
                        session.metadata.get("external.protocol"),
                    )))
                }
            } else {
                Err(bamboo_agent_core::AgentError::LLM(
                    "Child session requires external runtime, but no external runner is configured"
                        .to_string(),
                ))
            }
        } else {
            let (provider_override, provider_name, provider_type) =
                resolve_child_provider_override(provider_router.as_ref(), &session, &model);
            let disabled_tools: Option<std::collections::BTreeSet<String>> =
                job.disabled_tools.map(|v| v.into_iter().collect());
            agent
                .execute(
                    &mut session,
                    ExecuteRequest {
                        initial_message: String::new(), // handled by agent loop
                        event_tx: mpsc_tx,
                        cancel_token: cancel_token.clone(),
                        tools: Some(tools),
                        provider_override,
                        model: Some(model.clone()),
                        provider_name,
                        provider_type,
                        fast_model: None,
                        fast_model_provider: None,
                        background_model: None,
                        background_model_provider: None,
                        summarization_model: None,
                        summarization_model_provider: None,
                        reasoning_effort: None,
                        disabled_tools,
                        disabled_skill_ids: None,
                        selected_skill_ids: None,
                        selected_skill_mode: None,
                        image_fallback: None,
                        app_data_dir: ctx.app_data_dir.clone(),
                    },
                )
                .await
        };

        let timeout_error = timeout_reason.read().await.clone();
        let (status, error) = if let Some(reason) = timeout_error {
            ("timeout".to_string(), Some(reason))
        } else {
            match &result {
                Ok(_) => ("completed".to_string(), None),
                Err(e) if e.to_string().contains("cancelled") => {
                    ("cancelled".to_string(), Some(e.to_string()))
                }
                Err(e) => ("error".to_string(), Some(e.to_string())),
            }
        };

        finalize_runner(&agent_runners_for_status, &session_id_clone, &result).await;

        // Merge any queued injected messages that the pipeline didn't pick up
        // (e.g. if the loop exited before the next turn boundary).
        crate::runtime::runner::state_bridge::merge_pending_injected_messages(
            &mut session,
            Some(agent.storage()),
            Some(agent.persistence()),
        )
        .await;

        // Persist final session snapshot.
        session
            .metadata
            .insert("last_run_status".to_string(), status.clone());
        if let Some(err) = &error {
            session
                .metadata
                .insert("last_run_error".to_string(), err.clone());
        } else {
            session.metadata.remove("last_run_error");
        }
        let _ = agent.persistence().save_runtime_session(&mut session).await;
        {
            let mut sessions = sessions_cache.write().await;
            sessions.insert(session_id_clone.clone(), session);
        }

        // Stop forwarding/heartbeats and emit terminal child status through the
        // same durable completion path used by success/error/cancel/timeout.
        done.cancel();
        publish_child_completion_parts(
            &parent_tx_for_done,
            completion_handler,
            parent_id_for_done,
            child_id_for_done,
            status,
            error,
        )
        .await;

        // Allow dead code: session_event_senders keeps the sender alive during the task.
        drop(session_event_senders);
    });

    Ok(())
}
