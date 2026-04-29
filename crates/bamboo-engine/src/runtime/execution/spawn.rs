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

use super::event_forwarder::create_event_forwarder;
use super::runner_lifecycle::{finalize_runner, try_reserve_runner};
use super::runner_state::AgentRunner;
use super::session_events::get_or_create_event_sender;

#[derive(Debug, Clone)]
pub struct SpawnJob {
    pub parent_session_id: String,
    pub child_session_id: String,
    pub model: String,
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

fn resolve_child_provider_override(
    router: Option<&Arc<ProviderModelRouter>>,
    session: &Session,
    model: &str,
) -> (Option<Arc<dyn LLMProvider>>, Option<String>) {
    let model_ref = child_model_ref(session, model);
    let provider_name = model_ref
        .as_ref()
        .map(|model_ref| model_ref.provider.clone());
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
    (provider, provider_name)
}

async fn run_spawn_job(ctx: SpawnContext, job: SpawnJob) -> Result<(), String> {
    // Ensure both session event streams exist.
    let parent_tx =
        get_or_create_event_sender(&ctx.session_event_senders, &job.parent_session_id).await;
    let child_tx =
        get_or_create_event_sender(&ctx.session_event_senders, &job.child_session_id).await;

    let emit_error_completion = |error: String| {
        let _ = parent_tx.send(AgentEvent::SubSessionCompleted {
            parent_session_id: job.parent_session_id.clone(),
            child_session_id: job.child_session_id.clone(),
            status: "error".to_string(),
            error: Some(error.clone()),
        });
        error
    };

    // Load child session.
    let mut session = match ctx
        .agent
        .storage()
        .load_session(&job.child_session_id)
        .await
    {
        Ok(Some(s)) => s,
        Ok(None) => return Err(emit_error_completion("child session not found".to_string())),
        Err(e) => {
            return Err(emit_error_completion(format!(
                "failed to load child session: {e}"
            )))
        }
    };

    if session.kind != SessionKind::Child {
        return Err(emit_error_completion(
            "spawn job child session is not kind=child".to_string(),
        ));
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
        let _ = ctx.agent.storage().save_session(&session).await;
        let _ = parent_tx.send(AgentEvent::SubSessionCompleted {
            parent_session_id: job.parent_session_id.clone(),
            child_session_id: job.child_session_id.clone(),
            status: "skipped".to_string(),
            error: Some("No pending message to execute".to_string()),
        });
        return Ok(());
    }

    // Persist a running marker early so list_sessions can reconstruct status.
    session
        .metadata
        .insert("last_run_status".to_string(), "running".to_string());
    session.metadata.remove("last_run_error");
    let _ = ctx.agent.storage().save_session(&session).await;

    // Insert runner status.
    let Some(cancel_token) =
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
                                let _ = parent_tx.send(AgentEvent::SubSessionEvent {
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
                        let _ = parent_tx.send(AgentEvent::SubSessionHeartbeat {
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
                        .execute_external_child(&mut session, &job, mpsc_tx, cancel_token)
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
            let (provider_override, provider_name) =
                resolve_child_provider_override(provider_router.as_ref(), &session, &model);
            agent
                .execute(
                    &mut session,
                    ExecuteRequest {
                        initial_message: String::new(), // handled by agent loop
                        event_tx: mpsc_tx,
                        cancel_token,
                        tools: Some(tools),
                        provider_override,
                        model: Some(model.clone()),
                        provider_name,
                        background_model: None,
                        background_model_provider: None,
                        reasoning_effort: None,
                        disabled_tools: None,
                        disabled_skill_ids: None,
                        selected_skill_ids: None,
                        selected_skill_mode: None,
                        image_fallback: None,
                    },
                )
                .await
        };

        let (status, error) = match &result {
            Ok(_) => ("completed".to_string(), None),
            Err(e) if e.to_string().contains("cancelled") => {
                ("cancelled".to_string(), Some(e.to_string()))
            }
            Err(e) => ("error".to_string(), Some(e.to_string())),
        };

        finalize_runner(&agent_runners_for_status, &session_id_clone, &result).await;

        // Merge any queued injected messages that the pipeline didn't pick up
        // (e.g. if the loop exited before the next turn boundary).
        if let Ok(Some(latest)) = agent.storage().load_session(&session_id_clone).await {
            if let Some(raw) = latest.metadata.get("pending_injected_messages") {
                if let Ok(messages) = serde_json::from_str::<Vec<serde_json::Value>>(raw) {
                    for msg in messages {
                        if let Some(content) = msg.get("content").and_then(|v| v.as_str()) {
                            session
                                .add_message(bamboo_agent_core::Message::user(content.to_string()));
                        }
                    }
                    session.metadata.remove("pending_injected_messages");
                }
            }
        }

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
        let _ = agent.storage().save_session(&session).await;
        {
            let mut sessions = sessions_cache.write().await;
            sessions.insert(session_id_clone.clone(), session);
        }

        // Stop forwarding/heartbeats and emit terminal child status.
        done.cancel();
        let _ = parent_tx_for_done.send(AgentEvent::SubSessionCompleted {
            parent_session_id: parent_id_for_done,
            child_session_id: child_id_for_done,
            status,
            error,
        });

        // Allow dead code: session_event_senders keeps the sender alive during the task.
        drop(session_event_senders);
    });

    Ok(())
}
