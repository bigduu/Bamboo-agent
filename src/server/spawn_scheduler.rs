//! Sub-session spawn scheduler (async background jobs).
//!
//! This module provides a small background queue for spawning child sessions.
//! The key requirement is: spawn is async (tool returns immediately), but the UI
//! can observe child progress via events forwarded to the parent session stream.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tokio::sync::{broadcast, mpsc, RwLock};
use tokio_util::sync::CancellationToken;

use crate::agent::core::storage::SessionStoreV2;
use crate::agent::core::storage::Storage;
use crate::agent::core::tools::ToolExecutor;
use crate::agent::core::{AgentEvent, Role, Session, SessionKind};
use crate::agent::llm::LLMProvider;
use crate::agent::loop_module::{run_agent_loop_with_config, AgentLoopConfig};
use crate::agent::metrics::MetricsCollector;
use crate::agent::skill::SkillManager;
use crate::core::Config;
use crate::server::app_state::{AgentRunner, AgentStatus};

#[derive(Debug, Clone)]
pub struct SpawnJob {
    pub parent_session_id: String,
    pub child_session_id: String,
    pub model: String,
}

#[derive(Clone)]
pub struct SpawnContext {
    pub session_store: Arc<SessionStoreV2>,
    pub storage: Arc<dyn Storage>,
    pub provider: Arc<dyn LLMProvider>,
    pub tools: Arc<dyn ToolExecutor>,
    pub config: Arc<RwLock<Config>>,
    pub skill_manager: Arc<SkillManager>,
    pub metrics_collector: MetricsCollector,
    pub sessions_cache: Arc<RwLock<HashMap<String, Session>>>,
    pub agent_runners: Arc<RwLock<HashMap<String, AgentRunner>>>,
    pub session_event_senders: Arc<RwLock<HashMap<String, broadcast::Sender<AgentEvent>>>>,
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

async fn get_or_create_sender(
    senders: &Arc<RwLock<HashMap<String, broadcast::Sender<AgentEvent>>>>,
    session_id: &str,
) -> broadcast::Sender<AgentEvent> {
    let mut guard = senders.write().await;
    if let Some(existing) = guard.get(session_id) {
        return existing.clone();
    }
    let (tx, _) = broadcast::channel(1000);
    guard.insert(session_id.to_string(), tx.clone());
    tx
}

async fn run_spawn_job(ctx: SpawnContext, job: SpawnJob) -> Result<(), String> {
    // Ensure both session event streams exist.
    let parent_tx = get_or_create_sender(&ctx.session_event_senders, &job.parent_session_id).await;
    let child_tx = get_or_create_sender(&ctx.session_event_senders, &job.child_session_id).await;

    let emit_error_completion = |error: String| {
        let _ = parent_tx.send(AgentEvent::SubSessionCompleted {
            parent_session_id: job.parent_session_id.clone(),
            child_session_id: job.child_session_id.clone(),
            status: "error".to_string(),
            error: Some(error.clone()),
        });
        error
    };

    // Start child execution if not already running.
    // Load child session.
    let mut session = match ctx.storage.load_session(&job.child_session_id).await {
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
        let _ = ctx.storage.save_session(&session).await;
        let _ = parent_tx.send(AgentEvent::SubSessionCompleted {
            parent_session_id: job.parent_session_id.clone(),
            child_session_id: job.child_session_id.clone(),
            status: "skipped".to_string(),
            error: Some("No pending message to execute".to_string()),
        });
        return Ok(());
    }

    // Persist a running marker early so list_sessions can reconstruct status
    // even if the parent UI briefly misses streamed events.
    session
        .metadata
        .insert("last_run_status".to_string(), "running".to_string());
    session.metadata.remove("last_run_error");
    let _ = ctx.storage.save_session(&session).await;

    // Insert runner status (for cancellation/status introspection).
    let cancel_token = {
        let mut runners = ctx.agent_runners.write().await;
        if let Some(runner) = runners.get(&job.child_session_id) {
            if matches!(runner.status, AgentStatus::Running) {
                return Ok(());
            }
        }

        runners.remove(&job.child_session_id);
        let mut runner = AgentRunner::new();
        runner.status = AgentStatus::Running;
        // Use the stable session event sender for this session id.
        runner.event_sender = child_tx.clone();
        let cancel_token = runner.cancel_token.clone();
        runners.insert(job.child_session_id.clone(), runner);
        cancel_token
    };

    // Forward ALL child events to parent, wrapped so the UI can demux.
    // Also emit a heartbeat while the child is running.
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
                                // Drop lagged events; UI can open the child session to see full history.
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

    // Create mpsc channel for agent loop -> session events sender.
    let (mpsc_tx, mut mpsc_rx) = tokio::sync::mpsc::channel::<crate::agent::core::AgentEvent>(100);
    let child_tx_for_forwarder = child_tx.clone();
    let agent_runners_for_status = ctx.agent_runners.clone();
    let child_id_for_status = job.child_session_id.clone();
    tokio::spawn(async move {
        while let Some(event) = mpsc_rx.recv().await {
            // best-effort send to session stream
            let _ = child_tx_for_forwarder.send(event.clone());

            // store budget replay on runner (same behavior as execute handler)
            if matches!(
                &event,
                crate::agent::core::AgentEvent::TokenBudgetUpdated { .. }
            ) {
                let mut runners = agent_runners_for_status.write().await;
                if let Some(runner) = runners.get_mut(&child_id_for_status) {
                    runner.last_budget_event = Some(event);
                }
            }
        }
    });

    // Run child loop in background.
    let provider = ctx.provider.clone();
    let tools = ctx.tools.clone();
    let storage = ctx.storage.clone();
    let skill_manager = ctx.skill_manager.clone();
    let metrics = ctx.metrics_collector.clone();
    let attachment_reader = ctx.session_store.clone();
    let model = job.model.clone();
    let session_id_clone = job.child_session_id.clone();
    let agent_runners_for_status = ctx.agent_runners.clone();
    let sessions_cache = ctx.sessions_cache.clone();
    let persist = ctx.storage.clone();
    let done = forwarder_done.clone();
    let parent_tx_for_done = parent_tx.clone();
    let parent_id_for_done = job.parent_session_id.clone();
    let child_id_for_done = job.child_session_id.clone();

    tokio::spawn(async move {
        let system_prompt = session
            .messages
            .iter()
            .find(|m| matches!(m.role, Role::System))
            .map(|m| m.content.clone());

        let initial_message = session
            .messages
            .last()
            .filter(|m| matches!(m.role, Role::User))
            .map(|m| m.content.clone())
            .unwrap_or_default();

        let config_snapshot = ctx.config.read().await.clone();
        let disabled_tools = config_snapshot.disabled_tool_names();
        let provider_name = config_snapshot.provider.clone();

        session.model = model.clone();

        let result = run_agent_loop_with_config(
            &mut session,
            initial_message,
            mpsc_tx,
            provider,
            tools,
            cancel_token,
            AgentLoopConfig {
                max_rounds: 200,
                system_prompt,
                additional_tool_schemas: Vec::new(),
                skill_manager: Some(skill_manager),
                skip_initial_user_message: true,
                storage: Some(storage),
                attachment_reader: Some(attachment_reader),
                metrics_collector: Some(metrics),
                model_name: Some(model),
                provider_name: Some(provider_name),
                disabled_tools,
                ..Default::default()
            },
        )
        .await;

        let (status, error) = match &result {
            Ok(_) => ("completed".to_string(), None),
            Err(e) if e.to_string().contains("cancelled") => {
                ("cancelled".to_string(), Some(e.to_string()))
            }
            Err(e) => ("error".to_string(), Some(e.to_string())),
        };

        {
            let mut runners = agent_runners_for_status.write().await;
            if let Some(runner) = runners.get_mut(&session_id_clone) {
                runner.status = match result {
                    Ok(_) => AgentStatus::Completed,
                    Err(e) if e.to_string().contains("cancelled") => AgentStatus::Cancelled,
                    Err(e) => AgentStatus::Error(e.to_string()),
                };
                runner.completed_at = Some(Utc::now());
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
        let _ = persist.save_session(&session).await;
        {
            let mut sessions = sessions_cache.write().await;
            sessions.insert(session_id_clone.clone(), session);
        }

        // Stop forwarding/heartbeats and emit a terminal child status to the parent stream.
        done.cancel();
        let _ = parent_tx_for_done.send(AgentEvent::SubSessionCompleted {
            parent_session_id: parent_id_for_done,
            child_session_id: child_id_for_done,
            status,
            error,
        });
    });

    // NOTE: We intentionally do not wait here; the job is "started".
    // Completion is detected via forwarded child events.

    Ok(())
}
