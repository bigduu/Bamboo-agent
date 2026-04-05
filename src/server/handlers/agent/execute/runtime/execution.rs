use std::collections::BTreeSet;
use std::sync::Arc;

use chrono::Utc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use crate::{
    agent::{
        core::{AgentEvent, Session},
        loop_module::{run_agent_loop_with_config, AgentLoopConfig, ImageFallbackConfig},
    },
    core::ReasoningEffort,
    server::app_state::{AgentStatus, AppState},
};

use super::session_state::{
    initial_user_message_for_session, selected_skill_ids_for_session,
    selected_skill_mode_for_session, system_prompt_for_session,
};

const SKILL_CONTEXT_START_MARKER: &str = "<!-- BAMBOO_SKILL_CONTEXT_START -->";
const TOOL_GUIDE_START_MARKER: &str = "<!-- BAMBOO_TOOL_GUIDE_START -->";
const EXTERNAL_MEMORY_START_MARKER: &str = "<!-- BAMBOO_EXTERNAL_MEMORY_START -->";
const TASK_LIST_START_MARKER: &str = "<!-- BAMBOO_TASK_LIST_START -->";

pub(in crate::server::handlers::agent::execute) struct SpawnAgentExecution {
    pub(in crate::server::handlers::agent::execute) state: actix_web::web::Data<AppState>,
    pub(in crate::server::handlers::agent::execute) session_id: String,
    pub(in crate::server::handlers::agent::execute) session: Session,
    pub(in crate::server::handlers::agent::execute) is_child_session: bool,
    pub(in crate::server::handlers::agent::execute) provider_name: String,
    pub(in crate::server::handlers::agent::execute) model: String,
    /// Fast/cheap model for lightweight tasks (summarization, task evaluation).
    /// Falls back to `model` at the agent loop level when None.
    pub(in crate::server::handlers::agent::execute) fast_model: Option<String>,
    pub(in crate::server::handlers::agent::execute) reasoning_effort: Option<ReasoningEffort>,
    pub(in crate::server::handlers::agent::execute) reasoning_effort_source: String,
    pub(in crate::server::handlers::agent::execute) disabled_tools: BTreeSet<String>,
    pub(in crate::server::handlers::agent::execute) disabled_skill_ids: BTreeSet<String>,
    pub(in crate::server::handlers::agent::execute) cancel_token: CancellationToken,
    pub(in crate::server::handlers::agent::execute) mpsc_tx: mpsc::Sender<AgentEvent>,
    pub(in crate::server::handlers::agent::execute) image_fallback: Option<ImageFallbackConfig>,
}

pub(in crate::server::handlers::agent::execute) fn spawn_agent_execution(
    args: SpawnAgentExecution,
) {
    let span_session_id = args.session_id.clone();
    let session_span = tracing::info_span!("agent_execution", session_id = %span_session_id);

    tokio::spawn(
        async move {
            let SpawnAgentExecution {
                state,
                session_id,
                mut session,
                is_child_session,
                provider_name,
                model,
                fast_model,
                reasoning_effort,
                reasoning_effort_source,
                disabled_tools,
                disabled_skill_ids,
                cancel_token,
                mpsc_tx,
                image_fallback,
            } = args;
            let initial_title = session.title.clone();
            let initial_pinned = session.pinned;

            let system_prompt = system_prompt_for_session(&session);
            let initial_message = initial_user_message_for_session(&session);
            let selected_skill_ids = selected_skill_ids_for_session(&session);
            let selected_skill_mode = selected_skill_mode_for_session(&session);

            // Use child tool set for child sessions (no spawn schemas), otherwise root tools.
            let tools = if is_child_session {
                state.child_tools.clone()
            } else {
                state.tools.clone()
            };

            // Execute is session-driven. The resolved model/reasoning have already been
            // materialized onto the session before spawning this runtime.
            tracing::info!(
                "[{}] Using resolved session model: {}, reasoning_effort={}, reasoning_source={}",
                session_id,
                model,
                reasoning_effort
                    .map(crate::core::ReasoningEffort::as_str)
                    .unwrap_or("none"),
                reasoning_effort_source
            );

            // Keep session.model aligned with the resolved model for persistence/debugging.
            session.model = model.clone();

            if let Some(prompt) = system_prompt.as_ref() {
                log_base_system_prompt_snapshot(&session_id, prompt);
            }

            // Run agent loop.
            let storage: Arc<dyn crate::agent::core::storage::Storage> = state.storage.clone();
            let result = run_agent_loop_with_config(
                &mut session,
                initial_message,
                mpsc_tx.clone(),
                // Use the reloadable provider handle so config/provider switches take effect
                // without requiring a server restart.
                state.get_provider().await,
                tools,
                cancel_token,
                AgentLoopConfig {
                    max_rounds: 200,
                    system_prompt,
                    disabled_skill_ids,
                    selected_skill_ids,
                    selected_skill_mode,
                    skill_manager: Some(state.skill_manager.clone()),
                    skip_initial_user_message: true,
                    storage: Some(storage),
                    attachment_reader: Some(state.session_store.clone()),
                    metrics_collector: Some(state.metrics_service.collector()),
                    model_name: Some(model),
                    fast_model_name: state.config.read().await.get_fast_model(),
                    background_model_name: fast_model,
                    provider_name: Some(provider_name),
                    reasoning_effort,
                    disabled_tools,
                    image_fallback,
                    ..Default::default()
                },
            )
            .await;

            // Send terminal event for all error cases (including cancellation).
            if let Some(error_event) = terminal_error_event_for_result(&result) {
                let _ = mpsc_tx.send(error_event).await;
            }

            // Update runner status.
            {
                let mut runners = state.agent_runners.write().await;
                if let Some(runner) = runners.get_mut(&session_id) {
                    runner.status = status_from_execution_result(&result);
                    runner.completed_at = Some(Utc::now());
                }
            }

            // Avoid clobbering concurrent UI edits (title/pin) that may arrive while
            // the agent loop is running. If this execution never touched those fields,
            // preserve the latest persisted values.
            match state.storage.load_session(&session_id).await {
                Ok(Some(latest_persisted)) => preserve_concurrent_session_overrides(
                    &mut session,
                    &latest_persisted,
                    &initial_title,
                    initial_pinned,
                ),
                Ok(None) => {}
                Err(error) => {
                    tracing::warn!(
                        "[{}] Failed to load latest session before final save: {}",
                        session_id,
                        error
                    );
                }
            }

            // Save session.
            state.save_session(&session).await;

            // Update memory.
            {
                let mut sessions = state.sessions.write().await;
                sessions.insert(session_id.clone(), session);
            }

            // Remove cancellation token (legacy).
            {
                let mut tokens = state.cancel_tokens.write().await;
                tokens.remove(&session_id);
            }

            tracing::info!("[{}] Agent execution completed", session_id);
        }
        .instrument(session_span),
    );
}

fn log_base_system_prompt_snapshot(session_id: &str, prompt: &str) {
    tracing::info!(
        "[{}] Base system prompt snapshot: len={} chars, has_skill={}, has_tool_guide={}, has_external_memory={}, has_task_list={}",
        session_id,
        prompt.len(),
        prompt.contains(SKILL_CONTEXT_START_MARKER),
        prompt.contains(TOOL_GUIDE_START_MARKER),
        prompt.contains(EXTERNAL_MEMORY_START_MARKER),
        prompt.contains(TASK_LIST_START_MARKER),
    );

    tracing::debug!(
        "[{}] ========== BASE SYSTEM PROMPT SNAPSHOT ==========",
        session_id
    );
    tracing::debug!("[{}] Snapshot length: {} chars", session_id, prompt.len());
    tracing::debug!("[{}] -----------------------------------", session_id);
    tracing::debug!("[{}] {}", session_id, prompt);
    tracing::debug!(
        "[{}] ========== END BASE SYSTEM PROMPT SNAPSHOT ==========",
        session_id
    );
}

pub(super) fn preserve_concurrent_session_overrides(
    session: &mut Session,
    latest_persisted: &Session,
    initial_title: &str,
    initial_pinned: bool,
) {
    // Preserve user/system edits made via PATCH /sessions/{id} during execution.
    if session.title == initial_title {
        session.title = latest_persisted.title.clone();
    }
    if session.pinned == initial_pinned {
        session.pinned = latest_persisted.pinned;
    }
}

pub(super) fn terminal_error_event_for_result<E>(result: &Result<(), E>) -> Option<AgentEvent>
where
    E: std::fmt::Display,
{
    match result {
        Ok(_) => None,
        Err(error) if is_cancelled_error(error) => Some(AgentEvent::Error {
            message: "Agent execution cancelled by user".to_string(),
        }),
        Err(error) => Some(AgentEvent::Error {
            message: error.to_string(),
        }),
    }
}

pub(super) fn status_from_execution_result<E>(result: &Result<(), E>) -> AgentStatus
where
    E: std::fmt::Display,
{
    match result {
        Ok(_) => AgentStatus::Completed,
        Err(error) if is_cancelled_error(error) => AgentStatus::Cancelled,
        Err(error) => AgentStatus::Error(error.to_string()),
    }
}

pub(super) fn is_cancelled_error<E>(error: &E) -> bool
where
    E: std::fmt::Display,
{
    error.to_string().contains("cancelled")
}
