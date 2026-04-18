//! Core agent execution spawning logic.
//!
//! Provides [`spawn_session_execution`] which handles the full lifecycle of a
//! background agent run: spawn task → execute → finalize runner → persist session.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use tokio::sync::{mpsc, RwLock};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use bamboo_application_agent::tools::ToolExecutor;
use bamboo_application_agent::{AgentEvent, Session};
use bamboo_shared_types::ReasoningEffort;

use crate::config::ImageFallbackConfig;
use crate::execution::runner_lifecycle::finalize_runner;
use crate::execution::runner_state::AgentRunner;
use crate::runtime::ExecuteRequest;
use crate::Agent;

const SKILL_CONTEXT_START_MARKER: &str = "<!-- BAMBOO_SKILL_CONTEXT_START -->";
const TOOL_GUIDE_START_MARKER: &str = "<!-- BAMBOO_TOOL_GUIDE_START -->";
const EXTERNAL_MEMORY_START_MARKER: &str = "<!-- BAMBOO_EXTERNAL_MEMORY_START -->";
const TASK_LIST_START_MARKER: &str = "<!-- BAMBOO_TASK_LIST_START -->";

/// Arguments for spawning a background agent execution.
///
/// This is the crate-agnostic equivalent of the server's `SpawnAgentExecution`.
/// It holds everything needed to run the agent loop and persist the result,
/// without depending on HTTP types or `AppState`.
pub struct SessionExecutionArgs {
    // Core execution.
    pub agent: Arc<Agent>,
    pub session_id: String,
    pub session: Session,

    // Execution parameters.
    pub tools_override: Option<Arc<dyn ToolExecutor>>,
    pub provider_name: Option<String>,
    pub model: String,
    pub fast_model: Option<String>,
    pub reasoning_effort: Option<ReasoningEffort>,
    pub reasoning_effort_source: String,
    pub disabled_tools: Option<BTreeSet<String>>,
    pub disabled_skill_ids: Option<BTreeSet<String>>,
    pub selected_skill_ids: Option<Vec<String>>,
    pub selected_skill_mode: Option<String>,
    pub cancel_token: CancellationToken,
    pub mpsc_tx: mpsc::Sender<AgentEvent>,
    pub image_fallback: Option<ImageFallbackConfig>,

    // Post-execution resources.
    pub runners: Arc<RwLock<HashMap<String, AgentRunner>>>,
    pub sessions_cache: Arc<RwLock<HashMap<String, Session>>>,
}

/// Spawn a background agent execution task.
///
/// This function spawns a tokio task that:
/// 1. Executes the agent loop via `agent.execute()`
/// 2. Sends a terminal error event if the execution fails
/// 3. Finalizes the runner status
/// 4. Checks for concurrent session overrides (title/pin edits)
/// 5. Persists the session via storage
/// 6. Updates the in-memory session cache
pub fn spawn_session_execution(args: SessionExecutionArgs) {
    let span_session_id = args.session_id.clone();
    let session_span = tracing::info_span!("agent_execution", session_id = %span_session_id);

    tokio::spawn(
        async move {
            let SessionExecutionArgs {
                agent,
                session_id,
                mut session,
                tools_override,
                provider_name,
                model,
                fast_model,
                reasoning_effort,
                reasoning_effort_source,
                disabled_tools,
                disabled_skill_ids,
                selected_skill_ids,
                selected_skill_mode,
                cancel_token,
                mpsc_tx,
                image_fallback,
                runners,
                sessions_cache,
            } = args;
            let initial_title = session.title.clone();
            let initial_pinned = session.pinned;

            let initial_message = initial_user_message_for_session(&session);
            let selected_skill_ids = selected_skill_ids
                .or_else(|| selected_skill_ids_for_session(&session));
            let selected_skill_mode = selected_skill_mode
                .or_else(|| selected_skill_mode_for_session(&session));

            tracing::info!(
                "[{}] Using resolved session model: {}, reasoning_effort={}, reasoning_source={}",
                session_id,
                model,
                reasoning_effort
                    .map(ReasoningEffort::as_str)
                    .unwrap_or("none"),
                reasoning_effort_source
            );

            session.model = model.clone();

            let system_prompt = system_prompt_for_session(&session);
            if let Some(prompt) = system_prompt.as_ref() {
                log_base_system_prompt_snapshot(&session_id, prompt);
            }

            let result = agent
                .execute(
                    &mut session,
                    ExecuteRequest {
                        initial_message,
                        event_tx: mpsc_tx.clone(),
                        cancel_token,
                        tools: tools_override,
                        model: Some(model),
                        provider_name,
                        background_model: fast_model,
                        reasoning_effort,
                        disabled_tools,
                        disabled_skill_ids,
                        selected_skill_ids,
                        selected_skill_mode,
                        image_fallback,
                    },
                )
                .await;

            // Send terminal event for all error cases (including cancellation).
            if let Some(error_event) = terminal_error_event_for_result(&result) {
                let _ = mpsc_tx.send(error_event).await;
            }

            // Update runner status.
            finalize_runner(&runners, &session_id, &result).await;

            // Avoid clobbering concurrent UI edits (title/pin).
            match agent.storage().load_session(&session_id).await {
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
            if let Err(error) = agent.storage().save_session(&session).await {
                tracing::warn!("[{}] Failed to save session: {}", session_id, error);
            }

            // Update memory cache.
            {
                let mut sessions = sessions_cache.write().await;
                sessions.insert(session_id.clone(), session);
            }

            tracing::info!("[{}] Agent execution completed", session_id);
        }
        .instrument(session_span),
    );
}

/// Log a snapshot of the base system prompt for debugging.
pub fn log_base_system_prompt_snapshot(session_id: &str, prompt: &str) {
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

/// Preserve concurrent session overrides (title/pin) made via external edits
/// while the agent loop was running.
pub fn preserve_concurrent_session_overrides(
    session: &mut Session,
    latest_persisted: &Session,
    initial_title: &str,
    initial_pinned: bool,
) {
    if session.title == initial_title {
        session.title = latest_persisted.title.clone();
    }
    if session.pinned == initial_pinned {
        session.pinned = latest_persisted.pinned;
    }
}

/// Map an execution result to a terminal error event.
pub fn terminal_error_event_for_result<E>(result: &Result<(), E>) -> Option<AgentEvent>
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

fn is_cancelled_error<E>(error: &E) -> bool
where
    E: std::fmt::Display,
{
    error.to_string().contains("cancelled")
}

// Session metadata helpers (pure functions, no server dependency).

fn system_prompt_for_session(session: &Session) -> Option<String> {
    session
        .messages
        .iter()
        .find(|message| matches!(message.role, bamboo_application_agent::Role::System))
        .map(|message| message.content.clone())
}

fn initial_user_message_for_session(session: &Session) -> String {
    session
        .messages
        .last()
        .filter(|message| matches!(message.role, bamboo_application_agent::Role::User))
        .map(|message| message.content.clone())
        .unwrap_or_default()
}

fn selected_skill_ids_for_session(session: &Session) -> Option<Vec<String>> {
    session
        .metadata
        .get("selected_skill_ids")
        .and_then(|raw| bamboo_application_skills::selection::parse_selected_skill_ids_metadata(raw))
}

fn selected_skill_mode_for_session(session: &Session) -> Option<String> {
    let value = session
        .metadata
        .get("skill_mode")
        .or_else(|| session.metadata.get("mode"))?;
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}
