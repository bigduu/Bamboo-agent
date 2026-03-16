use std::sync::Arc;

use chrono::Utc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    agent::{
        core::{AgentEvent, Session},
        loop_module::{run_agent_loop_with_config, AgentLoopConfig, ImageFallbackConfig},
    },
    core::ReasoningEffort,
    server::app_state::{AgentStatus, AppState},
};

use super::session_state::{initial_user_message_for_session, system_prompt_for_session};

pub(in crate::server::handlers::agent::execute) struct SpawnAgentExecution {
    pub(in crate::server::handlers::agent::execute) state: actix_web::web::Data<AppState>,
    pub(in crate::server::handlers::agent::execute) session_id: String,
    pub(in crate::server::handlers::agent::execute) session: Session,
    pub(in crate::server::handlers::agent::execute) is_child_session: bool,
    pub(in crate::server::handlers::agent::execute) model: String,
    pub(in crate::server::handlers::agent::execute) reasoning_effort: Option<ReasoningEffort>,
    pub(in crate::server::handlers::agent::execute) reasoning_effort_source: String,
    pub(in crate::server::handlers::agent::execute) cancel_token: CancellationToken,
    pub(in crate::server::handlers::agent::execute) mpsc_tx: mpsc::Sender<AgentEvent>,
    pub(in crate::server::handlers::agent::execute) image_fallback: Option<ImageFallbackConfig>,
}

pub(in crate::server::handlers::agent::execute) fn spawn_agent_execution(
    args: SpawnAgentExecution,
) {
    tokio::spawn(async move {
        let SpawnAgentExecution {
            state,
            session_id,
            mut session,
            is_child_session,
            model,
            reasoning_effort,
            reasoning_effort_source,
            cancel_token,
            mpsc_tx,
            image_fallback,
        } = args;

        let system_prompt = system_prompt_for_session(&session);
        let initial_message = initial_user_message_for_session(&session);

        // Use child tool set for child sessions (no spawn schemas), otherwise root tools.
        let tools = if is_child_session {
            state.child_tools.clone()
        } else {
            state.tools.clone()
        };

        // Use model from request (not from session - session.model is just for recording/debugging).
        log::info!(
            "[{}] Using model from request: {}, reasoning_effort={}, reasoning_source={}",
            session_id,
            model,
            reasoning_effort
                .map(crate::core::ReasoningEffort::as_str)
                .unwrap_or("none"),
            reasoning_effort_source
        );

        // Update session.model for debugging/recording purposes.
        session.model = model.clone();

        if let Some(prompt) = system_prompt.as_ref() {
            log::info!("[{}] ========== SYSTEM PROMPT ==========", session_id);
            log::info!("[{}] Final prompt length: {} chars", session_id, prompt.len());
            log::info!("[{}] -----------------------------------", session_id);
            log::info!("[{}] {}", session_id, prompt);
            log::info!("[{}] ========== END SYSTEM PROMPT ==========", session_id);
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
                max_rounds: 50,
                system_prompt,
                skill_manager: Some(state.skill_manager.clone()),
                skip_initial_user_message: true,
                storage: Some(storage),
                attachment_reader: Some(state.session_store.clone()),
                metrics_collector: Some(state.metrics_service.collector()),
                model_name: Some(model),
                reasoning_effort,
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

        log::info!("[{}] Agent execution completed", session_id);
    });
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
