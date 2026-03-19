use actix_web::{web, HttpResponse};
use tokio::sync::mpsc;

use super::image_fallback::{resolve_image_fallback, validate_image_fallback_for_session};
use super::runtime::{
    consume_pending_ask_user_resume, has_pending_user_message, reserve_runner,
    spawn_agent_execution, spawn_event_forwarder, RunnerReservation, SpawnAgentExecution,
};
use super::session::load_session;
use super::ExecuteRequest;
use crate::agent::core::SessionKind;
use crate::server::app_state::AppState;

use self::response::{
    already_running_response, bad_request_error_response, completed_response,
    internal_server_error_response, started_response,
};
use self::validation::validate_and_normalize_model;

mod response;
#[cfg(test)]
mod tests;
mod validation;

/// Execute the AI agent on a chat session.
///
/// This endpoint triggers the agent loop to process pending messages
/// in the session. Use this after creating a chat message with `POST /api/v1/chat`.
///
/// # HTTP Method
///
/// `POST /api/v1/execute/{session_id}`
///
/// # Path Parameters
///
/// - `session_id` - The session identifier returned from `/api/v1/chat`
///
/// # Request Body
///
/// JSON-encoded [`ExecuteRequest`] containing the required `model` field
///
/// # Response
///
/// - `202 Accepted` - Agent execution started successfully
/// - `400 Bad Request` - Missing or empty `model` parameter
/// - `404 Not Found` - Session does not exist
/// - `500 Internal Server Error` - Failed to load session
///
/// # Execution Flow
///
/// 1. Validates the `model` parameter is provided
/// 2. Loads the session from memory or storage
/// 3. Checks for pending user messages
/// 4. Returns `completed` if no pending messages
/// 5. Checks if agent is already running (returns `already_running`)
/// 6. Spawns agent loop in background thread
/// 7. Returns immediately with events URL
///
/// # Event Subscription
///
/// After starting execution, subscribe to events using:
/// ```text
/// GET /api/v1/events/{session_id}
/// ```
///
/// # Concurrency
///
/// This endpoint is safe to call multiple times. If the agent is already
/// running for the session, it returns status `already_running`.
///
/// # Example
///
/// ```bash
/// curl -X POST http://localhost:9562/api/v1/execute/session-123 \
///   -H "Content-Type: application/json" \
///   -d '{"model": "gpt-4o-mini"}'
/// ```
pub async fn handler(
    state: web::Data<AppState>,
    path: web::Path<String>,
    req: web::Json<ExecuteRequest>,
) -> HttpResponse {
    let session_id = path.into_inner();
    let model = match validate_and_normalize_model(&req.model) {
        Ok(model) => model,
        Err(response) => return response,
    };
    let request_reasoning_effort = req.reasoning_effort;

    log::debug!(
        "[{}] Execute request received with model: {}",
        session_id,
        model
    );

    let mut session = match load_session(&state, &session_id).await {
        Ok(session) => session,
        Err(response) => return response,
    };
    let is_child_session = session.kind == SessionKind::Child;

    // Snapshot server config for this execution.
    //
    // IMPORTANT: Do NOT rewrite `session.messages` here. Image fallback (placeholder/OCR)
    // is intended for LLM request construction only. Persisted session history must keep
    // the original multimodal parts so the frontend can render attachments.
    let config_snapshot = state.config.read().await.clone();
    let default_reasoning_effort = config_snapshot.get_reasoning_effort();
    let effective_reasoning_effort = request_reasoning_effort.or(default_reasoning_effort);
    let disabled_tools = config_snapshot.disabled_tool_names();
    let reasoning_effort_source = if request_reasoning_effort.is_some() {
        "request"
    } else if default_reasoning_effort.is_some() {
        "provider_default"
    } else {
        "none"
    };
    let image_fallback = match resolve_image_fallback(&config_snapshot) {
        Ok(value) => value,
        Err(error) => return internal_server_error_response(error),
    };

    if let Err(error) = validate_image_fallback_for_session(&session, image_fallback.as_ref()) {
        return bad_request_error_response(error);
    }

    if !has_pending_user_message(&session) {
        log::debug!(
            "[{}] No pending user message, returning completed status",
            session_id
        );
        return completed_response(&session_id);
    }

    if let Some(reasoning_effort) = effective_reasoning_effort {
        session.metadata.insert(
            "reasoning_effort".to_string(),
            reasoning_effort.as_str().to_string(),
        );
        session.metadata.insert(
            "reasoning_effort_source".to_string(),
            reasoning_effort_source.to_string(),
        );
    } else {
        session.metadata.remove("reasoning_effort");
        session.metadata.remove("reasoning_effort_source");
    }

    // Stable, long-lived session event sender (also used for background jobs).
    let session_tx = state.get_session_event_sender(&session_id).await;

    let cancel_token = match reserve_runner(state.get_ref(), &session_id, &session_tx).await {
        RunnerReservation::Started(token) => token,
        RunnerReservation::AlreadyRunning => return already_running_response(&session_id),
    };

    consume_pending_ask_user_resume(&mut session);

    log::info!("[{}] Starting agent execution", session_id);

    // Create mpsc channel for agent loop.
    let (mpsc_tx, mpsc_rx) = mpsc::channel::<crate::agent::core::AgentEvent>(100);

    spawn_event_forwarder(
        state.clone(),
        session_id.clone(),
        mpsc_rx,
        session_tx.clone(),
    );
    spawn_agent_execution(SpawnAgentExecution {
        state: state.clone(),
        session_id: session_id.clone(),
        session,
        is_child_session,
        model,
        reasoning_effort: effective_reasoning_effort,
        reasoning_effort_source: reasoning_effort_source.to_string(),
        disabled_tools,
        cancel_token,
        mpsc_tx,
        image_fallback,
    });

    started_response(&session_id)
}
