use actix_web::{web, HttpRequest, HttpResponse, Responder};

use super::stream::{live_stream_response, terminal_response};
use super::terminal::terminal_event_if_ready;
use crate::app_state::{AgentStatus, AppState};

/// Subscribe to real-time agent execution events via Server-Sent Events (SSE).
///
/// This endpoint opens a persistent SSE connection that streams agent events
/// in real-time. Call this after starting execution with `POST /api/v1/execute/{session_id}`.
///
/// # HTTP Method
///
/// `GET /api/v1/events/{session_id}`
pub async fn handler(
    state: web::Data<AppState>,
    path: web::Path<String>,
    _req: HttpRequest,
) -> impl Responder {
    let session_id = path.into_inner();
    tracing::debug!("[{}] Events subscription requested", session_id);

    // Validate session exists (index-backed).
    if state
        .session_store
        .get_index_entry(&session_id)
        .await
        .is_none()
    {
        tracing::warn!("[{}] Session not found for events subscription", session_id);
        return HttpResponse::NotFound().json(serde_json::json!({
            "error": "Session not found",
            "session_id": session_id
        }));
    }

    let sender = state.get_session_event_sender(&session_id).await;
    let receiver = sender.subscribe();

    // Snapshot runner info (if present). After restarts we may not have runners in-memory,
    // so don't rely solely on this for "already completed" detection.
    let runner_snapshot = {
        let runners = state.agent_runners.read().await;
        runners.get(&session_id).cloned()
    };

    // Replay last budget event if available (for late subscribers).
    let budget_event_to_replay = runner_snapshot
        .as_ref()
        .and_then(|runner| runner.last_budget_event.clone());

    // If the runner is not actively running (or missing), and the session has no pending
    // user message, return a one-shot terminal event and close the stream. This makes it safe
    // for UIs to "subscribe once" on open even when they missed the live stream.
    //
    // IMPORTANT: If there are running child sessions that forward events into this session's
    // event stream, we must keep the SSE stream open even if the parent runner is not running.
    let runner_status = runner_snapshot.as_ref().map(|runner| runner.status.clone());
    let should_attempt_terminal = !matches!(runner_status, Some(AgentStatus::Running));
    if should_attempt_terminal {
        if let Some(terminal_event) =
            terminal_event_if_ready(&state, &session_id, runner_status).await
        {
            return terminal_response(budget_event_to_replay, terminal_event);
        }
    }

    live_stream_response(budget_event_to_replay, receiver)
}
