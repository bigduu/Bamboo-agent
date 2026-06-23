use actix_web::{web, HttpRequest, HttpResponse, Responder};
use serde::Deserialize;

use super::stream::{live_stream_response, terminal_response};
use super::terminal::terminal_event_if_ready;
use crate::app_state::{AgentStatus, AppState};

/// Upper bound for the client-supplied token-coalescing window (milliseconds).
///
/// `batch_ms` is an untrusted query parameter; this caps the buffering window
/// so a hostile/typo'd value cannot defer flushes out to the heartbeat interval.
pub(super) const MAX_BATCH_MS: u64 = 1_000;

/// Query parameters for the per-session events (SSE) endpoint.
#[derive(Debug, Default, Deserialize)]
pub struct EventsQuery {
    /// Token-coalescing window in milliseconds (v2-P0).
    ///
    /// `0` (the default) preserves the legacy behavior exactly: every event is
    /// emitted in its own SSE frame immediately, with no buffering. When
    /// `> 0`, consecutive token-class events (`Token` / `ReasoningToken` /
    /// `ToolToken` of the same `tool_call_id`) are merged into a single frame,
    /// bounding added latency to `batch_ms`. Desktop clients pass `0`; mobile
    /// clients pass e.g. `50`.
    #[serde(default)]
    pub batch_ms: u64,
}

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
    query: web::Query<EventsQuery>,
    _req: HttpRequest,
) -> impl Responder {
    let session_id = path.into_inner();
    // Clamp the client-supplied window to a sane ceiling. `batch_ms` is an
    // untrusted query parameter; without a cap a pathological value (up to
    // u64::MAX ms) would push the effective flush bound out to the 15s
    // heartbeat and let the coalescing buffer grow for the whole window.
    // 1s is well beyond any useful coalescing window (mobile uses ~50ms).
    let batch_ms = query.batch_ms.min(MAX_BATCH_MS);
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

    // Ensure a backend notification relay is running for this session so that
    // approval/clarification/context/subagent events surface as notifications.
    state.ensure_notification_relay(&session_id, sender.clone());

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

    // Collect cached critical events for replay (TaskListUpdated, SubAgent*, etc.).
    let critical_events_to_replay: Vec<_> = runner_snapshot
        .as_ref()
        .map(|runner| runner.last_critical_events.clone())
        .unwrap_or_default();

    // If the runner is not actively running (or missing), and the session has no pending
    // user message, return a one-shot terminal event and close the stream. This makes it safe
    // for UIs to "subscribe once" on open even when they missed the live stream.
    //
    // IMPORTANT: If there are running child sessions that forward events into this session's
    // event stream, we must keep the SSE stream open even if the parent runner is not running.
    let runner_status = runner_snapshot.as_ref().map(|runner| runner.status.clone());
    let should_attempt_terminal = !matches!(runner_status, Some(AgentStatus::Running));
    tracing::debug!(
        "[{}] Events decision: runner_present={}, runner_status={:?}, should_attempt_terminal={}, critical_events_to_replay={}",
        session_id,
        runner_snapshot.is_some(),
        runner_status,
        should_attempt_terminal,
        critical_events_to_replay.len(),
    );
    if should_attempt_terminal {
        if let Some(terminal_event) =
            terminal_event_if_ready(&state, &session_id, runner_status).await
        {
            tracing::debug!(
                "[{}] Events -> ONE-SHOT terminal stream (closing immediately); the client will treat this as a finished run",
                session_id,
            );
            return terminal_response(
                budget_event_to_replay,
                critical_events_to_replay,
                terminal_event,
            );
        }
    }

    tracing::debug!(
        "[{}] Events -> LIVE stream opened (kept open, awaiting runner events)",
        session_id,
    );
    live_stream_response(
        budget_event_to_replay,
        critical_events_to_replay,
        receiver,
        state.clone(),
        session_id,
        batch_ms,
    )
}
