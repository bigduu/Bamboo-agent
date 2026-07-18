use actix_web::{web, HttpRequest, HttpResponse, Responder};
use serde::Deserialize;

use super::stream::{live_stream_response, terminal_response};
use super::terminal::terminal_event_if_ready;
use crate::app_state::{AgentStatus, AppState};

/// Upper bound for the client-supplied token-coalescing window (milliseconds).
///
/// `batch_ms` is an untrusted query parameter; this caps the buffering window
/// so a hostile/typo'd value cannot defer flushes out to the heartbeat interval.
pub(crate) const MAX_BATCH_MS: u64 = 1_000;

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
            "error": crate::error::error_value("Session not found"),
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
    // Held by the stream body for the connection's lifetime; decrements the
    // session's live-watcher count on drop (graceful close or client
    // disconnect) — see `app_state::watchers`.
    let watcher_guard =
        crate::app_state::watchers::WatcherGuard::new(state.session_watchers.clone(), &session_id);
    live_stream_response(
        budget_event_to_replay,
        critical_events_to_replay,
        receiver,
        state.clone(),
        session_id,
        batch_ms,
        watcher_guard,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::Responder;
    use bamboo_agent_core::{Message, Session};
    use std::time::Duration;

    #[actix_web::test]
    async fn expired_startup_admission_waits_for_locked_live_reconcile() {
        let dir = tempfile::tempdir().expect("temporary app data");
        let state = web::Data::new(
            AppState::new(dir.path().to_path_buf())
                .await
                .expect("app state"),
        );
        let session_id = "sse-admission-startup-race";
        let mut session = Session::new(session_id, "test-model");
        session.add_message(Message::user("slow startup"));
        crate::handlers::agent::events::mark_pending_turn(&mut session);
        session.metadata.insert(
            "execute.startup_handoff_at".to_string(),
            (chrono::Utc::now() - chrono::Duration::seconds(120)).to_rfc3339(),
        );
        state.save_session(&mut session).await;

        // Models `/execute` registering after admission began but before an
        // unlocked synthetic terminal could be emitted. The subscriber must
        // remain live until the owner leaves and the locked reconciler wins.
        let startup_guard =
            crate::handlers::agent::events::begin_execute_startup(state.get_ref(), session_id);
        let request = actix_web::test::TestRequest::get().to_http_request();
        let response = handler(
            state.clone(),
            web::Path::from(session_id.to_string()),
            web::Query(EventsQuery::default()),
            request.clone(),
        )
        .await
        .respond_to(&request);
        let collect = actix_web::body::to_bytes(response.into_body());
        tokio::pin!(collect);

        assert!(
            tokio::time::timeout(Duration::from_millis(350), &mut collect)
                .await
                .is_err(),
            "an in-flight execute owner must prevent admission from closing"
        );
        drop(startup_guard);

        let bytes = tokio::time::timeout(Duration::from_secs(2), &mut collect)
            .await
            .expect("locked reconcile closes the SSE stream");
        let bytes = match bytes {
            Ok(bytes) => bytes,
            Err(_) => panic!("read SSE body"),
        };
        let body = String::from_utf8(bytes.to_vec()).expect("UTF-8 SSE");
        assert!(body.contains("was not started"), "{body}");
        assert_eq!(body.matches("[DONE]").count(), 1);

        let stored = state
            .storage
            .load_session(session_id)
            .await
            .expect("load session")
            .expect("stored session");
        assert_eq!(stored.last_run_status().as_deref(), Some("error"));
        assert!(crate::handlers::agent::events::startup_work_id(&stored).is_none());
    }
}
