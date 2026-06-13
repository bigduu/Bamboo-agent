use actix_web::http::header;
use actix_web::{web, HttpResponse};
use std::time::Duration;
use tokio::sync::broadcast;

use bamboo_agent_core::AgentEvent;

use crate::app_state::AppState;

use super::terminal::has_running_child;

pub(super) fn terminal_response(
    budget_event_to_replay: Option<AgentEvent>,
    critical_events_to_replay: Vec<AgentEvent>,
    terminal_event: AgentEvent,
) -> HttpResponse {
    HttpResponse::Ok()
        .append_header((header::CONTENT_TYPE, "text/event-stream; charset=utf-8"))
        .append_header((header::CACHE_CONTROL, "no-cache, no-transform"))
        .append_header((header::CONNECTION, "keep-alive"))
        .append_header(("X-Accel-Buffering", "no"))
        .streaming(async_stream::stream! {
            // Replay cached critical state events first (task list, sub-sessions, …).
            for event in &critical_events_to_replay {
                if let Some(sse_data) = as_sse_data(Some(event)) {
                    yield Ok::<_, actix_web::Error>(web::Bytes::from(sse_data));
                }
            }
            if let Some(sse_data) = as_sse_data(budget_event_to_replay.as_ref()) {
                yield Ok::<_, actix_web::Error>(web::Bytes::from(sse_data));
            }
            if let Ok(event_json) = serde_json::to_string(&terminal_event) {
                let sse_data = format!("data: {}\n\n", event_json);
                yield Ok::<_, actix_web::Error>(web::Bytes::from(sse_data));
            }
            yield Ok::<_, actix_web::Error>(web::Bytes::from(done_sse_data()));
        })
}

pub(super) fn live_stream_response(
    budget_event_to_replay: Option<AgentEvent>,
    critical_events_to_replay: Vec<AgentEvent>,
    mut receiver: broadcast::Receiver<AgentEvent>,
    state: web::Data<AppState>,
    session_id: String,
) -> HttpResponse {
    HttpResponse::Ok()
        .append_header((header::CONTENT_TYPE, "text/event-stream; charset=utf-8"))
        .append_header((header::CACHE_CONTROL, "no-cache, no-transform"))
        .append_header((header::CONNECTION, "keep-alive"))
        .append_header(("X-Accel-Buffering", "no"))
        .streaming(async_stream::stream! {
            // Replay cached critical state events first (task list, sub-sessions, …).
            for event in &critical_events_to_replay {
                if let Some(sse_data) = as_sse_data(Some(event)) {
                    yield Ok::<_, actix_web::Error>(web::Bytes::from(sse_data));
                }
            }
            if let Some(sse_data) = as_sse_data(budget_event_to_replay.as_ref()) {
                yield Ok::<_, actix_web::Error>(web::Bytes::from(sse_data));
            }

            // Some platforms (notably desktop/webview stacks in release mode) can terminate idle
            // HTTP streams. Emit a small SSE comment heartbeat periodically to keep the
            // connection alive even when the agent is "thinking" (no tokens yet).
            let mut heartbeat = tokio::time::interval(Duration::from_secs(15));
            // Skip the immediate tick.
            heartbeat.tick().await;

            // The parent's own turn can finish (emit a terminal event) while its
            // child actors are still running. Children are designed to outlive the
            // parent turn and forward their progress/preview onto THIS stream, so we
            // must NOT close it on the parent terminal while children remain — doing
            // so leaves child events broadcasting to a receiver-less channel. Hold
            // the terminal open and close only once no running child is left.
            let mut awaiting_children = false;

            loop {
                tokio::select! {
                    _ = heartbeat.tick() => {
                        if awaiting_children && !has_running_child(&state, &session_id).await {
                            yield Ok::<_, actix_web::Error>(web::Bytes::from(done_sse_data()));
                            break;
                        }
                        yield Ok::<_, actix_web::Error>(web::Bytes::from(keepalive_sse_data()));
                    }
                    recv = receiver.recv() => {
                        match recv {
                            Ok(event) => {
                                let is_terminal = is_terminal_event(&event);
                                let is_child_completed =
                                    matches!(event, AgentEvent::SubAgentCompleted { .. });
                                let Ok(event_json) = serde_json::to_string(&event) else {
                                    continue;
                                };
                                let sse_data = format!("data: {}\n\n", event_json);
                                yield Ok::<_, actix_web::Error>(web::Bytes::from(sse_data));
                                if is_terminal {
                                    // Forward the parent terminal, but keep the stream
                                    // alive while child actors are still running.
                                    if has_running_child(&state, &session_id).await {
                                        awaiting_children = true;
                                        continue;
                                    }
                                    yield Ok::<_, actix_web::Error>(web::Bytes::from(done_sse_data()));
                                    break;
                                }
                                // A child just finished: if it was the last running
                                // child after the parent already terminated, close now.
                                if awaiting_children
                                    && is_child_completed
                                    && !has_running_child(&state, &session_id).await
                                {
                                    yield Ok::<_, actix_web::Error>(web::Bytes::from(done_sse_data()));
                                    break;
                                }
                            }
                            Err(broadcast::error::RecvError::Lagged(_skipped)) => {
                                // Best-effort stream; late subscribers can open history.
                                continue;
                            }
                            Err(broadcast::error::RecvError::Closed) => {
                                // Should not happen for long-lived session senders, but exit cleanly.
                                break;
                            }
                        }
                    }
                }
            }
        })
}

fn as_sse_data(event: Option<&AgentEvent>) -> Option<String> {
    let event = event?;
    serde_json::to_string(event)
        .ok()
        .map(|event_json| format!("data: {}\n\n", event_json))
}

fn done_sse_data() -> &'static str {
    "data: [DONE]\n\n"
}

fn keepalive_sse_data() -> &'static str {
    "data: [KEEPALIVE]\n\n"
}

fn is_terminal_event(event: &AgentEvent) -> bool {
    matches!(
        event,
        AgentEvent::Complete { .. } | AgentEvent::Cancelled { .. } | AgentEvent::Error { .. }
    )
}
