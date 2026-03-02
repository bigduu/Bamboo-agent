//! Server-Sent Events (SSE) handler for real-time agent event streaming.
//!
//! This module provides the HTTP endpoint for subscribing to agent execution
//! events via Server-Sent Events protocol.

use actix_web::http::header;
use actix_web::{web, HttpRequest, HttpResponse, Responder};

use crate::agent::core::agent::events::TokenUsage;
use crate::agent::core::AgentEvent;
use crate::agent::core::SessionKind;
use crate::server::app_state::AppState;
use crate::server::app_state::AgentStatus;
use tokio::sync::broadcast;

/// Subscribe to real-time agent execution events via Server-Sent Events (SSE).
///
/// This endpoint opens a persistent SSE connection that streams agent events
/// in real-time. Call this after starting execution with `POST /api/v1/execute/{session_id}`.
///
/// # HTTP Method
///
/// `GET /api/v1/events/{session_id}`
///
/// # Path Parameters
///
/// - `session_id` - The session identifier to subscribe to
///
/// # Response
///
/// - `200 OK` - SSE stream established successfully
/// - `404 Not Found` - Session does not exist
///
/// # Response Format
///
/// Returns a text/event-stream with events in the format:
/// ```text
/// data: {"type":"TextDelta","delta":"Hello"}
///
/// data: {"type":"Complete","usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}}
/// ```
///
/// # Event Types
///
/// The stream can emit the following event types:
///
/// - `TextDelta` - Partial text generation
/// - `ToolCall` - Agent is calling a tool
/// - `ToolResult` - Tool execution completed
/// - `TokenBudgetUpdated` - Token usage statistics updated
/// - `Complete` - Agent execution completed (terminal event)
/// - `Error` - Agent execution failed (terminal event)
///
/// # Terminal Events
///
/// The SSE stream will close automatically after receiving:
/// - `Complete` - Successful completion
/// - `Error` - Execution error
///
/// # Late Subscribers
///
/// If you subscribe after agent execution has started:
/// - You'll receive the last `TokenBudgetUpdated` event immediately
/// - Subsequent events will stream normally
/// - Terminal events will still be delivered
///
/// # Completed/Errored Agents
///
/// If the agent has already finished:
/// - Returns immediate `Complete` or `Error` event
/// - Stream closes immediately after
///
/// # Example
///
/// ```javascript
/// const eventSource = new EventSource('/api/v1/events/session-123');
///
/// eventSource.onmessage = (event) => {
///   const data = JSON.parse(event.data);
///   console.log('Received event:', data);
///
///   if (data.type === 'Complete' || data.type === 'Error') {
///     eventSource.close();
///   }
/// };
/// ```
pub async fn handler(
    state: web::Data<AppState>,
    path: web::Path<String>,
    _req: HttpRequest,
) -> impl Responder {
    let session_id = path.into_inner();
    log::debug!("[{}] Events subscription requested", session_id);

    // Validate session exists (index-backed).
    if state
        .session_store
        .get_index_entry(&session_id)
        .await
        .is_none()
    {
        log::warn!("[{}] Session not found for events subscription", session_id);
        return HttpResponse::NotFound().json(serde_json::json!({
            "error": "Session not found",
            "session_id": session_id
        }));
    }

    let sender = state.get_session_event_sender(&session_id).await;
    let mut receiver = sender.subscribe();

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
    let runner_status = runner_snapshot.as_ref().map(|r| r.status.clone());
    let should_attempt_terminal = !matches!(runner_status, Some(AgentStatus::Running));
    if should_attempt_terminal {
        // Determine whether any running child session belongs to this parent.
        // We intentionally do not hold the runners lock across awaits.
        let running_session_ids: Vec<String> = {
            let runners = state.agent_runners.read().await;
            runners
                .iter()
                .filter_map(|(sid, runner)| {
                    if matches!(runner.status, AgentStatus::Running) {
                        Some(sid.clone())
                    } else {
                        None
                    }
                })
                .collect()
        };

        let mut has_running_child = false;
        for sid in running_session_ids {
            let Some(entry) = state.session_store.get_index_entry(&sid).await else {
                continue;
            };
            if entry.kind == SessionKind::Child
                && entry.parent_session_id.as_deref() == Some(session_id.as_str())
            {
                has_running_child = true;
                break;
            }
        }

        let last_message_is_user = match state.storage.load_session(&session_id).await {
            Ok(Some(session)) => session
                .messages
                .last()
                .map(|m| matches!(m.role, crate::agent::core::agent::Role::User))
                .unwrap_or(false),
            _ => false,
        };

        if !last_message_is_user && !has_running_child {
            let terminal_event = match runner_status {
                Some(AgentStatus::Error(msg)) => AgentEvent::Error { message: msg },
                Some(AgentStatus::Cancelled) => AgentEvent::Error {
                    message: "Agent execution cancelled by user".to_string(),
                },
                _ => AgentEvent::Complete {
                    // We don't persist TokenUsage today; clients can fetch history for results.
                    usage: TokenUsage {
                        prompt_tokens: 0,
                        completion_tokens: 0,
                        total_tokens: 0,
                    },
                },
            };

            return HttpResponse::Ok()
                .append_header((header::CONTENT_TYPE, "text/event-stream"))
                .append_header((header::CACHE_CONTROL, "no-cache"))
                .append_header((header::CONNECTION, "keep-alive"))
                .streaming(async_stream::stream! {
                    if let Some(ref budget_event) = budget_event_to_replay {
                        if let Ok(event_json) = serde_json::to_string(budget_event) {
                            let sse_data = format!("data: {}\n\n", event_json);
                            yield Ok::<_, actix_web::Error>(actix_web::web::Bytes::from(sse_data));
                        }
                    }

                    if let Ok(event_json) = serde_json::to_string(&terminal_event) {
                        let sse_data = format!("data: {}\n\n", event_json);
                        yield Ok::<_, actix_web::Error>(actix_web::web::Bytes::from(sse_data));
                    }
                });
        }
    }

    HttpResponse::Ok()
        .append_header((header::CONTENT_TYPE, "text/event-stream"))
        .append_header((header::CACHE_CONTROL, "no-cache"))
        .append_header((header::CONNECTION, "keep-alive"))
        .streaming(async_stream::stream! {
            if let Some(ref budget_event) = budget_event_to_replay {
                if let Ok(event_json) = serde_json::to_string(budget_event) {
                    let sse_data = format!("data: {}\n\n", event_json);
                    yield Ok::<_, actix_web::Error>(actix_web::web::Bytes::from(sse_data));
                }
            }

            loop {
                match receiver.recv().await {
                    Ok(event) => {
                        let Ok(event_json) = serde_json::to_string(&event) else {
                            continue;
                        };
                        let sse_data = format!("data: {}\n\n", event_json);
                        yield Ok::<_, actix_web::Error>(actix_web::web::Bytes::from(sse_data));
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        // Best-effort stream; late subscribers can open history.
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        // Should not happen for long-lived session senders, but exit cleanly.
                        break;
                    }
                }
            }
        })
}
