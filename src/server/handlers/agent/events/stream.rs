use actix_web::http::header;
use actix_web::{web, HttpResponse};
use std::time::Duration;
use tokio::sync::broadcast;

use crate::agent::core::AgentEvent;

pub(super) fn terminal_response(
    budget_event_to_replay: Option<AgentEvent>,
    terminal_event: AgentEvent,
) -> HttpResponse {
    HttpResponse::Ok()
        .append_header((header::CONTENT_TYPE, "text/event-stream"))
        .append_header((header::CACHE_CONTROL, "no-cache"))
        .append_header((header::CONNECTION, "keep-alive"))
        .streaming(async_stream::stream! {
            if let Some(sse_data) = as_sse_data(budget_event_to_replay.as_ref()) {
                yield Ok::<_, actix_web::Error>(web::Bytes::from(sse_data));
            }
            if let Ok(event_json) = serde_json::to_string(&terminal_event) {
                let sse_data = format!("data: {}\n\n", event_json);
                yield Ok::<_, actix_web::Error>(web::Bytes::from(sse_data));
            }
        })
}

pub(super) fn live_stream_response(
    budget_event_to_replay: Option<AgentEvent>,
    mut receiver: broadcast::Receiver<AgentEvent>,
) -> HttpResponse {
    HttpResponse::Ok()
        .append_header((header::CONTENT_TYPE, "text/event-stream"))
        .append_header((header::CACHE_CONTROL, "no-cache"))
        .append_header((header::CONNECTION, "keep-alive"))
        .streaming(async_stream::stream! {
            if let Some(sse_data) = as_sse_data(budget_event_to_replay.as_ref()) {
                yield Ok::<_, actix_web::Error>(web::Bytes::from(sse_data));
            }

            // Some platforms (notably desktop/webview stacks in release mode) can terminate idle
            // HTTP streams. Emit a small SSE comment heartbeat periodically to keep the
            // connection alive even when the agent is "thinking" (no tokens yet).
            let mut heartbeat = tokio::time::interval(Duration::from_secs(15));
            // Skip the immediate tick.
            heartbeat.tick().await;

            loop {
                tokio::select! {
                    _ = heartbeat.tick() => {
                        yield Ok::<_, actix_web::Error>(web::Bytes::from(": heartbeat\n\n"));
                    }
                    recv = receiver.recv() => {
                        match recv {
                            Ok(event) => {
                                let Ok(event_json) = serde_json::to_string(&event) else {
                                    continue;
                                };
                                let sse_data = format!("data: {}\n\n", event_json);
                                yield Ok::<_, actix_web::Error>(web::Bytes::from(sse_data));
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
