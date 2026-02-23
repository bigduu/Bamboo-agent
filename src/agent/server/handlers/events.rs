use actix_web::http::header;
use actix_web::{web, HttpRequest, HttpResponse, Responder};

use crate::state::{AgentStatus, AppState};
use agent_core::TokenUsage;

pub async fn handler(
    state: web::Data<AppState>,
    path: web::Path<String>,
    _req: HttpRequest,
) -> impl Responder {
    let session_id = path.into_inner();
    log::debug!("[{}] Events subscription requested", session_id);

    // Check if there's a runner for this session
    let (event_receiver, runner_status, budget_event_to_replay) = {
        let runners = state.agent_runners.read().await;
        match runners.get(&session_id) {
            Some(runner) => {
                let rx = runner.event_sender.subscribe();
                let status = runner.status.clone();
                let budget_event = runner.last_budget_event.clone();
                log::debug!("[{}] Found runner with status: {:?}", session_id, status);
                (Some(rx), Some(status), budget_event)
            }
            None => {
                log::debug!("[{}] No runner found for session", session_id);
                (None, None, None)
            }
        }
    };

    match event_receiver {
        Some(mut receiver) => {
            // Check if runner is already completed or errored - if so, send immediate event
            match runner_status {
                Some(AgentStatus::Completed) => {
                    log::debug!("[{}] Runner already completed, sending immediate complete event", session_id);
                    return HttpResponse::Ok()
                        .append_header((header::CONTENT_TYPE, "text/event-stream"))
                        .append_header((header::CACHE_CONTROL, "no-cache"))
                        .streaming(async_stream::stream! {
                            let event = agent_core::AgentEvent::Complete {
                                usage: TokenUsage {
                                    prompt_tokens: 0,
                                    completion_tokens: 0,
                                    total_tokens: 0,
                                }
                            };
                            let event_json = serde_json::to_string(&event).unwrap();
                            let sse_data = format!("data: {}\n\n", event_json);
                            yield Ok::<_, actix_web::Error>(
                                actix_web::web::Bytes::from(sse_data)
                            );
                        });
                }
                Some(AgentStatus::Error(err)) => {
                    log::debug!("[{}] Runner already errored, sending immediate error event: {}", session_id, err);
                    return HttpResponse::Ok()
                        .append_header((header::CONTENT_TYPE, "text/event-stream"))
                        .append_header((header::CACHE_CONTROL, "no-cache"))
                        .streaming(async_stream::stream! {
                            let event = agent_core::AgentEvent::Error {
                                message: err.clone(),
                            };
                            let event_json = serde_json::to_string(&event).unwrap();
                            let sse_data = format!("data: {}\n\n", event_json);
                            yield Ok::<_, actix_web::Error>(
                                actix_web::web::Bytes::from(sse_data)
                            );
                        });
                }
                _ => {
                    // Runner is still running or in another state, continue to normal streaming
                }
            }

            // Has runner, stream events from broadcast channel
            HttpResponse::Ok()
                .append_header((header::CONTENT_TYPE, "text/event-stream"))
                .append_header((header::CACHE_CONTROL, "no-cache"))
                .append_header((header::CONNECTION, "keep-alive"))
                .streaming(async_stream::stream! {
                    // Replay last budget event if available (for late subscribers)
                    if let Some(ref budget_event) = budget_event_to_replay {
                        let event_json = match serde_json::to_string(budget_event) {
                            Ok(json) => json,
                            Err(_) => {
                                log::warn!("[{}] Failed to serialize budget event for replay", session_id);
                                String::new()
                            }
                        };
                        if !event_json.is_empty() {
                            let sse_data = format!("data: {}\n\n", event_json);
                            yield Ok::<_, actix_web::Error>(
                                actix_web::web::Bytes::from(sse_data)
                            );
                        }
                    }

                    while let Ok(event) = receiver.recv().await {
                        let event_json = match serde_json::to_string(&event) {
                            Ok(json) => json,
                            Err(_) => continue,
                        };

                        let sse_data = format!("data: {}\n\n", event_json);
                        yield Ok::<_, actix_web::Error>(
                            actix_web::web::Bytes::from(sse_data)
                        );

                        // Terminal events end the stream
                        match &event {
                            agent_core::AgentEvent::Complete { .. } |
                            agent_core::AgentEvent::Error { .. } => break,
                            _ => {}
                        }
                    }
                })
        }
        None => {
            // No runner, check if session exists
            let session_exists = {
                let sessions = state.sessions.read().await;
                sessions.contains_key(&session_id)
            } || state
                .storage
                .load_session(&session_id)
                .await
                .ok()
                .flatten()
                .is_some();

            if session_exists {
                // Session exists but agent never ran or completed long ago
                log::debug!(
                    "[{}] Session exists but no active runner, sending immediate complete",
                    session_id
                );
                HttpResponse::Ok()
                    .append_header((header::CONTENT_TYPE, "text/event-stream"))
                    .streaming(async_stream::stream! {
                        let event = agent_core::AgentEvent::Complete {
                            usage: TokenUsage {
                                prompt_tokens: 0,
                                completion_tokens: 0,
                                total_tokens: 0,
                            }
                        };
                        let event_json = serde_json::to_string(&event).unwrap();
                        let sse_data = format!("data: {}\n\n", event_json);
                        yield Ok::<_, actix_web::Error>(
                            actix_web::web::Bytes::from(sse_data)
                        );
                    })
            } else {
                log::warn!("[{}] Session not found for events subscription", session_id);
                HttpResponse::NotFound().json(serde_json::json!({
                    "error": "Session not found",
                    "session_id": session_id
                }))
            }
        }
    }
}
