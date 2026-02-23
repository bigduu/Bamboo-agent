use actix_web::http::header;
use actix_web::{web, HttpRequest, HttpResponse, Responder};
use chrono::Utc;
use std::sync::Arc;
use tokio::sync::mpsc;

use crate::state::{AgentRunner, AgentStatus, AppState};
use agent_loop::{run_agent_loop_with_config, AgentLoopConfig};

/// DEPRECATED: Use POST /execute + GET /events instead
/// This handler maintains backward compatibility by combining execute + events
pub async fn handler(
    state: web::Data<AppState>,
    path: web::Path<String>,
    _req: HttpRequest,
) -> impl Responder {
    let session_id = path.into_inner();
    log::warn!(
        "[{}] DEPRECATED: /stream endpoint called. Use /execute + /events instead",
        session_id
    );

    // Load session from memory or storage first (async, no locks held)
    let mut session = {
        let sessions = state.sessions.read().await;
        sessions.get(&session_id).cloned()
    };

    if session.is_none() {
        match state.storage.load_session(&session_id).await {
            Ok(Some(s)) => session = Some(s),
            Ok(None) => {
                log::warn!("[{}] Session not found", session_id);
                return HttpResponse::NotFound().json(serde_json::json!({
                    "error": "Session not found",
                    "session_id": session_id
                }));
            }
            Err(e) => {
                log::error!("[{}] Failed to load session: {}", session_id, e);
                return HttpResponse::InternalServerError().json(serde_json::json!({
                    "error": format!("Failed to load session: {}", e)
                }));
            }
        }
    }

    let mut session = session.unwrap();

    // Check if there's a pending user message
    let last_message_is_user = session
        .messages
        .last()
        .map(|m| matches!(m.role, agent_core::agent::Role::User))
        .unwrap_or(false);

    if !last_message_is_user {
        // No pending message, just send complete event
        log::debug!(
            "[{}] Session already processed, sending complete event",
            session_id
        );
        let runner = AgentRunner::new();
        return subscribe_to_runner(runner, session_id);
    }

    // Atomically check and insert runner to prevent race conditions
    let (broadcast_tx, cancel_token) = {
        let mut runners = state.agent_runners.write().await;

        // Check if there's already a running runner for this session
        if let Some(runner) = runners.get(&session_id) {
            if matches!(runner.status, AgentStatus::Running) {
                log::debug!("[{}] Runner already running, subscribing to events", session_id);
                return subscribe_to_runner(runner.clone(), session_id);
            }
            log::debug!("[{}] Existing runner with status {:?}, will restart", session_id, runner.status);
        }

        // Remove stale runner and insert new one atomically
        runners.remove(&session_id);

        let mut runner = AgentRunner::new();
        runner.status = AgentStatus::Running;
        let broadcast_tx = runner.event_sender.clone();
        let cancel_token = runner.cancel_token.clone();

        runners.insert(session_id.clone(), runner);

        (broadcast_tx, cancel_token)
    };

    log::info!("[{}] Starting agent execution (legacy stream handler)", session_id);

    // Create mpsc channel for agent loop
    let (mpsc_tx, mut mpsc_rx) = mpsc::channel::<agent_core::AgentEvent>(100);

    // Start agent loop in background
    let state_clone = state.get_ref().clone();
    let session_id_clone = session_id.clone();

    // Spawn event forwarder: mpsc -> broadcast
    let session_id_forwarder = session_id.clone();
    tokio::spawn(async move {
        while let Some(event) = mpsc_rx.recv().await {
            if broadcast_tx.send(event.clone()).is_err() {
                log::debug!("[{}] No subscribers for event", session_id_forwarder);
            }
        }
        log::debug!("[{}] Event forwarder finished", session_id_forwarder);
    });

    // Spawn agent loop
    tokio::spawn(async move {
        // Get system prompt
        let system_prompt = session
            .messages
            .iter()
            .find(|m| matches!(m.role, agent_core::agent::Role::System))
            .map(|m| m.content.clone());

        // Get initial user message
        let initial_message = session
            .messages
            .last()
            .filter(|m| matches!(m.role, agent_core::agent::Role::User))
            .map(|m| m.content.clone())
            .unwrap_or_default();

        // Get all tool schemas
        let all_tool_schemas = state_clone.get_all_tool_schemas();

        // Model is required in session
        let model_name = session.model.clone();

        if let Some(prompt) = system_prompt.as_ref() {
            println!("\n========== SYSTEM PROMPT ==========");
            println!("Session: {}", session_id_clone);
            println!("Final prompt length: {} chars", prompt.len());
            println!("-----------------------------------");
            println!("{}", prompt);
            println!("========== END SYSTEM PROMPT ==========\n");
        }

        // Run agent loop
        let storage: Arc<dyn agent_core::storage::Storage> =
            Arc::new(state_clone.storage.clone());

        let result = run_agent_loop_with_config(
            &mut session,
            initial_message,
            mpsc_tx.clone(),
            state_clone.llm.clone(),
            state_clone.tools.clone(),
            cancel_token,
            AgentLoopConfig {
                max_rounds: 50,
                system_prompt,
                additional_tool_schemas: all_tool_schemas,
                skill_manager: Some(state_clone.skill_manager.clone()),
                skip_initial_user_message: true,
                storage: Some(storage),
                metrics_collector: Some(state_clone.metrics_service.collector()),
                model_name: Some(model_name),
                ..Default::default()
            },
        )
        .await;

        // Send terminal event for all error cases (including cancellation)
        if let Err(ref e) = result {
            if e.to_string().contains("cancelled") {
                // Emit a specific cancellation event so SSE streams can close cleanly
                let _ = mpsc_tx.send(agent_core::AgentEvent::Error {
                    message: "Agent execution cancelled by user".to_string(),
                }).await;
            } else {
                let _ = mpsc_tx.send(agent_core::AgentEvent::Error {
                    message: e.to_string(),
                }).await;
            }
        }

        // Update runner status
        {
            let mut runners = state_clone.agent_runners.write().await;
            if let Some(runner) = runners.get_mut(&session_id_clone) {
                runner.status = match result {
                    Ok(_) => AgentStatus::Completed,
                    Err(e) if e.to_string().contains("cancelled") => AgentStatus::Cancelled,
                    Err(e) => AgentStatus::Error(e.to_string()),
                };
                runner.completed_at = Some(Utc::now());
            }
        }

        // Save session
        state_clone.save_session(&session).await;

        // Update memory
        {
            let mut sessions = state_clone.sessions.write().await;
            sessions.insert(session_id_clone.clone(), session);
        }

        // Remove cancellation token (legacy)
        {
            let mut tokens = state_clone.cancel_tokens.write().await;
            tokens.remove(&session_id_clone);
        }

        log::info!("[{}] Agent execution completed (legacy stream)", session_id_clone);
    });

    // Subscribe to the broadcast channel
    let mut receiver = {
        let runners = state.agent_runners.read().await;
        runners.get(&session_id).unwrap().event_sender.subscribe()
    };

    HttpResponse::Ok()
        .append_header((header::CONTENT_TYPE, "text/event-stream"))
        .append_header((header::CACHE_CONTROL, "no-cache"))
        .append_header((header::CONNECTION, "keep-alive"))
        .streaming(async_stream::stream! {
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

/// Subscribe to an existing runner's broadcast channel
fn subscribe_to_runner(
    runner: AgentRunner,
    _session_id: String,
) -> HttpResponse {
    let mut receiver = runner.event_sender.subscribe();

    HttpResponse::Ok()
        .append_header((header::CONTENT_TYPE, "text/event-stream"))
        .append_header((header::CACHE_CONTROL, "no-cache"))
        .append_header((header::CONNECTION, "keep-alive"))
        .streaming(async_stream::stream! {
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

// Implement Clone for AppState (for passing between threads)
impl Clone for AppState {
    fn clone(&self) -> Self {
        Self {
            sessions: self.sessions.clone(),
            storage: self.storage.clone(),
            llm: self.llm.clone(),
            tools: self.tools.clone(),
            cancel_tokens: self.cancel_tokens.clone(),
            skill_manager: self.skill_manager.clone(),
            mcp_manager: self.mcp_manager.clone(),
            metrics_service: self.metrics_service.clone(),
            model_name: self.model_name.clone(),
            agent_runners: self.agent_runners.clone(),
        }
    }
}
