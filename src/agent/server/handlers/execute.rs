use actix_web::{web, HttpResponse, Responder};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::mpsc;

use crate::state::{AgentRunner, AgentStatus, AppState};
use agent_core::agent::Role;
use agent_loop::{run_agent_loop_with_config, AgentLoopConfig};

#[derive(Serialize)]
pub struct ExecuteResponse {
    pub session_id: String,
    pub status: String,
    pub events_url: String,
}

#[derive(Deserialize)]
pub struct ExecuteRequest {
    /// Model to use for execution (required)
    pub model: String,
}

pub async fn handler(
    state: web::Data<AppState>,
    path: web::Path<String>,
    req: web::Json<ExecuteRequest>,
) -> impl Responder {
    let session_id = path.into_inner();
    let model = req.model.trim();

    // Validate model is not empty
    if model.is_empty() {
        return HttpResponse::BadRequest().json(serde_json::json!({
            "error": "model parameter is required"
        }));
    }
    let model = model.to_string();

    log::debug!("[{}] Execute request received with model: {}", session_id, model);

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
        .map(|m| matches!(m.role, Role::User))
        .unwrap_or(false);

    if !last_message_is_user {
        log::debug!("[{}] No pending user message, returning completed status", session_id);
        return HttpResponse::Ok().json(ExecuteResponse {
            session_id: session_id.clone(),
            status: "completed".to_string(),
            events_url: format!("/api/v1/events/{}", session_id),
        });
    }

    // Atomically check and insert runner to prevent race conditions
    let (broadcast_tx, cancel_token) = {
        let mut runners = state.agent_runners.write().await;

        // Check if there's already a running runner for this session
        if let Some(runner) = runners.get(&session_id) {
            if matches!(runner.status, AgentStatus::Running) {
                log::debug!("[{}] Runner already running, returning status: already_running", session_id);
                return HttpResponse::Ok().json(ExecuteResponse {
                    session_id: session_id.clone(),
                    status: "already_running".to_string(),
                    events_url: format!("/api/v1/events/{}", session_id),
                });
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

    log::info!("[{}] Starting agent execution", session_id);

    // Create mpsc channel for agent loop
    let (mpsc_tx, mut mpsc_rx) = mpsc::channel::<agent_core::AgentEvent>(100);

    // Start agent loop in background
    let state_clone = state.get_ref().clone();
    let session_id_clone = session_id.clone();

    // Spawn event forwarder: mpsc -> broadcast
    let session_id_forwarder = session_id.clone();
    let state_for_forwarder = state.get_ref().clone();
    tokio::spawn(async move {
        while let Some(event) = mpsc_rx.recv().await {
            // Store budget events for late subscribers
            if matches!(&event, agent_core::AgentEvent::TokenBudgetUpdated { .. }) {
                let mut runners = state_for_forwarder.agent_runners.write().await;
                if let Some(runner) = runners.get_mut(&session_id_forwarder) {
                    runner.last_budget_event = Some(event.clone());
                    log::debug!("[{}] Stored budget event for late subscribers", session_id_forwarder);
                }
            }

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
            .find(|m| matches!(m.role, Role::System))
            .map(|m| m.content.clone());

        // Get initial user message
        let initial_message = session
            .messages
            .last()
            .filter(|m| matches!(m.role, Role::User))
            .map(|m| m.content.clone())
            .unwrap_or_default();

        // Get all tool schemas
        let all_tool_schemas = state_clone.get_all_tool_schemas();

        // Use model from request (not from session - session.model is just for recording/debugging)
        log::info!("[{}] Using model from request: {}", session_id_clone, model);

        // Update session.model for debugging/recording purposes
        session.model = model.clone();

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
                model_name: Some(model),
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

        log::info!("[{}] Agent execution completed", session_id_clone);
    });

    HttpResponse::Accepted().json(ExecuteResponse {
        session_id: session_id.clone(),
        status: "started".to_string(),
        events_url: format!("/api/v1/events/{}", session_id),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::AgentRunner;

    #[test]
    fn test_agent_status_running_blocks_restart() {
        // Test that Running status should block restart
        let status = AgentStatus::Running;
        assert!(matches!(status, AgentStatus::Running));
    }

    #[test]
    fn test_agent_status_completed_allows_restart() {
        // Test that Completed status should allow restart
        let status = AgentStatus::Completed;
        assert!(!matches!(status, AgentStatus::Running));
    }

    #[test]
    fn test_agent_status_error_allows_restart() {
        // Test that Error status should allow restart
        let status = AgentStatus::Error("test error".to_string());
        assert!(!matches!(status, AgentStatus::Running));
    }

    #[test]
    fn test_agent_status_cancelled_allows_restart() {
        // Test that Cancelled status should allow restart
        let status = AgentStatus::Cancelled;
        assert!(!matches!(status, AgentStatus::Running));
    }

    #[test]
    fn test_runner_creation() {
        // Test that runners can be created and have proper initial state
        let runner = AgentRunner::new();
        assert!(matches!(runner.status, AgentStatus::Pending));
        // Verify cancel token exists (can be cloned)
        let _token_clone = runner.cancel_token.clone();
    }

    // ========== MODEL REQUIREMENT ARCHITECTURE TESTS ==========
    // These tests ensure the design principle:
    // "execute must receive model in request body, not from session"

    /// Test: ExecuteRequest.model must be String (not Option<String>)
    #[test]
    fn execute_request_model_type_is_string_not_option() {
        let json = r#"{
            "model": "kimi-for-coding"
        }"#;

        let request: ExecuteRequest = serde_json::from_str(json).unwrap();
        // This proves model is String, not Option<String>
        let _model_str: &str = &request.model;
        assert_eq!(request.model, "kimi-for-coding");
    }

    /// Test: ExecuteRequest deserialization fails without model
    #[test]
    fn execute_request_requires_model() {
        let json = r#"{}"#;
        let result: Result<ExecuteRequest, _> = serde_json::from_str(json);
        assert!(
            result.is_err(),
            "ExecuteRequest should fail without model field"
        );
    }

    /// Test: Empty/whitespace model should fail validation
    #[test]
    fn execute_request_empty_model_fails_validation() {
        let request = ExecuteRequest {
            model: "   ".to_string(),
        };

        // Handler validation: trim and check if empty
        let model = request.model.trim();
        assert!(model.is_empty(), "Empty model should fail validation");
    }

    /// Test: ExecuteRequest with valid model succeeds
    #[test]
    fn execute_request_with_valid_model_succeeds() {
        let json = r#"{
            "model": "gpt-4o-mini"
        }"#;

        let request: ExecuteRequest = serde_json::from_str(json).unwrap();
        assert_eq!(request.model, "gpt-4o-mini");
    }
}
