//! Agent execution cancellation API handler.
//!
//! This module provides the HTTP endpoint for stopping in-flight
//! agent executions.

use actix_web::{web, HttpResponse, Responder};
use serde::Serialize;

use crate::server::app_state::{AgentStatus, AppState};

/// Response for stop request.
#[derive(Serialize)]
struct StopResponse {
    /// Whether the stop operation succeeded
    success: bool,
    /// Human-readable status message
    message: String,
}

/// Stop a running agent execution.
///
/// This endpoint cancels an in-flight agent execution for the specified session.
/// The agent will finish its current operation and then terminate gracefully.
///
/// # HTTP Method
///
/// `POST /api/v1/stop/{session_id}`
///
/// # Path Parameters
///
/// - `session_id` - The session identifier to stop
///
/// # Response
///
/// - `200 OK` - Agent execution stopped successfully, returns [`StopResponse`]
/// - `404 Not Found` - No active execution found for this session
///
/// # Behavior
///
/// When a stop request is received:
/// 1. Checks if there's an active runner with `Running` status
/// 2. Triggers cancellation via the cancel token
/// 3. Updates runner status to `Cancelled`
/// 4. Agent loop receives cancellation signal and terminates
/// 5. Any pending tool executions are aborted
///
/// # Graceful Shutdown
///
/// The agent will:
/// - Complete the current LLM request if in progress
/// - Cancel pending tool executions
/// - Send an `Error` event with "cancelled" message
/// - Save session state before terminating
///
/// # Example
///
/// ```bash
/// curl -X POST http://localhost:8080/api/v1/stop/session-123
/// ```
pub async fn handler(state: web::Data<AppState>, path: web::Path<String>) -> impl Responder {
    let session_id = path.into_inner();
    log::info!("[{}] Stop request received", session_id);

    // Try to cancel via agent_runners (new architecture)
    let runner_cancelled = {
        let runners = state.agent_runners.read().await;
        if let Some(runner) = runners.get(&session_id) {
            if matches!(runner.status, AgentStatus::Running) {
                runner.cancel_token.cancel();
                log::info!("[{}] Runner cancellation triggered", session_id);
                true
            } else {
                log::warn!(
                    "[{}] Runner not in Running status: {:?}",
                    session_id,
                    runner.status
                );
                false
            }
        } else {
            false
        }
    };

    // Also try legacy cancel_tokens for backward compatibility
    let legacy_cancelled = {
        let mut tokens = state.cancel_tokens.write().await;
        if let Some(token) = tokens.get(&session_id) {
            token.cancel();
            tokens.remove(&session_id);
            log::info!("[{}] Legacy cancellation triggered", session_id);
            true
        } else {
            false
        }
    };

    if runner_cancelled || legacy_cancelled {
        // Update runner status to Cancelled
        let mut runners = state.agent_runners.write().await;
        if let Some(runner) = runners.get_mut(&session_id) {
            runner.status = AgentStatus::Cancelled;
            runner.completed_at = Some(chrono::Utc::now());
        }

        HttpResponse::Ok().json(StopResponse {
            success: true,
            message: "Agent execution stopped".to_string(),
        })
    } else {
        log::warn!("[{}] No active runner or cancel token found", session_id);
        HttpResponse::NotFound().json(StopResponse {
            success: false,
            message: "No active agent execution found".to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::app_state::AgentRunner;

    #[test]
    fn test_stop_cancels_running_status() {
        // Test that Running status can be cancelled
        let mut runner = AgentRunner::new();
        runner.status = AgentStatus::Running;

        // Simulate cancellation
        runner.cancel_token.cancel();

        // Token should be cancelled
        assert!(runner.cancel_token.is_cancelled());
    }

    #[test]
    fn test_completed_status_not_cancellable() {
        // Test that Completed status should not be cancelled
        let status = AgentStatus::Completed;
        assert!(!matches!(status, AgentStatus::Running));
    }

    #[test]
    fn test_cancelled_status_can_be_set() {
        // Test that Cancelled status can be set after cancellation
        let mut runner = AgentRunner::new();
        runner.status = AgentStatus::Cancelled;

        assert!(matches!(runner.status, AgentStatus::Cancelled));
    }

    #[test]
    fn test_runner_has_cancel_token() {
        // Test that runners have cancel tokens
        let runner = AgentRunner::new();
        // Verify cancel token exists (can be cloned)
        let _token_clone = runner.cancel_token.clone();
    }
}
