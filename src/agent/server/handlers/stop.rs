use actix_web::{web, HttpResponse, Responder};
use serde::Serialize;

use crate::state::{AgentStatus, AppState};

#[derive(Serialize)]
struct StopResponse {
    success: bool,
    message: String,
}

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
                log::warn!("[{}] Runner not in Running status: {:?}", session_id, runner.status);
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
    use crate::state::AgentRunner;

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
