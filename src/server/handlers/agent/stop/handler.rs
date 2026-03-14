use actix_web::{web, HttpResponse, Responder};

use super::types::StopResponse;
use crate::server::app_state::{AgentStatus, AppState};

/// Stop a running agent execution.
///
/// `POST /api/v1/stop/{session_id}`
pub async fn handler(state: web::Data<AppState>, path: web::Path<String>) -> impl Responder {
    let session_id = path.into_inner();
    log::info!("[{}] Stop request received", session_id);

    let runner_cancelled = cancel_running_runner(&state, &session_id).await;
    let legacy_cancelled = cancel_legacy_token(&state, &session_id).await;

    if runner_cancelled || legacy_cancelled {
        mark_runner_cancelled(&state, &session_id).await;
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

async fn cancel_running_runner(state: &web::Data<AppState>, session_id: &str) -> bool {
    let runners = state.agent_runners.read().await;
    let Some(runner) = runners.get(session_id) else {
        return false;
    };

    if !matches!(runner.status, AgentStatus::Running) {
        log::warn!(
            "[{}] Runner not in Running status: {:?}",
            session_id,
            runner.status
        );
        return false;
    }

    runner.cancel_token.cancel();
    log::info!("[{}] Runner cancellation triggered", session_id);
    true
}

async fn cancel_legacy_token(state: &web::Data<AppState>, session_id: &str) -> bool {
    let mut tokens = state.cancel_tokens.write().await;
    let Some(token) = tokens.get(session_id) else {
        return false;
    };
    token.cancel();
    tokens.remove(session_id);
    log::info!("[{}] Legacy cancellation triggered", session_id);
    true
}

async fn mark_runner_cancelled(state: &web::Data<AppState>, session_id: &str) {
    let mut runners = state.agent_runners.write().await;
    if let Some(runner) = runners.get_mut(session_id) {
        runner.status = AgentStatus::Cancelled;
        runner.completed_at = Some(chrono::Utc::now());
    }
}
