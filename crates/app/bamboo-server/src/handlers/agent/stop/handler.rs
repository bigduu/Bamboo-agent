use actix_web::{web, HttpResponse, Responder};

use super::types::StopResponse;
use crate::app_state::{AgentStatus, AppState};

/// Stop a running agent execution.
///
/// `POST /api/v1/stop/{session_id}`
pub async fn handler(state: web::Data<AppState>, path: web::Path<String>) -> impl Responder {
    let session_id = path.into_inner();
    tracing::info!("[{}] Stop request received", session_id);

    if cancel_session(&state, &session_id).await {
        HttpResponse::Ok().json(StopResponse {
            success: true,
            message: "Agent execution stopped".to_string(),
        })
    } else {
        tracing::warn!("[{}] No active runner or cancel token found", session_id);
        HttpResponse::NotFound().json(StopResponse {
            success: false,
            message: "No active agent execution found".to_string(),
        })
    }
}

/// Cancel a running agent session through the shared v1 HTTP / v2 WebSocket
/// control path, returning whether an active cancellation was acknowledged.
pub(crate) async fn cancel_session(state: &web::Data<AppState>, session_id: &str) -> bool {
    match cancel_running_runner(state, session_id).await {
        RunnerCancelOutcome::Triggered { run_id } => {
            // The authoritative token has already fired. Do not queue this
            // modern stop behind the unrelated legacy token registry.
            mark_runner_cancelled(state, session_id, Some(&run_id)).await;
            true
        }
        // Retried control frames must acknowledge the same exact stop without
        // falling through to unrelated legacy bookkeeping.
        RunnerCancelOutcome::AlreadyCancelled => true,
        RunnerCancelOutcome::NotActive => {
            let legacy_cancelled = cancel_legacy_token(state, session_id).await;
            if legacy_cancelled {
                mark_runner_cancelled(state, session_id, None).await;
            }
            legacy_cancelled
        }
    }
}

enum RunnerCancelOutcome {
    Triggered { run_id: String },
    AlreadyCancelled,
    NotActive,
}

async fn cancel_running_runner(
    state: &web::Data<AppState>,
    session_id: &str,
) -> RunnerCancelOutcome {
    let runners = state.agent_runners.read().await;
    let Some(runner) = runners.get(session_id) else {
        return RunnerCancelOutcome::NotActive;
    };

    match runner.status {
        AgentStatus::Running => {
            runner.cancel_token.cancel();
            tracing::info!("[{}] Runner cancellation triggered", session_id);
            RunnerCancelOutcome::Triggered {
                run_id: runner.run_id.clone(),
            }
        }
        AgentStatus::Cancelled => RunnerCancelOutcome::AlreadyCancelled,
        _ => {
            tracing::warn!(
                "[{}] Runner not in Running status: {:?}",
                session_id,
                runner.status
            );
            RunnerCancelOutcome::NotActive
        }
    }
}

async fn cancel_legacy_token(state: &web::Data<AppState>, session_id: &str) -> bool {
    let mut tokens = state.cancel_tokens.write().await;
    let Some(token) = tokens.get(session_id) else {
        return false;
    };
    token.cancel();
    tokens.remove(session_id);
    tracing::info!("[{}] Legacy cancellation triggered", session_id);
    true
}

pub(super) async fn mark_runner_cancelled(
    state: &web::Data<AppState>,
    session_id: &str,
    expected_run_id: Option<&str>,
) {
    let mut runners = state.agent_runners.write().await;
    if let Some(runner) = runners.get_mut(session_id) {
        if expected_run_id.is_some_and(|run_id| runner.run_id != run_id) {
            return;
        }
        runner.status = AgentStatus::Cancelled;
        runner.completed_at = Some(chrono::Utc::now());
    }
}
