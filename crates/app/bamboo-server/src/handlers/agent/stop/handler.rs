use actix_web::{web, HttpRequest, HttpResponse, Responder, ResponseError};

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

/// Additive HTTP wrapper: a delayed selected-execution cancel must not look
/// up a newer owner or fall through to the legacy token table.
pub async fn handler_with_run(
    state: web::Data<AppState>,
    http: HttpRequest,
    path: web::Path<String>,
) -> HttpResponse {
    let Some(header) = http.headers().get("Bamboo-Run-Id") else {
        return handler(state, path)
            .await
            .respond_to(&http)
            .map_into_boxed_body();
    };
    let run_id = match header
        .to_str()
        .ok()
        .filter(|s| !s.is_empty() && s.len() <= 128 && s.bytes().all(|b| b.is_ascii_graphic()))
    {
        Some(value) if http.headers().get_all("Bamboo-Run-Id").count() == 1 => value,
        _ => {
            return crate::error::json_error(
                actix_web::http::StatusCode::BAD_REQUEST,
                "Invalid Bamboo-Run-Id",
            )
        }
    };
    let id = path.into_inner();
    if let Err(error) =
        crate::handlers::agent::tickets::require_supervisor_owner(&state, &http, &id).await
    {
        return crate::handlers::agent::tickets::TicketHttpError::from(error).error_response();
    }
    if cancel_exact_run(&state, &id, run_id).await {
        HttpResponse::Ok().json(serde_json::json!({"success":true,"session_id":id,"run_id":run_id}))
    } else {
        crate::error::json_error(
            actix_web::http::StatusCode::CONFLICT,
            "Requested run is not the current cancellable owner",
        )
    }
}

pub(crate) async fn cancel_exact_run(state: &web::Data<AppState>, id: &str, run_id: &str) -> bool {
    let runners = state.agent_runners.read().await;
    let Some(runner) = runners.get(id).filter(|r| r.run_id == run_id) else {
        return false;
    };
    match runner.status {
        AgentStatus::Pending | AgentStatus::Running => runner.cancel_token.cancel(),
        AgentStatus::Cancelled => return true,
        _ => return false,
    }
    drop(runners);
    mark_runner_cancelled(state, id, Some(run_id)).await;
    true
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
