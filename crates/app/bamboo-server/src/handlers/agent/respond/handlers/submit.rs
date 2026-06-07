use actix_web::{web, HttpResponse, Result};

use crate::app_state::AppState;
use bamboo_engine::session_app::respond::PlanModeTransition;
use bamboo_agent_core::AgentEvent;

use super::super::types::RespondRequest;

/// Submit a user response to a pending question from the `conclusion_with_options` tool.
///
/// When the agent calls the `conclusion_with_options` tool, it pauses execution and waits
/// for user input. This endpoint submits the user's response, allowing
/// the agent to resume execution.
///
/// # HTTP Method
///
/// `POST /api/v1/sessions/{session_id}/respond`
pub async fn submit_response(
    state: web::Data<AppState>,
    session_id: web::Path<String>,
    req: web::Json<RespondRequest>,
) -> Result<HttpResponse> {
    let session_id = session_id.into_inner();
    let user_response = req.response.clone();

    tracing::info!("[{}] Received user response: {}", session_id, user_response);

    let input = bamboo_engine::session_app::types::RespondInput {
        session_id: session_id.clone(),
        user_response: user_response.clone(),
        model: req.model.clone(),
        model_ref: req.model_ref.clone(),
        provider: req.provider.clone(),
        reasoning_effort: req.reasoning_effort,
    };

    let (_session, user_response, plan_mode_transition) =
        match bamboo_engine::session_app::respond::submit_pending_response(state.as_ref(), input).await {
            Ok(result) => result,
            Err(error) => {
                return match error {
                    bamboo_engine::session_app::errors::RespondError::NotFound(_) => {
                        Ok(HttpResponse::NotFound().json(serde_json::json!({
                            "error": "Session not found"
                        })))
                    }
                    bamboo_engine::session_app::errors::RespondError::NoPendingQuestion => {
                        Ok(HttpResponse::BadRequest().json(serde_json::json!({
                            "error": "No pending question waiting for response"
                        })))
                    }
                    bamboo_engine::session_app::errors::RespondError::InvalidResponse(msg) => {
                        Ok(HttpResponse::BadRequest().json(serde_json::json!({
                            "error": "Invalid response",
                            "message": msg,
                        })))
                    }
                    _ => Ok(HttpResponse::InternalServerError().json(serde_json::json!({
                        "error": format!("Response submission failed: {error}")
                    }))),
                };
            }
        };

    if let Some(event) = plan_mode_transition
        .as_ref()
        .map(|transition| match transition {
            PlanModeTransition::Entered {
                reason,
                pre_permission_mode,
                entered_at,
                status,
                plan_file_path,
            } => AgentEvent::PlanModeEntered {
                session_id: session_id.clone(),
                reason: reason.clone(),
                pre_permission_mode: pre_permission_mode.clone(),
                entered_at: *entered_at,
                status: *status,
                plan_file_path: plan_file_path.clone(),
            },
            PlanModeTransition::Exited {
                approved,
                restored_mode,
                plan,
            } => AgentEvent::PlanModeExited {
                session_id: session_id.clone(),
                approved: *approved,
                restored_mode: restored_mode.clone(),
                plan: plan.clone(),
            },
        })
    {
        let tx = state.get_session_event_sender(&session_id).await;
        let _ = tx.send(event);
    }

    tracing::info!(
        "[{}] Response processed successfully, agent loop can resume",
        session_id
    );

    // Build resume config snapshot from server config via the single-source-of-
    // truth resolver (provider-name derivation + global auxiliary models + gold).
    let config_snapshot = state.config.read().await.clone();
    let resume_config = bamboo_engine::session_app::resolution::resolve_resume_config_snapshot(
        &config_snapshot,
        &state.provider_registry,
        &_session,
        None,
    );

    let auto_resume_outcome = bamboo_engine::session_app::resume::resume_session_execution(
        &crate::app_state::resume_adapter::AppStateResumeRef(state),
        &session_id,
        resume_config,
    )
    .await;

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "success": true,
        "message": "Response recorded. Agent loop will continue.",
        "response": user_response,
        "auto_resume_status": auto_resume_outcome.status_str(),
        "run_id": auto_resume_outcome.run_id()
    })))
}
