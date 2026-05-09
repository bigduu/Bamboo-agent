use actix_web::{web, HttpResponse, Result};

use crate::app_state::AppState;
use crate::session_app::provider_model::session_effective_model_ref;
use crate::session_app::respond::PlanModeTransition;
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

    let input = crate::session_app::types::RespondInput {
        session_id: session_id.clone(),
        user_response: user_response.clone(),
        model: req.model.clone(),
        model_ref: req.model_ref.clone(),
        provider: req.provider.clone(),
        reasoning_effort: req.reasoning_effort,
    };

    let (_session, user_response, plan_mode_transition) =
        match crate::session_app::respond::submit_pending_response(state.as_ref(), input).await {
            Ok(result) => result,
            Err(error) => {
                return match error {
                    crate::session_app::errors::RespondError::NotFound(_) => {
                        Ok(HttpResponse::NotFound().json(serde_json::json!({
                            "error": "Session not found"
                        })))
                    }
                    crate::session_app::errors::RespondError::NoPendingQuestion => {
                        Ok(HttpResponse::BadRequest().json(serde_json::json!({
                            "error": "No pending question waiting for response"
                        })))
                    }
                    crate::session_app::errors::RespondError::InvalidResponse(msg) => {
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

    if let Some(event) = plan_mode_transition.as_ref().map(|transition| match transition {
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
    }) {
        let tx = state.get_session_event_sender(&session_id).await;
        let _ = tx.send(event);
    }

    tracing::info!(
        "[{}] Response processed successfully, agent loop can resume",
        session_id
    );

    // Build resume config snapshot from server config.
    let config_snapshot = state.config.read().await.clone();
    let resolved_provider_name = session_effective_model_ref(&_session)
        .map(|model_ref| model_ref.provider)
        .unwrap_or_else(|| config_snapshot.provider.clone());
    let resolved_bg = crate::model_config_helper::resolve_background_model(
        &config_snapshot,
        &resolved_provider_name,
        &state.provider_registry,
    );
    let resume_config = crate::session_app::types::ResumeConfigSnapshot {
        provider_name: resolved_provider_name.clone(),
        fast_model: resolved_bg.as_ref().map(|m| m.model_name.clone()),
        fast_model_ref: None,
        background_model_provider: resolved_bg.map(|m| m.provider),
        disabled_tools: config_snapshot.disabled_tool_names(),
        disabled_skill_ids: config_snapshot.disabled_skill_ids(),
        image_fallback: crate::handlers::agent::execute::image_fallback::resolve_image_fallback(
            &config_snapshot,
        )
        .ok()
        .flatten(),
    };

    let auto_resume_outcome = crate::session_app::resume::resume_session_execution(
        &crate::app_state::resume_adapter::AppStateResumeRef(state),
        &session_id,
        resume_config,
    )
    .await;

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "success": true,
        "message": "Response recorded. Agent loop will continue.",
        "response": user_response,
        "auto_resume_status": auto_resume_outcome.as_str()
    })))
}
