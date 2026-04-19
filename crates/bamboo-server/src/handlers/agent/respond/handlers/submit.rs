use actix_web::{web, HttpResponse, Result};

use crate::app_state::AppState;

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
        reasoning_effort: req.reasoning_effort,
    };

    let (_session, user_response) =
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

    tracing::info!(
        "[{}] Response processed successfully, agent loop can resume",
        session_id
    );

    // Build resume config snapshot from server config.
    let config_snapshot = state.config.read().await.clone();
    let resume_config = crate::session_app::types::ResumeConfigSnapshot {
        provider_name: config_snapshot.provider.clone(),
        fast_model: config_snapshot.get_memory_background_model(),
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
