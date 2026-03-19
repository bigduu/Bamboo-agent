use actix_web::{body, web, HttpResponse, Result};

use crate::agent::core::agent::types::PendingQuestion;
use crate::agent::core::Session;
use crate::server::app_state::AppState;

use super::super::session::load_session_from_memory_or_storage;
use super::super::types::RespondRequest;

const ASK_USER_RESUME_PENDING_KEY: &str = "ask_user_resume_pending";

/// Submit a user response to a pending question from the `ask_user` tool.
///
/// When the agent calls the `ask_user` tool, it pauses execution and waits
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
    let requested_model = req.model.clone();

    log::info!("[{}] Received user response: {}", session_id, user_response);

    let Some(mut session) = load_session_from_memory_or_storage(&state, &session_id).await else {
        return Ok(HttpResponse::NotFound().json(serde_json::json!({
            "error": "Session not found"
        })));
    };

    let pending = match session.pending_question.take() {
        Some(pending) => pending,
        None => {
            return Ok(HttpResponse::BadRequest().json(serde_json::json!({
                "error": "No pending question waiting for response"
            })));
        }
    };

    if let Err(error_message) = validate_pending_response(&pending, &user_response) {
        // Put the pending question back when validation fails.
        session.pending_question = Some(pending);
        return Ok(HttpResponse::BadRequest().json(serde_json::json!({
            "error": "Invalid response",
            "message": error_message,
        })));
    }

    let tool_call_id = pending.tool_call_id.clone();
    log::debug!(
        "[{}] Looking for tool result message with tool_call_id: {}",
        session_id,
        tool_call_id
    );
    log::debug!(
        "[{}] Session has {} messages",
        session_id,
        session.messages.len()
    );

    let found = update_or_append_tool_result_message(&mut session, &tool_call_id, &user_response);
    if found {
        log::info!("[{}] Updated existing tool result message", session_id);
    } else {
        log::warn!(
            "[{}] Tool result message not found for tool_call_id: {}, added fallback message",
            session_id,
            tool_call_id
        );
    }

    session.clear_pending_question();
    session
        .metadata
        .insert(ASK_USER_RESUME_PENDING_KEY.to_string(), "true".to_string());

    if let Err(error) = state.storage.save_session(&session).await {
        log::warn!(
            "[{}] Failed to save session after response: {}",
            session_id,
            error
        );
    }

    {
        let mut sessions = state.sessions.write().await;
        sessions.insert(session_id.clone(), session);
    }

    log::info!(
        "[{}] Response processed successfully, agent loop can resume",
        session_id
    );

    let auto_resume_status =
        trigger_auto_resume_if_requested(state.clone(), &session_id, requested_model).await;

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "success": true,
        "message": "Response recorded. Agent loop will continue.",
        "response": user_response,
        "auto_resume_status": auto_resume_status
    })))
}

async fn trigger_auto_resume_if_requested(
    state: web::Data<AppState>,
    session_id: &str,
    requested_model: Option<String>,
) -> String {
    let Some(model) = requested_model.map(|model| model.trim().to_string()) else {
        return "not_requested".to_string();
    };
    if model.is_empty() {
        return "invalid_model".to_string();
    }

    let response = crate::server::handlers::agent::execute::handler(
        state,
        web::Path::from(session_id.to_string()),
        web::Json(crate::server::handlers::agent::execute::ExecuteRequest {
            model,
            reasoning_effort: None,
        }),
    )
    .await;

    extract_execute_status(response).await
}

async fn extract_execute_status(response: HttpResponse) -> String {
    let status_code = response.status();
    let body = response.into_body();
    let Ok(bytes) = body::to_bytes(body).await else {
        return fallback_execute_status(status_code);
    };

    let Ok(value) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
        return fallback_execute_status(status_code);
    };

    value
        .get("status")
        .and_then(|status| status.as_str())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| fallback_execute_status(status_code))
}

fn fallback_execute_status(status_code: actix_web::http::StatusCode) -> String {
    if status_code == actix_web::http::StatusCode::ACCEPTED {
        return "started".to_string();
    }
    if status_code.is_success() {
        return "completed".to_string();
    }
    "error".to_string()
}

pub(super) fn validate_pending_response(
    pending: &PendingQuestion,
    user_response: &str,
) -> Result<(), String> {
    if pending.allow_custom {
        return Ok(());
    }

    let valid = pending.options.iter().any(|option| option == user_response);
    if valid {
        Ok(())
    } else {
        let options_str = pending.options.join(", ");
        Err(format!("Response must be one of: {options_str}"))
    }
}

pub(super) fn update_or_append_tool_result_message(
    session: &mut Session,
    tool_call_id: &str,
    user_response: &str,
) -> bool {
    for message in &mut session.messages {
        if message.tool_call_id.as_deref() == Some(tool_call_id) {
            message.content = selected_message_content(user_response);
            return true;
        }
    }

    session.add_message(crate::agent::core::Message::tool_result(
        tool_call_id,
        selected_message_content(user_response),
    ));
    false
}

fn selected_message_content(user_response: &str) -> String {
    format!("User selected: {}", user_response)
}
