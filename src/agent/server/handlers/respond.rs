use actix_web::{web, HttpResponse, Result};
use serde::Deserialize;

use crate::state::AppState;
use agent_core::Message;

#[derive(Debug, Deserialize)]
pub struct RespondRequest {
    /// The user's response - either one of the options or custom input
    pub response: String,
}

/// Submit a user response to a pending question from ask_user tool
pub async fn submit_response(
    state: web::Data<AppState>,
    session_id: web::Path<String>,
    req: web::Json<RespondRequest>,
) -> Result<HttpResponse> {
    let session_id = session_id.into_inner();
    let user_response = req.response.clone();

    log::info!("[{}] Received user response: {}", session_id, user_response);

    // Try to get session from memory first, then from storage
    let session = {
        let sessions = state.sessions.read().await;
        sessions.get(&session_id).cloned()
    };

    let mut session = match session {
        Some(s) => s,
        None => match state.storage.load_session(&session_id).await {
            Ok(Some(session)) => {
                // Load into memory for future requests
                let mut sessions = state.sessions.write().await;
                sessions.insert(session_id.clone(), session.clone());
                session
            }
            _ => {
                return Ok(HttpResponse::NotFound().json(serde_json::json!({
                    "error": "Session not found"
                })));
            }
        }
    };

    // Check if there's a pending question
    let pending = match session.pending_question.take() {
        Some(p) => p,
        None => {
            return Ok(HttpResponse::BadRequest().json(serde_json::json!({
                "error": "No pending question waiting for response"
            })));
        }
    };

    // Validate response if custom input is not allowed
    if !pending.allow_custom {
        let valid = pending.options.iter().any(|opt| opt == &user_response);
        if !valid {
            let options_str = pending.options.join(", ");
            // Put the pending question back
            session.pending_question = Some(pending);
            return Ok(HttpResponse::BadRequest().json(serde_json::json!({
                "error": "Invalid response",
                "message": format!("Response must be one of: {}", options_str)
            })));
        }
    }

    // Find and update the existing tool result message (the placeholder added by ask_user)
    let tool_call_id = pending.tool_call_id.clone();
    log::debug!("[{}] Looking for tool result message with tool_call_id: {}", session_id, tool_call_id);
    log::debug!("[{}] Session has {} messages", session_id, session.messages.len());

    let mut found = false;
    for (idx, message) in session.messages.iter_mut().enumerate() {
        log::debug!("[{}] Message {}: role={:?}, tool_call_id={:?}",
            session_id, idx, message.role, message.tool_call_id);
        if let Some(id) = &message.tool_call_id {
            if id == &tool_call_id {
                // Update the placeholder message with actual user response
                log::info!("[{}] Found tool result message at index {}, updating content", session_id, idx);
                message.content = format!("User selected: {}", user_response);
                found = true;
                break;
            }
        }
    }

    if !found {
        // Fallback: if no existing tool result found, add a new one
        // This shouldn't happen in normal flow, but handles edge cases
        log::warn!("[{}] Tool result message not found for tool_call_id: {}, adding new one", session_id, tool_call_id);
        session.add_message(Message::tool_result(
            tool_call_id,
            format!("User selected: {}", user_response),
        ));
    }

    // Also add a user message to record the choice
    session.add_message(Message::user(format!(
        "I chose '{}' in response to: {}",
        user_response, pending.question
    )));

    // Clear the pending question
    session.clear_pending_question();

    // Save the session
    if let Err(e) = state.storage.save_session(&session).await {
        log::warn!("[{}] Failed to save session after response: {}", session_id, e);
    }

    // Update in-memory session
    {
        let mut sessions = state.sessions.write().await;
        sessions.insert(session_id.clone(), session);
    }

    log::info!("[{}] Response processed successfully, agent loop can resume", session_id);

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "success": true,
        "message": "Response recorded. Agent loop will continue.",
        "response": user_response
    })))
}

/// Get the pending question for a session (if any)
pub async fn get_pending_question(
    state: web::Data<AppState>,
    session_id: web::Path<String>,
) -> Result<HttpResponse> {
    let session_id = session_id.into_inner();

    // Try to get session from memory first, then from storage
    let session = {
        let sessions = state.sessions.read().await;
        sessions.get(&session_id).cloned()
    };

    let session = match session {
        Some(s) => s,
        None => match state.storage.load_session(&session_id).await {
            Ok(Some(session)) => {
                // Load into memory for future requests
                let mut sessions = state.sessions.write().await;
                sessions.insert(session_id.clone(), session.clone());
                session
            }
            _ => {
                return Ok(HttpResponse::NotFound().json(serde_json::json!({
                    "error": "Session not found"
                })));
            }
        }
    };

    match session.pending_question {
        Some(pending) => Ok(HttpResponse::Ok().json(serde_json::json!({
            "has_pending_question": true,
            "question": pending.question,
            "options": pending.options,
            "allow_custom": pending.allow_custom,
            "tool_call_id": pending.tool_call_id
        }))),
        None => Ok(HttpResponse::Ok().json(serde_json::json!({
            "has_pending_question": false
        })))
    }
}
