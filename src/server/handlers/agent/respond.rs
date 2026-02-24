//! User response API handler for interactive agent questions.
//!
//! This module provides HTTP endpoints for submitting user responses
//! when the agent asks questions via the `ask_user` tool.

use actix_web::{web, HttpResponse, Result};
use serde::Deserialize;

use crate::agent::core::Message;
use crate::server::app_state::AppState;

/// Request payload for submitting a user response.
///
/// # Fields
///
/// * `response` - The user's response text or selected option
///
/// # Example
///
/// ```json
/// {
///   "response": "Option 1"
/// }
/// ```
#[derive(Debug, Deserialize)]
pub struct RespondRequest {
    /// The user's response - either one of the options or custom input
    pub response: String,
}

/// Submit a user response to a pending question from the `ask_user` tool.
///
/// When the agent calls the `ask_user` tool, it pauses execution and waits
/// for user input. This endpoint submits the user's response, allowing
/// the agent to resume execution.
///
/// # HTTP Method
///
/// `POST /api/v1/sessions/{session_id}/respond`
///
/// # Path Parameters
///
/// - `session_id` - The session identifier with a pending question
///
/// # Request Body
///
/// JSON-encoded [`RespondRequest`] containing the user's response
///
/// # Response
///
/// - `200 OK` - Response recorded successfully
/// - `400 Bad Request` - No pending question or invalid response
/// - `404 Not Found` - Session does not exist
///
/// # Response Validation
///
/// If the pending question has predefined options and `allow_custom` is false,
/// the response must match one of the provided options exactly.
///
/// # Agent Behavior
///
/// After receiving a response:
/// 1. The pending question is cleared
/// 2. The tool result is updated with the user's choice
/// 3. A user message is added to the conversation
/// 4. The session is saved
/// 5. The agent loop resumes execution
///
/// # Example
///
/// ```bash
/// curl -X POST http://localhost:8080/api/v1/sessions/session-123/respond \
///   -H "Content-Type: application/json" \
///   -d '{"response": "Use TypeScript"}'
/// ```
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
        },
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

    let mut found = false;
    for (idx, message) in session.messages.iter_mut().enumerate() {
        log::debug!(
            "[{}] Message {}: role={:?}, tool_call_id={:?}",
            session_id,
            idx,
            message.role,
            message.tool_call_id
        );
        if let Some(id) = &message.tool_call_id {
            if id == &tool_call_id {
                // Update the placeholder message with actual user response
                log::info!(
                    "[{}] Found tool result message at index {}, updating content",
                    session_id,
                    idx
                );
                message.content = format!("User selected: {}", user_response);
                found = true;
                break;
            }
        }
    }

    if !found {
        // Fallback: if no existing tool result found, add a new one
        // This shouldn't happen in normal flow, but handles edge cases
        log::warn!(
            "[{}] Tool result message not found for tool_call_id: {}, adding new one",
            session_id,
            tool_call_id
        );
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
        log::warn!(
            "[{}] Failed to save session after response: {}",
            session_id,
            e
        );
    }

    // Update in-memory session
    {
        let mut sessions = state.sessions.write().await;
        sessions.insert(session_id.clone(), session);
    }

    log::info!(
        "[{}] Response processed successfully, agent loop can resume",
        session_id
    );

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "success": true,
        "message": "Response recorded. Agent loop will continue.",
        "response": user_response
    })))
}

/// Get the pending question for a session (if any).
///
/// This endpoint retrieves the current pending question that the agent
/// is waiting for the user to answer.
///
/// # HTTP Method
///
/// `GET /api/v1/sessions/{session_id}/question`
///
/// # Path Parameters
///
/// - `session_id` - The session identifier
///
/// # Response
///
/// Returns a JSON object with pending question details if one exists.
///
/// # Response Format (Pending Question)
///
/// ```json
/// {
///   "has_pending_question": true,
///   "question": "Which language should I use?",
///   "options": ["TypeScript", "JavaScript", "Python"],
///   "allow_custom": false,
///   "tool_call_id": "call_123"
/// }
/// ```
///
/// # Response Format (No Pending Question)
///
/// ```json
/// {
///   "has_pending_question": false
/// }
/// ```
///
/// # Use Case
///
/// This endpoint is useful for:
/// - Checking if user input is required before displaying a UI
/// - Polling for pending questions in automated workflows
/// - Debugging agent state
///
/// # Example
///
/// ```bash
/// curl http://localhost:8080/api/v1/sessions/session-123/question
/// ```
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
        },
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
        }))),
    }
}
