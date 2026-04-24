//! Respond use case: submit a user response to a pending question.

use bamboo_agent_core::{PendingQuestion, Session};

use super::errors::RespondError;
use super::provider_model::{derive_model_ref, persist_legacy_model_provider, persist_model_ref};
use super::repository::SessionAccess;
use super::types::RespondInput;

const CONCLUSION_WITH_OPTIONS_RESUME_PENDING_KEY: &str = "conclusion_with_options_resume_pending";

/// Submit a pending response: load session, validate, update messages,
/// persist, and return the updated session.
///
/// The caller (handler) is responsible for auto-resume triggering.
pub async fn submit_pending_response(
    repo: &dyn SessionAccess,
    input: RespondInput,
) -> Result<(Session, String), RespondError> {
    // ---- Load session (merged for respond to pick up in-memory pending question) ----
    let mut session = repo
        .load_merged(&input.session_id)
        .await?
        .ok_or_else(|| RespondError::NotFound(input.session_id.clone()))?;

    // ---- Take pending question ----
    let pending = session
        .pending_question
        .take()
        .ok_or(RespondError::NoPendingQuestion)?;

    // ---- Validate response ----
    if let Err(error_message) = validate_pending_response(&pending, &input.user_response) {
        // Put the pending question back when validation fails.
        session.pending_question = Some(pending);
        return Err(RespondError::InvalidResponse(error_message));
    }

    let tool_call_id = pending.tool_call_id.clone();
    tracing::debug!(
        "[{}] Looking for tool result message with tool_call_id: {}",
        input.session_id,
        tool_call_id
    );

    // ---- Update or append tool result message ----
    let found =
        update_or_append_tool_result_message(&mut session, &tool_call_id, &input.user_response);
    if found {
        tracing::info!(
            "[{}] Updated existing tool result message",
            input.session_id
        );
    } else {
        tracing::warn!(
            "[{}] Tool result message not found for tool_call_id: {}, added fallback message",
            input.session_id,
            tool_call_id
        );
    }

    // ---- Clear pending question and set resume marker ----
    session.clear_pending_question();
    session.metadata.insert(
        CONCLUSION_WITH_OPTIONS_RESUME_PENDING_KEY.to_string(),
        "true".to_string(),
    );

    // ---- Merge model/reasoning from request ----
    let request_model_ref = derive_model_ref(
        input.model_ref.as_ref(),
        input.provider.as_deref(),
        input.model.as_deref(),
    );
    if let Some(model_ref) = request_model_ref.as_ref() {
        persist_model_ref(&mut session, model_ref);
    } else {
        persist_legacy_model_provider(
            &mut session,
            input.model.as_deref(),
            input.provider.as_deref(),
        );
    }
    if let Some(reasoning_effort) = input.reasoning_effort {
        session.reasoning_effort = Some(reasoning_effort);
    }

    // ---- Save ----
    repo.save_and_cache(&session).await?;

    tracing::info!(
        "[{}] Response processed successfully, agent loop can resume",
        input.session_id
    );

    Ok((session, input.user_response))
}

// ---- Internal helpers ----

pub fn validate_pending_response(
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

pub fn update_or_append_tool_result_message(
    session: &mut Session,
    tool_call_id: &str,
    user_response: &str,
) -> bool {
    for message in &mut session.messages {
        if message.tool_call_id.as_deref() == Some(tool_call_id) {
            message.content = selected_message_content(user_response);
            message.tool_success = Some(true);
            return true;
        }
    }

    session.add_message(bamboo_agent_core::Message::tool_result_with_status(
        tool_call_id,
        selected_message_content(user_response),
        true,
    ));
    false
}

fn selected_message_content(user_response: &str) -> String {
    format!("User selected: {}", user_response)
}
