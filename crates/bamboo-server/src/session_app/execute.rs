//! Execute use case: prepare a session for agent execution.

use bamboo_agent_core::Role;
use bamboo_domain::Session;
use bamboo_domain::reasoning::ReasoningEffort;

use super::errors::ExecutePreparationError;
use super::repository::SessionAccess;
use super::types::{
    ExecuteClientSync, ExecuteInput, ExecutePreparationOutcome, ExecuteSyncReason,
    ExecutionConfigSnapshot, ServerExecuteSnapshot,
};

/// Prepare an execute: load session, resolve model/reasoning, validate,
/// update metadata, return outcome.
///
/// The caller (handler) is responsible for runner reservation and agent spawning
/// based on the returned outcome.
pub async fn prepare_execute(
    repo: &dyn SessionAccess,
    config: ExecutionConfigSnapshot,
    input: ExecuteInput,
) -> Result<ExecutePreparationOutcome, ExecutePreparationError> {
    // ---- Load session ----
    let mut session = repo
        .load_session(&input.session_id)
        .await?
        .ok_or_else(|| ExecutePreparationError::NotFound(input.session_id.clone()))?;

    let is_child_session = session.kind == bamboo_agent_core::SessionKind::Child;
    let server_snapshot = ServerExecuteSnapshot::from_session(&session);

    // ---- Client sync check ----
    if let Some(reason) = evaluate_client_sync(input.client_sync.as_ref(), &server_snapshot) {
        return Ok(ExecutePreparationOutcome::SyncMismatch {
            reason,
            server_snapshot,
        });
    }

    // ---- Resolve model cascade: session → config default → request ----
    let session_model = normalize_model(Some(session.model.as_str()));
    let request_model = normalize_model(input.request_model.as_deref());
    let request_model_used = request_model.is_some();
    let effective_model = session_model
        .clone()
        .or_else(|| config.default_model.clone())
        .or(request_model);

    let Some(effective_model) = effective_model else {
        return Ok(ExecutePreparationOutcome::ModelRequired);
    };

    // ---- Resolve reasoning effort cascade: session → config default → request ----
    let effective_reasoning_effort = session
        .reasoning_effort
        .or(config.default_reasoning_effort)
        .or(input.request_reasoning_effort);

    // ---- Model source / reasoning source ----
    let model_source = if session_model.is_some() {
        "session"
    } else if config.default_model.is_some() {
        "provider_default"
    } else if request_model_used {
        "request"
    } else {
        "none"
    };

    let reasoning_effort_source = if session.reasoning_effort.is_some() {
        "session"
    } else if config.default_reasoning_effort.is_some() {
        "provider_default"
    } else if input.request_reasoning_effort.is_some() {
        "request"
    } else {
        "none"
    };

    // ---- Image fallback validation ----
    if let Err(error) =
        validate_image_fallback_for_session(&session, config.image_fallback.as_ref())
    {
        return Ok(ExecutePreparationOutcome::ImageFallbackError(error));
    }

    // ---- Check for pending user message ----
    if !server_snapshot.has_pending_user_message {
        return Ok(ExecutePreparationOutcome::NoPendingMessage { server_snapshot });
    }

    // ---- Update session metadata ----
    session.model = effective_model.clone();
    session.reasoning_effort = effective_reasoning_effort;

    session
        .metadata
        .insert("model_source".to_string(), model_source.to_string());

    if effective_reasoning_effort.is_some() {
        session.metadata.insert(
            "reasoning_effort_source".to_string(),
            reasoning_effort_source.to_string(),
        );
        session.metadata.insert(
            "reasoning_effort_compat".to_string(),
            effective_reasoning_effort
                .map(ReasoningEffort::as_str)
                .unwrap_or_default()
                .to_string(),
        );
    } else {
        session.metadata.remove("reasoning_effort_source");
        session.metadata.remove("reasoning_effort_compat");
    }

    // ---- Skill mode ----
    if let Some(skill_mode) = input.request_skill_mode {
        let trimmed = skill_mode.trim();
        if trimmed.is_empty() {
            session.metadata.remove("skill_mode");
        } else {
            session
                .metadata
                .insert("skill_mode".to_string(), trimmed.to_string());
        }
    }

    // ---- Consume pending conclusion with options resume ----
    consume_pending_conclusion_with_options_resume(&mut session);

    Ok(ExecutePreparationOutcome::Ready {
        session,
        effective_model,
        effective_reasoning_effort,
        model_source,
        reasoning_source: reasoning_effort_source,
        is_child_session,
    })
}

// ---- Internal helpers ----

fn normalize_model(model: Option<&str>) -> Option<String> {
    model
        .map(str::trim)
        .filter(|m| !m.is_empty() && *m != "unknown")
        .map(String::from)
}

pub fn evaluate_client_sync(
    client_sync: Option<&ExecuteClientSync>,
    server_snapshot: &ServerExecuteSnapshot,
) -> Option<ExecuteSyncReason> {
    let client_sync = client_sync?;

    let client_pending_question_tool_call_id = client_sync
        .client_pending_question_tool_call_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let server_pending_question_tool_call_id = server_snapshot
        .pending_question_tool_call_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());

    if client_sync.client_has_pending_question != server_snapshot.has_pending_question {
        return Some(ExecuteSyncReason::PendingQuestionMismatch);
    }

    if client_sync.client_has_pending_question
        && client_pending_question_tool_call_id.is_some()
        && client_pending_question_tool_call_id != server_pending_question_tool_call_id
    {
        return Some(ExecuteSyncReason::PendingQuestionMismatch);
    }

    if client_sync.client_message_count != server_snapshot.message_count {
        return Some(ExecuteSyncReason::MessageCountMismatch);
    }

    let client_last_message_id = client_sync
        .client_last_message_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let server_last_message_id = server_snapshot
        .last_message_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());

    if client_last_message_id != server_last_message_id {
        return Some(ExecuteSyncReason::LastMessageIdMismatch);
    }

    None
}

fn validate_image_fallback_for_session(
    session: &Session,
    image_fallback: Option<&bamboo_engine::ImageFallbackConfig>,
) -> Result<(), String> {
    use bamboo_engine::ImageFallbackMode;

    if matches!(
        image_fallback,
        Some(bamboo_engine::ImageFallbackConfig {
            mode: ImageFallbackMode::Error,
            ..
        })
    ) {
        let images_seen = session
            .messages
            .iter()
            .filter_map(|message| message.content_parts.as_ref())
            .flat_map(|parts| parts.iter())
            .filter(|part| {
                matches!(
                    part,
                    bamboo_agent_core::MessagePart::ImageUrl { .. }
                )
            })
            .count();

        if images_seen > 0 {
            return Err(format!(
                "This server does not currently support image inputs (found {images_seen} image part(s)). \
                 Configure hooks.image_fallback.mode='placeholder' or 'ocr' to degrade gracefully."
            ));
        }
    }

    Ok(())
}

/// Whether the session has resumable user work (pending tool response, retry, or last message is from user).
pub fn has_pending_user_message(session: &Session) -> bool {
    if has_pending_conclusion_with_options_resume(session) || has_pending_retry_resume(session) {
        return true;
    }
    session
        .messages
        .last()
        .map(|message| matches!(message.role, Role::User))
        .unwrap_or(false)
}

pub fn consume_pending_conclusion_with_options_resume(session: &mut Session) {
    session
        .metadata
        .remove("conclusion_with_options_resume_pending");
    session.metadata.remove("retry_resume_pending");
    session.metadata.remove("retry_resume_reason");
}

pub fn has_pending_conclusion_with_options_resume(session: &Session) -> bool {
    session
        .metadata
        .get("conclusion_with_options_resume_pending")
        .is_some_and(|value| value == "true")
}

pub fn has_pending_retry_resume(session: &Session) -> bool {
    session
        .metadata
        .get("retry_resume_pending")
        .is_some_and(|value| value == "true")
}

impl ServerExecuteSnapshot {
    pub fn from_session(session: &Session) -> Self {
        Self {
            message_count: session.messages.len(),
            last_message_id: session.messages.last().map(|message| message.id.clone()),
            has_pending_question: session.pending_question.is_some(),
            pending_question_tool_call_id: session
                .pending_question
                .as_ref()
                .map(|pending| pending.tool_call_id.clone()),
            has_pending_user_message: has_pending_user_message(session),
        }
    }
}
