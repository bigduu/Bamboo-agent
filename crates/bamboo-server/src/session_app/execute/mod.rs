//! Execute use case: prepare a session for agent execution.

use bamboo_domain::reasoning::ReasoningEffort;
use bamboo_domain::Session;

use super::errors::ExecutePreparationError;
use super::provider_model::{
    persist_legacy_model_provider, persist_model_ref, session_effective_model_ref,
};
use super::repository::SessionAccess;
use super::types::{
    ExecuteInput, ExecutePreparationOutcome, ExecutionConfigSnapshot, ServerExecuteSnapshot,
};

mod billing;
mod resume_markers;
mod sync;
mod validation;

#[cfg(test)]
mod tests;

pub use billing::{billable_user_turn_count, is_billable_user_turn, is_system_resume_message};
pub use resume_markers::{
    consume_pending_clarification_resume, consume_pending_conclusion_with_options_resume,
    has_pending_clarification_resume, has_pending_conclusion_with_options_resume,
    has_pending_retry_resume, has_pending_user_message,
};
pub use sync::evaluate_client_sync;

pub(crate) use sync::is_hidden_from_ui;

use validation::validate_image_fallback_for_session;

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
        match input.client_sync.as_ref() {
            Some(cs) => tracing::debug!(
                "[{}] Execute sync MISMATCH reason={:?}: client(count={}, last_id={:?}, pending_q={}, pq_tool={:?}) vs server(count={}, last_id={:?}, pending_q={}, pq_tool={:?}); total_messages_in_session={}",
                input.session_id,
                reason,
                cs.client_message_count,
                cs.client_last_message_id,
                cs.client_has_pending_question,
                cs.client_pending_question_tool_call_id,
                server_snapshot.message_count,
                server_snapshot.last_message_id,
                server_snapshot.has_pending_question,
                server_snapshot.pending_question_tool_call_id,
                session.messages.len(),
            ),
            None => tracing::debug!(
                "[{}] Execute sync MISMATCH reason={:?} but no client_sync was sent",
                input.session_id,
                reason
            ),
        }
        return Ok(ExecutePreparationOutcome::SyncMismatch {
            reason,
            server_snapshot,
        });
    }

    // ---- Resolve model cascade ----
    // Flag ON (new): session.model_ref → request.model_ref → config.default_model_ref
    // Flag OFF (old): session.model → config.default_model → request.model
    let (effective_model_ref, effective_model, model_source) = if config.provider_model_ref_enabled
    {
        resolve_model_ref_cascade(&session, &input, &config)
    } else {
        let (effective_model, model_source) = resolve_model_cascade(&session, &input, &config);
        (None, effective_model, model_source)
    };

    let Some(effective_model) = effective_model else {
        return Ok(ExecutePreparationOutcome::ModelRequired);
    };

    // ---- Resolve reasoning effort cascade: session → request → provider default ----
    // Single shared cascade (see `bamboo_engine::model_areas`). Stays `Option` so
    // non-reasoning models send no reasoning parameter.
    let (effective_reasoning_effort, reasoning_effort_source) = {
        let (effort, source) = bamboo_engine::model_areas::resolve_effective_reasoning_effort(
            session.reasoning_effort,
            input.request_reasoning_effort,
            config.default_reasoning_effort,
        );
        (effort, source.as_str())
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
    if let Some(model_ref) = effective_model_ref.as_ref() {
        persist_model_ref(&mut session, model_ref);
    } else {
        persist_legacy_model_provider(
            &mut session,
            Some(effective_model.as_str()),
            Some(config.provider_name.as_str()),
        );
    }
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

    // ---- Consume pending clarification resume markers ----
    consume_pending_clarification_resume(&mut session);

    Ok(ExecutePreparationOutcome::Ready {
        session: Box::new(session),
        effective_model,
        effective_reasoning_effort,
        model_source,
        reasoning_source: reasoning_effort_source,
        is_child_session,
    })
}

/// Old-path model resolution: session.model → config.default_model → request.model
pub(crate) fn resolve_model_cascade(
    session: &Session,
    input: &ExecuteInput,
    config: &ExecutionConfigSnapshot,
) -> (Option<String>, &'static str) {
    let session_model = normalize_model(Some(session.model.as_str()));
    let request_model = normalize_model(input.request_model.as_deref());
    let request_model_used = request_model.is_some();
    let model_source = if session_model.is_some() {
        "session"
    } else if config.default_model.is_some() {
        "provider_default"
    } else if request_model_used {
        "request"
    } else {
        "none"
    };
    let effective_model = session_model
        .or_else(|| config.default_model.clone())
        .or(request_model);

    (effective_model, model_source)
}

/// New-path model resolution: session.model_ref → request.model_ref → config.default_model_ref.
pub(crate) fn resolve_model_ref_cascade(
    session: &Session,
    input: &ExecuteInput,
    config: &ExecutionConfigSnapshot,
) -> (
    Option<bamboo_domain::ProviderModelRef>,
    Option<String>,
    &'static str,
) {
    let session_model_ref = session_effective_model_ref(session);
    let request_model_ref = super::provider_model::derive_model_ref(
        input.request_model_ref.as_ref(),
        input.request_provider.as_deref(),
        input.request_model.as_deref(),
    );
    let config_model_ref = config.default_model_ref.clone();

    let (effective_model_ref, model_source) = if let Some(model_ref) = session_model_ref {
        (Some(model_ref), "session")
    } else if let Some(model_ref) = request_model_ref {
        (Some(model_ref), "request")
    } else if let Some(model_ref) = config_model_ref {
        (Some(model_ref), "provider_default")
    } else {
        (None, "none")
    };

    if let Some(model_ref) = effective_model_ref {
        let effective_model = normalize_model(Some(model_ref.model.as_str()));
        (Some(model_ref), effective_model, model_source)
    } else {
        let (effective_model, legacy_source) = resolve_model_cascade(session, input, config);
        (None, effective_model, legacy_source)
    }
}

// ---- Internal helpers ----

fn normalize_model(model: Option<&str>) -> Option<String> {
    model
        .map(str::trim)
        .filter(|m| !m.is_empty() && *m != "unknown")
        .map(String::from)
}
