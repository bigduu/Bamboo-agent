//! Execute use case: prepare a session for agent execution.

use bamboo_agent_core::{Message, Role};
use bamboo_domain::reasoning::ReasoningEffort;
use bamboo_domain::Session;

use super::errors::ExecutePreparationError;
use super::provider_model::{
    persist_legacy_model_provider, persist_model_ref, session_effective_model_ref,
};
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
    let effective_reasoning_effort = session
        .reasoning_effort
        .or(input.request_reasoning_effort)
        .or(config.default_reasoning_effort);

    // ---- Reasoning source ----
    let reasoning_effort_source = if session.reasoning_effort.is_some() {
        "session"
    } else if input.request_reasoning_effort.is_some() {
        "request"
    } else if config.default_reasoning_effort.is_some() {
        "provider_default"
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
            .filter(|part| matches!(part, bamboo_agent_core::MessagePart::ImageUrl { .. }))
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

const CLARIFICATION_RESUME_PENDING_KEY: &str = "clarification_resume_pending";
const CONCLUSION_WITH_OPTIONS_RESUME_PENDING_KEY: &str = "conclusion_with_options_resume_pending";

/// Whether the session has resumable user work (pending tool response, retry, or last message is from user).
pub fn has_pending_user_message(session: &Session) -> bool {
    if has_pending_clarification_resume(session) || has_pending_retry_resume(session) {
        return true;
    }
    session
        .messages
        .last()
        .map(|message| matches!(message.role, Role::User))
        .unwrap_or(false)
}

pub fn consume_pending_clarification_resume(session: &mut Session) {
    session.metadata.remove(CLARIFICATION_RESUME_PENDING_KEY);
    session
        .metadata
        .remove(CONCLUSION_WITH_OPTIONS_RESUME_PENDING_KEY);
    session.metadata.remove("retry_resume_pending");
    session.metadata.remove("retry_resume_reason");
}

pub fn has_pending_clarification_resume(session: &Session) -> bool {
    session
        .metadata
        .get(CLARIFICATION_RESUME_PENDING_KEY)
        .or_else(|| {
            session
                .metadata
                .get(CONCLUSION_WITH_OPTIONS_RESUME_PENDING_KEY)
        })
        .is_some_and(|value| value == "true")
}

pub fn has_pending_conclusion_with_options_resume(session: &Session) -> bool {
    has_pending_clarification_resume(session)
}

pub fn has_pending_retry_resume(session: &Session) -> bool {
    session
        .metadata
        .get("retry_resume_pending")
        .is_some_and(|value| value == "true")
}

pub fn consume_pending_conclusion_with_options_resume(session: &mut Session) {
    consume_pending_clarification_resume(session)
}

/// Returns true if the message is flagged `metadata.hidden_from_ui == true`.
///
/// Such messages are runtime-injected (e.g. child-completion resume, retry
/// resume, conclusion-with-options resume) and are filtered out of any
/// client-facing view. Both `GET /history` and the execute sync snapshot
/// must use this exact predicate so the client and server agree on the
/// visible message_count and last_message_id.
pub(crate) fn is_hidden_from_ui(message: &Message) -> bool {
    message
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.get("hidden_from_ui"))
        .and_then(|value| value.as_bool())
        .unwrap_or(false)
}

// ---------------------------------------------------------------------------
// Billing helpers
// ---------------------------------------------------------------------------
//
// Some upstream products bill on "user-initiated message turns" rather than on
// LLM request count. With the runtime-level suspend/resume model used by the
// SubAgent tool, the engine can append `Role::User` messages that are NOT
// caused by the human user (e.g. child-completion resume, retry resume,
// conclusion_with_options resume). These helpers let a billing layer count
// only genuine user turns and skip system-injected resume messages.

/// Returns true if the message was synthesized by the runtime to resume a
/// suspended root session (child completion, retry, conclusion-with-options,
/// gold auto-continue).
///
/// Such messages have one or both of the following stable markers:
/// - `metadata.hidden_from_ui == true`
/// - `metadata.runtime_kind` set to a known resume kind
pub fn is_system_resume_message(message: &Message) -> bool {
    if !matches!(message.role, Role::User) {
        return false;
    }
    let Some(metadata) = message.metadata.as_ref() else {
        return false;
    };

    if metadata
        .get("hidden_from_ui")
        .and_then(|value| value.as_bool())
        .unwrap_or(false)
    {
        return true;
    }

    matches!(
        metadata
            .get("runtime_kind")
            .and_then(|value| value.as_str()),
        Some("child_completion_resume")
            | Some("retry_resume")
            | Some("conclusion_with_options_resume")
            | Some("clarification_resume")
            | Some("gold_continue_resume")
            | Some("gold_goal_resume")
    )
}

/// Returns true when the message represents a billable end-user turn.
///
/// Use this in any per-message billing accounting (e.g. "1 user message =
/// 1 quota unit"). LLM request / token billing should still be done at the
/// provider call layer; this helper is only relevant when the product itself
/// counts user-initiated messages.
pub fn is_billable_user_turn(message: &Message) -> bool {
    matches!(message.role, Role::User) && !is_system_resume_message(message)
}

/// Count the number of billable user turns in a session — i.e. user messages
/// that were actually initiated by the human, excluding runtime-injected
/// resume messages from child completion, retry, or conclusion-with-options.
pub fn billable_user_turn_count(session: &Session) -> usize {
    session
        .messages
        .iter()
        .filter(|message| is_billable_user_turn(message))
        .count()
}

impl ServerExecuteSnapshot {
    pub fn from_session(session: &Session) -> Self {
        // Mirror the GET /history filter so the client (which only ever sees
        // visible messages) and the server agree on message_count /
        // last_message_id. Hidden runtime resume messages stay internal and
        // do not leak into the sync protocol.
        let visible: Vec<&Message> = session
            .messages
            .iter()
            .filter(|message| !is_hidden_from_ui(message))
            .collect();
        Self {
            message_count: visible.len(),
            last_message_id: visible.last().map(|message| message.id.clone()),
            has_pending_question: session.pending_question.is_some(),
            pending_question_tool_call_id: session
                .pending_question
                .as_ref()
                .map(|pending| pending.tool_call_id.clone()),
            has_pending_user_message: has_pending_user_message(session),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_domain::ProviderModelRef;

    fn make_session(model: &str) -> Session {
        let mut s = Session::new("test-session", model);
        // Add a user message so has_pending_user_message is true
        s.messages.push(bamboo_agent_core::Message::user("hello"));
        s
    }

    fn make_input() -> ExecuteInput {
        ExecuteInput {
            session_id: "test-session".to_string(),
            request_model: None,
            request_model_ref: None,
            request_provider: None,
            request_reasoning_effort: None,
            request_skill_mode: None,
            client_sync: None,
        }
    }

    fn make_config() -> ExecutionConfigSnapshot {
        ExecutionConfigSnapshot {
            provider_model_ref_enabled: false,
            ..Default::default()
        }
    }

    // ---- normalize_model ----

    #[test]
    fn normalize_model_some() {
        assert_eq!(normalize_model(Some("gpt-4")), Some("gpt-4".to_string()));
    }

    #[test]
    fn normalize_model_trims_whitespace() {
        assert_eq!(
            normalize_model(Some("  gpt-4  ")),
            Some("gpt-4".to_string())
        );
    }

    #[test]
    fn normalize_model_none() {
        assert_eq!(normalize_model(None), None);
    }

    #[test]
    fn normalize_model_empty() {
        assert_eq!(normalize_model(Some("")), None);
    }

    #[test]
    fn normalize_model_whitespace_only() {
        assert_eq!(normalize_model(Some("   ")), None);
    }

    #[test]
    fn normalize_model_unknown() {
        assert_eq!(normalize_model(Some("unknown")), None);
    }

    // ---- resolve_model_cascade (flag OFF) ----

    #[test]
    fn cascade_old_prefers_session_model() {
        let session = make_session("claude-3");
        let input = make_input();
        let config = make_config();

        let (model, source) = resolve_model_cascade(&session, &input, &config);
        assert_eq!(model, Some("claude-3".to_string()));
        assert_eq!(source, "session");
    }

    #[test]
    fn cascade_old_falls_back_to_config_default() {
        let session = make_session("unknown");
        let input = make_input();
        let mut config = make_config();
        config.default_model = Some("gpt-4o".to_string());

        let (model, source) = resolve_model_cascade(&session, &input, &config);
        assert_eq!(model, Some("gpt-4o".to_string()));
        assert_eq!(source, "provider_default");
    }

    #[test]
    fn cascade_old_falls_back_to_request_model() {
        let session = make_session("unknown");
        let mut input = make_input();
        input.request_model = Some("gpt-4-turbo".to_string());
        let config = make_config();

        let (model, source) = resolve_model_cascade(&session, &input, &config);
        assert_eq!(model, Some("gpt-4-turbo".to_string()));
        assert_eq!(source, "request");
    }

    #[test]
    fn cascade_old_no_model_returns_none() {
        let session = make_session("unknown");
        let input = make_input();
        let config = make_config();

        let (model, source) = resolve_model_cascade(&session, &input, &config);
        assert_eq!(model, None);
        assert_eq!(source, "none");
    }

    #[test]
    fn cascade_old_session_overrides_request() {
        let session = make_session("claude-3");
        let mut input = make_input();
        input.request_model = Some("gpt-4".to_string());
        let config = make_config();

        let (model, source) = resolve_model_cascade(&session, &input, &config);
        assert_eq!(model, Some("claude-3".to_string()));
        assert_eq!(source, "session");
    }

    // ---- resolve_model_ref_cascade (flag ON) ----

    #[test]
    fn cascade_new_prefers_session_model_ref() {
        let mut session = make_session("unknown");
        session.model_ref = Some(ProviderModelRef::new("anthropic", "claude-3"));
        let input = make_input();
        let mut config = make_config();
        config.provider_model_ref_enabled = true;

        let (model_ref, model, source) = resolve_model_ref_cascade(&session, &input, &config);
        assert_eq!(
            model_ref,
            Some(ProviderModelRef::new("anthropic", "claude-3"))
        );
        assert_eq!(model, Some("claude-3".to_string()));
        assert_eq!(source, "session");
    }

    #[test]
    fn cascade_new_falls_back_to_request_model_ref_before_config_default_ref() {
        let session = make_session("unknown");
        let mut input = make_input();
        input.request_model_ref = Some(ProviderModelRef::new("gemini", "gemini-pro"));
        let mut config = make_config();
        config.provider_model_ref_enabled = true;
        config.default_model_ref = Some(ProviderModelRef::new("openai", "gpt-4o"));

        let (model_ref, model, source) = resolve_model_ref_cascade(&session, &input, &config);
        assert_eq!(
            model_ref,
            Some(ProviderModelRef::new("gemini", "gemini-pro"))
        );
        assert_eq!(model, Some("gemini-pro".to_string()));
        assert_eq!(source, "request");
    }

    #[test]
    fn cascade_new_falls_back_to_config_default_ref() {
        let session = make_session("unknown");
        let input = make_input();
        let mut config = make_config();
        config.provider_model_ref_enabled = true;
        config.default_model_ref = Some(ProviderModelRef::new("openai", "gpt-4o"));

        let (model_ref, model, source) = resolve_model_ref_cascade(&session, &input, &config);
        assert_eq!(model_ref, Some(ProviderModelRef::new("openai", "gpt-4o")));
        assert_eq!(model, Some("gpt-4o".to_string()));
        assert_eq!(source, "provider_default");
    }

    #[test]
    fn cascade_new_falls_back_to_old_cascade_when_no_refs() {
        let mut session = make_session("claude-3");
        session.model_ref = None;
        let input = make_input();
        let mut config = make_config();
        config.provider_model_ref_enabled = true;

        let (model_ref, model, source) = resolve_model_ref_cascade(&session, &input, &config);
        assert_eq!(model_ref, None);
        assert_eq!(model, Some("claude-3".to_string()));
        assert_eq!(source, "session");
    }

    #[test]
    fn cascade_new_session_ref_overrides_request_ref() {
        let mut session = make_session("unknown");
        session.model_ref = Some(ProviderModelRef::new("anthropic", "claude-3"));
        let mut input = make_input();
        input.request_model_ref = Some(ProviderModelRef::new("openai", "gpt-4o"));
        let mut config = make_config();
        config.provider_model_ref_enabled = true;

        let (model_ref, model, source) = resolve_model_ref_cascade(&session, &input, &config);
        assert_eq!(
            model_ref,
            Some(ProviderModelRef::new("anthropic", "claude-3"))
        );
        assert_eq!(model, Some("claude-3".to_string()));
        assert_eq!(source, "session");
    }

    #[test]
    fn cascade_new_uses_session_provider_metadata_even_without_structured_ref() {
        let mut session = make_session("gpt-4o");
        session.model_ref = None;
        session
            .metadata
            .insert("provider_name".to_string(), "openai".to_string());
        let input = make_input();
        let mut config = make_config();
        config.provider_model_ref_enabled = true;

        let (model_ref, model, source) = resolve_model_ref_cascade(&session, &input, &config);
        assert_eq!(model_ref, Some(ProviderModelRef::new("openai", "gpt-4o")));
        assert_eq!(model, Some("gpt-4o".to_string()));
        assert_eq!(source, "session");
    }

    #[test]
    fn cascade_new_no_model_anywhere_returns_none() {
        let session = make_session("unknown");
        let input = make_input();
        let mut config = make_config();
        config.provider_model_ref_enabled = true;

        let (model_ref, model, source) = resolve_model_ref_cascade(&session, &input, &config);
        assert_eq!(model_ref, None);
        assert_eq!(model, None);
        assert_eq!(source, "none");
    }

    // ---- evaluate_client_sync ----

    #[test]
    fn sync_none_when_no_client_sync() {
        let snapshot = ServerExecuteSnapshot {
            message_count: 1,
            last_message_id: Some("msg-1".to_string()),
            has_pending_question: false,
            pending_question_tool_call_id: None,
            has_pending_user_message: true,
        };
        assert_eq!(evaluate_client_sync(None, &snapshot), None);
    }

    #[test]
    fn sync_mismatch_pending_question_flag() {
        let client_sync = ExecuteClientSync {
            client_message_count: 1,
            client_last_message_id: Some("msg-1".to_string()),
            client_has_pending_question: true,
            client_pending_question_tool_call_id: None,
        };
        let snapshot = ServerExecuteSnapshot {
            message_count: 1,
            last_message_id: Some("msg-1".to_string()),
            has_pending_question: false,
            pending_question_tool_call_id: None,
            has_pending_user_message: true,
        };
        assert_eq!(
            evaluate_client_sync(Some(&client_sync), &snapshot),
            Some(ExecuteSyncReason::PendingQuestionMismatch)
        );
    }

    #[test]
    fn sync_mismatch_message_count() {
        let client_sync = ExecuteClientSync {
            client_message_count: 2,
            client_last_message_id: Some("msg-2".to_string()),
            client_has_pending_question: false,
            client_pending_question_tool_call_id: None,
        };
        let snapshot = ServerExecuteSnapshot {
            message_count: 1,
            last_message_id: Some("msg-1".to_string()),
            has_pending_question: false,
            pending_question_tool_call_id: None,
            has_pending_user_message: true,
        };
        assert_eq!(
            evaluate_client_sync(Some(&client_sync), &snapshot),
            Some(ExecuteSyncReason::MessageCountMismatch)
        );
    }

    #[test]
    fn sync_mismatch_last_message_id() {
        let client_sync = ExecuteClientSync {
            client_message_count: 1,
            client_last_message_id: Some("msg-old".to_string()),
            client_has_pending_question: false,
            client_pending_question_tool_call_id: None,
        };
        let snapshot = ServerExecuteSnapshot {
            message_count: 1,
            last_message_id: Some("msg-new".to_string()),
            has_pending_question: false,
            pending_question_tool_call_id: None,
            has_pending_user_message: true,
        };
        assert_eq!(
            evaluate_client_sync(Some(&client_sync), &snapshot),
            Some(ExecuteSyncReason::LastMessageIdMismatch)
        );
    }

    #[test]
    fn sync_ok_when_matching() {
        let client_sync = ExecuteClientSync {
            client_message_count: 1,
            client_last_message_id: Some("msg-1".to_string()),
            client_has_pending_question: false,
            client_pending_question_tool_call_id: None,
        };
        let snapshot = ServerExecuteSnapshot {
            message_count: 1,
            last_message_id: Some("msg-1".to_string()),
            has_pending_question: false,
            pending_question_tool_call_id: None,
            has_pending_user_message: true,
        };
        assert_eq!(evaluate_client_sync(Some(&client_sync), &snapshot), None);
    }

    #[test]
    fn sync_ok_with_matching_pending_question_and_tool_call_id() {
        let client_sync = ExecuteClientSync {
            client_message_count: 2,
            client_last_message_id: Some("msg-2".to_string()),
            client_has_pending_question: true,
            client_pending_question_tool_call_id: Some("tc-1".to_string()),
        };
        let snapshot = ServerExecuteSnapshot {
            message_count: 2,
            last_message_id: Some("msg-2".to_string()),
            has_pending_question: true,
            pending_question_tool_call_id: Some("tc-1".to_string()),
            has_pending_user_message: false,
        };
        assert_eq!(evaluate_client_sync(Some(&client_sync), &snapshot), None);
    }

    #[test]
    fn sync_mismatch_pending_question_tool_call_id() {
        let client_sync = ExecuteClientSync {
            client_message_count: 2,
            client_last_message_id: Some("msg-2".to_string()),
            client_has_pending_question: true,
            client_pending_question_tool_call_id: Some("tc-old".to_string()),
        };
        let snapshot = ServerExecuteSnapshot {
            message_count: 2,
            last_message_id: Some("msg-2".to_string()),
            has_pending_question: true,
            pending_question_tool_call_id: Some("tc-new".to_string()),
            has_pending_user_message: false,
        };
        assert_eq!(
            evaluate_client_sync(Some(&client_sync), &snapshot),
            Some(ExecuteSyncReason::PendingQuestionMismatch)
        );
    }

    // ---- has_pending_user_message ----

    #[test]
    fn pending_user_message_true_when_last_is_user() {
        let session = make_session("gpt-4");
        assert!(has_pending_user_message(&session));
    }

    #[test]
    fn pending_user_message_false_when_last_is_assistant() {
        let mut session = make_session("gpt-4");
        session
            .messages
            .push(bamboo_agent_core::Message::assistant("response", None));
        assert!(!has_pending_user_message(&session));
    }

    #[test]
    fn pending_user_message_false_when_empty() {
        let session = Session::new("test", "gpt-4");
        assert!(!has_pending_user_message(&session));
    }

    // ---- has_pending_conclusion_with_options_resume / has_pending_retry_resume ----

    #[test]
    fn conclusion_with_options_resume_true() {
        let mut session = Session::new("test", "gpt-4");
        session.metadata.insert(
            "conclusion_with_options_resume_pending".to_string(),
            "true".to_string(),
        );
        assert!(has_pending_conclusion_with_options_resume(&session));
    }

    #[test]
    fn conclusion_with_options_resume_false_when_missing() {
        let session = Session::new("test", "gpt-4");
        assert!(!has_pending_conclusion_with_options_resume(&session));
    }

    #[test]
    fn conclusion_with_options_resume_false_when_not_true() {
        let mut session = Session::new("test", "gpt-4");
        session.metadata.insert(
            "conclusion_with_options_resume_pending".to_string(),
            "false".to_string(),
        );
        assert!(!has_pending_conclusion_with_options_resume(&session));
    }

    #[test]
    fn retry_resume_true() {
        let mut session = Session::new("test", "gpt-4");
        session
            .metadata
            .insert("retry_resume_pending".to_string(), "true".to_string());
        assert!(has_pending_retry_resume(&session));
    }

    #[test]
    fn retry_resume_false_when_missing() {
        let session = Session::new("test", "gpt-4");
        assert!(!has_pending_retry_resume(&session));
    }

    // ---- consume_pending_conclusion_with_options_resume ----

    #[test]
    fn consume_removes_resume_metadata() {
        let mut session = Session::new("test", "gpt-4");
        session.metadata.insert(
            "conclusion_with_options_resume_pending".to_string(),
            "true".to_string(),
        );
        session
            .metadata
            .insert("retry_resume_pending".to_string(), "true".to_string());
        session
            .metadata
            .insert("retry_resume_reason".to_string(), "timeout".to_string());

        consume_pending_conclusion_with_options_resume(&mut session);

        assert!(!session
            .metadata
            .contains_key("conclusion_with_options_resume_pending"));
        assert!(!session.metadata.contains_key("retry_resume_pending"));
        assert!(!session.metadata.contains_key("retry_resume_reason"));
    }

    // ---- ServerExecuteSnapshot::from_session ----

    #[test]
    fn snapshot_from_session_counts_messages() {
        let mut session = Session::new("test", "gpt-4");
        session
            .messages
            .push(bamboo_agent_core::Message::user("hi"));
        session
            .messages
            .push(bamboo_agent_core::Message::assistant("hello", None));
        session.messages.last_mut().unwrap().id = "msg-2".to_string();

        let snapshot = ServerExecuteSnapshot::from_session(&session);
        assert_eq!(snapshot.message_count, 2);
        assert_eq!(snapshot.last_message_id, Some("msg-2".to_string()));
        assert!(!snapshot.has_pending_question);
        assert!(!snapshot.has_pending_user_message);
    }

    #[test]
    fn snapshot_empty_session() {
        let session = Session::new("test", "gpt-4");
        let snapshot = ServerExecuteSnapshot::from_session(&session);

        assert_eq!(snapshot.message_count, 0);
        assert_eq!(snapshot.last_message_id, None);
        assert!(!snapshot.has_pending_question);
        assert!(!snapshot.has_pending_user_message);
    }

    #[test]
    fn snapshot_excludes_hidden_user_message() {
        // Mirror the runtime resume injection: visible user, assistant reply,
        // then a hidden_from_ui synthetic user message. The client only ever
        // sees the first two via /history, so the snapshot must agree.
        let mut session = Session::new("test", "gpt-4");
        session.messages.push(Message::user("hi"));
        session.messages.last_mut().unwrap().id = "msg-1".to_string();
        session.messages.push(Message::assistant("hello", None));
        session.messages.last_mut().unwrap().id = "msg-2".to_string();
        let mut hidden = make_user_with_metadata(
            "runtime",
            serde_json::json!({
                "hidden_from_ui": true,
                "runtime_kind": "child_completion_resume",
            }),
        );
        hidden.id = "msg-3".to_string();
        session.messages.push(hidden);

        let snapshot = ServerExecuteSnapshot::from_session(&session);
        assert_eq!(snapshot.message_count, 2);
        assert_eq!(snapshot.last_message_id, Some("msg-2".to_string()));
        // Raw last is a (hidden) user message, so the runtime still has work
        // to do — the snapshot must keep this true so resume can proceed.
        assert!(snapshot.has_pending_user_message);
    }

    #[test]
    fn snapshot_excludes_trailing_hidden_message_for_last_id() {
        // If the trailing message is hidden, last_message_id must come from
        // the last visible message — otherwise the client (which never saw
        // the hidden id) will report LastMessageIdMismatch on every execute.
        let mut session = Session::new("test", "gpt-4");
        session.messages.push(Message::user("hi"));
        session.messages.last_mut().unwrap().id = "visible-user".to_string();
        session.messages.push(Message::assistant("hello", None));
        session.messages.last_mut().unwrap().id = "visible-assistant".to_string();
        let mut hidden =
            make_user_with_metadata("runtime", serde_json::json!({ "hidden_from_ui": true }));
        hidden.id = "hidden-tail".to_string();
        session.messages.push(hidden);

        let snapshot = ServerExecuteSnapshot::from_session(&session);
        assert_eq!(
            snapshot.last_message_id,
            Some("visible-assistant".to_string())
        );
    }

    #[test]
    fn evaluate_client_sync_passes_with_hidden_messages_filtered() {
        // Regression for the "Execute remains out-of-sync after 2 recovery
        // attempt(s)" loop: server has 1 hidden runtime message appended
        // after the visible turn, the client only sees the visible turn,
        // and execute used to return MessageCountMismatch forever.
        let mut session = Session::new("test", "gpt-4");
        session.messages.push(Message::user("hi"));
        session.messages.last_mut().unwrap().id = "msg-1".to_string();
        session.messages.push(Message::assistant("hello", None));
        session.messages.last_mut().unwrap().id = "msg-2".to_string();
        let mut hidden =
            make_user_with_metadata("runtime", serde_json::json!({ "hidden_from_ui": true }));
        hidden.id = "msg-3".to_string();
        session.messages.push(hidden);

        let snapshot = ServerExecuteSnapshot::from_session(&session);
        let client_sync = ExecuteClientSync {
            client_message_count: 2,
            client_last_message_id: Some("msg-2".to_string()),
            client_has_pending_question: false,
            client_pending_question_tool_call_id: None,
        };

        assert_eq!(evaluate_client_sync(Some(&client_sync), &snapshot), None);
    }

    #[test]
    fn snapshot_visibility_matches_history_filter() {
        // The bug this whole change fixes was a *consistency* failure: the
        // /history endpoint and the execute sync snapshot were using
        // different rules to decide which messages are visible to the
        // client. Lock that rule in by computing the visible view both
        // ways from the same session and asserting they agree on
        // count and last id.
        let mut session = Session::new("test", "gpt-4");
        let mut visible_user = Message::user("hello");
        visible_user.id = "v-1".to_string();
        session.messages.push(visible_user);
        let mut hidden_a =
            make_user_with_metadata("runtime", serde_json::json!({ "hidden_from_ui": true }));
        hidden_a.id = "h-1".to_string();
        session.messages.push(hidden_a);
        let mut visible_assistant = Message::assistant("world", None);
        visible_assistant.id = "v-2".to_string();
        session.messages.push(visible_assistant);
        let mut hidden_b = make_user_with_metadata(
            "runtime",
            serde_json::json!({
                "hidden_from_ui": true,
                "runtime_kind": "retry_resume",
            }),
        );
        hidden_b.id = "h-2".to_string();
        session.messages.push(hidden_b);

        // Reproduce the /history filter using the same shared helper.
        let history_visible: Vec<&Message> = session
            .messages
            .iter()
            .filter(|m| !is_hidden_from_ui(m))
            .collect();

        let snapshot = ServerExecuteSnapshot::from_session(&session);
        assert_eq!(snapshot.message_count, history_visible.len());
        assert_eq!(
            snapshot.last_message_id,
            history_visible.last().map(|m| m.id.clone())
        );
    }

    // ---- billing helpers ----

    fn make_user_with_metadata(content: &str, metadata: serde_json::Value) -> Message {
        let mut msg = Message::user(content);
        msg.metadata = Some(metadata);
        msg
    }

    #[test]
    fn is_system_resume_detects_hidden_from_ui_flag() {
        let msg = make_user_with_metadata("runtime", serde_json::json!({ "hidden_from_ui": true }));
        assert!(is_system_resume_message(&msg));
        assert!(!is_billable_user_turn(&msg));
    }

    #[test]
    fn is_system_resume_detects_known_runtime_kinds() {
        for kind in [
            "child_completion_resume",
            "retry_resume",
            "conclusion_with_options_resume",
            "gold_continue_resume",
            "gold_goal_resume",
        ] {
            let msg =
                make_user_with_metadata("runtime", serde_json::json!({ "runtime_kind": kind }));
            assert!(
                is_system_resume_message(&msg),
                "expected runtime_kind={kind} to be detected"
            );
            assert!(!is_billable_user_turn(&msg));
        }
    }

    #[test]
    fn is_system_resume_ignores_unknown_runtime_kinds() {
        let msg = make_user_with_metadata(
            "runtime",
            serde_json::json!({ "runtime_kind": "something_else" }),
        );
        assert!(!is_system_resume_message(&msg));
        assert!(is_billable_user_turn(&msg));
    }

    #[test]
    fn is_system_resume_returns_false_for_plain_user_message() {
        let msg = Message::user("hello");
        assert!(!is_system_resume_message(&msg));
        assert!(is_billable_user_turn(&msg));
    }

    #[test]
    fn is_system_resume_returns_false_for_assistant_messages() {
        let msg = Message::assistant("hi", None);
        assert!(!is_system_resume_message(&msg));
        assert!(!is_billable_user_turn(&msg));
    }

    #[test]
    fn billable_user_turn_count_skips_runtime_messages() {
        let mut session = Session::new("test", "gpt-4");
        session.messages.push(Message::user("first"));
        session.messages.push(Message::assistant("response", None));
        session.messages.push(make_user_with_metadata(
            "runtime",
            serde_json::json!({
                "hidden_from_ui": true,
                "runtime_kind": "child_completion_resume",
            }),
        ));
        session
            .messages
            .push(Message::assistant("response 2", None));
        session.messages.push(Message::user("second"));

        assert_eq!(billable_user_turn_count(&session), 2);
    }
}
