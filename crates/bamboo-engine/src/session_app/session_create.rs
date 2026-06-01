//! Session creation use case.
//!
//! Pure business logic for constructing a new session from request
//! parameters and configuration defaults. The handler builds the
//! value types from HTTP request / AppState, then delegates here.

use bamboo_agent_core::{Message, Session};
use bamboo_domain::reasoning::ReasoningEffort;
use bamboo_domain::ProviderModelRef;

use super::provider_model::{persist_legacy_model_provider, persist_model_ref};
use crate::model_config_helper::GOLD_CONFIG_METADATA_KEY;

/// Request-level input for session creation.
pub struct CreateSessionInput {
    pub id: String,
    pub title: Option<String>,
    pub system_prompt: Option<String>,
    pub model: Option<String>,
    pub model_ref: Option<ProviderModelRef>,
    pub reasoning_effort: Option<ReasoningEffort>,
    pub gold_config_json: Option<String>,
}

/// Configuration defaults for session creation.
///
/// Captured from server `Config` as plain values so the crate stays
/// decoupled from `bamboo-infrastructure-config`.
pub struct CreateSessionConfig {
    pub default_model: Option<String>,
    pub default_reasoning_effort: Option<ReasoningEffort>,
    pub global_default_prompt: String,
    pub builtin_fallback_prompt: &'static str,
}

/// Build a new session from request input and config defaults.
pub fn build_new_session(input: &CreateSessionInput, config: &CreateSessionConfig) -> Session {
    let model = input
        .model_ref
        .as_ref()
        .map(|model_ref| model_ref.model.clone())
        .unwrap_or_else(|| resolve_model(input.model.as_deref(), config.default_model.as_deref()));
    let mut session = Session::new(input.id.clone(), model);
    if let Some(model_ref) = input.model_ref.as_ref() {
        persist_model_ref(&mut session, model_ref);
    } else {
        persist_legacy_model_provider(&mut session, input.model.as_deref(), None);
    }
    session.reasoning_effort =
        resolve_reasoning_effort(input.reasoning_effort, config.default_reasoning_effort);
    if let Some(gold_config_json) = trimmed_non_empty(input.gold_config_json.as_deref()) {
        session
            .metadata
            .insert(GOLD_CONFIG_METADATA_KEY.to_string(), gold_config_json);
    }

    if let Some(title) = trimmed_non_empty(input.title.as_deref()) {
        session.title = title;
    }
    let explicit_prompt = trimmed_non_empty(input.system_prompt.as_deref());
    let has_explicit_prompt = explicit_prompt.is_some();
    let base_prompt = explicit_prompt.unwrap_or_else(|| {
        let trimmed = config.global_default_prompt.trim();
        if trimmed.is_empty() {
            config.builtin_fallback_prompt.to_string()
        } else {
            trimmed.to_string()
        }
    });
    session
        .metadata
        .insert("base_system_prompt".to_string(), base_prompt.clone());

    if has_explicit_prompt {
        session.add_message(Message::system(base_prompt));
        crate::runner::refresh_prompt_snapshot(&mut session);
    }

    session
}

/// Resolve the model from request → config → fallback.
pub fn resolve_model(request_model: Option<&str>, config_model: Option<&str>) -> String {
    trimmed_non_empty(request_model)
        .or_else(|| config_model.map(ToString::to_string))
        .unwrap_or_else(|| "unknown".to_string())
}

/// Resolve reasoning effort from request → config.
pub fn resolve_reasoning_effort(
    request_effort: Option<ReasoningEffort>,
    config_effort: Option<ReasoningEffort>,
) -> Option<ReasoningEffort> {
    request_effort.or(config_effort)
}

/// Trim whitespace and return `None` for empty strings.
pub fn trimmed_non_empty(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;

    const BUILTIN_FALLBACK: &str = "You are a helpful assistant.";

    fn default_config() -> CreateSessionConfig {
        CreateSessionConfig {
            default_model: None,
            default_reasoning_effort: None,
            global_default_prompt: "Global fallback".to_string(),
            builtin_fallback_prompt: BUILTIN_FALLBACK,
        }
    }

    #[test]
    fn resolve_model_uses_request_model_when_present() {
        assert_eq!(resolve_model(Some("  gpt-5  "), None), "gpt-5");
    }

    #[test]
    fn resolve_model_falls_back_to_config() {
        assert_eq!(resolve_model(None, Some("gpt-4")), "gpt-4");
    }

    #[test]
    fn resolve_model_falls_back_to_unknown() {
        assert_eq!(resolve_model(None, None), "unknown");
    }

    #[test]
    fn resolve_model_ignores_blank_request() {
        assert_eq!(resolve_model(Some("   "), Some("gpt-4")), "gpt-4");
    }

    #[test]
    fn resolve_reasoning_effort_prefers_request() {
        assert_eq!(
            resolve_reasoning_effort(Some(ReasoningEffort::High), Some(ReasoningEffort::Low)),
            Some(ReasoningEffort::High)
        );
    }

    #[test]
    fn resolve_reasoning_effort_falls_back_to_config() {
        assert_eq!(
            resolve_reasoning_effort(None, Some(ReasoningEffort::Medium)),
            Some(ReasoningEffort::Medium)
        );
    }

    #[test]
    fn build_new_session_applies_title_and_system_prompt() {
        let input = CreateSessionInput {
            id: "session-1".to_string(),
            title: Some("  Sprint Session  ".to_string()),
            system_prompt: Some("  You are helpful  ".to_string()),
            model: Some("gpt-5".to_string()),
            model_ref: None,
            reasoning_effort: Some(ReasoningEffort::High),
            gold_config_json: None,
        };
        let session = build_new_session(&input, &default_config());

        assert_eq!(session.title, "Sprint Session");
        assert_eq!(
            session
                .metadata
                .get("base_system_prompt")
                .map(String::as_str),
            Some("You are helpful")
        );
        assert_eq!(session.reasoning_effort, Some(ReasoningEffort::High));
        assert_eq!(
            session.messages.first().map(|m| m.content.as_str()),
            Some("You are helpful")
        );
    }

    #[test]
    fn build_new_session_uses_global_default_when_no_explicit_prompt() {
        let input = CreateSessionInput {
            id: "session-2".to_string(),
            title: None,
            system_prompt: None,
            model: Some("gpt-5".to_string()),
            model_ref: None,
            reasoning_effort: None,
            gold_config_json: None,
        };
        let session = build_new_session(&input, &default_config());

        assert_eq!(
            session
                .metadata
                .get("base_system_prompt")
                .map(String::as_str),
            Some("Global fallback")
        );
        assert!(session.messages.is_empty());
    }

    #[test]
    fn build_new_session_uses_builtin_fallback_when_global_is_empty() {
        let config = CreateSessionConfig {
            global_default_prompt: "   ".to_string(),
            ..default_config()
        };
        let input = CreateSessionInput {
            id: "session-3".to_string(),
            title: None,
            system_prompt: None,
            model: Some("gpt-5".to_string()),
            model_ref: None,
            reasoning_effort: None,
            gold_config_json: None,
        };
        let session = build_new_session(&input, &config);

        assert_eq!(
            session
                .metadata
                .get("base_system_prompt")
                .map(String::as_str),
            Some(BUILTIN_FALLBACK)
        );
    }

    #[test]
    fn build_new_session_with_explicit_prompt_generates_snapshot() {
        let input = CreateSessionInput {
            id: "session-4".to_string(),
            title: None,
            system_prompt: Some("Custom prompt".to_string()),
            model: Some("gpt-5".to_string()),
            model_ref: None,
            reasoning_effort: None,
            gold_config_json: None,
        };
        let session = build_new_session(&input, &default_config());

        let snapshot = crate::runner::read_prompt_snapshot(&session)
            .expect("prompt snapshot should exist");
        assert_eq!(snapshot.base_system_prompt, "Custom prompt");
        assert_eq!(snapshot.effective_system_prompt, "Custom prompt");
    }

    #[test]
    fn build_new_session_with_model_ref_persists_bare_model_and_provider_metadata() {
        let input = CreateSessionInput {
            id: "session-5".to_string(),
            title: None,
            system_prompt: None,
            model: Some("ignored-compat-model".to_string()),
            model_ref: Some(ProviderModelRef::new("anthropic", "claude-3-7-sonnet")),
            reasoning_effort: None,
            gold_config_json: None,
        };
        let session = build_new_session(&input, &default_config());

        assert_eq!(session.model, "claude-3-7-sonnet");
        assert_eq!(
            session.model_ref,
            Some(ProviderModelRef::new("anthropic", "claude-3-7-sonnet"))
        );
        assert_eq!(
            session.metadata.get("provider_name").map(String::as_str),
            Some("anthropic")
        );
    }
}
