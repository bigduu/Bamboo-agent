use super::super::super::types::CreateSessionRequest;
use crate::core::{Config, ProviderConfigs, ReasoningEffort};

use super::create::{build_new_session, model_from_request, reasoning_effort_from_request};

#[test]
fn model_from_request_uses_provider_default_when_absent_or_blank() {
    let config = Config {
        provider: "copilot".to_string(),
        ..Config::default()
    };
    let expected = config
        .get_model()
        .expect("provider default model should exist");
    let absent = CreateSessionRequest {
        title: None,
        system_prompt: None,
        model: None,
        reasoning_effort: None,
    };
    assert_eq!(model_from_request(&absent, &config), expected);

    let blank = CreateSessionRequest {
        title: None,
        system_prompt: None,
        model: Some("   ".to_string()),
        reasoning_effort: None,
    };
    assert_eq!(
        model_from_request(&blank, &config),
        config.get_model().unwrap()
    );
}

#[test]
fn model_from_request_trims_non_empty_model() {
    let config = Config::default();
    let req = CreateSessionRequest {
        title: None,
        system_prompt: None,
        model: Some("  gpt-5  ".to_string()),
        reasoning_effort: None,
    };
    assert_eq!(model_from_request(&req, &config), "gpt-5");
}

#[test]
fn build_new_session_applies_title_and_system_prompt_metadata() {
    let config = Config::default();
    let req = CreateSessionRequest {
        title: Some("  Sprint Session  ".to_string()),
        system_prompt: Some("  You are helpful  ".to_string()),
        model: Some("gpt-5".to_string()),
        reasoning_effort: Some(ReasoningEffort::High),
    };

    let session = build_new_session("session-1", &req, "Global fallback", &config);

    assert_eq!(session.title, "Sprint Session");
    assert_eq!(
        session
            .metadata
            .get("base_system_prompt")
            .map(String::as_str),
        Some("You are helpful")
    );
    assert_eq!(session.reasoning_effort, Some(ReasoningEffort::High));
    assert!(matches!(
        session.messages.first().map(|message| &message.role),
        Some(crate::agent::core::Role::System)
    ));
    assert_eq!(
        session
            .messages
            .first()
            .map(|message| message.content.as_str()),
        Some("You are helpful")
    );
    let snapshot = crate::agent::loop_module::runner::read_prompt_snapshot(&session)
        .expect("prompt snapshot should exist for explicit prompt session");
    assert_eq!(snapshot.base_system_prompt, "You are helpful");
    assert_eq!(snapshot.effective_system_prompt, "You are helpful");
}

#[test]
fn build_new_session_uses_global_default_template_when_request_prompt_is_missing() {
    let config = Config {
        provider: "copilot".to_string(),
        providers: ProviderConfigs::default(),
        ..Config::default()
    };
    let req = CreateSessionRequest {
        title: Some("New Session".to_string()),
        system_prompt: None,
        model: Some("gpt-5".to_string()),
        reasoning_effort: None,
    };

    let session = build_new_session("session-1", &req, "Global fallback", &config);
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
fn reasoning_effort_from_request_falls_back_to_provider_default() {
    let config = Config {
        provider: "copilot".to_string(),
        providers: ProviderConfigs::default(),
        ..Config::default()
    };
    let req = CreateSessionRequest {
        title: None,
        system_prompt: None,
        model: None,
        reasoning_effort: None,
    };

    assert_eq!(reasoning_effort_from_request(&req, &config), None);
}
