use super::super::super::types::CreateSessionRequest;
use super::create::{build_new_session, model_from_request};

#[test]
fn model_from_request_uses_unknown_when_absent_or_blank() {
    let absent = CreateSessionRequest {
        title: None,
        system_prompt: None,
        model: None,
    };
    assert_eq!(model_from_request(&absent), "unknown");

    let blank = CreateSessionRequest {
        title: None,
        system_prompt: None,
        model: Some("   ".to_string()),
    };
    assert_eq!(model_from_request(&blank), "unknown");
}

#[test]
fn model_from_request_trims_non_empty_model() {
    let req = CreateSessionRequest {
        title: None,
        system_prompt: None,
        model: Some("  gpt-5  ".to_string()),
    };
    assert_eq!(model_from_request(&req), "gpt-5");
}

#[test]
fn build_new_session_applies_title_and_system_prompt_metadata() {
    let req = CreateSessionRequest {
        title: Some("  Sprint Session  ".to_string()),
        system_prompt: Some("  You are helpful  ".to_string()),
        model: Some("gpt-5".to_string()),
    };

    let session = build_new_session("session-1", &req, "Global fallback");

    assert_eq!(session.title, "Sprint Session");
    assert_eq!(
        session
            .metadata
            .get("base_system_prompt")
            .map(String::as_str),
        Some("You are helpful")
    );
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
}

#[test]
fn build_new_session_uses_global_default_template_when_request_prompt_is_missing() {
    let req = CreateSessionRequest {
        title: Some("New Session".to_string()),
        system_prompt: None,
        model: Some("gpt-5".to_string()),
    };

    let session = build_new_session("session-1", &req, "Global fallback");
    assert_eq!(
        session
            .metadata
            .get("base_system_prompt")
            .map(String::as_str),
        Some("Global fallback")
    );
    assert!(session.messages.is_empty());
}
