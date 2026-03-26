use super::response::execute_response_payload;
use super::validation::validate_and_normalize_model;
use super::{
    apply_copilot_ask_user_enhancement_tool_filter,
    is_copilot_ask_user_enhancement_enabled_for_session,
};
use crate::agent::core::Session;
use std::collections::BTreeSet;

#[test]
fn validate_and_normalize_model_rejects_empty_value() {
    assert!(validate_and_normalize_model("   ").is_err());
}

#[test]
fn validate_and_normalize_model_trims_whitespace() {
    let model = validate_and_normalize_model(" gpt-4o-mini ").expect("model should be valid");
    assert_eq!(model, "gpt-4o-mini");
}

#[test]
fn execute_response_payload_formats_status_and_events_url() {
    let payload = execute_response_payload("session-123", "started");
    assert_eq!(payload.session_id, "session-123");
    assert_eq!(payload.status, "started");
    assert_eq!(payload.events_url, "/api/v1/events/session-123");
}

#[test]
fn copilot_ask_user_enhancement_flag_requires_copilot_provider_and_true_metadata() {
    let mut enabled_session = Session::new("session-1", "model");
    enabled_session.metadata.insert(
        "copilot_ask_user_enhancement_enabled".to_string(),
        "true".to_string(),
    );
    assert!(is_copilot_ask_user_enhancement_enabled_for_session(
        &enabled_session,
        "copilot"
    ));
    assert!(is_copilot_ask_user_enhancement_enabled_for_session(
        &enabled_session,
        " COPILOT "
    ));

    let mut disabled_session = Session::new("session-2", "model");
    disabled_session.metadata.insert(
        "copilot_ask_user_enhancement_enabled".to_string(),
        "false".to_string(),
    );
    assert!(!is_copilot_ask_user_enhancement_enabled_for_session(
        &disabled_session,
        "copilot"
    ));

    let no_metadata_session = Session::new("session-3", "model");
    assert!(!is_copilot_ask_user_enhancement_enabled_for_session(
        &no_metadata_session,
        "copilot"
    ));

    assert!(!is_copilot_ask_user_enhancement_enabled_for_session(
        &enabled_session,
        "openai"
    ));
}

#[test]
fn tool_filter_disables_conclusion_and_mermaid_when_enhancement_not_enabled() {
    let mut disabled_tools = BTreeSet::new();
    let session = Session::new("session-1", "model");
    apply_copilot_ask_user_enhancement_tool_filter(&mut disabled_tools, &session, "copilot");

    assert!(disabled_tools.contains("conclusion"));
    assert!(disabled_tools.contains("mermaid"));
}

#[test]
fn tool_filter_keeps_conclusion_and_mermaid_available_when_enhancement_enabled() {
    let mut disabled_tools = BTreeSet::new();
    let mut session = Session::new("session-1", "model");
    session.metadata.insert(
        "copilot_ask_user_enhancement_enabled".to_string(),
        "true".to_string(),
    );

    apply_copilot_ask_user_enhancement_tool_filter(&mut disabled_tools, &session, "copilot");

    assert!(!disabled_tools.contains("conclusion"));
    assert!(!disabled_tools.contains("mermaid"));
}
