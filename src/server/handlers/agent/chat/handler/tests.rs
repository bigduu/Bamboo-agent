use crate::agent::core::Session;

use super::request::{optional_non_empty, resolve_session_id, validate_and_normalize_model};
use super::session::{
    clear_skill_runtime_state, resolve_base_prompt, resolve_copilot_ask_user_enhancement_enabled,
    resolve_enhance_prompt, resolve_selected_skill_ids, resolve_workspace_path,
};

#[test]
fn validate_and_normalize_model_rejects_empty_values() {
    let response = validate_and_normalize_model("   ").expect_err("model should be required");
    assert_eq!(response.status(), actix_web::http::StatusCode::BAD_REQUEST);
}

#[test]
fn validate_and_normalize_model_trims_whitespace() {
    let model = validate_and_normalize_model("  gpt-5  ").expect("model should be accepted");
    assert_eq!(model, "gpt-5");
}

#[test]
fn optional_non_empty_returns_none_for_blank_string() {
    let value = optional_non_empty(Some("   "));
    assert_eq!(value, None);
}

#[test]
fn resolve_session_id_uses_provided_value_without_trimming() {
    let session_id = resolve_session_id(Some("  existing-id  "));
    assert_eq!(session_id, "  existing-id  ");
}

#[test]
fn resolve_base_prompt_prefers_request_and_persists_metadata() {
    let mut session = Session::new("session-1", "model");
    let base_prompt = resolve_base_prompt(&mut session, Some("request prompt"), "fallback");
    assert_eq!(base_prompt, "request prompt");
    assert_eq!(
        session
            .metadata
            .get("base_system_prompt")
            .map(String::as_str),
        Some("request prompt")
    );
}

#[test]
fn resolve_base_prompt_falls_back_to_existing_metadata() {
    let mut session = Session::new("session-1", "model");
    session.metadata.insert(
        "base_system_prompt".to_string(),
        "stored prompt".to_string(),
    );

    let base_prompt = resolve_base_prompt(&mut session, None, "fallback");
    assert_eq!(base_prompt, "stored prompt");
}

#[test]
fn resolve_base_prompt_falls_back_to_existing_system_message_before_global_default() {
    let mut session = Session::new("session-1", "model");
    session.add_message(crate::agent::core::Message::system("Existing system"));

    let base_prompt = resolve_base_prompt(&mut session, None, "global default");
    assert_eq!(base_prompt, "Existing system");
    assert_eq!(
        session
            .metadata
            .get("base_system_prompt")
            .map(String::as_str),
        Some("Existing system")
    );
}

#[test]
fn resolve_base_prompt_uses_global_default_when_missing_everywhere() {
    let mut session = Session::new("session-1", "model");
    let base_prompt = resolve_base_prompt(&mut session, None, "global default");
    assert_eq!(base_prompt, "global default");
    assert_eq!(
        session
            .metadata
            .get("base_system_prompt")
            .map(String::as_str),
        Some("global default")
    );
}

#[test]
fn resolve_workspace_path_uses_request_then_metadata() {
    let mut session = Session::new("session-1", "model");

    let from_request = resolve_workspace_path(&mut session, Some("/tmp/workspace"));
    assert_eq!(from_request.as_deref(), Some("/tmp/workspace"));
    assert_eq!(
        session.metadata.get("workspace_path").map(String::as_str),
        Some("/tmp/workspace")
    );

    let from_metadata = resolve_workspace_path(&mut session, None);
    assert_eq!(from_metadata.as_deref(), Some("/tmp/workspace"));
}

#[test]
fn resolve_enhance_prompt_stores_and_clears_metadata() {
    let mut session = Session::new("session-1", "model");

    let from_request = resolve_enhance_prompt(&mut session, Some("Extra guidance"));
    assert_eq!(from_request.as_deref(), Some("Extra guidance"));
    assert_eq!(
        session.metadata.get("enhance_prompt").map(String::as_str),
        Some("Extra guidance")
    );

    let from_empty_request = resolve_enhance_prompt(&mut session, None);
    assert_eq!(from_empty_request, None);
    assert!(!session.metadata.contains_key("enhance_prompt"));
}

#[test]
fn resolve_copilot_ask_user_enhancement_enabled_stores_and_clears_metadata() {
    let mut session = Session::new("session-1", "model");

    let from_request = resolve_copilot_ask_user_enhancement_enabled(&mut session, Some(true));
    assert_eq!(from_request, Some(true));
    assert_eq!(
        session
            .metadata
            .get("copilot_ask_user_enhancement_enabled")
            .map(String::as_str),
        Some("true")
    );

    let from_request = resolve_copilot_ask_user_enhancement_enabled(&mut session, Some(false));
    assert_eq!(from_request, Some(false));
    assert_eq!(
        session
            .metadata
            .get("copilot_ask_user_enhancement_enabled")
            .map(String::as_str),
        Some("false")
    );

    let from_empty_request = resolve_copilot_ask_user_enhancement_enabled(&mut session, None);
    assert_eq!(from_empty_request, None);
    assert!(!session
        .metadata
        .contains_key("copilot_ask_user_enhancement_enabled"));
}

#[test]
fn resolve_selected_skill_ids_prefers_structured_request_and_persists_as_json() {
    let mut session = Session::new("session-1", "model");
    let selected = resolve_selected_skill_ids(
        &mut session,
        Some(&[
            "pdf".to_string(),
            "skill-creator".to_string(),
            "pdf".to_string(),
        ]),
        "hello",
    )
    .expect("selected skills should be present");

    assert_eq!(
        selected,
        vec!["pdf".to_string(), "skill-creator".to_string()]
    );
    assert_eq!(
        session
            .metadata
            .get("selected_skill_ids")
            .map(String::as_str),
        Some("[\"pdf\",\"skill-creator\"]")
    );
}

#[test]
fn resolve_selected_skill_ids_falls_back_to_legacy_hint_when_structured_field_absent() {
    let mut session = Session::new("session-1", "model");
    let selected = resolve_selected_skill_ids(
        &mut session,
        None,
        "[User explicitly selected skill: PDF Skill (ID: pdf)]\n\nPlease parse this file",
    )
    .expect("selected skills should be present");

    assert_eq!(selected, vec!["pdf".to_string()]);
    assert_eq!(
        session
            .metadata
            .get("selected_skill_ids")
            .map(String::as_str),
        Some("[\"pdf\"]")
    );
}

#[test]
fn resolve_selected_skill_ids_clears_stale_metadata_when_no_selection_provided() {
    let mut session = Session::new("session-1", "model");
    session
        .metadata
        .insert("selected_skill_ids".to_string(), "[\"pdf\"]".to_string());

    let selected = resolve_selected_skill_ids(&mut session, None, "normal prompt");
    assert!(selected.is_none());
    assert!(!session.metadata.contains_key("selected_skill_ids"));
}

#[test]
fn clear_skill_runtime_state_removes_loaded_skill_markers() {
    let mut session = Session::new("session-1", "model");
    session.metadata.insert(
        "skill_runtime_loaded_skill_ids".to_string(),
        r#"["demo"]"#.to_string(),
    );
    session.metadata.insert(
        "skill_runtime_last_loaded_skill_id".to_string(),
        "demo".to_string(),
    );

    clear_skill_runtime_state(&mut session);

    assert!(!session
        .metadata
        .contains_key("skill_runtime_loaded_skill_ids"));
    assert!(!session
        .metadata
        .contains_key("skill_runtime_last_loaded_skill_id"));
}
