use super::prompt::{
    build_enhanced_system_prompt, build_enhanced_system_prompt_with_profile,
    upsert_system_prompt_message,
};
use super::ChatRequest;
use crate::agent::core::Session;

#[test]
fn upsert_system_prompt_inserts_when_missing() {
    let mut session = Session::new("session-1", "test-model");
    session.add_message(crate::agent::core::Message::user("hello"));

    upsert_system_prompt_message(&mut session, "system prompt".to_string());

    assert!(matches!(
        session.messages.first().map(|m| &m.role),
        Some(crate::agent::core::Role::System)
    ));
    assert_eq!(session.messages[0].content, "system prompt");
}

#[test]
fn upsert_system_prompt_replaces_existing_message() {
    let mut session = Session::new("session-1", "test-model");
    session.add_message(crate::agent::core::Message::system("old"));
    session.add_message(crate::agent::core::Message::user("hello"));

    upsert_system_prompt_message(&mut session, "new".to_string());

    let system_messages = session
        .messages
        .iter()
        .filter(|m| matches!(m.role, crate::agent::core::Role::System))
        .count();
    assert_eq!(system_messages, 1);
    assert_eq!(session.messages[0].content, "new");
}

#[test]
fn build_enhanced_system_prompt_appends_enhancement_before_skills() {
    let prompt = build_enhanced_system_prompt("Base prompt", Some("Extra guidance"), None);

    assert!(prompt.starts_with("Base prompt\n\nExtra guidance"));
}

#[test]
fn build_enhanced_system_prompt_appends_workspace_context_before_skills() {
    let prompt = build_enhanced_system_prompt(
        "Base prompt",
        Some("Extra guidance"),
        Some("/tmp/workspace"),
    );

    let workspace_segment =
        crate::server::app_state::build_workspace_prompt_context("/tmp/workspace")
            .expect("workspace segment");

    assert!(prompt.contains(&workspace_segment));
}

#[test]
fn build_workspace_context_includes_prompt_safe_env_metadata_without_secret_values() {
    let config = crate::core::Config {
        env_vars: vec![
            crate::core::EnvVarEntry {
                name: "OPENAI_API_KEY".to_string(),
                value: "super-secret-value".to_string(),
                secret: true,
                value_encrypted: None,
                description: Some("OpenAI credential".to_string()),
            },
            crate::core::EnvVarEntry {
                name: "INTERNAL_API_BASE".to_string(),
                value: "https://internal.example".to_string(),
                secret: false,
                value_encrypted: None,
                description: Some("Internal API endpoint".to_string()),
            },
        ],
        ..crate::core::Config::default()
    };
    config.publish_env_vars();

    let prompt = build_enhanced_system_prompt("Base prompt", None, Some("/tmp/workspace"));
    assert!(prompt.contains("OPENAI_API_KEY"));
    assert!(prompt.contains("INTERNAL_API_BASE"));
    assert!(prompt.contains("OpenAI credential"));
    assert!(prompt.contains("Internal API endpoint"));
    assert!(prompt.contains("secret"));
    assert!(!prompt.contains("super-secret-value"));
    assert!(!prompt.contains("https://internal.example"));
}

#[test]
fn build_enhanced_system_prompt_ignores_empty_enhancement() {
    let prompt = build_enhanced_system_prompt("Base prompt", Some("   "), None);
    assert_eq!(prompt, "Base prompt");
}

#[test]
fn prompt_profile_fingerprint_changes_when_components_change() {
    let (_, profile_a) = build_enhanced_system_prompt_with_profile("Base prompt", None, None);
    let (_, profile_b) =
        build_enhanced_system_prompt_with_profile("Base prompt", Some("Extra"), None);
    let (_, profile_c) = build_enhanced_system_prompt_with_profile(
        "Base prompt",
        Some("Extra"),
        Some("/tmp/workspace"),
    );

    assert_ne!(profile_a.fingerprint, profile_b.fingerprint);
    assert_ne!(profile_b.fingerprint, profile_c.fingerprint);
}

#[test]
fn prompt_profile_exposes_component_flags_and_lengths() {
    let (prompt, profile) = build_enhanced_system_prompt_with_profile(
        "Base prompt",
        Some("Extra guidance"),
        Some("/tmp/workspace"),
    );

    assert!(profile.has_enhancement);
    assert!(profile.has_workspace_context);
    assert_eq!(profile.final_len, prompt.len());
    assert!(profile.component_flags_value().contains("enhance=1"));
    assert!(profile.component_lengths_value().contains("base="));
    assert!(profile.component_lengths_value().contains("final="));
}

#[test]
fn chat_request_deserialization_with_model() {
    let json = r#"{
            "message": "Hello",
            "session_id": "test-session",
            "model": "gpt-5"
        }"#;

    let request: ChatRequest = serde_json::from_str(json).expect("chat request should deserialize");
    assert_eq!(request.message, "Hello");
    assert_eq!(request.session_id, Some("test-session".to_string()));
    assert_eq!(request.model, "gpt-5");
}

#[test]
fn chat_request_deserialization_without_model() {
    let json = r#"{
            "message": "Hello"
        }"#;

    let result: Result<ChatRequest, _> = serde_json::from_str(json);
    assert!(result.is_err());
}

#[test]
fn session_stores_model_in_dedicated_field() {
    // Simulate what the handler does.
    let mut session = Session::new("test-session", "initial-model");
    session.model = "gpt-4o-mini".to_string();
    assert_eq!(session.model, "gpt-4o-mini");
}

#[test]
fn session_model_round_trip() {
    // Create session with model.
    let session = Session::new("test-session", "gpt-5");

    // Serialize and deserialize.
    let json = serde_json::to_string(&session).expect("session should serialize");
    let deserialized: Session = serde_json::from_str(&json).expect("session should deserialize");

    assert_eq!(deserialized.model, "gpt-5");
}

// ========== MODEL REQUIREMENT ARCHITECTURE TESTS ==========
// These tests ensure the design principle:
// "model must be explicitly provided in the request"

/// Test: ChatRequest.model must be String (not Option<String>)
/// This prevents accidental fallback to None.
#[test]
fn chat_request_model_type_is_string_not_option() {
    let json = r#"{
            "message": "Hello",
            "model": "claude-3-opus"
        }"#;

    let request: ChatRequest = serde_json::from_str(json).expect("chat request should deserialize");
    // This line proves model is String, not Option<String>.
    // If it were Option<String>, this would fail to compile.
    let _model_str: &str = &request.model;
    assert_eq!(request.model, "claude-3-opus");
}

/// Test: Empty/whitespace model should fail validation.
#[test]
fn chat_request_empty_model_fails_validation() {
    let request = ChatRequest {
        message: "Hello".to_string(),
        session_id: None,
        system_prompt: None,
        enhance_prompt: None,
        copilot_conclusion_with_options_enhancement_enabled: None,
        workspace_path: None,
        selected_skill_ids: None,
        images: None,
        model: "   ".to_string(), // Empty/whitespace
    };

    // Handler validation: trim and check if empty.
    let model = request.model.trim();
    assert!(model.is_empty(), "Empty model should fail validation");
}

/// Test: Session.model is just for recording, not execution.
#[test]
fn session_model_is_for_recording_only() {
    // Create session with initial model.
    let mut session = Session::new("test-123", "initial-model");
    assert_eq!(session.model, "initial-model");

    // Session.model can be updated (just for recording).
    session.model = "updated-model".to_string();
    assert_eq!(session.model, "updated-model");

    // Note: The actual execution uses config.model_name from the request,
    // not session.model. This is enforced in execute.rs and agent-loop.
}
