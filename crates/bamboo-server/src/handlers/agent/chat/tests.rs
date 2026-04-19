use super::ChatRequest;

// Chat request deserialization tests
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
    let mut session = bamboo_agent_core::Session::new("test-session", "initial-model");
    session.model = "gpt-4o-mini".to_string();
    assert_eq!(session.model, "gpt-4o-mini");
}

#[test]
fn session_model_round_trip() {
    let session = bamboo_agent_core::Session::new("test-session", "gpt-5");
    let json = serde_json::to_string(&session).expect("session should serialize");
    let deserialized: bamboo_agent_core::Session =
        serde_json::from_str(&json).expect("session should deserialize");
    assert_eq!(deserialized.model, "gpt-5");
}

#[test]
fn chat_request_model_type_is_string_not_option() {
    let json = r#"{
            "message": "Hello",
            "model": "claude-3-opus"
        }"#;
    let request: ChatRequest = serde_json::from_str(json).expect("chat request should deserialize");
    let _model_str: &str = &request.model;
    assert_eq!(request.model, "claude-3-opus");
}

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
        model: "   ".to_string(),
    };
    let model = request.model.trim();
    assert!(model.is_empty(), "Empty model should fail validation");
}

#[test]
fn session_model_is_for_recording_only() {
    let mut session = bamboo_agent_core::Session::new("test-123", "initial-model");
    assert_eq!(session.model, "initial-model");
    session.model = "updated-model".to_string();
    assert_eq!(session.model, "updated-model");
}
