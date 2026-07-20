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
    assert_eq!(request.model.as_deref(), Some("gpt-5"));
}

/// #480: `model` is optional on `POST /chat` — omitting it must deserialize
/// successfully (not error), leaving `model: None` for the handler to resolve
/// against the server's default via `bamboo_engine::resolved_defaults`.
#[test]
fn chat_request_deserialization_without_model_defaults_to_none() {
    let json = r#"{
            "message": "Hello"
        }"#;
    let request: ChatRequest =
        serde_json::from_str(json).expect("model-less chat request should deserialize");
    assert!(request.model.is_none());
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

/// #480: `ChatRequest.model` is `Option<String>` (not a bare `String`) so a
/// request can omit it and let the server resolve a default.
#[test]
fn chat_request_model_field_is_optional_string() {
    let json = r#"{
            "message": "Hello",
            "model": "claude-3-opus"
        }"#;
    let request: ChatRequest = serde_json::from_str(json).expect("chat request should deserialize");
    let _model_opt: &Option<String> = &request.model;
    assert_eq!(request.model.as_deref(), Some("claude-3-opus"));
}

#[test]
fn chat_request_blank_model_trims_to_empty() {
    let request = ChatRequest {
        message: "Hello".to_string(),
        session_id: None,
        system_prompt: None,
        enhance_prompt: None,
        copilot_conclusion_with_options_enhancement_enabled: None,
        workspace_path: None,
        selected_skill_ids: None,
        workflow_selection: None,
        orchestration_opt_in: None,
        images: None,
        model: Some("   ".to_string()),
        provider: None,
        model_ref: None,
    };
    // A whitespace-only model is treated the same as an absent one by the
    // handler's `request::resolve_model` (falls back to the server default).
    let model = request.model.as_deref().unwrap_or_default().trim();
    assert!(model.is_empty(), "Blank model should be treated as absent");
}

#[test]
fn session_model_is_for_recording_only() {
    let mut session = bamboo_agent_core::Session::new("test-123", "initial-model");
    assert_eq!(session.model, "initial-model");
    session.model = "updated-model".to_string();
    assert_eq!(session.model, "updated-model");
}
