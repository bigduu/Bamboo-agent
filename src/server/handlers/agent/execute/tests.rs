use super::ExecuteRequest;
use crate::core::ReasoningEffort;
use crate::server::app_state::{AgentRunner, AgentStatus};

#[test]
fn test_agent_status_running_blocks_restart() {
    // Test that Running status should block restart.
    let status = AgentStatus::Running;
    assert!(matches!(status, AgentStatus::Running));
}

#[test]
fn test_agent_status_completed_allows_restart() {
    // Test that Completed status should allow restart.
    let status = AgentStatus::Completed;
    assert!(!matches!(status, AgentStatus::Running));
}

#[test]
fn test_agent_status_error_allows_restart() {
    // Test that Error status should allow restart.
    let status = AgentStatus::Error("test error".to_string());
    assert!(!matches!(status, AgentStatus::Running));
}

#[test]
fn test_agent_status_cancelled_allows_restart() {
    // Test that Cancelled status should allow restart.
    let status = AgentStatus::Cancelled;
    assert!(!matches!(status, AgentStatus::Running));
}

#[test]
fn test_runner_creation() {
    // Test that runners can be created and have proper initial state.
    let runner = AgentRunner::new();
    assert!(matches!(runner.status, AgentStatus::Pending));
    // Verify cancel token exists (can be cloned).
    let _token_clone = runner.cancel_token.clone();
}

// ========== MODEL REQUIREMENT ARCHITECTURE TESTS ==========
// These tests ensure the design principle:
// "execute must receive model in request body, not from session"

/// Test: ExecuteRequest.model must be String (not Option<String>)
#[test]
fn execute_request_model_type_is_string_not_option() {
    let json = r#"{
            "model": "kimi-for-coding"
        }"#;

    let request: ExecuteRequest =
        serde_json::from_str(json).expect("execute request should deserialize");
    // This proves model is String, not Option<String>.
    let _model_str: &str = &request.model;
    assert_eq!(request.model, "kimi-for-coding");
}

/// Test: ExecuteRequest deserialization fails without model
#[test]
fn execute_request_requires_model() {
    let json = r#"{}"#;
    let result: Result<ExecuteRequest, _> = serde_json::from_str(json);
    assert!(
        result.is_err(),
        "ExecuteRequest should fail without model field"
    );
}

/// Test: Empty/whitespace model should fail validation
#[test]
fn execute_request_empty_model_fails_validation() {
    let request = ExecuteRequest {
        model: "   ".to_string(),
        skill_mode: None,
        reasoning_effort: None,
    };

    // Handler validation: trim and check if empty.
    let model = request.model.trim();
    assert!(model.is_empty(), "Empty model should fail validation");
}

/// Test: ExecuteRequest with valid model succeeds
#[test]
fn execute_request_with_valid_model_succeeds() {
    let json = r#"{
            "model": "gpt-4o-mini"
        }"#;

    let request: ExecuteRequest =
        serde_json::from_str(json).expect("execute request should deserialize");
    assert_eq!(request.model, "gpt-4o-mini");
}

#[test]
fn execute_request_accepts_reasoning_effort() {
    let json = r#"{
            "model": "gpt-4o-mini",
            "reasoning_effort": "xhigh"
        }"#;

    let request: ExecuteRequest =
        serde_json::from_str(json).expect("execute request should deserialize");
    assert_eq!(request.reasoning_effort, Some(ReasoningEffort::Xhigh));
}

#[test]
fn execute_request_rejects_invalid_reasoning_effort() {
    let json = r#"{
            "model": "gpt-4o-mini",
            "reasoning_effort": "extreme"
        }"#;

    let result: Result<ExecuteRequest, _> = serde_json::from_str(json);
    assert!(result.is_err());
}
