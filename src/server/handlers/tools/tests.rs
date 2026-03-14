use crate::{agent::core::tools::ToolResult, server::error::AppError};

use super::{
    models::{ToolExecutionRequest, ToolParameter},
    request::{
        build_tool_call, canonical_tool_name_or_error, parse_arguments, trimmed_session_id,
        validate_session_context_requirement,
    },
    response::build_execution_response,
};

#[test]
fn canonical_tool_name_or_error_resolves_aliases() {
    let request = ToolExecutionRequest {
        tool_name: "sub_task".to_string(),
        parameters: vec![],
        session_id: None,
    };

    let canonical =
        canonical_tool_name_or_error(&request.tool_name).expect("expected canonical name");
    assert_eq!(canonical, "Task");
}

#[test]
fn parse_arguments_parses_json_and_plain_strings() {
    let args = parse_arguments(vec![
        ToolParameter {
            name: "recursive".to_string(),
            value: "false".to_string(),
        },
        ToolParameter {
            name: "path".to_string(),
            value: "/tmp".to_string(),
        },
    ]);

    assert_eq!(args["recursive"], false);
    assert_eq!(args["path"], "/tmp");
}

#[test]
fn trimmed_session_id_removes_blank_values() {
    let request = ToolExecutionRequest {
        tool_name: "read_file".to_string(),
        parameters: vec![],
        session_id: Some("   ".to_string()),
    };

    assert!(trimmed_session_id(request.session_id.as_deref()).is_none());
}

#[test]
fn validate_session_context_requirement_rejects_missing_session_for_edit() {
    let error = validate_session_context_requirement("Edit", None)
        .expect_err("expected missing-session validation error");

    match error {
        AppError::BadRequest(message) => {
            assert!(message.contains("requires session_id"));
            assert!(message.contains("Edit"));
        }
        other => panic!("unexpected error: {other}"),
    }
}

#[test]
fn build_tool_call_serializes_arguments() {
    let args = parse_arguments(vec![ToolParameter {
        name: "path".to_string(),
        value: "/tmp/test.txt".to_string(),
    }]);
    let call = build_tool_call("Read".to_string(), args).expect("tool call should build");

    assert_eq!(call.function.name, "Read");
    assert!(call.function.arguments.contains("/tmp/test.txt"));
}

#[test]
fn build_execution_response_defaults_display_preference() {
    let result = ToolResult {
        success: true,
        result: "{\"ok\":true}".to_string(),
        display_preference: None,
    };

    let response =
        build_execution_response("get_current_dir".to_string(), result).expect("response builds");
    let payload: serde_json::Value =
        serde_json::from_str(&response.result).expect("payload should be valid json");

    assert_eq!(payload["tool_name"], "get_current_dir");
    assert_eq!(payload["display_preference"], "Default");
}
