use crate::error::AppError;
use bamboo_agent_core::tools::ToolResult;

use super::{
    direct_tool_session_flags, enforce_plan_tool_gate,
    models::{ToolExecutionRequest, ToolParameter},
    request::{
        build_tool_call, canonical_tool_name_or_error, parse_arguments, trimmed_session_id,
        validate_session_context_requirement,
    },
    response::build_execution_response,
};

#[test]
fn direct_http_unknown_session_id_inherits_configured_auto() {
    let flags = direct_tool_session_flags(bamboo_domain::PermissionMode::Auto, None);

    assert!(flags.auto_approve_permissions);
    assert!(!flags.bypass_permissions);
    assert!(!flags.plan_read_only);
}

#[test]
fn direct_http_persisted_modes_remain_typed_and_plan_is_authoritative() {
    let mut auto = bamboo_agent_core::Session::new("http-auto", "model");
    let mut auto_runtime = bamboo_domain::AgentRuntimeState::new("http-auto");
    auto_runtime.set_permission_mode(bamboo_domain::SessionPermissionMode::Auto);
    auto.agent_runtime_state = Some(auto_runtime);
    let auto_flags = direct_tool_session_flags(bamboo_domain::PermissionMode::Default, Some(&auto));
    assert!(auto_flags.auto_approve_permissions);
    assert!(!auto_flags.bypass_permissions);

    let mut bypass = bamboo_agent_core::Session::new("http-bypass", "model");
    let mut bypass_runtime = bamboo_domain::AgentRuntimeState::new("http-bypass");
    bypass_runtime.set_permission_mode(bamboo_domain::SessionPermissionMode::Bypass);
    bypass.agent_runtime_state = Some(bypass_runtime);
    let bypass_flags =
        direct_tool_session_flags(bamboo_domain::PermissionMode::Auto, Some(&bypass));
    assert!(bypass_flags.bypass_permissions);
    assert!(!bypass_flags.auto_approve_permissions);

    let mut plan_auto = auto;
    plan_auto
        .agent_runtime_state
        .as_mut()
        .expect("runtime state")
        .plan_mode = Some(bamboo_domain::PlanModeState {
        entered_at: chrono::Utc::now(),
        pre_permission_mode: "auto".to_string(),
        plan_file_path: None,
        status: bamboo_domain::PlanModeStatus::Exploring,
    });
    let plan_flags =
        direct_tool_session_flags(bamboo_domain::PermissionMode::Default, Some(&plan_auto));
    assert!(plan_flags.plan_read_only);
    assert!(plan_flags.auto_approve_permissions);
    assert!(!plan_flags.bypass_permissions);
}

#[test]
fn direct_http_plan_auto_blocks_mutation_and_keeps_reads_available() {
    let mut session = bamboo_agent_core::Session::new("http-plan-auto", "model");
    let mut runtime = bamboo_domain::AgentRuntimeState::new("http-plan-auto");
    runtime.set_permission_mode(bamboo_domain::SessionPermissionMode::Auto);
    session.agent_runtime_state = Some(runtime);
    let flags =
        bamboo_agent_core::tools::ToolExecutionSessionFlags::from_session_and_configured_mode(
            &session,
            bamboo_domain::PermissionMode::Plan,
        );

    assert!(flags.plan_read_only);
    assert!(flags.auto_approve_permissions);
    assert!(!flags.bypass_permissions);
    assert!(matches!(
        enforce_plan_tool_gate(flags, "Write"),
        Err(AppError::ToolExecutionError(message)) if message.contains("Plan mode")
    ));
    enforce_plan_tool_gate(flags, "Read").expect("read-only HTTP tool remains available");
}

#[test]
fn canonical_tool_name_or_error_resolves_aliases() {
    let request = ToolExecutionRequest {
        tool_name: "sub_task".to_string(),
        parameters: vec![],
        session_id: None,
    };

    let canonical =
        canonical_tool_name_or_error(&request.tool_name).expect("expected canonical name");
    assert_eq!(canonical, "SubAgent");
}

#[test]
fn canonical_tool_name_or_error_resolves_sub_agent_manager_alias() {
    let canonical = canonical_tool_name_or_error("sub_session_manager")
        .expect("legacy manager alias should resolve");
    assert_eq!(canonical, "SubAgent");
}

#[test]
fn validate_session_context_requirement_rejects_missing_session_for_sub_agent_manager_alias() {
    let canonical = canonical_tool_name_or_error("sub_session_manager")
        .expect("legacy manager alias should resolve");
    let error = validate_session_context_requirement(&canonical, None)
        .expect_err("legacy manager alias should still require session context");

    match error {
        AppError::BadRequest(message) => {
            assert!(message.contains("requires session_id"));
            assert!(message.contains("SubAgent"));
        }
        other => panic!("unexpected error: {other}"),
    }
}

#[test]
fn canonical_tool_name_or_error_rejects_removed_compress_context() {
    let error = canonical_tool_name_or_error("compress_context")
        .expect_err("compress_context should not resolve once removed");
    assert!(matches!(error, AppError::ToolNotFound(name) if name == "compress_context"));
}

#[test]
fn canonical_tool_name_or_error_accepts_memory_server_tool() {
    let canonical = canonical_tool_name_or_error("memory").expect("memory should resolve");
    assert_eq!(canonical, "memory");
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
fn validate_session_context_requirement_rejects_missing_session_for_session_history() {
    let error = validate_session_context_requirement("session_history", None)
        .expect_err("expected missing-session validation error");

    match error {
        AppError::BadRequest(message) => {
            assert!(message.contains("requires session_id"));
            assert!(message.contains("session_history"));
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
        images: Vec::new(),
    };

    let response =
        build_execution_response("get_current_dir".to_string(), result).expect("response builds");
    let payload: serde_json::Value =
        serde_json::from_str(&response.result).expect("payload should be valid json");

    assert_eq!(payload["tool_name"], "get_current_dir");
    assert_eq!(payload["display_preference"], "Default");
}

#[test]
fn build_execution_response_uses_custom_display_preference() {
    let result = ToolResult {
        success: true,
        result: "{\"data\":\"test\"}".to_string(),
        display_preference: Some("Collapsible".to_string()),
        images: Vec::new(),
    };

    let response =
        build_execution_response("read_file".to_string(), result).expect("response builds");
    let payload: serde_json::Value =
        serde_json::from_str(&response.result).expect("payload should be valid json");

    assert_eq!(payload["tool_name"], "read_file");
    assert_eq!(payload["display_preference"], "Collapsible");
    assert_eq!(payload["result"], "{\"data\":\"test\"}");
}

#[test]
fn build_execution_response_preserves_result_content() {
    let complex_result = serde_json::json!({
        "status": "success",
        "data": {
            "files": ["a.txt", "b.txt"],
            "count": 2
        }
    });

    let result = ToolResult {
        success: true,
        result: complex_result.to_string(),
        display_preference: Some("Hidden".to_string()),
        images: Vec::new(),
    };

    let response =
        build_execution_response("list_files".to_string(), result).expect("response builds");
    let payload: serde_json::Value =
        serde_json::from_str(&response.result).expect("payload should be valid json");

    assert_eq!(payload["tool_name"], "list_files");
    assert_eq!(payload["success"], true);
    assert_eq!(payload["display_preference"], "Hidden");

    // Result is stored as a string in the payload
    let result_str = payload["result"].as_str().expect("result should be string");
    let result_value: serde_json::Value =
        serde_json::from_str(result_str).expect("result should be valid json");
    assert_eq!(result_value["status"], "success");
    assert_eq!(result_value["data"]["count"], 2);
}

#[test]
fn build_execution_response_handles_failed_tool_result() {
    let result = ToolResult {
        success: false,
        result: "Error: File not found".to_string(),
        display_preference: None,
        images: Vec::new(),
    };

    let response =
        build_execution_response("read_file".to_string(), result).expect("response builds");
    let payload: serde_json::Value =
        serde_json::from_str(&response.result).expect("payload should be valid json");

    assert_eq!(payload["tool_name"], "read_file");
    assert_eq!(payload["success"], false);
    assert_eq!(payload["display_preference"], "Default");
    assert_eq!(payload["result"], "Error: File not found");
}

#[test]
fn build_execution_response_handles_empty_result() {
    let result = ToolResult {
        success: true,
        result: "".to_string(),
        display_preference: Some("Default".to_string()),
        images: Vec::new(),
    };

    let response =
        build_execution_response("empty_tool".to_string(), result).expect("response builds");
    let payload: serde_json::Value =
        serde_json::from_str(&response.result).expect("payload should be valid json");

    assert_eq!(payload["tool_name"], "empty_tool");
    assert_eq!(payload["result"], "");
}

#[test]
fn build_execution_response_handles_special_characters_in_result() {
    let result = ToolResult {
        success: true,
        result: "Result with special chars: <>&\"'\\n\\t".to_string(),
        display_preference: None,
        images: Vec::new(),
    };

    let response = build_execution_response("special_chars_tool".to_string(), result)
        .expect("response builds");
    let payload: serde_json::Value =
        serde_json::from_str(&response.result).expect("payload should be valid json");

    assert_eq!(payload["tool_name"], "special_chars_tool");
    assert!(payload["result"].as_str().unwrap().contains("<>&\"'"));
}

#[test]
fn build_execution_response_handles_unicode_in_result() {
    let result = ToolResult {
        success: true,
        result: "Unicode: 你好世界 🌍".to_string(),
        display_preference: Some("Default".to_string()),
        images: Vec::new(),
    };

    let response =
        build_execution_response("unicode_tool".to_string(), result).expect("response builds");
    let payload: serde_json::Value =
        serde_json::from_str(&response.result).expect("payload should be valid json");

    assert_eq!(payload["tool_name"], "unicode_tool");
    assert!(payload["result"].as_str().unwrap().contains("你好世界"));
}

#[test]
fn build_execution_response_handles_large_result() {
    let large_data = "x".repeat(10000);
    let result = ToolResult {
        success: true,
        result: large_data.clone(),
        display_preference: None,
        images: Vec::new(),
    };

    let response =
        build_execution_response("large_tool".to_string(), result).expect("response builds");
    let payload: serde_json::Value =
        serde_json::from_str(&response.result).expect("payload should be valid json");

    assert_eq!(payload["tool_name"], "large_tool");
    assert_eq!(payload["result"].as_str().unwrap().len(), 10000);
}
