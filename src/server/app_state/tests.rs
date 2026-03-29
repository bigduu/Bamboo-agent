use super::{AppState, DEFAULT_BASE_PROMPT};
use crate::agent::core::tools::{FunctionCall, ToolCall, ToolError};
use crate::agent::tools::permission::config::{PermissionConfig, PermissionRule, PermissionType};
use crate::agent::tools::permission::storage::PermissionStorage;
use serde_json::json;

fn make_tool_call(name: &str, args: serde_json::Value) -> ToolCall {
    ToolCall {
        id: format!("call_{name}"),
        tool_type: "function".to_string(),
        function: FunctionCall {
            name: name.to_string(),
            arguments: args.to_string(),
        },
    }
}

#[test]
fn default_base_prompt_does_not_unconditionally_require_conclusion_with_options() {
    let normalized = DEFAULT_BASE_PROMPT.to_ascii_lowercase();
    assert!(!normalized.contains("before ending a task, always call conclusion_with_options"));
    assert!(!normalized.contains("do not ask final confirmation in plain assistant text"));
}
#[tokio::test]
async fn test_app_state_creation() {
    let temp_dir = tempfile::tempdir().unwrap();
    let state = AppState::new(temp_dir.path().to_path_buf())
        .await
        .expect("app state should initialize");

    // Verify basic fields
    assert!(state.sessions.read().await.is_empty());
}

#[tokio::test]
async fn root_tools_include_server_overlays_and_memory_note() {
    let temp_dir = tempfile::tempdir().unwrap();
    let state = AppState::new(temp_dir.path().to_path_buf())
        .await
        .expect("app state should initialize");
    let names: std::collections::HashSet<String> = state
        .get_all_tool_schemas()
        .into_iter()
        .map(|schema| schema.function.name)
        .collect();

    assert!(names.contains("Task"));
    assert!(names.contains("SubSession"));
    assert!(names.contains("schedule_tasks"));
    assert!(names.contains("sub_session_manager"));
    assert!(names.contains("session_inspector"));
    assert!(names.contains("load_skill"));
    assert!(names.contains("read_skill_resource"));
    assert!(names.contains("memory_note"));
}

#[tokio::test]
async fn child_tools_exclude_schedule_and_session_inspector() {
    let temp_dir = tempfile::tempdir().unwrap();
    let state = AppState::new(temp_dir.path().to_path_buf())
        .await
        .expect("app state should initialize");
    let names: std::collections::HashSet<String> = state
        .child_tools
        .list_tools()
        .into_iter()
        .map(|schema| schema.function.name)
        .collect();

    assert!(!names.contains("schedule_tasks"));
    assert!(!names.contains("sub_session_manager"));
    assert!(!names.contains("session_inspector"));
    assert!(names.contains("load_skill"));
    assert!(names.contains("read_skill_resource"));
    assert!(names.contains("memory_note"));
}

#[tokio::test]
async fn overlay_tools_require_session_context() {
    let temp_dir = tempfile::tempdir().unwrap();
    let state = AppState::new(temp_dir.path().to_path_buf())
        .await
        .expect("app state should initialize");

    let schedule_result = state
        .tools
        .execute(&make_tool_call(
            "schedule_tasks",
            json!({ "action": "list" }),
        ))
        .await;
    assert!(matches!(
        schedule_result,
        Err(ToolError::Execution(msg)) if msg.contains("session_id")
    ));

    let inspector_result = state
        .tools
        .execute(&make_tool_call(
            "session_inspector",
            json!({ "action": "list" }),
        ))
        .await;
    assert!(matches!(
        inspector_result,
        Err(ToolError::Execution(msg)) if msg.contains("session_id")
    ));

    let sub_session_manager_result = state
        .tools
        .execute(&make_tool_call(
            "sub_session_manager",
            json!({ "action": "list" }),
        ))
        .await;
    assert!(matches!(
        sub_session_manager_result,
        Err(ToolError::Execution(msg)) if msg.contains("session_id")
    ));
}

#[tokio::test]
async fn app_state_uses_persisted_permission_config_in_data_dir() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = PermissionStorage::new(temp_dir.path());
    let config = PermissionConfig::new();
    config.set_enabled(true);
    config.add_rule(PermissionRule::new(PermissionType::WriteFile, "*", false));
    storage.save(&config).await.unwrap();

    let state = AppState::new(temp_dir.path().to_path_buf())
        .await
        .expect("app state should initialize");
    let target = temp_dir.path().join("blocked.txt");
    let call = make_tool_call(
        "Write",
        json!({
            "file_path": target,
            "content": "blocked"
        }),
    );

    let result = state.tools.execute(&call).await;
    assert!(matches!(result, Err(ToolError::Execution(_))));
    assert!(!target.exists());
}
