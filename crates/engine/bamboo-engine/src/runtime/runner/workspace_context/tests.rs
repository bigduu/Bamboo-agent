use super::{
    apply_workspace_path_to_session, extract_workspace_path_from_tool_result,
    should_apply_workspace_update,
};
use crate::project_context::WorkspaceBindingStatus;
use bamboo_agent_core::tools::{FunctionCall, ToolCall, ToolResult};
use bamboo_agent_core::{Message, Session};

#[test]
fn extract_workspace_path_from_tool_result_supports_alias_name() {
    let tool_call = ToolCall {
        id: "call_1".to_string(),
        tool_type: "function".to_string(),
        function: FunctionCall {
            name: "setWorkspace".to_string(),
            arguments: "{}".to_string(),
        },
    };
    let result = ToolResult {
        success: true,
        result: r#"{"workspace":"/tmp/ws"}"#.to_string(),
        display_preference: Some("json".to_string()),
        images: Vec::new(),
    };

    assert_eq!(
        extract_workspace_path_from_tool_result(&tool_call, &result),
        Some(super::workspace_update::WorkspaceUpdate {
            path: "/tmp/ws".to_string(),
            binding_status: WorkspaceBindingStatus::Unregistered,
        })
    );
}

#[test]
fn workspace_tool_set_is_an_explicit_persisted_update() {
    let session = Session::new("session-workspace", "test-model");
    let tool_call = ToolCall {
        id: "call-workspace".to_string(),
        tool_type: "function".to_string(),
        function: FunctionCall {
            name: "Workspace".to_string(),
            arguments: r#"{"path":"/tmp/project"}"#.to_string(),
        },
    };
    assert!(should_apply_workspace_update(&session, &tool_call));
}

#[test]
fn should_apply_workspace_update_when_workspace_is_missing_for_write() {
    let session = Session::new("session-1", "test-model");
    let tool_call = ToolCall {
        id: "call_1".to_string(),
        tool_type: "function".to_string(),
        function: FunctionCall {
            name: "Write".to_string(),
            arguments: r#"{"file_path":"/tmp/project/src/main.rs"}"#.to_string(),
        },
    };

    assert!(should_apply_workspace_update(&session, &tool_call));
}

#[test]
fn should_not_apply_workspace_update_when_target_is_inside_workspace() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let workspace = temp_dir.path().join("workspace");
    let file_path = workspace.join("src").join("main.rs");
    std::fs::create_dir_all(file_path.parent().expect("has parent"))
        .expect("create workspace dirs");
    std::fs::write(&file_path, "fn main() {}\n").expect("write test file");

    let workspace_display = bamboo_config::paths::path_to_display_string(&workspace);
    let file_display = bamboo_config::paths::path_to_display_string(&file_path);

    let mut session = Session::new("session-1", "test-model");
    session
        .metadata
        .insert("workspace_path".to_string(), workspace_display);

    let tool_call = ToolCall {
        id: "call_1".to_string(),
        tool_type: "function".to_string(),
        function: FunctionCall {
            name: "Edit".to_string(),
            arguments: serde_json::json!({ "file_path": file_display }).to_string(),
        },
    };

    assert!(!should_apply_workspace_update(&session, &tool_call));
}

#[test]
fn should_apply_workspace_update_when_target_is_outside_workspace() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let workspace = temp_dir.path().join("workspace");
    let external_dir = temp_dir.path().join("external");
    let notebook_path = external_dir.join("notes.ipynb");
    std::fs::create_dir_all(&workspace).expect("create workspace dir");
    std::fs::create_dir_all(&external_dir).expect("create external dir");
    std::fs::write(&notebook_path, "{}").expect("write notebook");

    let workspace_display = bamboo_config::paths::path_to_display_string(&workspace);
    let notebook_display = bamboo_config::paths::path_to_display_string(&notebook_path);

    let mut session = Session::new("session-1", "test-model");
    session
        .metadata
        .insert("workspace_path".to_string(), workspace_display);

    let tool_call = ToolCall {
        id: "call_1".to_string(),
        tool_type: "function".to_string(),
        function: FunctionCall {
            name: "NotebookEdit".to_string(),
            arguments: serde_json::json!({ "notebook_path": notebook_display }).to_string(),
        },
    };

    assert!(should_apply_workspace_update(&session, &tool_call));
}

#[test]
fn apply_workspace_path_to_session_updates_metadata_without_mutating_system() {
    let mut session = Session::new("session-1", "test-model");
    session.add_message(Message::system("Base prompt".to_string()));
    let message_id_before = session.messages[0].id.clone();

    apply_workspace_path_to_session(
        &mut session,
        "/tmp/workspace",
        WorkspaceBindingStatus::Unregistered,
    );

    assert_eq!(
        session.metadata.get("workspace_path"),
        Some(&"/tmp/workspace".to_string())
    );
    assert_eq!(
        session
            .metadata
            .get(crate::project_context::WORKSPACE_SOURCE_METADATA_KEY)
            .map(String::as_str),
        Some("explicit")
    );
    assert_eq!(
        session
            .metadata
            .get(crate::project_context::WORKSPACE_BINDING_STATUS_METADATA_KEY)
            .map(String::as_str),
        Some("unregistered")
    );
    assert_eq!(session.messages.len(), 1);
    assert_eq!(session.messages[0].id, message_id_before);
    assert_eq!(session.messages[0].content, "Base prompt");
    let snapshot = crate::runtime::runner::read_prompt_snapshot(&session)
        .expect("prompt snapshot should exist after workspace update");
    assert!(snapshot
        .workspace_context
        .as_deref()
        .is_some_and(|value| value.contains("/tmp/workspace")));
    assert_eq!(snapshot.effective_system_prompt, "Base prompt");
}

#[test]
fn apply_workspace_path_to_session_without_system_creates_no_message() {
    let mut session = Session::new("session-no-system", "test-model");

    apply_workspace_path_to_session(
        &mut session,
        "/tmp/workspace",
        WorkspaceBindingStatus::Unregistered,
    );

    assert!(session.messages.is_empty());
    assert_eq!(
        session.workspace_path_meta().as_deref(),
        Some("/tmp/workspace")
    );
}

#[test]
fn registered_workspace_update_strips_legacy_project_host_paths_once() {
    let project_block = format!(
        "{}\nProject ID: project-1\nProject name: Zenith\nProject home: /data/projects/project-1\n{}",
        crate::runtime::context::PROJECT_CONTEXT_START_MARKER,
        crate::runtime::context::PROJECT_CONTEXT_END_MARKER,
    );
    let mut session = Session::new("session-registered", "test-model");
    session.add_message(Message::system(format!("Base\n\n{project_block}")));
    let message_id_before = session.messages[0].id.clone();

    apply_workspace_path_to_session(
        &mut session,
        "/tmp/registered",
        WorkspaceBindingStatus::Registered,
    );

    assert_eq!(session.messages.len(), 1);
    assert_eq!(session.messages[0].id, message_id_before);
    assert_eq!(session.messages[0].content, "Base");
    assert!(!session.messages[0].content.contains("/data/projects"));
    assert_eq!(
        session.workspace_path_meta(),
        Some("/tmp/registered".to_string())
    );
    assert_eq!(
        session
            .metadata
            .get(crate::project_context::WORKSPACE_BINDING_STATUS_METADATA_KEY)
            .map(String::as_str),
        Some("registered")
    );
}
