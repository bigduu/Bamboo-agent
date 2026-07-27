use super::prompt::upsert_workspace_context;
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
fn upsert_workspace_context_replaces_existing_segment() {
    let guidance = crate::runtime::context::workspace_prompt_guidance();
    let old = format!(
        "Base prompt\n\nWorkspace path: /old/path\n{}\n\n## Tool Usage Guidelines\nX",
        guidance
    );
    let updated = upsert_workspace_context(&old, "/new/path", WorkspaceBindingStatus::Unregistered);

    assert!(updated.contains("Workspace path: /new/path"));
    assert!(!updated.contains("Workspace path: /old/path"));
    assert!(updated.contains(crate::runtime::context::WORKSPACE_CONTEXT_START_MARKER));
    assert!(updated.contains(crate::runtime::context::WORKSPACE_CONTEXT_END_MARKER));
    assert!(updated.contains("## Tool Usage Guidelines"));
}

#[test]
fn workspace_upsert_preserves_stable_project_block() {
    let project_block = format!(
        "{}\nProject ID: project-1\nProject name: Zenith\n{}",
        crate::runtime::context::PROJECT_CONTEXT_START_MARKER,
        crate::runtime::context::PROJECT_CONTEXT_END_MARKER,
    );
    let old_workspace = crate::runtime::context::build_workspace_prompt_context("/old/path")
        .expect("workspace context");
    let prompt = format!("Base prompt\n\n{project_block}\n\n{old_workspace}");

    let updated =
        upsert_workspace_context(&prompt, "/new/path", WorkspaceBindingStatus::Unregistered);
    assert!(updated.contains(&project_block));
    assert_eq!(
        updated
            .matches(crate::runtime::context::PROJECT_CONTEXT_START_MARKER)
            .count(),
        1
    );
    assert_eq!(
        updated
            .matches(crate::runtime::context::WORKSPACE_CONTEXT_START_MARKER)
            .count(),
        1
    );
}

#[test]
fn apply_workspace_path_to_session_updates_metadata_and_prompt() {
    let mut session = Session::new("session-1", "test-model");
    session.add_message(Message::system("Base prompt".to_string()));

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
    let system_content = session
        .messages
        .iter()
        .find(|message| matches!(message.role, bamboo_agent_core::Role::System))
        .map(|message| message.content.clone())
        .unwrap_or_default();
    assert!(system_content.contains("Workspace path: /tmp/workspace"));
    assert!(system_content.contains("Workspace source: explicit"));
    assert!(system_content.contains(crate::runtime::context::WORKSPACE_CONTEXT_START_MARKER));
    assert!(system_content.contains(crate::runtime::context::WORKSPACE_CONTEXT_END_MARKER));
    let snapshot = crate::runtime::runner::read_prompt_snapshot(&session)
        .expect("prompt snapshot should exist after workspace update");
    assert!(snapshot
        .workspace_context
        .as_deref()
        .is_some_and(|value| value.contains("/tmp/workspace")));
    assert!(snapshot
        .effective_system_prompt
        .contains("Workspace path: /tmp/workspace"));
}

#[test]
fn registered_workspace_update_preserves_project_block_byte_for_byte() {
    let project_block = format!(
        "{}\nProject ID: project-1\nProject name: Zenith\nProject home: /data/projects/project-1\n{}",
        crate::runtime::context::PROJECT_CONTEXT_START_MARKER,
        crate::runtime::context::PROJECT_CONTEXT_END_MARKER,
    );
    let mut session = Session::new("session-registered", "test-model");
    session.add_message(Message::system(format!("Base\n\n{project_block}")));

    apply_workspace_path_to_session(
        &mut session,
        "/tmp/registered",
        WorkspaceBindingStatus::Registered,
    );

    let system = session
        .messages
        .iter()
        .find(|message| matches!(message.role, bamboo_agent_core::Role::System))
        .unwrap();
    assert!(system.content.contains(&project_block));
    assert!(system.content.contains("Binding status: registered"));
    assert_eq!(
        system
            .content
            .matches(crate::runtime::context::PROJECT_CONTEXT_START_MARKER)
            .count(),
        1
    );
    assert_eq!(
        session.workspace_path_meta(),
        Some("/tmp/registered".to_string())
    );
}
