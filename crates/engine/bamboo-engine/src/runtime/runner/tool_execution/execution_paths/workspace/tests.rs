use super::maybe_apply_workspace_update;
use crate::project_context::{
    WORKSPACE_BINDING_STATUS_METADATA_KEY, WORKSPACE_SOURCE_METADATA_KEY,
};
use bamboo_agent_core::tools::{FunctionCall, ToolCall, ToolResult};
use bamboo_agent_core::{Message, Session};
use serde_json::json;

fn call(name: &str, arguments: serde_json::Value) -> ToolCall {
    ToolCall {
        id: "workspace-result".into(),
        tool_type: "function".into(),
        function: FunctionCall {
            name: name.into(),
            arguments: arguments.to_string(),
        },
    }
}

fn result(payload: serde_json::Value) -> ToolResult {
    ToolResult {
        success: true,
        result: payload.to_string(),
        display_preference: Some("json".into()),
        images: Vec::new(),
    }
}

#[test]
fn file_operations_preserve_existing_and_missing_session_scope() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("project");
    let temporary = root.path().join("temporary-files");
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::create_dir_all(&temporary).unwrap();
    let workspace = workspace.to_string_lossy().into_owned();
    let targets = [
        std::path::PathBuf::from(&workspace).join("inside.md"),
        temporary.join("issue-body.md"),
        std::path::PathBuf::from("relative.md"),
    ];

    for name in [
        "Write",
        "Edit",
        "write_file",
        "default::Write",
        "default::Edit",
    ] {
        for target in &targets {
            for initial_workspace in [None, Some(""), Some(workspace.as_str())] {
                for project in [None, Some("project-1")] {
                    let mut session = Session::new("file-workspace", "test-model");
                    session.add_message(Message::system("Preserve this context"));
                    if let Some(path) = initial_workspace {
                        session.set_workspace_path_meta(path);
                        session.metadata.insert(
                            WORKSPACE_SOURCE_METADATA_KEY.into(),
                            "project_default".into(),
                        );
                        session.metadata.insert(
                            WORKSPACE_BINDING_STATUS_METADATA_KEY.into(),
                            "registered".into(),
                        );
                    }
                    if let Some(project) = project {
                        session.set_project_id_meta(project);
                    }
                    let before = serde_json::to_value(&session).unwrap();
                    // Even a file result claiming a registered workspace is
                    // data, not permission to bind or retarget the Session.
                    let output = result(json!({
                        "workspace": temporary,
                        "binding_status": "registered",
                        "operation": name,
                    }));
                    maybe_apply_workspace_update(
                        &mut session,
                        &call(name, json!({"file_path": target})),
                        &output,
                        "file-workspace",
                    );
                    assert_eq!(
                        serde_json::to_value(&session).unwrap(),
                        before,
                        "{name} at {} must preserve all Session state ({initial_workspace:?}, {project:?})",
                        target.display(),
                    );
                }
            }
        }
    }
}

#[test]
fn explicit_workspace_results_and_legacy_aliases_still_update_session_scope() {
    for name in [
        "Workspace",
        "SetWorkspace",
        "setWorkspace",
        "set_workspace",
        "default::setWorkspace",
    ] {
        let mut session = Session::new("explicit-workspace", "test-model");
        session.set_project_id_meta("project-1");
        session.set_workspace_path_meta("old-workspace");
        maybe_apply_workspace_update(
            &mut session,
            &call(name, json!({"path":"new-workspace"})),
            &result(json!({"workspace":"new-workspace","binding_status":"registered"})),
            "explicit-workspace",
        );
        assert_eq!(
            session.workspace_path_meta().as_deref(),
            Some("new-workspace")
        );
        assert_eq!(session.project_id_meta().as_deref(), Some("project-1"));
        assert_eq!(session.metadata[WORKSPACE_SOURCE_METADATA_KEY], "explicit");
        assert_eq!(
            session.metadata[WORKSPACE_BINDING_STATUS_METADATA_KEY],
            "registered"
        );
    }
}

#[test]
fn failed_or_missing_workspace_results_preserve_session_scope() {
    for (success, payload) in [
        (false, json!({"workspace":"rejected-workspace"})),
        (true, json!({})),
        (true, json!({"workspace":"  "})),
    ] {
        let mut output = result(payload);
        output.success = success;
        let mut session = Session::new("failed-workspace", "test-model");
        session.set_workspace_path_meta("old-workspace");
        let before = serde_json::to_value(&session).unwrap();
        maybe_apply_workspace_update(
            &mut session,
            &call("Workspace", json!({"path":"rejected-workspace"})),
            &output,
            "failed-workspace",
        );
        assert_eq!(serde_json::to_value(&session).unwrap(), before);
    }
}
