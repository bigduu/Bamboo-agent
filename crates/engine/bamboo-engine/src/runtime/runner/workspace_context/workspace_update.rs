use crate::project_context::WorkspaceBindingStatus;
use bamboo_agent_core::tools::{ToolCall, ToolResult};
use bamboo_agent_core::Session;

mod path_utils;
mod tool_args;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime::runner) struct WorkspaceUpdate {
    pub path: String,
    pub binding_status: WorkspaceBindingStatus,
}

pub(super) fn extract_workspace_path_from_tool_result(
    tool_call: &ToolCall,
    result: &ToolResult,
) -> Option<WorkspaceUpdate> {
    if !result.success || !tool_args::is_workspace_update_tool(tool_call)? {
        return None;
    }

    let payload: serde_json::Value = serde_json::from_str(&result.result).ok()?;
    let path = payload
        .get("workspace")
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)?;
    let binding_status = match payload
        .get("binding_status")
        .and_then(|value| value.as_str())
    {
        Some("registered") => WorkspaceBindingStatus::Registered,
        _ => WorkspaceBindingStatus::Unregistered,
    };
    Some(WorkspaceUpdate {
        path,
        binding_status,
    })
}

pub(super) fn should_apply_workspace_update(session: &Session, tool_call: &ToolCall) -> bool {
    let Some(normalized_tool_name) = tool_args::normalized_tool_name(tool_call) else {
        return false;
    };

    if matches!(normalized_tool_name.as_str(), "SetWorkspace" | "Workspace") {
        return true;
    }

    if !tool_args::is_write_or_edit_tool_name(normalized_tool_name) {
        return false;
    }

    let current_workspace = session
        .workspace_path_meta()
        .map(|path| path.trim().to_string())
        .filter(|path| !path.is_empty());
    let Some(current_workspace) = current_workspace.as_deref() else {
        return true;
    };

    let Some(target_file_path) = tool_args::extract_target_file_path_from_tool_call(tool_call)
    else {
        return true;
    };

    !path_utils::path_is_within_workspace(&target_file_path, current_workspace)
}

pub(super) fn is_explicit_workspace_tool(tool_call: &ToolCall) -> bool {
    tool_args::normalized_tool_name(tool_call)
        .is_some_and(|name| matches!(name.as_str(), "SetWorkspace" | "Workspace"))
}
