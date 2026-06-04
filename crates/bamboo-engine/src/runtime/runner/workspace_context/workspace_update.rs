use bamboo_agent_core::tools::{ToolCall, ToolResult};
use bamboo_agent_core::Session;

mod path_utils;
mod tool_args;

pub(super) fn extract_workspace_path_from_tool_result(
    tool_call: &ToolCall,
    result: &ToolResult,
) -> Option<String> {
    if !result.success || !tool_args::is_workspace_update_tool(tool_call)? {
        return None;
    }

    let payload: serde_json::Value = serde_json::from_str(&result.result).ok()?;
    payload
        .get("workspace")
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

pub(super) fn should_apply_workspace_update(session: &Session, tool_call: &ToolCall) -> bool {
    let Some(normalized_tool_name) = tool_args::normalized_tool_name(tool_call) else {
        return false;
    };

    if normalized_tool_name == "SetWorkspace" {
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
