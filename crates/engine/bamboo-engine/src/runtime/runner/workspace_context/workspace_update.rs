use crate::project_context::WorkspaceBindingStatus;
use bamboo_agent_core::tools::{ToolCall, ToolResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime::runner) struct WorkspaceUpdate {
    pub path: String,
    pub binding_status: WorkspaceBindingStatus,
}

pub(super) fn extract_workspace_path_from_tool_result(
    tool_call: &ToolCall,
    result: &ToolResult,
) -> Option<WorkspaceUpdate> {
    // File tools also report a `workspace`, but that is only the file's parent
    // directory. Only an explicit Workspace operation can change Session Source.
    if !result.success
        || !matches!(
            bamboo_tools::normalize_tool_ref(&tool_call.function.name).as_deref(),
            Some("SetWorkspace" | "Workspace")
        )
    {
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
