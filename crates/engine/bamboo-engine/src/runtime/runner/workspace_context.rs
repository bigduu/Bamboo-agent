//! Workspace context helpers used by the agent loop runner.

mod prompt;
mod workspace_update;

pub(super) fn apply_workspace_path_to_session(
    session: &mut bamboo_agent_core::Session,
    workspace_path: &str,
    binding_status: crate::project_context::WorkspaceBindingStatus,
) {
    prompt::apply_workspace_path_to_session(session, workspace_path, binding_status);
}

pub(super) fn extract_workspace_path_from_tool_result(
    tool_call: &bamboo_agent_core::tools::ToolCall,
    result: &bamboo_agent_core::tools::ToolResult,
) -> Option<workspace_update::WorkspaceUpdate> {
    workspace_update::extract_workspace_path_from_tool_result(tool_call, result)
}

#[cfg(test)]
mod tests;
