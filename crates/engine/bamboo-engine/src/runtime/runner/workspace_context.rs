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

pub(super) fn should_apply_workspace_update(
    session: &bamboo_agent_core::Session,
    tool_call: &bamboo_agent_core::tools::ToolCall,
) -> bool {
    workspace_update::should_apply_workspace_update(session, tool_call)
}

pub(super) fn is_explicit_workspace_tool(tool_call: &bamboo_agent_core::tools::ToolCall) -> bool {
    workspace_update::is_explicit_workspace_tool(tool_call)
}

#[cfg(test)]
mod tests;
