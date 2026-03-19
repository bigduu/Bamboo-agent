use crate::agent::core::tools::{ToolCall, ToolResult};
use crate::agent::core::Session;

pub(super) fn maybe_apply_workspace_update(
    session: &mut Session,
    tool_call: &ToolCall,
    result: &ToolResult,
    session_id: &str,
) {
    if let Some(workspace_path) =
        super::super::super::workspace_context::extract_workspace_path_from_tool_result(
            tool_call, result,
        )
    {
        if super::super::super::workspace_context::should_apply_workspace_update(session, tool_call)
        {
            super::super::super::workspace_context::apply_workspace_path_to_session(
                session,
                &workspace_path,
            );
            tracing::info!(
                "[{}] Updated session workspace_path via {}: {}",
                session_id,
                tool_call.function.name,
                workspace_path
            );
        }
    }
}
