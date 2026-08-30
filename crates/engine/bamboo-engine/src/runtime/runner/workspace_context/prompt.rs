use crate::project_context::{
    WorkspaceBindingStatus, WorkspaceSource, WORKSPACE_BINDING_STATUS_METADATA_KEY,
    WORKSPACE_SOURCE_METADATA_KEY,
};
use bamboo_agent_core::Session;

/// Commit a host-selected workspace to the authoritative session metadata.
///
/// Provider-visible context is rebuilt from this state during round assembly.
/// Assigning or switching a workspace never creates or edits a persisted
/// System message.
pub(super) fn apply_workspace_path_to_session(
    session: &mut Session,
    workspace_path: &str,
    binding_status: WorkspaceBindingStatus,
) {
    let workspace_path = workspace_path.trim();
    if workspace_path.is_empty() {
        return;
    }

    session.set_workspace_path_meta(workspace_path);
    session.metadata.insert(
        WORKSPACE_SOURCE_METADATA_KEY.to_string(),
        WorkspaceSource::Explicit.as_str().to_string(),
    );
    session.metadata.insert(
        WORKSPACE_BINDING_STATUS_METADATA_KEY.to_string(),
        binding_status.as_str().to_string(),
    );

    // Keep the diagnostic snapshot current without making it authoritative
    // and without touching Session.messages.
    crate::runtime::runner::refresh_prompt_snapshot(session);
}
