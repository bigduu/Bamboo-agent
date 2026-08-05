//! Session management endpoints (V2 index-backed).

mod handlers;
mod types;

pub use handlers::{
    activate_discoverable_tools, cleanup_sessions, clear_session, create_session,
    deactivate_discoverable_tools, get_attachment, get_session, get_session_create_operation,
    get_system_prompt_snapshot, list_discoverable_tools, list_sessions, patch_session,
    regenerate_session_title, run_project_dream, running_sessions_snapshot,
};
pub use types::{
    ActivateDiscoverableToolsRequest, CleanupRequest, CreateSessionRequest, CreateSessionResponse,
    DiscoverableToolsResponse, GetSessionResponse, ListSessionsResponse, PatchSessionRequest,
    RunningSessionEntry, RunningSessionsResponse, SessionCreateOperationError,
    SessionCreateOperationResponse, SessionCreateOperationStatus, SessionSummary,
    SessionSystemPromptResponse,
};

#[cfg(test)]
mod tests;
