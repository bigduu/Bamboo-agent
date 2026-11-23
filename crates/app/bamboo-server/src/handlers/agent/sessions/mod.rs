//! Session management endpoints (V2 index-backed).

mod handlers;
mod types;

pub(crate) use handlers::spawn_automatic_session_archive;
pub use handlers::{
    activate_discoverable_tools, archive_session, cleanup_sessions, clear_session, compact_session,
    copy_session, create_session, deactivate_discoverable_tools, get_attachment, get_session,
    get_session_compaction, get_session_create_operation, get_system_prompt_snapshot,
    inspect_session_version_migration, list_discoverable_tools, list_sessions,
    migrate_session_version, patch_session, preview_session_archives, recover_root_mode,
    regenerate_session_title, restore_session_archive, run_project_dream,
    running_sessions_snapshot, select_root_mode,
};
pub use types::{
    ActivateDiscoverableToolsRequest, CleanupRequest, CopySessionResponse, CreateSessionRequest,
    CreateSessionResponse, DiscoverableToolsResponse, GetSessionResponse, ListSessionsResponse,
    PatchSessionRequest, RunningSessionEntry, RunningSessionsResponse, SessionActivity,
    SessionCreateOperationError, SessionCreateOperationResponse, SessionCreateOperationStatus,
    SessionSummary, SessionSystemPromptResponse,
};

#[cfg(test)]
mod tests;
