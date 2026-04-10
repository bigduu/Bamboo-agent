//! Session management endpoints (V2 index-backed).

mod handlers;
mod types;

pub use handlers::{
    cleanup_sessions, clear_session, create_session, get_attachment, get_session,
    get_system_prompt_snapshot, list_sessions, patch_session, run_project_dream,
};
pub use types::{
    CleanupRequest, CreateSessionRequest, CreateSessionResponse, GetSessionResponse,
    ListSessionsResponse, PatchSessionRequest, SessionSummary, SessionSystemPromptResponse,
};

#[cfg(test)]
mod tests;
