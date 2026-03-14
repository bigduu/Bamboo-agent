//! Agent management endpoints for Claude Code integration.

mod fs;
mod projects;
mod routes;
mod sessions;
mod settings;
mod types;

pub use projects::{create_project, get_project_sessions, list_projects};
pub use routes::config;
pub use sessions::{
    cancel_claude_execution, claude_events, execute_claude_code, get_session_jsonl,
    list_running_claude_sessions, list_running_claude_sessions_stateful,
};
pub use settings::{
    get_claude_settings, get_system_prompt, save_claude_settings, save_system_prompt,
};
pub use types::{
    CancelRequest, ClaudeSettings, CreateProjectRequest, ExecuteRequest, Project,
    SaveSettingsRequest, SaveSystemPromptRequest, Session,
};
