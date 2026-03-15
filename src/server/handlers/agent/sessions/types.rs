use serde::{Deserialize, Serialize};

use crate::agent::core::storage::SessionIndexEntry;

#[derive(Debug, Serialize)]
pub struct SessionSummary {
    pub id: String,
    pub kind: crate::agent::core::SessionKind,
    pub title: String,
    pub pinned: bool,
    pub parent_session_id: Option<String>,
    pub root_session_id: String,
    pub spawn_depth: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_by_schedule_id: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub last_activity_at: chrono::DateTime<chrono::Utc>,
    pub message_count: usize,
    pub has_attachments: bool,
    pub is_running: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_run_status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_run_error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_usage: Option<crate::agent::core::TokenBudgetUsage>,
}

impl SessionSummary {
    pub(crate) fn from_entry(entry: SessionIndexEntry, is_running: bool) -> Self {
        Self {
            id: entry.id,
            kind: entry.kind,
            title: entry.title,
            pinned: entry.pinned,
            parent_session_id: entry.parent_session_id,
            root_session_id: entry.root_session_id,
            spawn_depth: entry.spawn_depth,
            created_by_schedule_id: entry.created_by_schedule_id,
            created_at: entry.created_at,
            updated_at: entry.updated_at,
            last_activity_at: entry.last_activity_at,
            message_count: entry.message_count,
            has_attachments: entry.has_attachments,
            is_running,
            last_run_status: entry.last_run_status,
            last_run_error: entry.last_run_error,
            token_usage: entry.token_usage,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ListSessionsResponse {
    pub sessions: Vec<SessionSummary>,
}

#[derive(Debug, Deserialize)]
pub struct CreateSessionRequest {
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub system_prompt: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct CreateSessionResponse {
    pub session: SessionSummary,
}

#[derive(Debug, Serialize)]
pub struct GetSessionResponse {
    pub session: SessionSummary,
}

#[derive(Debug, Serialize)]
pub struct SessionSystemPromptResponse {
    pub session_id: String,
    pub base_system_prompt: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enhancement_prompt: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace_context: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skill_context: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_guide_context: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub external_memory: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub todo_list: Option<String>,
    pub effective_system_prompt: String,
}

#[derive(Debug, Deserialize)]
pub struct PatchSessionRequest {
    pub title: Option<String>,
    pub pinned: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct CleanupRequest {
    pub mode: String,
    #[serde(default)]
    pub keep_pinned: bool,
}
