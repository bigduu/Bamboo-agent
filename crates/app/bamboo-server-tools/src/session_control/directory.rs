//! Authoritative, read-only session directory for the Supervisor.
//!
//! The directory answers "which sessions exist in my authorized scope, who
//! opened them, has a human engaged them, and what is their persisted
//! progress" without relying on the Supervisor's own conversational memory.
//! Every entry is an observation; none grants control authority. Followup,
//! loop and attach still require their existing explicit link/CAS contracts.

use async_trait::async_trait;
use bamboo_agent_core::tools::{ToolCtx, ToolError};
use bamboo_domain::HumanInputMark;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Upper bound for one directory page. Mirrors `MAX_DIRECTORY_PAGE_SIZE` in
/// session_inspector::discovery.
pub const MAX_DIRECTORY_PAGE_SIZE: usize = 20;
/// Upper bound for a lexical query filter, in bytes.
pub const MAX_DIRECTORY_QUERY_BYTES: usize = 128;
/// Upper bound for an opaque cursor, in bytes.
pub const MAX_DIRECTORY_CURSOR_BYTES: usize = 4_096;

#[derive(Debug, Clone, Default)]
pub struct SupervisorDirectoryRequest {
    pub query: Option<String>,
    pub limit: Option<usize>,
    pub cursor: Option<String>,
}

impl SupervisorDirectoryRequest {
    pub fn validate(&self) -> Result<(), ToolError> {
        if self
            .limit
            .is_some_and(|limit| !(1..=MAX_DIRECTORY_PAGE_SIZE).contains(&limit))
        {
            return Err(invalid("directory limit must be between 1 and 20"));
        }
        if self
            .query
            .as_ref()
            .is_some_and(|query| query.len() > MAX_DIRECTORY_QUERY_BYTES)
            || self
                .cursor
                .as_ref()
                .is_some_and(|cursor| cursor.len() > MAX_DIRECTORY_CURSOR_BYTES)
        {
            return Err(invalid("directory query or cursor exceeds its bound"));
        }
        Ok(())
    }
}

/// Where a directory entry came from. `SupervisorCreated` roots carry a live
/// or tombstoned management link minted at creation; `UserOpened` sessions
/// exist in a Host-authorized Project without any management link.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SessionDirectoryOrigin {
    SupervisorCreated,
    UserOpened,
}

/// One observed session. Metadata only: no transcript, no credentials, and no
/// control grant of any kind.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionDirectoryEntry {
    pub session_id: String,
    pub origin: SessionDirectoryOrigin,
    /// Whether a live management link exists (followup/loop control routes
    /// remain available). Tombstoned or never-linked entries are `false`.
    pub linked: bool,
    pub project_id: String,
    pub title: String,
    pub created_at: DateTime<Utc>,
    pub last_activity_at: DateTime<Utc>,
    /// Mirrored index value ("running" | "completed" | ... | "unknown").
    pub last_run_status: String,
    pub has_pending_question: bool,
    /// Human engagement mark; `None` means no canonical Human UI input was
    /// admitted since creation. Host/tool dispatch never sets it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_human_input: Option<HumanInputMark>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SupervisorDirectoryPage {
    pub caller_session_id: String,
    pub scope_revision: u64,
    pub entries: Vec<SessionDirectoryEntry>,
    pub next_cursor: Option<String>,
}

/// Host-installed capability. The model cannot select or manufacture this
/// port; its presence in the tool schema is the capability boundary.
#[async_trait]
pub trait SupervisorDirectoryPort: Send + Sync {
    async fn directory(
        &self,
        ctx: &ToolCtx,
        request: SupervisorDirectoryRequest,
    ) -> Result<SupervisorDirectoryPage, ToolError>;
}

fn invalid(message: &str) -> ToolError {
    ToolError::InvalidArguments(format!("session_directory_invalid_request: {message}"))
}
