//! Child session management use cases.
//!
//! Provides the application-layer logic for managing child sessions within
//! a root session. The server layer implements `ChildSessionPort` to supply
//! the infrastructure operations (load, save, schedule, cancel).

use async_trait::async_trait;
use bamboo_domain::session::runtime_state::ChildWaitPolicy;
use bamboo_domain::Session;
use std::collections::HashMap;

mod actions;
mod helpers;

#[cfg(test)]
mod tests;

pub use actions::{
    cancel_child_action, create_child_action, delete_child_action, get_child_action,
    list_children_action, run_child_action, send_message_to_child_action, update_child_action,
};
pub use helpers::{
    compute_status_guidance, format_child_assignment, map_child_entry, metadata_text,
    normalize_non_empty_optional, normalize_required_text, replace_or_append_last_user_message,
    resolve_system_prompt, truncate_after_index, truncate_after_last_user,
};

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum ChildSessionError {
    #[error("session not found: {0}")]
    NotFound(String),
    #[error("session is not a root session: {0}")]
    NotRootSession(String),
    #[error("session is not a child session: {0}")]
    NotChildSession(String),
    #[error("child session {child_id} does not belong to parent {parent_id}")]
    NotChildOfParent { child_id: String, parent_id: String },
    #[error("{0}")]
    InvalidArguments(String),
    #[error("{0}")]
    Execution(String),
}

// ---------------------------------------------------------------------------
// Value types
// ---------------------------------------------------------------------------

/// Summary of a child session for listing.
#[derive(Debug, Clone)]
pub struct ChildSessionEntry {
    pub child_session_id: String,
    pub title: String,
    pub pinned: bool,
    pub message_count: usize,
    pub updated_at: String,
    pub last_run_status: Option<String>,
    pub last_run_error: Option<String>,
}

/// Result of deleting a child session.
#[derive(Debug, Clone)]
pub struct DeleteChildResult {
    pub deleted: bool,
    pub cancelled_running_child: bool,
}

/// Diagnostic snapshot of a running child session runner.
#[derive(Debug, Clone)]
pub struct ChildRunnerInfo {
    pub started_at: Option<chrono::DateTime<chrono::Utc>>,
    pub completed_at: Option<chrono::DateTime<chrono::Utc>>,
    pub last_tool_name: Option<String>,
    pub last_tool_phase: Option<String>,
    pub last_event_at: Option<chrono::DateTime<chrono::Utc>>,
    pub round_count: u32,
}

/// Default system prompt for child sessions.
pub const CHILD_SYSTEM_PROMPT: &str = r#"You are a **Child Session**, delegated by a parent session.

Requirements:
- Focus only on the assigned task and avoid unrelated conversation.
- You may use tools to complete the task.
- Do not create or trigger any additional child sessions (no recursive spawn).
- Keep output concise: provide the conclusion first, then only necessary evidence or steps.
"#;

/// System prompt for plan-mode exploration child sessions.
pub const PLAN_AGENT_SYSTEM_PROMPT: &str = r#"You are a **Plan Agent**, a read-only exploration specialist delegated by a parent session.

Your role is EXCLUSIVELY to explore the codebase and gather information to help design an implementation plan. You MUST NOT modify anything.

=== CRITICAL: READ-ONLY MODE — NO FILE MODIFICATIONS ===

You are FORBIDDEN from using these tools:
- Write — do not create new files
- Edit — do not modify existing files
- NotebookEdit — do not edit notebooks
- Bash — do not execute shell commands
- BashOutput — do not execute shell commands
- KillShell — do not manage processes
- SubAgent — do not spawn further child sessions

You MAY use these read-only tools:
- Read — read file contents
- Glob — list files matching patterns
- Grep — search code for patterns
- GetFileInfo — get file metadata
- WebFetch — fetch web content
- WebSearch — search the web
- MemoryNote — write observations to session memory

Requirements:
- Focus only on the assigned exploration task.
- Provide clear, structured findings: what you discovered, where the relevant code is, and what it does.
- Keep output concise but thorough — the parent session needs enough detail to design a plan.
- If you cannot find something after reasonable searching, say so clearly.
"#;

/// Input for creating a child session.
#[derive(Debug, Clone)]
pub struct CreateChildInput {
    pub parent_session: Session,
    pub child_id: String,
    pub title: String,
    pub responsibility: String,
    pub assignment_prompt: String,
    pub subagent_type: String,
    /// Absolute path to the working directory for the child session.
    pub workspace: String,
    /// Optional model override resolved from subagent_type routing.
    /// When `None`, the child inherits the parent session's model.
    pub model_override: Option<String>,
    /// Optional provider+model override resolved from subagent routing.
    /// When present, this preserves cross-provider routing for child execution.
    pub model_ref_override: Option<bamboo_domain::ProviderModelRef>,
    /// Runtime metadata resolved from subagent routing (e.g. external agent config).
    pub runtime_metadata: std::collections::HashMap<String, String>,
    /// Optional system prompt override resolved from the
    /// `SubagentProfileRegistry`. When `None`, the child falls back to
    /// the legacy hard-coded prompts (`PLAN_AGENT_SYSTEM_PROMPT` for
    /// `subagent_type == "plan"`, `CHILD_SYSTEM_PROMPT` otherwise) so
    /// that callers that have not yet been wired up keep their pre-PR-3
    /// behaviour byte-for-byte.
    pub system_prompt_override: Option<String>,
    /// Whether to immediately enqueue the child for execution.
    /// Defaults to `true`.
    pub auto_run: bool,
    /// Optional reasoning effort to apply to the child's own LLM calls.
    /// `None` (the default) leaves `Session::reasoning_effort` at `None`,
    /// so the provider falls back to its default. The child does NOT
    /// inherit the parent's reasoning_effort — fan-out children that
    /// only need a quick lookup should not pay for `xhigh` reasoning
    /// just because the orchestrator is running at `xhigh`.
    pub reasoning_effort: Option<bamboo_domain::ReasoningEffort>,
}

/// Result of creating a child session.
#[derive(Debug, Clone)]
pub struct CreateChildResult {
    pub child_session_id: String,
    pub model: String,
}

/// A queued follow-up message stored in session metadata for later injection.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct QueuedInjectedMessage {
    pub content: String,
    #[serde(default)]
    pub created_at: Option<chrono::DateTime<chrono::Utc>>,
}

// ---------------------------------------------------------------------------
// Port trait
// ---------------------------------------------------------------------------

#[async_trait]
pub trait ChildSessionPort: Send + Sync {
    async fn load_root_session(&self, root_id: &str) -> Result<Session, ChildSessionError>;
    async fn load_child_for_parent(
        &self,
        parent_id: &str,
        child_id: &str,
    ) -> Result<Session, ChildSessionError>;
    async fn save_child_session(&self, child: &mut Session) -> Result<(), ChildSessionError>;
    async fn is_child_running(&self, child_id: &str) -> bool;
    async fn list_children(&self, parent_id: &str) -> Vec<ChildSessionEntry>;
    async fn enqueue_child_run(
        &self,
        parent: &Session,
        child: &Session,
    ) -> Result<(), ChildSessionError>;
    async fn cancel_child_run_and_wait(&self, child_id: &str) -> Result<(), ChildSessionError>;
    async fn delete_child_session(
        &self,
        parent_id: &str,
        child_id: &str,
    ) -> Result<DeleteChildResult, ChildSessionError>;
    /// Return live diagnostic info for a running child session, if available.
    async fn get_child_runner_info(&self, child_id: &str) -> Option<ChildRunnerInfo>;

    /// Register a durable parent wait for a single enqueued child. Idempotent
    /// and coalesced per parent (concurrent sibling spawns merge into one write).
    async fn register_parent_wait_for_child(
        &self,
        parent_session_id: &str,
        child_session_id: &str,
        tool_call_id: Option<&str>,
    ) -> Result<(), ChildSessionError>;

    /// Register a durable parent wait over an explicit set of children with a
    /// chosen policy (the `SubAgent.wait` action). Returns the number of
    /// children the wait now covers (0 = nothing to wait on).
    async fn register_parent_wait_for_children(
        &self,
        parent_session_id: &str,
        child_session_ids: &[String],
        policy: ChildWaitPolicy,
    ) -> Result<usize, ChildSessionError>;

    /// The parent's currently-active (non-terminal) child session ids.
    async fn active_child_ids(&self, parent_session_id: &str) -> Vec<String>;

    /// Best-effort: ensure the child's session-index entry is visible
    /// immediately after creation (the index is otherwise eventually
    /// consistent). Failures are ignored by the caller.
    async fn ensure_child_indexed(&self, child_session_id: &str);
}

// ---------------------------------------------------------------------------
// Subagent resolution port
// ---------------------------------------------------------------------------

/// Resolves subagent-type–specific configuration (model, runtime metadata,
/// system prompt) for the `SubAgent` tool.
///
/// Kept separate from [`ChildSessionPort`] (session CRUD/lifecycle/state): this
/// port is pure `subagent_type` → config resolution. The server layer
/// implements it; the tool depends only on the trait, carrying no `AppState`
/// coupling.
#[async_trait]
pub trait SubagentResolutionPort: Send + Sync {
    /// Provider+model ref for a `subagent_type`, or `None` to use defaults.
    async fn resolve_subagent_model(
        &self,
        subagent_type: &str,
    ) -> Option<bamboo_domain::ProviderModelRef>;

    /// Runtime metadata (e.g. external-agent routing) for a `subagent_type`.
    async fn resolve_runtime_metadata(&self, subagent_type: &str) -> HashMap<String, String>;

    /// Canonical system prompt for a `subagent_type` (falls back to
    /// `general-purpose` for unknown/empty values; never empty).
    fn resolve_subagent_prompt(&self, subagent_type: &str) -> String;
}
