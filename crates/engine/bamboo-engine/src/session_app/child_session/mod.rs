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
    assemble_session_tree, build_session_tree_action, cancel_child_action, create_child_action,
    delete_child_action, get_child_action, list_children_action, run_child_action,
    send_message_to_child_action, update_child_action, SessionTreeNode,
};
pub use helpers::{
    compute_status_guidance, format_child_assignment, map_child_entry, metadata_text,
    normalize_non_empty_optional, normalize_required_text, replace_or_append_last_user_message,
    truncate_after_index, truncate_after_last_user,
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

/// Short delegation note appended to the full root-style base prompt for a
/// sub-agent. A sub-agent is a first-class agent (same base prompt + context
/// enhancement as a top-level session); this note only frames the delegation
/// relationship and the expected concise hand-back to the parent.
pub const DELEGATION_NOTE: &str = r#"---

You are running as a delegated sub-agent, spawned by a parent agent to handle a focused task. You have the full capabilities of a top-level agent, including the ability to spawn your own sub-agents when that helps. Stay focused on the assigned task, and when you finish, report a concise conclusion first (the parent receives your final message as the task result)."#;

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
    /// Lifecycle of this child: `Some("resident")` marks a reusable resident
    /// agent (one stable session reused for successive tasks under the same
    /// root); `None`/`Some("oneshot")` is the default throwaway child.
    pub lifecycle: Option<String>,
    /// For a resident agent, the stable reuse key (scoped to the root session).
    pub resident_name: Option<String>,
    /// For a resident agent, how successive tasks treat prior context:
    /// `"reset"` (default — independent tasks) or `"accumulate"` (remember).
    pub resident_context: Option<String>,
    /// Tool names to disable for this child (denylist; matched by EXACT
    /// `ToolSchema.function.name`). `None` (the default) = full toolset. A
    /// read-only Guardian reviewer sets e.g. {"Edit","Write","SubAgent",...}.
    /// Carried to the child's `SpawnJob.disabled_tools` via the child session
    /// metadata (see `create_child_action`) so the worker trims its toolset.
    pub disabled_tools: Option<std::collections::BTreeSet<String>>,
    /// Model-controllable context fork (Phase 3): when `Some(n)` with `n > 0`,
    /// the last `n` non-system parent messages are rendered into a "Forked
    /// context from parent" block prepended to the child's task brief. `None`
    /// (the default) keeps the child on a clean, freshly-seeded context.
    pub context_fork: Option<usize>,
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
    /// Save a child session whose `agent_runtime_state` posture flags
    /// (`bypass_permissions` / `no_human_approver`) the caller just set
    /// authoritatively (the #74 resident-reuse re-seed) — persists them as-is
    /// instead of adopting the child's stale on-disk value, unlike
    /// [`Self::save_child_session`], which protects a concurrent `PATCH` to a
    /// running child. Use ONLY right after deliberately writing those flags. #540.
    async fn save_child_session_authoritative_flags(
        &self,
        child: &mut Session,
    ) -> Result<(), ChildSessionError>;
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

    /// Find an existing resident agent in the same root tree by its stable
    /// `resident_name`, returning its child session id if one exists. Used to
    /// reuse a resident agent for a new task instead of minting a new child.
    /// Index-backed (matches `root_session_id` + `metadata["resident_name"]`).
    async fn find_resident_child(
        &self,
        root_session_id: &str,
        resident_name: &str,
    ) -> Option<String>;

    /// Best-effort: ensure the child's session-index entry is visible
    /// immediately after creation (the index is otherwise eventually
    /// consistent). Failures are ignored by the caller.
    async fn ensure_child_indexed(&self, child_session_id: &str);
}

// ---------------------------------------------------------------------------
// Subagent resolution port
// ---------------------------------------------------------------------------

/// Resolves subagent-type–specific configuration (model, runtime metadata)
/// for the `SubAgent` tool.
///
/// Kept separate from [`ChildSessionPort`] (session CRUD/lifecycle/state): this
/// port is pure `subagent_type` → config resolution (cross-provider model
/// routing + actor/external-agent metadata). The server layer implements it;
/// the tool depends only on the trait, carrying no `AppState` coupling.
#[async_trait]
pub trait SubagentResolutionPort: Send + Sync {
    /// Provider+model ref for a `subagent_type`, or `None` to use defaults.
    async fn resolve_subagent_model(
        &self,
        subagent_type: &str,
    ) -> Option<bamboo_domain::ProviderModelRef>;

    /// Runtime metadata (e.g. external-agent routing) for a `subagent_type`.
    async fn resolve_runtime_metadata(&self, subagent_type: &str) -> HashMap<String, String>;
}

/// Models available from one configured provider (best-effort listing).
#[derive(Debug, Clone, serde::Serialize)]
pub struct ProviderModelList {
    pub provider: String,
    pub models: Vec<String>,
    /// Set when this provider's listing failed (auth missing, network, …);
    /// the provider is still usable with an explicitly known model id.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Lists the models the parent can pin a child session to
/// (`SubAgent` tool `action=list_models` / `create.model`).
///
/// Separate from [`SubagentResolutionPort`] because it is backed by the live
/// provider registry rather than per-`subagent_type` config resolution.
#[async_trait]
pub trait ModelCatalogPort: Send + Sync {
    /// Best-effort model listing per configured provider. Providers whose
    /// listing fails are still returned (with `error` set) so the caller can
    /// see they exist.
    async fn list_models(&self) -> Vec<ProviderModelList>;

    /// The default provider name (used to resolve a bare model id without a
    /// `provider:` prefix).
    fn default_provider(&self) -> String;
}
