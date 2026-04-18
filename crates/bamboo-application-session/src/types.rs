//! Value types for session use cases.

use std::collections::BTreeSet;

use bamboo_application_runtime::ImageFallbackConfig;
use bamboo_domain_session::Session;
use bamboo_shared_types::reasoning::ReasoningEffort;

/// Resolved configuration snapshot for execution.
///
/// Built from `Config` in the handler layer and passed to use cases.
/// This avoids leaking `bamboo-infrastructure-config` into the crate.
#[derive(Clone, Default)]
pub struct ExecutionConfigSnapshot {
    pub default_model: Option<String>,
    pub default_reasoning_effort: Option<ReasoningEffort>,
    pub disabled_tools: Vec<String>,
    pub disabled_skill_ids: Vec<String>,
    pub provider_name: String,
    pub fast_model: Option<String>,
    pub image_fallback: Option<ImageFallbackConfig>,
}

// ---- Chat types ----

/// Input for the chat turn use case.
pub struct ChatTurnInput {
    pub session_id: String,
    pub model: String,
    pub message: String,
    pub system_prompt: Option<String>,
    pub enhance_prompt: Option<String>,
    pub workspace_path: Option<String>,
    pub selected_skill_ids: Option<Vec<String>>,
    pub copilot_conclusion_with_options_enhancement_enabled: Option<bool>,
    /// Optional data directory for workspace path fallback when neither request
    /// nor metadata provides one.
    pub data_dir: Option<std::path::PathBuf>,
}

/// Outcome of preparing a chat turn.
pub struct PreparedChatTurn {
    pub session: Session,
}

// ---- Execute types ----

/// Input for the execute preparation use case.
pub struct ExecuteInput {
    pub session_id: String,
    pub request_model: Option<String>,
    pub request_reasoning_effort: Option<ReasoningEffort>,
    pub request_skill_mode: Option<String>,
    pub client_sync: Option<ExecuteClientSync>,
}

/// Client-side sync state sent with execute requests.
#[derive(Debug, Clone)]
pub struct ExecuteClientSync {
    pub client_message_count: usize,
    pub client_last_message_id: Option<String>,
    pub client_has_pending_question: bool,
    pub client_pending_question_tool_call_id: Option<String>,
}

/// Reason for a sync mismatch between client and server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecuteSyncReason {
    PendingQuestionMismatch,
    MessageCountMismatch,
    LastMessageIdMismatch,
}

impl ExecuteSyncReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::PendingQuestionMismatch => "pending_question_mismatch",
            Self::MessageCountMismatch => "message_count_mismatch",
            Self::LastMessageIdMismatch => "last_message_id_mismatch",
        }
    }
}

/// Server-side snapshot of session state used for sync comparison.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerExecuteSnapshot {
    pub message_count: usize,
    pub last_message_id: Option<String>,
    pub has_pending_question: bool,
    pub pending_question_tool_call_id: Option<String>,
    pub has_pending_user_message: bool,
}

/// Sync info to include in execute responses.
#[derive(Debug, Clone)]
pub struct ExecuteSyncInfo {
    pub need_sync: bool,
    pub reason: Option<ExecuteSyncReason>,
    pub server_message_count: usize,
    pub server_last_message_id: Option<String>,
    pub has_pending_question: bool,
    pub pending_question_tool_call_id: Option<String>,
    pub has_pending_user_message: bool,
}

impl ServerExecuteSnapshot {
    pub fn to_sync_info(&self, reason: Option<ExecuteSyncReason>) -> ExecuteSyncInfo {
        ExecuteSyncInfo {
            need_sync: reason.is_some(),
            reason,
            server_message_count: self.message_count,
            server_last_message_id: self.last_message_id.clone(),
            has_pending_question: self.has_pending_question,
            pending_question_tool_call_id: self.pending_question_tool_call_id.clone(),
            has_pending_user_message: self.has_pending_user_message,
        }
    }
}

/// Outcome of preparing an execute.
pub enum ExecutePreparationOutcome {
    /// Session is ready for agent execution.
    Ready {
        session: Session,
        effective_model: String,
        effective_reasoning_effort: Option<ReasoningEffort>,
        model_source: &'static str,
        reasoning_source: &'static str,
        is_child_session: bool,
    },
    /// Agent is already running for this session.
    AlreadyRunning {
        server_snapshot: ServerExecuteSnapshot,
    },
    /// No pending user message, nothing to execute.
    NoPendingMessage {
        server_snapshot: ServerExecuteSnapshot,
    },
    /// Client/server state mismatch detected.
    SyncMismatch {
        reason: ExecuteSyncReason,
        server_snapshot: ServerExecuteSnapshot,
    },
    /// No model could be resolved.
    ModelRequired,
    /// Image fallback validation failed.
    ImageFallbackError(String),
}

// ---- Respond types ----

/// Input for the respond use case.
pub struct RespondInput {
    pub session_id: String,
    pub user_response: String,
    pub model: Option<String>,
    pub reasoning_effort: Option<ReasoningEffort>,
}

/// Outcome of submitting a pending response.
pub struct SubmitResponseOutcome {
    pub session: Session,
    pub user_response: String,
}

// ---- Resume types ----

/// Resolved configuration snapshot for resume execution.
///
/// Captures the subset of config needed to spawn a resumed agent loop,
/// decoupled from the full server config.
#[derive(Clone)]
pub struct ResumeConfigSnapshot {
    pub provider_name: String,
    pub fast_model: Option<String>,
    pub disabled_tools: BTreeSet<String>,
    pub disabled_skill_ids: BTreeSet<String>,
    pub image_fallback: Option<ImageFallbackConfig>,
}

/// Outcome of a resume attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResumeOutcome {
    /// Execution spawned successfully.
    Started,
    /// A runner is already active for this session.
    AlreadyRunning,
    /// No pending user message, nothing to execute.
    Completed,
    /// Session not found.
    NotFound,
}

impl ResumeOutcome {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Started => "started",
            Self::AlreadyRunning => "already_running",
            Self::Completed => "completed",
            Self::NotFound => "error: session not found",
        }
    }
}
