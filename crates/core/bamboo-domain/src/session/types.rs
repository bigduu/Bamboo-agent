use crate::provider_model_ref::ProviderModelRef;
use crate::reasoning::ReasoningEffort;
use crate::session::budget_types::{TokenBudget, TokenBudgetUsage};
use crate::session::message_part::{ImageUrlRef, MessagePart};
use crate::session::task::{TaskItemStatus, TaskList};
use crate::session::tool_types::ToolCall;
use crate::tool_types::ToolResultImage;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use uuid::Uuid;

const MAX_TOOL_MESSAGE_BYTES: usize = 256 * 1024;
const TOOL_MESSAGE_HEAD_BYTES: usize = 160 * 1024;
const TOOL_MESSAGE_TAIL_BYTES: usize = 64 * 1024;
const TOOL_MESSAGE_TRUNCATION_MARKER: &str = "[... tool output truncated ...]";

/// Message role in a conversation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    System,
    User,
    Assistant,
    Tool,
}

/// Assistant message phase used by Responses-style models.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MessagePhase {
    Commentary,
    FinalAnswer,
}

impl MessagePhase {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Commentary => "commentary",
            Self::FinalAnswer => "final_answer",
        }
    }
}

/// Message content in a conversation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MessageContent {
    Text { text: String },
    ToolCalls { tool_calls: Vec<ToolCall> },
}

/// A single message in a conversation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    #[serde(default = "generate_id", skip_serializing_if = "String::is_empty")]
    pub id: String,
    pub role: Role,
    pub content: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_parts: Option<Vec<MessagePart>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image_ocr: Option<Vec<ImageOcrResult>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<MessagePhase>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<Vec<ToolCall>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_success: Option<bool>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub compressed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compressed_by_event_id: Option<String>,
    /// When true, this message is protected from context compression and will
    /// never be moved into the summary set.
    #[serde(default, skip_serializing_if = "is_false")]
    pub never_compress: bool,
    /// Progressive compression level: 0=uncompressed, 1=lightly compacted (head/tail),
    /// 2=heavily compacted. Applied at context preparation time, not persisted.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub compression_level: u8,
    #[serde(default = "Utc::now")]
    pub created_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

/// OCR line with bounding box (pixels relative to the image).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ImageOcrLine {
    pub text: String,
    pub left: i32,
    pub top: i32,
    pub width: i32,
    pub height: i32,
}

/// OCR results for a single image part.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ImageOcrResult {
    pub image_url: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub lines: Vec<ImageOcrLine>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

fn generate_id() -> String {
    Uuid::new_v4().to_string()
}

fn is_false(value: &bool) -> bool {
    !*value
}

fn is_zero(value: &u8) -> bool {
    *value == 0
}

impl Message {
    pub fn user(content: impl Into<String>) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            role: Role::User,
            content: content.into(),
            reasoning: None,
            content_parts: None,
            image_ocr: None,
            phase: None,
            tool_calls: None,
            tool_call_id: None,
            tool_success: None,
            compressed: false,
            compressed_by_event_id: None,
            never_compress: false,
            compression_level: 0,
            created_at: Utc::now(),
            metadata: None,
        }
    }

    pub fn user_with_parts(content: impl Into<String>, parts: Vec<MessagePart>) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            role: Role::User,
            content: content.into(),
            reasoning: None,
            content_parts: Some(parts),
            image_ocr: None,
            phase: None,
            tool_calls: None,
            tool_call_id: None,
            tool_success: None,
            compressed: false,
            compressed_by_event_id: None,
            never_compress: false,
            compression_level: 0,
            created_at: Utc::now(),
            metadata: None,
        }
    }

    pub fn assistant(content: impl Into<String>, tool_calls: Option<Vec<ToolCall>>) -> Self {
        Self::assistant_with_reasoning(content, tool_calls, None)
    }

    pub fn assistant_with_reasoning(
        content: impl Into<String>,
        tool_calls: Option<Vec<ToolCall>>,
        reasoning: Option<String>,
    ) -> Self {
        let phase = if tool_calls.as_ref().is_some_and(|calls| !calls.is_empty()) {
            Some(MessagePhase::Commentary)
        } else {
            Some(MessagePhase::FinalAnswer)
        };
        Self {
            id: Uuid::new_v4().to_string(),
            role: Role::Assistant,
            content: content.into(),
            reasoning,
            content_parts: None,
            image_ocr: None,
            phase,
            tool_calls,
            tool_call_id: None,
            tool_success: None,
            compressed: false,
            compressed_by_event_id: None,
            never_compress: false,
            compression_level: 0,
            created_at: Utc::now(),
            metadata: None,
        }
    }

    pub fn tool_result(tool_call_id: impl Into<String>, content: impl Into<String>) -> Self {
        Self::tool_result_with_status(tool_call_id, content, true)
    }

    pub fn tool_result_with_status(
        tool_call_id: impl Into<String>,
        content: impl Into<String>,
        success: bool,
    ) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            role: Role::Tool,
            content: content.into(),
            reasoning: None,
            content_parts: None,
            image_ocr: None,
            phase: None,
            tool_calls: None,
            tool_call_id: Some(tool_call_id.into()),
            tool_success: Some(success),
            compressed: false,
            compressed_by_event_id: None,
            never_compress: false,
            compression_level: 0,
            created_at: Utc::now(),
            metadata: None,
        }
    }

    /// Tool-result message carrying both text and images (e.g. an MCP
    /// `screenshot`). The text goes in `content` (token-capped/persisted as
    /// usual); each image becomes a `MessagePart::ImageUrl` data URL in
    /// `content_parts`, so vision-capable providers receive the real image.
    pub fn tool_result_with_images(
        tool_call_id: impl Into<String>,
        content: impl Into<String>,
        success: bool,
        images: Vec<ToolResultImage>,
    ) -> Self {
        let parts: Vec<MessagePart> = images
            .into_iter()
            .map(|img| MessagePart::ImageUrl {
                image_url: ImageUrlRef {
                    url: format!("data:{};base64,{}", img.mime_type, img.data),
                    detail: None,
                },
            })
            .collect();
        let content_parts = (!parts.is_empty()).then_some(parts);
        Self {
            id: Uuid::new_v4().to_string(),
            role: Role::Tool,
            content: content.into(),
            reasoning: None,
            content_parts,
            image_ocr: None,
            phase: None,
            tool_calls: None,
            tool_call_id: Some(tool_call_id.into()),
            tool_success: Some(success),
            compressed: false,
            compressed_by_event_id: None,
            never_compress: false,
            compression_level: 0,
            created_at: Utc::now(),
            metadata: None,
        }
    }

    pub fn system(content: impl Into<String>) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            role: Role::System,
            content: content.into(),
            reasoning: None,
            content_parts: None,
            image_ocr: None,
            phase: None,
            tool_calls: None,
            tool_call_id: None,
            tool_success: None,
            compressed: false,
            compressed_by_event_id: None,
            never_compress: false,
            compression_level: 0,
            created_at: Utc::now(),
            metadata: None,
        }
    }
}

/// A pending question waiting for user response.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum PendingQuestionSource {
    #[default]
    PauseTool,
    AgenticClarification,
    ExternalAgent,
    Gold,
}

/// A pending question waiting for user response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingQuestion {
    pub tool_call_id: String,
    /// Name of the tool that created this pending question (e.g. "EnterPlanMode", "ExitPlanMode").
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub tool_name: String,
    pub question: String,
    pub options: Vec<String>,
    pub allow_custom: bool,
    #[serde(default)]
    pub source: PendingQuestionSource,
}

/// Summary of conversation context for budget management.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConversationSummary {
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub content: String,
    pub message_count: usize,
    pub token_count: u32,
}

impl ConversationSummary {
    pub fn new(content: impl Into<String>, message_count: usize, token_count: u32) -> Self {
        let now = Utc::now();
        Self {
            created_at: now,
            updated_at: now,
            content: content.into(),
            message_count,
            token_count,
        }
    }

    pub fn update(&mut self, content: impl Into<String>, message_count: usize, token_count: u32) {
        self.content = content.into();
        self.message_count = message_count;
        self.token_count = token_count;
        self.updated_at = Utc::now();
    }
}

/// Trigger type for a compression event.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum CompressionTriggerType {
    #[default]
    Auto,
    Manual,
    CriticalOverflow,
}

/// Persistent context-compression event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionEvent {
    pub id: String,
    pub created_at: DateTime<Utc>,
    pub messages_compressed: usize,
    pub segments_removed: usize,
    #[serde(default)]
    pub usage_before_percent: f64,
    #[serde(default)]
    pub usage_after_percent: f64,
    #[serde(default)]
    pub summary_tokens: u32,
    #[serde(default)]
    pub trigger_type: CompressionTriggerType,
    #[serde(default)]
    pub compression_ratio: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_used: Option<String>,
    #[serde(default)]
    pub latency_ms: u64,
}

impl CompressionEvent {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        messages_compressed: usize,
        segments_removed: usize,
        usage_before_percent: f64,
        usage_after_percent: f64,
        summary_tokens: u32,
        trigger_type: CompressionTriggerType,
        compression_ratio: f64,
        model_used: Option<String>,
        latency_ms: u64,
    ) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            created_at: Utc::now(),
            messages_compressed,
            segments_removed,
            usage_before_percent,
            usage_after_percent,
            summary_tokens,
            trigger_type,
            compression_ratio,
            model_used,
            latency_ms,
        }
    }
}

/// Prompt-memory observability summary captured during external-memory injection.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct PromptMemoryObservability {
    pub project_prompt_injection_enabled: bool,
    pub relevant_recall_enabled: bool,
    #[serde(default)]
    pub relevant_recall_rerank_enabled: bool,
    pub project_first_dream_enabled: bool,
    pub latest_user_query_present: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_project_key: Option<String>,
    pub session_notes_status: String,
    pub project_memory_index_status: String,
    pub relevant_memory_status: String,
    pub project_dream_status: String,
    pub global_dream_fallback_status: String,
    pub dream_source: String,
    #[serde(default)]
    pub session_topic_count: usize,
    #[serde(default)]
    pub truncated_session_topic_count: usize,
    #[serde(default)]
    pub relevant_memory_count: usize,
    #[serde(default)]
    pub session_note_section_chars: usize,
    #[serde(default)]
    pub project_memory_index_section_chars: usize,
    #[serde(default)]
    pub relevant_memory_section_chars: usize,
    #[serde(default)]
    pub project_dream_section_chars: usize,
    #[serde(default)]
    pub global_dream_fallback_section_chars: usize,
    #[serde(default)]
    pub context_pressure_warning_chars: usize,
    #[serde(default)]
    pub external_memory_section_chars: usize,
}

/// Structured snapshot of the effective system prompt and its major sections.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PromptSnapshot {
    pub base_system_prompt: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enhancement_prompt: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace_context: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instruction_context: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub env_context: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skill_context: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_guide_context: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dream_notebook: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_memory_note: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_memory_index: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub relevant_durable_memories: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_dream: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub global_dream_fallback: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_memory_observability: Option<PromptMemoryObservability>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub external_memory: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_list: Option<String>,
    pub effective_system_prompt: String,
}

/// A complete conversation session with state management.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Session {
    pub id: String,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub pinned: bool,
    #[serde(default)]
    pub title_version: u64,
    /// Authoritative UI metadata revision. Bumped by every authoritative
    /// metadata write (title / pinned / future replayable metadata fields).
    /// Runtime / non-authoritative paths must not bump this; they read it
    /// to detect when their session struct holds stale UI metadata.
    #[serde(default)]
    pub metadata_version: u64,
    #[serde(default)]
    pub kind: SessionKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_session_id: Option<String>,
    #[serde(default)]
    pub root_session_id: String,
    #[serde(default)]
    pub spawn_depth: u32,
    pub messages: Vec<Message>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(
        default,
        rename = "task_list",
        alias = "todo_list",
        skip_serializing_if = "Option::is_none"
    )]
    pub task_list: Option<TaskList>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pending_question: Option<PendingQuestion>,
    #[serde(default)]
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_ref: Option<ProviderModelRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<ReasoningEffort>,
    #[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]
    pub metadata: std::collections::HashMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token_budget: Option<TokenBudget>,
    /// Per-process cache of the model-limit-derived budget, keyed by the model it
    /// was resolved for. Never persisted (`#[serde(skip)]`), so a reloaded session
    /// re-resolves from the current `model_limits.json`, and a mid-session model
    /// switch invalidates it (the key no longer matches). `token_budget` above
    /// stays the persisted genuine/child override that takes priority. (#180)
    #[serde(skip)]
    pub resolved_token_budget: Option<(String, TokenBudget)>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token_usage: Option<TokenBudgetUsage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conversation_summary: Option<ConversationSummary>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_snapshot: Option<PromptSnapshot>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub compression_events: Vec<CompressionEvent>,
    /// Custom instructions for conversation summarization at the session level.
    /// Overrides config-level `compression_instructions` when set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compression_instructions: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_runtime_state: Option<crate::session::runtime_state::AgentRuntimeState>,
    /// Typed view over the well-known runtime metadata keys previously smuggled
    /// through `metadata`. Accessed via the symmetric accessor layer
    /// (`runtime_metadata_access`) which dual-writes the legacy `metadata`
    /// strings and falls back to them on read. Additive/optional: old persisted
    /// sessions without this field load cleanly.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_metadata: Option<crate::session::runtime_metadata::SessionRuntimeMetadata>,
    /// Runtime-only flag: when set, the next mid-turn compression check should
    /// force compression regardless of threshold. Set by `compact_context` tool.
    #[serde(skip)]
    pub force_manual_compression: Option<String>,
    /// Workspace directory for file operations in this session.
    /// For child sessions, this is set from the `workspace` field in `CreateChildInput`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<String>,
}

/// Session type marker for spawn-session support.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SessionKind {
    #[default]
    Root,
    Child,
}

impl Session {
    /// Load-boundary migration: a persisted `Root` session's `token_budget` can
    /// only be a stale pre-#180 resolved-budget cache — genuine budgets flow
    /// through `config.token_budget`, and only a `Child` persists an assigned
    /// sub-budget. Clear it so `resolve_token_budget` re-resolves from the
    /// current `model_limits.json` instead of short-circuiting on the stale
    /// value forever.
    ///
    /// MUST be called only on freshly disk-loaded sessions — never inside
    /// `resolve_token_budget`, which can't tell a stale disk cache from a
    /// legitimate in-memory budget injection (tests, child sub-budgets). (#230)
    pub fn clear_stale_root_token_budget(&mut self) {
        if self.kind == SessionKind::Root {
            self.token_budget = None;
        }
    }

    /// The effective token budget: a genuine/child override (`token_budget`) if
    /// set, otherwise the per-process resolved-budget cache
    /// (`resolved_token_budget`). Both are `None` until the first resolution.
    /// Downstream readers should use this rather than `token_budget` directly so
    /// they observe the engine-resolved budget without persisting it. Note the
    /// cache is returned regardless of which model it was resolved for; that is
    /// safe because `resolve_token_budget` runs at round start (re-keying to the
    /// current model) before any reader — don't call this expecting model-freshness
    /// without a preceding same-round resolve. (#180)
    pub fn effective_token_budget(&self) -> Option<&TokenBudget> {
        self.token_budget.as_ref().or_else(|| {
            self.resolved_token_budget
                .as_ref()
                .map(|(_, budget)| budget)
        })
    }

    pub fn new(id: impl Into<String>, model: impl Into<String>) -> Self {
        let now = Utc::now();
        let id = id.into();
        Self {
            id: id.clone(),
            title: "New Session".to_string(),
            pinned: false,
            title_version: 0,
            metadata_version: 0,
            kind: SessionKind::Root,
            parent_session_id: None,
            root_session_id: id,
            spawn_depth: 0,
            messages: Vec::new(),
            created_at: now,
            updated_at: now,
            task_list: None,
            pending_question: None,
            model: model.into(),
            model_ref: None,
            reasoning_effort: None,
            metadata: std::collections::HashMap::new(),
            token_budget: None,
            resolved_token_budget: None,
            token_usage: None,
            conversation_summary: None,
            prompt_snapshot: None,
            compression_events: Vec::new(),
            compression_instructions: None,
            agent_runtime_state: None,
            runtime_metadata: None,
            force_manual_compression: None,
            workspace: None,
        }
    }

    /// Create a child session directly under `root_session_id` — a flat,
    /// depth-1 child of the root. Use [`Session::new_child_of`] when the parent
    /// may itself be a child (nested sub-agents).
    pub fn new_child(
        id: impl Into<String>,
        root_session_id: impl Into<String>,
        model: impl Into<String>,
        title: impl Into<String>,
    ) -> Self {
        let root_session_id = root_session_id.into();
        Self::new_child_inner(
            id,
            root_session_id.clone(),
            root_session_id,
            1,
            model,
            title,
        )
    }

    /// Create a child session whose parent is `parent`, supporting arbitrary
    /// nesting depth. The child inherits the parent's tree root and sits one
    /// level deeper, so completion/SSE bookkeeping (which keys on
    /// `root_session_id`) sees the whole tree regardless of depth.
    pub fn new_child_of(
        id: impl Into<String>,
        parent: &Session,
        model: impl Into<String>,
        title: impl Into<String>,
    ) -> Self {
        Self::new_child_inner(
            id,
            parent.id.clone(),
            parent.root_session_id.clone(),
            parent.spawn_depth.saturating_add(1),
            model,
            title,
        )
    }

    fn new_child_inner(
        id: impl Into<String>,
        parent_session_id: String,
        root_session_id: String,
        spawn_depth: u32,
        model: impl Into<String>,
        title: impl Into<String>,
    ) -> Self {
        let now = Utc::now();
        let id = id.into();
        Self {
            id: id.clone(),
            title: title.into(),
            pinned: false,
            title_version: 0,
            metadata_version: 0,
            kind: SessionKind::Child,
            parent_session_id: Some(parent_session_id),
            root_session_id,
            spawn_depth,
            messages: Vec::new(),
            created_at: now,
            updated_at: now,
            task_list: None,
            pending_question: None,
            model: model.into(),
            model_ref: None,
            reasoning_effort: None,
            metadata: std::collections::HashMap::new(),
            token_budget: None,
            resolved_token_budget: None,
            token_usage: None,
            conversation_summary: None,
            prompt_snapshot: None,
            compression_events: Vec::new(),
            compression_instructions: None,
            agent_runtime_state: None,
            runtime_metadata: None,
            force_manual_compression: None,
            workspace: None,
        }
    }

    pub fn add_message(&mut self, mut message: Message) {
        if matches!(message.role, Role::Tool) {
            if let Some(truncated) = truncate_tool_message_content(&message.content) {
                message.content = truncated;
            }
        }
        self.messages.push(message);
        self.updated_at = Utc::now();
    }

    pub fn compact_oversized_tool_messages(&mut self) -> usize {
        let mut compacted = 0usize;
        for message in &mut self.messages {
            if !matches!(message.role, Role::Tool) {
                continue;
            }
            if let Some(truncated) = truncate_tool_message_content(&message.content) {
                message.content = truncated;
                compacted += 1;
            }
        }
        if compacted > 0 {
            self.updated_at = Utc::now();
        }
        compacted
    }

    /// Clear all ephemeral/derived state that should not persist across turns.
    ///
    /// Resets token usage, compression state, previous response metadata,
    /// and message compression flags. Typically called before a truncation
    /// or when refreshing session state for a new execution.
    pub fn clear_derived_context_state(&mut self) {
        self.token_usage = None;
        self.conversation_summary = None;
        self.compression_events.clear();
        self.metadata.remove("responses.previous_response_id");
        for message in &mut self.messages {
            message.compressed = false;
            message.compressed_by_event_id = None;
        }
    }

    pub fn set_task_list(&mut self, task_list: TaskList) {
        self.task_list = Some(task_list);
        self.updated_at = Utc::now();
    }

    pub fn update_task_item(
        &mut self,
        item_id: &str,
        status: TaskItemStatus,
        notes: Option<&str>,
        criteria_met: Option<&[String]>,
    ) -> Result<String, String> {
        fn normalize_criterion(value: &str) -> Option<String> {
            let normalized = value
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
                .trim()
                .to_lowercase();
            if normalized.is_empty() {
                None
            } else {
                Some(normalized)
            }
        }

        fn parse_criterion_ref(value: &str) -> Option<usize> {
            let trimmed = value.trim().to_ascii_lowercase();
            let as_c_ref = trimmed
                .strip_prefix("criterion_")
                .or_else(|| trimmed.strip_prefix("criterion-"))
                .or_else(|| trimmed.strip_prefix('c'));
            if let Some(raw_index) = as_c_ref {
                return raw_index.parse::<usize>().ok().filter(|index| *index > 0);
            }
            None
        }

        fn missing_completion_criteria(
            required: &[String],
            criteria_met: &[String],
        ) -> Vec<String> {
            let mut required_lookup: std::collections::HashMap<String, usize> =
                std::collections::HashMap::new();
            for (index, criterion) in required.iter().enumerate() {
                if let Some(normalized) = normalize_criterion(criterion) {
                    required_lookup.insert(normalized, index + 1);
                }
            }

            let mut met_refs: HashSet<usize> = HashSet::new();
            for criterion in criteria_met {
                if let Some(index) = parse_criterion_ref(criterion) {
                    met_refs.insert(index);
                    continue;
                }
                if let Some(normalized) = normalize_criterion(criterion) {
                    if let Some(index) = required_lookup.get(&normalized).copied() {
                        met_refs.insert(index);
                    }
                }
            }

            required
                .iter()
                .enumerate()
                .filter_map(|(index, criterion)| {
                    if met_refs.contains(&(index + 1)) {
                        return None;
                    }
                    Some(criterion.trim().to_string())
                })
                .collect()
        }

        if let Some(ref mut task_list) = self.task_list {
            if let Some(item) = task_list.items.iter_mut().find(|i| i.id == item_id) {
                let mut desired_status = status;
                let mut effective_notes = notes.map(str::to_string);
                if matches!(desired_status, TaskItemStatus::Completed)
                    && !matches!(item.status, TaskItemStatus::Completed)
                    && !item.completion_criteria.is_empty()
                {
                    let provided_criteria = criteria_met.unwrap_or(&[]);
                    let missing =
                        missing_completion_criteria(&item.completion_criteria, provided_criteria);
                    if !missing.is_empty() {
                        desired_status = TaskItemStatus::InProgress;
                        let gate_note = format!(
                            "Completion criteria not fully met; keeping task in_progress. Missing: {}",
                            missing.join(" | ")
                        );
                        effective_notes = match effective_notes {
                            Some(mut note) if !note.trim().is_empty() => {
                                note.push('\n');
                                note.push_str(&gate_note);
                                Some(note)
                            }
                            _ => Some(gate_note),
                        };
                    }
                }

                let transitioned =
                    item.transition_to(desired_status, effective_notes.as_deref(), None);
                task_list.updated_at = Utc::now();
                self.updated_at = Utc::now();
                if transitioned {
                    Ok(format!("Updated item '{}' to {:?}", item_id, item.status))
                } else {
                    Ok(format!("Task item '{}' remains {:?}", item_id, item.status))
                }
            } else {
                Err(format!("Task item '{}' not found", item_id))
            }
        } else {
            Err("No task list exists for this session".to_string())
        }
    }

    pub fn format_task_list_for_prompt(&self) -> String {
        self.task_list
            .as_ref()
            .map_or_else(String::new, |list| list.format_for_prompt())
    }

    pub fn set_pending_question(
        &mut self,
        tool_call_id: String,
        tool_name: String,
        question: String,
        options: Vec<String>,
        allow_custom: bool,
    ) {
        self.set_pending_question_with_source(
            tool_call_id,
            tool_name,
            question,
            options,
            allow_custom,
            PendingQuestionSource::PauseTool,
        );
    }

    pub fn set_pending_question_with_source(
        &mut self,
        tool_call_id: String,
        tool_name: String,
        question: String,
        options: Vec<String>,
        allow_custom: bool,
        source: PendingQuestionSource,
    ) {
        self.pending_question = Some(PendingQuestion {
            tool_call_id,
            tool_name,
            question,
            options,
            allow_custom,
            source,
        });
        self.updated_at = Utc::now();
    }

    pub fn clear_pending_question(&mut self) {
        self.pending_question = None;
        self.updated_at = Utc::now();
    }

    pub fn has_pending_question(&self) -> bool {
        self.pending_question.is_some()
    }
}

fn utf8_prefix_by_bytes(text: &str, max_bytes: usize) -> &str {
    if text.len() <= max_bytes {
        return text;
    }
    let mut end = max_bytes;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

fn utf8_suffix_by_bytes(text: &str, max_bytes: usize) -> &str {
    if text.len() <= max_bytes {
        return text;
    }
    let mut start = text.len().saturating_sub(max_bytes);
    while start < text.len() && !text.is_char_boundary(start) {
        start += 1;
    }
    &text[start..]
}

fn truncate_tool_message_content(content: &str) -> Option<String> {
    if content.len() <= MAX_TOOL_MESSAGE_BYTES {
        return None;
    }

    let head = utf8_prefix_by_bytes(content, TOOL_MESSAGE_HEAD_BYTES);
    let tail = utf8_suffix_by_bytes(content, TOOL_MESSAGE_TAIL_BYTES);
    let omitted_bytes = content
        .len()
        .saturating_sub(head.len())
        .saturating_sub(tail.len());

    let marker = format!(
        "\n\n{} original={} bytes omitted={} bytes kept={} bytes\n\n",
        TOOL_MESSAGE_TRUNCATION_MARKER,
        content.len(),
        omitted_bytes,
        head.len().saturating_add(tail.len())
    );

    let mut compacted = String::with_capacity(head.len() + marker.len() + tail.len());
    compacted.push_str(head);
    compacted.push_str(&marker);
    compacted.push_str(tail);
    Some(compacted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn message_without_metadata_serializes_without_metadata_key() {
        let msg = Message::user("hello");
        let serialized = serde_json::to_string(&msg).unwrap();
        assert!(
            !serialized.contains("\"metadata\""),
            "metadata key should be absent when None"
        );
    }

    #[test]
    fn tool_result_with_images_puts_images_in_content_parts() {
        let msg = Message::tool_result_with_images(
            "call-1",
            "note text",
            true,
            vec![ToolResultImage {
                mime_type: "image/jpeg".to_string(),
                data: "abc".to_string(),
            }],
        );
        assert_eq!(msg.role, Role::Tool);
        assert_eq!(msg.content, "note text");
        assert_eq!(msg.tool_call_id.as_deref(), Some("call-1"));
        assert_eq!(msg.tool_success, Some(true));
        let parts = msg.content_parts.expect("content_parts should be set");
        assert_eq!(parts.len(), 1);
        match &parts[0] {
            MessagePart::ImageUrl { image_url } => {
                assert_eq!(image_url.url, "data:image/jpeg;base64,abc");
            }
            _ => panic!("expected an ImageUrl part"),
        }
    }

    #[test]
    fn tool_result_with_no_images_has_no_content_parts() {
        let msg = Message::tool_result_with_images("call-2", "text", true, vec![]);
        assert!(msg.content_parts.is_none());
    }

    #[test]
    fn message_with_metadata_serializes_and_deserializes() {
        let mut msg = Message::tool_result("call-1", "result");
        let meta = json!({
            "elapsed_ms": 150u64,
            "is_mutating": false,
            "auto_approved": true,
            "tool_name": "Read",
            "success": true,
        });
        msg.metadata = Some(meta.clone());

        let serialized = serde_json::to_string(&msg).unwrap();
        assert!(
            serialized.contains("\"metadata\""),
            "serialized JSON should contain the metadata key"
        );
        assert!(
            serialized.contains("\"elapsed_ms\":150"),
            "serialized JSON should contain elapsed_ms value"
        );

        let deserialized: Message = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized.metadata, Some(meta));
        assert_eq!(deserialized.content, "result");
        assert_eq!(deserialized.tool_call_id, Some("call-1".to_string()));
    }

    #[test]
    fn old_json_without_metadata_field_deserializes_as_none() {
        let json = r#"{
            "id": "msg-1",
            "role": "tool",
            "content": "ok",
            "tool_call_id": "call-1",
            "created_at": "2025-01-01T00:00:00Z"
        }"#;
        let msg: Message = serde_json::from_str(json).unwrap();
        assert!(
            msg.metadata.is_none(),
            "metadata should default to None for old JSON without the field"
        );
    }

    #[test]
    fn metadata_with_null_value_deserializes_as_none() {
        let json = r#"{
            "id": "msg-2",
            "role": "tool",
            "content": "ok",
            "tool_call_id": "call-2",
            "metadata": null,
            "created_at": "2025-01-01T00:00:00Z"
        }"#;
        let msg: Message = serde_json::from_str(json).unwrap();
        assert!(
            msg.metadata.is_none(),
            "metadata should be None when JSON value is null"
        );
    }

    #[test]
    fn all_constructors_have_metadata_none() {
        let user_msg = Message::user("hi");
        assert!(user_msg.metadata.is_none());

        let system_msg = Message::system("sys");
        assert!(system_msg.metadata.is_none());

        let assistant_msg = Message::assistant("resp", None);
        assert!(assistant_msg.metadata.is_none());

        let tool_result_msg = Message::tool_result("call-1", "result");
        assert!(tool_result_msg.metadata.is_none());

        let tool_result_status_msg = Message::tool_result_with_status("call-2", "result", true);
        assert!(tool_result_status_msg.metadata.is_none());
    }

    #[test]
    fn message_clone_preserves_metadata() {
        let mut msg = Message::tool_result("call-1", "data");
        msg.metadata = Some(json!({"elapsed_ms": 42}));

        let cloned = msg.clone();
        assert_eq!(cloned.metadata, msg.metadata);
    }

    #[test]
    fn session_add_message_preserves_metadata() {
        let mut session = Session::new("test-session", "test-model");
        let mut msg = Message::tool_result("call-1", "short result");
        msg.metadata = Some(json!({
            "elapsed_ms": 100,
            "is_mutating": false,
        }));

        session.add_message(msg);

        let stored = session.messages.last().unwrap();
        assert!(stored.metadata.is_some());
        let meta = stored.metadata.as_ref().unwrap();
        assert_eq!(meta["elapsed_ms"], 100);
        assert_eq!(meta["is_mutating"], false);
    }

    #[test]
    fn session_serializes_and_deserializes_prompt_snapshot() {
        let mut session = Session::new("session-with-snapshot", "gpt-test");
        session.prompt_snapshot = Some(PromptSnapshot {
            base_system_prompt: "Base prompt".to_string(),
            enhancement_prompt: Some("Extra guidance".to_string()),
            workspace_context: Some("Workspace path: /tmp/ws".to_string()),
            instruction_context: Some("Instruction block".to_string()),
            env_context: Some("Env block".to_string()),
            skill_context: Some("Skill block".to_string()),
            tool_guide_context: Some("Tool block".to_string()),
            dream_notebook: Some("Dream block".to_string()),
            session_memory_note: Some("Session note block".to_string()),
            project_memory_index: Some("Project index block".to_string()),
            relevant_durable_memories: Some("Relevant memories block".to_string()),
            project_dream: Some("Project dream block".to_string()),
            global_dream_fallback: Some("Global fallback block".to_string()),
            prompt_memory_observability: Some(PromptMemoryObservability {
                project_prompt_injection_enabled: true,
                relevant_recall_enabled: true,
                relevant_recall_rerank_enabled: false,
                project_first_dream_enabled: true,
                latest_user_query_present: true,
                resolved_project_key: Some("project-key".to_string()),
                session_notes_status: "loaded".to_string(),
                project_memory_index_status: "loaded".to_string(),
                relevant_memory_status: "lexical".to_string(),
                project_dream_status: "loaded".to_string(),
                global_dream_fallback_status: "skipped_project_memory_or_dream_present".to_string(),
                dream_source: "project".to_string(),
                session_topic_count: 1,
                truncated_session_topic_count: 0,
                relevant_memory_count: 2,
                session_note_section_chars: 42,
                project_memory_index_section_chars: 84,
                relevant_memory_section_chars: 126,
                project_dream_section_chars: 64,
                global_dream_fallback_section_chars: 0,
                context_pressure_warning_chars: 0,
                external_memory_section_chars: 320,
            }),
            external_memory: Some("Memory block".to_string()),
            task_list: Some("Task block".to_string()),
            effective_system_prompt: "Effective prompt".to_string(),
        });

        let json = serde_json::to_string(&session).expect("session should serialize");
        let roundtrip: Session = serde_json::from_str(&json).expect("session should deserialize");
        assert_eq!(
            roundtrip
                .prompt_snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.enhancement_prompt.as_deref()),
            Some("Extra guidance")
        );
        assert_eq!(
            roundtrip
                .prompt_snapshot
                .as_ref()
                .map(|snapshot| snapshot.effective_system_prompt.as_str()),
            Some("Effective prompt")
        );
    }

    #[test]
    fn clear_derived_context_state_resets_all_ephemeral_fields() {
        let mut session = Session::new("test-clear-derived", "gpt-5");
        session.token_usage = Some(TokenBudgetUsage {
            system_tokens: 100,
            summary_tokens: 50,
            window_tokens: 200,
            total_tokens: 350,
            max_context_tokens: 1000,
            budget_limit: 800,
            truncation_occurred: false,
            segments_removed: 0,
            prompt_cached_tool_outputs: 0,
            prompt_cached_tool_tokens_saved: 0,
            thinking_tokens: 0,
            cache_read_input_tokens: 0,
        });
        session.conversation_summary = Some(ConversationSummary {
            created_at: Utc::now(),
            updated_at: Utc::now(),
            content: "summary".to_string(),
            message_count: 5,
            token_count: 100,
        });
        session.compression_events = vec![CompressionEvent::new(
            1,
            2,
            50.0,
            25.0,
            10,
            CompressionTriggerType::Auto,
            2.0,
            None,
            0,
        )];
        session.metadata.insert(
            "responses.previous_response_id".to_string(),
            "resp-123".to_string(),
        );
        session.add_message(Message::system("hello"));
        session.messages[0].compressed = true;
        session.messages[0].compressed_by_event_id = Some("evt-1".to_string());

        session.clear_derived_context_state();

        assert!(session.token_usage.is_none());
        assert!(session.conversation_summary.is_none());
        assert!(session.compression_events.is_empty());
        assert!(!session
            .metadata
            .contains_key("responses.previous_response_id"));
        assert!(!session.messages[0].compressed);
        assert!(session.messages[0].compressed_by_event_id.is_none());
    }

    #[test]
    fn never_compress_field_deserializes_as_false_by_default() {
        let json = r#"{
            "id": "msg-nc",
            "role": "user",
            "content": "test",
            "created_at": "2025-01-01T00:00:00Z"
        }"#;
        let msg: Message = serde_json::from_str(json).unwrap();
        assert!(
            !msg.never_compress,
            "never_compress should default to false"
        );
    }

    #[test]
    fn never_compress_true_preserved_through_roundtrip() {
        let mut msg = Message::user("important");
        msg.never_compress = true;
        let json = serde_json::to_string(&msg).unwrap();
        let back: Message = serde_json::from_str(&json).unwrap();
        assert!(back.never_compress);
    }

    #[test]
    fn never_compress_false_omitted_from_serialization() {
        let msg = Message::user("normal");
        let json = serde_json::to_string(&msg).unwrap();
        assert!(
            !json.contains("never_compress"),
            "false should be omitted: {json}"
        );
    }

    #[test]
    fn compression_level_zero_omitted_from_serialization() {
        let msg = Message::user("normal");
        let json = serde_json::to_string(&msg).unwrap();
        assert!(
            !json.contains("compression_level"),
            "zero should be omitted: {json}"
        );
    }

    #[test]
    fn compression_level_preserved_through_roundtrip() {
        let mut msg = Message::assistant("analysis", None);
        msg.compression_level = 1;
        let json = serde_json::to_string(&msg).unwrap();
        let back: Message = serde_json::from_str(&json).unwrap();
        assert_eq!(back.compression_level, 1);
    }

    #[test]
    fn compression_trigger_type_serde_roundtrip() {
        for variant in [
            CompressionTriggerType::Auto,
            CompressionTriggerType::Manual,
            CompressionTriggerType::CriticalOverflow,
        ] {
            let json = serde_json::to_string(&variant).unwrap();
            let back: CompressionTriggerType = serde_json::from_str(&json).unwrap();
            assert_eq!(back, variant, "roundtrip failed for {variant:?}");
        }
    }

    #[test]
    fn compression_trigger_type_snake_case_serialization() {
        assert_eq!(
            serde_json::to_string(&CompressionTriggerType::CriticalOverflow).unwrap(),
            "\"critical_overflow\""
        );
        assert_eq!(
            serde_json::to_string(&CompressionTriggerType::Manual).unwrap(),
            "\"manual\""
        );
    }

    #[test]
    fn compression_trigger_type_default_is_auto() {
        assert_eq!(
            CompressionTriggerType::default(),
            CompressionTriggerType::Auto
        );
    }

    #[test]
    fn compression_event_extended_fields_roundtrip() {
        let event = CompressionEvent::new(
            42,   // messages_compressed
            10,   // segments_removed
            92.5, // usage_before_percent
            35.2, // usage_after_percent
            500,  // summary_tokens
            CompressionTriggerType::Manual,
            2.63, // compression_ratio
            Some("gpt-5.4-mini".to_string()),
            1500, // latency_ms
        );

        let json = serde_json::to_string(&event).unwrap();
        let back: CompressionEvent = serde_json::from_str(&json).unwrap();

        assert_eq!(back.messages_compressed, 42);
        assert_eq!(back.segments_removed, 10);
        assert!((back.usage_before_percent - 92.5).abs() < 0.01);
        assert!((back.usage_after_percent - 35.2).abs() < 0.01);
        assert_eq!(back.summary_tokens, 500);
        assert_eq!(back.trigger_type, CompressionTriggerType::Manual);
        assert!((back.compression_ratio - 2.63).abs() < 0.01);
        assert_eq!(back.model_used.as_deref(), Some("gpt-5.4-mini"));
        assert_eq!(back.latency_ms, 1500);
    }

    #[test]
    fn compression_event_backward_compat_deserializes_old_format() {
        // Old format without the new extended fields
        let json = r#"{
            "id": "evt-old",
            "created_at": "2025-01-01T00:00:00Z",
            "messages_compressed": 10,
            "segments_removed": 5,
            "usage_before_percent": 80.0,
            "usage_after_percent": 40.0,
            "summary_tokens": 200
        }"#;
        let event: CompressionEvent = serde_json::from_str(json).unwrap();
        assert_eq!(event.trigger_type, CompressionTriggerType::Auto); // default
        assert_eq!(event.compression_ratio, 0.0); // default
        assert!(event.model_used.is_none()); // default
        assert_eq!(event.latency_ms, 0); // default
    }

    #[test]
    fn force_manual_compression_not_serialized() {
        let mut session = Session::new("test-session", "test-model");
        session.force_manual_compression = Some("keep errors".to_string());
        let json = serde_json::to_string(&session).unwrap();
        assert!(
            !json.contains("force_manual_compression"),
            "runtime-only flag should not be serialized: {json}"
        );
    }

    #[test]
    fn compression_instructions_serialized_when_present() {
        let mut session = Session::new("test-session", "test-model");
        session.compression_instructions = Some("Focus on API contracts".to_string());
        let json = serde_json::to_string(&session).unwrap();
        assert!(json.contains("compression_instructions"));
        assert!(json.contains("Focus on API contracts"));
    }

    #[test]
    fn compression_instructions_omitted_when_none() {
        let session = Session::new("test-session", "test-model");
        let json = serde_json::to_string(&session).unwrap();
        assert!(!json.contains("compression_instructions"));
    }

    #[test]
    fn new_child_is_depth_one_under_root() {
        let root = Session::new("root-1", "m");
        let child = Session::new_child_of("child-1", &root, "m", "c");
        assert_eq!(child.kind, SessionKind::Child);
        assert_eq!(child.parent_session_id.as_deref(), Some("root-1"));
        assert_eq!(child.root_session_id, "root-1");
        assert_eq!(child.spawn_depth, 1);
    }

    #[test]
    fn clear_stale_root_token_budget_clears_root_keeps_child() {
        // #230: a Root's persisted token_budget is a stale pre-#180 cache → clear
        // it on load so it re-resolves. A Child's is a genuine assigned sub-budget
        // → keep it.
        let mut root = Session::new("root-1", "m");
        root.token_budget = Some(crate::TokenBudget::for_model(1000));
        root.clear_stale_root_token_budget();
        assert!(
            root.token_budget.is_none(),
            "Root token_budget cleared on load"
        );

        let parent = Session::new("root-1", "m");
        let mut child = Session::new_child_of("child-1", &parent, "m", "c");
        child.token_budget = Some(crate::TokenBudget::for_model(500));
        child.clear_stale_root_token_budget();
        assert!(
            child.token_budget.is_some(),
            "Child assigned sub-budget preserved on load"
        );
    }

    #[test]
    fn new_child_of_supports_nesting_lineage() {
        // root -> child -> grandchild: parent walks one level, root stays
        // constant, depth increments.
        let root = Session::new("root-1", "m");
        let child = Session::new_child_of("child-1", &root, "m", "c");
        let grandchild = Session::new_child_of("gc-1", &child, "m", "g");
        assert_eq!(grandchild.parent_session_id.as_deref(), Some("child-1"));
        assert_eq!(grandchild.root_session_id, "root-1");
        assert_eq!(grandchild.spawn_depth, 2);
    }
}
