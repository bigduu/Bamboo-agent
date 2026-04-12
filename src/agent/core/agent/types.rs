//! Core agent types for sessions, messages, and conversations.
//!
//! This module defines the fundamental types used throughout the agent system
//! for managing conversations, sessions, and message exchanges.
//!
//! # Key Types
//!
//! - [`Role`] - Message role (System, User, Assistant, Tool)
//! - [`Message`] - A single message in a conversation
//! - [`MessageContent`] - Message content (text or tool calls)
//! - [`Session`] - A complete conversation session with state
//! - [`PendingQuestion`] - User question waiting for response
//! - [`ConversationSummary`] - Summary of truncated context
//!
//! # Session Lifecycle
//!
//! 1. Create session with `Session::new(id, model)`
//! 2. Add messages with `session.add_message(Message::user("..."))`
//! 3. Track progress with a task list
//! 4. Persist to storage
//!
//! # Example
//!
//! ```rust,ignore
//! use bamboo_agent::agent::core::agent::types::*;
//!
//! let mut session = Session::new("session-1", "gpt-4o-mini");
//! session.add_message(Message::user("Hello"));
//! session.add_message(Message::assistant("Hi there!", None));
//! ```

use crate::agent::core::tools::ToolCall;
use crate::agent::core::{TaskItemStatus, TaskList};
use crate::core::ReasoningEffort;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use uuid::Uuid;

const MAX_TOOL_MESSAGE_BYTES: usize = 256 * 1024;
const TOOL_MESSAGE_HEAD_BYTES: usize = 160 * 1024;
const TOOL_MESSAGE_TAIL_BYTES: usize = 64 * 1024;
const TOOL_MESSAGE_TRUNCATION_MARKER: &str = "[... tool output truncated ...]";

/// Message role in a conversation.
///
/// Identifies the sender of a message in the conversation history.
///
/// # Variants
///
/// * `System` - System instructions/prompts
/// * `User` - User input
/// * `Assistant` - AI assistant response
/// * `Tool` - Tool execution result
///
/// # Example
///
/// ```rust,ignore
/// let role = Role::User;
/// assert_eq!(role, Role::User);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    /// System instructions or prompts
    System,
    /// User input message
    User,
    /// AI assistant response
    Assistant,
    /// Tool execution result
    Tool,
}

/// Assistant message phase used by Responses-style models.
///
/// Some models distinguish between intermediate "commentary" content while
/// planning/executing tools and the final user-facing answer.
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
///
/// Can be either plain text or a list of tool calls from the assistant.
///
/// # Variants
///
/// * `Text(String)` - Plain text content
/// * `ToolCalls(Vec<ToolCall>)` - Tool call requests
///
/// # Example
///
/// ```rust,ignore
/// let text = MessageContent::Text("Hello".to_string());
/// let tools = MessageContent::ToolCalls(vec![tool_call]);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum MessageContent {
    /// Plain text content
    Text(String),
    /// Tool call requests from assistant
    ToolCalls(Vec<ToolCall>),
}

/// A single message in a conversation.
///
/// Represents one turn in the conversation, including the role,
/// content, optional tool calls, and metadata.
///
/// # Fields
///
/// * `id` - Unique message identifier
/// * `role` - Message sender role
/// * `content` - Message content text
/// * `tool_calls` - Optional tool calls (for Assistant messages)
/// * `tool_call_id` - Optional tool call ID (for Tool messages)
/// * `created_at` - Message timestamp
///
/// # Example
///
/// ```rust,ignore
/// let user_msg = Message::user("What is Rust?");
/// let assistant_msg = Message::assistant("Rust is a systems language", None);
/// let tool_msg = Message::tool_result("call-123", "Tool result");
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    /// Unique message identifier (auto-generated)
    #[serde(default = "generate_id", skip_serializing_if = "String::is_empty")]
    pub id: String,
    /// Message sender role
    pub role: Role,
    /// Message content text
    pub content: String,
    /// Optional model reasoning/thinking trace for this assistant turn.
    ///
    /// This is persisted for UI replay/debugging and only set on assistant
    /// messages when the provider emits reasoning tokens.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<String>,
    /// Optional multimodal content parts (e.g. text + images).
    ///
    /// This keeps image inputs available for preflight hooks (OCR / image fallback)
    /// and for multimodal-capable upstream models. Text-only subsystems can keep
    /// using `content` as a best-effort projection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_parts: Option<Vec<crate::agent::llm::models::ContentPart>>,
    /// Optional OCR results for image parts in this message (persisted).
    ///
    /// This is intentionally kept separate from `content` / `content_parts` so the UI
    /// can choose whether/how to render OCR text without losing the original image.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image_ocr: Option<Vec<ImageOcrResult>>,
    /// Optional assistant response phase (`commentary` / `final_answer`).
    ///
    /// Primarily used for Responses-style providers to preserve turn structure.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<MessagePhase>,
    /// Tool calls (for Assistant messages)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<Vec<ToolCall>>,
    /// Tool call ID (for Tool result messages)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
    /// Tool execution success flag (for Tool result messages).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_success: Option<bool>,
    /// Whether this message is archived/compressed and excluded from LLM requests.
    #[serde(default, skip_serializing_if = "is_false")]
    pub compressed: bool,
    /// Compression event ID that archived this message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compressed_by_event_id: Option<String>,
    /// Message creation timestamp
    #[serde(default = "Utc::now")]
    pub created_at: DateTime<Utc>,
    /// Optional metadata for tool lifecycle tracking and other extensions.
    ///
    /// This is persisted to `session.json` for UI replay/debugging but is
    /// **stripped** when building LLM context to avoid polluting the prompt.
    ///
    /// Typical fields for tool result messages:
    /// - `elapsed_ms`: Wall-clock execution time in milliseconds
    /// - `is_mutating`: Whether the tool writes files / runs commands
    /// - `auto_approved`: Whether execution was auto-approved
    /// - `tool_name`: Canonical tool name
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
    /// The `image_url.url` this OCR result corresponds to.
    pub image_url: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub lines: Vec<ImageOcrLine>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Generate a unique ID for messages.
fn generate_id() -> String {
    Uuid::new_v4().to_string()
}

fn is_false(value: &bool) -> bool {
    !*value
}

impl Message {
    /// Create a user message.
    ///
    /// # Arguments
    ///
    /// * `content` - User message text
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let msg = Message::user("Hello, assistant!");
    /// assert_eq!(msg.role, Role::User);
    /// ```
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
            created_at: Utc::now(),
            metadata: None,
        }
    }

    /// Create a user message with multimodal content parts.
    pub fn user_with_parts(
        content: impl Into<String>,
        parts: Vec<crate::agent::llm::models::ContentPart>,
    ) -> Self {
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
            created_at: Utc::now(),
            metadata: None,
        }
    }

    /// Create an assistant message.
    ///
    /// # Arguments
    ///
    /// * `content` - Assistant response text
    /// * `tool_calls` - Optional tool calls made by assistant
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let msg = Message::assistant("Hello!", None);
    /// let msg_with_tools = Message::assistant("Let me help", Some(vec![tool_call]));
    /// ```
    pub fn assistant(content: impl Into<String>, tool_calls: Option<Vec<ToolCall>>) -> Self {
        Self::assistant_with_reasoning(content, tool_calls, None)
    }

    /// Create an assistant message with optional reasoning trace.
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
            created_at: Utc::now(),
            metadata: None,
        }
    }

    /// Create a tool result message.
    ///
    /// # Arguments
    ///
    /// * `tool_call_id` - ID of the tool call this is responding to
    /// * `content` - Tool execution result
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let result = Message::tool_result("call-123", "File contents here");
    /// assert_eq!(result.role, Role::Tool);
    /// ```
    pub fn tool_result(tool_call_id: impl Into<String>, content: impl Into<String>) -> Self {
        Self::tool_result_with_status(tool_call_id, content, true)
    }

    /// Create a tool result message with an explicit success flag.
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
            created_at: Utc::now(),
            metadata: None,
        }
    }

    /// Create a system message.
    ///
    /// # Arguments
    ///
    /// * `content` - System instructions/prompt
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let msg = Message::system("You are a helpful assistant");
    /// assert_eq!(msg.role, Role::System);
    /// ```
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
            created_at: Utc::now(),
            metadata: None,
        }
    }
}

/// A pending question waiting for user response.
///
/// When the agent calls the `conclusion_with_options` tool, it creates a pending question
/// that blocks execution until the user responds via the API.
///
/// # Fields
///
/// * `tool_call_id` - ID of the tool call that asked the question
/// * `question` - Question text to display to user
/// * `options` - Predefined response options
/// * `allow_custom` - Whether user can enter custom response
///
/// # Example
///
/// ```rust,ignore
/// let pending = PendingQuestion {
///     tool_call_id: "call-123".to_string(),
///     question: "Which language?".to_string(),
///     options: vec!["Rust".to_string(), "Python".to_string()],
///     allow_custom: false,
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingQuestion {
    /// ID of the tool call that created this question
    pub tool_call_id: String,
    /// Question to ask the user
    pub question: String,
    /// Predefined response options
    pub options: Vec<String>,
    /// Whether custom responses are allowed
    pub allow_custom: bool,
}

/// Summary of conversation context for budget management.
///
/// When conversations are truncated due to token limits, a summary
/// can preserve key information from earlier context.
///
/// # Fields
///
/// * `created_at` - When the summary was created
/// * `updated_at` - When the summary was last updated
/// * `content` - Summary text
/// * `message_count` - Number of messages summarized
/// * `token_count` - Token count of the summary
///
/// # Usage
///
/// Summaries are created when the conversation exceeds token budget:
/// 1. Old messages are summarized by the LLM
/// 2. Summary replaces old messages
/// 3. New messages continue the conversation
///
/// # Example
///
/// ```rust,ignore
/// let summary = ConversationSummary::new(
///     "User discussed Rust programming",
///     10,
///     50
/// );
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConversationSummary {
    /// When the summary was created
    pub created_at: DateTime<Utc>,
    /// When the summary was last updated
    pub updated_at: DateTime<Utc>,
    /// The summary text
    pub content: String,
    /// Number of messages summarized
    pub message_count: usize,
    /// Token count of the summary
    pub token_count: u32,
}

impl ConversationSummary {
    /// Create a new conversation summary.
    ///
    /// # Arguments
    ///
    /// * `content` - Summary text
    /// * `message_count` - Number of messages being summarized
    /// * `token_count` - Token count of the summary
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let summary = ConversationSummary::new(
    ///     "Discussion about Rust async programming",
    ///     15,
    ///     75
    /// );
    /// ```
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

    /// Update the summary with new content.
    ///
    /// # Arguments
    ///
    /// * `content` - New summary text
    /// * `message_count` - Updated message count
    /// * `token_count` - Updated token count
    pub fn update(&mut self, content: impl Into<String>, message_count: usize, token_count: u32) {
        self.content = content.into();
        self.message_count = message_count;
        self.token_count = token_count;
        self.updated_at = Utc::now();
    }
}

/// Persistent context-compression event.
///
/// Each event captures one compaction operation so the UI can display
/// a timeline of multiple compression boundaries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionEvent {
    /// Unique compression event identifier.
    pub id: String,
    /// Event timestamp.
    pub created_at: DateTime<Utc>,
    /// Number of messages archived by this event.
    pub messages_compressed: usize,
    /// Number of segments removed in budget preparation for this event.
    pub segments_removed: usize,
    /// Context usage percentage before compression.
    #[serde(default)]
    pub usage_before_percent: f64,
    /// Context usage percentage after compression.
    #[serde(default)]
    pub usage_after_percent: f64,
    /// Number of summary tokens in the compression plan.
    #[serde(default)]
    pub summary_tokens: u32,
}

impl CompressionEvent {
    pub fn new(
        messages_compressed: usize,
        segments_removed: usize,
        usage_before_percent: f64,
        usage_after_percent: f64,
        summary_tokens: u32,
    ) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            created_at: Utc::now(),
            messages_compressed,
            segments_removed,
            usage_before_percent,
            usage_after_percent,
            summary_tokens,
        }
    }
}

/// Structured snapshot of parsed external-memory subsections used for prompt observability.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct PromptSnapshotExternalMemoryParts {
    pub dream_notebook: Option<String>,
    pub session_memory_note: Option<String>,
    pub project_memory_index: Option<String>,
    pub relevant_durable_memories: Option<String>,
    pub project_dream: Option<String>,
    pub global_dream_fallback: Option<String>,
}

pub(crate) fn parse_prompt_external_memory_sections(
    external_memory: Option<&str>,
) -> PromptSnapshotExternalMemoryParts {
    let Some(external_memory) = external_memory
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return PromptSnapshotExternalMemoryParts::default();
    };

    let legacy_dream_notebook = extract_prompt_markdown_block_by_heading(
        external_memory,
        "### Cross-session Dream Notebook (read-only)",
    );
    let project_memory_index = extract_prompt_markdown_block_by_heading(
        external_memory,
        "### Project Durable Memory Index",
    );
    let relevant_durable_memories =
        extract_prompt_plain_section_by_heading(external_memory, "### Relevant Durable Memories");
    let project_dream =
        extract_prompt_markdown_block_by_heading(external_memory, "### Project Dream Summary");
    let global_dream_fallback = extract_prompt_markdown_block_by_heading(
        external_memory,
        "### Global Dream Summary (fallback)",
    );
    let session_memory_note = extract_prompt_markdown_block_by_heading(
        external_memory,
        "### Session Memory Note (markdown)",
    )
    .or_else(|| collect_prompt_session_memory_topics(external_memory));
    let dream_notebook = legacy_dream_notebook
        .clone()
        .or_else(|| project_dream.clone())
        .or_else(|| global_dream_fallback.clone());

    PromptSnapshotExternalMemoryParts {
        dream_notebook,
        session_memory_note,
        project_memory_index,
        relevant_durable_memories,
        project_dream,
        global_dream_fallback,
    }
}

fn extract_prompt_markdown_block_by_heading(content: &str, heading: &str) -> Option<String> {
    let start_idx = content.find(heading)?;
    let after_heading = &content[start_idx + heading.len()..];
    let fence_start_rel = after_heading.find("````md")?;
    let after_fence = &after_heading[fence_start_rel + "````md".len()..];
    let fence_end_rel = after_fence.find("````")?;
    let block = after_fence[..fence_end_rel].trim();
    (!block.is_empty()).then(|| block.to_string())
}

fn extract_prompt_plain_section_by_heading(content: &str, heading: &str) -> Option<String> {
    let mut collecting = false;
    let mut collected = Vec::new();

    for line in content.lines() {
        let trimmed = line.trim();
        if !collecting {
            if trimmed == heading {
                collecting = true;
            }
            continue;
        }

        if trimmed.starts_with("### ") {
            break;
        }
        collected.push(line);
    }

    let section = collected.join("\n").trim().to_string();
    (!section.is_empty()).then_some(section)
}

fn collect_prompt_session_memory_topics(content: &str) -> Option<String> {
    let mut collected = Vec::new();
    let mut remaining = content;
    let heading = "### Session Memory Topic: `";
    while let Some(start_idx) = remaining.find(heading) {
        let after_start = &remaining[start_idx..];
        let Some(line_end) = after_start.find('\n') else {
            break;
        };
        let title_line = after_start[..line_end].trim();
        let rest = &after_start[line_end + 1..];
        let Some(fence_start_rel) = rest.find("````md") else {
            remaining = rest;
            continue;
        };
        let after_fence = &rest[fence_start_rel + "````md".len()..];
        let Some(fence_end_rel) = after_fence.find("````") else {
            break;
        };
        let block = after_fence[..fence_end_rel].trim();
        if !block.is_empty() {
            collected.push(format!("{}\n\n{}", title_line, block));
        }
        remaining = &after_fence[fence_end_rel + "````".len()..];
    }

    (!collected.is_empty()).then(|| collected.join("\n\n---\n\n"))
}

/// Prompt-memory observability summary captured during external-memory injection.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct PromptMemoryObservability {
    pub project_prompt_injection_enabled: bool,
    pub relevant_recall_enabled: bool,
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
///
/// Represents a full conversation session including message history,
/// task list, pending questions, and session metadata.
///
/// # Fields
///
/// * `id` - Unique session identifier
/// * `messages` - Conversation message history
/// * `created_at` - Session creation timestamp
/// * `updated_at` - Last update timestamp
/// * `task_list` - Optional task tracking list
/// * `pending_question` - Question waiting for user response
/// * `model` - LLM model name for this session
/// * `metadata` - Extensible key-value metadata
/// * `token_budget` - Token budget configuration
/// * `token_usage` - Last token usage information
/// * `conversation_summary` - Summary of truncated context
///
/// # Lifecycle
///
/// 1. Create: `Session::new("session-id", "gpt-4o-mini")`
/// 2. Add messages: `session.add_message(Message::user("Hello"))`
/// 3. Track tasks: `session.set_task_list(task_list)`
/// 4. Ask questions: `session.set_pending_question(...)`
/// 5. Persist to storage
///
/// # Example
///
/// ```rust,ignore
/// let mut session = Session::new("session-1", "gpt-4o-mini");
/// session.add_message(Message::user("Help me with Rust"));
/// session.add_message(Message::assistant("I'd be happy to help!", None));
///
/// // Track progress
/// session.set_task_list(task_list);
///
/// // Save
/// storage.save_session(&session).await?;
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Session {
    /// Unique session identifier
    pub id: String,
    /// Human-friendly title for UI (stored on backend as the source of truth).
    #[serde(default)]
    pub title: String,
    /// Whether the session is pinned (root and child sessions can be pinned).
    #[serde(default)]
    pub pinned: bool,
    /// Session kind (root or child). Child sessions are spawned by a root session.
    #[serde(default)]
    pub kind: SessionKind,
    /// Parent session id when `kind == child` (root sessions must keep this as None).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_session_id: Option<String>,
    /// Root session id for this session tree. For root sessions this equals `id`.
    #[serde(default)]
    pub root_session_id: String,
    /// Spawn depth within the session tree. For root sessions this is 0; for child sessions 1.
    #[serde(default)]
    pub spawn_depth: u32,
    /// Conversation message history
    pub messages: Vec<Message>,
    /// Session creation timestamp
    pub created_at: DateTime<Utc>,
    /// Last update timestamp
    pub updated_at: DateTime<Utc>,
    /// Optional task list for task tracking.
    #[serde(
        default,
        rename = "task_list",
        alias = "todo_list",
        skip_serializing_if = "Option::is_none"
    )]
    pub task_list: Option<TaskList>,
    /// Pending question when waiting for user response via conclusion_with_options tool
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pending_question: Option<PendingQuestion>,
    /// Model name for this session (e.g., "gpt-4o", "gpt-4o-mini")
    #[serde(default)]
    pub model: String,
    /// Reasoning effort configured for this session.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<ReasoningEffort>,
    /// Session metadata for extensibility (other configuration)
    #[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]
    pub metadata: std::collections::HashMap<String, String>,
    /// Token budget configuration for this session
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token_budget: Option<crate::agent::core::budget::TokenBudget>,
    /// Last token usage information (updated after each LLM call)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token_usage: Option<crate::agent::core::agent::events::TokenBudgetUsage>,
    /// Conversation summary for context management
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conversation_summary: Option<ConversationSummary>,
    /// Structured snapshot of the effective system prompt and its sections.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_snapshot: Option<PromptSnapshot>,
    /// Historical compression events used by the UI to render compression separators.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub compression_events: Vec<CompressionEvent>,
}

/// Session type marker for spawn-session support.
///
/// - `root`: user-facing main session (can spawn child sessions)
/// - `child`: sub session spawned from a root (cannot spawn further)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SessionKind {
    #[default]
    Root,
    Child,
}

impl Session {
    /// Create a new session.
    ///
    /// # Arguments
    ///
    /// * `id` - Unique session identifier
    /// * `model` - LLM model name (e.g., "gpt-4o-mini")
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let session = Session::new("session-123", "gpt-4o-mini");
    /// assert_eq!(session.id, "session-123");
    /// assert_eq!(session.model, "gpt-4o-mini");
    /// ```
    pub fn new(id: impl Into<String>, model: impl Into<String>) -> Self {
        let now = Utc::now();
        let id = id.into();
        Self {
            id: id.clone(),
            title: "New Session".to_string(),
            pinned: false,
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
            reasoning_effort: None,
            metadata: std::collections::HashMap::new(),
            token_budget: None,
            token_usage: None,
            conversation_summary: None,
            prompt_snapshot: None,
            compression_events: Vec::new(),
        }
    }

    /// Create a new child session (sub-session) under a root session.
    pub fn new_child(
        id: impl Into<String>,
        root_session_id: impl Into<String>,
        model: impl Into<String>,
        title: impl Into<String>,
    ) -> Self {
        let now = Utc::now();
        let id = id.into();
        let root_session_id = root_session_id.into();
        Self {
            id: id.clone(),
            title: title.into(),
            pinned: false,
            kind: SessionKind::Child,
            parent_session_id: Some(root_session_id.clone()),
            root_session_id,
            spawn_depth: 1,
            messages: Vec::new(),
            created_at: now,
            updated_at: now,
            task_list: None,
            pending_question: None,
            model: model.into(),
            reasoning_effort: None,
            metadata: std::collections::HashMap::new(),
            token_budget: None,
            token_usage: None,
            conversation_summary: None,
            prompt_snapshot: None,
            compression_events: Vec::new(),
        }
    }

    /// Add a message to the conversation.
    ///
    /// Updates the session's `updated_at` timestamp.
    ///
    /// # Arguments
    ///
    /// * `message` - Message to add
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// session.add_message(Message::user("Hello"));
    /// assert_eq!(session.messages.len(), 1);
    /// ```
    pub fn add_message(&mut self, mut message: Message) {
        if matches!(message.role, Role::Tool) {
            if let Some(truncated) = truncate_tool_message_content(&message.content) {
                message.content = truncated;
            }
        }
        self.messages.push(message);
        self.updated_at = Utc::now();
    }

    /// Truncate oversized historical tool messages in-place.
    ///
    /// Returns the number of tool messages that were compacted.
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

    /// Set the task list for this session.
    ///
    /// # Arguments
    ///
    /// * `task_list` - Task list to set
    pub fn set_task_list(&mut self, task_list: TaskList) {
        self.task_list = Some(task_list);
        self.updated_at = Utc::now();
    }

    /// Update a task item status.
    ///
    /// # Arguments
    ///
    /// * `item_id` - ID of the task item to update
    /// * `status` - New status
    /// * `notes` - Optional notes to append
    ///
    /// # Returns
    ///
    /// Success message or error string
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

    /// Format task list for display in system prompt.
    ///
    /// Returns a formatted string of the task list suitable
    /// for inclusion in the LLM system prompt.
    pub fn format_task_list_for_prompt(&self) -> String {
        self.task_list
            .as_ref()
            .map_or_else(String::new, |list| list.format_for_prompt())
    }

    /// Set a pending question when waiting for user response
    /// Set a pending question when waiting for user response.
    ///
    /// Called when the agent uses the `conclusion_with_options` tool to request
    /// user input before continuing execution.
    ///
    /// # Arguments
    ///
    /// * `tool_call_id` - ID of the tool call
    /// * `question` - Question to ask
    /// * `options` - Predefined response options
    /// * `allow_custom` - Whether custom responses allowed
    pub fn set_pending_question(
        &mut self,
        tool_call_id: String,
        question: String,
        options: Vec<String>,
        allow_custom: bool,
    ) {
        self.pending_question = Some(PendingQuestion {
            tool_call_id,
            question,
            options,
            allow_custom,
        });
        self.updated_at = Utc::now();
    }

    /// Clear the pending question after receiving user response
    /// Clear the pending question after receiving user response.
    ///
    /// Removes the pending question once the user has submitted
    /// their response via the API.
    pub fn clear_pending_question(&mut self) {
        self.pending_question = None;
        self.updated_at = Utc::now();
    }

    /// Check if there's a pending question waiting for response
    /// Check if there's a pending question waiting for response.
    ///
    /// # Returns
    ///
    /// `true` if a pending question exists
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

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::core::tools::FunctionCall;
    use serde_json::json;

    // ── Metadata serialization / deserialization ────────────────────────

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
        // Simulates loading a session.json from before the metadata feature.
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

    // ── Constructor helpers produce metadata: None ──────────────────────

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

    // ── Metadata does not leak into to_provider outputs ────────────────

    #[test]
    fn tool_result_metadata_not_leaked_to_openai_provider() {
        use crate::agent::llm::api::models::ChatMessage as OpenAIChatMessage;
        use crate::agent::llm::protocol::ToProvider;

        let mut msg = Message::tool_result("call-1", "tool output");
        msg.metadata = Some(json!({
            "elapsed_ms": 200,
            "is_mutating": true,
        }));

        let openai_msg: OpenAIChatMessage = msg.to_provider().unwrap();
        // OpenAIChatMessage has no metadata field — verify it serializes clean.
        let serialized = serde_json::to_string(&openai_msg).unwrap();
        assert!(
            !serialized.contains("elapsed_ms"),
            "OpenAI provider message should not contain tool lifecycle metadata"
        );
        assert!(
            !serialized.contains("is_mutating"),
            "OpenAI provider message should not contain is_mutating"
        );
    }

    #[test]
    fn tool_result_metadata_not_leaked_to_provider_batch() {
        use crate::agent::llm::api::models::ChatMessage as OpenAIChatMessage;
        use crate::agent::llm::protocol::ToProviderBatch;

        let mut tool_msg = Message::tool_result("call-1", "tool output");
        tool_msg.metadata = Some(json!({
            "elapsed_ms": 300,
            "is_mutating": false,
            "auto_approved": true,
        }));

        let tc = crate::agent::core::tools::ToolCall {
            id: "call-1".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "Read".to_string(),
                arguments: r#"{"file_path":"test.rs"}"#.to_string(),
            },
        };
        let mut assistant_msg = Message::assistant("reading file", Some(vec![tc]));
        assistant_msg.metadata = Some(json!({"extra": "should vanish"}));

        let messages = vec![Message::user("show me the file"), assistant_msg, tool_msg];

        let provider_msgs: Vec<OpenAIChatMessage> = messages.to_provider_batch().unwrap();
        for pm in &provider_msgs {
            let serialized = serde_json::to_string(pm).unwrap();
            assert!(
                !serialized.contains("elapsed_ms"),
                "Provider batch output should not contain elapsed_ms"
            );
            assert!(
                !serialized.contains("is_mutating"),
                "Provider batch output should not contain is_mutating"
            );
            assert!(
                !serialized.contains("should vanish"),
                "Provider batch output should not contain stray metadata"
            );
        }
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
    fn assistant_with_tool_calls_metadata_not_leaked_to_openai_provider() {
        use crate::agent::llm::api::models::ChatMessage as OpenAIChatMessage;
        use crate::agent::llm::protocol::ToProvider;

        let tc = crate::agent::core::tools::ToolCall {
            id: "call-1".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "Bash".to_string(),
                arguments: r#"{"command":"ls"}"#.to_string(),
            },
        };
        let mut msg = Message::assistant("Let me check.", Some(vec![tc]));
        msg.metadata = Some(json!({"extra": "should not appear"}));

        let openai_msg: OpenAIChatMessage = msg.to_provider().unwrap();
        let serialized = serde_json::to_string(&openai_msg).unwrap();
        assert!(
            !serialized.contains("should not appear"),
            "Assistant metadata should not leak to OpenAI provider format"
        );
    }

    // ── Metadata roundtrip through clone ────────────────────────────────

    #[test]
    fn message_clone_preserves_metadata() {
        let mut msg = Message::tool_result("call-1", "data");
        msg.metadata = Some(json!({"elapsed_ms": 42}));

        let cloned = msg.clone();
        assert_eq!(cloned.metadata, msg.metadata);
    }

    // ── Metadata survives Session add_message ──────────────────────────

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
}
