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
//! 3. Track progress with todo list
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

use crate::agent::core::todo::{TodoItemStatus, TodoList};
use crate::agent::core::tools::ToolCall;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
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
    /// Tool calls (for Assistant messages)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<Vec<ToolCall>>,
    /// Tool call ID (for Tool result messages)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
    /// Whether this message is archived/compressed and excluded from LLM requests.
    #[serde(default, skip_serializing_if = "is_false")]
    pub compressed: bool,
    /// Compression event ID that archived this message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compressed_by_event_id: Option<String>,
    /// Message creation timestamp
    #[serde(default = "Utc::now")]
    pub created_at: DateTime<Utc>,
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
            content_parts: None,
            image_ocr: None,
            tool_calls: None,
            tool_call_id: None,
            compressed: false,
            compressed_by_event_id: None,
            created_at: Utc::now(),
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
            content_parts: Some(parts),
            image_ocr: None,
            tool_calls: None,
            tool_call_id: None,
            compressed: false,
            compressed_by_event_id: None,
            created_at: Utc::now(),
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
        Self {
            id: Uuid::new_v4().to_string(),
            role: Role::Assistant,
            content: content.into(),
            content_parts: None,
            image_ocr: None,
            tool_calls,
            tool_call_id: None,
            compressed: false,
            compressed_by_event_id: None,
            created_at: Utc::now(),
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
        Self {
            id: Uuid::new_v4().to_string(),
            role: Role::Tool,
            content: content.into(),
            content_parts: None,
            image_ocr: None,
            tool_calls: None,
            tool_call_id: Some(tool_call_id.into()),
            compressed: false,
            compressed_by_event_id: None,
            created_at: Utc::now(),
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
            content_parts: None,
            image_ocr: None,
            tool_calls: None,
            tool_call_id: None,
            compressed: false,
            compressed_by_event_id: None,
            created_at: Utc::now(),
        }
    }
}

/// A pending question waiting for user response.
///
/// When the agent calls the `ask_user` tool, it creates a pending question
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
}

impl CompressionEvent {
    pub fn new(messages_compressed: usize, segments_removed: usize) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            created_at: Utc::now(),
            messages_compressed,
            segments_removed,
        }
    }
}

/// A complete conversation session with state management.
///
/// Represents a full conversation session including message history,
/// todo list, pending questions, and session metadata.
///
/// # Fields
///
/// * `id` - Unique session identifier
/// * `messages` - Conversation message history
/// * `created_at` - Session creation timestamp
/// * `updated_at` - Last update timestamp
/// * `todo_list` - Optional task tracking list
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
/// 3. Track tasks: `session.set_todo_list(todo_list)`
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
/// session.set_todo_list(todo_list);
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
    /// Optional todo list for task tracking
    #[serde(skip_serializing_if = "Option::is_none")]
    pub todo_list: Option<TodoList>,
    /// Pending question when waiting for user response via ask_user tool
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pending_question: Option<PendingQuestion>,
    /// Model name for this session (e.g., "gpt-4o", "gpt-4o-mini")
    #[serde(default)]
    pub model: String,
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
            todo_list: None,
            pending_question: None,
            model: model.into(),
            metadata: std::collections::HashMap::new(),
            token_budget: None,
            token_usage: None,
            conversation_summary: None,
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
            todo_list: None,
            pending_question: None,
            model: model.into(),
            metadata: std::collections::HashMap::new(),
            token_budget: None,
            token_usage: None,
            conversation_summary: None,
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

    /// Set the todo list for this session
    /// Set the todo list for this session.
    ///
    /// # Arguments
    ///
    /// * `todo_list` - Todo list to set
    pub fn set_todo_list(&mut self, todo_list: TodoList) {
        self.todo_list = Some(todo_list);
        self.updated_at = Utc::now();
    }

    /// Update a todo item status
    /// Update a todo item status.
    ///
    /// # Arguments
    ///
    /// * `item_id` - ID of the todo item to update
    /// * `status` - New status
    /// * `notes` - Optional notes to append
    ///
    /// # Returns
    ///
    /// Success message or error string
    pub fn update_todo_item(
        &mut self,
        item_id: &str,
        status: TodoItemStatus,
        notes: Option<&str>,
    ) -> Result<String, String> {
        if let Some(ref mut todo_list) = self.todo_list {
            if let Some(item) = todo_list.items.iter_mut().find(|i| i.id == item_id) {
                item.status = status;
                if let Some(n) = notes {
                    if !item.notes.is_empty() {
                        item.notes.push('\n');
                    }
                    item.notes.push_str(n);
                }
                todo_list.updated_at = Utc::now();
                self.updated_at = Utc::now();
                Ok(format!("Updated item '{}' to {:?}", item_id, item.status))
            } else {
                Err(format!("Todo item '{}' not found", item_id))
            }
        } else {
            Err("No todo list exists for this session".to_string())
        }
    }

    /// Format todo list for display in system prompt
    /// Format todo list for display in system prompt.
    ///
    /// Returns a formatted string of the todo list suitable
    /// for inclusion in the LLM system prompt.
    pub fn format_todo_list_for_prompt(&self) -> String {
        self.todo_list
            .as_ref()
            .map_or_else(String::new, |list| list.format_for_prompt())
    }

    /// Set a pending question when waiting for user response
    /// Set a pending question when waiting for user response.
    ///
    /// Called when the agent uses the `ask_user` tool to request
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
