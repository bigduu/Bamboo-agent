use crate::agent::core::tools::ToolResult;
use crate::agent::core::todo::{TodoItemStatus, TodoList};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AgentEvent {
    Token {
        content: String,
    },

    ToolStart {
        tool_call_id: String,
        tool_name: String,
        arguments: serde_json::Value,
    },

    ToolComplete {
        tool_call_id: String,
        result: ToolResult,
    },

    ToolError {
        tool_call_id: String,
        error: String,
    },

    NeedClarification {
        question: String,
        options: Option<Vec<String>>,
    },

    /// Emitted when todo list is created or updated
    TodoListUpdated {
        todo_list: TodoList,
    },

    /// Emitted when a todo item makes progress (delta update)
    TodoListItemProgress {
        session_id: String,
        item_id: String,
        status: TodoItemStatus,
        tool_calls_count: usize,
        version: u64,
    },

    /// Emitted when all todo items are completed
    TodoListCompleted {
        session_id: String,
        completed_at: DateTime<Utc>,
        total_rounds: u32,
        total_tool_calls: usize,
    },

    /// Emitted when todo evaluation starts (NEW)
    TodoEvaluationStarted {
        session_id: String,
        items_count: usize,
    },

    /// Emitted when todo evaluation completes (NEW)
    TodoEvaluationCompleted {
        session_id: String,
        updates_count: usize,
        reasoning: String,
    },

    /// Emitted when token budget is prepared (after context truncation)
    TokenBudgetUpdated {
        usage: TokenBudgetUsage,
    },

    /// Emitted when conversation context is summarized
    ContextSummarized {
        summary: String,
        messages_summarized: usize,
        tokens_saved: u32,
    },

    Complete {
        usage: TokenUsage,
    },

    Error {
        message: String,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct TokenUsage {
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    pub total_tokens: u32,
}

/// Token budget usage information sent to frontend
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenBudgetUsage {
    /// Tokens used by system message(s)
    pub system_tokens: u32,
    /// Tokens used by conversation summary (if any)
    pub summary_tokens: u32,
    /// Tokens used by recent message window
    pub window_tokens: u32,
    /// Total tokens in prepared context
    pub total_tokens: u32,
    /// Budget limit for input tokens
    pub budget_limit: u32,
    /// Whether truncation occurred
    pub truncation_occurred: bool,
    /// Number of message segments removed
    pub segments_removed: usize,
}
