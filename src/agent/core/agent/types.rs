use crate::agent::core::todo::{TodoItemStatus, TodoList};
use crate::agent::core::tools::ToolCall;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    System,
    User,
    Assistant,
    Tool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum MessageContent {
    Text(String),
    ToolCalls(Vec<ToolCall>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    #[serde(default = "generate_id", skip_serializing_if = "String::is_empty")]
    pub id: String,
    pub role: Role,
    pub content: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<Vec<ToolCall>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
    #[serde(default = "Utc::now")]
    pub created_at: DateTime<Utc>,
}

fn generate_id() -> String {
    Uuid::new_v4().to_string()
}

impl Message {
    pub fn user(content: impl Into<String>) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            role: Role::User,
            content: content.into(),
            tool_calls: None,
            tool_call_id: None,
            created_at: Utc::now(),
        }
    }

    pub fn assistant(content: impl Into<String>, tool_calls: Option<Vec<ToolCall>>) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            role: Role::Assistant,
            content: content.into(),
            tool_calls,
            tool_call_id: None,
            created_at: Utc::now(),
        }
    }

    pub fn tool_result(tool_call_id: impl Into<String>, content: impl Into<String>) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            role: Role::Tool,
            content: content.into(),
            tool_calls: None,
            tool_call_id: Some(tool_call_id.into()),
            created_at: Utc::now(),
        }
    }

    pub fn system(content: impl Into<String>) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            role: Role::System,
            content: content.into(),
            tool_calls: None,
            tool_call_id: None,
            created_at: Utc::now(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingQuestion {
    pub tool_call_id: String,
    pub question: String,
    pub options: Vec<String>,
    pub allow_custom: bool,
}

/// Summary of conversation context for budget management.
///
/// When conversations are truncated due to token limits, a summary
/// can preserve key information from earlier context.
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
    pub fn update(&mut self, content: impl Into<String>, message_count: usize, token_count: u32) {
        self.content = content.into();
        self.message_count = message_count;
        self.token_count = token_count;
        self.updated_at = Utc::now();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Session {
    pub id: String,
    pub messages: Vec<Message>,
    pub created_at: DateTime<Utc>,
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
}

impl Session {
    pub fn new(id: impl Into<String>, model: impl Into<String>) -> Self {
        let now = Utc::now();
        Self {
            id: id.into(),
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
        }
    }

    pub fn add_message(&mut self, message: Message) {
        self.messages.push(message);
        self.updated_at = Utc::now();
    }

    /// Set the todo list for this session
    pub fn set_todo_list(&mut self, todo_list: TodoList) {
        self.todo_list = Some(todo_list);
        self.updated_at = Utc::now();
    }

    /// Update a todo item status
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
    pub fn format_todo_list_for_prompt(&self) -> String {
        self.todo_list
            .as_ref()
            .map_or_else(String::new, |list| list.format_for_prompt())
    }

    /// Set a pending question when waiting for user response
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
    pub fn clear_pending_question(&mut self) {
        self.pending_question = None;
        self.updated_at = Utc::now();
    }

    /// Check if there's a pending question waiting for response
    pub fn has_pending_question(&self) -> bool {
        self.pending_question.is_some()
    }
}
