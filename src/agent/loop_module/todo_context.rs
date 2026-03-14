//! TodoList context for Agent Loop integration
//!
//! This module provides TodoLoopContext which integrates TodoList
//! as a first-class citizen in the Agent Loop, similar to Token Budget.

use crate::agent::core::todo::TodoItemStatus;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

mod auto_status;
mod conversion;
mod prompt;
mod tracking;

/// TodoList context for Agent Loop
///
/// Acts as a first-class citizen in the agent loop, tracking
/// task progress throughout the entire conversation lifecycle.
#[derive(Debug, Clone)]
pub struct TodoLoopContext {
    /// Session ID
    pub session_id: String,

    /// Todo items with execution tracking
    pub items: Vec<TodoLoopItem>,

    /// Currently active todo item ID
    pub active_item_id: Option<String>,

    /// Current round number
    pub current_round: u32,

    /// Maximum rounds allowed
    pub max_rounds: u32,

    /// Creation timestamp
    pub created_at: DateTime<Utc>,

    /// Last update timestamp
    pub updated_at: DateTime<Utc>,

    /// Version number for conflict detection
    pub version: u64,
}

/// Todo item with execution tracking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TodoLoopItem {
    /// Item ID
    pub id: String,

    /// Item description
    pub description: String,

    /// Item status
    pub status: TodoItemStatus,

    /// Tool call history (tracks execution process)
    pub tool_calls: Vec<ToolCallRecord>,

    /// Round when item was started
    pub started_at_round: Option<u32>,

    /// Round when item was completed
    pub completed_at_round: Option<u32>,
}

/// Record of a tool call execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCallRecord {
    /// Round number
    pub round: u32,

    /// Tool name
    pub tool_name: String,

    /// Whether the call succeeded
    pub success: bool,

    /// Timestamp
    pub timestamp: DateTime<Utc>,
}

impl TodoLoopContext {
    /// Check if all items are completed
    pub fn is_all_completed(&self) -> bool {
        !self.items.is_empty()
            && self
                .items
                .iter()
                .all(|item| matches!(item.status, TodoItemStatus::Completed))
    }
}

#[cfg(test)]
mod tests;
