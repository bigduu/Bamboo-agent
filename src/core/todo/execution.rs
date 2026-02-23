//! TodoExecution and TodoStatus
//!
//! Tracks execution state and results for TodoItems.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Status of a todo item
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum TodoStatus {
    /// Waiting to be executed
    #[default]
    Pending,

    /// Waiting for user approval before execution
    AwaitingApproval { approval_id: Uuid },

    /// Currently being executed
    InProgress,

    /// Successfully completed
    Completed,

    /// Execution failed
    Failed { error: String },

    /// Skipped (user chose to skip or not needed)
    Skipped { reason: String },
}

impl TodoStatus {
    /// Check if this status represents a terminal state
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed { .. } | Self::Skipped { .. }
        )
    }

    /// Check if this status is awaiting action
    pub fn is_pending(&self) -> bool {
        matches!(self, Self::Pending | Self::AwaitingApproval { .. })
    }

    /// Get status as a simple string for display
    pub fn as_str(&self) -> &str {
        match self {
            Self::Pending => "pending",
            Self::AwaitingApproval { .. } => "awaiting_approval",
            Self::InProgress => "in_progress",
            Self::Completed => "completed",
            Self::Failed { .. } => "failed",
            Self::Skipped { .. } => "skipped",
        }
    }
}

/// Execution details for a todo item
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct TodoExecution {
    /// Result of the execution (if successful)
    pub result: Option<serde_json::Value>,

    /// Error message (if failed)
    pub error: Option<String>,

    /// Duration of execution in milliseconds
    pub duration_ms: Option<u64>,

    /// ID of the approval request (if approval was required)
    pub approval_id: Option<Uuid>,

    /// Number of retry attempts
    pub retry_count: u8,

    /// Additional metadata
    pub metadata: Option<serde_json::Value>,
}

impl TodoExecution {
    /// Create execution with result
    pub fn with_result(result: serde_json::Value) -> Self {
        Self {
            result: Some(result),
            ..Default::default()
        }
    }

    /// Create execution with error
    pub fn with_error(error: impl Into<String>) -> Self {
        Self {
            error: Some(error.into()),
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_status_is_terminal() {
        assert!(!TodoStatus::Pending.is_terminal());
        assert!(!TodoStatus::InProgress.is_terminal());
        assert!(TodoStatus::Completed.is_terminal());
        assert!(TodoStatus::Failed {
            error: "test".into()
        }
        .is_terminal());
    }
}
