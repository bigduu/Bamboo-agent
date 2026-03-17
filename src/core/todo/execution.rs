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
        assert!(TodoStatus::Skipped {
            reason: "test".into()
        }
        .is_terminal());
    }

    #[test]
    fn test_status_is_pending() {
        assert!(TodoStatus::Pending.is_pending());
        assert!(TodoStatus::AwaitingApproval {
            approval_id: Uuid::new_v4()
        }
        .is_pending());
        assert!(!TodoStatus::InProgress.is_pending());
        assert!(!TodoStatus::Completed.is_pending());
        assert!(!TodoStatus::Failed {
            error: "test".into()
        }
        .is_pending());
    }

    #[test]
    fn test_status_as_str() {
        assert_eq!(TodoStatus::Pending.as_str(), "pending");
        assert_eq!(
            TodoStatus::AwaitingApproval {
                approval_id: Uuid::new_v4()
            }
            .as_str(),
            "awaiting_approval"
        );
        assert_eq!(TodoStatus::InProgress.as_str(), "in_progress");
        assert_eq!(TodoStatus::Completed.as_str(), "completed");
        assert_eq!(
            TodoStatus::Failed {
                error: "test".into()
            }
            .as_str(),
            "failed"
        );
        assert_eq!(
            TodoStatus::Skipped {
                reason: "test".into()
            }
            .as_str(),
            "skipped"
        );
    }

    #[test]
    fn test_status_default() {
        let status = TodoStatus::default();
        assert!(matches!(status, TodoStatus::Pending));
    }

    #[test]
    fn test_status_serialization() {
        let status = TodoStatus::InProgress;
        let json = serde_json::to_string(&status).unwrap();
        assert!(json.contains("in_progress"));
    }

    #[test]
    fn test_status_deserialization() {
        let json = r#"{"status":"completed"}"#;
        let status: TodoStatus = serde_json::from_str(json).unwrap();
        assert!(matches!(status, TodoStatus::Completed));
    }

    #[test]
    fn test_status_clone() {
        let status = TodoStatus::InProgress;
        let cloned = status.clone();
        assert!(matches!(cloned, TodoStatus::InProgress));
    }

    #[test]
    fn test_status_debug() {
        let status = TodoStatus::Pending;
        let debug_str = format!("{:?}", status);
        assert!(debug_str.contains("Pending"));
    }

    #[test]
    fn test_execution_default() {
        let execution = TodoExecution::default();
        assert!(execution.result.is_none());
        assert!(execution.error.is_none());
        assert!(execution.duration_ms.is_none());
        assert!(execution.approval_id.is_none());
        assert_eq!(execution.retry_count, 0);
        assert!(execution.metadata.is_none());
    }

    #[test]
    fn test_execution_with_result() {
        let result = serde_json::json!({"key": "value"});
        let execution = TodoExecution::with_result(result.clone());
        assert_eq!(execution.result, Some(result));
        assert!(execution.error.is_none());
    }

    #[test]
    fn test_execution_with_error() {
        let execution = TodoExecution::with_error("Something went wrong");
        assert!(execution.result.is_none());
        assert_eq!(execution.error, Some("Something went wrong".to_string()));
    }

    #[test]
    fn test_execution_clone() {
        let execution = TodoExecution::with_result(serde_json::json!({"test": "data"}));
        let cloned = execution.clone();
        assert_eq!(execution.result, cloned.result);
    }

    #[test]
    fn test_execution_debug() {
        let execution = TodoExecution::default();
        let debug_str = format!("{:?}", execution);
        assert!(debug_str.contains("TodoExecution"));
    }

    #[test]
    fn test_execution_serialization() {
        let execution = TodoExecution::with_result(serde_json::json!({"output": "success"}));
        let json = serde_json::to_string(&execution).unwrap();
        assert!(json.contains("output"));
    }

    #[test]
    fn test_execution_deserialization() {
        let json = r#"{"result":null,"error":null,"duration_ms":100,"approval_id":null,"retry_count":0,"metadata":null}"#;
        let execution: TodoExecution = serde_json::from_str(json).unwrap();
        assert_eq!(execution.duration_ms, Some(100));
    }
}
