//! Metrics types for tracking agent performance and usage
//!
//! This module provides data structures for collecting and aggregating
//! metrics about agent sessions, token usage, tool calls, and performance.

use std::collections::HashMap;

use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};

/// Token usage statistics
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TokenUsage {
    /// Number of tokens in the prompt
    pub prompt_tokens: u64,
    /// Number of tokens in the completion
    pub completion_tokens: u64,
    /// Total tokens (prompt + completion)
    pub total_tokens: u64,
}

impl TokenUsage {
    pub fn add_assign(&mut self, other: TokenUsage) {
        self.prompt_tokens += other.prompt_tokens;
        self.completion_tokens += other.completion_tokens;
        self.total_tokens += other.total_tokens;
    }
}

impl From<crate::agent::core::agent::events::TokenUsage> for TokenUsage {
    fn from(value: crate::agent::core::agent::events::TokenUsage) -> Self {
        Self {
            prompt_tokens: u64::from(value.prompt_tokens),
            completion_tokens: u64::from(value.completion_tokens),
            total_tokens: u64::from(value.total_tokens),
        }
    }
}

/// Round execution status
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RoundStatus {
    /// Round is currently running
    Running,
    /// Round completed successfully
    Success,
    /// Round ended with an error
    Error,
    /// Round was cancelled by user
    Cancelled,
}

impl RoundStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Success => "success",
            Self::Error => "error",
            Self::Cancelled => "cancelled",
        }
    }

    pub fn from_db(value: &str) -> Option<Self> {
        match value {
            "running" => Some(Self::Running),
            "success" => Some(Self::Success),
            "error" => Some(Self::Error),
            "cancelled" => Some(Self::Cancelled),
            _ => None,
        }
    }
}

/// Session execution status
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SessionStatus {
    /// Session is currently active
    Running,
    /// Session completed successfully
    Completed,
    /// Session ended with an error
    Error,
    /// Session was cancelled by user
    Cancelled,
}

impl SessionStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Error => "error",
            Self::Cancelled => "cancelled",
        }
    }

    pub fn from_db(value: &str) -> Option<Self> {
        match value {
            "running" => Some(Self::Running),
            "completed" => Some(Self::Completed),
            "error" => Some(Self::Error),
            "cancelled" => Some(Self::Cancelled),
            _ => None,
        }
    }
}

/// Metrics for a single tool call
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ToolCallMetrics {
    /// Unique identifier for the tool call
    pub tool_call_id: String,
    /// Name of the tool that was called
    pub tool_name: String,
    /// When the tool call started
    pub started_at: DateTime<Utc>,
    /// When the tool call completed
    pub completed_at: Option<DateTime<Utc>>,
    /// Whether the call succeeded
    pub success: Option<bool>,
    /// Error message if the call failed
    pub error: Option<String>,
    /// Duration of the call in milliseconds
    pub duration_ms: Option<u64>,
}

/// Metrics for a single conversation round
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RoundMetrics {
    /// Unique round identifier
    pub round_id: String,
    /// Session this round belongs to
    pub session_id: String,
    /// Model used for this round
    pub model: String,
    /// When the round started
    pub started_at: DateTime<Utc>,
    /// When the round completed
    pub completed_at: Option<DateTime<Utc>>,
    /// Token usage for this round
    pub token_usage: TokenUsage,
    /// Tool calls made during this round
    pub tool_calls: Vec<ToolCallMetrics>,
    /// Round execution status
    pub status: RoundStatus,
    /// Error message if round failed
    pub error: Option<String>,
    /// Round duration in milliseconds
    pub duration_ms: Option<u64>,
    /// Number of tool outputs compacted into prompt-side cache summaries in this round.
    #[serde(default)]
    pub prompt_cached_tool_outputs: u32,
}

/// Metrics for an entire session
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SessionMetrics {
    /// Unique session identifier
    pub session_id: String,
    /// Model used for this session
    pub model: String,
    /// When the session started
    pub started_at: DateTime<Utc>,
    /// When the session completed
    pub completed_at: Option<DateTime<Utc>>,
    /// Total number of rounds in the session
    pub total_rounds: u32,
    /// Total token usage across all rounds
    pub total_token_usage: TokenUsage,
    /// Total number of tool calls
    pub tool_call_count: u32,
    /// Breakdown of tool calls by tool name
    pub tool_breakdown: HashMap<String, u32>,
    /// Session execution status
    pub status: SessionStatus,
    /// Total number of messages exchanged
    pub message_count: u32,
    /// Session duration in milliseconds
    pub duration_ms: Option<u64>,
    /// Total number of prompt-side cached tool outputs observed across rounds.
    #[serde(default)]
    pub prompt_cached_tool_outputs: u64,
}

/// Detailed session metrics with round information
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SessionDetail {
    /// Session-level metrics
    pub session: SessionMetrics,
    /// Metrics for each round in the session
    pub rounds: Vec<RoundMetrics>,
}

/// Aggregated metrics for a single day
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DailyMetrics {
    /// Date for these metrics
    pub date: NaiveDate,
    /// Total number of sessions
    pub total_sessions: u32,
    /// Total number of rounds
    pub total_rounds: u32,
    /// Total token usage
    pub total_token_usage: TokenUsage,
    /// Total number of tool calls
    pub total_tool_calls: u32,
    /// Token usage breakdown by model
    pub model_breakdown: HashMap<String, TokenUsage>,
    /// Tool call breakdown by tool name
    pub tool_breakdown: HashMap<String, u32>,
    /// Total number of prompt-side cached tool outputs observed on this day.
    #[serde(default)]
    pub prompt_cached_tool_outputs: u64,
}

/// Overall metrics summary
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MetricsSummary {
    /// Total number of sessions
    pub total_sessions: u64,
    /// Total token usage
    pub total_tokens: TokenUsage,
    /// Total number of tool calls
    pub total_tool_calls: u64,
    /// Number of currently active sessions
    pub active_sessions: u64,
    /// Total number of prompt-side cached tool outputs.
    #[serde(default)]
    pub prompt_cached_tool_outputs: u64,
}

/// Metrics aggregated by model
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ModelMetrics {
    /// Model name
    pub model: String,
    /// Number of sessions using this model
    pub sessions: u64,
    /// Number of rounds using this model
    pub rounds: u64,
    /// Token usage for this model
    pub tokens: TokenUsage,
    /// Number of tool calls using this model
    pub tool_calls: u64,
    /// Number of prompt-side cached tool outputs for this model.
    #[serde(default)]
    pub prompt_cached_tool_outputs: u64,
}

/// Date filter for metrics queries
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct MetricsDateFilter {
    /// Start date (inclusive)
    pub start_date: Option<NaiveDate>,
    /// End date (inclusive)
    pub end_date: Option<NaiveDate>,
}

/// Filter for session metrics queries
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct SessionMetricsFilter {
    /// Start date filter
    pub start_date: Option<NaiveDate>,
    /// End date filter
    pub end_date: Option<NaiveDate>,
    /// Filter by model name
    pub model: Option<String>,
    /// Limit number of results
    pub limit: Option<u32>,
}

/// Status of a forwarded request
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ForwardStatus {
    /// Request succeeded
    Success,
    /// Request failed
    Error,
}

impl ForwardStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Error => "error",
        }
    }

    pub fn from_db(value: &str) -> Option<Self> {
        match value {
            "success" => Some(Self::Success),
            "error" => Some(Self::Error),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ForwardRequestMetrics {
    pub forward_id: String,
    pub endpoint: String,
    pub model: String,
    pub is_stream: bool,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub status_code: Option<u16>,
    pub status: Option<ForwardStatus>,
    pub token_usage: Option<TokenUsage>,
    pub error: Option<String>,
    pub duration_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ForwardMetricsSummary {
    pub total_requests: u64,
    pub successful_requests: u64,
    pub failed_requests: u64,
    pub total_tokens: TokenUsage,
    pub avg_duration_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ForwardEndpointMetrics {
    pub endpoint: String,
    pub requests: u64,
    pub successful: u64,
    pub failed: u64,
    pub tokens: TokenUsage,
    pub avg_duration_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct ForwardMetricsFilter {
    pub start_date: Option<NaiveDate>,
    pub end_date: Option<NaiveDate>,
    pub endpoint: Option<String>,
    pub model: Option<String>,
    pub limit: Option<u32>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_token_usage_default() {
        let usage = TokenUsage::default();
        assert_eq!(usage.prompt_tokens, 0);
        assert_eq!(usage.completion_tokens, 0);
        assert_eq!(usage.total_tokens, 0);
    }

    #[test]
    fn test_token_usage_add_assign() {
        let mut usage1 = TokenUsage {
            prompt_tokens: 100,
            completion_tokens: 50,
            total_tokens: 150,
        };
        let usage2 = TokenUsage {
            prompt_tokens: 200,
            completion_tokens: 100,
            total_tokens: 300,
        };

        usage1.add_assign(usage2);

        assert_eq!(usage1.prompt_tokens, 300);
        assert_eq!(usage1.completion_tokens, 150);
        assert_eq!(usage1.total_tokens, 450);
    }

    #[test]
    fn test_token_usage_serialization() {
        let usage = TokenUsage {
            prompt_tokens: 100,
            completion_tokens: 50,
            total_tokens: 150,
        };

        let json = serde_json::to_string(&usage).unwrap();
        assert!(json.contains("\"prompt_tokens\":100"));
        assert!(json.contains("\"completion_tokens\":50"));
    }

    #[test]
    fn test_round_status_as_str() {
        assert_eq!(RoundStatus::Running.as_str(), "running");
        assert_eq!(RoundStatus::Success.as_str(), "success");
        assert_eq!(RoundStatus::Error.as_str(), "error");
        assert_eq!(RoundStatus::Cancelled.as_str(), "cancelled");
    }

    #[test]
    fn test_round_status_from_db() {
        assert_eq!(RoundStatus::from_db("running"), Some(RoundStatus::Running));
        assert_eq!(RoundStatus::from_db("success"), Some(RoundStatus::Success));
        assert_eq!(RoundStatus::from_db("invalid"), None);
    }

    #[test]
    fn test_round_status_serialization() {
        let status = RoundStatus::Success;
        let json = serde_json::to_string(&status).unwrap();
        assert!(json.contains("\"success\""));
    }

    #[test]
    fn test_session_status_as_str() {
        assert_eq!(SessionStatus::Running.as_str(), "running");
        assert_eq!(SessionStatus::Completed.as_str(), "completed");
        assert_eq!(SessionStatus::Error.as_str(), "error");
        assert_eq!(SessionStatus::Cancelled.as_str(), "cancelled");
    }

    #[test]
    fn test_session_status_from_db() {
        assert_eq!(
            SessionStatus::from_db("running"),
            Some(SessionStatus::Running)
        );
        assert_eq!(
            SessionStatus::from_db("completed"),
            Some(SessionStatus::Completed)
        );
        assert_eq!(SessionStatus::from_db("invalid"), None);
    }

    #[test]
    fn test_forward_status_as_str() {
        assert_eq!(ForwardStatus::Success.as_str(), "success");
        assert_eq!(ForwardStatus::Error.as_str(), "error");
    }

    #[test]
    fn test_forward_status_from_db() {
        assert_eq!(
            ForwardStatus::from_db("success"),
            Some(ForwardStatus::Success)
        );
        assert_eq!(ForwardStatus::from_db("error"), Some(ForwardStatus::Error));
        assert_eq!(ForwardStatus::from_db("invalid"), None);
    }

    #[test]
    fn test_metrics_date_filter_default() {
        let filter = MetricsDateFilter::default();
        assert!(filter.start_date.is_none());
        assert!(filter.end_date.is_none());
    }

    #[test]
    fn test_session_metrics_filter_default() {
        let filter = SessionMetricsFilter::default();
        assert!(filter.start_date.is_none());
        assert!(filter.model.is_none());
        assert!(filter.limit.is_none());
    }

    #[test]
    fn test_forward_metrics_filter_default() {
        let filter = ForwardMetricsFilter::default();
        assert!(filter.start_date.is_none());
        assert!(filter.endpoint.is_none());
        assert!(filter.limit.is_none());
    }

    #[test]
    fn test_tool_call_metrics_serialization() {
        let metrics = ToolCallMetrics {
            tool_call_id: "call-123".to_string(),
            tool_name: "bash".to_string(),
            started_at: Utc::now(),
            completed_at: Some(Utc::now()),
            success: Some(true),
            error: None,
            duration_ms: Some(150),
        };

        let json = serde_json::to_string(&metrics).unwrap();
        assert!(json.contains("\"tool_call_id\":\"call-123\""));
        assert!(json.contains("\"tool_name\":\"bash\""));
    }

    #[test]
    fn test_round_metrics_serialization() {
        let metrics = RoundMetrics {
            round_id: "round-1".to_string(),
            session_id: "session-1".to_string(),
            model: "gpt-4".to_string(),
            started_at: Utc::now(),
            completed_at: None,
            token_usage: TokenUsage::default(),
            tool_calls: vec![],
            status: RoundStatus::Running,
            error: None,
            duration_ms: None,
            prompt_cached_tool_outputs: 0,
        };

        let json = serde_json::to_string(&metrics).unwrap();
        assert!(json.contains("\"round_id\":\"round-1\""));
        assert!(json.contains("\"model\":\"gpt-4\""));
    }

    #[test]
    fn test_session_metrics_serialization() {
        let metrics = SessionMetrics {
            session_id: "session-1".to_string(),
            model: "gpt-4".to_string(),
            started_at: Utc::now(),
            completed_at: None,
            total_rounds: 5,
            total_token_usage: TokenUsage::default(),
            tool_call_count: 10,
            tool_breakdown: HashMap::new(),
            status: SessionStatus::Running,
            message_count: 15,
            duration_ms: None,
            prompt_cached_tool_outputs: 0,
        };

        let json = serde_json::to_string(&metrics).unwrap();
        assert!(json.contains("\"session_id\":\"session-1\""));
        assert!(json.contains("\"total_rounds\":5"));
    }

    #[test]
    fn test_daily_metrics_serialization() {
        let metrics = DailyMetrics {
            date: NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
            total_sessions: 10,
            total_rounds: 50,
            total_token_usage: TokenUsage::default(),
            total_tool_calls: 100,
            prompt_cached_tool_outputs: 0,
            model_breakdown: HashMap::new(),
            tool_breakdown: HashMap::new(),
        };

        let json = serde_json::to_string(&metrics).unwrap();
        assert!(json.contains("\"total_sessions\":10"));
        assert!(json.contains("\"total_rounds\":50"));
    }

    #[test]
    fn test_metrics_summary_serialization() {
        let summary = MetricsSummary {
            total_sessions: 100,
            total_tokens: TokenUsage::default(),
            total_tool_calls: 500,
            active_sessions: 5,
            prompt_cached_tool_outputs: 0,
        };

        let json = serde_json::to_string(&summary).unwrap();
        assert!(json.contains("\"total_sessions\":100"));
        assert!(json.contains("\"active_sessions\":5"));
    }

    #[test]
    fn test_model_metrics_serialization() {
        let metrics = ModelMetrics {
            model: "gpt-4".to_string(),
            sessions: 50,
            rounds: 200,
            tokens: TokenUsage::default(),
            tool_calls: 100,
            prompt_cached_tool_outputs: 0,
        };

        let json = serde_json::to_string(&metrics).unwrap();
        assert!(json.contains("\"model\":\"gpt-4\""));
        assert!(json.contains("\"sessions\":50"));
    }

    #[test]
    fn test_forward_request_metrics_serialization() {
        let metrics = ForwardRequestMetrics {
            forward_id: "fwd-123".to_string(),
            endpoint: "/api/chat".to_string(),
            model: "gpt-4".to_string(),
            is_stream: true,
            started_at: Utc::now(),
            completed_at: Some(Utc::now()),
            status_code: Some(200),
            status: Some(ForwardStatus::Success),
            token_usage: None,
            error: None,
            duration_ms: Some(250),
        };

        let json = serde_json::to_string(&metrics).unwrap();
        assert!(json.contains("\"forward_id\":\"fwd-123\""));
        assert!(json.contains("\"endpoint\":\"/api/chat\""));
    }

    #[test]
    fn test_forward_metrics_summary_serialization() {
        let summary = ForwardMetricsSummary {
            total_requests: 1000,
            successful_requests: 950,
            failed_requests: 50,
            total_tokens: TokenUsage::default(),
            avg_duration_ms: Some(200),
        };

        let json = serde_json::to_string(&summary).unwrap();
        assert!(json.contains("\"total_requests\":1000"));
        assert!(json.contains("\"successful_requests\":950"));
    }

    #[test]
    fn test_token_usage_clone() {
        let usage = TokenUsage {
            prompt_tokens: 100,
            completion_tokens: 50,
            total_tokens: 150,
        };
        let cloned = usage.clone();
        assert_eq!(usage.prompt_tokens, cloned.prompt_tokens);
    }

    #[test]
    fn test_round_status_clone() {
        let status = RoundStatus::Success;
        let cloned = status.clone();
        assert_eq!(status, cloned);
    }

    #[test]
    fn test_session_status_clone() {
        let status = SessionStatus::Completed;
        let cloned = status.clone();
        assert_eq!(status, cloned);
    }

    #[test]
    fn test_forward_status_clone() {
        let status = ForwardStatus::Success;
        let cloned = status.clone();
        assert_eq!(status, cloned);
    }

    #[test]
    fn test_token_usage_eq() {
        let usage1 = TokenUsage {
            prompt_tokens: 100,
            completion_tokens: 50,
            total_tokens: 150,
        };
        let usage2 = TokenUsage {
            prompt_tokens: 100,
            completion_tokens: 50,
            total_tokens: 150,
        };
        assert_eq!(usage1, usage2);
    }

    #[test]
    fn test_round_status_eq() {
        assert_eq!(RoundStatus::Running, RoundStatus::Running);
        assert_ne!(RoundStatus::Running, RoundStatus::Success);
    }

    #[test]
    fn test_session_status_eq() {
        assert_eq!(SessionStatus::Running, SessionStatus::Running);
        assert_ne!(SessionStatus::Running, SessionStatus::Completed);
    }
}
