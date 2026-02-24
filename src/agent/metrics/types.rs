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
