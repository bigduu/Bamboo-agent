use std::collections::HashMap;

use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TokenUsage {
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
}

impl TokenUsage {
    pub fn add_assign(&mut self, other: TokenUsage) {
        self.prompt_tokens += other.prompt_tokens;
        self.completion_tokens += other.completion_tokens;
        self.total_tokens += other.total_tokens;
    }
}

impl From<agent_core::agent::events::TokenUsage> for TokenUsage {
    fn from(value: agent_core::agent::events::TokenUsage) -> Self {
        Self {
            prompt_tokens: u64::from(value.prompt_tokens),
            completion_tokens: u64::from(value.completion_tokens),
            total_tokens: u64::from(value.total_tokens),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RoundStatus {
    Running,
    Success,
    Error,
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SessionStatus {
    Running,
    Completed,
    Error,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ToolCallMetrics {
    pub tool_call_id: String,
    pub tool_name: String,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub success: Option<bool>,
    pub error: Option<String>,
    pub duration_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RoundMetrics {
    pub round_id: String,
    pub session_id: String,
    pub model: String,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub token_usage: TokenUsage,
    pub tool_calls: Vec<ToolCallMetrics>,
    pub status: RoundStatus,
    pub error: Option<String>,
    pub duration_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SessionMetrics {
    pub session_id: String,
    pub model: String,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub total_rounds: u32,
    pub total_token_usage: TokenUsage,
    pub tool_call_count: u32,
    pub tool_breakdown: HashMap<String, u32>,
    pub status: SessionStatus,
    pub message_count: u32,
    pub duration_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SessionDetail {
    pub session: SessionMetrics,
    pub rounds: Vec<RoundMetrics>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DailyMetrics {
    pub date: NaiveDate,
    pub total_sessions: u32,
    pub total_rounds: u32,
    pub total_token_usage: TokenUsage,
    pub total_tool_calls: u32,
    pub model_breakdown: HashMap<String, TokenUsage>,
    pub tool_breakdown: HashMap<String, u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MetricsSummary {
    pub total_sessions: u64,
    pub total_tokens: TokenUsage,
    pub total_tool_calls: u64,
    pub active_sessions: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ModelMetrics {
    pub model: String,
    pub sessions: u64,
    pub rounds: u64,
    pub tokens: TokenUsage,
    pub tool_calls: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct MetricsDateFilter {
    pub start_date: Option<NaiveDate>,
    pub end_date: Option<NaiveDate>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct SessionMetricsFilter {
    pub start_date: Option<NaiveDate>,
    pub end_date: Option<NaiveDate>,
    pub model: Option<String>,
    pub limit: Option<u32>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ForwardStatus {
    Success,
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
