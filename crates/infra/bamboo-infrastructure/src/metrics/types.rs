//! Metrics types for tracking agent performance and usage
//!
//! This module provides data structures for collecting and aggregating
//! metrics about agent sessions, token usage, tool calls, and performance.

use std::collections::HashMap;

use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};

/// Re-exported shared token usage type.
///
/// See [`bamboo_domain::TokenUsage`] for the canonical definition.
pub use bamboo_domain::TokenUsage;

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
    /// Session execution is paused awaiting an external response or resume trigger
    AwaitingResponse,
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
            Self::AwaitingResponse => "awaiting_response",
            Self::Completed => "completed",
            Self::Error => "error",
            Self::Cancelled => "cancelled",
        }
    }

    pub fn from_db(value: &str) -> Option<Self> {
        match value {
            "running" => Some(Self::Running),
            "awaiting_response" => Some(Self::AwaitingResponse),
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
    /// Tokens saved by prompt-side tool output compaction in this round.
    #[serde(default)]
    pub prompt_cached_tool_tokens_saved: u32,
    /// Number of context compression events applied during this round.
    #[serde(default)]
    pub compression_count: u32,
    /// Tokens saved by context compression during this round.
    #[serde(default)]
    pub tokens_saved: u32,
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
    /// Total tokens saved by prompt-side tool output compaction across all rounds.
    #[serde(default)]
    pub prompt_cached_tool_tokens_saved: u64,
    /// Total number of context compression events across all rounds.
    #[serde(default)]
    pub total_compression_events: u64,
    /// Total tokens saved by context compression across all rounds.
    #[serde(default)]
    pub total_tokens_saved: u64,
}

/// Detailed session metrics with round information
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SessionDetail {
    /// Session-level metrics
    pub session: SessionMetrics,
    /// Metrics for each round in the session
    pub rounds: Vec<RoundMetrics>,
}

/// Outcome of Bamboo's automatic compact-memory recall at the prompt boundary.
///
/// This describes host-side selection only. It does not claim that a provider
/// processed the request or that the model adopted any recalled fact.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PromptMemoryRecallOutcome {
    /// Automatic relevant-memory recall was disabled for this round.
    Disabled,
    /// Recall was enabled, but there was no user query to search with.
    NoQuery,
    /// Recall completed successfully and found no candidate.
    NoMatch,
    /// The canonical memory lookup failed.
    LookupError,
    /// Deterministic lexical selection was used.
    Lexical,
    /// Model reranking selected the final compact records.
    Reranked,
    /// Model reranking failed and lexical selection was used instead.
    RerankFallback,
}

impl PromptMemoryRecallOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::NoQuery => "no_query",
            Self::NoMatch => "no_match",
            Self::LookupError => "lookup_error",
            Self::Lexical => "lexical",
            Self::Reranked => "reranked",
            Self::RerankFallback => "rerank_fallback",
        }
    }

    pub fn from_db(value: &str) -> Option<Self> {
        match value {
            "disabled" => Some(Self::Disabled),
            "no_query" => Some(Self::NoQuery),
            "no_match" => Some(Self::NoMatch),
            "lookup_error" => Some(Self::LookupError),
            "lexical" => Some(Self::Lexical),
            "reranked" => Some(Self::Reranked),
            "rerank_fallback" => Some(Self::RerankFallback),
            _ => None,
        }
    }
}

/// One Project-scoped compact memory included in an observed provider request.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PromptMemoryExposureItem {
    /// Opaque canonical Jiandu record identity.
    pub memory_id: String,
    /// Scope at observation time. Schema v1 accepts only `project` items.
    pub scope: String,
    /// Coarse lifecycle status at observation time.
    pub status_at_observation: String,
    /// One-based position in the final compact-memory request ordering.
    pub rank: u32,
    /// Characters in this item's final rendered compact representation.
    pub rendered_chars: u32,
}

/// First successfully bootstrapped compact-memory prompt observation for one
/// execution-scoped logical round.
///
/// Metrics delivery remains best-effort. A row proves only that Bamboo reached
/// its provider-stream bootstrap boundary with these compact records; it is not
/// a durable delivery acknowledgement or evidence of model adoption.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PromptMemoryExposureObservation {
    pub schema_version: u32,
    pub round_id: String,
    pub session_id: String,
    /// Server-resolved Project identity, absent for an unbound round.
    pub project_id: Option<String>,
    pub observed_at: DateTime<Utc>,
    pub recall_enabled: bool,
    pub query_present: bool,
    pub recall_outcome: PromptMemoryRecallOutcome,
    /// All final compact relevant-memory records, including Global fallback.
    pub all_compact_exposed_count: u32,
    /// Final Project records persisted in `project_items`.
    pub project_exposed_count: u32,
    /// True when the final compact set was non-empty but contained no Project item.
    pub out_of_project_only: bool,
    /// Characters in the final rendered relevant-memory section.
    pub compact_section_chars: u32,
    /// Project-only opaque identities. Global identities are never persisted.
    pub project_items: Vec<PromptMemoryExposureItem>,
}

/// Aggregated metrics for a single day
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DailyMetrics {
    /// Date for these metrics
    pub date: NaiveDate,
    /// Sessions whose session-level `started_at` falls on this date.
    ///
    /// Round and tool usage below use their own occurrence dates, so a later
    /// day can legitimately report zero sessions with non-zero usage.
    pub total_sessions: u32,
    /// Rounds whose round-level `started_at` falls on this date.
    pub total_rounds: u32,
    /// Token usage from rounds attributed to this date.
    pub total_token_usage: TokenUsage,
    /// Tool calls whose tool-level `started_at` falls on this date.
    pub total_tool_calls: u32,
    /// Round token usage attributed by each round's own model.
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
    /// Total tokens saved by prompt-side tool output compaction.
    #[serde(default)]
    pub tool_context_tokens_saved: u64,
    /// Total number of context compression events.
    #[serde(default)]
    pub total_compression_events: u64,
    /// Total tokens saved by context compression.
    #[serde(default)]
    pub total_tokens_saved: u64,
    /// Total tokens saved by non-tool context compression.
    #[serde(default)]
    pub non_tool_compression_tokens_saved: u64,
    /// Number of completed sessions in the selected range.
    #[serde(default)]
    pub completed_sessions: u64,
    /// Number of sessions currently paused awaiting an external response.
    #[serde(default)]
    pub awaiting_response_sessions: u64,
    /// Number of sessions that ended with an error.
    #[serde(default)]
    pub error_sessions: u64,
    /// Number of sessions cancelled by the user.
    #[serde(default)]
    pub cancelled_sessions: u64,
    /// Total number of execute sync mismatches observed for the filtered period.
    #[serde(default)]
    pub total_sync_mismatches: u64,
    /// Breakdown of execute sync mismatches by stable reason label.
    #[serde(default)]
    pub sync_mismatch_breakdown: HashMap<String, u64>,
}

/// Metrics aggregated by model
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ModelMetrics {
    /// Model name
    pub model: String,
    /// Sessions whose session-level model and start date match this bucket.
    pub sessions: u64,
    /// Rounds whose own model and start date match this bucket.
    pub rounds: u64,
    /// Token usage from those round rows.
    pub tokens: TokenUsage,
    /// Tool calls attributed through the model on their owning round.
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

/// Date and model filter for model-aware aggregate metrics queries.
///
/// This is intentionally separate from [`MetricsDateFilter`] so adding model
/// filtering does not break downstream code that constructs the original
/// public type with a complete struct literal.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct ModelMetricsDateFilter {
    /// Start date (inclusive)
    pub start_date: Option<NaiveDate>,
    /// End date (inclusive)
    pub end_date: Option<NaiveDate>,
    /// Filter by model name. Blank values are treated as no filter.
    pub model: Option<String>,
}

impl From<MetricsDateFilter> for ModelMetricsDateFilter {
    fn from(filter: MetricsDateFilter) -> Self {
        Self {
            start_date: filter.start_date,
            end_date: filter.end_date,
            model: None,
        }
    }
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
    /// Request has been recorded but not yet completed
    Pending,
    /// Request succeeded
    Success,
    /// Request failed
    Error,
}

impl ForwardStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Success => "success",
            Self::Error => "error",
        }
    }

    pub fn from_db(value: &str) -> Option<Self> {
        match value {
            "pending" => Some(Self::Pending),
            "success" => Some(Self::Success),
            "error" => Some(Self::Error),
            _ => None,
        }
    }
}

/// Provider token details that are not safely interchangeable with the base
/// prompt/completion totals.
///
/// The fields stay optional so a missing provider value is distinct from an
/// authoritative zero. In particular, OpenAI cache writes are not Anthropic
/// cache creations and must never be folded into that counter.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ForwardTokenDetails {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_creation_input_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_read_input_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_write_input_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_output_tokens: Option<u64>,
}

impl ForwardTokenDetails {
    pub fn is_empty(&self) -> bool {
        self.cache_creation_input_tokens.is_none()
            && self.cache_read_input_tokens.is_none()
            && self.cache_write_input_tokens.is_none()
            && self.reasoning_output_tokens.is_none()
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
    #[serde(default, skip_serializing_if = "ForwardTokenDetails::is_empty")]
    pub token_details: ForwardTokenDetails,
    pub error: Option<String>,
    pub duration_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ForwardMetricsSummary {
    pub total_requests: u64,
    pub successful_requests: u64,
    pub failed_requests: u64,
    pub total_tokens: TokenUsage,
    #[serde(default, skip_serializing_if = "ForwardTokenDetails::is_empty")]
    pub token_details: ForwardTokenDetails,
    pub avg_duration_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ForwardEndpointMetrics {
    pub endpoint: String,
    pub requests: u64,
    pub successful: u64,
    pub failed: u64,
    pub tokens: TokenUsage,
    #[serde(default, skip_serializing_if = "ForwardTokenDetails::is_empty")]
    pub token_details: ForwardTokenDetails,
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
        assert_eq!(
            SessionStatus::AwaitingResponse.as_str(),
            "awaiting_response"
        );
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
            SessionStatus::from_db("awaiting_response"),
            Some(SessionStatus::AwaitingResponse)
        );
        assert_eq!(
            SessionStatus::from_db("completed"),
            Some(SessionStatus::Completed)
        );
        assert_eq!(SessionStatus::from_db("invalid"), None);
    }

    #[test]
    fn test_forward_status_as_str() {
        assert_eq!(ForwardStatus::Pending.as_str(), "pending");
        assert_eq!(ForwardStatus::Success.as_str(), "success");
        assert_eq!(ForwardStatus::Error.as_str(), "error");
    }

    #[test]
    fn test_forward_status_from_db() {
        assert_eq!(
            ForwardStatus::from_db("pending"),
            Some(ForwardStatus::Pending)
        );
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
    fn model_metrics_date_filter_from_legacy_filter_defaults_to_all_models() {
        let legacy = MetricsDateFilter {
            start_date: None,
            end_date: None,
        };
        let filter = ModelMetricsDateFilter::from(legacy);

        assert!(filter.model.is_none());
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
            prompt_cached_tool_tokens_saved: 0,
            compression_count: 0,
            tokens_saved: 0,
        };

        let json = serde_json::to_string(&metrics).unwrap();
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
            prompt_cached_tool_tokens_saved: 0,
            total_compression_events: 0,
            total_tokens_saved: 0,
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
            tool_context_tokens_saved: 0,
            total_compression_events: 0,
            total_tokens_saved: 0,
            non_tool_compression_tokens_saved: 0,
            completed_sessions: 80,
            awaiting_response_sessions: 10,
            error_sessions: 7,
            cancelled_sessions: 3,
            total_sync_mismatches: 0,
            sync_mismatch_breakdown: HashMap::new(),
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
            token_details: ForwardTokenDetails {
                cache_creation_input_tokens: None,
                cache_read_input_tokens: Some(32),
                cache_write_input_tokens: Some(48),
                reasoning_output_tokens: Some(5),
            },
            error: None,
            duration_ms: Some(250),
        };

        let json = serde_json::to_string(&metrics).unwrap();
        assert!(json.contains("\"forward_id\":\"fwd-123\""));
        assert!(json.contains("\"endpoint\":\"/api/chat\""));
        assert!(json.contains("\"cache_read_input_tokens\":32"));
        assert!(json.contains("\"cache_write_input_tokens\":48"));
    }

    #[test]
    fn test_forward_metrics_summary_serialization() {
        let summary = ForwardMetricsSummary {
            total_requests: 1000,
            successful_requests: 950,
            failed_requests: 50,
            total_tokens: TokenUsage::default(),
            token_details: ForwardTokenDetails::default(),
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
        let cloned = usage;
        assert_eq!(usage.prompt_tokens, cloned.prompt_tokens);
    }

    #[test]
    fn test_round_status_clone() {
        let status = RoundStatus::Success;
        let cloned = status;
        assert_eq!(status, cloned);
    }

    #[test]
    fn test_session_status_clone() {
        let status = SessionStatus::Completed;
        let cloned = status;
        assert_eq!(status, cloned);
    }

    #[test]
    fn test_forward_status_clone() {
        let status = ForwardStatus::Pending;
        let cloned = status;
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
        assert_ne!(SessionStatus::AwaitingResponse, SessionStatus::Completed);
    }

    #[test]
    fn test_round_metrics_compression_fields_deserialize_with_defaults() {
        let json = r#"{"round_id":"r1","session_id":"s1","model":"m","started_at":"2026-01-01T00:00:00Z","completed_at":null,"token_usage":{"prompt_tokens":0,"completion_tokens":0,"total_tokens":0},"tool_calls":[],"status":"running","error":null,"duration_ms":null,"prompt_cached_tool_outputs":0}"#;
        let metrics: RoundMetrics = serde_json::from_str(json).unwrap();
        assert_eq!(metrics.compression_count, 0);
        assert_eq!(metrics.tokens_saved, 0);
    }

    #[test]
    fn test_session_metrics_compression_fields_deserialize_with_defaults() {
        let json = r#"{"session_id":"s1","model":"m","started_at":"2026-01-01T00:00:00Z","completed_at":null,"total_rounds":0,"total_token_usage":{"prompt_tokens":0,"completion_tokens":0,"total_tokens":0},"tool_call_count":0,"tool_breakdown":{},"status":"running","message_count":0,"duration_ms":null,"prompt_cached_tool_outputs":0}"#;
        let metrics: SessionMetrics = serde_json::from_str(json).unwrap();
        assert_eq!(metrics.total_compression_events, 0);
        assert_eq!(metrics.total_tokens_saved, 0);
    }

    #[test]
    fn test_metrics_summary_additive_fields_deserialize_with_defaults() {
        let json = r#"{"total_sessions":1,"total_tokens":{"prompt_tokens":0,"completion_tokens":0,"total_tokens":0},"total_tool_calls":0,"active_sessions":0}"#;
        let summary: MetricsSummary = serde_json::from_str(json).unwrap();
        assert_eq!(summary.prompt_cached_tool_outputs, 0);
        assert_eq!(summary.total_compression_events, 0);
        assert_eq!(summary.total_tokens_saved, 0);
        assert_eq!(summary.completed_sessions, 0);
        assert_eq!(summary.awaiting_response_sessions, 0);
        assert_eq!(summary.error_sessions, 0);
        assert_eq!(summary.cancelled_sessions, 0);
    }
}
