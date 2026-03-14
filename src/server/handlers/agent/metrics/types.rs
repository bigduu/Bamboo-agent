use chrono::NaiveDate;
use serde::{Deserialize, Serialize};

/// Query parameters for metrics summary requests
#[derive(Debug, Deserialize)]
pub struct MetricsSummaryQuery {
    /// Start date for the metrics range (YYYY-MM-DD)
    pub start_date: Option<NaiveDate>,
    /// End date for the metrics range (YYYY-MM-DD)
    pub end_date: Option<NaiveDate>,
}

/// Query parameters for session metrics requests
#[derive(Debug, Deserialize)]
pub struct MetricsSessionsQuery {
    /// Start date for filtering sessions
    pub start_date: Option<NaiveDate>,
    /// End date for filtering sessions
    pub end_date: Option<NaiveDate>,
    /// Filter by model name
    pub model: Option<String>,
    /// Maximum number of sessions to return
    pub limit: Option<u32>,
}

/// Query parameters for daily metrics requests
#[derive(Debug, Deserialize)]
pub struct MetricsDailyQuery {
    /// Number of days to include (default: 30, max: 365)
    pub days: Option<u32>,
    /// End date for the range
    pub end_date: Option<NaiveDate>,
    /// Granularity: "daily", "weekly", or "monthly" (default: "daily")
    pub granularity: Option<String>,
}

/// Query parameters for forward metrics requests
#[derive(Debug, Deserialize)]
pub struct ForwardMetricsQuery {
    /// Start date for the metrics range
    pub start_date: Option<NaiveDate>,
    /// End date for the metrics range
    pub end_date: Option<NaiveDate>,
    /// Filter by endpoint
    pub endpoint: Option<String>,
    /// Filter by model
    pub model: Option<String>,
    /// Maximum number of records to return
    pub limit: Option<u32>,
}

/// Unified summary combining chat and forward metrics
#[derive(Debug, Serialize)]
pub struct UnifiedSummary {
    /// Chat session metrics
    pub chat: crate::agent::metrics::MetricsSummary,
    /// Forward proxy metrics
    pub forward: crate::agent::metrics::ForwardMetricsSummary,
    /// Combined aggregate metrics
    pub combined: CombinedSummary,
}

/// Combined aggregate metrics from both chat and forward sources
#[derive(Debug, Serialize)]
pub struct CombinedSummary {
    /// Total number of requests (sessions + forwards)
    pub total_requests: u64,
    /// Total tokens used
    pub total_tokens: u64,
    /// Number of successful requests
    pub total_success: u64,
    /// Number of failed requests
    pub total_errors: u64,
    /// Success rate percentage
    pub success_rate: f64,
}

/// Unified timeline point combining chat and forward metrics
#[derive(Debug, Serialize)]
pub struct UnifiedTimelinePoint {
    /// Date in YYYY-MM-DD format
    pub date: String,
    /// Tokens used in chat sessions
    pub chat_tokens: u64,
    /// Number of chat sessions
    pub chat_sessions: u32,
    /// Tokens used in forward requests
    pub forward_tokens: u64,
    /// Number of forward requests
    pub forward_requests: u32,
    /// Total tokens (chat + forward)
    pub total_tokens: u64,
}
