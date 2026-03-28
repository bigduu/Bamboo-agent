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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    #[test]
    fn test_metrics_summary_query_deserialization() {
        let json = r#"{"start_date":"2024-01-01","end_date":"2024-01-31"}"#;
        let query: MetricsSummaryQuery = serde_json::from_str(json).unwrap();

        assert_eq!(
            query.start_date,
            Some(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap())
        );
        assert_eq!(
            query.end_date,
            Some(NaiveDate::from_ymd_opt(2024, 1, 31).unwrap())
        );
    }

    #[test]
    fn test_metrics_summary_query_empty() {
        let json = r#"{}"#;
        let query: MetricsSummaryQuery = serde_json::from_str(json).unwrap();

        assert!(query.start_date.is_none());
        assert!(query.end_date.is_none());
    }

    #[test]
    fn test_metrics_sessions_query_minimal() {
        let json = r#"{}"#;
        let query: MetricsSessionsQuery = serde_json::from_str(json).unwrap();

        assert!(query.start_date.is_none());
        assert!(query.model.is_none());
        assert!(query.limit.is_none());
    }

    #[test]
    fn test_metrics_sessions_query_full() {
        let json = r#"{"start_date":"2024-01-01","model":"gpt-4","limit":50}"#;
        let query: MetricsSessionsQuery = serde_json::from_str(json).unwrap();

        assert!(query.start_date.is_some());
        assert_eq!(query.model, Some("gpt-4".to_string()));
        assert_eq!(query.limit, Some(50));
    }

    #[test]
    fn test_metrics_daily_query_defaults() {
        let json = r#"{}"#;
        let query: MetricsDailyQuery = serde_json::from_str(json).unwrap();

        assert!(query.days.is_none());
        assert!(query.end_date.is_none());
        assert!(query.granularity.is_none());
    }

    #[test]
    fn test_metrics_daily_query_with_options() {
        let json = r#"{"days":30,"granularity":"weekly"}"#;
        let query: MetricsDailyQuery = serde_json::from_str(json).unwrap();

        assert_eq!(query.days, Some(30));
        assert_eq!(query.granularity, Some("weekly".to_string()));
    }

    #[test]
    fn test_forward_metrics_query_minimal() {
        let json = r#"{}"#;
        let query: ForwardMetricsQuery = serde_json::from_str(json).unwrap();

        assert!(query.start_date.is_none());
        assert!(query.endpoint.is_none());
        assert!(query.model.is_none());
        assert!(query.limit.is_none());
    }

    #[test]
    fn test_forward_metrics_query_with_filters() {
        let json = r#"{"endpoint":"/api/chat","model":"claude-3","limit":100}"#;
        let query: ForwardMetricsQuery = serde_json::from_str(json).unwrap();

        assert_eq!(query.endpoint, Some("/api/chat".to_string()));
        assert_eq!(query.model, Some("claude-3".to_string()));
        assert_eq!(query.limit, Some(100));
    }

    #[test]
    fn test_metrics_summary_query_debug() {
        let query = MetricsSummaryQuery {
            start_date: None,
            end_date: None,
        };

        let debug_str = format!("{:?}", query);
        assert!(debug_str.contains("MetricsSummaryQuery"));
    }

    #[test]
    fn test_metrics_sessions_query_debug() {
        let query = MetricsSessionsQuery {
            start_date: None,
            end_date: None,
            model: None,
            limit: None,
        };

        let debug_str = format!("{:?}", query);
        assert!(debug_str.contains("MetricsSessionsQuery"));
    }

    #[test]
    fn test_metrics_daily_query_debug() {
        let query = MetricsDailyQuery {
            days: Some(7),
            end_date: None,
            granularity: Some("daily".to_string()),
        };

        let debug_str = format!("{:?}", query);
        assert!(debug_str.contains("MetricsDailyQuery"));
    }

    #[test]
    fn test_forward_metrics_query_debug() {
        let query = ForwardMetricsQuery {
            start_date: None,
            end_date: None,
            endpoint: None,
            model: None,
            limit: None,
        };

        let debug_str = format!("{:?}", query);
        assert!(debug_str.contains("ForwardMetricsQuery"));
    }
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
    /// Total number of prompt-side cached tool outputs in chat sessions.
    pub prompt_cached_tool_outputs: u64,
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
    /// Prompt-side cached tool outputs observed in chat sessions on this date.
    pub prompt_cached_tool_outputs: u64,
}
