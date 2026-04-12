use std::collections::BTreeMap;

use chrono::NaiveDate;
use serde::{Deserialize, Serialize};

use crate::agent::core::memory_store::MemoryScope;

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

/// Query parameters for usage breakdown requests
#[derive(Debug, Deserialize)]
pub struct MetricsUsageQuery {
    /// Start date for filtering sessions/events
    pub start_date: Option<NaiveDate>,
    /// End date for filtering sessions/events
    pub end_date: Option<NaiveDate>,
    /// Filter by model name
    pub model: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UsageCountItem {
    pub name: String,
    pub count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SkillUsageItem {
    pub skill_id: String,
    pub count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct McpServerUsageItem {
    pub server_id: String,
    pub count: u64,
    pub unique_tools: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct McpToolUsageItem {
    pub alias: String,
    pub server_id: String,
    pub tool_name: String,
    pub count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetricsUsageBreakdownResponse {
    pub total_sessions: u64,
    pub total_tool_calls: u64,
    pub core_tool_calls: u64,
    pub skill_load_calls: u64,
    pub mcp_calls: u64,
    pub unique_skills: u64,
    pub unique_mcp_servers: u64,
    pub unique_mcp_tools: u64,
    pub sessions_with_skill_loads: u64,
    pub sessions_with_mcp_calls: u64,
    #[serde(default)]
    pub top_core_tools: Vec<UsageCountItem>,
    #[serde(default)]
    pub top_skills: Vec<SkillUsageItem>,
    #[serde(default)]
    pub top_mcp_servers: Vec<McpServerUsageItem>,
    #[serde(default)]
    pub top_mcp_tools: Vec<McpToolUsageItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct PromptMemoryMetricsSummary {
    /// Number of sessions that recorded prompt-memory observability.
    pub observed_sessions: u64,
    /// Sessions where project memory index was loaded/loaded_truncated.
    pub project_memory_index_hits: u64,
    /// Sessions where relevant durable memories were rendered.
    pub relevant_memory_hits: u64,
    /// Sessions where rerank actually succeeded and changed recall strategy.
    pub relevant_memory_reranked_hits: u64,
    /// Sessions where rerank was enabled for the round.
    pub relevant_memory_rerank_enabled_sessions: u64,
    /// Sessions where relevant recall fell back to lexical after rerank attempt.
    pub relevant_memory_rerank_fallbacks: u64,
    /// Sessions with global Dream fallback injected.
    pub global_dream_fallback_hits: u64,
    /// Sessions with project Dream injected.
    pub project_dream_hits: u64,
    /// Sessions that surfaced a context-pressure warning in external memory.
    pub context_pressure_warning_hits: u64,
    /// Total relevant recalled memories rendered across observed sessions.
    pub total_relevant_memory_count: u64,
    /// Average relevant recalled memories rendered per observed session.
    pub avg_relevant_memory_count: u64,
    /// Average chars rendered for the relevant-memory section.
    pub avg_relevant_memory_section_chars: u64,
    /// Average chars rendered for the external-memory section.
    pub avg_external_memory_section_chars: u64,
    /// Breakdown of relevant recall status values.
    #[serde(default)]
    pub relevant_memory_status_breakdown: BTreeMap<String, u64>,
    /// Breakdown of dream source values.
    #[serde(default)]
    pub dream_source_breakdown: BTreeMap<String, u64>,
    /// Breakdown of resolved prompt-memory project/global scope usage.
    #[serde(default)]
    pub resolved_scope_breakdown: BTreeMap<String, u64>,
}

/// Query parameters for memory metrics summary and timeline requests
#[derive(Debug, Deserialize)]
pub struct MemoryMetricsQuery {
    /// Optional scope filter. When omitted, aggregate across global + project durable memory.
    pub scope: Option<MemoryScope>,
    /// Optional project key for project scope queries.
    pub project_key: Option<String>,
    /// Number of days to include when requesting timeline views.
    pub days: Option<u32>,
    /// End date for timeline aggregation.
    pub end_date: Option<NaiveDate>,
    /// Granularity: "daily", "weekly", or "monthly".
    pub granularity: Option<String>,
}

/// Aggregated durable memory summary for dashboard display.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemoryMetricsSummary {
    /// Scope requested by the caller, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<MemoryScope>,
    /// Project key requested by the caller, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_key: Option<String>,
    /// Total number of durable memories across the selected scope(s).
    pub total_memories: u64,
    /// Total number of stale candidates across the selected scope(s).
    pub stale_candidate_count: u64,
    /// Number of tracked project scopes contributing to this summary.
    pub project_count: u64,
    /// Breakdown of memories by durable memory type.
    #[serde(default)]
    pub by_type: BTreeMap<String, u64>,
    /// Breakdown of memories by durable memory status.
    #[serde(default)]
    pub by_status: BTreeMap<String, u64>,
    /// Breakdown of memories by scope label.
    #[serde(default)]
    pub by_scope: BTreeMap<String, u64>,
    /// Latest observed reindex timestamp across the selected scope(s).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_reindex_at: Option<String>,
    /// Latest observed dream timestamp across the selected scope(s).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_dream_at: Option<String>,
    /// Aggregated prompt-memory observability derived from persisted sessions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_memory: Option<PromptMemoryMetricsSummary>,
}

/// Timeline point for durable memory activity and inventory trends.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemoryTimelinePoint {
    /// Label shown in the chart.
    pub label: String,
    /// Period start in YYYY-MM-DD format.
    pub period_start: String,
    /// Period end in YYYY-MM-DD format.
    pub period_end: String,
    /// Number of memories created in the period.
    pub created_memories: u64,
    /// Number of memories updated in the period.
    pub updated_memories: u64,
    /// Running total memories observed by the end of the period.
    pub total_memories: u64,
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
    /// Current durable memory summary for dashboard display.
    pub memory: MemoryMetricsSummary,
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
    /// Total number of execute sync mismatches observed in the selected range.
    pub total_sync_mismatches: u64,
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
