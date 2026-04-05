use actix_web::HttpResponse;

mod core_handlers;
mod types;
mod unified_handlers;

pub use core_handlers::{
    by_model, daily, forward_by_endpoint, forward_requests, forward_summary, memory_summary,
    memory_timeline, session_detail, sessions, summary, usage_breakdown,
};
pub use types::{
    CombinedSummary, ForwardMetricsQuery, McpServerUsageItem, McpToolUsageItem, MemoryMetricsQuery,
    MemoryMetricsSummary, MemoryTimelinePoint, MetricsDailyQuery, MetricsSessionsQuery,
    MetricsSummaryQuery, MetricsUsageBreakdownResponse, MetricsUsageQuery, SkillUsageItem,
    UnifiedSummary, UnifiedTimelinePoint, UsageCountItem,
};
pub use unified_handlers::{v2_unified_summary, v2_unified_timeline};

fn internal_error(error: impl std::fmt::Display) -> HttpResponse {
    HttpResponse::InternalServerError().json(serde_json::json!({
        "error": error.to_string(),
    }))
}
