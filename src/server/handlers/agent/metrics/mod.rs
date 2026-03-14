use actix_web::HttpResponse;

mod core_handlers;
mod types;
mod unified_handlers;

pub use core_handlers::{
    by_model, daily, forward_by_endpoint, forward_requests, forward_summary, session_detail,
    sessions, summary,
};
pub use types::{
    CombinedSummary, ForwardMetricsQuery, MetricsDailyQuery, MetricsSessionsQuery,
    MetricsSummaryQuery, UnifiedSummary, UnifiedTimelinePoint,
};
pub use unified_handlers::{v2_unified_summary, v2_unified_timeline};

fn internal_error(error: impl std::fmt::Display) -> HttpResponse {
    HttpResponse::InternalServerError().json(serde_json::json!({
        "error": error.to_string(),
    }))
}
