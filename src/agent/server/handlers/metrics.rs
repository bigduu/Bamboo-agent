use actix_web::{web, HttpResponse, Responder};
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};

use crate::state::AppState;

#[derive(Debug, Deserialize)]
pub struct MetricsSummaryQuery {
    pub start_date: Option<NaiveDate>,
    pub end_date: Option<NaiveDate>,
}

#[derive(Debug, Deserialize)]
pub struct MetricsSessionsQuery {
    pub start_date: Option<NaiveDate>,
    pub end_date: Option<NaiveDate>,
    pub model: Option<String>,
    pub limit: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct MetricsDailyQuery {
    pub days: Option<u32>,
    pub end_date: Option<NaiveDate>,
    pub granularity: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ForwardMetricsQuery {
    pub start_date: Option<NaiveDate>,
    pub end_date: Option<NaiveDate>,
    pub endpoint: Option<String>,
    pub model: Option<String>,
    pub limit: Option<u32>,
}

// ============================================================================
// Unified API Types (v2)
// ============================================================================

/// Unified summary combining chat and forward metrics
#[derive(Debug, Serialize)]
pub struct UnifiedSummary {
    pub chat: agent_metrics::MetricsSummary,
    pub forward: agent_metrics::ForwardMetricsSummary,
    pub combined: CombinedSummary,
}

#[derive(Debug, Serialize)]
pub struct CombinedSummary {
    pub total_requests: u64,
    pub total_tokens: u64,
    pub total_success: u64,
    pub total_errors: u64,
    pub success_rate: f64,
}

/// Unified timeline point combining chat and forward metrics
#[derive(Debug, Serialize)]
pub struct UnifiedTimelinePoint {
    pub date: String,
    pub chat_tokens: u64,
    pub chat_sessions: u32,
    pub forward_tokens: u64,
    pub forward_requests: u32,
    pub total_tokens: u64,
}

// ============================================================================
// Original Handlers
// ============================================================================

pub async fn summary(
    state: web::Data<AppState>,
    query: web::Query<MetricsSummaryQuery>,
) -> impl Responder {
    match state
        .metrics_service
        .summary(query.start_date, query.end_date)
        .await
    {
        Ok(summary) => HttpResponse::Ok().json(summary),
        Err(error) => internal_error(error),
    }
}

pub async fn by_model(
    state: web::Data<AppState>,
    query: web::Query<MetricsSummaryQuery>,
) -> impl Responder {
    match state
        .metrics_service
        .by_model(query.start_date, query.end_date)
        .await
    {
        Ok(data) => HttpResponse::Ok().json(data),
        Err(error) => internal_error(error),
    }
}

pub async fn sessions(
    state: web::Data<AppState>,
    query: web::Query<MetricsSessionsQuery>,
) -> impl Responder {
    let filter = agent_metrics::SessionMetricsFilter {
        start_date: query.start_date,
        end_date: query.end_date,
        model: query.model.clone(),
        limit: query.limit,
    };

    match state.metrics_service.sessions(filter).await {
        Ok(data) => HttpResponse::Ok().json(data),
        Err(error) => internal_error(error),
    }
}

pub async fn session_detail(state: web::Data<AppState>, path: web::Path<String>) -> impl Responder {
    let session_id = path.into_inner();
    match state.metrics_service.session_detail(&session_id).await {
        Ok(Some(detail)) => HttpResponse::Ok().json(detail),
        Ok(None) => HttpResponse::NotFound().json(serde_json::json!({
            "error": "Metrics for session not found",
            "session_id": session_id,
        })),
        Err(error) => internal_error(error),
    }
}

pub async fn daily(
    state: web::Data<AppState>,
    query: web::Query<MetricsDailyQuery>,
) -> impl Responder {
    let days = query.days.unwrap_or(30).clamp(1, 365);
    let granularity = query.granularity.as_deref().unwrap_or("daily");

    match granularity {
        "weekly" => match state.metrics_service.weekly(days, query.end_date).await {
            Ok(data) => HttpResponse::Ok().json(data),
            Err(error) => internal_error(error),
        },
        "monthly" => match state.metrics_service.monthly(days, query.end_date).await {
            Ok(data) => HttpResponse::Ok().json(data),
            Err(error) => internal_error(error),
        },
        _ => match state.metrics_service.daily(days, query.end_date).await {
            Ok(data) => HttpResponse::Ok().json(data),
            Err(error) => internal_error(error),
        },
    }
}

// Forward metrics handlers
pub async fn forward_summary(
    state: web::Data<AppState>,
    query: web::Query<ForwardMetricsQuery>,
) -> impl Responder {
    let filter = agent_metrics::ForwardMetricsFilter {
        start_date: query.start_date,
        end_date: query.end_date,
        endpoint: query.endpoint.clone(),
        model: query.model.clone(),
        limit: query.limit,
    };

    match state.metrics_service.forward_summary(filter).await {
        Ok(summary) => HttpResponse::Ok().json(summary),
        Err(error) => internal_error(error),
    }
}

pub async fn forward_by_endpoint(
    state: web::Data<AppState>,
    query: web::Query<ForwardMetricsQuery>,
) -> impl Responder {
    let filter = agent_metrics::ForwardMetricsFilter {
        start_date: query.start_date,
        end_date: query.end_date,
        endpoint: None, // Group by all endpoints
        model: query.model.clone(),
        limit: query.limit,
    };

    match state.metrics_service.forward_by_endpoint(filter).await {
        Ok(data) => HttpResponse::Ok().json(data),
        Err(error) => internal_error(error),
    }
}

pub async fn forward_requests(
    state: web::Data<AppState>,
    query: web::Query<ForwardMetricsQuery>,
) -> impl Responder {
    let filter = agent_metrics::ForwardMetricsFilter {
        start_date: query.start_date,
        end_date: query.end_date,
        endpoint: query.endpoint.clone(),
        model: query.model.clone(),
        limit: query.limit,
    };

    match state.metrics_service.forward_requests(filter).await {
        Ok(data) => HttpResponse::Ok().json(data),
        Err(error) => internal_error(error),
    }
}

fn internal_error(error: impl std::fmt::Display) -> HttpResponse {
    HttpResponse::InternalServerError().json(serde_json::json!({
        "error": error.to_string(),
    }))
}

// ============================================================================
// Unified API Handlers (v2)
// ============================================================================

/// GET /metrics/v2/summary - Combined chat and forward summary
pub async fn v2_unified_summary(
    state: web::Data<AppState>,
    query: web::Query<MetricsSummaryQuery>,
) -> impl Responder {
    let chat_result = state
        .metrics_service
        .summary(query.start_date, query.end_date)
        .await;

    let forward_result = state
        .metrics_service
        .forward_summary(agent_metrics::ForwardMetricsFilter {
            start_date: query.start_date,
            end_date: query.end_date,
            endpoint: None,
            model: None,
            limit: None,
        })
        .await;

    match (chat_result, forward_result) {
        (Ok(chat), Ok(forward)) => {
            let total_requests = chat.total_sessions + forward.total_requests;
            let total_tokens = chat.total_tokens.total_tokens + forward.total_tokens.total_tokens;
            let total_success = (chat.total_sessions - chat.active_sessions)
                + forward.successful_requests;
            let total_errors = forward.failed_requests;
            let success_rate = if total_requests > 0 {
                (total_success as f64 / total_requests as f64) * 100.0
            } else {
                0.0
            };

            let unified = UnifiedSummary {
                chat,
                forward,
                combined: CombinedSummary {
                    total_requests,
                    total_tokens,
                    total_success,
                    total_errors,
                    success_rate,
                },
            };

            HttpResponse::Ok().json(unified)
        }
        (Err(e), _) | (_, Err(e)) => internal_error(e),
    }
}

/// GET /metrics/v2/timeline - Combined timeline data
pub async fn v2_unified_timeline(
    state: web::Data<AppState>,
    query: web::Query<MetricsDailyQuery>,
) -> impl Responder {
    let days = query.days.unwrap_or(30).clamp(1, 365);

    let chat_result = state.metrics_service.daily(days, query.end_date).await;
    let forward_result = state
        .metrics_service
        .forward_daily(agent_metrics::ForwardMetricsFilter {
            start_date: None,
            end_date: query.end_date,
            endpoint: None,
            model: None,
            limit: Some(days),
        })
        .await;

    match (chat_result, forward_result) {
        (Ok(chat_daily), Ok(forward_daily)) => {
            // Build maps for efficient lookup
            let chat_map: std::collections::HashMap<String, &agent_metrics::DailyMetrics> =
                chat_daily
                    .iter()
                    .map(|d| (d.date.to_string(), d))
                    .collect();

            let forward_map: std::collections::HashMap<String, &agent_metrics::DailyMetrics> =
                forward_daily
                    .iter()
                    .map(|d| (d.date.to_string(), d))
                    .collect();

            // Get all unique dates
            let mut dates: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
            for date in chat_map.keys() {
                dates.insert(date.clone());
            }
            for date in forward_map.keys() {
                dates.insert(date.clone());
            }

            // Build unified timeline
            let timeline: Vec<UnifiedTimelinePoint> = dates
                .into_iter()
                .map(|date| {
                    let chat = chat_map.get(&date);
                    let forward = forward_map.get(&date);

                    let chat_tokens = chat
                        .map(|d| d.total_token_usage.total_tokens)
                        .unwrap_or(0);
                    let chat_sessions = chat.map(|d| d.total_sessions).unwrap_or(0);
                    let forward_tokens = forward
                        .map(|d| d.total_token_usage.total_tokens)
                        .unwrap_or(0);
                    let forward_requests = forward.map(|d| d.total_sessions).unwrap_or(0);

                    UnifiedTimelinePoint {
                        date: date.clone(),
                        chat_tokens,
                        chat_sessions,
                        forward_tokens,
                        forward_requests,
                        total_tokens: chat_tokens + forward_tokens,
                    }
                })
                .collect();

            HttpResponse::Ok().json(timeline)
        }
        (Err(e), _) | (_, Err(e)) => internal_error(e),
    }
}
