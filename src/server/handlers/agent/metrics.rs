use actix_web::{web, HttpResponse, Responder};
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};

use crate::server::app_state::AppState;

// ============================================================================
// Query Parameter Types
// ============================================================================

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

// ============================================================================
// Unified API Types (v2)
// ============================================================================

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

// ============================================================================
// Original Handlers
// ============================================================================

/// Gets chat metrics summary
///
/// # HTTP Route
/// `GET /metrics/summary`
///
/// # Query Parameters
/// - `start_date`: (Optional) Start date (YYYY-MM-DD)
/// - `end_date`: (Optional) End date (YYYY-MM-DD)
///
/// # Response Format
/// Returns [`MetricsSummary`](crate::agent::metrics::MetricsSummary):
/// ```json
/// {
///   "total_sessions": 100,
///   "active_sessions": 5,
///   "total_tokens": {
///     "prompt_tokens": 50000,
///     "completion_tokens": 30000,
///     "total_tokens": 80000
///   }
/// }
/// ```
///
/// # Response Status
/// - `200 OK`: Successfully retrieved summary
/// - `500 Internal Server Error`: Failed to retrieve metrics
///
/// # Example
/// ```bash
/// curl "http://localhost:3000/metrics/summary?start_date=2024-01-01&end_date=2024-01-31"
/// ```
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

/// Gets metrics grouped by model
///
/// # HTTP Route
/// `GET /metrics/by-model`
///
/// # Query Parameters
/// - `start_date`: (Optional) Start date (YYYY-MM-DD)
/// - `end_date`: (Optional) End date (YYYY-MM-DD)
///
/// # Response Format
/// Returns array of model metrics:
/// ```json
/// [
///   {
///     "model": "claude-3-5-sonnet-20241022",
///     "total_sessions": 50,
///     "total_tokens": 40000
///   }
/// ]
/// ```
///
/// # Response Status
/// - `200 OK`: Successfully retrieved metrics by model
/// - `500 Internal Server Error`: Failed to retrieve metrics
///
/// # Example
/// ```bash
/// curl "http://localhost:3000/metrics/by-model"
/// ```
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

/// Lists sessions with optional filters
///
/// # HTTP Route
/// `GET /metrics/sessions`
///
/// # Query Parameters
/// - `start_date`: (Optional) Filter sessions from this date
/// - `end_date`: (Optional) Filter sessions until this date
/// - `model`: (Optional) Filter by model name
/// - `limit`: (Optional) Maximum number of sessions to return
///
/// # Response Format
/// Returns array of session metrics:
/// ```json
/// [
///   {
///     "session_id": "session-123",
///     "model": "claude-3-5-sonnet-20241022",
///     "created_at": "2024-01-15T10:30:00Z",
///     "total_tokens": 1500
///   }
/// ]
/// ```
///
/// # Response Status
/// - `200 OK`: Successfully retrieved sessions
/// - `500 Internal Server Error`: Failed to retrieve sessions
///
/// # Example
/// ```bash
/// curl "http://localhost:3000/metrics/sessions?limit=10&model=claude-3-5-sonnet-20241022"
/// ```
pub async fn sessions(
    state: web::Data<AppState>,
    query: web::Query<MetricsSessionsQuery>,
) -> impl Responder {
    let filter = crate::agent::metrics::SessionMetricsFilter {
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

/// Gets detailed metrics for a specific session
///
/// # HTTP Route
/// `GET /metrics/sessions/{session_id}`
///
/// # Path Parameters
/// - `session_id`: Session identifier
///
/// # Response Format
/// Returns detailed session metrics:
/// ```json
/// {
///   "session_id": "session-123",
///   "model": "claude-3-5-sonnet-20241022",
///   "created_at": "2024-01-15T10:30:00Z",
///   "messages": [...],
///   "total_tokens": 1500
/// }
/// ```
///
/// # Response Status
/// - `200 OK`: Session found and returned
/// - `404 Not Found`: Session metrics not found
/// - `500 Internal Server Error`: Failed to retrieve session
///
/// # Example
/// ```bash
/// curl http://localhost:3000/metrics/sessions/session-123
/// ```
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

/// Gets daily/weekly/monthly metrics timeline
///
/// # HTTP Route
/// `GET /metrics/daily`
///
/// # Query Parameters
/// - `days`: (Optional) Number of days to include (default: 30, max: 365)
/// - `end_date`: (Optional) End date for the range
/// - `granularity`: (Optional) "daily", "weekly", or "monthly" (default: "daily")
///
/// # Response Format
/// Returns array of daily metrics:
/// ```json
/// [
///   {
///     "date": "2024-01-15",
///     "total_sessions": 10,
///     "total_token_usage": {
///       "prompt_tokens": 5000,
///       "completion_tokens": 3000,
///       "total_tokens": 8000
///     }
///   }
/// ]
/// ```
///
/// # Response Status
/// - `200 OK`: Successfully retrieved timeline
/// - `500 Internal Server Error`: Failed to retrieve metrics
///
/// # Example
/// ```bash
/// curl "http://localhost:3000/metrics/daily?days=7&granularity=daily"
/// ```
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

// ============================================================================
// Forward Metrics Handlers
// ============================================================================

/// Gets forward proxy metrics summary
///
/// # HTTP Route
/// `GET /metrics/forward/summary`
///
/// # Query Parameters
/// - `start_date`: (Optional) Start date for filtering
/// - `end_date`: (Optional) End date for filtering
/// - `endpoint`: (Optional) Filter by endpoint
/// - `model`: (Optional) Filter by model
/// - `limit`: (Optional) Maximum records to include
///
/// # Response Format
/// Returns forward metrics summary:
/// ```json
/// {
///   "total_requests": 1000,
///   "successful_requests": 950,
///   "failed_requests": 50,
///   "total_tokens": {
///     "prompt_tokens": 50000,
///     "completion_tokens": 30000,
///     "total_tokens": 80000
///   }
/// }
/// ```
///
/// # Response Status
/// - `200 OK`: Successfully retrieved summary
/// - `500 Internal Server Error`: Failed to retrieve metrics
///
/// # Example
/// ```bash
/// curl "http://localhost:3000/metrics/forward/summary"
/// ```
pub async fn forward_summary(
    state: web::Data<AppState>,
    query: web::Query<ForwardMetricsQuery>,
) -> impl Responder {
    let filter = crate::agent::metrics::ForwardMetricsFilter {
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

/// Gets forward metrics grouped by endpoint
///
/// # HTTP Route
/// `GET /metrics/forward/by-endpoint`
///
/// # Query Parameters
/// - `start_date`: (Optional) Start date for filtering
/// - `end_date`: (Optional) End date for filtering
/// - `model`: (Optional) Filter by model
/// - `limit`: (Optional) Maximum records to include
///
/// # Response Format
/// Returns array of endpoint metrics:
/// ```json
/// [
///   {
///     "endpoint": "/v1/chat/completions",
///     "total_requests": 500,
///     "total_tokens": 40000
///   }
/// ]
/// ```
///
/// # Response Status
/// - `200 OK`: Successfully retrieved endpoint metrics
/// - `500 Internal Server Error`: Failed to retrieve metrics
///
/// # Example
/// ```bash
/// curl "http://localhost:3000/metrics/forward/by-endpoint"
/// ```
pub async fn forward_by_endpoint(
    state: web::Data<AppState>,
    query: web::Query<ForwardMetricsQuery>,
) -> impl Responder {
    let filter = crate::agent::metrics::ForwardMetricsFilter {
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

/// Lists individual forward proxy requests
///
/// # HTTP Route
/// `GET /metrics/forward/requests`
///
/// # Query Parameters
/// - `start_date`: (Optional) Filter requests from this date
/// - `end_date`: (Optional) Filter requests until this date
/// - `endpoint`: (Optional) Filter by endpoint
/// - `model`: (Optional) Filter by model
/// - `limit`: (Optional) Maximum number of requests to return
///
/// # Response Format
/// Returns array of forward request records:
/// ```json
/// [
///   {
///     "request_id": "req-123",
///     "endpoint": "/v1/chat/completions",
///     "model": "gpt-4",
///     "timestamp": "2024-01-15T10:30:00Z",
///     "tokens": 1500,
///     "status": "success"
///   }
/// ]
/// ```
///
/// # Response Status
/// - `200 OK`: Successfully retrieved requests
/// - `500 Internal Server Error`: Failed to retrieve requests
///
/// # Example
/// ```bash
/// curl "http://localhost:3000/metrics/forward/requests?limit=10"
/// ```
pub async fn forward_requests(
    state: web::Data<AppState>,
    query: web::Query<ForwardMetricsQuery>,
) -> impl Responder {
    let filter = crate::agent::metrics::ForwardMetricsFilter {
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

/// Helper function to create internal error response
fn internal_error(error: impl std::fmt::Display) -> HttpResponse {
    HttpResponse::InternalServerError().json(serde_json::json!({
        "error": error.to_string(),
    }))
}

// ============================================================================
// Unified API Handlers (v2)
// ============================================================================

/// Gets unified metrics summary combining chat and forward data
///
/// # HTTP Route
/// `GET /metrics/v2/summary`
///
/// # Query Parameters
/// - `start_date`: (Optional) Start date (YYYY-MM-DD)
/// - `end_date`: (Optional) End date (YYYY-MM-DD)
///
/// # Response Format
/// Returns [`UnifiedSummary`] with combined metrics:
/// ```json
/// {
///   "chat": {
///     "total_sessions": 100,
///     "total_tokens": {...}
///   },
///   "forward": {
///     "total_requests": 500,
///     "total_tokens": {...}
///   },
///   "combined": {
///     "total_requests": 600,
///     "total_tokens": 120000,
///     "success_rate": 98.5
///   }
/// }
/// ```
///
/// # Response Status
/// - `200 OK`: Successfully retrieved unified summary
/// - `500 Internal Server Error`: Failed to retrieve metrics
///
/// # Example
/// ```bash
/// curl "http://localhost:3000/metrics/v2/summary"
/// ```
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
        .forward_summary(crate::agent::metrics::ForwardMetricsFilter {
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
            let total_success =
                (chat.total_sessions - chat.active_sessions) + forward.successful_requests;
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

/// Gets unified timeline combining chat and forward metrics
///
/// # HTTP Route
/// `GET /metrics/v2/timeline`
///
/// # Query Parameters
/// - `days`: (Optional) Number of days to include (default: 30, max: 365)
/// - `end_date`: (Optional) End date for the range
/// - `granularity`: (Optional) Ignored (always daily for now)
///
/// # Response Format
/// Returns array of [`UnifiedTimelinePoint`]:
/// ```json
/// [
///   {
///     "date": "2024-01-15",
///     "chat_tokens": 5000,
///     "chat_sessions": 10,
///     "forward_tokens": 3000,
///     "forward_requests": 20,
///     "total_tokens": 8000
///   }
/// ]
/// ```
///
/// # Response Status
/// - `200 OK`: Successfully retrieved timeline
/// - `500 Internal Server Error`: Failed to retrieve metrics
///
/// # Example
/// ```bash
/// curl "http://localhost:3000/metrics/v2/timeline?days=7"
/// ```
pub async fn v2_unified_timeline(
    state: web::Data<AppState>,
    query: web::Query<MetricsDailyQuery>,
) -> impl Responder {
    let days = query.days.unwrap_or(30).clamp(1, 365);

    let chat_result = state.metrics_service.daily(days, query.end_date).await;
    let forward_result = state
        .metrics_service
        .forward_daily(crate::agent::metrics::ForwardMetricsFilter {
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
            let chat_map: std::collections::HashMap<String, &crate::agent::metrics::DailyMetrics> =
                chat_daily.iter().map(|d| (d.date.to_string(), d)).collect();

            let forward_map: std::collections::HashMap<
                String,
                &crate::agent::metrics::DailyMetrics,
            > = forward_daily
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

                    let chat_tokens = chat.map(|d| d.total_token_usage.total_tokens).unwrap_or(0);
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
