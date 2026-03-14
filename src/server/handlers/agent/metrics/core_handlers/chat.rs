use actix_web::{web, HttpResponse, Responder};

use super::super::{internal_error, MetricsDailyQuery, MetricsSessionsQuery, MetricsSummaryQuery};
use super::filters::{
    build_sessions_filter, normalize_days, resolve_timeline_granularity, TimelineGranularity,
};
use crate::server::app_state::AppState;

/// Gets chat metrics summary
///
/// # HTTP Route
/// `GET /metrics/summary`
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
pub async fn sessions(
    state: web::Data<AppState>,
    query: web::Query<MetricsSessionsQuery>,
) -> impl Responder {
    let filter = build_sessions_filter(&query);

    match state.metrics_service.sessions(filter).await {
        Ok(data) => HttpResponse::Ok().json(data),
        Err(error) => internal_error(error),
    }
}

/// Gets detailed metrics for a specific session
///
/// # HTTP Route
/// `GET /metrics/sessions/{session_id}`
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
pub async fn daily(
    state: web::Data<AppState>,
    query: web::Query<MetricsDailyQuery>,
) -> impl Responder {
    let days = normalize_days(query.days);

    match resolve_timeline_granularity(query.granularity.as_deref()) {
        TimelineGranularity::Weekly => {
            match state.metrics_service.weekly(days, query.end_date).await {
                Ok(data) => HttpResponse::Ok().json(data),
                Err(error) => internal_error(error),
            }
        }
        TimelineGranularity::Monthly => {
            match state.metrics_service.monthly(days, query.end_date).await {
                Ok(data) => HttpResponse::Ok().json(data),
                Err(error) => internal_error(error),
            }
        }
        TimelineGranularity::Daily => match state.metrics_service.daily(days, query.end_date).await
        {
            Ok(data) => HttpResponse::Ok().json(data),
            Err(error) => internal_error(error),
        },
    }
}
