use actix_web::{web, HttpResponse, Responder};

use super::super::{internal_error, ForwardMetricsQuery};
use super::filters::{build_forward_filter, build_forward_grouped_filter};
use crate::server::app_state::AppState;

/// Gets forward proxy metrics summary
///
/// # HTTP Route
/// `GET /metrics/forward/summary`
pub async fn forward_summary(
    state: web::Data<AppState>,
    query: web::Query<ForwardMetricsQuery>,
) -> impl Responder {
    let filter = build_forward_filter(&query);

    match state.metrics_service.forward_summary(filter).await {
        Ok(summary) => HttpResponse::Ok().json(summary),
        Err(error) => internal_error(error),
    }
}

/// Gets forward metrics grouped by endpoint
///
/// # HTTP Route
/// `GET /metrics/forward/by-endpoint`
pub async fn forward_by_endpoint(
    state: web::Data<AppState>,
    query: web::Query<ForwardMetricsQuery>,
) -> impl Responder {
    let filter = build_forward_grouped_filter(&query);

    match state.metrics_service.forward_by_endpoint(filter).await {
        Ok(data) => HttpResponse::Ok().json(data),
        Err(error) => internal_error(error),
    }
}

/// Lists individual forward proxy requests
///
/// # HTTP Route
/// `GET /metrics/forward/requests`
pub async fn forward_requests(
    state: web::Data<AppState>,
    query: web::Query<ForwardMetricsQuery>,
) -> impl Responder {
    let filter = build_forward_filter(&query);

    match state.metrics_service.forward_requests(filter).await {
        Ok(data) => HttpResponse::Ok().json(data),
        Err(error) => internal_error(error),
    }
}
