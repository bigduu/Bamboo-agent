use actix_web::{web, HttpResponse, Responder};

use crate::app_state::AppState;

/// Gets bounded, aggregate session-persistence latency and queue metrics.
///
/// Session ids are never metric labels. Operators can correlate an individual
/// slow save through the `bamboo.session_persistence` structured trace target.
///
/// # HTTP Route
/// `GET /api/v1/metrics/persistence`
pub async fn persistence(state: web::Data<AppState>) -> impl Responder {
    HttpResponse::Ok().json(state.session_store.persistence_metrics())
}
