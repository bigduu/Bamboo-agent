use actix_web::{web, HttpResponse, Responder};

use super::{
    internal_error, CombinedSummary, MetricsDailyQuery, MetricsSummaryQuery, UnifiedSummary,
    UnifiedTimelinePoint,
};
use crate::server::app_state::AppState;

/// Gets unified metrics summary combining chat and forward data
///
/// # HTTP Route
/// `GET /metrics/v2/summary`
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
            // Build maps for efficient lookup.
            let chat_map: std::collections::HashMap<String, &crate::agent::metrics::DailyMetrics> =
                chat_daily.iter().map(|d| (d.date.to_string(), d)).collect();

            let forward_map: std::collections::HashMap<
                String,
                &crate::agent::metrics::DailyMetrics,
            > = forward_daily
                .iter()
                .map(|d| (d.date.to_string(), d))
                .collect();

            // Get all unique dates.
            let mut dates: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
            for date in chat_map.keys() {
                dates.insert(date.clone());
            }
            for date in forward_map.keys() {
                dates.insert(date.clone());
            }

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
