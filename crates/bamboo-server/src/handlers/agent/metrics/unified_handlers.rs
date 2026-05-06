use actix_web::{web, HttpResponse, Responder};

use super::{
    internal_error, CombinedSummary, MemoryMetricsQuery, MetricsDailyQuery, MetricsSummaryQuery,
    UnifiedSummary, UnifiedTimelinePoint,
};
use crate::app_state::AppState;
use bamboo_memory::memory_store::MemoryStore;

use super::core_handlers::memory::build_memory_summary;

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
        .forward_summary(bamboo_engine::ForwardMetricsFilter {
            start_date: query.start_date,
            end_date: query.end_date,
            endpoint: None,
            model: None,
            limit: None,
        })
        .await;

    let memory_store = MemoryStore::new(state.app_data_dir.clone());
    let memory_result = build_memory_summary(
        &memory_store,
        &state.session_store,
        state.storage.as_ref(),
        &MemoryMetricsQuery {
            scope: None,
            project_key: None,
            days: None,
            end_date: None,
            granularity: None,
        },
    )
    .await;

    match (chat_result, forward_result, memory_result) {
        (Ok(chat), Ok(forward), Ok(memory)) => {
            let prompt_cached_tool_outputs = chat.prompt_cached_tool_outputs;
            let total_sync_mismatches = chat.total_sync_mismatches;
            let total_requests = chat.total_sessions + forward.total_requests;
            let total_tokens = chat.total_tokens.total_tokens + forward.total_tokens.total_tokens;
            let total_success = chat.completed_sessions + forward.successful_requests;
            let total_errors = chat.error_sessions + chat.cancelled_sessions + forward.failed_requests;
            let resolved_total = total_success + total_errors;
            let success_rate = if resolved_total > 0 {
                (total_success as f64 / resolved_total as f64) * 100.0
            } else {
                0.0
            };

            let unified = UnifiedSummary {
                chat: chat.clone(),
                forward,
                combined: CombinedSummary {
                    total_requests,
                    total_tokens,
                    total_success,
                    total_errors,
                    success_rate,
                    prompt_cached_tool_outputs,
                    total_compression_events: Some(chat.total_compression_events),
                    total_tokens_saved: Some(chat.total_tokens_saved),
                    total_sync_mismatches,
                },
                memory,
            };

            HttpResponse::Ok().json(unified)
        }
        (Err(error), _, _) => internal_error(error),
        (_, Err(error), _) => internal_error(error),
        (_, _, Err(error)) => internal_error(error),
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
        .forward_daily(bamboo_engine::ForwardMetricsFilter {
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
            let chat_map: std::collections::HashMap<String, &bamboo_engine::DailyMetrics> =
                chat_daily.iter().map(|d| (d.date.to_string(), d)).collect();

            let forward_map: std::collections::HashMap<String, &bamboo_engine::DailyMetrics> =
                forward_daily
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
                        prompt_cached_tool_outputs: chat
                            .map(|d| d.prompt_cached_tool_outputs)
                            .unwrap_or(0),
                    }
                })
                .collect();

            HttpResponse::Ok().json(timeline)
        }
        (Err(e), _) | (_, Err(e)) => internal_error(e),
    }
}
