use std::collections::{BTreeMap, BTreeSet};

use actix_web::{web, HttpResponse, Responder};
use bamboo_metrics::{DailyMetrics, PeriodMetrics, TokenUsage};

use super::{
    core_handlers::filters::{
        build_chat_timeline_filter, build_summary_filter, resolve_timeline_granularity,
        TimelineGranularity,
    },
    internal_error, CombinedSummary, MemoryMetricsQuery, MetricsDailyQuery, MetricsSummaryQuery,
    UnifiedSummary, UnifiedTimelinePoint,
};
use crate::app_state::AppState;
use bamboo_memory::memory_store::MemoryStore;

use super::core_handlers::memory::build_memory_summary;

fn build_unified_summary_filters(
    query: &MetricsSummaryQuery,
) -> (
    bamboo_metrics::ModelMetricsDateFilter,
    bamboo_metrics::ForwardMetricsFilter,
) {
    let chat = build_summary_filter(query);
    let forward = bamboo_metrics::ForwardMetricsFilter {
        start_date: chat.start_date,
        end_date: chat.end_date,
        endpoint: None,
        model: chat.model.clone(),
        limit: None,
    };
    (chat, forward)
}

fn build_unified_timeline_filters(
    query: &MetricsDailyQuery,
) -> (
    super::core_handlers::filters::ChatTimelineFilter,
    bamboo_metrics::ForwardMetricsFilter,
) {
    let chat = build_chat_timeline_filter(query);
    let forward = bamboo_metrics::ForwardMetricsFilter {
        start_date: None,
        end_date: chat.end_date,
        endpoint: None,
        model: chat.model.clone(),
        limit: Some(chat.days),
    };
    (chat, forward)
}

/// Gets unified metrics summary combining chat and forward data
///
/// # HTTP Route
/// `GET /metrics/v2/summary`
pub async fn v2_unified_summary(
    state: web::Data<AppState>,
    query: web::Query<MetricsSummaryQuery>,
) -> impl Responder {
    let (chat_filter, forward_filter) = build_unified_summary_filters(&query);
    let chat_result = state.metrics_service.summary_filtered(chat_filter).await;

    let forward_result = state.metrics_service.forward_summary(forward_filter).await;

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
            let total_errors =
                chat.error_sessions + chat.cancelled_sessions + forward.failed_requests;
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
    let (filter, forward_filter) = build_unified_timeline_filters(&query);
    let granularity = resolve_timeline_granularity(query.granularity.as_deref());

    let chat_result = state
        .metrics_service
        .daily_for_model(filter.days, filter.end_date, filter.model.clone())
        .await;
    let forward_result = state.metrics_service.forward_daily(forward_filter).await;

    match (chat_result, forward_result) {
        (Ok(chat_daily), Ok(forward_daily)) => HttpResponse::Ok().json(build_unified_timeline(
            chat_daily,
            forward_daily,
            granularity,
        )),
        (Err(e), _) | (_, Err(e)) => internal_error(e),
    }
}

fn build_unified_timeline(
    chat_daily: Vec<DailyMetrics>,
    forward_daily: Vec<DailyMetrics>,
    granularity: TimelineGranularity,
) -> Vec<UnifiedTimelinePoint> {
    let (chat_daily, forward_daily) = align_daily_inputs(chat_daily, forward_daily);

    match granularity {
        TimelineGranularity::Daily => chat_daily
            .into_iter()
            .zip(forward_daily)
            .map(|(chat, forward)| unified_point(chat.date.to_string(), None, chat, forward))
            .collect(),
        TimelineGranularity::Weekly => merge_periods(
            bamboo_metrics::aggregate_weekly(&chat_daily),
            bamboo_metrics::aggregate_weekly(&forward_daily),
        ),
        TimelineGranularity::Monthly => merge_periods(
            bamboo_metrics::aggregate_monthly(&chat_daily),
            bamboo_metrics::aggregate_monthly(&forward_daily),
        ),
    }
}

/// Give both sources the same ordered date domain before aggregation.
///
/// Filling a missing source with a zero-valued day makes the existing metrics
/// period aggregator the single boundary authority for both chat and forward.
/// In particular, sparse activity on different dates can no longer produce
/// different `period_end` values for the two sides of one unified bucket.
fn align_daily_inputs(
    chat_daily: Vec<DailyMetrics>,
    forward_daily: Vec<DailyMetrics>,
) -> (Vec<DailyMetrics>, Vec<DailyMetrics>) {
    let mut chat_by_date = chat_daily
        .into_iter()
        .map(|metrics| (metrics.date, metrics))
        .collect::<BTreeMap<_, _>>();
    let mut forward_by_date = forward_daily
        .into_iter()
        .map(|metrics| (metrics.date, metrics))
        .collect::<BTreeMap<_, _>>();
    let dates = chat_by_date
        .keys()
        .chain(forward_by_date.keys())
        .copied()
        .collect::<BTreeSet<_>>();

    dates
        .into_iter()
        .map(|date| {
            (
                chat_by_date
                    .remove(&date)
                    .unwrap_or_else(|| empty_daily_metrics(date)),
                forward_by_date
                    .remove(&date)
                    .unwrap_or_else(|| empty_daily_metrics(date)),
            )
        })
        .unzip()
}

fn merge_periods(
    chat_periods: Vec<PeriodMetrics>,
    forward_periods: Vec<PeriodMetrics>,
) -> Vec<UnifiedTimelinePoint> {
    chat_periods
        .into_iter()
        .zip(forward_periods)
        .map(|(chat, forward)| {
            debug_assert_eq!(chat.period_start, forward.period_start);
            debug_assert_eq!(chat.period_end, forward.period_end);

            let period_start = chat.period_start.to_string();
            let period_end = chat.period_end.to_string();
            unified_point(
                chat.label.clone(),
                Some((period_start, period_end)),
                period_as_daily(chat),
                period_as_daily(forward),
            )
        })
        .collect()
}

fn unified_point(
    date: String,
    period: Option<(String, String)>,
    chat: DailyMetrics,
    forward: DailyMetrics,
) -> UnifiedTimelinePoint {
    let chat_tokens = chat.total_token_usage.total_tokens;
    let forward_tokens = forward.total_token_usage.total_tokens;
    let (period_start, period_end) = period
        .map(|(start, end)| (Some(start), Some(end)))
        .unwrap_or((None, None));

    UnifiedTimelinePoint {
        date,
        period_start,
        period_end,
        chat_tokens,
        chat_sessions: chat.total_sessions,
        forward_tokens,
        forward_requests: forward.total_sessions,
        total_tokens: chat_tokens + forward_tokens,
        prompt_cached_tool_outputs: chat.prompt_cached_tool_outputs,
    }
}

fn period_as_daily(period: PeriodMetrics) -> DailyMetrics {
    DailyMetrics {
        date: period.period_start,
        total_sessions: period.total_sessions,
        total_rounds: period.total_rounds,
        total_token_usage: period.total_token_usage,
        total_tool_calls: period.total_tool_calls,
        prompt_cached_tool_outputs: period.prompt_cached_tool_outputs,
        model_breakdown: period.model_breakdown,
        tool_breakdown: period.tool_breakdown,
    }
}

fn empty_daily_metrics(date: chrono::NaiveDate) -> DailyMetrics {
    DailyMetrics {
        date,
        total_sessions: 0,
        total_rounds: 0,
        total_token_usage: TokenUsage::default(),
        total_tool_calls: 0,
        prompt_cached_tool_outputs: 0,
        model_breakdown: Default::default(),
        tool_breakdown: Default::default(),
    }
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;

    use super::*;

    fn daily(date: (i32, u32, u32), requests: u32, tokens: u64) -> DailyMetrics {
        DailyMetrics {
            date: NaiveDate::from_ymd_opt(date.0, date.1, date.2).expect("valid fixture date"),
            total_sessions: requests,
            total_rounds: requests,
            total_token_usage: TokenUsage {
                prompt_tokens: tokens,
                completion_tokens: 0,
                total_tokens: tokens,
            },
            total_tool_calls: 0,
            prompt_cached_tool_outputs: requests.into(),
            model_breakdown: Default::default(),
            tool_breakdown: Default::default(),
        }
    }

    #[test]
    fn unified_handlers_apply_one_normalized_model_to_chat_and_forward_sources() {
        let summary_query = MetricsSummaryQuery {
            start_date: Some(NaiveDate::from_ymd_opt(2099, 2, 1).expect("date")),
            end_date: Some(NaiveDate::from_ymd_opt(2099, 2, 28).expect("date")),
            model: Some("  model-a  ".to_string()),
        };
        let (chat_summary, forward_summary) = build_unified_summary_filters(&summary_query);
        assert_eq!(chat_summary.model.as_deref(), Some("model-a"));
        assert_eq!(forward_summary.model, chat_summary.model);

        let timeline_query = MetricsDailyQuery {
            days: Some(7),
            end_date: summary_query.end_date,
            model: Some("model-a".to_string()),
            granularity: Some("weekly".to_string()),
        };
        let (chat_timeline, forward_timeline) = build_unified_timeline_filters(&timeline_query);
        assert_eq!(chat_timeline.model.as_deref(), Some("model-a"));
        assert_eq!(forward_timeline.model, chat_timeline.model);
        assert_eq!(forward_timeline.limit, Some(chat_timeline.days));

        let cleared_query = MetricsDailyQuery {
            days: None,
            end_date: None,
            model: Some("   ".to_string()),
            granularity: None,
        };
        let (cleared_chat, cleared_forward) = build_unified_timeline_filters(&cleared_query);
        assert_eq!(cleared_chat.model, None);
        assert_eq!(cleared_forward.model, None);
    }

    #[test]
    fn daily_timeline_preserves_schema_and_combines_sources_by_date() {
        let timeline = build_unified_timeline(
            vec![daily((2099, 2, 1), 1, 10)],
            vec![daily((2099, 2, 2), 2, 20)],
            TimelineGranularity::Daily,
        );

        assert_eq!(timeline.len(), 2);
        assert_eq!(timeline[0].date, "2099-02-01");
        assert_eq!(timeline[0].chat_tokens, 10);
        assert_eq!(timeline[0].forward_tokens, 0);
        assert_eq!(timeline[1].chat_tokens, 0);
        assert_eq!(timeline[1].forward_tokens, 20);
        assert!(timeline.iter().all(|point| point.period_start.is_none()));
        assert!(timeline.iter().all(|point| point.period_end.is_none()));

        let json = serde_json::to_value(&timeline[0]).expect("serialize daily point");
        assert!(json.get("period_start").is_none());
        assert!(json.get("period_end").is_none());
    }

    #[test]
    fn weekly_timeline_uses_one_boundary_contract_for_sparse_sources() {
        // These dates are Saturday and Sunday in the same Monday-based week.
        let timeline = build_unified_timeline(
            vec![daily((2099, 1, 31), 1, 10)],
            vec![daily((2099, 2, 1), 2, 20)],
            TimelineGranularity::Weekly,
        );

        assert_eq!(timeline.len(), 1);
        assert_eq!(timeline[0].date, "2099-01-26..2099-02-01");
        assert_eq!(timeline[0].period_start.as_deref(), Some("2099-01-26"));
        assert_eq!(timeline[0].period_end.as_deref(), Some("2099-02-01"));
        assert_eq!(timeline[0].chat_sessions, 1);
        assert_eq!(timeline[0].forward_requests, 2);
        assert_eq!(timeline[0].total_tokens, 30);
    }
}
