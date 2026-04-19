use super::super::{ForwardMetricsQuery, MetricsSessionsQuery};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TimelineGranularity {
    Daily,
    Weekly,
    Monthly,
}

pub(super) fn build_sessions_filter(
    query: &MetricsSessionsQuery,
) -> bamboo_engine::SessionMetricsFilter {
    bamboo_engine::SessionMetricsFilter {
        start_date: query.start_date,
        end_date: query.end_date,
        model: query.model.clone(),
        limit: query.limit,
    }
}

pub(super) fn build_forward_filter(
    query: &ForwardMetricsQuery,
) -> bamboo_engine::ForwardMetricsFilter {
    bamboo_engine::ForwardMetricsFilter {
        start_date: query.start_date,
        end_date: query.end_date,
        endpoint: query.endpoint.clone(),
        model: query.model.clone(),
        limit: query.limit,
    }
}

pub(super) fn build_forward_grouped_filter(
    query: &ForwardMetricsQuery,
) -> bamboo_engine::ForwardMetricsFilter {
    bamboo_engine::ForwardMetricsFilter {
        start_date: query.start_date,
        end_date: query.end_date,
        endpoint: None,
        model: query.model.clone(),
        limit: query.limit,
    }
}

pub(super) fn normalize_days(days: Option<u32>) -> u32 {
    days.unwrap_or(30).clamp(1, 365)
}

pub(super) fn resolve_timeline_granularity(value: Option<&str>) -> TimelineGranularity {
    match value.unwrap_or("daily") {
        "weekly" => TimelineGranularity::Weekly,
        "monthly" => TimelineGranularity::Monthly,
        _ => TimelineGranularity::Daily,
    }
}
