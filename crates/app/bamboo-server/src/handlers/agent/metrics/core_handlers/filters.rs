use super::super::{ForwardMetricsQuery, MetricsSessionsQuery};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::handlers::agent::metrics) enum TimelineGranularity {
    Daily,
    Weekly,
    Monthly,
}

pub(super) fn build_sessions_filter(
    query: &MetricsSessionsQuery,
) -> bamboo_metrics::SessionMetricsFilter {
    bamboo_metrics::SessionMetricsFilter {
        start_date: query.start_date,
        end_date: query.end_date,
        model: query.model.clone(),
        limit: normalize_limit(query.limit),
    }
}

pub(super) fn build_forward_filter(
    query: &ForwardMetricsQuery,
) -> bamboo_metrics::ForwardMetricsFilter {
    bamboo_metrics::ForwardMetricsFilter {
        start_date: query.start_date,
        end_date: query.end_date,
        endpoint: query.endpoint.clone(),
        model: query.model.clone(),
        limit: normalize_limit(query.limit),
    }
}

pub(super) fn build_forward_grouped_filter(
    query: &ForwardMetricsQuery,
) -> bamboo_metrics::ForwardMetricsFilter {
    bamboo_metrics::ForwardMetricsFilter {
        start_date: query.start_date,
        end_date: query.end_date,
        endpoint: None,
        model: query.model.clone(),
        limit: normalize_limit(query.limit),
    }
}

pub(in crate::handlers::agent::metrics) fn normalize_days(days: Option<u32>) -> u32 {
    days.unwrap_or(30).clamp(1, 365)
}

/// Default and hard cap for metrics list `limit`. A client omitting `limit`
/// previously produced an unbounded query / full-table scan (#252); always
/// resolve to a bounded value (mirrors [`normalize_days`]).
const DEFAULT_METRICS_LIMIT: u32 = 100;
const MAX_METRICS_LIMIT: u32 = 1000;

pub(super) fn normalize_limit(limit: Option<u32>) -> Option<u32> {
    Some(
        limit
            .unwrap_or(DEFAULT_METRICS_LIMIT)
            .clamp(1, MAX_METRICS_LIMIT),
    )
}

pub(in crate::handlers::agent::metrics) fn resolve_timeline_granularity(
    value: Option<&str>,
) -> TimelineGranularity {
    match value.unwrap_or("daily") {
        "weekly" => TimelineGranularity::Weekly,
        "monthly" => TimelineGranularity::Monthly,
        _ => TimelineGranularity::Daily,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_limit_defaults_and_caps() {
        // Omitted → a bounded default, not an unbounded query (#252).
        assert_eq!(normalize_limit(None), Some(DEFAULT_METRICS_LIMIT));
        // In-range passes through.
        assert_eq!(normalize_limit(Some(50)), Some(50));
        // Over the cap is clamped down; zero is clamped up to 1.
        assert_eq!(normalize_limit(Some(1_000_000)), Some(MAX_METRICS_LIMIT));
        assert_eq!(normalize_limit(Some(0)), Some(1));
        // Never returns None (the unbounded case).
        assert!(normalize_limit(None).is_some());
    }
}
