use chrono::NaiveDate;

use super::super::{
    ForwardMetricsQuery, MetricsDailyQuery, MetricsSessionsQuery, MetricsSummaryQuery,
};
use super::filters::{
    build_chat_timeline_filter, build_forward_filter, build_forward_grouped_filter,
    build_sessions_filter, build_summary_filter, normalize_days, resolve_timeline_granularity,
    TimelineGranularity,
};

#[test]
fn normalize_days_clamps_and_defaults() {
    assert_eq!(normalize_days(None), 30);
    assert_eq!(normalize_days(Some(0)), 1);
    assert_eq!(normalize_days(Some(30)), 30);
    assert_eq!(normalize_days(Some(999)), 365);
}

#[test]
fn resolve_timeline_granularity_defaults_to_daily_for_unknown_values() {
    assert_eq!(
        resolve_timeline_granularity(Some("weekly")),
        TimelineGranularity::Weekly
    );
    assert_eq!(
        resolve_timeline_granularity(Some("monthly")),
        TimelineGranularity::Monthly
    );
    assert_eq!(
        resolve_timeline_granularity(Some("unexpected")),
        TimelineGranularity::Daily
    );
    assert_eq!(
        resolve_timeline_granularity(None),
        TimelineGranularity::Daily
    );
}

#[test]
fn build_sessions_filter_copies_query_fields() {
    let query = MetricsSessionsQuery {
        start_date: Some(NaiveDate::from_ymd_opt(2026, 1, 1).expect("date")),
        end_date: Some(NaiveDate::from_ymd_opt(2026, 1, 31).expect("date")),
        model: Some("gpt-5".to_string()),
        limit: Some(50),
    };

    let filter = build_sessions_filter(&query);
    assert_eq!(filter.start_date, query.start_date);
    assert_eq!(filter.end_date, query.end_date);
    assert_eq!(filter.model, query.model);
    assert_eq!(filter.limit, query.limit);
}

#[test]
fn chat_summary_and_timeline_filters_trim_model_and_clear_blank_values() {
    let summary = build_summary_filter(&MetricsSummaryQuery {
        start_date: Some(NaiveDate::from_ymd_opt(2026, 3, 1).expect("date")),
        end_date: Some(NaiveDate::from_ymd_opt(2026, 3, 31).expect("date")),
        model: Some("  gpt-5  ".to_string()),
    });
    assert_eq!(summary.model.as_deref(), Some("gpt-5"));

    let timeline = build_chat_timeline_filter(&MetricsDailyQuery {
        days: Some(14),
        end_date: Some(NaiveDate::from_ymd_opt(2026, 3, 31).expect("date")),
        model: Some("   ".to_string()),
        granularity: Some("weekly".to_string()),
    });
    assert_eq!(timeline.days, 14);
    assert_eq!(timeline.model, None);
}

#[test]
fn build_forward_filters_handle_endpoint_for_grouping() {
    let query = ForwardMetricsQuery {
        start_date: Some(NaiveDate::from_ymd_opt(2026, 2, 1).expect("date")),
        end_date: Some(NaiveDate::from_ymd_opt(2026, 2, 28).expect("date")),
        endpoint: Some("gemini.generate_content".to_string()),
        model: Some("gpt-5".to_string()),
        limit: Some(100),
    };

    let standard = build_forward_filter(&query);
    assert_eq!(
        standard.endpoint.as_deref(),
        Some("gemini.generate_content")
    );
    assert_eq!(standard.model.as_deref(), Some("gpt-5"));

    let grouped = build_forward_grouped_filter(&query);
    assert_eq!(grouped.endpoint, None);
    assert_eq!(grouped.model.as_deref(), Some("gpt-5"));
}
