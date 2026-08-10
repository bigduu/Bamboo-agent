use bamboo_metrics::MetricsDateFilter;

#[test]
fn legacy_metrics_date_filter_complete_struct_literal_still_compiles() {
    let filter = MetricsDateFilter {
        start_date: None,
        end_date: None,
    };

    assert!(filter.start_date.is_none());
    assert!(filter.end_date.is_none());
}
