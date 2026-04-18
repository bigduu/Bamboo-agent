use chrono::{DateTime, Utc};

use bamboo_application_metrics::{MetricsCollector, SessionStatus as MetricsSessionStatus};

pub(in crate::runner) fn record_session_started(
    metrics_collector: Option<&MetricsCollector>,
    session_id: &str,
    model_name: &str,
    created_at: DateTime<Utc>,
    message_count: u32,
) {
    let Some(metrics) = metrics_collector else {
        return;
    };

    metrics.session_started(session_id.to_string(), model_name.to_string(), created_at);
    metrics.session_message_count(session_id.to_string(), message_count, Utc::now());
}

pub(in crate::runner) fn record_session_cancelled(
    metrics_collector: Option<&MetricsCollector>,
    session_id: &str,
    message_count: u32,
) {
    let Some(metrics) = metrics_collector else {
        return;
    };
    metrics.session_message_count(session_id.to_string(), message_count, Utc::now());
    metrics.session_completed(
        session_id.to_string(),
        MetricsSessionStatus::Cancelled,
        Utc::now(),
    );
}

pub(in crate::runner) fn record_session_completed_if_resolved(
    metrics_collector: Option<&MetricsCollector>,
    session_id: &str,
    message_count: u32,
    has_pending_question: bool,
) {
    let Some(metrics) = metrics_collector else {
        return;
    };

    metrics.session_message_count(session_id.to_string(), message_count, Utc::now());
    if !has_pending_question {
        metrics.session_completed(
            session_id.to_string(),
            MetricsSessionStatus::Completed,
            Utc::now(),
        );
    }
}
