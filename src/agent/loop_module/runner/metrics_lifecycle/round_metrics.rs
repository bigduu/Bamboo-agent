use chrono::Utc;

use crate::agent::metrics::{
    MetricsCollector, RoundStatus as MetricsRoundStatus, SessionStatus as MetricsSessionStatus,
    TokenUsage as MetricsTokenUsage,
};

pub(in crate::agent::loop_module::runner) fn record_round_started(
    metrics_collector: Option<&MetricsCollector>,
    round_id: &str,
    session_id: &str,
    model_name: &str,
) {
    let Some(metrics) = metrics_collector else {
        return;
    };
    metrics.round_started(
        round_id.to_string(),
        session_id.to_string(),
        model_name.to_string(),
        Utc::now(),
    );
}

pub(in crate::agent::loop_module::runner) fn record_round_completed(
    metrics_collector: Option<&MetricsCollector>,
    round_id: &str,
    session_id: &str,
    message_count: u32,
    round_status: MetricsRoundStatus,
    round_usage: MetricsTokenUsage,
    round_error: Option<String>,
) {
    let Some(metrics) = metrics_collector else {
        return;
    };
    metrics.round_completed(
        round_id.to_string(),
        Utc::now(),
        round_status,
        round_usage,
        round_error,
    );
    metrics.session_message_count(session_id.to_string(), message_count, Utc::now());
}

pub(in crate::agent::loop_module::runner) fn record_round_and_session_error(
    metrics_collector: Option<&MetricsCollector>,
    round_id: &str,
    session_id: &str,
    message_count: u32,
    round_status: MetricsRoundStatus,
    round_error: Option<String>,
    session_status: MetricsSessionStatus,
) {
    let Some(metrics) = metrics_collector else {
        return;
    };

    metrics.round_completed(
        round_id.to_string(),
        Utc::now(),
        round_status,
        MetricsTokenUsage::default(),
        round_error,
    );
    metrics.session_message_count(session_id.to_string(), message_count, Utc::now());
    metrics.session_completed(session_id.to_string(), session_status, Utc::now());
}
