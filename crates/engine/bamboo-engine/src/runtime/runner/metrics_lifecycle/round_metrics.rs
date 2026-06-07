use chrono::Utc;

use bamboo_metrics::{
    MetricsCollector, RoundStatus as MetricsRoundStatus, SessionStatus as MetricsSessionStatus,
    TokenUsage as MetricsTokenUsage,
};

pub(in crate::runtime::runner) fn record_round_started(
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

#[allow(clippy::too_many_arguments)]
pub(in crate::runtime::runner) fn record_round_completed(
    metrics_collector: Option<&MetricsCollector>,
    round_id: &str,
    session_id: &str,
    message_count: u32,
    round_status: MetricsRoundStatus,
    round_usage: MetricsTokenUsage,
    prompt_cached_tool_outputs: u32,
    prompt_cached_tool_tokens_saved: u32,
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
        prompt_cached_tool_outputs,
        prompt_cached_tool_tokens_saved,
        round_error,
    );
    metrics.session_message_count(session_id.to_string(), message_count, Utc::now());
}

pub(in crate::runtime::runner) fn record_round_and_session_error(
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
        0,
        0,
        round_error,
    );
    metrics.session_message_count(session_id.to_string(), message_count, Utc::now());
    metrics.session_completed(session_id.to_string(), session_status, Utc::now());
}
