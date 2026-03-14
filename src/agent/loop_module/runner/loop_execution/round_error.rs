use crate::agent::core::AgentError;
use crate::agent::metrics::{
    MetricsCollector, RoundStatus as MetricsRoundStatus, SessionStatus as MetricsSessionStatus,
};

fn map_round_error_status(error: &AgentError) -> (MetricsRoundStatus, MetricsSessionStatus) {
    if matches!(error, AgentError::Cancelled) {
        (
            MetricsRoundStatus::Cancelled,
            MetricsSessionStatus::Cancelled,
        )
    } else {
        (MetricsRoundStatus::Error, MetricsSessionStatus::Error)
    }
}

pub(super) fn record_round_failure(
    metrics_collector: Option<&MetricsCollector>,
    round_id: &str,
    session_id: &str,
    message_count: u32,
    error: &AgentError,
) {
    let (round_status, session_status) = map_round_error_status(error);
    super::super::metrics_lifecycle::record_round_and_session_error(
        metrics_collector,
        round_id,
        session_id,
        message_count,
        round_status,
        Some(error.to_string()),
        session_status,
    );
}
