use bamboo_agent_core::Session;
use crate::metrics::MetricsCollector;

pub(super) fn record_session_resolution(
    metrics_collector: Option<&MetricsCollector>,
    session_id: &str,
    session: &Session,
) {
    super::super::metrics_lifecycle::record_session_completed_if_resolved(
        metrics_collector,
        session_id,
        session.messages.len() as u32,
        session.has_pending_question(),
    );
}
