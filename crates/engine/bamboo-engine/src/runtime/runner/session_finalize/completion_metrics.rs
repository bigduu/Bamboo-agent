use bamboo_metrics::MetricsCollector;
use bamboo_agent_core::Session;
use bamboo_domain::AgentStatusState;

pub(super) fn record_session_resolution(
    metrics_collector: Option<&MetricsCollector>,
    session_id: &str,
    session: &Session,
    runtime_state: &bamboo_domain::AgentRuntimeState,
) {
    super::super::metrics_lifecycle::record_session_completed_if_resolved(
        metrics_collector,
        session_id,
        session.messages.len() as u32,
        session.has_pending_question()
            || matches!(runtime_state.status, AgentStatusState::Suspended),
    );
}
