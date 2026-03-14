use tokio_util::sync::CancellationToken;

use crate::agent::core::AgentError;
use crate::agent::metrics::MetricsCollector;

pub(super) fn ensure_not_cancelled(
    cancel_token: &CancellationToken,
    metrics_collector: Option<&MetricsCollector>,
    session_id: &str,
    message_count: usize,
) -> Result<(), AgentError> {
    if cancel_token.is_cancelled() {
        super::super::metrics_lifecycle::record_session_cancelled(
            metrics_collector,
            session_id,
            message_count as u32,
        );
        return Err(AgentError::Cancelled);
    }

    Ok(())
}
