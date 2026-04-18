//! Session finalization helpers for the agent loop runner.

use tokio::sync::mpsc;

use bamboo_application_agent::{AgentEvent, Session};
use crate::config::AgentLoopConfig;
use crate::task_context::TaskLoopContext;
use bamboo_application_metrics::MetricsCollector;

mod completion_event;
mod completion_metrics;

use completion_event::send_complete_event_if_needed;
use completion_metrics::record_session_resolution;

pub(super) async fn finalize_session(
    task_context: Option<TaskLoopContext>,
    session: &mut Session,
    event_tx: &mpsc::Sender<AgentEvent>,
    session_id: &str,
    config: &AgentLoopConfig,
    metrics_collector: Option<&MetricsCollector>,
    sent_complete: bool,
) {
    super::task_lifecycle::finalize_task_context(
        task_context,
        session,
        event_tx,
        session_id,
        config,
    )
    .await;

    send_complete_event_if_needed(event_tx, sent_complete).await;

    record_session_resolution(metrics_collector, session_id, session);
}

#[cfg(test)]
mod tests;
