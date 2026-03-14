use tokio::sync::mpsc;

use crate::{agent::core::AgentEvent, server::app_state::AppState};

pub(in crate::server::handlers::agent::execute) fn spawn_event_forwarder(
    state: actix_web::web::Data<AppState>,
    session_id: String,
    mut mpsc_rx: mpsc::Receiver<AgentEvent>,
    session_tx: tokio::sync::broadcast::Sender<AgentEvent>,
) {
    tokio::spawn(async move {
        while let Some(event) = mpsc_rx.recv().await {
            // Store budget events for late subscribers.
            if matches!(&event, AgentEvent::TokenBudgetUpdated { .. }) {
                let mut runners = state.agent_runners.write().await;
                if let Some(runner) = runners.get_mut(&session_id) {
                    runner.last_budget_event = Some(event.clone());
                    log::debug!("[{}] Stored budget event for late subscribers", session_id);
                }
            }

            if session_tx.send(event.clone()).is_err() {
                log::debug!("[{}] No subscribers for event", session_id);
            }
        }

        log::debug!("[{}] Event forwarder finished", session_id);
    });
}
