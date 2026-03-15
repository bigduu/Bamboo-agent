use tokio::sync::mpsc;

use crate::{agent::core::AgentEvent, server::app_state::AppState};

pub(in crate::server::handlers::agent::execute) fn spawn_event_forwarder(
    state: actix_web::web::Data<AppState>,
    session_id: String,
    mut mpsc_rx: mpsc::Receiver<AgentEvent>,
    session_tx: tokio::sync::broadcast::Sender<AgentEvent>,
) {
    tokio::spawn(async move {
        let mut dropped_without_subscribers = 0usize;
        let mut no_subscriber_mode = false;

        while let Some(event) = mpsc_rx.recv().await {
            // Store budget events for late subscribers.
            if matches!(&event, AgentEvent::TokenBudgetUpdated { .. }) {
                let mut runners = state.agent_runners.write().await;
                if let Some(runner) = runners.get_mut(&session_id) {
                    runner.last_budget_event = Some(event.clone());
                    log::debug!("[{}] Stored budget event for late subscribers", session_id);
                }
            }

            if session_tx.receiver_count() == 0 {
                dropped_without_subscribers = dropped_without_subscribers.saturating_add(1);
                if !no_subscriber_mode {
                    no_subscriber_mode = true;
                    log::debug!(
                        "[{}] No subscribers for event stream; suppressing repeated logs",
                        session_id
                    );
                }
                continue;
            }

            if no_subscriber_mode {
                log::debug!(
                    "[{}] Subscribers resumed; dropped {} events while unsubscribed",
                    session_id,
                    dropped_without_subscribers
                );
                dropped_without_subscribers = 0;
                no_subscriber_mode = false;
            }

            let _ = session_tx.send(event);
        }

        if dropped_without_subscribers > 0 {
            log::debug!(
                "[{}] Event forwarder finished after dropping {} events with no subscribers",
                session_id,
                dropped_without_subscribers
            );
        } else {
            log::debug!("[{}] Event forwarder finished", session_id);
        }
    });
}
