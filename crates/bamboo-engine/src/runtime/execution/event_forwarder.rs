//! Event forwarding from MPSC to broadcast channels.
//!
//! Creates an MPSC channel for agent loop events and spawns a background task
//! that relays events to the session's broadcast sender while tracking budget events.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{broadcast, mpsc, RwLock};

use bamboo_agent_core::AgentEvent;

use super::runner_state::AgentRunner;

/// Create an MPSC channel for agent events and spawn a forwarding task
/// that relays events to the broadcast sender while tracking budget events.
///
/// Returns `(mpsc_tx, forwarder_handle)`.
pub fn create_event_forwarder(
    session_id: String,
    broadcast_tx: broadcast::Sender<AgentEvent>,
    runners: Arc<RwLock<HashMap<String, AgentRunner>>>,
) -> (mpsc::Sender<AgentEvent>, tokio::task::JoinHandle<()>) {
    let (mpsc_tx, mut mpsc_rx) = mpsc::channel::<AgentEvent>(100);

    let forwarder = tokio::spawn(async move {
        while let Some(event) = mpsc_rx.recv().await {
            if matches!(&event, AgentEvent::TokenBudgetUpdated { .. }) {
                let mut runners = runners.write().await;
                if let Some(runner) = runners.get_mut(&session_id) {
                    runner.last_budget_event = Some(event.clone());
                }
            }
            let _ = broadcast_tx.send(event);
        }
    });

    (mpsc_tx, forwarder)
}
