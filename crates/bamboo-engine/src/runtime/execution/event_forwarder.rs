//! Event forwarding from MPSC to broadcast channels.
//!
//! Creates an MPSC channel for agent loop events and spawns a background task
//! that relays events to the session's broadcast sender while tracking runner
//! diagnostic state (budget events, tool execution, round progress).

use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use tokio::sync::{broadcast, mpsc, RwLock};

use bamboo_agent_core::AgentEvent;

use super::runner_state::AgentRunner;

/// Create an MPSC channel for agent events and spawn a forwarding task
/// that relays events to the broadcast sender while tracking runner
/// diagnostic fields for live visibility.
///
/// Returns `(mpsc_tx, forwarder_handle)`.
pub fn create_event_forwarder(
    session_id: String,
    broadcast_tx: broadcast::Sender<AgentEvent>,
    runners: Arc<RwLock<HashMap<String, AgentRunner>>>,
) -> (mpsc::Sender<AgentEvent>, tokio::task::JoinHandle<()>) {
    let (mpsc_tx, mut mpsc_rx) = mpsc::channel::<AgentEvent>(100);

    let forwarder = tokio::spawn(async move {
        // Emit ExecutionStarted as the first event so the frontend can correlate
        // all subsequent events to this run via run_id.
        {
            let runners = runners.read().await;
            if let Some(runner) = runners.get(&session_id) {
                let started_event = AgentEvent::ExecutionStarted {
                    run_id: runner.run_id.clone(),
                    session_id: session_id.clone(),
                    started_at: Utc::now().to_rfc3339(),
                };
                let _ = broadcast_tx.send(started_event);
            }
        }

        while let Some(event) = mpsc_rx.recv().await {
            {
                let mut runners = runners.write().await;
                if let Some(runner) = runners.get_mut(&session_id) {
                    runner.last_event_at = Some(Utc::now());

                    match &event {
                        AgentEvent::TokenBudgetUpdated { .. } => {
                            runner.last_budget_event = Some(event.clone());
                        }
                        AgentEvent::ToolStart { tool_name, .. } => {
                            runner.last_tool_name = Some(tool_name.clone());
                            runner.last_tool_phase = Some("begin".to_string());
                        }
                        AgentEvent::ToolLifecycle {
                            tool_name, phase, ..
                        } => {
                            runner.last_tool_name = Some(tool_name.clone());
                            runner.last_tool_phase = Some(phase.clone());
                        }
                        AgentEvent::RunnerProgress { round_count, .. } => {
                            runner.round_count = *round_count;
                        }
                        _ => {}
                    }
                }
            }
            let _ = broadcast_tx.send(event);
        }
    });

    (mpsc_tx, forwarder)
}
