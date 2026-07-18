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

/// Inbox to the account-wide change feed: `(session_id, event)` before the
/// writer assigns a seq. Threaded as `Option` so engine-internal callers that
/// have no feed (tests, standalone embeddings) can pass `None`. Defined here so
/// the engine stays free of any `bamboo-server` dependency.
pub type AccountFeedInbox = mpsc::Sender<(Option<String>, AgentEvent)>;

/// Forward a durable change event onto the account feed, if an inbox is wired.
///
/// Ephemeral events (tokens, heartbeats, …) are filtered out before any clone,
/// so this is near-free on the hot path. `session_id` is supplied explicitly so
/// terminal events (which carry no id) still route to the right session.
fn mirror_to_account_feed(inbox: &Option<AccountFeedInbox>, session_id: &str, event: &AgentEvent) {
    if let Some(inbox) = inbox {
        if event.is_durable_change() {
            let route_session_id = event.session_id().unwrap_or(session_id);
            let _ = inbox.try_send((Some(route_session_id.to_string()), event.clone()));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn child_approval_change_routes_to_parent_account_envelope() {
        let (tx, mut rx) = mpsc::channel(4);
        let event = AgentEvent::ChildApprovalChanged {
            parent_session_id: "parent-1".into(),
            child_session_id: "child-1".into(),
            request_id: "req-1".into(),
            version: 2,
            status: "approved".into(),
            reason: None,
            tool_name: "Bash".into(),
            permission: "execute".into(),
            resource: "/tmp/x".into(),
            created_at: "2026-01-01T00:00:00Z".into(),
            resolved_at: Some("2026-01-01T00:00:01Z".into()),
        };

        mirror_to_account_feed(&Some(tx), "child-1", &event);
        let (session_id, mirrored) = rx.recv().await.unwrap();
        assert_eq!(session_id.as_deref(), Some("parent-1"));
        assert!(matches!(mirrored, AgentEvent::ChildApprovalChanged { .. }));
    }
}

/// Create an MPSC channel for agent events and spawn a forwarding task
/// that relays events to the broadcast sender while tracking runner
/// diagnostic fields for live visibility.
///
/// `account_feed_inbox`, when present, also mirrors durable change events onto
/// the account-wide feed for resumable multi-client sync.
///
/// Returns `(mpsc_tx, forwarder_handle)`.
pub fn create_event_forwarder(
    session_id: String,
    broadcast_tx: broadcast::Sender<AgentEvent>,
    runners: Arc<RwLock<HashMap<String, AgentRunner>>>,
    account_feed_inbox: Option<AccountFeedInbox>,
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
                mirror_to_account_feed(&account_feed_inbox, &session_id, &started_event);
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
            mirror_to_account_feed(&account_feed_inbox, &session_id, &event);
            let _ = broadcast_tx.send(event);
        }
    });

    (mpsc_tx, forwarder)
}
