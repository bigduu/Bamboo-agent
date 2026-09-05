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
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn hundreds_of_child_streams_progress_while_global_registry_is_locked() {
        let runners = Arc::new(RwLock::new(HashMap::new()));
        let mut streams = Vec::new();
        for index in 0..512 {
            let id = format!("child-{index}");
            let mut runner = AgentRunner::new();
            runner.status = super::super::runner_state::AgentStatus::Running;
            let mut receiver = runner.event_sender.subscribe();
            let sender = runner.event_sender.clone();
            let run_id = runner.run_id.clone();
            runners.write().await.insert(id.clone(), runner);
            let (input, task) = create_event_forwarder(id, run_id, sender, runners.clone(), None);
            assert!(matches!(
                receiver.recv().await.unwrap(),
                AgentEvent::ExecutionStarted { .. }
            ));
            streams.push((input, receiver, task));
        }
        let held_registry = runners.write().await;
        let started = std::time::Instant::now();
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            futures::future::join_all(streams.iter_mut().map(|(input, receiver, _)| async move {
                for _ in 0..64 {
                    input
                        .send(AgentEvent::Token {
                            content: "delta".into(),
                        })
                        .await
                        .unwrap();
                }
                for _ in 0..64 {
                    assert!(matches!(
                        receiver.recv().await.unwrap(),
                        AgentEvent::Token { .. }
                    ));
                }
            }))
            .await;
        })
        .await
        .expect("independent token streams must not wait for registry ownership");
        eprintln!(
            "512 child streams / 32768 tokens with held registry: {:?}",
            started.elapsed()
        );
        assert!(held_registry
            .values()
            .all(|runner| runner.last_activity_at().is_some()));
        drop(held_registry);
        for (input, _, task) in streams {
            drop(input);
            task.await.unwrap();
        }
    }

    #[tokio::test]
    async fn child_approval_change_routes_to_parent_account_envelope() {
        let (tx, mut rx) = mpsc::channel(4);
        let event = AgentEvent::ChildApprovalChanged {
            parent_session_id: "parent-1".into(),
            child_session_id: "child-1".into(),
            child_attempt: 1,
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

    #[tokio::test]
    async fn delayed_old_forwarder_cannot_publish_after_successor_reservation() {
        let session_id = "session-generation";
        let (broadcast_tx, mut broadcast_rx) = broadcast::channel(16);
        let mut successor = AgentRunner::new();
        successor.run_id = "run-new".to_string();
        successor.status = super::super::runner_state::AgentStatus::Running;
        successor.event_sender = broadcast_tx.clone();
        let runners = Arc::new(RwLock::new(HashMap::from([(
            session_id.to_string(),
            successor,
        )])));

        // The successor is already visible on the shared transport before an
        // old forwarder task finally gets CPU time.
        broadcast_tx
            .send(AgentEvent::ExecutionStarted {
                run_id: "run-new".to_string(),
                session_id: session_id.to_string(),
                started_at: Utc::now().to_rfc3339(),
            })
            .unwrap();
        let (old_tx, old_forwarder) = create_event_forwarder(
            session_id.to_string(),
            "run-old".to_string(),
            broadcast_tx.clone(),
            runners,
            None,
        );
        let _ = old_tx
            .send(AgentEvent::NeedClarification {
                question: "stale".to_string(),
                options: Some(vec!["A".to_string()]),
                tool_call_id: Some("old-tool".to_string()),
                tool_name: Some("ConclusionWithOptions".to_string()),
                allow_custom: false,
                source: Some(bamboo_agent_core::PendingQuestionSource::PauseTool),
            })
            .await;
        drop(old_tx);
        old_forwarder.await.unwrap();

        assert!(matches!(
            broadcast_rx.recv().await.unwrap(),
            AgentEvent::ExecutionStarted { ref run_id, .. } if run_id == "run-new"
        ));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), broadcast_rx.recv())
                .await
                .is_err(),
            "old Started/Need/Complete must all be suppressed"
        );
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
    run_id: String,
    broadcast_tx: broadcast::Sender<AgentEvent>,
    runners: Arc<RwLock<HashMap<String, AgentRunner>>>,
    account_feed_inbox: Option<AccountFeedInbox>,
) -> (mpsc::Sender<AgentEvent>, tokio::task::JoinHandle<()>) {
    let (mpsc_tx, mut mpsc_rx) = mpsc::channel::<AgentEvent>(100);

    let forwarder = tokio::spawn(async move {
        // The exact reservation generation is captured synchronously by the
        // caller. Never re-read the replaceable runner registry here: this
        // task may be scheduled only after a clarification handoff installs a
        // successor, which would mis-tag the old terminal as the new run.
        let started_event = AgentEvent::ExecutionStarted {
            run_id: run_id.clone(),
            session_id: session_id.clone(),
            started_at: Utc::now().to_rfc3339(),
        };
        let publication = {
            let runners = runners.read().await;
            let Some(runner) = runners
                .get(&session_id)
                .filter(|runner| runner.run_id == run_id)
            else {
                return;
            };
            mirror_to_account_feed(&account_feed_inbox, &session_id, &started_event);
            let _ = broadcast_tx.send(started_event);
            runner.event_publication.clone()
        };

        while let Some(event) = mpsc_rx.recv().await {
            let needs_runner_update = event.is_replayable_session_state()
                || matches!(
                    &event,
                    AgentEvent::TokenBudgetUpdated { .. }
                        | AgentEvent::ToolStart { .. }
                        | AgentEvent::ToolLifecycle { .. }
                        | AgentEvent::RunnerProgress { .. }
                );
            if !needs_runner_update {
                if !publication.publish(|| {
                    mirror_to_account_feed(&account_feed_inbox, &session_id, &event);
                    let _ = broadcast_tx.send(event);
                }) {
                    return;
                }
                continue;
            }
            let mut runners = runners.write().await;
            let Some(runner) = runners
                .get_mut(&session_id)
                .filter(|runner| runner.run_id == run_id)
            else {
                // A clarification handoff installed a successor before this
                // delayed forwarder/frame ran. Drop the entire stale stream;
                // broadcasting even its Started/Need would corrupt the shared
                // session generation state.
                return;
            };
            runner.last_event_at = Some(Utc::now());
            publication.touch();

            // Cache live state before publication so a subscriber installed
            // between a clarification pause and its response sees the exact
            // boundary. This generic forwarder powers Connect, schedules,
            // SDK spawn, and child-resume paths, so it must preserve the same
            // replay invariant as the server-owned forwarder.
            if event.is_replayable_session_state() {
                runner.push_critical_event(event.clone());
            }

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
            mirror_to_account_feed(&account_feed_inbox, &session_id, &event);
            let _ = broadcast_tx.send(event);
        }
    });

    (mpsc_tx, forwarder)
}
