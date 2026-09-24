//! Event forwarding from MPSC to broadcast channels.
//!
//! Creates an MPSC channel for agent loop events and spawns a background task
//! that relays events to the session's broadcast sender while tracking runner
//! diagnostic state (budget events, tool execution, round progress).

use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use tokio::sync::{broadcast, mpsc, watch, RwLock};

use bamboo_agent_core::AgentEvent;

use super::runner_state::AgentRunner;

/// Inbox to the account-wide change feed: `(session_id, event)` before the
/// writer assigns a seq. Threaded as `Option` so engine-internal callers that
/// have no feed (tests, standalone embeddings) can pass `None`. Defined here so
/// the engine stays free of any `bamboo-server` dependency.
pub type AccountFeedInbox = mpsc::Sender<(Option<String>, AgentEvent)>;

/// Completion signal for the durable history barrier emitted at the end of a
/// run. The runner must not become replaceable until the forwarder has actually
/// published `SessionHistoryCommitted`; merely enqueueing the event leaves a
/// race where a successor can retire this generation first.
#[derive(Clone, Debug)]
pub struct HistoryCommitBarrier {
    revision: watch::Receiver<u64>,
}

/// Forwarder-owned half of [`HistoryCommitBarrier`].
#[derive(Clone, Debug)]
pub struct HistoryCommitAcknowledger {
    revision: watch::Sender<u64>,
}

/// Create the acknowledgment pair shared by an event producer and forwarder.
pub fn history_commit_barrier() -> (HistoryCommitAcknowledger, HistoryCommitBarrier) {
    let (revision_tx, revision_rx) = watch::channel(0);
    (
        HistoryCommitAcknowledger {
            revision: revision_tx,
        },
        HistoryCommitBarrier {
            revision: revision_rx,
        },
    )
}

impl HistoryCommitAcknowledger {
    /// Mark one history barrier as synchronously published to every configured
    /// sink. Call this only after the publication fence accepts the event.
    pub fn acknowledge(&self) {
        self.revision
            .send_modify(|revision| *revision = revision.saturating_add(1));
    }
}

impl HistoryCommitBarrier {
    /// Enqueue a history barrier and wait until the matching forwarder has
    /// published it. Returns `false` if the forwarder disappeared first.
    pub async fn send_and_wait(
        &mut self,
        event_tx: &mpsc::Sender<AgentEvent>,
        session_id: String,
    ) -> bool {
        let observed = *self.revision.borrow_and_update();
        if event_tx
            .send(AgentEvent::SessionHistoryCommitted { session_id })
            .await
            .is_err()
        {
            return false;
        }

        loop {
            if *self.revision.borrow() > observed {
                return true;
            }
            if self.revision.changed().await.is_err() {
                return false;
            }
        }
    }
}

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

    use bamboo_agent_core::ToolResult;
    use serde_json::json;

    #[tokio::test]
    async fn generic_forwarder_projects_native_download_without_changing_ordinary_results() {
        let session_id = "generic-download-display";
        let (broadcast_tx, mut broadcast_rx) = broadcast::channel(16);
        let mut runner = AgentRunner::new();
        runner.status = super::super::runner_state::AgentStatus::Running;
        runner.event_sender = broadcast_tx.clone();
        let run_id = runner.run_id.clone();
        let runners = Arc::new(RwLock::new(HashMap::from([(
            session_id.to_string(),
            runner,
        )])));
        let (input, forwarder) =
            create_event_forwarder(session_id.to_string(), run_id, broadcast_tx, runners, None);
        assert!(matches!(
            broadcast_rx.recv().await.unwrap(),
            AgentEvent::ExecutionStarted { .. }
        ));

        let private = "private-file-url-and-bytes";
        input
            .send(AgentEvent::ToolStart {
                tool_call_id: "download".into(),
                tool_name: "browser".into(),
                arguments: json!({"action":"download","selector":private,"expected_epoch":9}),
            })
            .await
            .unwrap();
        input
            .send(AgentEvent::ToolError {
                tool_call_id: "download".into(),
                error: private.into(),
            })
            .await
            .unwrap();
        input
            .send(AgentEvent::ToolComplete {
                tool_call_id: "download".into(),
                result: ToolResult::text(false, private),
            })
            .await
            .unwrap();
        input
            .send(AgentEvent::ToolStart {
                tool_call_id: "read".into(),
                tool_name: "Read".into(),
                arguments: json!({"path":"readme.md"}),
            })
            .await
            .unwrap();
        input
            .send(AgentEvent::ToolComplete {
                tool_call_id: "read".into(),
                result: ToolResult::text(true, "ordinary Read result"),
            })
            .await
            .unwrap();
        drop(input);

        let mut visible = Vec::new();
        for _ in 0..5 {
            visible.push(
                tokio::time::timeout(std::time::Duration::from_secs(5), broadcast_rx.recv())
                    .await
                    .unwrap()
                    .unwrap(),
            );
        }
        for event in &visible[..3] {
            assert!(!serde_json::to_string(event).unwrap().contains(private));
        }
        assert!(
            matches!(&visible[2], AgentEvent::ToolComplete { result, .. }
            if result.result == "Tool result hidden" && result.images.is_empty())
        );
        assert!(
            matches!(&visible[4], AgentEvent::ToolComplete { result, .. }
            if result.result == "ordinary Read result")
        );
        forwarder.await.unwrap();
    }

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
    async fn history_commit_barrier_acknowledges_actual_publication() {
        let session_id = "session-history-barrier";
        let (broadcast_tx, mut broadcast_rx) = broadcast::channel(16);
        let mut runner = AgentRunner::new();
        runner.status = super::super::runner_state::AgentStatus::Running;
        runner.event_sender = broadcast_tx.clone();
        let run_id = runner.run_id.clone();
        let runners = Arc::new(RwLock::new(HashMap::from([(
            session_id.to_string(),
            runner,
        )])));
        let (event_tx, forwarder, mut barrier) = create_event_forwarder_with_history_commit_barrier(
            session_id.to_string(),
            run_id,
            broadcast_tx,
            runners,
            None,
        );

        assert!(matches!(
            broadcast_rx.recv().await.unwrap(),
            AgentEvent::ExecutionStarted { .. }
        ));
        assert!(
            barrier
                .send_and_wait(&event_tx, session_id.to_string())
                .await,
            "producer must not continue until the forwarder publishes the barrier"
        );
        assert!(matches!(
            broadcast_rx.recv().await.unwrap(),
            AgentEvent::SessionHistoryCommitted { session_id: id } if id == session_id
        ));

        drop(event_tx);
        forwarder.await.unwrap();
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
    let (event_tx, forwarder, _history_commit_barrier) =
        create_event_forwarder_with_history_commit_barrier(
            session_id,
            run_id,
            broadcast_tx,
            runners,
            account_feed_inbox,
        );
    (event_tx, forwarder)
}

/// Create an event forwarder plus a producer-side durable-history barrier.
/// Runtime paths that finalize and replace runner generations must use this
/// variant so they can await actual barrier publication before finalization.
pub fn create_event_forwarder_with_history_commit_barrier(
    session_id: String,
    run_id: String,
    broadcast_tx: broadcast::Sender<AgentEvent>,
    runners: Arc<RwLock<HashMap<String, AgentRunner>>>,
    account_feed_inbox: Option<AccountFeedInbox>,
) -> (
    mpsc::Sender<AgentEvent>,
    tokio::task::JoinHandle<()>,
    HistoryCommitBarrier,
) {
    let (mpsc_tx, mut mpsc_rx) = mpsc::channel::<AgentEvent>(100);
    let (history_commit_acknowledger, history_commit_barrier) = history_commit_barrier();

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

        let mut tool_event_display = bamboo_agent_core::NativeToolEventDisplay::default();
        while let Some(event) = mpsc_rx.recv().await {
            let event = tool_event_display.project(event);
            let needs_runner_update = event.is_replayable_session_state()
                || matches!(
                    &event,
                    AgentEvent::TokenBudgetUpdated { .. }
                        | AgentEvent::ToolStart { .. }
                        | AgentEvent::ToolLifecycle { .. }
                        | AgentEvent::RunnerProgress { .. }
                );
            if !needs_runner_update {
                let is_history_commit =
                    matches!(&event, AgentEvent::SessionHistoryCommitted { .. });
                if !publication.publish(|| {
                    mirror_to_account_feed(&account_feed_inbox, &session_id, &event);
                    let _ = broadcast_tx.send(event);
                }) {
                    return;
                }
                if is_history_commit {
                    history_commit_acknowledger.acknowledge();
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
            let is_history_commit = matches!(&event, AgentEvent::SessionHistoryCommitted { .. });
            mirror_to_account_feed(&account_feed_inbox, &session_id, &event);
            let _ = broadcast_tx.send(event);
            if is_history_commit {
                history_commit_acknowledger.acknowledge();
            }
        }
    });

    (mpsc_tx, forwarder, history_commit_barrier)
}
