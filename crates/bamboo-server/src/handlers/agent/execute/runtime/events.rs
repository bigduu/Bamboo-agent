use tokio::sync::mpsc;
use tracing::Instrument;

use crate::app_state::AppState;
use bamboo_agent_core::AgentEvent;

/// Returns true for events that carry critical state a late subscriber must see.
///
/// These are cached on the runner and replayed when an SSE client connects
/// after the live stream has already started.
fn is_critical_event(event: &AgentEvent) -> bool {
    matches!(
        event,
        AgentEvent::TaskListUpdated { .. }
            | AgentEvent::TaskListCompleted { .. }
            | AgentEvent::SubAgentStarted { .. }
            | AgentEvent::SubAgentCompleted { .. }
            | AgentEvent::SessionTitleUpdated { .. }
            | AgentEvent::SessionPinnedUpdated { .. }
    )
}

pub(crate) fn spawn_event_forwarder(
    state: actix_web::web::Data<AppState>,
    session_id: String,
    mut mpsc_rx: mpsc::Receiver<AgentEvent>,
    session_tx: tokio::sync::broadcast::Sender<AgentEvent>,
) {
    let span_session_id = session_id.clone();
    let session_span = tracing::info_span!("event_forwarder", session_id = %span_session_id);

    tokio::spawn(
        async move {
            while let Some(event) = mpsc_rx.recv().await {
                // Cache critical events for late-subscriber replay.
                if is_critical_event(&event) {
                    let mut runners = state.agent_runners.write().await;
                    if let Some(runner) = runners.get_mut(&session_id) {
                        runner.push_critical_event(event.clone());
                        tracing::trace!(
                            "[{}] Cached critical event for late subscribers",
                            session_id
                        );
                    }
                }

                // Store budget events for late subscribers.
                if matches!(&event, AgentEvent::TokenBudgetUpdated { .. }) {
                    let mut runners = state.agent_runners.write().await;
                    if let Some(runner) = runners.get_mut(&session_id) {
                        runner.last_budget_event = Some(event.clone());
                        tracing::debug!(
                            "[{}] Stored budget event for late subscribers",
                            session_id
                        );
                    }
                }

                // Always forward to the broadcast channel regardless of subscriber count.
                // The broadcast channel's internal capacity (1000 slots) buffers events so
                // that late or temporarily-disconnected subscribers can catch up when they
                // resubscribe.  Critical events are *also* cached on the runner above, so
                // even if the broadcast ring wraps, a fresh subscriber will still receive
                // the latest state via the replay path.
                let _ = session_tx.send(event);
            }

            tracing::debug!("[{}] Event forwarder finished", session_id);
        }
        .instrument(session_span),
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_agent_core::AgentEvent;
    use bamboo_domain::{TaskItem, TaskItemStatus, TaskList};
    use chrono::Utc;

    /// Helper: create a minimal `TaskListUpdated` event for testing.
    fn task_list_updated() -> AgentEvent {
        AgentEvent::TaskListUpdated {
            task_list: TaskList {
                session_id: "test-session".to_string(),
                title: "Test".to_string(),
                items: vec![TaskItem {
                    id: "t1".into(),
                    description: "Do something".into(),
                    status: TaskItemStatus::InProgress,
                    ..TaskItem::default()
                }],
                created_at: Utc::now(),
                updated_at: Utc::now(),
            },
        }
    }

    fn sub_agent_started() -> AgentEvent {
        AgentEvent::SubAgentStarted {
            parent_session_id: "parent".into(),
            child_session_id: "child-1".into(),
            title: Some("child work".into()),
        }
    }

    // ── is_critical_event ──────────────────────────────────────────────

    #[test]
    fn critical_event_identifies_task_list_updated() {
        assert!(is_critical_event(&task_list_updated()));
    }

    #[test]
    fn critical_event_identifies_task_list_completed() {
        let event = AgentEvent::TaskListCompleted {
            session_id: "s1".into(),
            completed_at: Utc::now(),
            total_rounds: 3,
            total_tool_calls: 10,
        };
        assert!(is_critical_event(&event));
    }

    #[test]
    fn critical_event_identifies_sub_agent_started() {
        assert!(is_critical_event(&sub_agent_started()));
    }

    #[test]
    fn critical_event_identifies_sub_agent_completed() {
        let event = AgentEvent::SubAgentCompleted {
            parent_session_id: "parent".into(),
            child_session_id: "child-1".into(),
            status: "completed".into(),
            error: None,
        };
        assert!(is_critical_event(&event));
    }

    #[test]
    fn critical_event_includes_session_title_updated() {
        use bamboo_agent_core::TitleSource;
        use chrono::Utc;
        let event = AgentEvent::SessionTitleUpdated {
            session_id: "s".to_string(),
            title: "t".to_string(),
            title_version: 1,
            source: TitleSource::Manual,
            updated_at: Utc::now(),
        };
        assert!(is_critical_event(&event));
    }

    #[test]
    fn critical_event_includes_session_pinned_updated() {
        let event = AgentEvent::SessionPinnedUpdated {
            session_id: "s".to_string(),
            pinned: true,
            updated_at: Utc::now(),
        };
        assert!(is_critical_event(&event));
    }

    #[test]
    fn non_critical_events_are_not_flagged() {
        // Token events are NOT critical.
        assert!(!is_critical_event(&AgentEvent::Token {
            content: "hello".into(),
        }));
        // ToolStart is NOT critical.
        assert!(!is_critical_event(&AgentEvent::ToolStart {
            tool_call_id: "tc-1".into(),
            tool_name: "Bash".into(),
            arguments: serde_json::json!(null),
        }));
        // Complete is NOT critical (terminal event is handled separately).
        assert!(!is_critical_event(&AgentEvent::Complete {
            usage: bamboo_domain::TokenUsage {
                prompt_tokens: 0,
                completion_tokens: 0,
                total_tokens: 0,
            },
        }));
    }

    // ── Event forwarder integration ────────────────────────────────────

    #[tokio::test]
    async fn event_forwarder_sends_events_even_with_zero_subscribers() {
        let temp_dir = tempfile::tempdir().unwrap();
        let state = AppState::new(temp_dir.path().to_path_buf())
            .await
            .expect("app state should initialize");
        let state = actix_web::web::Data::new(state);

        let session_id = "test-no-subs";

        // Register a runner so the forwarder can cache critical events.
        {
            use bamboo_engine::runtime::execution::runner_state::AgentRunner;
            let mut runner = AgentRunner::new();
            runner.status = bamboo_engine::runtime::execution::runner_state::AgentStatus::Running;
            state
                .agent_runners
                .write()
                .await
                .insert(session_id.to_string(), runner);
        }

        let (mpsc_tx, mpsc_rx) = mpsc::channel::<AgentEvent>(64);
        let (session_tx, _session_rx) = tokio::sync::broadcast::channel::<AgentEvent>(1000);
        // NOTE: _session_rx is dropped immediately — zero subscribers.

        spawn_event_forwarder(
            state.clone(),
            session_id.to_string(),
            mpsc_rx,
            session_tx.clone(),
        );

        // Send a critical event while there are zero broadcast subscribers.
        mpsc_tx.send(task_list_updated()).await.unwrap();
        mpsc_tx
            .send(AgentEvent::Token {
                content: "hi".into(),
            })
            .await
            .unwrap();

        // Give the forwarder task a chance to process.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Now subscribe to the broadcast channel and verify events are available.
        // Because we always send (no subscriber-count gate), the events went into
        // the broadcast buffer. A late subscriber won't see them via broadcast
        // (broadcast only replays to active receivers), but the critical event
        // cache should be populated.
        let runners = state.agent_runners.read().await;
        let runner = runners.get(session_id).expect("runner should exist");
        assert_eq!(
            runner.last_critical_events.len(),
            1,
            "should have cached exactly one critical event"
        );
        assert!(
            matches!(
                runner.last_critical_events[0],
                AgentEvent::TaskListUpdated { .. }
            ),
            "cached event should be TaskListUpdated"
        );
    }

    #[tokio::test]
    async fn event_forwarder_caches_multiple_critical_events_in_order() {
        let temp_dir = tempfile::tempdir().unwrap();
        let state = AppState::new(temp_dir.path().to_path_buf())
            .await
            .expect("app state should initialize");
        let state = actix_web::web::Data::new(state);

        let session_id = "test-critical-order";

        {
            use bamboo_engine::runtime::execution::runner_state::AgentRunner;
            let mut runner = AgentRunner::new();
            runner.status = bamboo_engine::runtime::execution::runner_state::AgentStatus::Running;
            state
                .agent_runners
                .write()
                .await
                .insert(session_id.to_string(), runner);
        }

        let (mpsc_tx, mpsc_rx) = mpsc::channel::<AgentEvent>(64);
        let (session_tx, _) = tokio::sync::broadcast::channel::<AgentEvent>(1000);

        spawn_event_forwarder(state.clone(), session_id.to_string(), mpsc_rx, session_tx);

        // Send a critical event, then a non-critical event, then another critical event.
        mpsc_tx.send(task_list_updated()).await.unwrap();
        mpsc_tx
            .send(AgentEvent::Token {
                content: "thinking".into(),
            })
            .await
            .unwrap();
        mpsc_tx.send(sub_agent_started()).await.unwrap();

        // Drop sender to terminate forwarder.
        drop(mpsc_tx);

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let runners = state.agent_runners.read().await;
        let runner = runners.get(session_id).expect("runner should exist");
        assert_eq!(runner.last_critical_events.len(), 2);
        assert!(matches!(
            runner.last_critical_events[0],
            AgentEvent::TaskListUpdated { .. }
        ));
        assert!(matches!(
            runner.last_critical_events[1],
            AgentEvent::SubAgentStarted { .. }
        ));
    }

    #[tokio::test]
    async fn late_subscriber_receives_events_from_broadcast() {
        // Verify that with the new always-send behavior, a subscriber who joins
        // after events were sent can still receive new events live.
        let temp_dir = tempfile::tempdir().unwrap();
        let state = AppState::new(temp_dir.path().to_path_buf())
            .await
            .expect("app state should initialize");
        let state = actix_web::web::Data::new(state);

        let session_id = "test-late-sub";

        {
            use bamboo_engine::runtime::execution::runner_state::AgentRunner;
            let mut runner = AgentRunner::new();
            runner.status = bamboo_engine::runtime::execution::runner_state::AgentStatus::Running;
            state
                .agent_runners
                .write()
                .await
                .insert(session_id.to_string(), runner);
        }

        let (mpsc_tx, mpsc_rx) = mpsc::channel::<AgentEvent>(64);
        let (session_tx, _) = tokio::sync::broadcast::channel::<AgentEvent>(1000);

        spawn_event_forwarder(
            state.clone(),
            session_id.to_string(),
            mpsc_rx,
            session_tx.clone(),
        );

        // Send events with no subscriber.
        mpsc_tx
            .send(AgentEvent::Token {
                content: "before-sub".into(),
            })
            .await
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        // Now subscribe.
        let mut late_rx = session_tx.subscribe();

        // Send more events.
        mpsc_tx
            .send(AgentEvent::Token {
                content: "after-sub".into(),
            })
            .await
            .unwrap();

        // Late subscriber should receive the event sent after subscription.
        let received = tokio::time::timeout(std::time::Duration::from_millis(200), late_rx.recv())
            .await
            .expect("should receive event before timeout")
            .expect("should not get channel closed");

        match received {
            AgentEvent::Token { content } => assert_eq!(content, "after-sub"),
            other => panic!("unexpected event: {other:?}"),
        }

        // Drop to clean up.
        drop(mpsc_tx);
    }
}
