//! Replayable session-event publishing infrastructure.
//!
//! This module owns the **single canonical path** for publishing UI-replayable
//! session events. Every replayable event must go through
//! [`publish_replayable_session_event`] so that:
//!
//! 1. The event is cached on the session's runner for late-subscriber replay
//!    *before* any subscriber sees it on the live broadcast channel.
//! 2. The cache-then-broadcast ordering is uniform across all writers, so
//!    reconnecting clients can never miss an event that earlier subscribers
//!    received.
//!
//! ## Invariant
//!
//! No code in the workspace may pair `runner.push_critical_event` with
//! `sender.send` outside this helper or [`crate::handlers::agent::execute::runtime::events::spawn_event_forwarder`].
//! Hand-rolling the pair has historically led to inverted ordering (broadcast
//! first, cache second), which leaves a small window where a late subscriber
//! receives the live event but the cache is still empty.
//!
//! ## When to use this helper
//!
//! Use this helper for **synchronous** writers that hold an `&AppState`
//! directly — e.g. HTTP handlers (`patch_session`, `regenerate_title`) and
//! background tasks (`title_gen`). The runtime forwarder
//! (`spawn_event_forwarder`) handles the equivalent ordering for events that
//! flow through the engine's mpsc channel.

use bamboo_agent_core::AgentEvent;

use crate::app_state::AppState;

/// Publishes a replayable session event with the correct cache-then-broadcast
/// ordering. See module docs for the invariant.
///
/// Order is fixed and **must not** be changed:
///
/// 1. `runner.push_critical_event(event.clone())` — populate the late-subscriber
///    replay cache while the event is still un-broadcast.
/// 2. `sender.send(event)` — broadcast to all live subscribers.
///
/// If no runner exists for `session_id` (the session has not started yet, or
/// has already terminated), the cache step is silently skipped and the event
/// is still broadcast. This matches the semantics of the runtime forwarder.
pub async fn publish_replayable_session_event(
    state: &AppState,
    session_id: &str,
    event: AgentEvent,
) {
    {
        let mut runners = state.agent_runners.write().await;
        if let Some(runner) = runners.get_mut(session_id) {
            runner.push_critical_event(event.clone());
        }
    }

    // Mirror onto the account-wide change feed (sequenced + journaled). The
    // sink filters ephemeral events internally; metadata events published here
    // (title/pinned) are durable and will be sequenced.
    state.account_sink.record(Some(session_id), &event);

    let sender = state.get_session_event_sender(session_id).await;
    let _ = sender.send(event);
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_agent_core::TitleSource;
    use bamboo_engine::runtime::execution::runner_state::{AgentRunner, AgentStatus};
    use chrono::Utc;

    fn title_event(session_id: &str) -> AgentEvent {
        AgentEvent::SessionTitleUpdated {
            session_id: session_id.to_string(),
            title: "test title".to_string(),
            title_version: 1,
            source: TitleSource::Manual,
            updated_at: Utc::now(),
        }
    }

    fn pinned_event(session_id: &str, pinned: bool) -> AgentEvent {
        AgentEvent::SessionPinnedUpdated {
            session_id: session_id.to_string(),
            pinned,
            updated_at: Utc::now(),
        }
    }

    async fn install_runner(state: &AppState, session_id: &str) {
        let mut runner = AgentRunner::new();
        runner.status = AgentStatus::Running;
        state
            .agent_runners
            .write()
            .await
            .insert(session_id.to_string(), runner);
    }

    #[tokio::test]
    async fn caches_before_broadcasting_for_title_event() {
        let temp_dir = tempfile::tempdir().unwrap();
        let state = AppState::new(temp_dir.path().to_path_buf())
            .await
            .expect("app state init");

        let session_id = "publish-order";
        install_runner(&state, session_id).await;

        // Subscribe BEFORE publishing so the broadcast can be observed.
        let sender = state.get_session_event_sender(session_id).await;
        let mut subscriber = sender.subscribe();

        publish_replayable_session_event(&state, session_id, title_event(session_id)).await;

        // After the helper returns:
        //   1. The cache must already contain the event (the cache step ran first).
        //   2. The subscriber must be able to receive it.
        let runners = state.agent_runners.read().await;
        let runner = runners.get(session_id).expect("runner registered");
        assert_eq!(
            runner.last_critical_events.len(),
            1,
            "cache must contain event after publish"
        );
        assert!(matches!(
            runner.last_critical_events[0],
            AgentEvent::SessionTitleUpdated { .. }
        ));
        drop(runners);

        let received =
            tokio::time::timeout(std::time::Duration::from_millis(100), subscriber.recv())
                .await
                .expect("broadcast received before timeout")
                .expect("broadcast not closed");
        assert!(matches!(received, AgentEvent::SessionTitleUpdated { .. }));
    }

    #[tokio::test]
    async fn caches_before_broadcasting_for_pinned_event() {
        let temp_dir = tempfile::tempdir().unwrap();
        let state = AppState::new(temp_dir.path().to_path_buf())
            .await
            .expect("app state init");

        let session_id = "publish-pinned";
        install_runner(&state, session_id).await;

        let sender = state.get_session_event_sender(session_id).await;
        let mut subscriber = sender.subscribe();

        publish_replayable_session_event(&state, session_id, pinned_event(session_id, true)).await;

        let runners = state.agent_runners.read().await;
        let runner = runners.get(session_id).expect("runner registered");
        assert_eq!(runner.last_critical_events.len(), 1);
        assert!(matches!(
            runner.last_critical_events[0],
            AgentEvent::SessionPinnedUpdated { pinned: true, .. }
        ));
        drop(runners);

        let received =
            tokio::time::timeout(std::time::Duration::from_millis(100), subscriber.recv())
                .await
                .expect("broadcast received")
                .expect("broadcast not closed");
        assert!(matches!(
            received,
            AgentEvent::SessionPinnedUpdated { pinned: true, .. }
        ));
    }

    #[tokio::test]
    async fn skips_cache_when_runner_absent_but_still_broadcasts() {
        let temp_dir = tempfile::tempdir().unwrap();
        let state = AppState::new(temp_dir.path().to_path_buf())
            .await
            .expect("app state init");

        let session_id = "no-runner";
        // No runner installed.

        let sender = state.get_session_event_sender(session_id).await;
        let mut subscriber = sender.subscribe();

        publish_replayable_session_event(&state, session_id, title_event(session_id)).await;

        // No runner, so no cache check needed; broadcast must still succeed.
        let received =
            tokio::time::timeout(std::time::Duration::from_millis(100), subscriber.recv())
                .await
                .expect("broadcast received")
                .expect("broadcast not closed");
        assert!(matches!(received, AgentEvent::SessionTitleUpdated { .. }));

        // Sanity: no panic, no runner created as a side effect.
        let runners = state.agent_runners.read().await;
        assert!(runners.get(session_id).is_none());
    }
}
