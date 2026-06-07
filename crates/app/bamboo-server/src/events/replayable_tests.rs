//! Server-side tests for the engine `publish_replayable_session_event` helper.
//!
//! These live in the server crate (not alongside the engine helper) because
//! they construct a full `AppState` to exercise the trait-based code path.

use bamboo_engine::events::publish_replayable_session_event;
use crate::app_state::AppState;
use bamboo_agent_core::AgentEvent;
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

    let received = tokio::time::timeout(std::time::Duration::from_millis(100), subscriber.recv())
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

    let received = tokio::time::timeout(std::time::Duration::from_millis(100), subscriber.recv())
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
    let received = tokio::time::timeout(std::time::Duration::from_millis(100), subscriber.recv())
        .await
        .expect("broadcast received")
        .expect("broadcast not closed");
    assert!(matches!(received, AgentEvent::SessionTitleUpdated { .. }));

    // Sanity: no panic, no runner created as a side effect.
    let runners = state.agent_runners.read().await;
    assert!(runners.get(session_id).is_none());
}
