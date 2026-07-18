use super::response::execute_response_payload;
use super::validation::validate_and_normalize_model;
use crate::handlers::agent::execute::{ExecuteClientSync, ExecuteSyncInfo, ExecuteSyncReason};

use bamboo_engine::session_app::types::ServerExecuteSnapshot;

fn evaluate_client_sync_adapter(
    client_sync: Option<&ExecuteClientSync>,
    server_snapshot: &ServerExecuteSnapshot,
) -> Option<ExecuteSyncReason> {
    let crate_sync = client_sync.map(|cs| bamboo_engine::session_app::types::ExecuteClientSync {
        client_message_count: cs.client_message_count,
        client_last_message_id: cs.client_last_message_id.clone(),
        client_has_pending_question: cs.client_has_pending_question,
        client_pending_question_tool_call_id: cs.client_pending_question_tool_call_id.clone(),
    });

    bamboo_engine::session_app::execute::evaluate_client_sync(crate_sync.as_ref(), server_snapshot)
        .map(|reason| match reason {
            bamboo_engine::session_app::types::ExecuteSyncReason::PendingQuestionMismatch => {
                ExecuteSyncReason::PendingQuestionMismatch
            }
            bamboo_engine::session_app::types::ExecuteSyncReason::MessageCountMismatch => {
                ExecuteSyncReason::MessageCountMismatch
            }
            bamboo_engine::session_app::types::ExecuteSyncReason::LastMessageIdMismatch => {
                ExecuteSyncReason::LastMessageIdMismatch
            }
        })
}

#[test]
fn validate_and_normalize_model_treats_empty_value_as_absent() {
    assert_eq!(
        validate_and_normalize_model(Some("   ")).expect("empty model should normalize"),
        None
    );
}

#[test]
fn validate_and_normalize_model_trims_whitespace() {
    let model = validate_and_normalize_model(Some(" gpt-4o-mini ")).expect("model should be valid");
    assert_eq!(model.as_deref(), Some("gpt-4o-mini"));
}

#[test]
fn execute_response_payload_formats_status_and_events_url() {
    let payload = execute_response_payload(
        "session-123",
        "started",
        Some(ExecuteSyncInfo {
            need_sync: false,
            reason: None,
            server_message_count: 2,
            server_last_message_id: Some("msg-2".to_string()),
            has_pending_question: false,
            pending_question_tool_call_id: None,
            has_pending_user_message: true,
        }),
        None,
    );
    assert_eq!(payload.session_id, "session-123");
    assert_eq!(payload.status, "started");
    assert_eq!(payload.events_url, "/api/v1/events/session-123");
    assert!(payload.sync.is_some());
}

#[test]
fn evaluate_client_sync_accepts_matching_snapshot() {
    let server_snapshot = ServerExecuteSnapshot {
        message_count: 3,
        last_message_id: Some("msg-3".to_string()),
        has_pending_question: true,
        pending_question_tool_call_id: Some("tool-1".to_string()),
        has_pending_user_message: false,
    };
    let client_sync = ExecuteClientSync {
        client_message_count: 3,
        client_last_message_id: Some("msg-3".to_string()),
        client_has_pending_question: true,
        client_pending_question_tool_call_id: Some("tool-1".to_string()),
    };

    assert_eq!(
        evaluate_client_sync_adapter(Some(&client_sync), &server_snapshot),
        None
    );
}

#[test]
fn evaluate_client_sync_detects_message_count_mismatch() {
    let server_snapshot = ServerExecuteSnapshot {
        message_count: 4,
        last_message_id: Some("msg-4".to_string()),
        has_pending_question: false,
        pending_question_tool_call_id: None,
        has_pending_user_message: true,
    };
    let client_sync = ExecuteClientSync {
        client_message_count: 3,
        client_last_message_id: Some("msg-4".to_string()),
        client_has_pending_question: false,
        client_pending_question_tool_call_id: None,
    };

    assert_eq!(
        evaluate_client_sync_adapter(Some(&client_sync), &server_snapshot),
        Some(ExecuteSyncReason::MessageCountMismatch)
    );
}

#[test]
fn evaluate_client_sync_detects_last_message_id_mismatch() {
    let server_snapshot = ServerExecuteSnapshot {
        message_count: 4,
        last_message_id: Some("msg-4".to_string()),
        has_pending_question: false,
        pending_question_tool_call_id: None,
        has_pending_user_message: true,
    };
    let client_sync = ExecuteClientSync {
        client_message_count: 4,
        client_last_message_id: Some("msg-3".to_string()),
        client_has_pending_question: false,
        client_pending_question_tool_call_id: None,
    };

    assert_eq!(
        evaluate_client_sync_adapter(Some(&client_sync), &server_snapshot),
        Some(ExecuteSyncReason::LastMessageIdMismatch)
    );
}

#[test]
fn evaluate_client_sync_detects_pending_question_mismatch() {
    let server_snapshot = ServerExecuteSnapshot {
        message_count: 4,
        last_message_id: Some("msg-4".to_string()),
        has_pending_question: true,
        pending_question_tool_call_id: Some("tool-2".to_string()),
        has_pending_user_message: false,
    };
    let client_sync = ExecuteClientSync {
        client_message_count: 4,
        client_last_message_id: Some("msg-4".to_string()),
        client_has_pending_question: true,
        client_pending_question_tool_call_id: Some("tool-1".to_string()),
    };

    assert_eq!(
        evaluate_client_sync_adapter(Some(&client_sync), &server_snapshot),
        Some(ExecuteSyncReason::PendingQuestionMismatch)
    );
}

#[test]
fn evaluate_client_sync_allows_missing_pending_question_tool_call_id() {
    let server_snapshot = ServerExecuteSnapshot {
        message_count: 4,
        last_message_id: Some("msg-4".to_string()),
        has_pending_question: true,
        pending_question_tool_call_id: Some("tool-2".to_string()),
        has_pending_user_message: false,
    };
    let client_sync = ExecuteClientSync {
        client_message_count: 4,
        client_last_message_id: Some("msg-4".to_string()),
        client_has_pending_question: true,
        client_pending_question_tool_call_id: None,
    };

    assert_eq!(
        evaluate_client_sync_adapter(Some(&client_sync), &server_snapshot),
        None
    );
}

#[actix_web::test]
async fn startup_failure_persists_and_broadcasts_for_owned_turn() {
    use actix_web::web;
    use bamboo_agent_core::{AgentEvent, Message, Session};

    let dir = tempfile::tempdir().expect("temporary app data");
    let state = web::Data::new(
        crate::AppState::new(dir.path().to_path_buf())
            .await
            .expect("app state"),
    );
    let session_id = "owned-startup-failure";
    let mut session = Session::new(session_id, "test-model");
    session.add_message(Message::user("start"));
    crate::handlers::agent::events::mark_pending_turn(&mut session);
    let turn_id = crate::handlers::agent::events::startup_work_id(&session).unwrap();
    state.save_and_cache_session(&mut session).await;
    let mut receiver = state.get_session_event_sender(session_id).await.subscribe();
    let mut startup_guard =
        crate::handlers::agent::events::begin_execute_startup(state.get_ref(), session_id);

    super::fail_pending_startup(
        &state,
        session_id,
        Some(&turn_id),
        "provider rejected",
        &mut startup_guard,
    )
    .await;

    let stored = state
        .storage
        .load_session(session_id)
        .await
        .expect("load session")
        .expect("stored session");
    assert_eq!(stored.last_run_status().as_deref(), Some("error"));
    assert!(stored
        .last_run_error()
        .is_some_and(|message| message.contains("provider rejected")));
    assert!(matches!(
        receiver.try_recv(),
        Ok(AgentEvent::Error { message }) if message.contains("provider rejected")
    ));
}

#[actix_web::test]
async fn pending_or_running_runner_wins_over_same_work_id_failure() {
    use actix_web::web;
    use bamboo_agent_core::{Message, Session};

    for (suffix, status) in [
        ("pending", crate::app_state::AgentStatus::Pending),
        ("running", crate::app_state::AgentStatus::Running),
    ] {
        let dir = tempfile::tempdir().expect("temporary app data");
        let state = web::Data::new(
            crate::AppState::new(dir.path().to_path_buf())
                .await
                .expect("app state"),
        );
        let session_id = format!("runner-wins-{suffix}");
        let mut session = Session::new(&session_id, "test-model");
        session.add_message(Message::user("start"));
        crate::handlers::agent::events::mark_pending_turn(&mut session);
        let turn_id = crate::handlers::agent::events::startup_work_id(&session).unwrap();
        state.save_and_cache_session(&mut session).await;
        let mut receiver = state
            .get_session_event_sender(&session_id)
            .await
            .subscribe();
        let mut runner = crate::app_state::AgentRunner::new();
        runner.status = status;
        state
            .agent_runners
            .write()
            .await
            .insert(session_id.clone(), runner);
        let mut startup_guard =
            crate::handlers::agent::events::begin_execute_startup(state.get_ref(), &session_id);

        super::fail_pending_startup(
            &state,
            &session_id,
            Some(&turn_id),
            "overlapping rejection",
            &mut startup_guard,
        )
        .await;

        let stored = state
            .storage
            .load_session(&session_id)
            .await
            .expect("load session")
            .expect("stored session");
        assert_eq!(stored.last_run_status().as_deref(), Some("pending"));
        assert_eq!(
            crate::handlers::agent::events::startup_work_id(&stored).as_deref(),
            Some(turn_id.as_str())
        );
        assert!(receiver.try_recv().is_err(), "live runner must stay silent");
    }
}

#[actix_web::test]
async fn overlapping_preparation_failure_defers_to_other_owner_and_runner() {
    use actix_web::web;
    use bamboo_agent_core::{Message, Session};

    let dir = tempfile::tempdir().expect("temporary app data");
    let state = web::Data::new(
        crate::AppState::new(dir.path().to_path_buf())
            .await
            .expect("app state"),
    );
    let session_id = "overlap-prep-runner-wins";
    let mut session = Session::new(session_id, "test-model");
    session.add_message(Message::user("start"));
    crate::handlers::agent::events::mark_pending_turn(&mut session);
    let turn_id = crate::handlers::agent::events::startup_work_id(&session).unwrap();
    state.save_and_cache_session(&mut session).await;
    let mut receiver = state.get_session_event_sender(session_id).await.subscribe();
    let mut first =
        crate::handlers::agent::events::begin_execute_startup(state.get_ref(), session_id);
    let second = crate::handlers::agent::events::begin_execute_startup(state.get_ref(), session_id);

    super::fail_pending_startup(
        &state,
        session_id,
        Some(&turn_id),
        "first preparation rejected",
        &mut first,
    )
    .await;
    assert!(receiver.try_recv().is_err());

    let mut runner = crate::app_state::AgentRunner::new();
    runner.status = crate::app_state::AgentStatus::Pending;
    state
        .agent_runners
        .write()
        .await
        .insert(session_id.to_string(), runner);
    drop(second);

    let stored = state
        .storage
        .load_session(session_id)
        .await
        .expect("load session")
        .expect("stored session");
    assert_eq!(stored.last_run_status().as_deref(), Some("pending"));
    assert_eq!(
        crate::handlers::agent::events::startup_work_id(&stored).as_deref(),
        Some(turn_id.as_str())
    );
    assert!(
        receiver.try_recv().is_err(),
        "other preparation owns startup"
    );
}

#[actix_web::test]
async fn last_of_two_preparation_failures_broadcasts_exactly_once() {
    use actix_web::web;
    use bamboo_agent_core::{AgentEvent, Message, Session};

    let dir = tempfile::tempdir().expect("temporary app data");
    let state = web::Data::new(
        crate::AppState::new(dir.path().to_path_buf())
            .await
            .expect("app state"),
    );
    let session_id = "overlap-prep-both-fail";
    let mut session = Session::new(session_id, "test-model");
    session.add_message(Message::user("start"));
    crate::handlers::agent::events::mark_pending_turn(&mut session);
    let turn_id = crate::handlers::agent::events::startup_work_id(&session).unwrap();
    state.save_and_cache_session(&mut session).await;
    let mut receiver = state.get_session_event_sender(session_id).await.subscribe();
    let mut first =
        crate::handlers::agent::events::begin_execute_startup(state.get_ref(), session_id);
    let mut second =
        crate::handlers::agent::events::begin_execute_startup(state.get_ref(), session_id);

    super::fail_pending_startup(
        &state,
        session_id,
        Some(&turn_id),
        "first preparation rejected",
        &mut first,
    )
    .await;
    assert!(receiver.try_recv().is_err(), "first owner must defer");
    super::fail_pending_startup(
        &state,
        session_id,
        Some(&turn_id),
        "second preparation rejected",
        &mut second,
    )
    .await;

    assert!(matches!(
        receiver.try_recv(),
        Ok(AgentEvent::Error { message }) if message.contains("second preparation rejected")
    ));
    assert!(
        receiver.try_recv().is_err(),
        "only one failure is broadcast"
    );
    let stored = state
        .storage
        .load_session(session_id)
        .await
        .expect("load session")
        .expect("stored session");
    assert_eq!(stored.last_run_status().as_deref(), Some("error"));
    assert!(crate::handlers::agent::events::startup_work_id(&stored).is_none());
}

#[actix_web::test]
async fn startup_failure_waiting_on_lock_cannot_overwrite_newer_turn() {
    use actix_web::web;
    use bamboo_agent_core::{Message, Session};

    let dir = tempfile::tempdir().expect("temporary app data");
    let state = web::Data::new(
        crate::AppState::new(dir.path().to_path_buf())
            .await
            .expect("app state"),
    );
    let session_id = "stale-startup-failure";
    let mut session = Session::new(session_id, "test-model");
    session.add_message(Message::user("first"));
    crate::handlers::agent::events::mark_pending_turn(&mut session);
    let stale_turn_id = crate::handlers::agent::events::startup_work_id(&session).unwrap();
    state.save_and_cache_session(&mut session).await;
    let mut receiver = state.get_session_event_sender(session_id).await.subscribe();

    // Model a newer chat writer already inside the canonical per-session lock.
    // The stale preparation failure starts and blocks behind it.
    let chat_guard = state.persistence.acquire_lock(session_id).await;
    let failure_state = state.clone();
    let mut failure_guard =
        crate::handlers::agent::events::begin_execute_startup(state.get_ref(), session_id);
    let failure = tokio::spawn(async move {
        super::fail_pending_startup(
            &failure_state,
            session_id,
            Some(&stale_turn_id),
            "late rejection",
            &mut failure_guard,
        )
        .await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    assert!(
        !failure.is_finished(),
        "stale failure must be waiting on the session lock"
    );

    session.add_message(Message::user("newer"));
    crate::handlers::agent::events::mark_pending_turn(&mut session);
    let current_turn_id = crate::handlers::agent::events::startup_work_id(&session).unwrap();
    state
        .storage
        .save_session(&session)
        .await
        .expect("persist newer turn while owning session lock");
    state.sessions.insert(
        session_id.to_string(),
        std::sync::Arc::new(parking_lot::RwLock::new(session)),
    );
    drop(chat_guard);
    failure.await.expect("failure task completes");

    let stored = state
        .storage
        .load_session(session_id)
        .await
        .expect("load session")
        .expect("stored session");
    assert_eq!(
        crate::handlers::agent::events::startup_work_id(&stored).as_deref(),
        Some(current_turn_id.as_str())
    );
    assert_eq!(stored.last_run_status().as_deref(), Some("pending"));
    assert!(receiver.try_recv().is_err(), "stale failure must be silent");
}
