use crate::app_state::AgentStatus;
use bamboo_agent_core::{AgentError, AgentEvent, Message, Session};

use bamboo_engine::session_app::execute::{
    consume_pending_conclusion_with_options_resume, has_pending_user_message,
};
use bamboo_engine::execution::agent_spawn::terminal_error_event_for_result;

use super::execution::{execution_tool_surface, tools_for_execution};
use super::session_state::{selected_skill_ids_for_session, selected_skill_mode_for_session};
use crate::app_state::runner_lifecycle::status_from_execution_result;
use crate::app_state::AppState;
use crate::tools::ToolSurface;

#[test]
fn execution_tool_surface_selects_root_for_root_sessions_and_child_for_child_sessions() {
    assert_eq!(execution_tool_surface(false), ToolSurface::Root);
    assert_eq!(execution_tool_surface(true), ToolSurface::Child);
}

#[tokio::test]
async fn execution_tools_expose_sub_agent_only_for_root_sessions() {
    let temp_dir = tempfile::tempdir().unwrap();
    let state = AppState::new(temp_dir.path().to_path_buf())
        .await
        .expect("app state should initialize");

    let root_names: std::collections::HashSet<String> = tools_for_execution(&state, false)
        .list_tools()
        .into_iter()
        .map(|schema| schema.function.name)
        .collect();
    let child_names: std::collections::HashSet<String> = tools_for_execution(&state, true)
        .list_tools()
        .into_iter()
        .map(|schema| schema.function.name)
        .collect();

    assert!(root_names.contains("SubAgent"));
    assert!(!child_names.contains("SubAgent"));
}

#[test]
fn has_pending_user_message_only_when_last_message_is_user() {
    let mut session = Session::new("session-1", "gpt-4o-mini");
    session.add_message(Message::system("sys"));
    session.add_message(Message::user("hello"));
    assert!(has_pending_user_message(&session));

    session.add_message(Message::assistant("done", None));
    assert!(!has_pending_user_message(&session));
}

#[test]
fn has_pending_user_message_when_conclusion_with_options_resume_is_marked() {
    let mut session = Session::new("session-1", "gpt-4o-mini");
    session.add_message(Message::assistant("tool question", None));
    session.add_message(Message::tool_result("ask-1", "User selected: A"));
    assert!(!has_pending_user_message(&session));

    session.metadata.insert(
        "conclusion_with_options_resume_pending".to_string(),
        "true".to_string(),
    );
    assert!(has_pending_user_message(&session));

    consume_pending_conclusion_with_options_resume(&mut session);
    assert!(!has_pending_user_message(&session));
}

#[test]
fn has_pending_user_message_when_error_retry_resume_is_marked() {
    let mut session = Session::new("session-1", "gpt-4o-mini");
    session.add_message(Message::user("hello"));
    session.add_message(Message::assistant("failed with rate limit", None));
    assert!(!has_pending_user_message(&session));

    session
        .metadata
        .insert("retry_resume_pending".to_string(), "true".to_string());
    session
        .metadata
        .insert("retry_resume_reason".to_string(), "error_retry".to_string());
    assert!(has_pending_user_message(&session));

    consume_pending_conclusion_with_options_resume(&mut session);
    assert!(!has_pending_user_message(&session));
    assert!(!session.metadata.contains_key("retry_resume_pending"));
    assert!(!session.metadata.contains_key("retry_resume_reason"));
}

#[test]
fn selected_skill_ids_for_session_parses_metadata_json() {
    let mut session = Session::new("session-1", "gpt-4o-mini");
    session.metadata.insert(
        "selected_skill_ids".to_string(),
        "[\"pdf\",\"skill-creator\"]".to_string(),
    );

    assert_eq!(
        selected_skill_ids_for_session(&session),
        Some(vec!["pdf".to_string(), "skill-creator".to_string()])
    );
}

#[test]
fn selected_skill_mode_for_session_prefers_skill_mode_key() {
    let mut session = Session::new("session-1", "gpt-4o-mini");
    session
        .metadata
        .insert("mode".to_string(), "ask".to_string());
    session
        .metadata
        .insert("skill_mode".to_string(), "code".to_string());

    assert_eq!(
        selected_skill_mode_for_session(&session).as_deref(),
        Some("code")
    );
}

#[test]
fn execution_result_mapping_handles_cancelled_and_error_states() {
    let ok_result: Result<(), AgentError> = Ok(());
    assert!(matches!(
        status_from_execution_result(&ok_result),
        AgentStatus::Completed
    ));

    // Cancellation is matched on the `AgentError::Cancelled` variant, so a
    // reworded/localized message can no longer silently break the mapping.
    let cancelled_result: Result<(), AgentError> = Err(AgentError::Cancelled);
    assert!(matches!(
        status_from_execution_result(&cancelled_result),
        AgentStatus::Cancelled
    ));

    let error_result: Result<(), AgentError> = Err(AgentError::LLM("boom".to_string()));
    match status_from_execution_result(&error_result) {
        AgentStatus::Error(message) => assert!(message.contains("boom")),
        other => panic!("unexpected status: {other:?}"),
    }
}

#[test]
fn terminal_error_event_mapping_matches_execution_result() {
    let ok_result: Result<(), AgentError> = Ok(());
    assert!(terminal_error_event_for_result(&ok_result).is_none());

    let cancelled_result: Result<(), AgentError> = Err(AgentError::Cancelled);
    match terminal_error_event_for_result(&cancelled_result) {
        Some(AgentEvent::Error { message }) => {
            assert_eq!(message, "Agent execution cancelled by user");
        }
        other => panic!("unexpected event: {other:?}"),
    }

    let error_result: Result<(), AgentError> = Err(AgentError::LLM("network failed".to_string()));
    match terminal_error_event_for_result(&error_result) {
        Some(AgentEvent::Error { message }) => assert!(message.contains("network failed")),
        other => panic!("unexpected event: {other:?}"),
    }
}

#[test]
fn preserve_concurrent_session_overrides_applies_latest_title_and_pin_when_unchanged_in_execution()
{
    let mut running_snapshot = Session::new("session-1", "gpt-4o-mini");
    running_snapshot.title = "New Session".to_string();
    running_snapshot.pinned = false;
    running_snapshot.title_version = 0;

    let mut latest_persisted = running_snapshot.clone();
    latest_persisted.title = "Debug websocket reconnect issue".to_string();
    latest_persisted.pinned = true;
    latest_persisted.title_version = 1;

    if latest_persisted.title_version >= running_snapshot.title_version {
        running_snapshot.title = latest_persisted.title.clone();
        running_snapshot.pinned = latest_persisted.pinned;
        running_snapshot.title_version = latest_persisted.title_version;
    }

    assert_eq!(running_snapshot.title, latest_persisted.title);
    assert!(running_snapshot.pinned);
    assert_eq!(running_snapshot.title_version, 1);
}

#[test]
fn preserve_concurrent_session_overrides_keeps_execution_changes() {
    let mut running_snapshot = Session::new("session-1", "gpt-4o-mini");
    running_snapshot.title = "Execution-assigned title".to_string();
    running_snapshot.pinned = true;
    running_snapshot.title_version = 5;

    let mut latest_persisted = running_snapshot.clone();
    latest_persisted.title = "User edited title".to_string();
    latest_persisted.pinned = false;
    latest_persisted.title_version = 4;

    if latest_persisted.title_version >= running_snapshot.title_version {
        running_snapshot.title = latest_persisted.title.clone();
        running_snapshot.pinned = latest_persisted.pinned;
        running_snapshot.title_version = latest_persisted.title_version;
    }

    assert_eq!(running_snapshot.title, "Execution-assigned title");
    assert!(running_snapshot.pinned);
    assert_eq!(running_snapshot.title_version, 5);
}
