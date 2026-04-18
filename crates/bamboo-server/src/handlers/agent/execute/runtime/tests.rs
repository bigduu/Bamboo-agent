use bamboo_application_agent::{AgentEvent, Message, Session};
use crate::app_state::AgentStatus;

use bamboo_application_runtime::execution::agent_spawn::{
    preserve_concurrent_session_overrides, terminal_error_event_for_result,
};
use bamboo_application_session::execute::{
    consume_pending_conclusion_with_options_resume, has_pending_user_message,
};

use super::session_state::{
    selected_skill_ids_for_session, selected_skill_mode_for_session,
};
use crate::app_state::runner_lifecycle::status_from_execution_result;

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
    let ok_result: anyhow::Result<()> = Ok(());
    assert!(matches!(
        status_from_execution_result(&ok_result),
        AgentStatus::Completed
    ));

    let cancelled_result: anyhow::Result<()> = Err(anyhow::anyhow!("request cancelled"));
    assert!(matches!(
        status_from_execution_result(&cancelled_result),
        AgentStatus::Cancelled
    ));

    let error_result: anyhow::Result<()> = Err(anyhow::anyhow!("boom"));
    match status_from_execution_result(&error_result) {
        AgentStatus::Error(message) => assert!(message.contains("boom")),
        other => panic!("unexpected status: {other:?}"),
    }
}

#[test]
fn terminal_error_event_mapping_matches_execution_result() {
    let ok_result: anyhow::Result<()> = Ok(());
    assert!(terminal_error_event_for_result(&ok_result).is_none());

    let cancelled_result: anyhow::Result<()> = Err(anyhow::anyhow!("cancelled by user"));
    match terminal_error_event_for_result(&cancelled_result) {
        Some(AgentEvent::Error { message }) => {
            assert_eq!(message, "Agent execution cancelled by user");
        }
        other => panic!("unexpected event: {other:?}"),
    }

    let error_result: anyhow::Result<()> = Err(anyhow::anyhow!("network failed"));
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

    let mut latest_persisted = running_snapshot.clone();
    latest_persisted.title = "Debug websocket reconnect issue".to_string();
    latest_persisted.pinned = true;

    preserve_concurrent_session_overrides(
        &mut running_snapshot,
        &latest_persisted,
        "New Session",
        false,
    );

    assert_eq!(running_snapshot.title, latest_persisted.title);
    assert!(running_snapshot.pinned);
}

#[test]
fn preserve_concurrent_session_overrides_keeps_execution_changes() {
    let mut running_snapshot = Session::new("session-1", "gpt-4o-mini");
    running_snapshot.title = "Execution-assigned title".to_string();
    running_snapshot.pinned = true;

    let mut latest_persisted = running_snapshot.clone();
    latest_persisted.title = "User edited title".to_string();
    latest_persisted.pinned = false;

    preserve_concurrent_session_overrides(
        &mut running_snapshot,
        &latest_persisted,
        "New Session",
        false,
    );

    assert_eq!(running_snapshot.title, "Execution-assigned title");
    assert!(running_snapshot.pinned);
}
