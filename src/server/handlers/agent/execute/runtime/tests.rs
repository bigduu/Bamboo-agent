use crate::{
    agent::core::{AgentEvent, Message, Session},
    server::app_state::AgentStatus,
};

use super::{
    execution::{status_from_execution_result, terminal_error_event_for_result},
    session_state::{
        consume_pending_ask_user_resume, has_pending_user_message,
        initial_user_message_for_session, selected_skill_ids_for_session,
        system_prompt_for_session,
    },
};

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
fn has_pending_user_message_when_ask_user_resume_is_marked() {
    let mut session = Session::new("session-1", "gpt-4o-mini");
    session.add_message(Message::assistant("tool question", None));
    session.add_message(Message::tool_result("ask-1", "User selected: A"));
    assert!(!has_pending_user_message(&session));

    session
        .metadata
        .insert("ask_user_resume_pending".to_string(), "true".to_string());
    assert!(has_pending_user_message(&session));

    consume_pending_ask_user_resume(&mut session);
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

    consume_pending_ask_user_resume(&mut session);
    assert!(!has_pending_user_message(&session));
    assert!(!session.metadata.contains_key("retry_resume_pending"));
    assert!(!session.metadata.contains_key("retry_resume_reason"));
}

#[test]
fn session_prompt_extractors_select_expected_messages() {
    let mut session = Session::new("session-1", "gpt-4o-mini");
    session.add_message(Message::system("primary system prompt"));
    session.add_message(Message::user("first user"));
    session.add_message(Message::assistant("assistant", None));
    session.add_message(Message::user("latest user"));

    assert_eq!(
        system_prompt_for_session(&session).as_deref(),
        Some("primary system prompt")
    );
    assert_eq!(
        initial_user_message_for_session(&session),
        "latest user".to_string()
    );
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
