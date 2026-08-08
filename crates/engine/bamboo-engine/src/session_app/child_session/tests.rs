use super::*;

#[test]
fn truncate_after_last_user_removes_assistant_tail() {
    let mut session = Session::new_child("child", "root", "test-model", "Child");
    session.add_message(bamboo_agent_core::Message::system("system"));
    session.add_message(bamboo_agent_core::Message::user("task"));
    session.add_message(bamboo_agent_core::Message::assistant("done", None));

    let removed = truncate_after_last_user(&mut session).expect("truncate should work");

    assert_eq!(removed, 1);
    assert_eq!(session.messages.len(), 2);
    assert!(matches!(
        session.messages[1].role,
        bamboo_agent_core::Role::User
    ));
}

#[test]
fn replace_or_append_last_user_message_replaces_existing() {
    let mut session = Session::new_child("child", "root", "test-model", "Child");
    session.add_message(bamboo_agent_core::Message::user("old"));
    session.add_message(bamboo_agent_core::Message::assistant("tail", None));

    let idx = replace_or_append_last_user_message(&mut session, "new".to_string());

    assert_eq!(idx, 0);
    assert_eq!(session.messages[0].content, "new");
    assert_eq!(session.messages.len(), 2);
}

#[test]
fn replace_last_user_message_marks_existing_model_history_for_rollback() {
    let mut session = Session::new_child("child", "root", "test-model", "Child");
    session.add_message(bamboo_agent_core::Message::user("old"));
    session.model_context_state = Some(bamboo_domain::ModelContextState {
        prefix_epoch: 2,
        cache_scope_sha256: Some("scope".to_string()),
        ..bamboo_domain::ModelContextState::default()
    });

    replace_or_append_last_user_message(&mut session, "new".to_string());

    let state = session.model_context_state.as_ref().unwrap();
    assert_eq!(state.prefix_epoch, 3);
    assert_eq!(
        state.last_reset_reason,
        Some(bamboo_domain::ModelContextResetReason::Rollback)
    );
}

#[test]
fn normalize_non_empty_optional_rejects_blank_strings() {
    let err = normalize_non_empty_optional(Some("  ".to_string()), "prompt")
        .expect_err("blank should be rejected");
    assert!(matches!(err, ChildSessionError::InvalidArguments(msg) if msg.contains("prompt")));
}

#[test]
fn format_child_assignment_builds_expected_string() {
    let result = format_child_assignment("Title", "Responsibility", "Type", "Task brief");
    assert!(result.contains("Title"));
    assert!(result.contains("Responsibility"));
    assert!(result.contains("Type"));
    assert!(result.contains("Task brief"));
}
