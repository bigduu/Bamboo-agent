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

// ----- resolve_system_prompt: PR-3 prompt routing contract --------------

#[test]
fn resolve_system_prompt_uses_override_verbatim() {
    let custom = "You are a custom subagent.";
    let prompt = resolve_system_prompt("anything", Some(custom));
    assert_eq!(prompt.as_ref(), custom);
}

#[test]
fn resolve_system_prompt_uses_override_even_when_subagent_type_is_plan() {
    // Override beats legacy plan routing: this is what lets the registry
    // actually swap the plan agent's prompt.
    let custom = "Plan override";
    let prompt = resolve_system_prompt("plan", Some(custom));
    assert_eq!(prompt.as_ref(), custom);
}

#[test]
fn resolve_system_prompt_falls_back_to_plan_for_plan_subagent_type() {
    let prompt = resolve_system_prompt("plan", None);
    assert_eq!(prompt.as_ref(), PLAN_AGENT_SYSTEM_PROMPT);
}

#[test]
fn resolve_system_prompt_plan_match_is_case_and_whitespace_insensitive() {
    let prompt = resolve_system_prompt("  PLAN  ", None);
    assert_eq!(prompt.as_ref(), PLAN_AGENT_SYSTEM_PROMPT);
}

#[test]
fn resolve_system_prompt_falls_back_to_general_for_unknown_subagent_type() {
    let prompt = resolve_system_prompt("researcher", None);
    assert_eq!(prompt.as_ref(), CHILD_SYSTEM_PROMPT);
}

#[test]
fn resolve_system_prompt_falls_back_to_general_for_empty_subagent_type() {
    let prompt = resolve_system_prompt("", None);
    assert_eq!(prompt.as_ref(), CHILD_SYSTEM_PROMPT);
}
