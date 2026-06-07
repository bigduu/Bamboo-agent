use super::*;
use crate::session_app::types::{ExecuteClientSync, ExecuteSyncReason};
use bamboo_agent_core::Message;
use bamboo_domain::ProviderModelRef;

fn make_session(model: &str) -> Session {
    let mut s = Session::new("test-session", model);
    // Add a user message so has_pending_user_message is true
    s.messages.push(bamboo_agent_core::Message::user("hello"));
    s
}

fn make_input() -> ExecuteInput {
    ExecuteInput {
        session_id: "test-session".to_string(),
        request_model: None,
        request_model_ref: None,
        request_provider: None,
        request_reasoning_effort: None,
        request_skill_mode: None,
        client_sync: None,
    }
}

fn make_config() -> ExecutionConfigSnapshot {
    ExecutionConfigSnapshot {
        provider_model_ref_enabled: false,
        ..Default::default()
    }
}

// ---- normalize_model ----

#[test]
fn normalize_model_some() {
    assert_eq!(normalize_model(Some("gpt-4")), Some("gpt-4".to_string()));
}

#[test]
fn normalize_model_trims_whitespace() {
    assert_eq!(
        normalize_model(Some("  gpt-4  ")),
        Some("gpt-4".to_string())
    );
}

#[test]
fn normalize_model_none() {
    assert_eq!(normalize_model(None), None);
}

#[test]
fn normalize_model_empty() {
    assert_eq!(normalize_model(Some("")), None);
}

#[test]
fn normalize_model_whitespace_only() {
    assert_eq!(normalize_model(Some("   ")), None);
}

#[test]
fn normalize_model_unknown() {
    assert_eq!(normalize_model(Some("unknown")), None);
}

// ---- resolve_model_cascade (flag OFF) ----

#[test]
fn cascade_old_prefers_session_model() {
    let session = make_session("claude-3");
    let input = make_input();
    let config = make_config();

    let (model, source) = resolve_model_cascade(&session, &input, &config);
    assert_eq!(model, Some("claude-3".to_string()));
    assert_eq!(source, "session");
}

#[test]
fn cascade_old_falls_back_to_config_default() {
    let session = make_session("unknown");
    let input = make_input();
    let mut config = make_config();
    config.default_model = Some("gpt-4o".to_string());

    let (model, source) = resolve_model_cascade(&session, &input, &config);
    assert_eq!(model, Some("gpt-4o".to_string()));
    assert_eq!(source, "provider_default");
}

#[test]
fn cascade_old_falls_back_to_request_model() {
    let session = make_session("unknown");
    let mut input = make_input();
    input.request_model = Some("gpt-4-turbo".to_string());
    let config = make_config();

    let (model, source) = resolve_model_cascade(&session, &input, &config);
    assert_eq!(model, Some("gpt-4-turbo".to_string()));
    assert_eq!(source, "request");
}

#[test]
fn cascade_old_no_model_returns_none() {
    let session = make_session("unknown");
    let input = make_input();
    let config = make_config();

    let (model, source) = resolve_model_cascade(&session, &input, &config);
    assert_eq!(model, None);
    assert_eq!(source, "none");
}

#[test]
fn cascade_old_session_overrides_request() {
    let session = make_session("claude-3");
    let mut input = make_input();
    input.request_model = Some("gpt-4".to_string());
    let config = make_config();

    let (model, source) = resolve_model_cascade(&session, &input, &config);
    assert_eq!(model, Some("claude-3".to_string()));
    assert_eq!(source, "session");
}

// ---- resolve_model_ref_cascade (flag ON) ----

#[test]
fn cascade_new_prefers_session_model_ref() {
    let mut session = make_session("unknown");
    session.model_ref = Some(ProviderModelRef::new("anthropic", "claude-3"));
    let input = make_input();
    let mut config = make_config();
    config.provider_model_ref_enabled = true;

    let (model_ref, model, source) = resolve_model_ref_cascade(&session, &input, &config);
    assert_eq!(
        model_ref,
        Some(ProviderModelRef::new("anthropic", "claude-3"))
    );
    assert_eq!(model, Some("claude-3".to_string()));
    assert_eq!(source, "session");
}

#[test]
fn cascade_new_falls_back_to_request_model_ref_before_config_default_ref() {
    let session = make_session("unknown");
    let mut input = make_input();
    input.request_model_ref = Some(ProviderModelRef::new("gemini", "gemini-pro"));
    let mut config = make_config();
    config.provider_model_ref_enabled = true;
    config.default_model_ref = Some(ProviderModelRef::new("openai", "gpt-4o"));

    let (model_ref, model, source) = resolve_model_ref_cascade(&session, &input, &config);
    assert_eq!(
        model_ref,
        Some(ProviderModelRef::new("gemini", "gemini-pro"))
    );
    assert_eq!(model, Some("gemini-pro".to_string()));
    assert_eq!(source, "request");
}

#[test]
fn cascade_new_falls_back_to_config_default_ref() {
    let session = make_session("unknown");
    let input = make_input();
    let mut config = make_config();
    config.provider_model_ref_enabled = true;
    config.default_model_ref = Some(ProviderModelRef::new("openai", "gpt-4o"));

    let (model_ref, model, source) = resolve_model_ref_cascade(&session, &input, &config);
    assert_eq!(model_ref, Some(ProviderModelRef::new("openai", "gpt-4o")));
    assert_eq!(model, Some("gpt-4o".to_string()));
    assert_eq!(source, "provider_default");
}

#[test]
fn cascade_new_falls_back_to_old_cascade_when_no_refs() {
    let mut session = make_session("claude-3");
    session.model_ref = None;
    let input = make_input();
    let mut config = make_config();
    config.provider_model_ref_enabled = true;

    let (model_ref, model, source) = resolve_model_ref_cascade(&session, &input, &config);
    assert_eq!(model_ref, None);
    assert_eq!(model, Some("claude-3".to_string()));
    assert_eq!(source, "session");
}

#[test]
fn cascade_new_session_ref_overrides_request_ref() {
    let mut session = make_session("unknown");
    session.model_ref = Some(ProviderModelRef::new("anthropic", "claude-3"));
    let mut input = make_input();
    input.request_model_ref = Some(ProviderModelRef::new("openai", "gpt-4o"));
    let mut config = make_config();
    config.provider_model_ref_enabled = true;

    let (model_ref, model, source) = resolve_model_ref_cascade(&session, &input, &config);
    assert_eq!(
        model_ref,
        Some(ProviderModelRef::new("anthropic", "claude-3"))
    );
    assert_eq!(model, Some("claude-3".to_string()));
    assert_eq!(source, "session");
}

#[test]
fn cascade_new_uses_session_provider_metadata_even_without_structured_ref() {
    let mut session = make_session("gpt-4o");
    session.model_ref = None;
    session
        .metadata
        .insert("provider_name".to_string(), "openai".to_string());
    let input = make_input();
    let mut config = make_config();
    config.provider_model_ref_enabled = true;

    let (model_ref, model, source) = resolve_model_ref_cascade(&session, &input, &config);
    assert_eq!(model_ref, Some(ProviderModelRef::new("openai", "gpt-4o")));
    assert_eq!(model, Some("gpt-4o".to_string()));
    assert_eq!(source, "session");
}

#[test]
fn cascade_new_no_model_anywhere_returns_none() {
    let session = make_session("unknown");
    let input = make_input();
    let mut config = make_config();
    config.provider_model_ref_enabled = true;

    let (model_ref, model, source) = resolve_model_ref_cascade(&session, &input, &config);
    assert_eq!(model_ref, None);
    assert_eq!(model, None);
    assert_eq!(source, "none");
}

// ---- evaluate_client_sync ----

#[test]
fn sync_none_when_no_client_sync() {
    let snapshot = ServerExecuteSnapshot {
        message_count: 1,
        last_message_id: Some("msg-1".to_string()),
        has_pending_question: false,
        pending_question_tool_call_id: None,
        has_pending_user_message: true,
    };
    assert_eq!(evaluate_client_sync(None, &snapshot), None);
}

#[test]
fn sync_mismatch_pending_question_flag() {
    let client_sync = ExecuteClientSync {
        client_message_count: 1,
        client_last_message_id: Some("msg-1".to_string()),
        client_has_pending_question: true,
        client_pending_question_tool_call_id: None,
    };
    let snapshot = ServerExecuteSnapshot {
        message_count: 1,
        last_message_id: Some("msg-1".to_string()),
        has_pending_question: false,
        pending_question_tool_call_id: None,
        has_pending_user_message: true,
    };
    assert_eq!(
        evaluate_client_sync(Some(&client_sync), &snapshot),
        Some(ExecuteSyncReason::PendingQuestionMismatch)
    );
}

#[test]
fn sync_mismatch_message_count() {
    let client_sync = ExecuteClientSync {
        client_message_count: 2,
        client_last_message_id: Some("msg-2".to_string()),
        client_has_pending_question: false,
        client_pending_question_tool_call_id: None,
    };
    let snapshot = ServerExecuteSnapshot {
        message_count: 1,
        last_message_id: Some("msg-1".to_string()),
        has_pending_question: false,
        pending_question_tool_call_id: None,
        has_pending_user_message: true,
    };
    assert_eq!(
        evaluate_client_sync(Some(&client_sync), &snapshot),
        Some(ExecuteSyncReason::MessageCountMismatch)
    );
}

#[test]
fn sync_mismatch_last_message_id() {
    let client_sync = ExecuteClientSync {
        client_message_count: 1,
        client_last_message_id: Some("msg-old".to_string()),
        client_has_pending_question: false,
        client_pending_question_tool_call_id: None,
    };
    let snapshot = ServerExecuteSnapshot {
        message_count: 1,
        last_message_id: Some("msg-new".to_string()),
        has_pending_question: false,
        pending_question_tool_call_id: None,
        has_pending_user_message: true,
    };
    assert_eq!(
        evaluate_client_sync(Some(&client_sync), &snapshot),
        Some(ExecuteSyncReason::LastMessageIdMismatch)
    );
}

#[test]
fn sync_ok_when_matching() {
    let client_sync = ExecuteClientSync {
        client_message_count: 1,
        client_last_message_id: Some("msg-1".to_string()),
        client_has_pending_question: false,
        client_pending_question_tool_call_id: None,
    };
    let snapshot = ServerExecuteSnapshot {
        message_count: 1,
        last_message_id: Some("msg-1".to_string()),
        has_pending_question: false,
        pending_question_tool_call_id: None,
        has_pending_user_message: true,
    };
    assert_eq!(evaluate_client_sync(Some(&client_sync), &snapshot), None);
}

#[test]
fn sync_ok_with_matching_pending_question_and_tool_call_id() {
    let client_sync = ExecuteClientSync {
        client_message_count: 2,
        client_last_message_id: Some("msg-2".to_string()),
        client_has_pending_question: true,
        client_pending_question_tool_call_id: Some("tc-1".to_string()),
    };
    let snapshot = ServerExecuteSnapshot {
        message_count: 2,
        last_message_id: Some("msg-2".to_string()),
        has_pending_question: true,
        pending_question_tool_call_id: Some("tc-1".to_string()),
        has_pending_user_message: false,
    };
    assert_eq!(evaluate_client_sync(Some(&client_sync), &snapshot), None);
}

#[test]
fn sync_mismatch_pending_question_tool_call_id() {
    let client_sync = ExecuteClientSync {
        client_message_count: 2,
        client_last_message_id: Some("msg-2".to_string()),
        client_has_pending_question: true,
        client_pending_question_tool_call_id: Some("tc-old".to_string()),
    };
    let snapshot = ServerExecuteSnapshot {
        message_count: 2,
        last_message_id: Some("msg-2".to_string()),
        has_pending_question: true,
        pending_question_tool_call_id: Some("tc-new".to_string()),
        has_pending_user_message: false,
    };
    assert_eq!(
        evaluate_client_sync(Some(&client_sync), &snapshot),
        Some(ExecuteSyncReason::PendingQuestionMismatch)
    );
}

// ---- has_pending_user_message ----

#[test]
fn pending_user_message_true_when_last_is_user() {
    let session = make_session("gpt-4");
    assert!(has_pending_user_message(&session));
}

#[test]
fn pending_user_message_false_when_last_is_assistant() {
    let mut session = make_session("gpt-4");
    session
        .messages
        .push(bamboo_agent_core::Message::assistant("response", None));
    assert!(!has_pending_user_message(&session));
}

#[test]
fn pending_user_message_false_when_empty() {
    let session = Session::new("test", "gpt-4");
    assert!(!has_pending_user_message(&session));
}

// ---- has_pending_conclusion_with_options_resume / has_pending_retry_resume ----

#[test]
fn conclusion_with_options_resume_true() {
    let mut session = Session::new("test", "gpt-4");
    session.metadata.insert(
        "conclusion_with_options_resume_pending".to_string(),
        "true".to_string(),
    );
    assert!(has_pending_conclusion_with_options_resume(&session));
}

#[test]
fn conclusion_with_options_resume_false_when_missing() {
    let session = Session::new("test", "gpt-4");
    assert!(!has_pending_conclusion_with_options_resume(&session));
}

#[test]
fn conclusion_with_options_resume_false_when_not_true() {
    let mut session = Session::new("test", "gpt-4");
    session.metadata.insert(
        "conclusion_with_options_resume_pending".to_string(),
        "false".to_string(),
    );
    assert!(!has_pending_conclusion_with_options_resume(&session));
}

#[test]
fn retry_resume_true() {
    let mut session = Session::new("test", "gpt-4");
    session
        .metadata
        .insert("retry_resume_pending".to_string(), "true".to_string());
    assert!(has_pending_retry_resume(&session));
}

#[test]
fn retry_resume_false_when_missing() {
    let session = Session::new("test", "gpt-4");
    assert!(!has_pending_retry_resume(&session));
}

// ---- consume_pending_conclusion_with_options_resume ----

#[test]
fn consume_removes_resume_metadata() {
    let mut session = Session::new("test", "gpt-4");
    session.metadata.insert(
        "conclusion_with_options_resume_pending".to_string(),
        "true".to_string(),
    );
    session
        .metadata
        .insert("retry_resume_pending".to_string(), "true".to_string());
    session
        .metadata
        .insert("retry_resume_reason".to_string(), "timeout".to_string());

    consume_pending_conclusion_with_options_resume(&mut session);

    assert!(!session
        .metadata
        .contains_key("conclusion_with_options_resume_pending"));
    assert!(!session.metadata.contains_key("retry_resume_pending"));
    assert!(!session.metadata.contains_key("retry_resume_reason"));
}

// ---- ServerExecuteSnapshot::from_session ----

#[test]
fn snapshot_from_session_counts_messages() {
    let mut session = Session::new("test", "gpt-4");
    session
        .messages
        .push(bamboo_agent_core::Message::user("hi"));
    session
        .messages
        .push(bamboo_agent_core::Message::assistant("hello", None));
    session.messages.last_mut().unwrap().id = "msg-2".to_string();

    let snapshot = ServerExecuteSnapshot::from_session(&session);
    assert_eq!(snapshot.message_count, 2);
    assert_eq!(snapshot.last_message_id, Some("msg-2".to_string()));
    assert!(!snapshot.has_pending_question);
    assert!(!snapshot.has_pending_user_message);
}

#[test]
fn snapshot_empty_session() {
    let session = Session::new("test", "gpt-4");
    let snapshot = ServerExecuteSnapshot::from_session(&session);

    assert_eq!(snapshot.message_count, 0);
    assert_eq!(snapshot.last_message_id, None);
    assert!(!snapshot.has_pending_question);
    assert!(!snapshot.has_pending_user_message);
}

#[test]
fn snapshot_excludes_hidden_user_message() {
    // Mirror the runtime resume injection: visible user, assistant reply,
    // then a hidden_from_ui synthetic user message. The client only ever
    // sees the first two via /history, so the snapshot must agree.
    let mut session = Session::new("test", "gpt-4");
    session.messages.push(Message::user("hi"));
    session.messages.last_mut().unwrap().id = "msg-1".to_string();
    session.messages.push(Message::assistant("hello", None));
    session.messages.last_mut().unwrap().id = "msg-2".to_string();
    let mut hidden = make_user_with_metadata(
        "runtime",
        serde_json::json!({
            "hidden_from_ui": true,
            "runtime_kind": "child_completion_resume",
        }),
    );
    hidden.id = "msg-3".to_string();
    session.messages.push(hidden);

    let snapshot = ServerExecuteSnapshot::from_session(&session);
    assert_eq!(snapshot.message_count, 2);
    assert_eq!(snapshot.last_message_id, Some("msg-2".to_string()));
    // Raw last is a (hidden) user message, so the runtime still has work
    // to do — the snapshot must keep this true so resume can proceed.
    assert!(snapshot.has_pending_user_message);
}

#[test]
fn snapshot_excludes_trailing_hidden_message_for_last_id() {
    // If the trailing message is hidden, last_message_id must come from
    // the last visible message — otherwise the client (which never saw
    // the hidden id) will report LastMessageIdMismatch on every execute.
    let mut session = Session::new("test", "gpt-4");
    session.messages.push(Message::user("hi"));
    session.messages.last_mut().unwrap().id = "visible-user".to_string();
    session.messages.push(Message::assistant("hello", None));
    session.messages.last_mut().unwrap().id = "visible-assistant".to_string();
    let mut hidden =
        make_user_with_metadata("runtime", serde_json::json!({ "hidden_from_ui": true }));
    hidden.id = "hidden-tail".to_string();
    session.messages.push(hidden);

    let snapshot = ServerExecuteSnapshot::from_session(&session);
    assert_eq!(
        snapshot.last_message_id,
        Some("visible-assistant".to_string())
    );
}

#[test]
fn evaluate_client_sync_passes_with_hidden_messages_filtered() {
    // Regression for the "Execute remains out-of-sync after 2 recovery
    // attempt(s)" loop: server has 1 hidden runtime message appended
    // after the visible turn, the client only sees the visible turn,
    // and execute used to return MessageCountMismatch forever.
    let mut session = Session::new("test", "gpt-4");
    session.messages.push(Message::user("hi"));
    session.messages.last_mut().unwrap().id = "msg-1".to_string();
    session.messages.push(Message::assistant("hello", None));
    session.messages.last_mut().unwrap().id = "msg-2".to_string();
    let mut hidden =
        make_user_with_metadata("runtime", serde_json::json!({ "hidden_from_ui": true }));
    hidden.id = "msg-3".to_string();
    session.messages.push(hidden);

    let snapshot = ServerExecuteSnapshot::from_session(&session);
    let client_sync = ExecuteClientSync {
        client_message_count: 2,
        client_last_message_id: Some("msg-2".to_string()),
        client_has_pending_question: false,
        client_pending_question_tool_call_id: None,
    };

    assert_eq!(evaluate_client_sync(Some(&client_sync), &snapshot), None);
}

#[test]
fn snapshot_visibility_matches_history_filter() {
    // The bug this whole change fixes was a *consistency* failure: the
    // /history endpoint and the execute sync snapshot were using
    // different rules to decide which messages are visible to the
    // client. Lock that rule in by computing the visible view both
    // ways from the same session and asserting they agree on
    // count and last id.
    let mut session = Session::new("test", "gpt-4");
    let mut visible_user = Message::user("hello");
    visible_user.id = "v-1".to_string();
    session.messages.push(visible_user);
    let mut hidden_a =
        make_user_with_metadata("runtime", serde_json::json!({ "hidden_from_ui": true }));
    hidden_a.id = "h-1".to_string();
    session.messages.push(hidden_a);
    let mut visible_assistant = Message::assistant("world", None);
    visible_assistant.id = "v-2".to_string();
    session.messages.push(visible_assistant);
    let mut hidden_b = make_user_with_metadata(
        "runtime",
        serde_json::json!({
            "hidden_from_ui": true,
            "runtime_kind": "retry_resume",
        }),
    );
    hidden_b.id = "h-2".to_string();
    session.messages.push(hidden_b);

    // Reproduce the /history filter using the same shared helper.
    let history_visible: Vec<&Message> = session
        .messages
        .iter()
        .filter(|m| !is_hidden_from_ui(m))
        .collect();

    let snapshot = ServerExecuteSnapshot::from_session(&session);
    assert_eq!(snapshot.message_count, history_visible.len());
    assert_eq!(
        snapshot.last_message_id,
        history_visible.last().map(|m| m.id.clone())
    );
}

// ---- billing helpers ----

fn make_user_with_metadata(content: &str, metadata: serde_json::Value) -> Message {
    let mut msg = Message::user(content);
    msg.metadata = Some(metadata);
    msg
}

#[test]
fn is_system_resume_detects_hidden_from_ui_flag() {
    let msg = make_user_with_metadata("runtime", serde_json::json!({ "hidden_from_ui": true }));
    assert!(is_system_resume_message(&msg));
    assert!(!is_billable_user_turn(&msg));
}

#[test]
fn is_system_resume_detects_known_runtime_kinds() {
    for kind in [
        "child_completion_resume",
        "retry_resume",
        "conclusion_with_options_resume",
        "gold_continue_resume",
        "gold_goal_resume",
    ] {
        let msg = make_user_with_metadata("runtime", serde_json::json!({ "runtime_kind": kind }));
        assert!(
            is_system_resume_message(&msg),
            "expected runtime_kind={kind} to be detected"
        );
        assert!(!is_billable_user_turn(&msg));
    }
}

#[test]
fn is_system_resume_ignores_unknown_runtime_kinds() {
    let msg = make_user_with_metadata(
        "runtime",
        serde_json::json!({ "runtime_kind": "something_else" }),
    );
    assert!(!is_system_resume_message(&msg));
    assert!(is_billable_user_turn(&msg));
}

#[test]
fn is_system_resume_returns_false_for_plain_user_message() {
    let msg = Message::user("hello");
    assert!(!is_system_resume_message(&msg));
    assert!(is_billable_user_turn(&msg));
}

#[test]
fn is_system_resume_returns_false_for_assistant_messages() {
    let msg = Message::assistant("hi", None);
    assert!(!is_system_resume_message(&msg));
    assert!(!is_billable_user_turn(&msg));
}

#[test]
fn billable_user_turn_count_skips_runtime_messages() {
    let mut session = Session::new("test", "gpt-4");
    session.messages.push(Message::user("first"));
    session.messages.push(Message::assistant("response", None));
    session.messages.push(make_user_with_metadata(
        "runtime",
        serde_json::json!({
            "hidden_from_ui": true,
            "runtime_kind": "child_completion_resume",
        }),
    ));
    session
        .messages
        .push(Message::assistant("response 2", None));
    session.messages.push(Message::user("second"));

    assert_eq!(billable_user_turn_count(&session), 2);
}
