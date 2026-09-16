use std::sync::Arc;

use bamboo_agent_core::{
    FunctionCall, Message, Session, Storage, Tool, ToolCall, ToolCtx, ToolError, ToolOutcome,
};
use bamboo_server_tools::SessionInspectorTool;
use bamboo_storage::SessionStoreV2;
use serde_json::{json, Value};

fn context(session_id: &str, tool_call_id: &str) -> ToolCtx {
    let mut context = ToolCtx::none(tool_call_id);
    context.session_id = Some(Arc::from(session_id));
    context
}

fn call(id: &str, name: &str, arguments: Value) -> ToolCall {
    ToolCall {
        id: id.to_string(),
        tool_type: "function".to_string(),
        function: FunctionCall {
            name: name.to_string(),
            arguments: arguments.to_string(),
        },
    }
}

fn history_call(id: &str, action: &str, message_id: Option<&str>) -> ToolCall {
    let arguments = match message_id {
        Some(message_id) => json!({"action":action,"message_id":message_id}),
        None => json!({"action":action}),
    };
    call(id, "session_history", arguments)
}

fn identified(mut message: Message, id: &str) -> Message {
    message.id = id.to_string();
    message
}

fn archived(mut message: Message) -> Message {
    message.compressed = true;
    message
}

fn completed(outcome: ToolOutcome) -> Value {
    let ToolOutcome::Completed(result) = outcome else {
        panic!("expected completed tool outcome")
    };
    assert!(result.success, "{}", result.result);
    serde_json::from_str(&result.result).expect("tool result JSON")
}

fn returned_message_ids(result: &Value) -> Vec<String> {
    result["turns"]
        .as_array()
        .expect("turns")
        .iter()
        .flat_map(|turn| turn["messages"].as_array().expect("turn messages"))
        .map(|message| message["id"].as_str().expect("message id").to_string())
        .collect()
}

#[tokio::test]
async fn read_around_returns_complete_anchor_chain_and_chronological_neighbors_without_mutation() {
    let home = tempfile::tempdir().unwrap();
    let store = Arc::new(
        SessionStoreV2::new(home.path().to_path_buf())
            .await
            .unwrap(),
    );
    let mut session = Session::new("around-exact-session", "test-model");
    session.add_message(identified(
        Message::system("host context"),
        "system-message",
    ));
    session.add_message(archived(identified(
        Message::user("archived neighbor request"),
        "before-user",
    )));
    session.add_message(archived(identified(
        Message::assistant("archived neighbor answer", None),
        "before-answer",
    )));
    session.add_message(identified(Message::user("anchor request"), "anchor-user"));
    session.add_message(identified(
        Message::assistant(
            "run exact tools",
            Some(vec![
                call(
                    "anchor-call-one",
                    "Bash",
                    json!({"command":"printf exact-anchor"}),
                ),
                call(
                    "anchor-call-two",
                    "Read",
                    json!({"path":"/tmp/exact-anchor","offset":3}),
                ),
            ]),
        ),
        "anchor-call-message",
    ));
    session.add_message(identified(
        Message::tool_result("anchor-call-one", "exact first result"),
        "anchor-result-one",
    ));
    session.add_message(identified(
        Message::tool_result("anchor-call-two", "exact second result"),
        "anchor-result-two",
    ));
    session.add_message(identified(
        Message::assistant("anchor conclusion", None),
        "anchor-final",
    ));
    session.add_message(identified(
        Message::user("active neighbor request"),
        "after-user",
    ));
    session.add_message(identified(
        Message::assistant("active neighbor answer", None),
        "after-answer",
    ));
    session.add_message(identified(
        Message::user("current request must stay hidden"),
        "current-request",
    ));
    for (call_id, action, message_id, result_id, result) in [
        (
            "generated-search",
            "search_current",
            None,
            "generated-search-result",
            "GENERATED-SEARCH-EVIDENCE",
        ),
        (
            "generated-read",
            "read_current",
            None,
            "generated-read-result",
            "GENERATED-READ-EVIDENCE",
        ),
        (
            "generated-around",
            "read_around",
            Some("anchor-result-one"),
            "generated-around-result",
            "GENERATED-AROUND-EVIDENCE",
        ),
    ] {
        session.add_message(identified(
            Message::assistant("", Some(vec![history_call(call_id, action, message_id)])),
            &format!("{call_id}-message"),
        ));
        session.add_message(identified(Message::tool_result(call_id, result), result_id));
    }
    session.add_message(identified(
        Message::assistant(
            "",
            Some(vec![history_call(
                "current-around",
                "read_around",
                Some("anchor-result-one"),
            )]),
        ),
        "current-around-message",
    ));
    store.save_session(&session).await.unwrap();
    let before = serde_json::to_value(store.load_session(&session.id).await.unwrap().unwrap())
        .expect("serialize before read");

    let self_tool = SessionInspectorTool::self_only(store.clone(), store.clone());
    let args = json!({
        "action":"read_around",
        "message_id":"anchor-result-one",
        "before_turns":1,
        "after_turns":1,
        "max_chars":20000,
    });
    let result = completed(
        self_tool
            .invoke(args.clone(), context(&session.id, "current-around"))
            .await
            .unwrap(),
    );
    assert_eq!(result["available"], true);
    assert_eq!(result["complete"], true);
    assert_eq!(result["truncated"], false);
    assert_eq!(result["anchor_message_id"], "anchor-result-one");
    assert_eq!(result["anchor_turn_id"], "anchor-user");
    assert_eq!(result["returned_before_turn_count"], 1);
    assert_eq!(result["returned_after_turn_count"], 1);
    assert_eq!(result["returned_turn_count"], 3);
    assert_eq!(
        returned_message_ids(&result),
        vec![
            "before-user",
            "before-answer",
            "anchor-user",
            "anchor-call-message",
            "anchor-result-one",
            "anchor-result-two",
            "anchor-final",
            "after-user",
            "after-answer",
        ]
    );
    assert_eq!(result["turns"][0]["contains_archived"], true);
    assert_eq!(
        result["turns"][1]["messages"][1]["tool_calls"][0]["function"]["arguments"],
        json!({"command":"printf exact-anchor"}).to_string()
    );
    assert_eq!(
        result["turns"][1]["messages"][3]["content"],
        "exact second result"
    );
    for hidden in [
        "host context",
        "current request must stay hidden",
        "GENERATED-SEARCH-EVIDENCE",
        "GENERATED-READ-EVIDENCE",
        "GENERATED-AROUND-EVIDENCE",
        "current-around-message",
    ] {
        assert!(!result.to_string().contains(hidden), "leaked {hidden}");
    }

    let root_tool = SessionInspectorTool::new(store.clone(), store.clone());
    let root_result = completed(
        root_tool
            .invoke(args, context(&session.id, "current-around"))
            .await
            .unwrap(),
    );
    assert_eq!(root_result, result);

    let after = serde_json::to_value(store.load_session(&session.id).await.unwrap().unwrap())
        .expect("serialize after read");
    assert_eq!(after, before, "read_around must not mutate Session state");
}

#[tokio::test]
async fn unavailable_anchors_share_one_non_distinguishing_result() {
    let home = tempfile::tempdir().unwrap();
    let store = Arc::new(
        SessionStoreV2::new(home.path().to_path_buf())
            .await
            .unwrap(),
    );
    let mut session = Session::new("around-unavailable-session", "test-model");
    session.add_message(identified(Message::system("system"), "system-anchor"));
    session.add_message(identified(Message::user("valid"), "valid-user"));
    session.add_message(identified(
        Message::assistant("valid answer", None),
        "valid-answer",
    ));
    session.add_message(identified(
        Message::user("malformed protocol turn"),
        "malformed-user",
    ));
    session.add_message(identified(
        Message::assistant(
            "incomplete",
            Some(vec![call(
                "missing-result",
                "Bash",
                json!({"command":"missing"}),
            )]),
        ),
        "incomplete-call-anchor",
    ));
    session.add_message(identified(
        Message::tool_result("orphan-call", "orphan"),
        "orphan-result-anchor",
    ));
    session.add_message(identified(
        Message::assistant("safe conclusion", None),
        "malformed-safe-final",
    ));
    session.add_message(identified(
        Message::assistant(
            "",
            Some(vec![history_call(
                "generated-around-call",
                "read_around",
                Some("valid-answer"),
            )]),
        ),
        "generated-around-call-message",
    ));
    session.add_message(identified(
        Message::tool_result("generated-around-call", "generated"),
        "generated-around-result-message",
    ));
    session.add_message(identified(
        Message::user("current"),
        "current-request-anchor",
    ));
    session.add_message(identified(
        Message::assistant(
            "",
            Some(vec![history_call(
                "current-around-call",
                "read_around",
                Some("valid-answer"),
            )]),
        ),
        "current-call-message-anchor",
    ));
    store.save_session(&session).await.unwrap();

    let mut other = Session::new("around-other-session", "test-model");
    other.add_message(identified(
        Message::user("other secret"),
        "cross-session-anchor",
    ));
    store.save_session(&other).await.unwrap();

    let tool = SessionInspectorTool::self_only(store.clone(), store);
    let mut expected = None;
    for anchor in [
        "missing-anchor",
        "cross-session-anchor",
        "system-anchor",
        "current-request-anchor",
        "current-call-message-anchor",
        "generated-around-call-message",
        "generated-around-result-message",
        "incomplete-call-anchor",
        "orphan-result-anchor",
    ] {
        let result = completed(
            tool.invoke(
                json!({"action":"read_around","message_id":anchor}),
                context(&session.id, "current-around-call"),
            )
            .await
            .unwrap(),
        );
        assert_eq!(result["available"], false);
        assert_eq!(result["unavailable_reason"], "message_unavailable");
        assert_eq!(result["turns"], json!([]));
        if let Some(expected) = expected.as_ref() {
            assert_eq!(&result, expected, "anchor {anchor} leaked a distinction");
        } else {
            expected = Some(result);
        }
    }

    let valid = completed(
        tool.invoke(
            json!({"action":"read_around","message_id":"valid-answer","before_turns":0,"after_turns":0}),
            context(&session.id, "current-around-call"),
        )
        .await
        .unwrap(),
    );
    assert_eq!(valid["available"], true);
    assert_eq!(
        returned_message_ids(&valid),
        vec!["valid-user", "valid-answer"]
    );
}

#[tokio::test]
async fn read_around_enforces_defaults_limits_and_contiguous_character_budgets() {
    let home = tempfile::tempdir().unwrap();
    let store = Arc::new(
        SessionStoreV2::new(home.path().to_path_buf())
            .await
            .unwrap(),
    );
    let mut session = Session::new("around-budget-session", "test-model");
    for (prefix, content) in [
        ("far-before", "a"),
        ("near-before", "b"),
        ("anchor", "c"),
        ("near-after", "d"),
        ("far-after", "e"),
    ] {
        session.add_message(identified(
            Message::user(content),
            &format!("{prefix}-user"),
        ));
        session.add_message(identified(
            Message::assistant(content, None),
            &format!("{prefix}-answer"),
        ));
    }
    session.add_message(identified(Message::user("current"), "budget-current"));
    session.add_message(identified(
        Message::assistant(
            "",
            Some(vec![history_call(
                "budget-around",
                "read_around",
                Some("anchor-answer"),
            )]),
        ),
        "budget-around-message",
    ));
    store.save_session(&session).await.unwrap();
    let tool = SessionInspectorTool::self_only(store.clone(), store);

    let defaults = completed(
        tool.invoke(
            json!({"action":"read_around","message_id":"anchor-answer"}),
            context(&session.id, "budget-around"),
        )
        .await
        .unwrap(),
    );
    assert_eq!(defaults["before_turns"], 1);
    assert_eq!(defaults["after_turns"], 1);
    assert_eq!(defaults["max_chars"], 12000);
    assert_eq!(
        returned_message_ids(&defaults),
        vec![
            "near-before-user",
            "near-before-answer",
            "anchor-user",
            "anchor-answer",
            "near-after-user",
            "near-after-answer",
        ]
    );

    let bounded = completed(
        tool.invoke(
            json!({
                "action":"read_around",
                "message_id":"anchor-answer",
                "before_turns":2,
                "after_turns":2,
                "max_chars":6,
            }),
            context(&session.id, "budget-around"),
        )
        .await
        .unwrap(),
    );
    assert_eq!(bounded["complete"], false);
    assert_eq!(bounded["truncated"], true);
    assert_eq!(bounded["truncation_reason"], "max_chars");
    assert_eq!(bounded["returned_before_turn_count"], 1);
    assert_eq!(bounded["returned_after_turn_count"], 1);
    assert_eq!(bounded["omitted_before_turn_count"], 1);
    assert_eq!(bounded["omitted_after_turn_count"], 1);
    assert_eq!(bounded["omitted_adjacent"].as_array().unwrap().len(), 2);
    assert_eq!(
        returned_message_ids(&bounded),
        vec![
            "near-before-user",
            "near-before-answer",
            "anchor-user",
            "anchor-answer",
            "near-after-user",
            "near-after-answer",
        ]
    );

    let tie_bounded = completed(
        tool.invoke(
            json!({
                "action":"read_around",
                "message_id":"anchor-answer",
                "before_turns":2,
                "after_turns":2,
                "max_chars":4,
            }),
            context(&session.id, "budget-around"),
        )
        .await
        .unwrap(),
    );
    assert_eq!(tie_bounded["returned_before_turn_count"], 1);
    assert_eq!(tie_bounded["returned_after_turn_count"], 0);
    assert_eq!(tie_bounded["omitted_before_turn_count"], 1);
    assert_eq!(tie_bounded["omitted_after_turn_count"], 2);
    assert_eq!(tie_bounded["omitted_adjacent"][0]["side"], "after");
    assert_eq!(
        tie_bounded["omitted_adjacent"][0]["nearest_omitted_distance"],
        1
    );
    assert_eq!(tie_bounded["omitted_adjacent"][1]["side"], "before");
    assert_eq!(
        returned_message_ids(&tie_bounded),
        vec![
            "near-before-user",
            "near-before-answer",
            "anchor-user",
            "anchor-answer",
        ]
    );

    let anchor_only = completed(
        tool.invoke(
            json!({"action":"read_around","message_id":"anchor-answer","before_turns":0,"after_turns":0}),
            context(&session.id, "budget-around"),
        )
        .await
        .unwrap(),
    );
    assert_eq!(anchor_only["complete"], true);
    assert_eq!(
        returned_message_ids(&anchor_only),
        vec!["anchor-user", "anchor-answer"]
    );

    let oversized = completed(
        tool.invoke(
            json!({"action":"read_around","message_id":"anchor-answer","max_chars":1}),
            context(&session.id, "budget-around"),
        )
        .await
        .unwrap(),
    );
    assert_eq!(oversized["available"], true);
    assert_eq!(oversized["returned_turn_count"], 0);
    assert_eq!(oversized["turns"], json!([]));
    assert_eq!(oversized["truncation_reason"], "anchor_exceeds_max_chars");
    assert_eq!(oversized["oversized_anchor"]["required_chars"], 2);

    for invalid in [
        json!({"action":"read_around","message_id":"anchor-answer","before_turns":6}),
        json!({"action":"read_around","message_id":"anchor-answer","after_turns":6}),
        json!({"action":"read_around","message_id":"anchor-answer","max_chars":0}),
        json!({"action":"read_around","message_id":"anchor-answer","max_chars":20001}),
        json!({"action":"read_around","message_id":""}),
    ] {
        assert!(matches!(
            tool.invoke(invalid, context(&session.id, "budget-around"))
                .await,
            Err(ToolError::InvalidArguments(_))
        ));
    }
}

#[tokio::test]
async fn read_around_never_splits_anchor_or_adjacent_turns_at_the_message_cap() {
    let home = tempfile::tempdir().unwrap();
    let store = Arc::new(
        SessionStoreV2::new(home.path().to_path_buf())
            .await
            .unwrap(),
    );

    let mut oversized = Session::new("around-oversized-message-session", "test-model");
    oversized.add_message(identified(
        Message::user("oversized"),
        "oversized-anchor-user",
    ));
    for index in 0..100 {
        oversized.add_message(identified(
            Message::assistant("", None),
            &format!("oversized-anchor-message-{index}"),
        ));
    }
    oversized.add_message(identified(Message::user("current"), "oversized-current"));
    oversized.add_message(identified(
        Message::assistant(
            "",
            Some(vec![history_call(
                "oversized-around",
                "read_around",
                Some("oversized-anchor-user"),
            )]),
        ),
        "oversized-around-message",
    ));
    store.save_session(&oversized).await.unwrap();
    let tool = SessionInspectorTool::self_only(store.clone(), store.clone());
    let anchor_oversized = completed(
        tool.invoke(
            json!({"action":"read_around","message_id":"oversized-anchor-user","max_chars":20000}),
            context(&oversized.id, "oversized-around"),
        )
        .await
        .unwrap(),
    );
    assert_eq!(anchor_oversized["returned_message_count"], 0);
    assert_eq!(anchor_oversized["turns"], json!([]));
    assert_eq!(
        anchor_oversized["truncation_reason"],
        "anchor_exceeds_max_messages"
    );
    assert_eq!(
        anchor_oversized["oversized_anchor"]["required_messages"],
        101
    );

    let mut adjacent = Session::new("around-adjacent-message-session", "test-model");
    adjacent.add_message(identified(Message::user("before"), "large-before-user"));
    for index in 0..98 {
        adjacent.add_message(identified(
            Message::assistant("", None),
            &format!("large-before-message-{index}"),
        ));
    }
    adjacent.add_message(identified(Message::user("a"), "small-anchor-user"));
    adjacent.add_message(identified(
        Message::assistant("a", None),
        "small-anchor-answer",
    ));
    adjacent.add_message(identified(Message::user("current"), "adjacent-current"));
    adjacent.add_message(identified(
        Message::assistant(
            "",
            Some(vec![history_call(
                "adjacent-around",
                "read_around",
                Some("small-anchor-answer"),
            )]),
        ),
        "adjacent-around-message",
    ));
    store.save_session(&adjacent).await.unwrap();

    let adjacent_bounded = completed(
        tool.invoke(
            json!({
                "action":"read_around",
                "message_id":"small-anchor-answer",
                "before_turns":1,
                "after_turns":0,
                "max_chars":20000,
            }),
            context(&adjacent.id, "adjacent-around"),
        )
        .await
        .unwrap(),
    );
    assert_eq!(adjacent_bounded["truncation_reason"], "max_messages");
    assert_eq!(adjacent_bounded["omitted_before_turn_count"], 1);
    assert_eq!(adjacent_bounded["returned_message_count"], 2);
    assert_eq!(
        returned_message_ids(&adjacent_bounded),
        vec!["small-anchor-user", "small-anchor-answer"]
    );
    assert_eq!(
        adjacent_bounded["omitted_adjacent"][0]["turn_required_messages"],
        99
    );
}
