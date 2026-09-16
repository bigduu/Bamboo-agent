use std::collections::HashSet;
use std::sync::Arc;

use bamboo_agent_core::{
    FunctionCall, ImageUrlRef, Message, MessagePart, Session, Storage, Tool, ToolCall, ToolCtx,
    ToolError, ToolOutcome,
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

fn history_call(id: &str, name: &str, action: &str) -> ToolCall {
    call(id, name, json!({"action": action}))
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
        .unwrap()
        .iter()
        .flat_map(|turn| turn["messages"].as_array().unwrap())
        .map(|message| message["id"].as_str().unwrap().to_string())
        .collect()
}

#[tokio::test]
async fn read_current_pages_exact_atomic_turns_with_a_cursor_stable_across_retrieval_calls() {
    let home = tempfile::tempdir().unwrap();
    let store = Arc::new(
        SessionStoreV2::new(home.path().to_path_buf())
            .await
            .unwrap(),
    );
    let mut session = Session::new("read-current-session", "test-model");
    session.add_message(identified(Message::system("host context"), "system"));
    session.add_message(archived(identified(
        Message::user("first archived request"),
        "turn-one-user",
    )));
    session.add_message(archived(identified(
        Message::assistant(
            "running exact command",
            Some(vec![
                call(
                    "ordinary-call",
                    "Bash",
                    json!({"command":"printf exact-argument"}),
                ),
                call(
                    "second-call",
                    "Read",
                    json!({"path":"/tmp/exact-path","offset":7}),
                ),
            ]),
        ),
        "turn-one-call",
    )));
    session.add_message(archived(identified(
        Message::tool_result("ordinary-call", "exact archived tool result"),
        "turn-one-result",
    )));
    session.add_message(archived(identified(
        Message::tool_result("second-call", "second exact archived result"),
        "turn-one-second-result",
    )));
    session.add_message(archived(identified(
        Message::assistant("first archived decision", None),
        "turn-one-final",
    )));
    session.add_message(identified(
        Message::user_with_parts(
            "second active request",
            vec![MessagePart::ImageUrl {
                image_url: ImageUrlRef {
                    url: "data:image/png;base64,PRIVATE-IMAGE-BYTES".to_string(),
                    detail: Some("high".to_string()),
                },
            }],
        ),
        "turn-two-user",
    ));
    session.add_message(identified(
        Message::assistant_with_reasoning(
            "second active decision",
            None,
            Some("PRIVATE-PROVIDER-REASONING".to_string()),
        )
        .with_reasoning_signature(Some("PRIVATE-PROVIDER-SIGNATURE".to_string())),
        "turn-two-final",
    ));
    session.add_message(identified(
        Message::user("current request must not be replayed"),
        "current-request",
    ));
    session.add_message(identified(
        Message::assistant(
            "",
            Some(vec![call(
                "prior-search",
                "session_history",
                json!({"action":"search_current","query":"exact"}),
            )]),
        ),
        "generated-search-call",
    ));
    session.add_message(identified(
        Message::tool_result("prior-search", "GENERATED-SEARCH-RESULT"),
        "generated-search-result",
    ));
    session.add_message(identified(
        Message::assistant(
            "",
            Some(vec![history_call(
                "prior-read",
                "session_history",
                "read_current",
            )]),
        ),
        "generated-read-call",
    ));
    session.add_message(identified(
        Message::tool_result("prior-read", "GENERATED-READ-RESULT"),
        "generated-read-result",
    ));
    session.add_message(identified(
        Message::assistant(
            "",
            Some(vec![history_call(
                "read-page-one",
                "session_history",
                "read_current",
            )]),
        ),
        "read-page-one-call",
    ));
    store.save_session(&session).await.unwrap();
    let before = serde_json::to_value(store.load_session(&session.id).await.unwrap().unwrap())
        .expect("serialize before read");

    let tool = SessionInspectorTool::self_only(store.clone(), store.clone());
    assert_eq!(tool.name(), "session_history");
    let first = completed(
        tool.invoke(
            json!({
                "action": "read_current",
                "direction": "backward",
                "limit": 1,
                "max_chars": 20000,
            }),
            context(&session.id, "read-page-one"),
        )
        .await
        .unwrap(),
    );
    assert_eq!(first["session_id"], session.id);
    assert_eq!(first["returned_turn_count"], 1);
    assert_eq!(first["complete"], false);
    assert_eq!(first["truncation_reason"], "turn_limit");
    assert_eq!(
        returned_message_ids(&first),
        vec!["turn-two-user", "turn-two-final"]
    );
    assert_eq!(first["turns"][0]["messages"][0]["has_images"], true);
    assert_eq!(first["turns"][0]["messages"][0]["image_count"], 1);
    assert!(!first.to_string().contains("PRIVATE-IMAGE-BYTES"));
    assert!(!first.to_string().contains("PRIVATE-PROVIDER-REASONING"));
    assert!(!first.to_string().contains("PRIVATE-PROVIDER-SIGNATURE"));
    assert!(!first.to_string().contains("current request must not"));
    assert!(!first.to_string().contains("GENERATED-SEARCH-RESULT"));
    assert!(!first.to_string().contains("GENERATED-READ-RESULT"));
    let cursor = first["next_cursor"].as_str().unwrap().to_string();

    let after = serde_json::to_value(store.load_session(&session.id).await.unwrap().unwrap())
        .expect("serialize after read");
    assert_eq!(
        after, before,
        "self-history reads must not mutate Session state"
    );

    session.add_message(identified(
        Message::tool_result("read-page-one", first.to_string()),
        "read-page-one-result",
    ));
    session.add_message(identified(
        Message::assistant(
            "",
            Some(vec![history_call(
                "read-page-two",
                "session_history",
                "read_current",
            )]),
        ),
        "read-page-two-call",
    ));
    store.save_session(&session).await.unwrap();

    let second = completed(
        tool.invoke(
            json!({"action":"read_current","cursor":cursor,"limit":1,"max_chars":20000}),
            context(&session.id, "read-page-two"),
        )
        .await
        .unwrap(),
    );
    assert_eq!(second["complete"], true);
    assert_eq!(second["next_cursor"], Value::Null);
    assert_eq!(second["returned_turn_count"], 1);
    assert_eq!(second["turns"][0]["contains_archived"], true);
    assert_eq!(
        returned_message_ids(&second),
        vec![
            "turn-one-user",
            "turn-one-call",
            "turn-one-result",
            "turn-one-second-result",
            "turn-one-final",
        ]
    );
    assert_eq!(
        second["turns"][0]["messages"][1]["tool_calls"][0]["function"]["arguments"],
        json!({"command":"printf exact-argument"}).to_string()
    );
    assert_eq!(
        second["turns"][0]["messages"][1]["tool_calls"][1]["function"]["arguments"],
        json!({"path":"/tmp/exact-path","offset":7}).to_string()
    );
    assert_eq!(
        second["turns"][0]["messages"][2]["content"],
        "exact archived tool result"
    );
    assert_eq!(
        second["turns"][0]["messages"][3]["content"],
        "second exact archived result"
    );
}

#[tokio::test]
async fn read_current_rejects_cross_session_stale_filter_and_malformed_cursors() {
    let home = tempfile::tempdir().unwrap();
    let store = Arc::new(
        SessionStoreV2::new(home.path().to_path_buf())
            .await
            .unwrap(),
    );
    let mut session = Session::new("cursor-session", "test-model");
    session.add_message(identified(Message::user("old one"), "old-one"));
    session.add_message(identified(
        Message::assistant("answer one", None),
        "answer-one",
    ));
    session.add_message(identified(Message::user("old two"), "old-two"));
    session.add_message(identified(
        Message::assistant("answer two", None),
        "answer-two",
    ));
    session.add_message(identified(Message::user("current"), "current-request"));
    session.add_message(identified(
        Message::assistant(
            "",
            Some(vec![history_call(
                "cursor-page-one",
                "session_history",
                "read_current",
            )]),
        ),
        "cursor-page-one-call",
    ));
    store.save_session(&session).await.unwrap();
    let tool = SessionInspectorTool::self_only(store.clone(), store.clone());
    let first = completed(
        tool.invoke(
            json!({"action":"read_current","limit":1}),
            context(&session.id, "cursor-page-one"),
        )
        .await
        .unwrap(),
    );
    let cursor = first["next_cursor"].as_str().unwrap().to_string();
    let snapshot_message_count = session.messages.len();

    let filter_mismatch = tool
        .invoke(
            json!({"action":"read_current","cursor":cursor,"archived_only":true}),
            context(&session.id, "cursor-page-one"),
        )
        .await
        .expect_err("cursor filters are bound");
    assert!(matches!(
        filter_mismatch,
        ToolError::InvalidArguments(message) if message.contains("direction or archived_only")
    ));
    let direction_mismatch = tool
        .invoke(
            json!({"action":"read_current","cursor":cursor,"direction":"forward"}),
            context(&session.id, "cursor-page-one"),
        )
        .await
        .expect_err("cursor direction is bound");
    assert!(matches!(
        direction_mismatch,
        ToolError::InvalidArguments(message) if message.contains("direction or archived_only")
    ));

    let mut other = Session::new("cursor-other-session", "test-model");
    other.add_message(Message::user("other history"));
    store.save_session(&other).await.unwrap();
    let cross_session = tool
        .invoke(
            json!({"action":"read_current","cursor":cursor}),
            context(&other.id, "standalone"),
        )
        .await
        .expect_err("cursor Session binding is enforced");
    assert!(matches!(
        cross_session,
        ToolError::InvalidArguments(message) if message.contains("another Session")
    ));

    let original_content = session.messages[0].content.clone();
    session.messages[0].content = "changed snapshot prefix".to_string();
    store.save_session(&session).await.unwrap();
    let changed_prefix = tool
        .invoke(
            json!({"action":"read_current","cursor":cursor}),
            context(&session.id, "cursor-page-one"),
        )
        .await
        .expect_err("changed snapshot prefix is stale");
    assert!(matches!(
        changed_prefix,
        ToolError::InvalidArguments(message) if message.contains("history snapshot changed")
    ));
    session.messages[0].content = original_content;

    session.add_message(identified(
        Message::tool_result("cursor-page-one", first.to_string()),
        "cursor-page-one-result",
    ));
    session.add_message(identified(Message::user("new request"), "new-request"));
    session.add_message(identified(
        Message::assistant(
            "",
            Some(vec![history_call(
                "cursor-new-request",
                "session_history",
                "read_current",
            )]),
        ),
        "cursor-new-request-call",
    ));
    store.save_session(&session).await.unwrap();
    let stale_request = tool
        .invoke(
            json!({"action":"read_current","cursor":cursor}),
            context(&session.id, "cursor-new-request"),
        )
        .await
        .expect_err("cursor cannot cross a User request");
    assert!(matches!(
        stale_request,
        ToolError::InvalidArguments(message) if message.contains("another user request")
    ));
    session.messages.truncate(snapshot_message_count);

    session.add_message(identified(
        Message::tool_result("cursor-page-one", first.to_string()),
        "cursor-page-one-result",
    ));
    session.add_message(identified(
        Message::assistant("ordinary work changed the snapshot continuation", None),
        "ordinary-between-pages",
    ));
    session.add_message(identified(
        Message::assistant(
            "",
            Some(vec![history_call(
                "cursor-page-two",
                "session_history",
                "read_current",
            )]),
        ),
        "cursor-page-two-call",
    ));
    store.save_session(&session).await.unwrap();
    let stale = tool
        .invoke(
            json!({"action":"read_current","cursor":cursor}),
            context(&session.id, "cursor-page-two"),
        )
        .await
        .expect_err("ordinary messages invalidate the snapshot continuation");
    assert!(matches!(
        stale,
        ToolError::InvalidArguments(message) if message.contains("non-history messages")
    ));

    for malformed in ["", "not-a-cursor", "bsh1.00.00"] {
        let error = tool
            .invoke(
                json!({"action":"read_current","cursor":malformed}),
                context(&session.id, "cursor-page-two"),
            )
            .await
            .expect_err("malformed cursor must fail explicitly");
        assert!(matches!(error, ToolError::InvalidArguments(_)));
    }
}

#[tokio::test]
async fn archived_filter_survives_store_reopen() {
    let home = tempfile::tempdir().unwrap();
    {
        let store = Arc::new(
            SessionStoreV2::new(home.path().to_path_buf())
                .await
                .unwrap(),
        );
        let mut session = Session::new("reopened-history-session", "test-model");
        session.add_message(archived(identified(
            Message::user("archived exact fact"),
            "archived-user",
        )));
        session.add_message(archived(identified(
            Message::assistant("archived exact answer", None),
            "archived-answer",
        )));
        session.add_message(identified(Message::user("active fact"), "active-user"));
        session.add_message(identified(
            Message::assistant("active answer", None),
            "active-answer",
        ));
        session.add_message(archived(identified(
            Message::user("second archived fact"),
            "second-archived-user",
        )));
        session.add_message(archived(identified(
            Message::assistant("second archived answer", None),
            "second-archived-answer",
        )));
        session.add_message(identified(Message::user("current"), "current-request"));
        session.add_message(identified(
            Message::assistant(
                "",
                Some(vec![history_call(
                    "reopen-read",
                    "session_history",
                    "read_current",
                )]),
            ),
            "reopen-read-call",
        ));
        store.save_session(&session).await.unwrap();
    }

    let reopened = Arc::new(
        SessionStoreV2::new(home.path().to_path_buf())
            .await
            .unwrap(),
    );
    let tool = SessionInspectorTool::self_only(reopened.clone(), reopened.clone());
    let first = completed(
        tool.invoke(
            json!({
                "action":"read_current",
                "direction":"forward",
                "archived_only":true,
                "limit":1,
                "max_chars":20000,
            }),
            context("reopened-history-session", "reopen-read"),
        )
        .await
        .unwrap(),
    );
    assert_eq!(first["complete"], false);
    assert_eq!(first["truncation_reason"], "turn_limit");
    assert_eq!(
        returned_message_ids(&first),
        vec!["archived-user", "archived-answer"]
    );
    assert!(first["turns"][0]["messages"]
        .as_array()
        .unwrap()
        .iter()
        .all(|message| message["archived"] == true));

    let cursor = first["next_cursor"].as_str().unwrap().to_string();
    let mut session = reopened
        .load_session("reopened-history-session")
        .await
        .unwrap()
        .unwrap();
    session.add_message(identified(
        Message::tool_result("reopen-read", first.to_string()),
        "reopen-read-result",
    ));
    session.add_message(identified(
        Message::assistant(
            "",
            Some(vec![history_call(
                "reopen-read-next",
                "session_history",
                "read_current",
            )]),
        ),
        "reopen-read-next-call",
    ));
    reopened.save_session(&session).await.unwrap();

    let second = completed(
        tool.invoke(
            json!({"action":"read_current","cursor":cursor,"limit":1,"max_chars":20000}),
            context("reopened-history-session", "reopen-read-next"),
        )
        .await
        .unwrap(),
    );
    assert_eq!(second["complete"], true);
    assert_eq!(second["next_cursor"], Value::Null);
    assert_eq!(
        returned_message_ids(&second),
        vec!["second-archived-user", "second-archived-answer"]
    );
}

#[tokio::test]
async fn malformed_and_orphan_tool_protocol_messages_are_never_half_replayed() {
    let home = tempfile::tempdir().unwrap();
    let store = Arc::new(
        SessionStoreV2::new(home.path().to_path_buf())
            .await
            .unwrap(),
    );
    let mut session = Session::new("malformed-protocol-session", "test-model");
    session.add_message(identified(
        Message::user("inspect protocol"),
        "protocol-user",
    ));
    session.add_message(identified(
        Message::assistant(
            "must stay hidden with its incomplete calls",
            Some(vec![
                call("partial-call", "Bash", json!({"command":"secret-partial"})),
                call("missing-call", "Read", json!({"path":"/secret/missing"})),
            ]),
        ),
        "incomplete-call-message",
    ));
    session.add_message(identified(
        Message::tool_result("partial-call", "half-chain result must stay hidden"),
        "partial-result",
    ));
    session.add_message(identified(
        Message::tool_result("orphan-call", "orphan result must stay hidden"),
        "orphan-result",
    ));
    session.add_message(identified(
        Message::assistant("safe completed conclusion", None),
        "protocol-final",
    ));
    session.add_message(identified(Message::user("current"), "current-request"));
    session.add_message(identified(
        Message::assistant(
            "",
            Some(vec![history_call(
                "protocol-read",
                "session_history",
                "read_current",
            )]),
        ),
        "protocol-read-call",
    ));
    store.save_session(&session).await.unwrap();

    let tool = SessionInspectorTool::self_only(store.clone(), store);
    let result = completed(
        tool.invoke(
            json!({"action":"read_current"}),
            context(&session.id, "protocol-read"),
        )
        .await
        .unwrap(),
    );
    assert_eq!(
        returned_message_ids(&result),
        vec!["protocol-user", "protocol-final"]
    );
    assert_eq!(result["excluded_incomplete_protocol_message_count"], 3);
    assert!(!result.to_string().contains("secret-partial"));
    assert!(!result.to_string().contains("half-chain result"));
    assert!(!result.to_string().contains("orphan result"));
}

#[tokio::test]
async fn read_current_enforces_defaults_ceilings_and_atomic_hard_budgets() {
    let home = tempfile::tempdir().unwrap();
    let store = Arc::new(
        SessionStoreV2::new(home.path().to_path_buf())
            .await
            .unwrap(),
    );
    let mut session = Session::new("budget-session", "test-model");
    for (user_id, answer_id, user, answer) in [
        ("old-user", "old-answer", "aa", "bb"),
        ("middle-user", "middle-answer", "cc", "dd"),
        ("large-user", "large-answer", "123456", "789012"),
    ] {
        session.add_message(identified(Message::user(user), user_id));
        session.add_message(identified(Message::assistant(answer, None), answer_id));
    }
    session.add_message(identified(Message::user("current"), "current-request"));
    session.add_message(identified(
        Message::assistant(
            "",
            Some(vec![history_call(
                "budget-read",
                "session_history",
                "read_current",
            )]),
        ),
        "budget-read-call",
    ));
    store.save_session(&session).await.unwrap();
    let tool = SessionInspectorTool::self_only(store.clone(), store.clone());

    let defaults = completed(
        tool.invoke(
            json!({"action":"read_current"}),
            context(&session.id, "budget-read"),
        )
        .await
        .unwrap(),
    );
    assert_eq!(defaults["limit"], 10);
    assert_eq!(defaults["max_chars"], 12000);
    assert_eq!(defaults["max_messages"], 100);

    for invalid in [
        json!({"action":"read_current","limit":0}),
        json!({"action":"read_current","limit":21}),
        json!({"action":"read_current","max_chars":0}),
        json!({"action":"read_current","max_chars":20001}),
    ] {
        assert!(matches!(
            tool.invoke(invalid, context(&session.id, "budget-read")).await,
            Err(ToolError::InvalidArguments(message)) if message.contains("must be between")
        ));
    }

    let oversized = completed(
        tool.invoke(
            json!({"action":"read_current","max_chars":5}),
            context(&session.id, "budget-read"),
        )
        .await
        .unwrap(),
    );
    assert_eq!(oversized["returned_turn_count"], 0);
    assert_eq!(oversized["skipped_turn_count"], 1);
    assert_eq!(oversized["truncation_reason"], "turn_exceeds_max_chars");
    assert_eq!(oversized["oversized_turn"]["required_chars"], 12);
    let cursor = oversized["next_cursor"].as_str().unwrap().to_string();

    let bounded = completed(
        tool.invoke(
            json!({"action":"read_current","cursor":cursor,"max_chars":5}),
            context(&session.id, "budget-read"),
        )
        .await
        .unwrap(),
    );
    assert_eq!(
        returned_message_ids(&bounded),
        vec!["middle-user", "middle-answer"]
    );
    assert_eq!(bounded["truncation_reason"], "max_chars");
    let cursor = bounded["next_cursor"].as_str().unwrap().to_string();
    let final_page = completed(
        tool.invoke(
            json!({"action":"read_current","cursor":cursor,"max_chars":5}),
            context(&session.id, "budget-read"),
        )
        .await
        .unwrap(),
    );
    assert_eq!(
        returned_message_ids(&final_page),
        vec!["old-user", "old-answer"]
    );
    assert_eq!(final_page["complete"], true);

    let mut many = Session::new("message-budget-session", "test-model");
    many.add_message(identified(Message::user("old"), "old-small-user"));
    many.add_message(identified(
        Message::assistant("ok", None),
        "old-small-answer",
    ));
    many.add_message(identified(Message::user("x"), "large-message-user"));
    for index in 0..100 {
        many.add_message(identified(
            Message::assistant("x", None),
            &format!("large-message-{index}"),
        ));
    }
    many.add_message(identified(Message::user("current"), "many-current-request"));
    many.add_message(identified(
        Message::assistant(
            "",
            Some(vec![history_call(
                "many-read",
                "session_history",
                "read_current",
            )]),
        ),
        "many-read-call",
    ));
    store.save_session(&many).await.unwrap();
    let message_oversized = completed(
        tool.invoke(
            json!({"action":"read_current","max_chars":20000}),
            context(&many.id, "many-read"),
        )
        .await
        .unwrap(),
    );
    assert_eq!(message_oversized["returned_turn_count"], 0);
    assert_eq!(
        message_oversized["truncation_reason"],
        "turn_exceeds_max_messages"
    );
    assert_eq!(
        message_oversized["oversized_turn"]["required_messages"],
        101
    );
    assert!(message_oversized["next_cursor"].is_string());
}

#[tokio::test]
async fn self_history_schema_rejects_authority_overrides() {
    let home = tempfile::tempdir().unwrap();
    let store = Arc::new(
        SessionStoreV2::new(home.path().to_path_buf())
            .await
            .unwrap(),
    );
    let session = Session::new("strict-read-schema", "test-model");
    store.save_session(&session).await.unwrap();

    let self_tool = SessionInspectorTool::self_only(store.clone(), store.clone());
    let root_tool = SessionInspectorTool::new(store.clone(), store);
    let actions = self_tool.parameters_schema()["properties"]["action"]["enum"]
        .as_array()
        .unwrap()
        .iter()
        .cloned()
        .collect::<HashSet<_>>();
    assert_eq!(
        actions,
        HashSet::from([json!("search_current"), json!("read_current")])
    );
    assert!(self_tool.parameters_schema()["properties"]
        .get("session_id")
        .is_none());

    for tool in [&self_tool, &root_tool] {
        for forbidden in [
            json!({"action":"read_current","session_id":"other"}),
            json!({"action":"read_current","boundary":999}),
            json!({"action":"read_current","compressed":false}),
        ] {
            let error = tool
                .invoke(forbidden, context(&session.id, "strict-call"))
                .await
                .expect_err("authority-bearing overrides are rejected");
            assert!(matches!(
                error,
                ToolError::InvalidArguments(message) if message.contains("runtime-derived")
            ));
        }
    }
}
