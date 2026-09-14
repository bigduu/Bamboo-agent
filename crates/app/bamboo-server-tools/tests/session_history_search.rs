use std::sync::Arc;

use bamboo_agent_core::{
    CompressionEvent, CompressionTriggerType, ConversationSummary, FunctionCall, Message, Session,
    Storage, Tool, ToolCall, ToolCtx, ToolError, ToolOutcome,
};
use bamboo_server_tools::SessionInspectorTool;
use bamboo_storage::SessionStoreV2;
use serde_json::{json, Value};

fn context(session_id: &str, tool_call_id: &str) -> ToolCtx {
    let mut context = ToolCtx::none(tool_call_id);
    context.session_id = Some(Arc::from(session_id));
    context
}

fn history_call(id: &str, query: &str) -> ToolCall {
    ToolCall {
        id: id.to_string(),
        tool_type: "function".to_string(),
        function: FunctionCall {
            name: "session_history".to_string(),
            arguments: json!({
                "action": "search_current",
                "query": query,
                "limit": 20,
            })
            .to_string(),
        },
    }
}

fn completed(outcome: ToolOutcome) -> Value {
    let ToolOutcome::Completed(result) = outcome else {
        panic!("expected completed tool outcome")
    };
    assert!(result.success, "{}", result.result);
    serde_json::from_str(&result.result).expect("tool result JSON")
}

#[tokio::test]
async fn search_current_is_self_scoped_reads_compressed_history_and_never_mutates_session() {
    let home = tempfile::tempdir().unwrap();
    let store = Arc::new(
        SessionStoreV2::new(home.path().to_path_buf())
            .await
            .unwrap(),
    );
    let query = "SELF-HISTORY-SENTINEL";
    let mut current = Session::new("current-session", "test-model");
    current.conversation_summary = Some(ConversationSummary::new("durable summary", 1, 4));
    current.compression_events.push(CompressionEvent::new(
        1,
        0,
        80.0,
        35.0,
        4,
        CompressionTriggerType::Auto,
        0.2,
        Some("summary-model".to_string()),
        10,
    ));

    let mut compressed = Message::user(format!(
        "{query}: exact fact retained only in compressed history"
    ));
    compressed.id = "authoritative-compressed-message".to_string();
    compressed.compressed = true;
    compressed.compressed_by_event_id = Some(current.compression_events[0].id.clone());
    current.add_message(compressed);

    let mut previous_call =
        Message::assistant("", Some(vec![history_call("previous-search", query)]));
    previous_call.id = "previous-search-call-message".to_string();
    current.add_message(previous_call);
    let mut previous_result = Message::tool_result(
        "previous-search",
        json!({"query": query, "matches": [{"content_preview": query}]}).to_string(),
    );
    previous_result.id = "previous-search-result-message".to_string();
    current.add_message(previous_result);
    let mut current_request = Message::user(format!("Please look up {query} in prior history"));
    current_request.id = "current-user-request-message".to_string();
    current.add_message(current_request);
    let mut current_call =
        Message::assistant("", Some(vec![history_call("current-search", query)]));
    current_call.id = "current-search-call-message".to_string();
    current.add_message(current_call);

    let mut other = Session::new("other-session", "test-model");
    let mut other_message = Message::user(format!("{query}: must never cross Session scope"));
    other_message.id = "other-session-message".to_string();
    other.add_message(other_message);

    store.save_session(&current).await.unwrap();
    store.save_session(&other).await.unwrap();
    let before = serde_json::to_value(store.load_session(&current.id).await.unwrap().unwrap())
        .expect("serialize before state");

    let tool = SessionInspectorTool::self_only(store.clone(), store.clone());
    let result = completed(
        tool.invoke(
            json!({"action": "search_current", "query": query, "limit": 20}),
            context(&current.id, "current-search"),
        )
        .await
        .unwrap(),
    );

    assert_eq!(result["session_id"], current.id);
    assert_eq!(result["searched_before_message_index"], 3);
    assert_eq!(result["match_count"], 1);
    assert_eq!(
        result["matches"][0]["id"],
        "authoritative-compressed-message"
    );
    assert_eq!(result["matches"][0]["compressed"], true);
    assert!(result["matches"][0]["content_preview"]
        .as_str()
        .unwrap()
        .contains(query));
    assert!(!result.to_string().contains("other-session-message"));
    assert!(!result
        .to_string()
        .contains("previous-search-result-message"));
    assert!(!result.to_string().contains("current-search-call-message"));
    assert!(!result.to_string().contains("current-user-request-message"));

    let after = serde_json::to_value(store.load_session(&current.id).await.unwrap().unwrap())
        .expect("serialize after state");
    assert_eq!(
        after, before,
        "history search must not mutate compressed flags, events, summary, accounting, or context state"
    );
}

#[tokio::test]
async fn self_only_schema_and_invoke_fail_closed_while_full_surface_keeps_root_actions() {
    let home = tempfile::tempdir().unwrap();
    let store = Arc::new(
        SessionStoreV2::new(home.path().to_path_buf())
            .await
            .unwrap(),
    );
    let session = Session::new("schema-session", "test-model");
    store.save_session(&session).await.unwrap();

    let self_only = SessionInspectorTool::self_only(store.clone(), store.clone());
    let self_schema = self_only.parameters_schema();
    assert_eq!(
        self_schema["properties"]["action"]["enum"],
        json!(["search_current"])
    );
    assert!(self_schema["properties"].get("session_id").is_none());
    assert_eq!(self_schema["additionalProperties"], false);
    assert_eq!(self_schema["properties"]["query"]["minLength"], 1);
    assert_eq!(self_schema["properties"]["query"]["maxLength"], 512);

    let list_error = self_only
        .invoke(json!({"action": "list"}), context(&session.id, "list-call"))
        .await
        .expect_err("self-only caller cannot craft a privileged action");
    assert!(
        matches!(list_error, ToolError::InvalidArguments(message) if message.contains("only permits"))
    );
    for forbidden in [
        json!({"action": "search_current", "query": "fact", "session_id": "other"}),
        json!({"action": "search_current", "query": "fact", "compressed": false}),
    ] {
        let error = self_only
            .invoke(forbidden, context(&session.id, "forbidden-call"))
            .await
            .expect_err("scope and compression selectors are host-owned");
        assert!(
            matches!(error, ToolError::InvalidArguments(message) if message.contains("runtime-derived"))
        );
    }

    let full = SessionInspectorTool::new(store.clone(), store);
    let actions = full.parameters_schema()["properties"]["action"]["enum"]
        .as_array()
        .unwrap()
        .clone();
    assert!(actions.contains(&json!("search_current")));
    assert!(actions.contains(&json!("list")));
    assert!(actions.contains(&json!("read_messages")));
    assert!(actions.contains(&json!("export_context")));
}

#[tokio::test]
async fn search_current_validates_queries_and_bounds_the_result_page() {
    let home = tempfile::tempdir().unwrap();
    let store = Arc::new(
        SessionStoreV2::new(home.path().to_path_buf())
            .await
            .unwrap(),
    );
    let mut session = Session::new("bounded-search-session", "test-model");
    for index in 0..55 {
        session.add_message(Message::user(format!(
            "BOUNDED-SEARCH-SENTINEL historical item {index}"
        )));
    }
    store.save_session(&session).await.unwrap();
    let tool = SessionInspectorTool::self_only(store.clone(), store);

    let bounded = completed(
        tool.invoke(
            json!({
                "action": "search_current",
                "query": "BOUNDED-SEARCH-SENTINEL",
                "limit": 500,
            }),
            context(&session.id, "bounded-call"),
        )
        .await
        .unwrap(),
    );
    assert_eq!(bounded["limit"], 50);
    assert_eq!(bounded["match_count"], 50);
    assert_eq!(bounded["matches"].as_array().unwrap().len(), 50);

    for invalid_query in [String::new(), "x".repeat(513)] {
        let error = tool
            .invoke(
                json!({"action": "search_current", "query": invalid_query}),
                context(&session.id, "invalid-query-call"),
            )
            .await
            .expect_err("invalid query must fail closed");
        assert!(matches!(error, ToolError::InvalidArguments(_)));
    }

    for (index, query) in ["---", "\"", "*", "NEAR(foo)", "foo\"bar", "a OR b", ":)"]
        .into_iter()
        .enumerate()
    {
        let punctuation = completed(
            tool.invoke(
                json!({"action": "search_current", "query": query}),
                context(&session.id, &format!("punctuation-call-{index}")),
            )
            .await
            .unwrap_or_else(|error| {
                panic!("query must be data, not FTS syntax: {query:?}: {error}")
            }),
        );
        assert_eq!(punctuation["match_count"], 0, "query={query:?}");
    }
}

#[tokio::test]
async fn search_current_rejects_stale_derived_index_content() {
    let home = tempfile::tempdir().unwrap();
    let store = Arc::new(
        SessionStoreV2::new(home.path().to_path_buf())
            .await
            .unwrap(),
    );
    let mut authoritative = Session::new("stale-index-session", "test-model");
    let mut prior = Message::assistant("authoritative content after an edit", None);
    prior.id = "edited-message".to_string();
    authoritative.add_message(prior);
    authoritative.add_message(Message::user("search my earlier message"));
    store.save_session(&authoritative).await.unwrap();
    store.flush_search_index().await;

    // Simulate a derived-index write that lags behind the durable Session: the
    // same message ID still contains text which no longer exists in JSON.
    let mut stale_projection = authoritative.clone();
    stale_projection.messages[0].content = "STALE-DERIVED-INDEX-SENTINEL".to_string();
    store
        .search_index()
        .upsert_session(&stale_projection)
        .await
        .unwrap();

    let tool = SessionInspectorTool::self_only(store.clone(), store);
    let result = completed(
        tool.invoke(
            json!({
                "action": "search_current",
                "query": "STALE-DERIVED-INDEX-SENTINEL"
            }),
            context(&authoritative.id, "stale-index-call"),
        )
        .await
        .unwrap(),
    );

    assert_eq!(result["match_count"], 0);
    assert!(result["matches"].as_array().unwrap().is_empty());
}
