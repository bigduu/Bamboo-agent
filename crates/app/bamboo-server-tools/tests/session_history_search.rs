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
    assert_eq!(result["searched_before_message_index"], 4);
    assert_eq!(result["match_count"], 1);
    assert_eq!(
        result["matches"][0]["id"],
        "authoritative-compressed-message"
    );
    assert_eq!(result["matches"][0]["compressed"], true);
    assert_eq!(result["index_fresh"], true);
    assert_eq!(result["fallback_scanned_messages"], 0);
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
async fn search_current_uses_fresh_cjk_index_for_positive_and_negative_pages() {
    let home = tempfile::tempdir().unwrap();
    let store = Arc::new(
        SessionStoreV2::new(home.path().to_path_buf())
            .await
            .unwrap(),
    );
    let mut session = Session::new("fresh-cjk-session", "test-model");
    let mut valid = Message::assistant("我们要压缩上下文，再用 memory 继续任务", None);
    valid.id = "fresh-cjk-valid".to_string();
    valid.compressed = true;
    session.add_message(valid);
    let mut false_candidate = Message::assistant("压缩。缩上。上下并不是连续短语", None);
    false_candidate.id = "fresh-cjk-false-candidate".to_string();
    session.add_message(false_candidate);
    session.add_message(Message::user("请搜索之前的上下文设计"));
    session.add_message(Message::assistant(
        "",
        Some(vec![history_call("fresh-cjk-call", "压缩上下")]),
    ));
    store.save_session(&session).await.unwrap();

    let tool = SessionInspectorTool::self_only(store.clone(), store);
    let positive = completed(
        tool.invoke(
            json!({"action": "search_current", "query": "压缩上下", "limit": 1}),
            context(&session.id, "fresh-cjk-call"),
        )
        .await
        .unwrap(),
    );
    assert_eq!(positive["search_backend"], "sqlite_fts_cjk_bigram");
    assert_eq!(positive["index_fresh"], true);
    assert_eq!(positive["index_results_complete"], true);
    assert_eq!(positive["fallback_scanned_messages"], 0);
    assert_eq!(positive["match_count"], 1);
    assert_eq!(positive["matches"][0]["id"], "fresh-cjk-valid");
    assert_eq!(positive["matches"][0]["match_source"], "fts_cjk_bigram");

    let negative = completed(
        tool.invoke(
            json!({"action": "search_current", "query": "完全不存在", "limit": 20}),
            context(&session.id, "fresh-cjk-call"),
        )
        .await
        .unwrap(),
    );
    assert_eq!(negative["search_backend"], "sqlite_fts_cjk_bigram");
    assert_eq!(negative["index_fresh"], true);
    assert_eq!(negative["match_count"], 0);
    assert_eq!(negative["fallback_scanned_messages"], 0);

    let single = completed(
        tool.invoke(
            json!({"action": "search_current", "query": "压", "limit": 20}),
            context(&session.id, "fresh-cjk-call"),
        )
        .await
        .unwrap(),
    );
    assert_eq!(single["search_backend"], "sqlite_literal");
    assert_eq!(single["index_fresh"], true);
    assert_eq!(single["fallback_scanned_messages"], 0);
    assert!(single["matches"]
        .as_array()
        .unwrap()
        .iter()
        .all(|hit| hit["match_source"] == "literal"));
}

#[tokio::test]
async fn search_current_recovers_compressed_messages_from_the_current_turn() {
    let home = tempfile::tempdir().unwrap();
    let store = Arc::new(
        SessionStoreV2::new(home.path().to_path_buf())
            .await
            .unwrap(),
    );
    let query = "MID-TURN-COMPRESSED-SENTINEL";
    let mut session = Session::new("mid-turn-compression-session", "test-model");

    let mut current_request = Message::user(format!("Please recover {query} from this turn"));
    current_request.id = "mid-turn-current-request".to_string();
    session.add_message(current_request);

    let mut compressed_assistant = Message::assistant(
        format!("{query}: exact detail archived by host compression"),
        Some(vec![ToolCall {
            id: "mid-turn-other-tool".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "read_file".to_string(),
                arguments: json!({"path": "/tmp/example"}).to_string(),
            },
        }]),
    );
    compressed_assistant.id = "mid-turn-compressed-assistant".to_string();
    compressed_assistant.compressed = true;
    compressed_assistant.compressed_by_event_id = Some("mid-turn-compression-event".to_string());
    session.add_message(compressed_assistant);

    let mut compressed_tool_result = Message::tool_result(
        "mid-turn-other-tool",
        format!("{query}: exact tool detail archived by host compression"),
    );
    compressed_tool_result.id = "mid-turn-compressed-tool-result".to_string();
    compressed_tool_result.compressed = true;
    compressed_tool_result.compressed_by_event_id = Some("mid-turn-compression-event".to_string());
    session.add_message(compressed_tool_result);

    let mut current_call =
        Message::assistant("", Some(vec![history_call("mid-turn-search", query)]));
    current_call.id = "mid-turn-current-search-call".to_string();
    session.add_message(current_call);
    store.save_session(&session).await.unwrap();
    let before = serde_json::to_value(store.load_session(&session.id).await.unwrap().unwrap())
        .expect("serialize before state");

    let tool = SessionInspectorTool::self_only(store.clone(), store.clone());
    let result = completed(
        tool.invoke(
            json!({"action": "search_current", "query": query}),
            context(&session.id, "mid-turn-search"),
        )
        .await
        .unwrap(),
    );

    assert_eq!(result["searched_before_message_index"], 3);
    assert_eq!(result["match_count"], 2);
    let matched_ids = result["matches"]
        .as_array()
        .unwrap()
        .iter()
        .map(|message| message["id"].as_str().unwrap())
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(
        matched_ids,
        std::collections::HashSet::from([
            "mid-turn-compressed-assistant",
            "mid-turn-compressed-tool-result",
        ])
    );
    assert!(result["matches"]
        .as_array()
        .unwrap()
        .iter()
        .all(|message| message["compressed"] == true));
    assert!(!result.to_string().contains("mid-turn-current-request"));
    assert!(!result.to_string().contains("mid-turn-current-search-call"));

    let after = serde_json::to_value(store.load_session(&session.id).await.unwrap().unwrap())
        .expect("serialize after state");
    assert_eq!(after, before, "mid-turn history search must be read-only");
}

#[tokio::test]
async fn search_current_without_persisted_call_searches_latest_completed_turn() {
    let home = tempfile::tempdir().unwrap();
    let store = Arc::new(
        SessionStoreV2::new(home.path().to_path_buf())
            .await
            .unwrap(),
    );
    let query = "LATEST-STANDALONE-SEARCH-SENTINEL";
    let mut session = Session::new("standalone-search-session", "test-model");
    let mut latest_user = Message::user(format!("{query}: latest completed user message"));
    latest_user.id = "latest-completed-user-message".to_string();
    session.add_message(latest_user);
    let mut latest_assistant = Message::assistant(
        format!("{query}: latest completed assistant response"),
        None,
    );
    latest_assistant.id = "latest-completed-assistant-message".to_string();
    session.add_message(latest_assistant);
    store.save_session(&session).await.unwrap();

    let tool = SessionInspectorTool::self_only(store.clone(), store);
    let result = completed(
        tool.invoke(
            json!({"action": "search_current", "query": query}),
            context(&session.id, "standalone-call-not-in-transcript"),
        )
        .await
        .unwrap(),
    );

    assert_eq!(result["searched_before_message_index"], 2);
    assert_eq!(result["match_count"], 2);
    let matched_ids = result["matches"]
        .as_array()
        .unwrap()
        .iter()
        .map(|message| message["id"].as_str().unwrap())
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(
        matched_ids,
        std::collections::HashSet::from([
            "latest-completed-user-message",
            "latest-completed-assistant-message",
        ])
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
    session.add_message(Message::assistant(
        format!(
            "{} release-checklist contains the historical deployment steps",
            "old analysis ".repeat(100)
        ),
        None,
    ));
    session.add_message(Message::user("Search my prior history now"));
    store.save_session(&session).await.unwrap();
    let tool = SessionInspectorTool::self_only(store.clone(), store.clone());

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

    let lexical = completed(
        tool.invoke(
            json!({"action": "search_current", "query": "release checklist"}),
            context(&session.id, "lexical-preview-call"),
        )
        .await
        .unwrap(),
    );
    assert_eq!(lexical["match_count"], 1);
    assert!(lexical["matches"][0]["content_preview"]
        .as_str()
        .unwrap()
        .contains("release-checklist"));

    store
        .search_index()
        .delete_session(&session.id)
        .await
        .unwrap();
    let lexical_fallback = completed(
        tool.invoke(
            json!({"action": "search_current", "query": "release checklist"}),
            context(&session.id, "lexical-fallback-call"),
        )
        .await
        .unwrap(),
    );
    assert_eq!(lexical_fallback["search_backend"], "session_json_fallback");
    assert_eq!(lexical_fallback["match_count"], 1);
    assert!(lexical_fallback["matches"][0]["content_preview"]
        .as_str()
        .unwrap()
        .contains("release-checklist"));

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
    assert_eq!(result["index_fresh"], false);
    assert!(result["fallback_scanned_messages"].as_u64().unwrap() > 0);
}

#[tokio::test]
async fn search_current_falls_back_when_the_derived_index_query_fails() {
    let home = tempfile::tempdir().unwrap();
    let store = Arc::new(
        SessionStoreV2::new(home.path().to_path_buf())
            .await
            .unwrap(),
    );
    let query = "INDEX-ERROR-FALLBACK-SENTINEL";
    let mut session = Session::new("index-error-session", "test-model");
    let mut prior = Message::assistant(format!("{query}: canonical content"), None);
    prior.id = "index-error-match".to_string();
    session.add_message(prior);
    session.add_message(Message::user("search earlier history"));
    store.save_session(&session).await.unwrap();
    store.flush_search_index().await;
    std::fs::remove_file(store.search_index().db_path()).unwrap();

    let tool = SessionInspectorTool::self_only(store.clone(), store);
    let result = completed(
        tool.invoke(
            json!({"action": "search_current", "query": query}),
            context(&session.id, "index-error-call"),
        )
        .await
        .unwrap(),
    );
    assert_eq!(result["search_backend"], "session_json_fallback");
    assert_eq!(result["index_fresh"], false);
    assert_eq!(result["match_count"], 1);
    assert_eq!(result["matches"][0]["id"], "index-error-match");
    assert!(result["fallback_scanned_messages"].as_u64().unwrap() > 0);
}
