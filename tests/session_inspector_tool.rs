//! Integration tests for the server-only `recall` tool (legacy alias: `session_inspector`).

use std::sync::Arc;

use bamboo_agent::agent::{Message, Session};
use bamboo_agent_core::storage::Storage;
use bamboo_agent_core::tools::{Tool, ToolExecutionContext};
use bamboo_agent_core::ConversationSummary;
use bamboo_infrastructure::SessionStoreV2;
use bamboo_agent::server::tools::SessionInspectorTool;

mod common;

fn ctx_for_session<'a>(session_id: &'a str) -> ToolExecutionContext<'a> {
    ToolExecutionContext {
        session_id: Some(session_id),
        tool_call_id: "tool_call",
        event_tx: None,
        available_tool_schemas: None,
    }
}

#[tokio::test]
async fn session_inspector_requires_session_id() {
    common::init_test_env();
    let dir = common::create_temp_dir();
    let store = Arc::new(SessionStoreV2::new(dir.path().to_path_buf()).await.unwrap());

    let tool = SessionInspectorTool::new(store.clone(), store.clone());
    let err = tool
        .execute_with_context(
            serde_json::json!({ "action": "list" }),
            ToolExecutionContext::none("tool_call"),
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("requires a session_id"));
}

#[tokio::test]
async fn session_inspector_list_and_read_messages_from_end() {
    common::init_test_env();
    let dir = common::create_temp_dir();
    let store = Arc::new(SessionStoreV2::new(dir.path().to_path_buf()).await.unwrap());

    // Caller session (just for context).
    let mut caller = Session::new("caller", "test-model");
    caller.add_message(Message::user("hi".to_string()));
    store.save_session(&caller).await.unwrap();

    // Target session with several messages.
    let mut s = Session::new("s1", "test-model");
    s.title = "Alpha Session".to_string();
    s.add_message(Message::system("system".to_string()));
    for i in 0..10 {
        s.add_message(Message::user(format!("user-{i}")));
        s.add_message(Message::assistant(format!("assistant-{i}"), None));
    }
    store.save_session(&s).await.unwrap();

    let tool = SessionInspectorTool::new(store.clone(), store.clone());

    // List by title query.
    let listed = tool
        .execute_with_context(
            serde_json::json!({ "action": "list", "query": "alpha", "limit": 10 }),
            ctx_for_session("caller"),
        )
        .await
        .unwrap();
    let listed_v: serde_json::Value = serde_json::from_str(&listed.result).unwrap();
    assert_eq!(listed_v["total"].as_u64().unwrap(), 1);
    assert_eq!(listed_v["sessions"][0]["id"].as_str().unwrap(), "s1");

    // Read last 3 non-system messages (from_end).
    let read = tool
        .execute_with_context(
            serde_json::json!({
                "action": "read_messages",
                "session_id": "s1",
                "from_end": true,
                "limit": 3,
                "include_system": false,
                "truncate_chars": 50
            }),
            ctx_for_session("caller"),
        )
        .await
        .unwrap();
    let read_v: serde_json::Value = serde_json::from_str(&read.result).unwrap();
    assert_eq!(read_v["slice_count"].as_u64().unwrap(), 3);
    // The last message should be assistant-9.
    let last = read_v["messages"].as_array().unwrap().last().unwrap();
    assert!(last["content"].as_str().unwrap().contains("assistant-9"));
}

#[tokio::test]
async fn session_inspector_search_tail_messages_finds_match() {
    common::init_test_env();
    let dir = common::create_temp_dir();
    let store = Arc::new(SessionStoreV2::new(dir.path().to_path_buf()).await.unwrap());

    let mut caller = Session::new("caller", "test-model");
    caller.add_message(Message::user("hi".to_string()));
    store.save_session(&caller).await.unwrap();

    let mut s = Session::new("s2", "test-model");
    s.title = "Beta".to_string();
    s.add_message(Message::system("system".to_string()));
    s.add_message(Message::user("something".to_string()));
    s.add_message(Message::assistant("needle-here".to_string(), None));
    store.save_session(&s).await.unwrap();

    let tool = SessionInspectorTool::new(store.clone(), store.clone());
    let out = tool
        .execute_with_context(
            serde_json::json!({
                "action": "search",
                "query": "needle",
                "mode": "tail_messages",
                "max_sessions": 10,
                "tail_messages": 10
            }),
            ctx_for_session("caller"),
        )
        .await
        .unwrap();

    let v: serde_json::Value = serde_json::from_str(&out.result).unwrap();
    assert!(
        v["matches"]
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m["type"].as_str() == Some("message_match")
                && m["session_id"].as_str() == Some("s2")),
        "expected a message_match for s2, got: {}",
        out.result
    );
}

#[tokio::test]
async fn session_inspector_read_compressed_cache_reads_sqlite_cached_rows() {
    common::init_test_env();
    let dir = common::create_temp_dir();
    let store = Arc::new(SessionStoreV2::new(dir.path().to_path_buf()).await.unwrap());

    let mut caller = Session::new("caller", "test-model");
    caller.add_message(Message::user("hi".to_string()));
    store.save_session(&caller).await.unwrap();

    let mut s = Session::new("compressed-1", "test-model");
    s.title = "Compressed Session".to_string();
    s.add_message(Message::system("system".to_string()));
    s.add_message(Message::user("old-user-context".to_string()));
    s.add_message(Message::assistant(
        "old-assistant-context".to_string(),
        None,
    ));
    s.add_message(Message::user("latest user".to_string()));
    s.add_message(Message::assistant("latest assistant".to_string(), None));
    s.conversation_summary = Some(ConversationSummary::new(
        "compressed summary snapshot",
        2,
        20,
    ));
    s.messages[1].compressed = true;
    s.messages[2].compressed = true;
    store.save_session(&s).await.unwrap();

    let tool = SessionInspectorTool::new(store.clone(), store.clone());
    let out = tool
        .execute_with_context(
            serde_json::json!({
                "action": "read_compressed_cache",
                "session_id": "compressed-1",
                "limit": 10
            }),
            ctx_for_session("caller"),
        )
        .await
        .unwrap();

    let v: serde_json::Value = serde_json::from_str(&out.result).unwrap();
    assert_eq!(v["source"].as_str(), Some("sqlite_fts"));
    assert_eq!(v["total_compressed_messages"].as_u64(), Some(2));
    assert_eq!(v["slice_count"].as_u64(), Some(2));
    assert_eq!(v["summary"].as_str(), Some("compressed summary snapshot"));
    assert!(v["messages"]
        .as_array()
        .unwrap()
        .iter()
        .any(|m| m["content"].as_str() == Some("old-user-context")));
}
