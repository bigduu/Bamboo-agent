use tokio::sync::mpsc;

use super::maybe_handle_user_question_tool;
use crate::agent::core::tools::{FunctionCall, ToolCall, ToolResult};
use crate::agent::core::{AgentEvent, Role, Session};
use crate::agent::loop_module::config::AgentLoopConfig;

#[tokio::test]
async fn maybe_handle_user_question_tool_sets_pending_question_and_emits_events() {
    let tool_call = ToolCall {
        id: "ask-1".to_string(),
        tool_type: "function".to_string(),
        function: FunctionCall {
            name: "ask_user".to_string(),
            arguments: "{}".to_string(),
        },
    };
    let result = ToolResult {
        success: true,
        result: serde_json::json!({
            "question": "Continue?",
            "options": ["Yes", "No"],
            "allow_custom": false
        })
        .to_string(),
        display_preference: Some("ask_user".to_string()),
    };

    let (tx, mut rx) = mpsc::channel(8);
    let mut session = Session::new("session-1", "model");

    let handled = maybe_handle_user_question_tool(
        &tool_call,
        &result,
        &mut session,
        &tx,
        None,
        "session-1",
        "round-1",
        &AgentLoopConfig::default(),
    )
    .await;

    assert!(handled);
    assert_eq!(session.messages.len(), 1);
    assert!(matches!(session.messages[0].role, Role::Tool));
    let saved_payload: serde_json::Value =
        serde_json::from_str(&session.messages[0].content).expect("saved tool result payload");
    assert_eq!(saved_payload["question"], "Continue?");
    assert_eq!(saved_payload["allow_custom"], false);

    let pending = session
        .pending_question
        .as_ref()
        .expect("pending question should be set");
    assert_eq!(pending.tool_call_id, "ask-1");
    assert_eq!(pending.question, "Continue?");
    assert_eq!(pending.options, vec!["Yes".to_string(), "No".to_string()]);
    assert!(!pending.allow_custom);

    let first_event = rx.recv().await.expect("first event");
    match first_event {
        AgentEvent::ToolComplete {
            tool_call_id,
            result: event_result,
        } => {
            assert_eq!(tool_call_id, "ask-1");
            assert!(event_result.success);
        }
        other => panic!("unexpected first event: {other:?}"),
    }

    let second_event = rx.recv().await.expect("second event");
    match second_event {
        AgentEvent::NeedClarification { question, options } => {
            assert_eq!(question, "Continue?");
            assert_eq!(options, Some(vec!["Yes".to_string(), "No".to_string()]));
        }
        other => panic!("unexpected second event: {other:?}"),
    }
}

#[tokio::test]
async fn maybe_handle_user_question_tool_ignores_unrelated_tool_calls() {
    let tool_call = ToolCall {
        id: "read-1".to_string(),
        tool_type: "function".to_string(),
        function: FunctionCall {
            name: "Read".to_string(),
            arguments: "{}".to_string(),
        },
    };
    let result = ToolResult {
        success: true,
        result: "{}".to_string(),
        display_preference: None,
    };

    let (tx, mut rx) = mpsc::channel(4);
    let mut session = Session::new("session-1", "model");

    let handled = maybe_handle_user_question_tool(
        &tool_call,
        &result,
        &mut session,
        &tx,
        None,
        "session-1",
        "round-1",
        &AgentLoopConfig::default(),
    )
    .await;

    assert!(!handled);
    assert!(session.pending_question.is_none());
    assert!(session.messages.is_empty());
    assert!(rx.try_recv().is_err());
}
