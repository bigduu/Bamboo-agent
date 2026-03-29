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
            name: "conclusion_with_options".to_string(),
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
        display_preference: Some("conclusion_with_options".to_string()),
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
async fn maybe_handle_user_question_tool_handles_request_permissions() {
    let tool_call = ToolCall {
        id: "perm-1".to_string(),
        tool_type: "function".to_string(),
        function: FunctionCall {
            name: "request_permissions".to_string(),
            arguments: "{}".to_string(),
        },
    };
    let result = ToolResult {
        success: true,
        result: serde_json::json!({
            "status": "awaiting_permission_approval",
            "question": "**Permission Request**\n\nNeed write access\n\n**Requested permissions:**\n- write_file `/tmp/deploy`\n",
            "reason": "Need write access",
            "permissions": [{"type": "write_file", "resource": "/tmp/deploy", "risk_level": "Medium Risk"}],
            "options": ["Approve", "Deny"],
            "allow_custom": false
        })
        .to_string(),
        display_preference: Some("request_permissions".to_string()),
    };

    let (tx, mut rx) = mpsc::channel(8);
    let mut session = Session::new("session-perm", "model");

    let handled = maybe_handle_user_question_tool(
        &tool_call,
        &result,
        &mut session,
        &tx,
        None,
        "session-perm",
        "round-1",
        &AgentLoopConfig::default(),
    )
    .await;

    assert!(
        handled,
        "request_permissions should be handled as a pause-tool"
    );
    assert_eq!(session.messages.len(), 1);
    assert!(matches!(session.messages[0].role, Role::Tool));

    let pending = session
        .pending_question
        .as_ref()
        .expect("pending question should be set for request_permissions");
    assert_eq!(pending.tool_call_id, "perm-1");
    assert!(pending.question.contains("Permission Request"));
    assert_eq!(
        pending.options,
        vec!["Approve".to_string(), "Deny".to_string()]
    );
    assert!(!pending.allow_custom);

    let first_event = rx.recv().await.expect("first event");
    assert!(matches!(first_event, AgentEvent::ToolComplete { .. }));

    let second_event = rx.recv().await.expect("second event");
    match second_event {
        AgentEvent::NeedClarification { question, options } => {
            assert!(question.contains("Permission Request"));
            assert_eq!(
                options,
                Some(vec!["Approve".to_string(), "Deny".to_string()])
            );
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
