use tokio::sync::mpsc;

use super::maybe_handle_user_question_tool;
use crate::runtime::config::AgentLoopConfig;
use bamboo_agent_core::tools::{FunctionCall, ToolCall, ToolResult};
use bamboo_agent_core::{AgentEvent, Role, Session};
use bamboo_domain::session::runtime_state::{AgentRuntimeState, PlanModeState, PlanModeStatus};
use bamboo_memory::plan_store::PlanStore;
use chrono::Utc;

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
        images: Vec::new(),
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
    assert_eq!(pending.tool_name, "conclusion_with_options");
    assert_eq!(pending.question, "Continue?");
    assert_eq!(pending.options, vec!["Yes".to_string(), "No".to_string()]);
    assert!(!pending.allow_custom);
    assert_eq!(
        pending.source,
        bamboo_agent_core::PendingQuestionSource::PauseTool
    );

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
        AgentEvent::NeedClarification {
            question,
            options,
            tool_call_id,
            tool_name,
            allow_custom,
        } => {
            assert_eq!(question, "Continue?");
            assert_eq!(options, Some(vec!["Yes".to_string(), "No".to_string()]));
            assert_eq!(tool_call_id, Some("ask-1".to_string()));
            assert_eq!(tool_name, Some("conclusion_with_options".to_string()));
            assert!(!allow_custom);
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
        images: Vec::new(),
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
    assert_eq!(pending.tool_name, "request_permissions");
    assert!(pending.question.contains("Permission Request"));
    assert_eq!(
        pending.options,
        vec!["Approve".to_string(), "Deny".to_string()]
    );
    assert!(!pending.allow_custom);
    assert_eq!(
        pending.source,
        bamboo_agent_core::PendingQuestionSource::PauseTool
    );

    let first_event = rx.recv().await.expect("first event");
    assert!(matches!(first_event, AgentEvent::ToolComplete { .. }));

    let second_event = rx.recv().await.expect("second event");
    match second_event {
        AgentEvent::NeedClarification {
            question,
            options,
            tool_call_id,
            tool_name,
            allow_custom,
        } => {
            assert!(question.contains("Permission Request"));
            assert_eq!(
                options,
                Some(vec!["Approve".to_string(), "Deny".to_string()])
            );
            assert_eq!(tool_call_id, Some("perm-1".to_string()));
            assert_eq!(tool_name, Some("request_permissions".to_string()));
            assert!(!allow_custom);
        }
        other => panic!("unexpected second event: {other:?}"),
    }
}

#[tokio::test]
async fn maybe_handle_user_question_tool_persists_exit_plan_file_and_emits_update() {
    let tool_call = ToolCall {
        id: "exit-1".to_string(),
        tool_type: "function".to_string(),
        function: FunctionCall {
            name: "ExitPlanMode".to_string(),
            arguments: "{}".to_string(),
        },
    };
    let plan_body = "# Plan\n\n## investigate\n- task_id: investigate\n- Investigate\n\n## implement\n- task_id: implement\n- Implement\n\n## verify\n- task_id: verify\n- Verify";
    let result = ToolResult {
        success: true,
        result: serde_json::json!({
            "status": "awaiting_user_input",
            "question": "Review?",
            "options": ["Approve (Default mode)", "Stay in plan mode"],
            "allow_custom": false,
            "plan": plan_body,
            "exit_mode": "default"
        })
        .to_string(),
        display_preference: Some("conclusion_with_options".to_string()),
        images: Vec::new(),
    };

    let temp_dir = tempfile::tempdir().expect("temp dir");
    let config = AgentLoopConfig {
        app_data_dir: Some(temp_dir.path().to_path_buf()),
        ..AgentLoopConfig::default()
    };
    let (tx, mut rx) = mpsc::channel(8);
    let mut session = Session::new("session-exit-plan", "model");
    session.agent_runtime_state = Some(AgentRuntimeState::new("run-1"));
    session
        .agent_runtime_state
        .as_mut()
        .unwrap()
        .round
        .current_round = 5;
    session
        .agent_runtime_state
        .as_mut()
        .unwrap()
        .round
        .last_round_id = Some("round-5".to_string());
    session.agent_runtime_state.as_mut().unwrap().plan_mode = Some(PlanModeState {
        entered_at: Utc::now(),
        pre_permission_mode: "default".to_string(),
        plan_file_path: None,
        status: PlanModeStatus::Designing,
    });
    session.task_list = Some(bamboo_domain::TaskList {
        session_id: "session-exit-plan".to_string(),
        title: "Plan Tasks".to_string(),
        items: vec![
            bamboo_domain::TaskItem {
                id: "discovery".to_string(),
                description: "Discovery".to_string(),
                status: bamboo_domain::TaskItemStatus::Completed,
                ..bamboo_domain::TaskItem::default()
            },
            bamboo_domain::TaskItem {
                id: "investigate".to_string(),
                description: "Investigate".to_string(),
                status: bamboo_domain::TaskItemStatus::InProgress,
                ..bamboo_domain::TaskItem::default()
            },
            bamboo_domain::TaskItem {
                id: "implement".to_string(),
                description: "Implement".to_string(),
                status: bamboo_domain::TaskItemStatus::Pending,
                ..bamboo_domain::TaskItem::default()
            },
        ],
        created_at: Utc::now(),
        updated_at: Utc::now(),
    });

    let handled = maybe_handle_user_question_tool(
        &tool_call,
        &result,
        &mut session,
        &tx,
        None,
        "session-exit-plan",
        "round-1",
        &config,
    )
    .await;

    assert!(handled);
    let plan_mode = session
        .agent_runtime_state
        .as_ref()
        .and_then(|state| state.plan_mode.as_ref())
        .expect("plan mode should remain active while awaiting approval");
    assert_eq!(plan_mode.status, PlanModeStatus::AwaitingApproval);
    let plan_file_path = plan_mode
        .plan_file_path
        .as_ref()
        .expect("plan file path should be recorded");
    assert!(
        plan_file_path.contains("/plan/")
            || plan_file_path.ends_with("plan\\session-exit-plan.md")
            || plan_file_path.contains("\\plan\\")
    );
    let saved_plan = std::fs::read_to_string(plan_file_path).expect("saved plan file");
    assert_eq!(saved_plan, plan_body);

    let store = PlanStore::new(temp_dir.path()).expect("plan store");
    let state = store
        .read_state("session-exit-plan")
        .expect("read state")
        .expect("state should exist");
    assert_eq!(state.status.as_deref(), Some("awaiting_approval"));
    assert!(state.plan_hash.is_some());
    assert_eq!(state.active_section_id.as_deref(), Some("investigate"));
    assert_eq!(state.next_section_id.as_deref(), Some("implement"));
    assert_eq!(state.last_completed_task_id.as_deref(), Some("discovery"));
    assert_eq!(state.round_hint, Some(5));
    let cursor = store
        .read_cursor("session-exit-plan")
        .expect("read cursor")
        .expect("cursor should exist");
    assert_eq!(cursor.cursor_type.as_deref(), Some("task_item"));
    assert_eq!(cursor.current_section_id.as_deref(), Some("investigate"));
    assert_eq!(cursor.current_task_ordinal, Some(2));
    assert_eq!(cursor.next_task_id.as_deref(), Some("implement"));
    assert_eq!(cursor.next_task_ordinal, Some(3));
    assert_eq!(cursor.last_completed_task_id.as_deref(), Some("discovery"));
    assert_eq!(cursor.round_hint, Some(5));
    assert_eq!(cursor.round_id_hint.as_deref(), Some("round-5"));
    assert_eq!(
        cursor.suspension_hook_point.as_deref(),
        Some("AfterToolExecution")
    );
    assert_eq!(cursor.tool_call_boundary.as_deref(), Some("ExitPlanMode"));
    assert!(cursor
        .resume_note
        .as_deref()
        .unwrap_or("")
        .contains("Resume"));

    let first_event = rx.recv().await.expect("first event");
    assert!(matches!(first_event, AgentEvent::ToolComplete { .. }));

    let second_event = rx.recv().await.expect("second event");
    match second_event {
        AgentEvent::PlanFileUpdated {
            session_id,
            file_path,
            content_summary,
        } => {
            assert_eq!(session_id, "session-exit-plan");
            assert_eq!(file_path, *plan_file_path);
            assert!(content_summary.contains("# Plan") || content_summary.contains("Plan"));
        }
        other => panic!("unexpected second event: {other:?}"),
    }

    let third_event = rx.recv().await.expect("third event");
    assert!(matches!(third_event, AgentEvent::NeedClarification { .. }));
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
        images: Vec::new(),
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
