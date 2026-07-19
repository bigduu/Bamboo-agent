use std::sync::Arc;
use std::sync::{Mutex, MutexGuard, OnceLock};

use bamboo_agent_core::composition::CompositionExecutor;
use bamboo_agent_core::tools::{
    execute_tool_call, handle_tool_result_with_agentic_support, AgenticToolResult, FunctionCall,
    ToolCall, ToolExecutor, ToolHandlingOutcome, ToolRegistry, ToolResult,
};
use bamboo_agent_core::{AgentEvent, Session};
use bamboo_tools::BuiltinToolExecutor;
use tokio::sync::mpsc;

use super::config::AgentLoopConfig;

/// Acquire a process-wide lock to serialize tests that mutate environment variables.
pub(crate) fn env_cache_lock_acquire() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn make_tool_call(id: &str, name: &str, arguments: &str) -> ToolCall {
    ToolCall {
        id: id.to_string(),
        tool_type: "function".to_string(),
        function: FunctionCall {
            name: name.to_string(),
            arguments: arguments.to_string(),
        },
    }
}

#[test]
fn agent_loop_config_default() {
    let config = AgentLoopConfig::default();
    assert_eq!(config.max_rounds, 200);
    assert!(config.system_prompt.is_none());
    assert!(config.additional_tool_schemas.is_empty());
    assert!(config.tool_registry.is_empty());
    assert!(config.skill_manager.is_none());
    assert!(config.selected_skill_ids.is_none());
    assert!(config.disabled_tools.is_empty());
    assert!(!config.skip_initial_user_message);
    // Issue #221: no budget configured anywhere means unlimited, matching
    // every other resource knob's opt-in-only default.
    assert_eq!(config.run_budget, bamboo_config::RunBudgetConfig::default());
}

/// Issue #221 plumb-through (engine hop): `ExecuteRequestBuilder::run_budget`
/// is the exact setter the server's `agent_spawn::build_execute_request`
/// calls when the HTTP request carried an override — verify it lands
/// unmodified on the built `ExecuteRequest`, ready for
/// `AgentRuntime::execute` to merge against the config-level default.
#[test]
fn execute_request_builder_carries_run_budget_override_through_to_the_request() {
    use super::runtime::ExecuteRequestBuilder;
    use tokio_util::sync::CancellationToken;

    let (tx, _rx) = mpsc::channel(1);
    let override_budget = bamboo_config::RunBudgetConfig {
        max_total_tokens: Some(42_000),
        max_tool_calls: Some(7),
        max_subagents: None,
    };

    let request = ExecuteRequestBuilder::new("hello", tx, CancellationToken::new())
        .run_budget(override_budget)
        .build();

    assert_eq!(request.run_budget, Some(override_budget));
}

/// Building WITHOUT `.run_budget(..)` leaves it `None` — `AgentRuntime::execute`
/// must read that as "use the config-level default", not "unlimited".
#[test]
fn execute_request_builder_defaults_run_budget_to_none() {
    use super::runtime::ExecuteRequestBuilder;
    use tokio_util::sync::CancellationToken;

    let (tx, _rx) = mpsc::channel(1);
    let request = ExecuteRequestBuilder::new("hello", tx, CancellationToken::new()).build();

    assert!(request.run_budget.is_none());
}

#[test]
fn skip_initial_message_flag() {
    let config = AgentLoopConfig {
        skip_initial_user_message: true,
        ..Default::default()
    };
    assert!(config.skip_initial_user_message);
}

#[tokio::test]
async fn need_clarification_sends_event() {
    let (event_tx, mut event_rx) = mpsc::channel(8);
    let tools: Arc<dyn ToolExecutor> = Arc::new(BuiltinToolExecutor::new());
    let mut session = Session::new("s1", "test-model");
    let tool_call = make_tool_call("call_parent", "smart_tool", "{}");
    let result = ToolResult {
        success: true,
        result: serde_json::to_string(&AgenticToolResult::NeedClarification {
            question: "Which file should I inspect?".to_string(),
            options: Some(vec!["src/main.rs".to_string(), "src/lib.rs".to_string()]),
        })
        .unwrap(),
        display_preference: None,
        images: Vec::new(),
    };

    let outcome = handle_tool_result_with_agentic_support(
        &result,
        &tool_call,
        &event_tx,
        &mut session,
        tools.as_ref(),
        None,
    )
    .await;

    assert_eq!(outcome, ToolHandlingOutcome::AwaitingClarification);
    let pending = session
        .pending_question
        .as_ref()
        .expect("agentic clarification should persist pending question");
    assert_eq!(pending.tool_call_id, "call_parent");
    assert_eq!(pending.tool_name, "smart_tool");
    assert_eq!(pending.question, "Which file should I inspect?");
    assert_eq!(
        pending.options,
        vec!["src/main.rs".to_string(), "src/lib.rs".to_string()]
    );
    assert_eq!(
        session
            .metadata
            .get("runtime.suspend_reason")
            .map(String::as_str),
        Some("awaiting_clarification")
    );

    let event = event_rx.recv().await.expect("missing clarification event");
    match event {
        AgentEvent::NeedClarification {
            question,
            options,
            tool_call_id,
            tool_name,
            ..
        } => {
            assert_eq!(question, "Which file should I inspect?");
            assert_eq!(
                options,
                Some(vec!["src/main.rs".to_string(), "src/lib.rs".to_string()])
            );
            assert_eq!(tool_call_id.as_deref(), Some("call_parent"));
            assert_eq!(tool_name.as_deref(), Some("smart_tool"));
        }
        other => panic!("unexpected event: {other:?}"),
    }
}

#[tokio::test]
async fn need_more_actions_executes_sub_actions() {
    let (event_tx, mut event_rx) = mpsc::channel(16);
    let tools: Arc<dyn ToolExecutor> = Arc::new(BuiltinToolExecutor::new());
    let mut session = Session::new("s2", "test-model");
    let file = tempfile::NamedTempFile::new().unwrap();
    tokio::fs::write(file.path(), "workspace context\n")
        .await
        .unwrap();
    let sub_action = make_tool_call(
        "call_sub",
        "Read",
        &serde_json::json!({
            "file_path": file.path()
        })
        .to_string(),
    );
    let parent_call = make_tool_call("call_parent", "smart_tool", "{}");
    let result = ToolResult {
        success: true,
        result: serde_json::to_string(&AgenticToolResult::NeedMoreActions {
            actions: vec![sub_action.clone()],
            reason: "Need workspace context".to_string(),
        })
        .unwrap(),
        display_preference: None,
        images: Vec::new(),
    };

    let outcome = handle_tool_result_with_agentic_support(
        &result,
        &parent_call,
        &event_tx,
        &mut session,
        tools.as_ref(),
        None,
    )
    .await;

    assert_eq!(outcome, ToolHandlingOutcome::Continue);
    assert!(session.messages.iter().any(|message| {
        message.tool_call_id.as_deref() == Some("call_sub") && !message.content.is_empty()
    }));

    let mut saw_sub_start = false;
    let mut saw_sub_complete = false;

    while let Ok(event) = event_rx.try_recv() {
        match event {
            AgentEvent::ToolStart { tool_call_id, .. } if tool_call_id == "call_sub" => {
                saw_sub_start = true;
            }
            AgentEvent::ToolComplete { tool_call_id, .. } if tool_call_id == "call_sub" => {
                saw_sub_complete = true;
            }
            _ => {}
        }
    }

    assert!(saw_sub_start);
    assert!(saw_sub_complete);
}

#[tokio::test]
async fn execute_tool_call_falls_back_when_composition_misses_tool() {
    let tools: Arc<dyn ToolExecutor> = Arc::new(BuiltinToolExecutor::new());
    let composition_executor = Arc::new(CompositionExecutor::new(Arc::new(ToolRegistry::new())));
    let file = tempfile::NamedTempFile::new().unwrap();
    tokio::fs::write(file.path(), "fallback\n").await.unwrap();
    let tool_call = make_tool_call(
        "call_sub",
        "Read",
        &serde_json::json!({
            "file_path": file.path(),
        })
        .to_string(),
    );

    let result = execute_tool_call(&tool_call, tools.as_ref(), Some(composition_executor))
        .await
        .expect("fallback execution should succeed");

    assert!(result.success);
    assert!(!result.result.is_empty());
}
