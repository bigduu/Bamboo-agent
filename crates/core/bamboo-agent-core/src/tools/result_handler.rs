use std::collections::VecDeque;
use std::sync::Arc;

use tokio::sync::mpsc;

use crate::composition::CompositionExecutor;
use crate::tools::executor::execute_tool_call_with_context;
use crate::tools::{
    convert_from_standard_result, plan_mode_allows_tool, AgenticToolResult, ToolCall, ToolError,
    ToolExecutionContext, ToolExecutionSessionFlags, ToolExecutor, ToolResult,
};
use crate::{AgentEvent, Message, PendingQuestionSource, Session};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolHandlingOutcome {
    Continue,
    AwaitingClarification,
    WaitingForChildren,
}

fn is_waiting_for_children_control(result: &ToolResult) -> bool {
    if result.display_preference.as_deref() == Some("runtime_control:waiting_for_children") {
        return true;
    }

    result.result.trim_start().starts_with('{')
        && serde_json::from_str::<serde_json::Value>(&result.result)
            .ok()
            .and_then(|value| value.get("runtime_control").cloned())
            .and_then(|control| control.as_str().map(str::to_string))
            .is_some_and(|control| control == "waiting_for_children")
}

pub const MAX_SUB_ACTIONS: usize = 64;

pub fn parse_tool_args(arguments: &str) -> std::result::Result<serde_json::Value, ToolError> {
    let args_raw = arguments.trim();

    if args_raw.is_empty() {
        return Ok(serde_json::json!({}));
    }

    serde_json::from_str(args_raw)
        .map_err(|error| ToolError::InvalidArguments(format!("Invalid JSON arguments: {error}")))
}

fn trim_end_whitespace_in_place(value: &mut String) {
    let trimmed_len = value.trim_end_matches(char::is_whitespace).len();
    value.truncate(trimmed_len);
}

fn strip_trailing_commas_in_place(value: &mut String) {
    loop {
        trim_end_whitespace_in_place(value);
        if value.ends_with(',') {
            value.pop();
            continue;
        }
        break;
    }
}

fn preview_for_log(value: &str, max_chars: usize) -> String {
    let mut iter = value.chars();
    let mut preview = String::new();
    for _ in 0..max_chars {
        match iter.next() {
            Some(ch) => preview.push(ch),
            None => break,
        }
    }
    if iter.next().is_some() {
        preview.push_str("...");
    }
    preview.replace('\n', "\\n").replace('\r', "\\r")
}

fn attempt_repair_truncated_json(arguments: &str) -> Option<String> {
    let args_raw = arguments.trim();
    if args_raw.is_empty() {
        return None;
    }
    if !args_raw.starts_with('{') && !args_raw.starts_with('[') {
        return None;
    }

    let mut stack: Vec<char> = Vec::new();
    let mut in_string = false;
    let mut escaped = false;

    for ch in args_raw.chars() {
        if in_string {
            if escaped {
                escaped = false;
                continue;
            }
            match ch {
                '\\' => escaped = true,
                '"' => in_string = false,
                _ => {}
            }
            continue;
        }

        match ch {
            '"' => in_string = true,
            '{' => stack.push('}'),
            '[' => stack.push(']'),
            '}' | ']' => {
                if stack.last().copied() == Some(ch) {
                    stack.pop();
                } else {
                    return None;
                }
            }
            _ => {}
        }
    }

    if !in_string && stack.is_empty() {
        return None;
    }

    let mut repaired = args_raw.to_string();
    if in_string {
        repaired.push('"');
    }

    while let Some(closing) = stack.pop() {
        strip_trailing_commas_in_place(&mut repaired);
        repaired.push(closing);
    }

    strip_trailing_commas_in_place(&mut repaired);
    Some(repaired)
}

/// Parse tool args with graceful fallback:
/// 1) strict JSON parse
/// 2) attempt repair for truncated/incomplete JSON
/// 3) fallback to empty object to keep the session alive
pub fn parse_tool_args_best_effort(arguments: &str) -> (serde_json::Value, Option<String>) {
    let args_raw = arguments.trim();
    if args_raw.is_empty() {
        return (serde_json::json!({}), None);
    }

    match serde_json::from_str::<serde_json::Value>(args_raw) {
        Ok(parsed) => (parsed, None),
        Err(primary_error) => {
            if let Some(repaired_json) = attempt_repair_truncated_json(args_raw) {
                match serde_json::from_str::<serde_json::Value>(&repaired_json) {
                    Ok(parsed) => {
                        let warning = format!(
                            "Invalid JSON arguments recovered via auto-repair: original_error={}, repaired_preview=\"{}\"",
                            primary_error,
                            preview_for_log(&repaired_json, 180)
                        );
                        return (parsed, Some(warning));
                    }
                    Err(repair_error) => {
                        let warning = format!(
                            "Invalid JSON arguments: {} (auto-repair failed: {}); falling back to empty object",
                            primary_error, repair_error
                        );
                        return (serde_json::json!({}), Some(warning));
                    }
                }
            }

            let warning = format!(
                "Invalid JSON arguments: {}; falling back to empty object",
                primary_error
            );
            (serde_json::json!({}), Some(warning))
        }
    }
}

pub fn try_parse_agentic_result(result: &ToolResult) -> Option<AgenticToolResult> {
    if result.result.trim_start().starts_with('{') {
        if let Ok(parsed) = serde_json::from_str::<AgenticToolResult>(&result.result) {
            return Some(parsed);
        }
    }

    match result.display_preference.as_deref() {
        Some("clarification") | Some("actions_needed") => {
            Some(convert_from_standard_result(result.clone()))
        }
        _ => None,
    }
}

pub async fn handle_tool_result_with_agentic_support(
    result: &ToolResult,
    tool_call: &ToolCall,
    event_tx: &mpsc::Sender<AgentEvent>,
    session: &mut Session,
    tools: &dyn ToolExecutor,
    session_flags: ToolExecutionSessionFlags,
    composition_executor: Option<Arc<CompositionExecutor>>,
) -> ToolHandlingOutcome {
    handle_tool_result_with_agentic_support_and_persistence(
        result,
        tool_call,
        event_tx,
        session,
        tools,
        session_flags,
        composition_executor,
        None,
    )
    .await
}

/// Engine integration variant that makes an agentic clarification durable
/// before publishing `NeedClarification`. The compatibility wrapper above is
/// retained for in-process callers without a persistence adapter.
#[allow(clippy::too_many_arguments)]
pub async fn handle_tool_result_with_agentic_support_and_persistence(
    result: &ToolResult,
    tool_call: &ToolCall,
    event_tx: &mpsc::Sender<AgentEvent>,
    session: &mut Session,
    tools: &dyn ToolExecutor,
    session_flags: ToolExecutionSessionFlags,
    composition_executor: Option<Arc<CompositionExecutor>>,
    clarification_persistence: Option<&Arc<dyn bamboo_domain::RuntimeSessionPersistence>>,
) -> ToolHandlingOutcome {
    let should_wait_for_children = is_waiting_for_children_control(result);
    if should_wait_for_children {
        session.metadata.insert(
            "runtime.suspend_reason".to_string(),
            "waiting_for_children".to_string(),
        );
    }
    let Some(agentic_result) = try_parse_agentic_result(result) else {
        // Image-producing tools (e.g. MCP `screenshot`) carry the picture in
        // `images`; route those into content_parts so the model actually sees
        // them. Text-only results keep the original (cheaper) path.
        let message = if result.images.is_empty() {
            Message::tool_result_with_status(
                tool_call.id.clone(),
                result.result.clone(),
                result.success,
            )
        } else {
            Message::tool_result_with_images(
                tool_call.id.clone(),
                result.result.clone(),
                result.success,
                result.images.clone(),
            )
        };
        session.add_message(message);
        return if should_wait_for_children {
            ToolHandlingOutcome::WaitingForChildren
        } else {
            ToolHandlingOutcome::Continue
        };
    };

    match agentic_result {
        AgenticToolResult::Success { result } => {
            session.add_message(Message::tool_result(tool_call.id.clone(), result));
            if should_wait_for_children {
                ToolHandlingOutcome::WaitingForChildren
            } else {
                ToolHandlingOutcome::Continue
            }
        }
        AgenticToolResult::Error { error } => {
            let _ = event_tx
                .send(AgentEvent::ToolError {
                    tool_call_id: tool_call.id.clone(),
                    error: error.clone(),
                })
                .await;

            session.add_message(Message::tool_result_with_status(
                tool_call.id.clone(),
                format!("Error: {error}"),
                false,
            ));

            ToolHandlingOutcome::Continue
        }
        AgenticToolResult::NeedClarification { question, options } => {
            persist_and_publish_agentic_clarification(
                event_tx,
                session,
                tool_call,
                question,
                options,
                clarification_persistence,
            )
            .await;

            ToolHandlingOutcome::AwaitingClarification
        }
        AgenticToolResult::NeedMoreActions { actions, reason } => {
            session.add_message(Message::tool_result(
                tool_call.id.clone(),
                format!(
                    "Need more actions: {reason} ({} actions pending)",
                    actions.len()
                ),
            ));

            execute_sub_actions_with_persistence(
                &actions,
                event_tx,
                session,
                tools,
                session_flags,
                composition_executor,
                clarification_persistence,
            )
            .await
        }
    }
}

async fn persist_and_publish_agentic_clarification(
    event_tx: &mpsc::Sender<AgentEvent>,
    session: &mut Session,
    tool_call: &ToolCall,
    question: String,
    options: Option<Vec<String>>,
    clarification_persistence: Option<&Arc<dyn bamboo_domain::RuntimeSessionPersistence>>,
) -> bool {
    persist_agentic_clarification(session, tool_call, question.clone(), options.clone());
    if let Some(persistence) = clarification_persistence {
        if let Err(error) = persistence.save_runtime_session(session).await {
            tracing::error!(
                session_id = %session.id,
                %error,
                "Failed to persist agentic clarification; suppressing NeedClarification"
            );
            let _ = event_tx
                .send(AgentEvent::Error {
                    message: format!(
                        "Clarification could not be saved; retry after session storage recovers: {error}"
                    ),
                })
                .await;
            return false;
        }
    }
    send_clarification_request(
        event_tx,
        question,
        options,
        Some(tool_call.id.clone()),
        Some(tool_call.function.name.clone()),
    )
    .await;
    true
}

fn persist_agentic_clarification(
    session: &mut Session,
    tool_call: &ToolCall,
    question: String,
    options: Option<Vec<String>>,
) {
    let normalized_options = options.unwrap_or_default();
    session.set_pending_question_with_source(
        tool_call.id.clone(),
        tool_call.function.name.clone(),
        question.clone(),
        normalized_options,
        true,
        PendingQuestionSource::AgenticClarification,
    );
    session.metadata.insert(
        "runtime.suspend_reason".to_string(),
        "awaiting_clarification".to_string(),
    );
    session.add_message(Message::tool_result(
        tool_call.id.clone(),
        format!("Clarification needed: {question}"),
    ));
}

pub async fn send_clarification_request(
    event_tx: &mpsc::Sender<AgentEvent>,
    question: String,
    options: Option<Vec<String>>,
    tool_call_id: Option<String>,
    tool_name: Option<String>,
) {
    let _ = event_tx
        .send(AgentEvent::NeedClarification {
            question,
            options,
            tool_call_id,
            tool_name,
            allow_custom: true,
            source: Some(PendingQuestionSource::AgenticClarification),
        })
        .await;
}

pub async fn execute_sub_actions(
    actions: &[ToolCall],
    event_tx: &mpsc::Sender<AgentEvent>,
    session: &mut Session,
    tools: &dyn ToolExecutor,
    session_flags: ToolExecutionSessionFlags,
    composition_executor: Option<Arc<CompositionExecutor>>,
) -> ToolHandlingOutcome {
    execute_sub_actions_with_persistence(
        actions,
        event_tx,
        session,
        tools,
        session_flags,
        composition_executor,
        None,
    )
    .await
}

async fn execute_sub_actions_with_persistence(
    actions: &[ToolCall],
    event_tx: &mpsc::Sender<AgentEvent>,
    session: &mut Session,
    tools: &dyn ToolExecutor,
    session_flags: ToolExecutionSessionFlags,
    composition_executor: Option<Arc<CompositionExecutor>>,
    clarification_persistence: Option<&Arc<dyn bamboo_domain::RuntimeSessionPersistence>>,
) -> ToolHandlingOutcome {
    let mut pending: VecDeque<ToolCall> = actions.iter().cloned().collect();
    let mut processed = 0usize;
    let available_tools = tools.list_tools();

    while let Some(action) = pending.pop_front() {
        if processed >= MAX_SUB_ACTIONS {
            let error = format!("Reached max sub-action limit ({MAX_SUB_ACTIONS})");
            let _ = event_tx
                .send(AgentEvent::ToolError {
                    tool_call_id: action.id.clone(),
                    error: error.clone(),
                })
                .await;
            session.add_message(Message::tool_result_with_status(
                action.id.clone(),
                error,
                false,
            ));
            return ToolHandlingOutcome::Continue;
        }

        processed += 1;

        if session_flags.plan_read_only && !plan_mode_allows_tool(&action.function.name) {
            let error = format!(
                "Plan mode: {} operation blocked",
                action.function.name.trim()
            );
            let _ = event_tx
                .send(AgentEvent::ToolError {
                    tool_call_id: action.id.clone(),
                    error: error.clone(),
                })
                .await;
            session.add_message(Message::tool_result_with_status(
                action.id.clone(),
                error,
                false,
            ));
            continue;
        }

        let args =
            parse_tool_args(&action.function.arguments).unwrap_or_else(|_| serde_json::json!({}));

        let _ = event_tx
            .send(AgentEvent::ToolStart {
                tool_call_id: action.id.clone(),
                tool_name: action.function.name.clone(),
                arguments: args,
            })
            .await;

        // NOTE: this is bamboo-agent-core's own loop; the bamboo SERVER runs the
        // bamboo-engine runtime (`tool_execution/per_call.rs`). Both build the
        // context via `for_dispatch` so per-session flags stay in sync.
        let tool_ctx = ToolExecutionContext::for_dispatch(
            &session.id,
            &action.id,
            event_tx,
            available_tools.as_slice(),
            session_flags,
            // bamboo-agent-core's own loop has no engine suspend/resume
            // machinery (that lives in bamboo-engine's pipeline), so it can
            // never safely auto-promote a Bash command — keep it synchronous
            // (issue #84, phase 2d).
            false,
            // Same reason: no engine loop to deliver a background-Bash
            // completion push into, so no completion sink.
            None,
            // This loop parses the `ToolStart` args with the stricter
            // `parse_tool_args` (not `parse_tool_args_best_effort`), which
            // differs on malformed input. Passing `None` lets the executor
            // re-parse leniently exactly as before — behavior preserved
            // (issue #106).
            None,
        );

        match execute_tool_call_with_context(&action, tools, composition_executor.clone(), tool_ctx)
            .await
        {
            Ok(result) => {
                let _ = event_tx
                    .send(AgentEvent::ToolComplete {
                        tool_call_id: action.id.clone(),
                        result: result.clone(),
                    })
                    .await;

                match try_parse_agentic_result(&result) {
                    Some(AgenticToolResult::Success { result }) => {
                        session.add_message(Message::tool_result(action.id.clone(), result));
                    }
                    Some(AgenticToolResult::Error { error }) => {
                        let _ = event_tx
                            .send(AgentEvent::ToolError {
                                tool_call_id: action.id.clone(),
                                error: error.clone(),
                            })
                            .await;
                        session.add_message(Message::tool_result_with_status(
                            action.id.clone(),
                            format!("Error: {error}"),
                            false,
                        ));
                    }
                    Some(AgenticToolResult::NeedClarification { question, options }) => {
                        persist_and_publish_agentic_clarification(
                            event_tx,
                            session,
                            &action,
                            question,
                            options,
                            clarification_persistence,
                        )
                        .await;
                        return ToolHandlingOutcome::AwaitingClarification;
                    }
                    Some(AgenticToolResult::NeedMoreActions {
                        actions: next_actions,
                        reason,
                    }) => {
                        session.add_message(Message::tool_result(
                            action.id.clone(),
                            format!(
                                "Need more actions: {reason} ({} actions pending)",
                                next_actions.len()
                            ),
                        ));
                        pending.extend(next_actions);
                    }
                    None => {
                        session.add_message(Message::tool_result_with_status(
                            action.id.clone(),
                            result.result.clone(),
                            result.success,
                        ));
                    }
                }
            }
            Err(error) => {
                let error_msg = error.to_string();
                let _ = event_tx
                    .send(AgentEvent::ToolError {
                        tool_call_id: action.id.clone(),
                        error: error_msg.clone(),
                    })
                    .await;
                session.add_message(Message::tool_result_with_status(
                    action.id.clone(),
                    format!("Error: {error_msg}"),
                    false,
                ));
            }
        }
    }

    ToolHandlingOutcome::Continue
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::mpsc;

    use crate::tools::{FunctionCall, ToolSchema};

    use super::*;

    struct StaticExecutor {
        results: HashMap<String, ToolResult>,
    }

    #[derive(Default)]
    struct BlockingClarificationPersistence {
        saved: std::sync::Mutex<Vec<Session>>,
        entered: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }

    #[async_trait]
    impl bamboo_domain::RuntimeSessionPersistence for BlockingClarificationPersistence {
        async fn save_runtime_session(&self, session: &mut Session) -> std::io::Result<()> {
            self.saved.lock().unwrap().push(session.clone());
            self.entered.notify_one();
            self.release.notified().await;
            Ok(())
        }
    }

    struct FailingClarificationPersistence;

    #[async_trait]
    impl bamboo_domain::RuntimeSessionPersistence for FailingClarificationPersistence {
        async fn save_runtime_session(&self, _session: &mut Session) -> std::io::Result<()> {
            Err(std::io::Error::other("injected clarification save failure"))
        }
    }

    impl StaticExecutor {
        fn new(results: HashMap<String, ToolResult>) -> Self {
            Self { results }
        }
    }

    #[async_trait]
    impl ToolExecutor for StaticExecutor {
        async fn execute(&self, call: &ToolCall) -> crate::tools::executor::Result<ToolResult> {
            self.results
                .get(&call.function.name)
                .cloned()
                .ok_or_else(|| ToolError::NotFound(call.function.name.clone()))
        }

        fn list_tools(&self) -> Vec<ToolSchema> {
            Vec::new()
        }
    }

    #[derive(Default)]
    struct ContextRecordingExecutor {
        calls: std::sync::atomic::AtomicUsize,
        flags: std::sync::Mutex<Vec<ToolExecutionSessionFlags>>,
    }

    #[async_trait]
    impl ToolExecutor for ContextRecordingExecutor {
        async fn execute(&self, _call: &ToolCall) -> crate::tools::executor::Result<ToolResult> {
            unreachable!("sub-actions must use context-aware dispatch")
        }

        async fn execute_with_context(
            &self,
            _call: &ToolCall,
            ctx: ToolExecutionContext<'_>,
        ) -> crate::tools::executor::Result<ToolResult> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.flags.lock().unwrap().push(ToolExecutionSessionFlags {
                bypass_permissions: ctx.bypass_permissions,
                auto_approve_permissions: ctx.auto_approve_permissions,
                plan_read_only: ctx.plan_read_only,
            });
            Ok(ToolResult::text(true, "sub-action-done"))
        }

        fn list_tools(&self) -> Vec<ToolSchema> {
            Vec::new()
        }
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

    #[tokio::test]
    async fn tool_result_with_images_adds_image_message_to_session() {
        let (event_tx, _rx) = mpsc::channel(8);
        let tools: Arc<dyn ToolExecutor> = Arc::new(StaticExecutor::new(HashMap::new()));
        let mut session = Session::new("s-img", "test-model");
        let tool_call = make_tool_call("call_shot", "screenshot", "{}");

        // A plain (non-agentic) tool result that carries an image, like an MCP
        // screenshot.
        let result = ToolResult {
            success: true,
            result: "screenshot 1280x536".to_string(),
            display_preference: None,
            images: vec![crate::tools::ToolResultImage {
                mime_type: "image/jpeg".to_string(),
                data: "AAAA".to_string(),
            }],
        };

        let outcome = handle_tool_result_with_agentic_support(
            &result,
            &tool_call,
            &event_tx,
            &mut session,
            tools.as_ref(),
            ToolExecutionSessionFlags::default(),
            None,
        )
        .await;

        assert_eq!(outcome, ToolHandlingOutcome::Continue);
        let msg = session
            .messages
            .last()
            .expect("a tool-result message was added");
        assert_eq!(msg.tool_call_id.as_deref(), Some("call_shot"));
        let parts = msg
            .content_parts
            .as_ref()
            .expect("image must be routed into content_parts");
        assert_eq!(parts.len(), 1);
    }

    #[tokio::test]
    async fn need_clarification_sends_event() {
        let (event_tx, mut event_rx) = mpsc::channel(8);
        let tools: Arc<dyn ToolExecutor> = Arc::new(StaticExecutor::new(HashMap::new()));
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
            ToolExecutionSessionFlags::default(),
            None,
        )
        .await;

        assert_eq!(outcome, ToolHandlingOutcome::AwaitingClarification);

        let event = event_rx.recv().await.expect("missing clarification event");
        match event {
            AgentEvent::NeedClarification {
                question, options, ..
            } => {
                assert_eq!(question, "Which file should I inspect?");
                assert_eq!(
                    options,
                    Some(vec!["src/main.rs".to_string(), "src/lib.rs".to_string()])
                );
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[tokio::test]
    async fn need_more_actions_executes_sub_actions() {
        let (event_tx, mut event_rx) = mpsc::channel(16);
        let sub_action = make_tool_call("call_sub", "sub_tool", "{}");
        let parent_call = make_tool_call("call_parent", "smart_tool", "{}");

        let mut results = HashMap::new();
        results.insert(
            "sub_tool".to_string(),
            ToolResult {
                success: true,
                result: "sub-action-done".to_string(),
                display_preference: None,
                images: Vec::new(),
            },
        );
        let tools: Arc<dyn ToolExecutor> = Arc::new(StaticExecutor::new(results));
        let mut session = Session::new("s2", "test-model");

        let result = ToolResult {
            success: true,
            result: serde_json::to_string(&AgenticToolResult::NeedMoreActions {
                actions: vec![sub_action],
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
            ToolExecutionSessionFlags::default(),
            None,
        )
        .await;

        assert_eq!(outcome, ToolHandlingOutcome::Continue);
        assert!(session
            .messages
            .iter()
            .any(
                |message| message.tool_call_id.as_deref() == Some("call_sub")
                    && message.content == "sub-action-done"
            ));

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
    async fn nested_clarification_is_persisted_before_its_event_is_published() {
        let (event_tx, mut event_rx) = mpsc::channel(16);
        let sub_action = make_tool_call("call_sub", "sub_tool", "{}");
        let parent_call = make_tool_call("call_parent", "smart_tool", "{}");
        let mut results = HashMap::new();
        results.insert(
            "sub_tool".to_string(),
            ToolResult {
                success: true,
                result: serde_json::to_string(&AgenticToolResult::NeedClarification {
                    question: "Which nested action?".to_string(),
                    options: Some(vec!["A".to_string(), "B".to_string()]),
                })
                .unwrap(),
                display_preference: None,
                images: Vec::new(),
            },
        );
        let tools: Arc<dyn ToolExecutor> = Arc::new(StaticExecutor::new(results));
        let persistence = Arc::new(BlockingClarificationPersistence::default());
        let persistence_port: Arc<dyn bamboo_domain::RuntimeSessionPersistence> =
            persistence.clone();
        let parent_result = ToolResult {
            success: true,
            result: serde_json::to_string(&AgenticToolResult::NeedMoreActions {
                actions: vec![sub_action],
                reason: "Need nested input".to_string(),
            })
            .unwrap(),
            display_preference: None,
            images: Vec::new(),
        };

        let handling = tokio::spawn(async move {
            let mut session = Session::new("nested-clarification", "test-model");
            let outcome = handle_tool_result_with_agentic_support_and_persistence(
                &parent_result,
                &parent_call,
                &event_tx,
                &mut session,
                tools.as_ref(),
                ToolExecutionSessionFlags::default(),
                None,
                Some(&persistence_port),
            )
            .await;
            (outcome, session)
        });

        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            persistence.entered.notified(),
        )
        .await
        .expect("nested clarification should reach persistence");
        let events_before_save: Vec<_> = std::iter::from_fn(|| event_rx.try_recv().ok()).collect();
        assert!(!events_before_save
            .iter()
            .any(|event| matches!(event, AgentEvent::NeedClarification { .. })));
        let saved = persistence.saved.lock().unwrap().last().cloned().unwrap();
        let pending = saved
            .pending_question
            .expect("saved nested pending question");
        assert_eq!(pending.tool_call_id, "call_sub");
        assert_eq!(pending.tool_name, "sub_tool");

        persistence.release.notify_one();
        let (outcome, session) = handling.await.unwrap();
        assert_eq!(outcome, ToolHandlingOutcome::AwaitingClarification);
        assert_eq!(session.pending_question.unwrap().tool_call_id, "call_sub");
        let event = tokio::time::timeout(std::time::Duration::from_secs(5), event_rx.recv())
            .await
            .expect("clarification should publish after save")
            .expect("event channel should stay open");
        assert!(matches!(
            event,
            AgentEvent::NeedClarification {
                tool_call_id: Some(ref id),
                ..
            } if id == "call_sub"
        ));
    }

    #[tokio::test]
    async fn failed_agentic_clarification_persistence_is_terminal_and_never_published() {
        let (event_tx, mut event_rx) = mpsc::channel(16);
        let tool_call = make_tool_call("call_pending", "smart_tool", "{}");
        let result = ToolResult {
            success: true,
            result: serde_json::to_string(&AgenticToolResult::NeedClarification {
                question: "Choose safely?".to_string(),
                options: Some(vec!["A".to_string(), "B".to_string()]),
            })
            .unwrap(),
            display_preference: None,
            images: Vec::new(),
        };
        let tools: Arc<dyn ToolExecutor> = Arc::new(StaticExecutor::new(HashMap::new()));
        let persistence: Arc<dyn bamboo_domain::RuntimeSessionPersistence> =
            Arc::new(FailingClarificationPersistence);
        let mut session = Session::new("failed-agentic-clarification", "test-model");

        let outcome = handle_tool_result_with_agentic_support_and_persistence(
            &result,
            &tool_call,
            &event_tx,
            &mut session,
            tools.as_ref(),
            ToolExecutionSessionFlags::default(),
            None,
            Some(&persistence),
        )
        .await;

        assert_eq!(outcome, ToolHandlingOutcome::AwaitingClarification);
        let events: Vec<_> = std::iter::from_fn(|| event_rx.try_recv().ok()).collect();
        assert!(events.iter().any(
            |event| matches!(event, AgentEvent::Error { message } if message.contains("could not be saved"))
        ));
        assert!(events
            .iter()
            .all(|event| !matches!(event, AgentEvent::NeedClarification { .. })));
    }

    #[tokio::test]
    async fn plan_blocks_mutating_sub_action_before_executor_dispatch() {
        let (event_tx, _event_rx) = mpsc::channel(16);
        let tools = Arc::new(ContextRecordingExecutor::default());
        let mut session = Session::new("plan-sub-action", "test-model");
        let parent_call = make_tool_call("call_parent", "smart_tool", "{}");
        let result = ToolResult {
            success: true,
            result: serde_json::to_string(&AgenticToolResult::NeedMoreActions {
                actions: vec![make_tool_call("call_write", "Write", "{}")],
                reason: "mutate".to_string(),
            })
            .unwrap(),
            display_preference: None,
            images: Vec::new(),
        };
        let flags = ToolExecutionSessionFlags {
            bypass_permissions: false,
            auto_approve_permissions: true,
            plan_read_only: true,
        };

        let outcome = handle_tool_result_with_agentic_support(
            &result,
            &parent_call,
            &event_tx,
            &mut session,
            tools.as_ref(),
            flags,
            None,
        )
        .await;

        assert_eq!(outcome, ToolHandlingOutcome::Continue);
        assert_eq!(tools.calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert!(session.messages.iter().any(|message| {
            message.tool_call_id.as_deref() == Some("call_write")
                && message.content.contains("Plan mode")
        }));
    }

    #[tokio::test]
    async fn global_and_explicit_auto_flags_survive_sub_action_dispatch_without_pause() {
        for explicit in [false, true] {
            let (event_tx, mut event_rx) = mpsc::channel(16);
            let tools = Arc::new(ContextRecordingExecutor::default());
            let mut session = Session::new(format!("auto-sub-{explicit}"), "test-model");
            let configured = if explicit {
                bamboo_domain::PermissionMode::Default
            } else {
                bamboo_domain::PermissionMode::Auto
            };
            if explicit {
                let mut runtime = bamboo_domain::AgentRuntimeState::new(&session.id);
                runtime.set_permission_mode(bamboo_domain::SessionPermissionMode::Auto);
                session.agent_runtime_state = Some(runtime);
            }
            let flags =
                ToolExecutionSessionFlags::from_session_and_configured_mode(&session, configured);
            assert!(flags.auto_approve_permissions);
            let parent_call = make_tool_call("call_parent", "smart_tool", "{}");
            let result = ToolResult {
                success: true,
                result: serde_json::to_string(&AgenticToolResult::NeedMoreActions {
                    actions: vec![make_tool_call("call_sub", "sub_tool", "{}")],
                    reason: "context".to_string(),
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
                flags,
                None,
            )
            .await;

            assert_eq!(outcome, ToolHandlingOutcome::Continue);
            assert!(!session.has_pending_question());
            assert_eq!(tools.flags.lock().unwrap().as_slice(), &[flags]);
            assert!(std::iter::from_fn(|| event_rx.try_recv().ok())
                .all(|event| !matches!(event, AgentEvent::NeedClarification { .. })));
        }
    }

    #[test]
    fn parse_tool_args_rejects_invalid_json() {
        let error = parse_tool_args("not-json").expect_err("invalid json should fail");
        assert!(matches!(error, ToolError::InvalidArguments(_)));
    }

    #[test]
    fn parse_tool_args_best_effort_repairs_truncated_json() {
        let (parsed, warning) = parse_tool_args_best_effort(r#"{"path":"README.md""#);

        assert_eq!(
            parsed.get("path").and_then(|v| v.as_str()),
            Some("README.md")
        );
        assert!(warning.is_some());
    }

    #[test]
    fn parse_tool_args_best_effort_falls_back_to_empty_object() {
        let (parsed, warning) = parse_tool_args_best_effort("not-json");

        assert_eq!(parsed, serde_json::json!({}));
        assert!(warning.is_some());
    }
}
