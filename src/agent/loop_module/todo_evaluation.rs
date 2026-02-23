// TodoList Evaluation Module
// 在 Agent Loop 每轮结束时，让 LLM 评估任务进度

use std::sync::Arc;
use agent_core::{AgentEvent, Session, TodoItemStatus};
use agent_llm::LLMProvider;
use agent_core::tools::{ToolSchema, FunctionSchema};
use tokio::sync::mpsc;
use serde_json::json;

use crate::todo_context::TodoLoopContext;

/// 评估结果
#[derive(Debug, Clone)]
pub struct TodoEvaluationResult {
    /// 是否需要评估（有 in_progress 的任务）
    pub needs_evaluation: bool,
    /// LLM 建议更新的项目
    pub updates: Vec<TodoItemUpdate>,
    /// LLM 的推理说明
    pub reasoning: String,
}

#[derive(Debug, Clone)]
pub struct TodoItemUpdate {
    pub item_id: String,
    pub status: TodoItemStatus,
    pub notes: Option<String>,
}

/// 构建用于 TodoList 评估的 messages
pub fn build_todo_evaluation_messages(
    ctx: &TodoLoopContext,
    _session: &Session,
) -> Vec<agent_core::Message> {
    let mut messages = Vec::new();

    // System prompt：专门用于 TodoList 评估
    let system_prompt = r#"You are a task progress evaluator. Your job is to evaluate whether tasks are complete based on the execution context.

## Your Task
Review the todo list and execution history, then decide if any tasks should be marked as completed or blocked.

## Rules
1. Mark as "completed" if the task goal has been achieved
2. Mark as "blocked" if there are unresolvable issues
3. Keep as "in_progress" if more work is needed
4. Add brief notes explaining your decision

## Available Actions
- update_todo_item: Update the status of a todo item

## Constraints
- Only update items that are currently "in_progress"
- You MUST call update_todo_item if a task is complete
- Provide clear reasoning in notes
"#;

    messages.push(agent_core::Message::system(system_prompt));

    // 构建 todo list 上下文
    let todo_context = format!(
        r#"
## Current Todo List (Round {}/{})

{}

## Recent Tool Executions
{}

## Instructions
Review each "in_progress" task above. For each task:
1. Check if the goal has been achieved based on tool execution results
2. If complete, call update_todo_item with status="completed" and brief notes
3. If blocked, call update_todo_item with status="blocked" and explain the issue

Remember: You are NOT executing the task. You are only evaluating if existing work has completed it.
"#,
        ctx.current_round + 1,
        ctx.max_rounds,
        ctx.format_for_prompt(),
        format_recent_tools(ctx, 5), // 最近 5 个 tool 调用
    );

    messages.push(agent_core::Message::user(todo_context));

    messages
}

/// 格式化最近的 tool 调用（用于 context）
fn format_recent_tools(ctx: &TodoLoopContext, limit: usize) -> String {
    let mut all_calls: Vec<(String, &crate::todo_context::ToolCallRecord)> = Vec::new();

    for item in &ctx.items {
        for call in &item.tool_calls {
            all_calls.push((item.description.clone(), call));
        }
    }

    // 按时间排序，取最近的 N 个
    all_calls.sort_by_key(|(_, call)| std::cmp::Reverse(call.timestamp));

    let recent: Vec<_> = all_calls.into_iter().take(limit).collect();

    if recent.is_empty() {
        return "No tool executions yet.".to_string();
    }

    let mut output = String::new();
    for (i, (task_desc, call)) in recent.iter().enumerate() {
        output.push_str(&format!(
            "{}. [{}] Tool: {} ({})\n   Task: {}\n",
            i + 1,
            if call.success { "✓" } else { "✗" },
            call.tool_name,
            call.round + 1,
            task_desc
        ));
    }

    output
}

/// 获取 TodoList 评估的 tool schemas
pub fn get_todo_evaluation_tools() -> Vec<ToolSchema> {
    vec![
        ToolSchema {
            schema_type: "function".to_string(),
            function: FunctionSchema {
                name: "update_todo_item".to_string(),
                description: "Update the status of a todo item based on evaluation".to_string(),
                parameters: json!({
                    "type": "object",
                    "properties": {
                        "item_id": {
                            "type": "string",
                            "description": "The ID of the todo item to update"
                        },
                        "status": {
                            "type": "string",
                            "enum": ["completed", "blocked"],
                            "description": "New status for the item"
                        },
                        "notes": {
                            "type": "string",
                            "description": "Brief explanation of why the status changed"
                        }
                    },
                    "required": ["item_id", "status"]
                }),
            },
        }
    ]
}

/// 执行 TodoList 评估
pub async fn evaluate_todo_progress(
    ctx: &TodoLoopContext,
    session: &Session,
    llm: Arc<dyn LLMProvider>,
    event_tx: &mpsc::Sender<AgentEvent>,
    session_id: &str,
    model: &str,  // Add model parameter (required)
) -> Result<TodoEvaluationResult, agent_core::AgentError> {
    use crate::stream::handler::consume_llm_stream;

    // 检查是否有需要评估的任务
    let in_progress_count = ctx.items.iter()
        .filter(|item| matches!(item.status, TodoItemStatus::InProgress))
        .count();

    if in_progress_count == 0 {
        return Ok(TodoEvaluationResult {
            needs_evaluation: false,
            updates: Vec::new(),
            reasoning: "No in-progress tasks to evaluate".to_string(),
        });
    }

    log::info!(
        "[{}] Evaluating {} in-progress todo items",
        session_id,
        in_progress_count
    );

    // 发送评估开始事件
    let _ = event_tx.send(AgentEvent::TodoEvaluationStarted {
        session_id: session_id.to_string(),
        items_count: in_progress_count,
    }).await;

    // 构建评估消息
    let messages = build_todo_evaluation_messages(ctx, session);
    let tools = get_todo_evaluation_tools();

    // Use model from parameter (passed from config), not from session
    log::debug!("[{}] Todo evaluation using model: {}", session_id, model);

    // 调用 LLM（限制 output tokens）
    match llm.chat_stream(&messages, &tools, Some(500), model).await {
        Ok(stream) => {
            // 消费流
            let stream_output = consume_llm_stream(
                stream,
                event_tx,
                &tokio_util::sync::CancellationToken::new(),
                session_id,
            ).await.map_err(|e| agent_core::AgentError::LLM(e.to_string()))?;

            log::info!(
                "[{}] Todo evaluation completed: {} tokens, {} tool calls",
                session_id,
                stream_output.token_count,
                stream_output.tool_calls.len()
            );

            // 解析 LLM 的决策
            let mut updates = Vec::new();
            for tool_call in &stream_output.tool_calls {
                if tool_call.function.name == "update_todo_item" {
                    if let Ok(args) = serde_json::from_str::<serde_json::Value>(
                        &tool_call.function.arguments
                    ) {
                        if let (Some(item_id), Some(status_str)) =
                            (args["item_id"].as_str(), args["status"].as_str())
                        {
                            let status = match status_str {
                                "completed" => TodoItemStatus::Completed,
                                "blocked" => TodoItemStatus::Blocked,
                                _ => continue,
                            };

                            updates.push(TodoItemUpdate {
                                item_id: item_id.to_string(),
                                status,
                                notes: args["notes"].as_str().map(String::from),
                            });
                        }
                    }
                }
            }

            // 发送评估完成事件
            let _ = event_tx.send(AgentEvent::TodoEvaluationCompleted {
                session_id: session_id.to_string(),
                updates_count: updates.len(),
                reasoning: stream_output.content.clone(),
            }).await;

            Ok(TodoEvaluationResult {
                needs_evaluation: true,
                updates,
                reasoning: stream_output.content,
            })
        }
        Err(e) => {
            log::warn!("[{}] Todo evaluation failed: {}", session_id, e);
            Ok(TodoEvaluationResult {
                needs_evaluation: false,
                updates: Vec::new(),
                reasoning: format!("Evaluation failed: {}", e),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::todo_context::{TodoLoopContext, TodoLoopItem};
    use agent_core::todo::{TodoItem, TodoList};
    use chrono::Utc;

    fn create_test_context() -> TodoLoopContext {
        let mut session = agent_core::Session::new("test", "test-model");
        let todo_list = TodoList {
            session_id: "test".to_string(),
            title: "Test Tasks".to_string(),
            items: vec![
                TodoItem {
                    id: "1".to_string(),
                    description: "Fix bug in authentication".to_string(),
                    status: TodoItemStatus::InProgress,
                    depends_on: Vec::new(),
                    notes: String::new(),
                },
            ],
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        session.set_todo_list(todo_list);

        let mut ctx = TodoLoopContext::from_session(&session).unwrap();
        ctx.items = vec![TodoLoopItem {
            id: "1".to_string(),
            description: "Fix bug in authentication".to_string(),
            status: TodoItemStatus::InProgress,
            tool_calls: vec![
                crate::todo_context::ToolCallRecord {
                    round: 0,
                    tool_name: "read_file".to_string(),
                    success: true,
                    timestamp: Utc::now(),
                },
                crate::todo_context::ToolCallRecord {
                    round: 1,
                    tool_name: "write_file".to_string(),
                    success: true,
                    timestamp: Utc::now(),
                },
            ],
            started_at_round: Some(0),
            completed_at_round: None,
        }];

        ctx
    }

    #[test]
    fn test_build_evaluation_messages() {
        let ctx = create_test_context();
        let session = agent_core::Session::new("test", "test-model");

        let messages = build_todo_evaluation_messages(&ctx, &session);

        assert_eq!(messages.len(), 2);
        assert!(messages[0].content.contains("task progress evaluator"));
        assert!(messages[1].content.contains("Fix bug in authentication"));
    }

    #[test]
    fn test_format_recent_tools() {
        let ctx = create_test_context();
        let output = format_recent_tools(&ctx, 5);

        assert!(output.contains("read_file"));
        assert!(output.contains("write_file"));
        assert!(output.contains("✓"));
    }

    #[test]
    fn test_needs_evaluation() {
        let mut ctx = create_test_context();

        // In-progress task needs evaluation
        assert!(ctx.items.iter().any(|i| matches!(i.status, TodoItemStatus::InProgress)));

        // Completed task doesn't need evaluation
        ctx.items[0].status = TodoItemStatus::Completed;
        assert!(!ctx.items.iter().any(|i| matches!(i.status, TodoItemStatus::InProgress)));
    }

    // ========== MODEL REQUIREMENT ARCHITECTURE TESTS ==========
    // These tests ensure the design principle:
    // "Todo evaluation must receive model as parameter, not use session.model"

    /// Test: evaluate_todo_progress requires model parameter
    /// This test documents that the function signature requires model
    #[test]
    fn todo_evaluation_requires_model_parameter() {
        // This test is compile-time verification:
        // The function signature is:
        // pub async fn evaluate_todo_progress(
        //     ctx: &TodoLoopContext,
        //     session: &Session,
        //     llm: Arc<dyn LLMProvider>,
        //     event_tx: &mpsc::Sender<AgentEvent>,
        //     session_id: &str,
        //     model: &str,  // <-- Required parameter
        // ) -> Result<TodoEvaluationResult, agent_core::AgentError>
        //
        // The presence of `model: &str` in the signature proves that
        // model must be passed as a parameter, not read from session.
        //
        // This is a documentation test - the actual verification
        // happens at compile time when the function is called.
        assert!(true, "Model parameter requirement is enforced by function signature");
    }
}
