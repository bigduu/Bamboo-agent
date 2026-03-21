use std::cmp::Reverse;

use crate::agent::core::{Message, Session};

use super::super::task_context::{TaskLoopContext, ToolCallRecord};

/// 构建用于 TaskList 评估的 messages
pub fn build_task_evaluation_messages(ctx: &TaskLoopContext, _session: &Session) -> Vec<Message> {
    let mut messages = Vec::new();

    let system_prompt = r#"You are a task progress evaluator. Your job is to evaluate whether tasks are complete based on the execution context.

## Your Task
Review the task list and execution history, then decide if any tasks should be marked as completed or blocked.

## Rules
1. Mark as "completed" if the task goal has been achieved
2. Mark as "blocked" if there are unresolvable issues
3. Keep as "in_progress" if more work is needed
4. Add brief notes explaining your decision

## Available Actions
- update_task_item: Update the status of a task item

## Constraints
- Only update items that are currently "in_progress"
- You MUST call update_task_item if a task is complete
- Provide clear reasoning in notes
"#;

    messages.push(Message::system(system_prompt));

    let task_context = format!(
        r#"
## Current Task List (Round {}/{})

{}

## Recent Tool Executions
{}

## Instructions
Review each "in_progress" task above. For each task:
1. Check if the goal has been achieved based on tool execution results
2. If complete, call update_task_item with status="completed" and brief notes
3. If blocked, call update_task_item with status="blocked" and explain the issue

Remember: You are NOT executing the task. You are only evaluating if existing work has completed it.
"#,
        ctx.current_round + 1,
        ctx.max_rounds,
        ctx.format_for_prompt(),
        format_recent_tools(ctx, 5),
    );

    messages.push(Message::user(task_context));
    messages
}

/// 格式化最近的 tool 调用（用于 context）
pub(super) fn format_recent_tools(ctx: &TaskLoopContext, limit: usize) -> String {
    let mut all_calls: Vec<(String, &ToolCallRecord)> = Vec::new();

    for item in &ctx.items {
        for call in &item.tool_calls {
            all_calls.push((item.description.clone(), call));
        }
    }

    all_calls.sort_by_key(|(_, call)| Reverse(call.timestamp));

    let recent: Vec<_> = all_calls.into_iter().take(limit).collect();

    if recent.is_empty() {
        return "No tool executions yet.".to_string();
    }

    let mut output = String::new();
    for (index, (task_desc, call)) in recent.iter().enumerate() {
        output.push_str(&format!(
            "{}. [{}] Tool: {} ({})\n   Task: {}\n",
            index + 1,
            if call.success { "✓" } else { "✗" },
            call.tool_name,
            call.round + 1,
            task_desc
        ));
    }

    output
}
