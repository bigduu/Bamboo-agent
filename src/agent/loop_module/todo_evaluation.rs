// TodoList Evaluation Module
// 在 Agent Loop 每轮结束时，让 LLM 评估任务进度

use crate::agent::core::TodoItemStatus;

mod executor;
mod message_builder;
mod schema;
mod token_estimation;
mod update_parsing;

/// 评估结果
#[derive(Debug, Clone)]
pub struct TodoEvaluationResult {
    /// 是否需要评估（有 in_progress 的任务）
    pub needs_evaluation: bool,
    /// LLM 建议更新的项目
    pub updates: Vec<TodoItemUpdate>,
    /// LLM 的推理说明
    pub reasoning: String,
    /// Estimated prompt tokens consumed by the evaluation call
    pub prompt_tokens: u64,
    /// Estimated completion tokens consumed by the evaluation call
    pub completion_tokens: u64,
}

#[derive(Debug, Clone)]
pub struct TodoItemUpdate {
    pub item_id: String,
    pub status: TodoItemStatus,
    pub notes: Option<String>,
}

pub use executor::evaluate_todo_progress;
pub use message_builder::build_todo_evaluation_messages;
pub use schema::get_todo_evaluation_tools;

#[cfg(test)]
mod tests;
