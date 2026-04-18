// TaskList Evaluation Module
// 在 Agent Loop 每轮结束时，让 LLM 评估任务进度

use bamboo_domain_session::TaskItemStatus;

mod executor;
mod message_builder;
mod schema;
mod token_estimation;
mod update_parsing;

/// 评估结果
#[derive(Debug, Clone)]
pub struct TaskEvaluationResult {
    /// 是否需要评估（有 in_progress 的任务）
    pub needs_evaluation: bool,
    /// LLM 建议更新的项目
    pub updates: Vec<TaskItemUpdate>,
    /// LLM 的推理说明
    pub reasoning: String,
    /// Estimated prompt tokens consumed by the evaluation call
    pub prompt_tokens: u64,
    /// Estimated completion tokens consumed by the evaluation call
    pub completion_tokens: u64,
}

#[derive(Debug, Clone)]
pub struct TaskItemUpdate {
    pub item_id: String,
    pub status: TaskItemStatus,
    pub notes: Option<String>,
    pub evidence: Option<String>,
    pub blocker: Option<String>,
    pub criteria_met: Option<Vec<String>>,
}

pub use executor::evaluate_task_progress;
pub use message_builder::build_task_evaluation_messages;
pub use schema::get_task_evaluation_tools;

#[cfg(test)]
mod tests;
