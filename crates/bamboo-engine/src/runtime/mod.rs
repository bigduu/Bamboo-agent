//! Agent execution runtime: loop, stream handling, task evaluation.

pub mod agent;
pub mod complexity_classifier;
pub mod config;
pub mod context;
pub mod execution;
pub mod gold_evaluation;
pub mod hooks;
pub mod managers;
pub mod runner;
pub mod runtime;
pub mod stream;
pub mod task_context;
pub mod task_evaluation;

pub use agent::{Agent, AgentBuilder};
pub use bamboo_domain::RuntimeSessionPersistence;
pub use complexity_classifier::{ComplexityClassifier, TaskComplexity};
pub use config::{AgentLoopConfig, GoldConfig, ImageFallbackConfig, ImageFallbackMode};
pub use execution::runner_state::{AgentRunner, AgentStatus};
pub use hooks::HookRunner;
pub use managers::{
    LifecycleManager, LlmManager, MemoryManager, MiniLoopExecutor, PromptManager, ToolManager,
};
pub use runner::{run_agent_loop, run_agent_loop_with_config};
pub use runtime::{AgentRuntime, AgentRuntimeBuilder, ExecuteRequest};
pub use task_context::TaskLoopContext;
pub use task_evaluation::{evaluate_task_progress, TaskEvaluationResult};

#[cfg(test)]
mod tests;
