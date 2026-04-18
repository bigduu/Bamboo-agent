//! Agent execution runtime: loop, stream handling, task evaluation.

pub mod agent;
pub mod config;
pub mod context;
pub mod execution;
pub mod runner;
pub mod runtime;
pub mod stream;
pub mod task_context;
pub mod task_evaluation;

pub use agent::{Agent, AgentBuilder};
pub use config::{AgentLoopConfig, ImageFallbackConfig, ImageFallbackMode};
pub use execution::runner_state::{AgentRunner, AgentStatus};
pub use runner::{run_agent_loop, run_agent_loop_with_config};
pub use runtime::{AgentRuntime, AgentRuntimeBuilder, ExecuteRequest};
pub use task_context::TaskLoopContext;
pub use task_evaluation::{evaluate_task_progress, TaskEvaluationResult};

#[cfg(test)]
mod tests;
