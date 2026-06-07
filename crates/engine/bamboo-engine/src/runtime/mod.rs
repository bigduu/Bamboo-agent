//! Agent execution runtime: loop, stream handling, task evaluation.

pub mod agent;
pub mod complexity_classifier;
pub mod config;
pub mod context;
pub mod execution;
pub mod gold_evaluation;
pub mod hooks;
pub mod managers;
pub mod model_roster;
pub mod runner;
#[allow(clippy::module_inception)]
pub mod runtime;
pub mod stream;
pub mod task_context;
pub mod task_evaluation;

pub use agent::{Agent, AgentBuilder};
pub use bamboo_domain::RuntimeSessionPersistence;
pub use complexity_classifier::{ComplexityClassifier, TaskComplexity};
pub use config::{GoldConfig, ImageFallbackConfig, ImageFallbackMode};
// `AgentLoopConfig` is intentionally NOT re-exported: its fields are `pub(crate)`,
// so it is unconstructible outside the engine. Internal call sites reach it via
// `crate::runtime::config::AgentLoopConfig`. External code drives the loop only
// through `AgentRuntime::execute`.
pub use execution::runner_state::{AgentRunner, AgentStatus};
pub use hooks::HookRunner;
pub use managers::{
    LifecycleManager, LlmManager, MemoryManager, MiniLoopExecutor, PromptManager, ToolManager,
};
pub use model_roster::{ModelRoster, RoleModel};
pub use runtime::{AgentRuntime, AgentRuntimeBuilder, ExecuteRequest, ExecuteRequestBuilder};
pub use task_context::TaskLoopContext;
pub use task_evaluation::{evaluate_task_progress, TaskEvaluationResult};

#[cfg(test)]
mod tests;
