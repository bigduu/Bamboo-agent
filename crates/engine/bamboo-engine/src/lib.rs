//! Bamboo engine — runtime, agent loop, and orchestration.

pub mod app_context;
pub mod auto_dream;
pub mod events;
pub mod external_agents;
pub mod gardener;
pub mod gold_auto_answer;
pub mod llm_summarizer;
pub mod message_hooks;
pub mod model_areas;
pub mod model_config_helper;
pub mod prompt_defaults;
pub mod runtime;
pub mod sdk;
pub mod session_app;
pub mod session_repository;
pub use session_repository::SessionRepository;
pub mod title_gen;
pub mod token_usage_log;
pub use token_usage_log::TokenUsageRecord;

pub use app_context::AgentSessionContext;
pub use runtime::execution::agent_spawn::{read_cached_session, SessionCache};
pub use session_app::child_completion_coordinator::ChildCompletionCoordinator;

// Re-export commonly used types from agent (via dependency)
pub use bamboo_agent_core::{
    AgentError, AgentEvent, AgentHook, FunctionCall, FunctionSchema, Message, MessageContent,
    MessagePart, MessagePhase, PromptSnapshot, Role, Session, TokenUsage, Tool, ToolCall,
    ToolError, ToolExecutionContext, ToolExecutor, ToolRegistry, ToolResult, ToolSchema,
};

// Re-export from runtime
pub use bamboo_domain::RuntimeSessionPersistence;
pub use runtime::agent::AgentBuilder;
// `AgentLoopConfig` is intentionally NOT re-exported: its fields are `pub(crate)`,
// so it cannot be constructed outside the engine. Execution funnels solely through
// `AgentRuntime::execute`.
pub use runtime::config::{
    ApprovalDelegate, AuxiliaryModelConfig, ChildApprovalOutcome, ChildApprovalRequest,
    GuardianConfig, GuardianSpawner, ImageFallbackConfig, ImageFallbackMode,
};
pub use runtime::execution::runner_state::{AgentRunner, AgentStatus};
pub use runtime::hooks::HookRunner;
pub use runtime::managers::{
    LifecycleManager, LlmManager, MemoryManager, MiniLoopExecutor, PromptManager, ToolManager,
};
pub use runtime::model_roster::{ModelRoster, RoleModel};
pub use runtime::runtime::{
    AgentRuntime, AgentRuntimeBuilder, ExecuteRequest, ExecuteRequestBuilder,
};
pub use runtime::task_context::TaskLoopContext;
pub use runtime::task_evaluation::{evaluate_task_progress, TaskEvaluationResult};
pub use runtime::Agent;

// Re-export from the ergonomic SDK surface (anti-fork single spawn core).
pub use sdk::runner::{child_runner, ChildRunner, RunChildInput};
pub use sdk::spawn::run_child_spawn;

// Sub-module re-exports for engine's own runtime modules
pub mod runner {
    pub use crate::runtime::runner::*;
}
pub mod context {
    pub use crate::runtime::context::*;
}
pub mod execution {
    pub use crate::runtime::execution::*;
}
pub mod config {
    pub use crate::runtime::config::*;
}
pub mod hooks {
    pub use crate::runtime::hooks::*;
}
pub mod managers {
    pub use crate::runtime::managers::*;
}
pub mod stream {
    pub use crate::runtime::stream::*;
}
pub mod task_context {
    pub use crate::runtime::task_context::*;
}
pub mod task_evaluation {
    pub use crate::runtime::task_evaluation::*;
}
pub mod agent {
    pub use crate::runtime::agent::*;
}
