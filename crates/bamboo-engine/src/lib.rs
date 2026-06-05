//! Bamboo engine — runtime, skills, metrics, MCP.

pub mod app_context;
pub mod auto_dream;
pub mod events;
pub mod external_agents;
pub mod gardener;
pub mod gold_auto_answer;
pub mod mcp;
pub mod llm_summarizer;
pub mod message_hooks;
pub mod metrics;
pub mod metrics_service;
pub mod model_areas;
pub mod model_config_helper;
pub mod profiles;
pub mod prompt_defaults;
pub mod runtime;
pub mod sdk;
pub mod server_tools;
pub mod session_app;
pub mod skills;
pub mod title_gen;

pub use app_context::AgentSessionContext;
pub use runtime::execution::agent_spawn::SessionCache;

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
pub use runtime::config::{AuxiliaryModelConfig, ImageFallbackConfig, ImageFallbackMode};
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
pub use sdk::runner::{profile_runner, ProfileRunner, RunProfileInput};
pub use sdk::spawn::run_child_spawn;

// Re-export the subagent profile system (built-ins + layered loader).
pub use profiles::{builtin_profiles, load_registry, LoaderError};

// Sub-module re-exports for backward compatibility
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
pub mod types {
    pub use crate::skills::types::*;
}
pub mod access_control {
    pub use crate::skills::access_control::*;
}
pub mod selection {
    pub use crate::skills::selection::*;
}
pub mod resource_helpers {
    pub use crate::skills::resource_helpers::*;
}
pub mod runtime_metadata {
    pub use crate::skills::runtime_metadata::*;
}
pub mod session_port {
    pub use crate::skills::session_port::*;
}
pub mod store {
    pub use crate::skills::store::*;
}
pub use skills::types::{SkillDefinition, SkillFilter, SkillStoreConfig};
pub use skills::SkillManager;
pub use skills::SkillStore;
pub use skills::SkillUpdate;

// Re-export from metrics
pub use metrics::aggregator::{aggregate_monthly, aggregate_weekly, PeriodMetrics};
pub use metrics::bus::MetricsBus;
pub use metrics::collector::MetricsCollector;
pub use metrics::events::MetricsEvent;
pub use metrics::storage::{MetricsError, MetricsResult, MetricsStorage, SqliteMetricsStorage};
pub use metrics::types::*;
pub use metrics::worker::MetricsWorker;

// Re-export from MCP
pub use mcp::config::*;
pub use mcp::executor::{CompositeToolExecutor, McpToolExecutor};
pub use mcp::manager::McpServerManager;
pub use mcp::types::*;
