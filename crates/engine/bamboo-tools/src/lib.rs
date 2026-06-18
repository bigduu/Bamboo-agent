//! Built-in tools for filesystem and command execution.
//!
//! This crate provides a plugin-based tool system using the ToolRegistry pattern.
//! All tools implement the `Tool` trait and can be dynamically registered.

pub mod approval;
pub mod events;
pub mod nested_spawn;
pub mod executor;
pub mod exposure;
pub mod guide;
pub mod orchestrator;
pub mod output_manager;
pub mod parallel;
pub use bamboo_permission as permission;
pub mod slash_commands;
#[allow(clippy::module_inception)]
pub mod tools;

// Re-export executor types
pub use executor::{BuiltinToolExecutor, BuiltinToolExecutorBuilder};

// Re-export cross-process approval proxy (Phase 2: child → parent delegation)
pub use approval::{current_approval_proxy, with_approval_proxy, ApprovalAsk, ApprovalProxy};

// Re-export nested-spawn proxy (Phase 6: nested execution)
pub use nested_spawn::{
    current_nested_spawn_proxy, with_nested_spawn_proxy, NestedSpawnProxy,
};

// Re-export tool name utilities
pub use bamboo_domain::tool_names::{
    is_builtin_tool, normalize_tool_ref, resolve_alias, BUILTIN_TOOL_NAMES,
};

// Re-export guide system types
pub use guide::{
    context::{GuideBuildContext, GuideLanguage},
    EnhancedPromptBuilder, ToolCategory, ToolExample, ToolGuide, ToolGuideSpec,
};

// Re-export orchestration types
pub use events::{ToolEmitter, ToolEvent, ToolEventPhase};
pub use orchestrator::{
    classify_tool, OrchestratorConfig, OrchestratorResult, ToolMutability, ToolOrchestrator,
};
pub use parallel::{ToolCallResult, ToolCallRuntime};

// Re-export output manager types
pub use output_manager::{ArtifactRef, ToolOutputManager};

// Re-export all tool implementations
pub use tools::{
    BashOutputTool, BashTool, ConclusionWithOptionsTool, EditTool, ExitPlanModeTool, GlobTool,
    GrepTool, KillShellTool, NotebookEditTool, ReadTool, SlashCommandTool, TaskTool, ToolRegistry,
    WebFetchTool, WebSearchTool, WriteTool,
};

// Re-export task types for convenience
pub use bamboo_domain::{TaskItem, TaskItemStatus, TaskList};

#[cfg(test)]
mod registry_tests;
