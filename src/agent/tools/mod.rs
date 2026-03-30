//! Built-in tools for filesystem and command execution.
//!
//! This crate provides a plugin-based tool system using the ToolRegistry pattern.
//! All tools implement the `Tool` trait and can be dynamically registered.
//!
//! # Overview
//!
//! The tools module provides a comprehensive framework for extending agent capabilities
//! through a plugin architecture. It includes:
//!
//! - **tools**: Claude-style built-in tool implementations
//! - **executor**: Tool execution engine with safety controls
//! - **permission**: Permission system for tool actions
//! - **guide**: Tool documentation and example generation
//! - **output_manager**: Manages tool output and artifact references
//!
//! # Key Components
//!
//! ## Tool Registry
//!
//! The [`ToolRegistry`] provides dynamic tool registration and lookup:
//!
//! ```no_run
//! use bamboo_agent::tools::{ToolRegistry, ReadTool, WriteTool};
//!
//! let registry = ToolRegistry::new();
//! registry.register(ReadTool::new()).unwrap();
//! registry.register(WriteTool::new()).unwrap();
//!
//! // Look up and execute tools
//! let _tool = registry.get("Read").expect("tool registered");
//! ```
//!
//! ## Built-in Tool Executor
//!
//! The [`BuiltinToolExecutor`] provides safe execution of built-in tools:
//!
//! - Permission checking via the permission system
//! - Output management and artifact tracking
//! - Working directory management
//!
//! ## Tool Guide System
//!
//! The guide system provides automatic documentation generation:
//!
//! - Tool schemas and examples
//! - Category-based organization
//! - Language-specific usage guides
//!
//! # Available Tools
//!
//! The module includes Claude-style built-in tools such as:
//!
//! - **Shell**: `Bash`, `BashOutput`, `KillShell`
//! - **File Operations**: `Read`, `Write`, `Edit`, `NotebookEdit`
//! - **Search**: `Glob`, `Grep`, `WebFetch`, `WebSearch`
//! - **Workflow**: `Task`, `ExitPlanMode`, `SlashCommand`
//!
//! # Example
//!
//! ```no_run
//! use bamboo_agent::tools::BuiltinToolExecutor;
//!
//! let executor = BuiltinToolExecutor::new();
//! let _schemas = executor.registry().list_tools();
//! ```
//!
//! # Re-exports
//!
//! This module re-exports:
//!
//! - All tool implementations from the `tools` submodule
//! - [`BuiltinToolExecutor`] and [`ToolOutputManager`] for execution
//! - [`ToolRegistry`] for dynamic tool registration
//! - [`ToolGuide`] for documentation generation

pub mod events;
mod executor;
pub mod exposure;
pub mod guide;
pub mod orchestrator;
pub mod output_manager;
pub mod parallel;
pub mod permission;
#[allow(clippy::module_inception)]
pub mod tools;

// Re-export executor types
pub use executor::{
    is_builtin_tool, normalize_tool_ref, resolve_alias, BuiltinToolExecutor,
    BuiltinToolExecutorBuilder, BUILTIN_TOOL_NAMES,
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

// Re-export task types from agent-core for convenience
pub use crate::agent::core::{TaskItem, TaskItemStatus, TaskList};

#[cfg(test)]
mod registry_tests;
