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
//! - **tools**: Built-in tool implementations (file ops, git, commands, etc.)
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
//! use bamboo_agent::tools::{ToolRegistry, ReadFileTool, WriteFileTool};
//!
//! let registry = ToolRegistry::new();
//! registry.register(ReadFileTool::new()).unwrap();
//! registry.register(WriteFileTool::new()).unwrap();
//!
//! // Look up and execute tools
//! let _tool = registry.get("read_file").expect("tool registered");
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
//! The module includes 20+ built-in tools organized by category:
//!
//! - **File Operations**: read, write, patch, list, search
//! - **Git Operations**: status, diff, commit, push
//! - **Command Execution**: shell commands, terminal sessions
//! - **User Interaction**: ask questions, get input
//! - **Task Management**: todo lists, task tracking
//! - **Utilities**: HTTP requests, glob search, sleep
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

mod executor;
pub mod guide;
pub mod output_manager;
pub mod permission;
#[allow(clippy::module_inception)]
pub mod tools;

// Re-export executor types
pub use executor::{
    is_builtin_tool, normalize_tool_ref, BuiltinToolExecutor, BuiltinToolExecutorBuilder,
    BUILTIN_TOOL_NAMES,
};

// Re-export guide system types
pub use guide::{
    context::{GuideBuildContext, GuideLanguage},
    EnhancedPromptBuilder, ToolCategory, ToolExample, ToolGuide, ToolGuideSpec,
};

// Re-export output manager types
pub use output_manager::{ArtifactRef, ToolOutputManager};

// Re-export all tool implementations
pub use tools::{
    ApplyPatchTool, AskUserTool, CreateTodoListTool, ExecuteCommandTool, FileExistsTool,
    GetCurrentDirTool, GetFileInfoTool, GitDiffTool, GitStatusTool, GitWriteTool, GlobSearchTool,
    HttpRequestTool, ListDirectoryTool, ReadFileRangeTool, ReadFileTool, SearchInFileTool,
    SearchInProjectTool, SetWorkspaceTool, SleepTool, TerminalSessionTool, ToolRegistry,
    UpdateTodoItemTool, WriteFileTool,
};

// Re-export todo types from agent-core for convenience
pub use crate::agent::core::{TodoItem, TodoItemStatus, TodoList};

#[cfg(test)]
mod registry_tests;
