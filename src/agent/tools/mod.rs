//! Built-in tools for filesystem and command execution.
//!
//! This crate provides a plugin-based tool system using the ToolRegistry pattern.
//! All tools implement the `Tool` trait and can be dynamically registered.

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
