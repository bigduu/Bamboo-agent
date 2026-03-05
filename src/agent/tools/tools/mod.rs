//! Built-in tool implementations for Bamboo agents.
//!
//! This module contains all built-in tools that provide agents with capabilities
//! for filesystem operations, code execution, git operations, and user interaction.
//!
//! # Tool Categories
//!
//! ## File Operations
//! - [`ReadFileTool`], [`ReadFileRangeTool`]: Read file contents
//! - [`WriteFileTool`]: Write content to files
//! - [`ApplyPatchTool`]: Apply unified diff patches
//! - [`ListDirectoryTool`]: List directory contents
//! - [`GetFileInfoTool`], [`FileExistsTool`]: File metadata and existence checks
//! - [`SearchInFileTool`], [`SearchInProjectTool`]: Search for patterns
//!
//! ## Git Operations
//! - [`GitStatusTool`]: Check repository status
//! - [`GitDiffTool`]: Show repository differences
//! - [`GitWriteTool`]: Perform git operations (commit, push, etc.)
//!
//! ## Command Execution
//! - [`ExecuteCommandTool`]: Run shell commands
//! - [`TerminalSessionTool`]: Interactive terminal sessions
//!
//! ## Utility Tools
//! - [`SetWorkspaceTool`]: Set working directory
//! - [`GetCurrentDirTool`]: Get current directory
//! - [`GlobSearchTool`]: Glob pattern file search
//! - [`HttpRequestTool`]: Make HTTP requests
//! - [`SleepTool`]: Introduce delays
//!
//! ## User Interaction
//! - [`AskUserTool`]: Request user input
//!
//! ## Task Management
//! - [`CreateTodoListTool`], [`UpdateTodoItemTool`]: Todo list management
//!
//! # Registry
//!
//! All tools are automatically registered with the [`ToolRegistry`], which provides:
//!
//! - Tool discovery and lookup
//! - Schema generation for each tool
//! - JSON Schema validation
//!
//! # Example
//!
//! ```no_run
//! use bamboo_agent::tools::{ToolRegistry, ReadFileTool};
//!
//! let registry = ToolRegistry::new();
//! registry.register(ReadFileTool::new()).unwrap();
//!
//! // Look up a tool
//! let tool = registry.get("read_file").expect("tool registered");
//! println!("Tool schema: {:?}", tool.to_schema());
//! ```
//!
//! # Adding New Tools
//!
//! To add a new tool:
//!
//! 1. Create a new module file (e.g., `my_tool.rs`)
//! 2. Implement the `Tool` trait for your struct
//! 3. Add a `new()` constructor function
//! 4. Register in the module's `mod.rs`
//! 5. Re-export from the parent module

// File operation tools
pub mod apply_patch;
pub mod ask_user;
pub mod claude_code;
pub mod create_todo_list;
pub mod execute_command;
pub mod file_exists;
pub mod get_current_dir;
pub mod get_file_info;
pub mod git_diff;
pub mod git_status;
pub mod git_write;
pub mod glob_search;
pub mod http_request;
pub mod list_directory;
pub mod memory_note;
pub mod read_file;
pub mod read_file_range;
pub mod registry;
pub mod search_in_file;
pub mod search_in_project;
pub mod set_workspace;
pub mod sleep;
pub mod terminal_session;
pub mod update_todo_item;
pub mod write_file;

// Re-export file operation tools
/// Tool for applying unified diff patches to files.
pub use apply_patch::ApplyPatchTool;
/// Tool for asking users questions during execution.
pub use ask_user::AskUserTool;
/// Tool for running Claude Code CLI.
pub use claude_code::ClaudeCodeTool;
/// Tool for creating todo lists to track task progress.
pub use create_todo_list::CreateTodoListTool;
/// Tool for executing shell commands with sandboxing.
pub use execute_command::ExecuteCommandTool;
/// Tool for checking if a file exists at a given path.
pub use file_exists::FileExistsTool;
/// Tool for getting the current working directory.
pub use get_current_dir::GetCurrentDirTool;
/// Tool for getting file metadata (size, permissions, timestamps).
pub use get_file_info::GetFileInfoTool;

// Re-export git operation tools
/// Tool for showing git diff between commits, staged changes, or working directory.
pub use git_diff::GitDiffTool;
/// Tool for checking git repository status (modified files, branch info).
pub use git_status::GitStatusTool;
/// Tool for performing git write operations (commit, push, branch, etc.).
pub use git_write::GitWriteTool;

// Re-export utility tools
/// Tool for glob pattern matching file search.
pub use glob_search::GlobSearchTool;
/// Tool for making HTTP requests to external APIs.
pub use http_request::HttpRequestTool;
/// Tool for listing directory contents with optional filtering.
pub use list_directory::ListDirectoryTool;
/// Tool for reading/updating the persistent per-session memory note.
pub use memory_note::MemoryNoteTool;
/// Tool for reading entire file contents.
pub use read_file::ReadFileTool;
/// Tool for reading specific line ranges from a file.
pub use read_file_range::ReadFileRangeTool;

// Re-export core registry
/// Registry for managing and looking up available tools.
pub use registry::ToolRegistry;

// Re-export search tools
/// Tool for searching patterns within a single file using regex.
pub use search_in_file::SearchInFileTool;
/// Tool for searching patterns across all files in a project.
pub use search_in_project::SearchInProjectTool;

// Re-export workspace and utility tools
/// Tool for setting the current workspace directory.
pub use set_workspace::SetWorkspaceTool;
/// Tool for introducing delays in execution.
pub use sleep::SleepTool;
/// Tool for creating and managing interactive terminal sessions.
pub use terminal_session::TerminalSessionTool;
/// Tool for updating todo list items.
pub use update_todo_item::UpdateTodoItemTool;
/// Tool for writing content to files.
pub use write_file::WriteFileTool;
