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
//! registry.register(ReadFileTool::new());
//!
//! // Look up a tool
//! let tool = registry.get("read_file")?;
//! println!("Tool schema: {:?}", tool.schema());
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

pub mod apply_patch;
pub mod ask_user;
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

pub use apply_patch::ApplyPatchTool;
pub use ask_user::AskUserTool;
pub use create_todo_list::CreateTodoListTool;
pub use execute_command::ExecuteCommandTool;
pub use file_exists::FileExistsTool;
pub use get_current_dir::GetCurrentDirTool;
pub use get_file_info::GetFileInfoTool;
pub use git_diff::GitDiffTool;
pub use git_status::GitStatusTool;
pub use git_write::GitWriteTool;
pub use glob_search::GlobSearchTool;
pub use http_request::HttpRequestTool;
pub use list_directory::ListDirectoryTool;
pub use read_file::ReadFileTool;
pub use read_file_range::ReadFileRangeTool;
pub use registry::ToolRegistry;
pub use search_in_file::SearchInFileTool;
pub use search_in_project::SearchInProjectTool;
pub use set_workspace::SetWorkspaceTool;
pub use sleep::SleepTool;
pub use terminal_session::TerminalSessionTool;
pub use update_todo_item::UpdateTodoItemTool;
pub use write_file::WriteFileTool;
