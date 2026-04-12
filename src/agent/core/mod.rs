//! Core agent functionality for Bamboo.
//!
//! This module provides the essential components for building and running AI agents,
//! including conversation management, tool execution, memory systems, and budget controls.
//!
//! # Architecture
//!
//! The core module is organized into several key subsystems:
//!
//! - **agent**: Agent implementation and event handling
//! - **budget**: Token budget management and limits
//! - **composition**: Agent composition and orchestration
//! - **memory**: External memory and conversation summarization
//! - **storage**: Persistent storage backends (JSONL, etc.)
//! - **task**: Task list management for task tracking
//! - **tools**: Tool execution framework and types
//!
//! # Key Types
//!
//! - Agent: Main agent implementation (see agent submodule)
//! - [`Session`]: Conversation session management
//! - [`Message`]: Message representation with roles and content
//! - [`ToolExecutor`]: Tool execution trait and implementation
//! - [`Storage`]: Storage trait for persistence
//! - [`ExternalMemory`]: Memory management system
//!
//! # Example
//!
//! ```no_run
//! use bamboo_agent::agent::{Session, Message, Role};
//!
//! // Create a new session
//! let session = Session::new("session-id", "gpt-4o-mini");
//!
//! // Add a message
//! let message = Message::user("Hello!");
//! assert_eq!(message.role, Role::User);
//! ```
//!
//! # Re-exports
//!
//! This module re-exports commonly used types for convenience:
//!
//! - Event types: [`AgentEvent`], [`TokenUsage`], [`TokenBudgetUsage`]
//! - Conversation types: [`ConversationSummary`], [`Message`], [`MessageContent`], [`Role`], [`Session`]
//! - Tool types: [`ToolCall`], [`ToolResult`], [`ToolExecutor`], [`ToolError`]
//! - Storage types: [`Storage`], [`JsonlStorage`]
//! - Memory types: [`ExternalMemory`], [`format_summary_as_note`]
//! - Task types: [`TaskList`], [`TaskItem`], [`TaskItemStatus`]

pub mod agent;
pub mod budget;
pub mod composition;
pub mod memory;
pub mod memory_store;
/// Persistent storage backends
pub mod storage;
pub mod todo;
pub mod tools;

pub use agent::events::{AgentEvent, TokenBudgetUsage, TokenUsage};
pub(crate) use agent::parse_prompt_external_memory_sections;
pub use agent::types::{
    ConversationSummary, ImageOcrLine, ImageOcrResult, Message, MessageContent, MessagePhase,
    PromptMemoryObservability, PromptSnapshot, Role, Session, SessionKind,
};
pub use agent::AgentError;
pub use budget::limits::create_budget_for_model;
pub use memory::{format_summary_as_note, ExternalMemory};
pub use storage::{JsonlStorage, Storage};
pub use todo::{TaskItem, TaskItemStatus, TaskList};
pub use tools::{
    execute_tool_call, finalize_tool_calls, handle_tool_result_with_agentic_support,
    parse_tool_args, try_parse_agentic_result, AgenticContext, AgenticTool, AgenticToolResult,
    SmartCodeReviewTool, ToolCall, ToolCallAccumulator, ToolError, ToolExecutor, ToolGoal,
    ToolHandlingOutcome, ToolResult, ToolSchema,
};

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;
