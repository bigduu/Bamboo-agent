//! Tool execution system for Bamboo agents.
//!
//! This module provides a comprehensive framework for defining, registering, and executing
//! tools that can be used by AI agents to interact with external systems.
//!
//! # Architecture
//!
//! The tools system is built around several key components:
//!
//! - **accumulator**: Accumulates partial tool calls from streaming responses
//! - **agentic**: Agentic tool execution with multi-step capabilities
//! - **executor**: Core tool execution logic
//! - **registry**: Tool registration and lookup
//! - **result_handler**: Processes tool results and handles agentic support
//! - **smart_code_review**: Specialized tool for intelligent code review
//! - **types**: Core type definitions for tools
//!
//! # Key Concepts
//!
//! ## Tool Registry
//!
//! Tools are registered in a central [`ToolRegistry`] that maps tool names to their
//! implementations. The registry supports:
//!
//! - Dynamic tool registration
//! - Tool name normalization
//! - Global singleton access via [`global_registry`]
//!
//! ## Tool Execution
//!
//! Tools implement the [`ToolExecutor`] trait and can be executed via [`execute_tool_call`].
//! The execution flow:
//!
//! 1. Parse tool arguments from JSON
//! 2. Execute the tool logic
//! 3. Return a [`ToolResult`] with success/failure status
//!
//! ## Agentic Tools
//!
//! Some tools support "agentic" behavior, allowing multi-step execution:
//!
//! - [`AgenticTool`]: Marker trait for agentic tools
//! - [`AgenticContext`]: Context for agentic execution
//! - [`AgenticToolResult`]: Extended result type with sub-actions
//!
//! # Example
//!
//! ```rust,ignore
//! use async_trait::async_trait;
//! use bamboo_agent::agent::core::tools::{
//!     execute_tool_call, FunctionCall, ToolCall, ToolError, ToolExecutor, ToolResult, ToolSchema,
//! };
//!
//! struct NoopExecutor;
//!
//! #[async_trait]
//! impl ToolExecutor for NoopExecutor {
//!     async fn execute(&self, call: &ToolCall) -> Result<ToolResult, ToolError> {
//!         Err(ToolError::NotFound(call.function.name.clone()))
//!     }
//!
//!     fn list_tools(&self) -> Vec<ToolSchema> {
//!         Vec::new()
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     // Execute a tool call (this example uses a no-op executor).
//!     let call = ToolCall {
//!         id: "call-1".to_string(),
//!         tool_type: "function".to_string(),
//!         function: FunctionCall {
//!             name: "read_file".to_string(),
//!             arguments: r#"{\"path\":\"/tmp/test.txt\"}"#.to_string(),
//!         },
//!     };
//!
//!     let _ = execute_tool_call(&call, &NoopExecutor, None).await;
//! }
//! ```
//!
//! # Re-exports
//!
//! Key types and functions re-exported for convenience:
//!
//! - Accumulator: [`ToolCallAccumulator`], [`PartialToolCall`], [`finalize_tool_calls`]
//! - Agentic: [`AgenticTool`], [`AgenticContext`], [`AgenticToolResult`], [`ToolGoal`]
//! - Executor: [`ToolExecutor`], [`execute_tool_call`], [`ToolError`]
//! - Registry: [`ToolRegistry`], [`Tool`], [`global_registry`]
//! - Types: [`ToolCall`], [`ToolResult`], [`ToolSchema`]

pub mod accumulator;
pub mod agentic;
pub mod context;
pub mod executor;
pub mod registry;
pub mod result_handler;
pub mod smart_code_review;
pub mod types;

pub use accumulator::{
    finalize_tool_calls, update_partial_tool_call, PartialToolCall, ToolCallAccumulator,
};
pub use agentic::{
    convert_from_standard_result, convert_to_standard_result, AgenticContext, AgenticTool,
    AgenticToolExecutor, AgenticToolResult, Interaction, InteractionRole, ToolGoal,
};
pub use context::ToolExecutionContext;
pub use executor::{execute_tool_call, execute_tool_call_with_context, ToolError, ToolExecutor};
pub use registry::{
    global_registry, normalize_tool_name, RegistryError, SharedTool, Tool, ToolRegistry,
};
pub use result_handler::{
    execute_sub_actions, handle_tool_result_with_agentic_support, parse_tool_args,
    parse_tool_args_best_effort, send_clarification_request, try_parse_agentic_result,
    ToolHandlingOutcome, MAX_SUB_ACTIONS,
};
pub use smart_code_review::SmartCodeReviewTool;
pub use types::{FunctionCall, FunctionSchema, ToolCall, ToolResult, ToolSchema};

/// Classification of a tool call for approval purposes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolMutability {
    ReadOnly,
    Mutating,
}

/// Read-only tools that don't require user approval.
const READ_ONLY_TOOLS: &[&str] = &[
    "Read",
    "GetFileInfo",
    "Glob",
    "Grep",
    "WebFetch",
    "WebSearch",
    "Workspace",
    "BashOutput",
    "session_note",
    "memory_note",
    "recall",
    "session_inspector",
    "compact_context",
    "Sleep",
];

/// Classify a tool call as read-only or mutating.
pub fn classify_tool(tool_name: &str) -> ToolMutability {
    if READ_ONLY_TOOLS
        .iter()
        .any(|&t| t.eq_ignore_ascii_case(tool_name))
    {
        ToolMutability::ReadOnly
    } else {
        ToolMutability::Mutating
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_compact_context_as_read_only() {
        assert_eq!(classify_tool("compact_context"), ToolMutability::ReadOnly);
    }

    #[test]
    fn classify_compact_context_case_insensitive() {
        assert_eq!(classify_tool("Compact_Context"), ToolMutability::ReadOnly);
        assert_eq!(classify_tool("COMPACT_CONTEXT"), ToolMutability::ReadOnly);
    }

    #[test]
    fn classify_write_as_mutating() {
        assert_eq!(classify_tool("Write"), ToolMutability::Mutating);
    }

    #[test]
    fn classify_unknown_as_mutating() {
        assert_eq!(
            classify_tool("totally_unknown_tool"),
            ToolMutability::Mutating
        );
    }

    #[test]
    fn classify_all_read_only_tools() {
        for name in READ_ONLY_TOOLS {
            assert_eq!(
                classify_tool(name),
                ToolMutability::ReadOnly,
                "{name} should be classified as read-only"
            );
        }
    }
}
