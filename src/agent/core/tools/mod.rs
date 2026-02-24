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
//! ```no_run
//! use bamboo_agent::core::tools::{ToolExecutor, ToolCall, ToolResult};
//!
//! // Execute a tool call
//! let call = ToolCall {
//!     id: "call-1".to_string(),
//!     name: "read_file".to_string(),
//!     arguments: r#"{"path": "/tmp/test.txt"}"#.to_string(),
//! };
//!
//! let result = execute_tool_call(&call, &executor)?;
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
    AgenticToolResult, Interaction, InteractionRole, ToolExecutor as AgenticToolExecutor, ToolGoal,
};
pub use executor::{execute_tool_call, ToolError, ToolExecutor};
pub use registry::{global_registry, normalize_tool_name, RegistryError, Tool, ToolRegistry};
pub use result_handler::{
    execute_sub_actions, handle_tool_result_with_agentic_support, parse_tool_args,
    send_clarification_request, try_parse_agentic_result, ToolHandlingOutcome, MAX_SUB_ACTIONS,
};
pub use smart_code_review::SmartCodeReviewTool;
pub use types::{FunctionCall, FunctionSchema, ToolCall, ToolResult, ToolSchema};
