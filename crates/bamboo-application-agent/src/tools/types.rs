//! Core type definitions for the tool system.
//!
//! ToolCall and FunctionCall are re-exported from bamboo-domain-session.
//! ToolResult, ToolSchema, and FunctionSchema are re-exported from bamboo-domain-tool.

// Re-exported from domain crates
pub use bamboo_domain_session::tool_types::{FunctionCall, ToolCall};
pub use bamboo_application_tool_types::tool_types::*;
