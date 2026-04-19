//! Core type definitions for the tool system.
//!
//! ToolCall and FunctionCall are re-exported from bamboo-domain-session.
//! ToolResult, ToolSchema, and FunctionSchema are re-exported from bamboo-domain-tool.

// Re-exported from domain crate
pub use bamboo_domain::session::tool_types::{FunctionCall, ToolCall};
pub use bamboo_domain::tool_types::*;
