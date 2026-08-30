//! MCP (Model Context Protocol) client library for Bamboo Agent
//!
//! This crate provides MCP client functionality allowing the agent to connect
//! to MCP servers and use their tools.

pub mod config;
pub mod error;
pub mod executor;
pub mod manager;
pub mod protocol;
pub mod tool_index;
pub mod transports;
pub mod types;

pub use config::*;
pub use error::{McpError, Result, ToolRegistrationError};
pub use executor::{CompositeToolExecutor, McpToolExecutor};
pub use manager::McpServerManager;
pub use protocol::*;
pub use tool_index::{ToolIndex, MAX_MCP_OWNERSHIP_LEDGER_RELATIONSHIPS};
pub use transports::*;
pub use types::*;
