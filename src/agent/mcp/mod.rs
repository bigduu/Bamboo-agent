//! MCP (Model Context Protocol) client library for Bamboo Agent
//!
//! This crate provides MCP client functionality allowing the agent to connect
//! to MCP servers and use their tools.

pub mod config;
pub mod error;
pub mod protocol;
pub mod transports;
pub mod types;

pub mod executor;
pub mod manager;
pub mod tool_index;

pub use config::*;
pub use error::{McpError, Result};
pub use executor::{CompositeToolExecutor, McpToolExecutor};
pub use manager::McpServerManager;
pub use protocol::*;
pub use tool_index::ToolIndex;
pub use transports::*;
pub use types::*;
