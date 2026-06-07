//! Agent-to-Agent (A2A) protocol client for Bamboo.
//!
//! Implements the Google A2A v1.0 specification:
//! - Agent Card discovery and validation
//! - JSON-RPC 2.0 transport
//! - SSE streaming for task events
//! - Send message, get task, cancel task operations

pub mod card;
pub mod client;
pub mod error;
pub mod jsonrpc;
pub mod sse;
pub mod types;

pub use card::{validate_agent_card_for_jsonrpc_mvp, AgentCardValidation};
pub use client::{A2AAuth, A2AClient, A2AClientConfig, A2AJsonRpcClient};
pub use error::{A2AClientError, A2AClientResult};
pub use sse::A2AStream;
pub use types::*;
