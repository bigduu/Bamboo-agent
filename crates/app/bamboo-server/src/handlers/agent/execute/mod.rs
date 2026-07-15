//! Agent execution API handler.
//!
//! This module provides the HTTP endpoint for triggering AI agent execution
//! on a previously created chat session.

pub(crate) mod defaults;
mod handler;
pub(crate) mod image_fallback;
pub(crate) mod runtime;
mod types;

pub use defaults::handler as defaults_handler;
pub use handler::handler;
pub(crate) use runtime::{spawn_agent_execution, spawn_event_forwarder};
pub use types::{
    ExecuteClientSync, ExecuteRequest, ExecuteResponse, ExecuteSyncInfo, ExecuteSyncReason,
};

#[cfg(test)]
mod tests;
