//! Agent execution API handler.
//!
//! This module provides the HTTP endpoint for triggering AI agent execution
//! on a previously created chat session.

mod handler;
mod image_fallback;
mod runtime;
mod session;
mod types;

pub use handler::handler;
pub use types::{
    ExecuteClientSync, ExecuteRequest, ExecuteResponse, ExecuteSyncInfo, ExecuteSyncReason,
};

#[cfg(test)]
mod tests;
