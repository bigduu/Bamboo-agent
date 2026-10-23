//! Chat API handler for creating and managing agent conversations.
//!
//! This module provides the HTTP endpoint for initiating chat sessions with the AI agent.

mod handler;
mod types;

#[cfg(test)]
pub(crate) use handler::admit_for_execute;
pub use handler::handler;
pub use types::{ChatImage, ChatRequest, ChatResponse};

#[cfg(test)]
mod tests;
