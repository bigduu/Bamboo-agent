//! Chat API handler for creating and managing agent conversations.
//!
//! This module provides the HTTP endpoint for initiating chat sessions with the AI agent.

mod handler;
mod prompt;
mod types;

pub use handler::handler;
pub use types::{ChatImage, ChatRequest, ChatResponse};

#[cfg(test)]
mod tests;
