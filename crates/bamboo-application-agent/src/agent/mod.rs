//! Agent core types and error handling
//!
//! This module provides the fundamental types for agent operation:
//! - Error types for agent operations
//! - Event types for agent lifecycle
//! - Core types for conversations and sessions

/// Agent error types
pub mod error;
/// Agent event types
pub mod events;
/// Agent core types (Session, Message, etc.)
pub mod types;

pub use error::AgentError;
pub use events::{AgentEvent, TokenUsage};
pub use types::{
    Message, MessageContent, MessagePhase, PromptMemoryObservability, PromptSnapshot, Role,
    Session, SessionKind, parse_prompt_external_memory_sections, PromptSnapshotExternalMemoryParts,
};
