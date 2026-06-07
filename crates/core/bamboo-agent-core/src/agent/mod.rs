//! Agent core types and error handling
//!
//! This module provides the fundamental types for agent operation:
//! - Error types for agent operations
//! - Event types for agent lifecycle
//! - Core types for conversations and sessions
//! - Hook trait for lifecycle extension points

/// Agent error types
pub mod error;
/// Agent event types
pub mod events;
/// Agent hook trait
pub mod hooks;
/// Agent core types (Session, Message, etc.)
pub mod types;

pub use bamboo_domain::{
    ContextBlock, ContextBlockPriority, ContextBlockStability, ContextBlockType,
};
pub use error::AgentError;
pub use events::{AgentEvent, TokenUsage};
pub use hooks::AgentHook;
pub use types::{
    parse_prompt_external_memory_sections, Message, MessageContent, MessagePhase,
    PromptMemoryObservability, PromptSnapshot, PromptSnapshotExternalMemoryParts, Role, Session,
    SessionKind,
};
