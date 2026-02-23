//! Core types and utilities (migrated from chat_core)
//!
//! This module provides the foundational types used across all chat-related functionality:
//! - `config` - Global backend configuration
//! - `encryption` - Encryption utilities
//! - `keyword_masking` - Keyword masking types
//! - `paths` - Path utilities (XDG-compliant)
//! - `todo` - TodoItem, TodoList for task tracking

pub mod config;
pub mod encryption;
pub mod keyword_masking;
pub mod paths;
pub mod todo;

// Re-export commonly used types
pub use config::{Config, ProxyAuth, ProviderConfigs, OpenAIConfig, AnthropicConfig, GeminiConfig, CopilotConfig};
pub use encryption::{decrypt, encrypt};
pub use keyword_masking::{KeywordEntry, KeywordMaskingConfig, MatchType};
pub use paths::*;
pub use todo::{TodoExecution, TodoItem, TodoItemType, TodoList, TodoListStatus, TodoStatus};
