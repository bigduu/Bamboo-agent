//! Bamboo session domain — Session, Message, Role, TaskList, and supporting types.

pub mod budget_types;
pub mod composition;
pub mod context_block;
pub mod hook_types;
pub mod message_part;
pub mod persistence;
pub mod prompt_block;
pub mod runtime_metadata;
pub mod runtime_metadata_access;
pub mod runtime_state;
pub mod task;
pub mod tool_types;
pub mod types;

// Re-exports for ergonomic access
pub use budget_types::{BudgetStrategy, TokenBudget, TokenBudgetUsage, TokenUsageBreakdown};
pub use composition::*;
pub use context_block::*;
pub use hook_types::*;
pub use message_part::{ImageUrlRef, MessagePart};
pub use persistence::*;
pub use prompt_block::{CacheControl, PromptBlock};
pub use runtime_metadata::SessionRuntimeMetadata;
pub use runtime_state::*;
pub use task::*;
pub use tool_types::{FunctionCall, ToolCall};
pub use types::*;
