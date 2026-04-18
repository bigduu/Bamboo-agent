//! Bamboo session domain — Session, Message, Role, TaskList, and supporting types.

pub mod budget_types;
pub mod message_part;
pub mod task;
pub mod tool_types;
pub mod types;

// Re-exports for ergonomic access
pub use budget_types::{BudgetStrategy, TokenBudget, TokenBudgetUsage, TokenUsageBreakdown};
pub use message_part::{ImageUrlRef, MessagePart};
pub use task::*;
pub use tool_types::{FunctionCall, ToolCall};
pub use types::*;
