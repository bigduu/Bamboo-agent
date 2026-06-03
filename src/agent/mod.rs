//! Agent system - Complete AI agent framework
//!
//! This module provides:
//! - Core agent abstractions and types (via bamboo-engine)

// Re-export commonly used types from the agent crate
pub use bamboo_agent_core::{
    AgentError, AgentEvent, Message, MessageContent, Role, Session, TokenBudgetUsage, TokenUsage,
};

// Task types from domain-session
pub use bamboo_domain::{TaskItem, TaskItemStatus, TaskList};

// Re-export commonly used types from llm
pub use bamboo_infrastructure::LLMProvider;

// Re-export commonly used types from tools
pub use bamboo_tools::{BuiltinToolExecutor, BuiltinToolExecutorBuilder, ToolOutputManager};

// Re-export commonly used types from metrics
pub use bamboo_engine::{MetricsBus, MetricsWorker};

// Re-export Agent and AgentBuilder from runtime for ergonomic top-level access.
pub use bamboo_engine::{Agent, AgentBuilder};
