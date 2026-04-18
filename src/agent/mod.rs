//! Agent system - Complete AI agent framework
//!
//! This module provides:
//! - Core agent abstractions and types (via bamboo-application-agent)

// Re-export commonly used types from the agent crate
pub use bamboo_application_agent::{
    AgentError, AgentEvent, Message, MessageContent, Role, Session, TokenBudgetUsage, TokenUsage,
};

// Task types from domain-session
pub use bamboo_domain_session::{TaskItem, TaskItemStatus, TaskList};

// Re-export commonly used types from llm
pub use bamboo_infrastructure_llm::LLMProvider;

// Re-export commonly used types from tools
pub use bamboo_application_tools::{BuiltinToolExecutor, BuiltinToolExecutorBuilder, ToolOutputManager};

// Re-export commonly used types from metrics
pub use bamboo_application_metrics::{MetricsBus, MetricsWorker};

// Re-export Agent and AgentBuilder from runtime for ergonomic top-level access.
pub use bamboo_application_runtime::{Agent, AgentBuilder};
