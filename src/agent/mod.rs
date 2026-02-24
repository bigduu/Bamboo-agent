//! Agent system - Complete AI agent framework
//!
//! This module provides a comprehensive agent system with:
//! - Core agent abstractions and types
//! - LLM provider integrations (OpenAI, Anthropic, Gemini, Copilot)
//! - Tool execution system
//! - Agent execution loop
//! - HTTP API server
//! - Metrics collection
//! - MCP (Model Context Protocol) support
//! - Skill management
//! - CLI interface

pub mod cli;
pub mod core;
pub mod llm;
/// Agent execution loop module
pub mod loop_module;
pub mod mcp;
/// Metrics collection and aggregation
pub mod metrics;
/// Agent HTTP server implementation
pub mod server;
pub mod skill;
pub mod tools;

// Re-export commonly used types from core
pub use core::{
    AgentError, AgentEvent, Message, MessageContent, Role, Session, TodoItem, TodoItemStatus,
    TodoList, TokenBudgetUsage, TokenUsage,
};

// Re-export commonly used types from llm
pub use llm::LLMProvider;

// Re-export commonly used types from loop_module
pub use loop_module::AgentLoopConfig;

// Re-export commonly used types from tools
pub use tools::{BuiltinToolExecutor, BuiltinToolExecutorBuilder, ToolOutputManager};

// Re-export commonly used types from metrics
pub use metrics::{MetricsBus, MetricsWorker};

// Note: server types are available through server module
