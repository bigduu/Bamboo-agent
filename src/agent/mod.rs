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
pub mod loop_module;
pub mod mcp;
pub mod metrics;
pub mod server;
pub mod skill;
pub mod tools;

// Re-export commonly used types
pub use core::{
    AgentContext, AgentResult, BudgetPolicy, CompositionEngine, MemoryBackend,
    StorageBackend, ToolRegistry,
};

pub use llm::{
    LLMProvider, LLMProviderBuilder, Message, StreamResponse,
};

pub use loop_module::{
    AgentLoop, AgentLoopConfig, LoopEvent,
};

pub use tools::{
    BuiltinToolExecutor, ToolExecutor, ToolOutputManager,
};

pub use metrics::{
    MetricsBus, MetricsWorker, UsageMetrics,
};

pub use server::{
    AgentServer, ServerState,
};
