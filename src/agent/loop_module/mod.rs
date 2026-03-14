//! Agent execution loop
//!
//! This module implements the main agent execution loop that processes
//! user requests, executes tools, and manages the conversation flow.
//!
//! # Components
//!
//! - **Runner**: Main loop implementation
//! - **Stream**: Streaming response handling
//! - **TodoContext**: Todo list integration
//! - **TodoEvaluation**: Progress evaluation logic
//!
//! # Example
//!
//! ```rust,ignore
//! use bamboo_agent::agent::loop_module::{run_agent_loop, AgentLoopConfig};
//!
//! let config = AgentLoopConfig::default();
//! let result = run_agent_loop(session, provider, config).await?;
//! ```

pub mod config;
pub mod runner;
pub mod stream;
pub mod todo_context;
pub mod todo_evaluation;

pub use config::{AgentLoopConfig, ImageFallbackConfig, ImageFallbackMode};
pub use runner::{run_agent_loop, run_agent_loop_with_config};
pub use todo_context::TodoLoopContext;
pub use todo_evaluation::{evaluate_todo_progress, TodoEvaluationResult};

#[cfg(test)]
mod tests;
