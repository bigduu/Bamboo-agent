//! Agent execution loop
//!
//! This module implements the main agent execution loop that processes
//! user requests, executes tools, and manages the conversation flow.
//!
//! # Components
//!
//! - **Runner**: Main loop implementation
//! - **Stream**: Streaming response handling
//! - **TaskContext**: Task list integration
//! - **TaskEvaluation**: Progress evaluation logic
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
pub mod task_context;
pub mod task_evaluation;

pub use config::{AgentLoopConfig, ImageFallbackConfig, ImageFallbackMode};
pub use runner::{run_agent_loop, run_agent_loop_with_config};
pub use task_context::TaskLoopContext;
pub use task_evaluation::{evaluate_task_progress, TaskEvaluationResult};

#[cfg(test)]
mod tests;
