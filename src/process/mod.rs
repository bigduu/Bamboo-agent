//! Process management for external agent runs and Claude sessions
//!
//! This module provides comprehensive process lifecycle management:
//!
//! # Features
//! - Registration and tracking of running processes
//! - Graceful and forceful termination strategies
//! - Live output capture for agent runs
//! - Cross-platform process killing (Windows + Unix)
//!
//! # Process Types
//! - **AgentRun**: External agent execution processes
//! - **ClaudeSession**: Claude Code session processes
//!
//! # Example
//!
//! ```rust,ignore
//! use bamboo_agent::process::ProcessRegistry;
//! use std::sync::{Arc, Mutex};
//!
//! #[tokio::main]
//! async fn main() {
//!     let registry = Arc::new(ProcessRegistry::new());
//!
//!     // Register a Claude session
//!     let run_id = registry.register_claude_session(
//!         "session-123".to_string(),
//!         12345,
//!         "/path/to/project".to_string(),
//!         "Implement feature X".to_string(),
//!         "claude-sonnet-4.6".to_string(),
//!         Arc::new(Mutex::new(None)),
//!     ).await.unwrap();
//!
//!     // Append live output
//!     registry.append_live_output(run_id, "Processing...").await.unwrap();
//!
//!     // Get output
//!     let output = registry.get_live_output(run_id).await.unwrap();
//!
//!     // Kill process when done
//!     registry.kill_process(run_id).await.unwrap();
//! }
//! ```

pub mod registry;

pub use registry::{
    ProcessHandle, ProcessInfo, ProcessRegistrationConfig, ProcessRegistry, ProcessType,
};
