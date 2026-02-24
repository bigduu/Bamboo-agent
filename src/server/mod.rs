//! Unified HTTP server consolidating web_service and agent/server
//!
//! This module provides a single, consolidated server implementation that:
//! - Eliminates the proxy pattern from web_service
//! - Provides direct provider access without HTTP callbacks
//! - Unifies state management across all endpoints
//! - Supports all API routes (agent, OpenAI, Anthropic, Gemini)
//!
//! # Architecture
//!
//! The server is organized into several key components:
//! - `app_state`: Unified state management with direct provider access
//! - `config`: CORS and security header configuration
//! - `metrics`: Unified metrics infrastructure
//! - `routes`: Route configuration for all API endpoints
//! - `server`: Entry points for running the server
//!
//! # Example
//!
//! ```no_run
//! use std::path::PathBuf;
//! use bamboo_agent::server::run;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), String> {
//!     let app_data_dir = PathBuf::from("~/.bamboo");
//!     run(app_data_dir, 3456).await
//! }
//! ```

pub mod app_state;
pub mod config;
pub mod metrics;

// Re-export commonly used types
pub use app_state::{AgentRunner, AgentStatus, AppState};
pub use config::{build_cors, build_security_headers};
pub use metrics::MetricsInfrastructure;
