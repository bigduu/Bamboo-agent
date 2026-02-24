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
//! - `handlers`: Agent API handlers (chat, execute, events, etc.)
//! - `controllers`: Multi-provider API controllers (OpenAI, Anthropic, Gemini)
//! - `services`: Business logic services
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
pub mod controllers;
pub mod error;
pub mod handlers;
pub mod logging;
pub mod metrics;
pub mod metrics_service;
pub mod model_config_helper;
pub mod routes;
pub mod server;
pub mod services;
pub mod workflow;

// Re-export commonly used types
pub use app_state::{AgentRunner, AgentStatus, AppState};
pub use config::{build_cors, build_security_headers};
pub use error::AppError;
pub use metrics::MetricsInfrastructure;
pub use routes::{
    configure_routes, configure_routes_with_rate_limiting, agent_routes,
    openai_compatible_routes, anthropic_routes, gemini_routes,
};
pub use server::{run, run_with_bind, run_with_bind_and_static, WebService};
