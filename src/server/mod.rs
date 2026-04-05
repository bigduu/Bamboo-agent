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
//! - **app_state**: Unified state management with direct provider access
//! - **config**: CORS and security header configuration
//! - **metrics**: Unified metrics infrastructure
//! - **handlers**: Agent API handlers (chat, execute, events, etc.)
//! - **controllers**: Multi-provider API controllers (OpenAI, Anthropic, Gemini)
//! - **services**: Business logic services
//! - **routes**: Route configuration for all API endpoints
//! - **server**: Entry points for running the server
//!
//! # Quick Start
//!
//! ```no_run
//! use std::path::PathBuf;
//! use bamboo_agent::server::run;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), String> {
//!     let app_data_dir = PathBuf::from("/path/to/bamboo-data-dir");
//!     run(app_data_dir, 3456).await
//! }
//! ```
//!
//! # Server Modes
//!
//! The server supports three modes:
//!
//! ## 1. Desktop Mode (Default)
//!
//! Binds to localhost only, No rate limiting. Perfect for local development.
//!
//! ```no_run
//! use std::path::PathBuf;
//! use bamboo_agent::server::run;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), String> {
//!     let data_dir = PathBuf::from("./.bamboo");
//!     run(data_dir, 9562).await
//! }
//! ```
//!
//! ## 2. Docker Mode
//!
//! Custom bind address with rate limiting. For containerized deployments.
//!
//! ```no_run
//! use std::path::PathBuf;
//! use bamboo_agent::server::run_with_bind;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), String> {
//!     let data_dir = PathBuf::from("./.bamboo");
//!     run_with_bind(data_dir, 9562, "0.0.0.0").await
//! }
//! ```
//!
//! ## 3. Production Mode with Frontend
//!
//! Serves static files alongside API. Full production setup.
//!
//! ```no_run
//! use std::path::PathBuf;
//! use bamboo_agent::server::run_with_bind_and_static;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), String> {
//!     let data_dir = PathBuf::from("./.bamboo");
//!     run_with_bind_and_static(
//!         data_dir,
//!         9562,
//!         "0.0.0.0",
//!         Some(PathBuf::from("./dist")),
//!     )
//!     .await
//! }
//! ```
//!
//! # Route Organization
//!
//! ## Agent Routes (/api/v1/*)
//!
//! Core agent functionality: chat, execute, events, metrics, MCP.
//!
//! ## OpenAI Routes (/v1/*)
//!
//! OpenAI-compatible API for tool integration.
//!
//! ## Anthropic Routes (/anthropic/v1/*)
//!
//! Anthropic Claude API compatible endpoints.
//!
//! ## Gemini Routes (/gemini/v1beta/*)
//!
//! Google Gemini API compatible endpoints.

pub mod app_state;
pub mod config;
pub mod config_manager;
pub mod error;
pub mod handlers;
// Backward compatibility: "controllers" and "handlers" are unified.
// Keep the old module name as an alias so existing imports keep working.
pub use handlers as controllers;
pub mod instruction_layer;
pub mod logging;
pub mod message_hooks;
pub mod metrics;
pub mod metrics_service;
pub mod model_config_helper;
pub mod prompt_defaults;
pub mod reloadable_provider;
pub mod request_hooks;
pub mod routes;
pub mod schedules;
pub mod server;
pub mod services;
pub mod spawn_scheduler;
pub mod tools;
pub mod workflow;

// Re-export commonly used types
pub use app_state::{AgentRunner, AgentStatus, AppState};
pub use config::{build_cors, build_security_headers};
pub use error::AppError;
pub use metrics::MetricsInfrastructure;
pub use routes::{
    agent_routes, anthropic_routes, bamboo_v1_routes, configure_routes,
    configure_routes_with_rate_limiting, gemini_routes, openai_prefixed_routes,
};
pub use server::{run, run_with_bind, run_with_bind_and_static, WebService};
