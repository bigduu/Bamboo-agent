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
//! use bamboo_server::run;
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
//! use bamboo_server::run;
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
//! use bamboo_server::run_with_bind;
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
//! use bamboo_server::run_with_bind_and_static;
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
pub(crate) mod codex_run_tokens;
pub mod config;
pub mod config_manager;
pub mod connect;
pub mod error;
pub mod events;
pub mod handlers;
mod lifecycle_hooks;
pub mod logging;
pub mod notify_sinks;
mod permission_audit;
pub mod plugin_installer;
pub mod plugin_source;
pub mod project_context;
pub mod rate_limit;
pub mod reloadable_provider;
pub mod routes;
pub mod schedule_app;
pub mod server;
pub mod service_manager;
pub mod services;
pub mod session_app;
pub mod tool_event_policy;
pub mod tool_event_router;

// Re-export shims: these modules were relocated to bamboo-engine; keep the
// historical bamboo_server::… paths working for external/integration-test refs.
pub use bamboo_engine::{message_hooks, model_areas, model_config_helper, prompt_defaults};
pub use bamboo_metrics::metrics_service;
// `server_tools` was extracted to the `bamboo-server-tools` crate; keep the
// historical `bamboo_server::server_tools::…` path working as a re-export shim.
pub use bamboo_server_tools as server_tools;
pub mod title_gen;
pub mod tools;
pub mod workflow;

// Re-export commonly used types
pub use app_state::{AgentRunner, AgentStatus, AppState};
pub use config::{build_cors, build_security_headers};
pub use error::AppError;
pub use routes::{
    agent_routes, anthropic_routes, bamboo_v1_routes, configure_routes,
    configure_routes_with_rate_limiting, gemini_routes, openai_prefixed_routes,
};
pub use server::{
    run, run_with_bind, run_with_bind_and_static, run_with_bind_and_static_tls, run_with_bind_tls,
    run_with_tls, WebService,
};
