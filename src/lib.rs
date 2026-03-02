//! Bamboo - A fully self-contained AI agent backend framework
//!
//! Bamboo provides a complete backend system for AI agents, including:
//! - Built-in HTTP/HTTPS server (Actix-web)
//! - Agent execution loop with tool support
//! - LLM provider integrations (OpenAI, Anthropic, Google Gemini, GitHub Copilot)
//! - Session management and persistence
//! - Workflow and slash command systems
//! - Process management for external tools
//! - Claude Code integration
//!
//! # Features
//!
//! - **Dual mode**: Binary (standalone server) or library (embedded)
//! - **Unified directory**: All data in ~/.bamboo directory
//! - **Production-ready**: Built-in CORS, rate limiting, security headers
//!
//! # Quick Start
//!
//! ## Binary Mode

// Allow some clippy lints that are pre-existing
#![allow(clippy::module_inception)]
#![allow(clippy::doc_overindented_list_items)]
#![allow(clippy::incompatible_msrv)]
//!
//! ```bash
//! bamboo serve --port 9562 --data-dir ~/.bamboo
//! ```
//!
//! ## Library Mode
//!
//! ```rust,ignore
//! use bamboo_agent::{BambooServer, core::Config};
//!
//! #[tokio::main]
//! async fn main() {
//!     let config = Config::new();
//!     let server = BambooServer::new(config);
//!     // server.start().await.unwrap(); // Not yet implemented
//! }
//! ```

use std::path::PathBuf;

pub mod error;

// Placeholder modules (will be populated during migration)
pub mod agent;
pub mod claude;
pub mod commands;
pub mod core;
pub mod process;
pub mod server;

// Ergonomic module re-exports for the agent subsystem (keeps docs and external
// imports stable without forcing callers through `bamboo_agent::agent::*`).
pub use agent::{llm, skill, tools};

// Re-export core Config as the primary configuration type
pub use core::config::ServerConfig;
pub use core::Config;
pub use error::{BambooError, Result};
pub use process::ProcessRegistry;

/// Main Bamboo server instance
pub struct BambooServer {
    config: core::Config,
    #[allow(dead_code)]
    data_dir: PathBuf,
}

impl BambooServer {
    /// Create a new Bamboo server with configuration
    pub fn new(config: core::Config) -> Self {
        Self {
            config,
            data_dir: core::paths::bamboo_dir(),
        }
    }

    /// Create a new Bamboo server with an explicit data directory.
    pub fn new_with_data_dir(config: core::Config, data_dir: PathBuf) -> Self {
        Self { config, data_dir }
    }

    /// Start the HTTP server (blocking)
    pub async fn start(self) -> Result<()> {
        // TODO: Implement server startup
        todo!("Server startup not yet implemented")
    }

    /// Get the server address
    pub fn server_addr(&self) -> String {
        self.config.server_addr()
    }
}

/// Builder pattern for creating BambooServer
///
/// Provides a fluent API for configuring and instantiating a BambooServer.
///
/// # Example
///
/// ```rust,ignore
/// use bamboo_agent::{BambooBuilder, BambooServer};
/// use std::path::PathBuf;
///
/// let server = BambooBuilder::new()
///     .port(9562)
///     .bind("127.0.0.1")
///     .data_dir(PathBuf::from("~/.bamboo"))
///     .build()
///     .unwrap();
/// ```
pub struct BambooBuilder {
    config: core::Config,
    data_dir: PathBuf,
}

impl BambooBuilder {
    /// Create a new BambooBuilder with default configuration
    pub fn new() -> Self {
        Self {
            config: core::Config::new(),
            data_dir: core::paths::bamboo_dir(),
        }
    }

    /// Set the server port
    ///
    /// # Arguments
    ///
    /// * `port` - Port number to listen on
    pub fn port(mut self, port: u16) -> Self {
        self.config.server.port = port;
        self
    }

    /// Set the bind address
    ///
    /// # Arguments
    ///
    /// * `addr` - IP address to bind to (e.g., "127.0.0.1", "0.0.0.0")
    pub fn bind(mut self, addr: impl Into<String>) -> Self {
        self.config.server.bind = addr.into();
        self
    }

    /// Set the data directory for storing configuration and data
    ///
    /// # Arguments
    ///
    /// * `dir` - Path to the data directory
    pub fn data_dir(mut self, dir: PathBuf) -> Self {
        self.data_dir = dir;
        self
    }

    /// Set the static files directory
    ///
    /// # Arguments
    ///
    /// * `dir` - Path to static files directory
    pub fn static_dir(mut self, dir: PathBuf) -> Self {
        self.config.server.static_dir = Some(dir);
        self
    }

    /// Set the number of workers
    ///
    /// # Arguments
    ///
    /// * `workers` - Number of worker threads
    pub fn workers(mut self, workers: usize) -> Self {
        self.config.server.workers = workers;
        self
    }

    /// Build the BambooServer instance
    ///
    /// # Returns
    ///
    /// A Result containing the configured BambooServer or an error
    pub fn build(self) -> Result<BambooServer> {
        Ok(BambooServer::new_with_data_dir(self.config, self.data_dir))
    }
}

impl Default for BambooBuilder {
    fn default() -> Self {
        Self::new()
    }
}
