//! Bamboo - A fully self-contained AI agent backend framework
//!
//! Bamboo provides a complete backend system for AI agents, including:
//! - Built-in HTTP/HTTPS server (Actix-web)
//! - Agent execution loop with tool support
//! - LLM provider integrations (OpenAI, Anthropic, Google Gemini, GitHub Copilot)
//! - Session management and persistence
//! - Workflow and slash command systems
//! - Process management for external tools
//!
//! # Features
//!
//! - **Dual mode**: Binary (standalone server) or library (embedded)
//! - **Unified directory**: All data in the Bamboo data directory (default `${HOME}/.bamboo`)
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
//! bamboo serve --port 9562 --data-dir "$HOME/.bamboo"
//! ```
//!
//! ## Library Mode
//!
//! ```rust,ignore
//! use bamboo_agent::{BambooServer, Config};
//!
//! #[tokio::main]
//! async fn main() {
//!     let config = Config::new();
//!     let server = BambooServer::new(config);
//!     server.start().await.unwrap();
//! }
//! ```

use std::path::PathBuf;

#[cfg(test)]
pub(crate) mod test_support {
    use std::sync::{Mutex, MutexGuard, OnceLock};

    pub(crate) fn env_cache_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    pub(crate) fn env_cache_lock_acquire() -> MutexGuard<'static, ()> {
        env_cache_lock()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

pub mod error;

// Placeholder modules (will be populated during migration)
pub mod agent;
pub mod commands;

// Server module is now a separate workspace crate
pub use bamboo_server as server;

// Ergonomic re-export: `bamboo_agent::tools` → `bamboo_tools` for backward compatibility.
pub use bamboo_tools as tools;

// Compatibility re-export matching the published crate API (`bamboo_agent::core::...`).
pub use bamboo_infrastructure as core;

// Re-export infrastructure crate so consumers can access config, paths, encryption, etc.
// via `bamboo_agent::infrastructure::...`
pub use agent::{Agent, AgentBuilder};
pub use bamboo_infrastructure as infrastructure;

// Re-export core Config as the primary configuration type
pub use bamboo_infrastructure::config::ServerConfig;
pub use bamboo_infrastructure::Config;
pub use error::{BambooError, Result};

/// Main Bamboo server instance
pub struct BambooServer {
    config: bamboo_infrastructure::Config,
    #[allow(dead_code)]
    data_dir: PathBuf,
}

impl BambooServer {
    /// Create a new Bamboo server with configuration
    pub fn new(config: bamboo_infrastructure::Config) -> Self {
        Self {
            config,
            data_dir: bamboo_infrastructure::paths::bamboo_dir(),
        }
    }

    /// Create a new Bamboo server with an explicit data directory.
    pub fn new_with_data_dir(config: bamboo_infrastructure::Config, data_dir: PathBuf) -> Self {
        Self { config, data_dir }
    }

    /// Start the HTTP server (blocking).
    ///
    /// Delegates to the appropriate server entrypoint based on the configuration:
    /// - If `static_dir` is set, serves static files alongside the API (Docker mode).
    /// - Otherwise, runs the API server with the configured bind address and port.
    ///
    /// This method blocks until the server shuts down.
    pub async fn start(self) -> Result<()> {
        bamboo_infrastructure::paths::init_bamboo_dir(self.data_dir.clone());

        let result = if self.config.server.static_dir.is_some() {
            server::run_with_bind_and_static(
                self.data_dir,
                self.config.server.port,
                &self.config.server.bind,
                self.config.server.static_dir.clone(),
            )
            .await
        } else if self.config.server.bind == "127.0.0.1" {
            server::run(self.data_dir, self.config.server.port).await
        } else {
            server::run_with_bind(
                self.data_dir,
                self.config.server.port,
                &self.config.server.bind,
            )
            .await
        };

        result.map_err(BambooError::HttpServer)
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
///     .data_dir(PathBuf::from("/path/to/bamboo-data-dir"))
///     .build()
///     .unwrap();
/// ```
pub struct BambooBuilder {
    config: bamboo_infrastructure::Config,
    data_dir: PathBuf,
}

impl BambooBuilder {
    /// Create a new BambooBuilder with default configuration
    pub fn new() -> Self {
        Self {
            config: bamboo_infrastructure::Config::new(),
            data_dir: bamboo_infrastructure::paths::bamboo_dir(),
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
