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
//! - **XDG-compliant**: Follows XDG Base Directory specification
//! - **Production-ready**: Built-in CORS, rate limiting, security headers
//!
//! # Quick Start
//!
//! ## Binary Mode
//!
//! ```bash
//! bamboo serve --port 8080 --data-dir ~/.local/share/bamboo
//! ```
//!
//! ## Library Mode
//!
//! ```rust
//! use bamboo::{BambooServer, BambooConfig};
//!
//! #[tokio::main]
//! async fn main() {
//!     let config = BambooConfig::default();
//!     let server = BambooServer::new(config);
//!     server.start().await.unwrap();
//! }
//! ```

use std::path::PathBuf;

pub mod config;
pub mod error;

// Placeholder modules (will be populated during migration)
pub mod core;
pub mod agent;
pub mod server;
pub mod process;
pub mod claude;
pub mod commands;

pub use config::{BambooConfig, ServerConfig};
pub use error::{BambooError, Result};

/// Main Bamboo server instance
pub struct BambooServer {
    config: BambooConfig,
}

impl BambooServer {
    /// Create a new Bamboo server with configuration
    pub fn new(config: BambooConfig) -> Self {
        Self { config }
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
pub struct BambooBuilder {
    config: BambooConfig,
}

impl BambooBuilder {
    pub fn new() -> Self {
        Self {
            config: BambooConfig::default(),
        }
    }

    pub fn port(mut self, port: u16) -> Self {
        self.config.server.port = port;
        self
    }

    pub fn bind(mut self, addr: &str) -> Self {
        self.config.server.bind = addr.to_string();
        self
    }

    pub fn data_dir(mut self, dir: PathBuf) -> Self {
        self.config.data_dir = dir;
        self
    }

    pub fn build(self) -> Result<BambooServer> {
        Ok(BambooServer::new(self.config))
    }
}

impl Default for BambooBuilder {
    fn default() -> Self {
        Self::new()
    }
}
