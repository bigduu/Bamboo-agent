//! Error types for Bamboo
//!
//! This module defines the main error type used throughout the Bamboo application
//! for handling various failure scenarios.

use thiserror::Error;

/// Main error type for Bamboo operations
///
/// This enum represents all possible errors that can occur when working
/// with the Bamboo system, including configuration, I/O, serialization,
/// HTTP server, process management, and agent-related errors.
#[derive(Debug, Error)]
pub enum BambooError {
    /// Configuration-related errors (invalid settings, missing config files, etc.)
    #[error("Configuration error: {0}")]
    Config(String),

    /// I/O errors from file system operations
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Serialization/deserialization errors (JSON, YAML, etc.)
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// HTTP server startup and runtime errors
    #[error("HTTP server error: {0}")]
    HttpServer(String),

    /// Process management errors (spawning, monitoring, etc.)
    #[error("Process management error: {0}")]
    ProcessManagement(String),

    /// Agent execution errors (LLM communication, tool execution, etc.)
    #[error("Agent error: {0}")]
    Agent(String),

    /// Generic errors from anyhow
    #[error("{0}")]
    Other(#[from] anyhow::Error),
}

/// Convenient result type alias for Bamboo operations
pub type Result<T> = std::result::Result<T, BambooError>;
