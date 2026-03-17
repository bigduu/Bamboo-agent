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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bamboo_error_config() {
        let err = BambooError::Config("Invalid setting".to_string());
        let msg = err.to_string();
        assert!(msg.contains("Configuration error"));
        assert!(msg.contains("Invalid setting"));
    }

    #[test]
    fn test_bamboo_error_http_server() {
        let err = BambooError::HttpServer("Failed to bind".to_string());
        let msg = err.to_string();
        assert!(msg.contains("HTTP server error"));
        assert!(msg.contains("Failed to bind"));
    }

    #[test]
    fn test_bamboo_error_process_management() {
        let err = BambooError::ProcessManagement("Process crashed".to_string());
        let msg = err.to_string();
        assert!(msg.contains("Process management error"));
        assert!(msg.contains("Process crashed"));
    }

    #[test]
    fn test_bamboo_error_agent() {
        let err = BambooError::Agent("Tool execution failed".to_string());
        let msg = err.to_string();
        assert!(msg.contains("Agent error"));
        assert!(msg.contains("Tool execution failed"));
    }

    #[test]
    fn test_bamboo_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "File not found");
        let bamboo_err: BambooError = io_err.into();
        assert!(matches!(bamboo_err, BambooError::Io(_)));
        let msg = bamboo_err.to_string();
        assert!(msg.contains("IO error"));
    }

    #[test]
    fn test_bamboo_error_from_serde_json() {
        let json_err = serde_json::from_str::<i32>("invalid");
        assert!(json_err.is_err());
        let bamboo_err: BambooError = json_err.unwrap_err().into();
        assert!(matches!(bamboo_err, BambooError::Serialization(_)));
        let msg = bamboo_err.to_string();
        assert!(msg.contains("Serialization error"));
    }

    #[test]
    fn test_bamboo_error_from_anyhow() {
        let anyhow_err = anyhow::anyhow!("Something went wrong");
        let bamboo_err: BambooError = anyhow_err.into();
        assert!(matches!(bamboo_err, BambooError::Other(_)));
        let msg = bamboo_err.to_string();
        assert!(msg.contains("Something went wrong"));
    }

    #[test]
    fn test_bamboo_error_debug() {
        let err = BambooError::Config("test".to_string());
        let debug_str = format!("{:?}", err);
        assert!(debug_str.contains("Config"));
    }

    #[test]
    fn test_result_ok() {
        let result: Result<String> = Ok("success".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_result_err() {
        let result: Result<String> = Err(BambooError::Config("error".to_string()));
        assert!(result.is_err());
    }

    #[test]
    fn test_bamboo_error_io_from() {
        let err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "Access denied");
        let bamboo_err = BambooError::from(err);
        assert!(matches!(bamboo_err, BambooError::Io(_)));
    }

    #[test]
    fn test_bamboo_error_chain() {
        let inner = anyhow::anyhow!("Inner error");
        let outer = BambooError::Other(inner);
        assert!(outer.to_string().contains("Inner error"));
    }
}
