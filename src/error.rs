//! Error types for Bamboo

use thiserror::Error;

#[derive(Debug, Error)]
pub enum BambooError {
    #[error("Configuration error: {0}")]
    Config(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("HTTP server error: {0}")]
    HttpServer(String),

    #[error("Process management error: {0}")]
    ProcessManagement(String),

    #[error("Agent error: {0}")]
    Agent(String),

    #[error("{0}")]
    Other(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, BambooError>;
