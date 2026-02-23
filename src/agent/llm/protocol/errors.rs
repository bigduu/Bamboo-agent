//! Error types for protocol conversion.

use thiserror::Error;

#[derive(Error, Debug)]
pub enum ProtocolError {
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Invalid role: {0}")]
    InvalidRole(String),

    #[error("Invalid content format: {0}")]
    InvalidContent(String),

    #[error("Missing required field: {0}")]
    MissingField(String),

    #[error("Unsupported feature '{feature}' for protocol '{protocol}'")]
    UnsupportedFeature {
        feature: String,
        protocol: String,
    },

    #[error("Invalid tool call: {0}")]
    InvalidToolCall(String),

    #[error("Invalid stream chunk: {0}")]
    InvalidStreamChunk(String),

    #[error("Protocol conversion error: {0}")]
    Conversion(String),
}

pub type ProtocolResult<T> = Result<T, ProtocolError>;
