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
    UnsupportedFeature { feature: String, protocol: String },

    #[error("Invalid tool call: {0}")]
    InvalidToolCall(String),

    #[error("Invalid stream chunk: {0}")]
    InvalidStreamChunk(String),

    #[error("Protocol conversion error: {0}")]
    Conversion(String),
}

pub type ProtocolResult<T> = Result<T, ProtocolError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_protocol_error_serialization() {
        let err = ProtocolError::InvalidRole("test".to_string());
        let msg = err.to_string();
        assert!(msg.contains("Invalid role"));
        assert!(msg.contains("test"));
    }

    #[test]
    fn test_protocol_error_invalid_content() {
        let err = ProtocolError::InvalidContent("bad format".to_string());
        let msg = err.to_string();
        assert!(msg.contains("Invalid content format"));
    }

    #[test]
    fn test_protocol_error_missing_field() {
        let err = ProtocolError::MissingField("id".to_string());
        let msg = err.to_string();
        assert!(msg.contains("Missing required field"));
        assert!(msg.contains("id"));
    }

    #[test]
    fn test_protocol_error_unsupported_feature() {
        let err = ProtocolError::UnsupportedFeature {
            feature: "vision".to_string(),
            protocol: "gemini".to_string(),
        };
        let msg = err.to_string();
        assert!(msg.contains("Unsupported feature"));
        assert!(msg.contains("vision"));
        assert!(msg.contains("gemini"));
    }

    #[test]
    fn test_protocol_error_invalid_tool_call() {
        let err = ProtocolError::InvalidToolCall("missing name".to_string());
        let msg = err.to_string();
        assert!(msg.contains("Invalid tool call"));
    }

    #[test]
    fn test_protocol_error_invalid_stream_chunk() {
        let err = ProtocolError::InvalidStreamChunk("empty content".to_string());
        let msg = err.to_string();
        assert!(msg.contains("Invalid stream chunk"));
    }

    #[test]
    fn test_protocol_error_conversion() {
        let err = ProtocolError::Conversion("failed to convert".to_string());
        let msg = err.to_string();
        assert!(msg.contains("Protocol conversion error"));
    }

    #[test]
    fn test_protocol_error_debug() {
        let err = ProtocolError::InvalidRole("test".to_string());
        let debug_str = format!("{:?}", err);
        assert!(debug_str.contains("InvalidRole"));
    }

    #[test]
    fn test_protocol_result_ok() {
        let result: ProtocolResult<String> = Ok("success".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_protocol_result_err() {
        let result: ProtocolResult<String> = Err(ProtocolError::MissingField("test".to_string()));
        assert!(result.is_err());
    }

    #[test]
    fn test_protocol_error_from_serde_json() {
        let json_err = serde_json::from_str::<i32>("invalid");
        assert!(json_err.is_err());
        let protocol_err: ProtocolError = json_err.unwrap_err().into();
        assert!(matches!(protocol_err, ProtocolError::Serialization(_)));
    }
}
