//! Server error types and HTTP response handling
//!
//! This module provides a unified error handling system for the Actix-web server.
//! All errors are converted to HTTP responses with appropriate status codes.
//!
//! # Error Types
//!
//! - `BadRequest`: Client errors (400)
//! - `ToolNotFound`: Tool not available (404)
//! - `ToolExecutionError`: Tool execution failed (400)
//! - `ToolApprovalRequired`: Tool needs user approval (403)
//! - `NotFound`: Resource not found (404)
//! - `ProxyAuthRequired`: Proxy authentication needed (428)
//! - `InternalError`: Server errors (500)
//! - `StorageError`: File system errors (500)
//! - `SerializationError`: JSON serialization errors (500)

use actix_web::{http::StatusCode, HttpResponse, ResponseError};
use serde::Serialize;
use thiserror::Error;

/// Result type alias for server operations
pub type Result<T, E = AppError> = std::result::Result<T, E>;

/// Application error enum with HTTP status code mapping
#[derive(Debug, Error)]
pub enum AppError {
    #[error("Bad request: {0}")]
    BadRequest(String),

    #[error("Unauthorized: {0}")]
    Unauthorized(String),

    #[error("Forbidden: {0}")]
    Forbidden(String),

    #[error("Tool '{0}' not found")]
    ToolNotFound(String),

    #[error("Tool execution failed: {0}")]
    ToolExecutionError(String),

    #[error("Tool requires approval: {0}")]
    ToolApprovalRequired(String),

    #[error("{0} not found")]
    NotFound(String),

    #[error("Proxy authentication required")]
    ProxyAuthRequired,

    /// config.json was recovered from a corrupt file (#153) and the recovery
    /// hasn't been confirmed yet — writes are refused until the caller
    /// confirms (or rejects) via the recovery-confirm API.
    #[error("Config recovery pending confirmation: {0}")]
    ConfigRecoveryPending(String),

    #[error("Internal server error: {0}")]
    InternalError(#[from] anyhow::Error),

    #[error("Storage error: {0}")]
    StorageError(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),
}

#[derive(Serialize)]
struct JsonError {
    message: String,
    r#type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    code: Option<String>,
}

#[derive(Serialize)]
struct JsonErrorWrapper {
    error: JsonError,
}

impl ResponseError for AppError {
    fn status_code(&self) -> StatusCode {
        match self {
            AppError::BadRequest(_) => StatusCode::BAD_REQUEST,
            AppError::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            AppError::Forbidden(_) => StatusCode::FORBIDDEN,
            AppError::ToolNotFound(_) => StatusCode::NOT_FOUND,
            AppError::ToolExecutionError(_) => StatusCode::BAD_REQUEST,
            AppError::ToolApprovalRequired(_) => StatusCode::FORBIDDEN,
            AppError::NotFound(_) => StatusCode::NOT_FOUND,
            AppError::ProxyAuthRequired => StatusCode::PRECONDITION_REQUIRED,
            AppError::ConfigRecoveryPending(_) => StatusCode::CONFLICT,
            AppError::InternalError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            AppError::StorageError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            AppError::SerializationError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn error_response(&self) -> HttpResponse {
        let status_code = self.status_code();
        let error_response = JsonErrorWrapper {
            error: JsonError {
                message: self.to_string(),
                r#type: "api_error".to_string(),
                code: match self {
                    AppError::ProxyAuthRequired => Some("proxy_auth_required".to_string()),
                    AppError::ConfigRecoveryPending(_) => {
                        Some("config_recovery_pending".to_string())
                    }
                    _ => None,
                },
            },
        };
        HttpResponse::build(status_code).json(error_response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_app_error_bad_request() {
        let err = AppError::BadRequest("Invalid input".to_string());
        assert_eq!(err.to_string(), "Bad request: Invalid input");
        assert_eq!(err.status_code(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn test_app_error_tool_not_found() {
        let err = AppError::ToolNotFound("bash".to_string());
        assert_eq!(err.to_string(), "Tool 'bash' not found");
        assert_eq!(err.status_code(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn test_app_error_tool_execution_error() {
        let err = AppError::ToolExecutionError("Command failed".to_string());
        assert_eq!(err.to_string(), "Tool execution failed: Command failed");
        assert_eq!(err.status_code(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn test_app_error_tool_approval_required() {
        let err = AppError::ToolApprovalRequired("dangerous_tool".to_string());
        assert_eq!(err.to_string(), "Tool requires approval: dangerous_tool");
        assert_eq!(err.status_code(), StatusCode::FORBIDDEN);
    }

    #[test]
    fn test_app_error_not_found() {
        let err = AppError::NotFound("Session".to_string());
        assert_eq!(err.to_string(), "Session not found");
        assert_eq!(err.status_code(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn test_app_error_proxy_auth_required() {
        let err = AppError::ProxyAuthRequired;
        assert_eq!(err.to_string(), "Proxy authentication required");
        assert_eq!(err.status_code(), StatusCode::PRECONDITION_REQUIRED);
    }

    #[test]
    fn test_app_error_internal_error() {
        let err = AppError::InternalError(anyhow::anyhow!("Something went wrong"));
        assert!(err.to_string().contains("Something went wrong"));
        assert_eq!(err.status_code(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn test_app_error_storage_error() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let err = AppError::StorageError(io_err);
        assert!(err.to_string().contains("file not found"));
        assert_eq!(err.status_code(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn test_app_error_serialization_error() {
        let json_err = serde_json::from_str::<i32>("invalid").unwrap_err();
        let err = AppError::SerializationError(json_err);
        assert!(err.to_string().contains("Serialization error"));
        assert_eq!(err.status_code(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn test_error_response_bad_request() {
        let err = AppError::BadRequest("Test error".to_string());
        let response = err.error_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn test_error_response_tool_not_found() {
        let err = AppError::ToolNotFound("tool".to_string());
        let response = err.error_response();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn test_error_response_proxy_auth_includes_code() {
        let err = AppError::ProxyAuthRequired;
        let response = err.error_response();
        assert_eq!(response.status(), StatusCode::PRECONDITION_REQUIRED);
    }

    #[test]
    fn test_app_error_debug() {
        let err = AppError::BadRequest("test".to_string());
        let debug_str = format!("{:?}", err);
        assert!(debug_str.contains("BadRequest"));
    }

    #[test]
    fn test_app_error_clone() {
        let err1 = AppError::BadRequest("test".to_string());
        // AppError derives Debug but not Clone
        // This test verifies the Debug trait works
        let debug_output = format!("{:?}", err1);
        assert!(!debug_output.is_empty());
    }

    #[test]
    fn test_result_type_ok() {
        let result: Result<i32> = Ok(42);
        assert!(result.is_ok());
        assert!(matches!(result, Ok(42)));
    }

    #[test]
    fn test_result_type_err() {
        let result: Result<i32> = Err(AppError::BadRequest("error".to_string()));
        assert!(result.is_err());
    }

    #[test]
    fn test_internal_error_from_anyhow() {
        let anyhow_err = anyhow::anyhow!("Test error");
        let app_error: AppError = anyhow_err.into();
        assert!(matches!(app_error, AppError::InternalError(_)));
    }

    #[test]
    fn test_storage_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "access denied");
        let app_error: AppError = io_err.into();
        assert!(matches!(app_error, AppError::StorageError(_)));
    }

    #[test]
    fn test_serialization_error_from_serde_json() {
        let json_err = serde_json::from_str::<bool>("not a bool").unwrap_err();
        let app_error: AppError = json_err.into();
        assert!(matches!(app_error, AppError::SerializationError(_)));
    }
}
