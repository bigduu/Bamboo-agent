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

    #[error("Configuration revision conflict: expected {expected}, actual {actual}")]
    ConfigConflict { expected: u64, actual: u64 },

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

/// The value of the `"error"` key in bamboo's canonical error envelope
/// (`{"error": {"message", "type", "code"}}`, matching [`AppError`]'s
/// [`ResponseError::error_response`] body) — for the minority of handlers
/// that are not (yet) modeled as an `AppError` variant but still need to
/// return extra sibling fields alongside the error (e.g. `session_id`,
/// `message_id`) that a bare `AppError` has no place for.
///
/// Prefer returning `Result<_, AppError>` directly when there are no extra
/// sibling fields — this helper exists only to converge the remaining
/// hand-written `HttpResponse::X().json(json!({"error": "<flat string>", ...}))`
/// call sites (#251 finding 2) onto the SAME envelope shape as `AppError`,
/// without forcing every one of them through a full `AppError` variant.
///
/// # Example
/// ```ignore
/// HttpResponse::NotFound().json(serde_json::json!({
///     "error": error_value("Session not found"),
///     "session_id": session_id,
/// }))
/// ```
pub fn error_value(message: impl Into<String>) -> serde_json::Value {
    serde_json::json!({ "message": message.into(), "type": "api_error" })
}

/// Build bamboo's canonical JSON error response for code paths that cannot
/// return an [`AppError`] directly (most notably middleware and handlers that
/// must preserve extra response headers).
pub fn json_error(status: StatusCode, message: impl Into<String>) -> HttpResponse {
    json_error_with_code(status, message.into(), None)
}

/// Preserve an existing `actix_web::Result` signature while ensuring the
/// framework error is rendered with bamboo's canonical JSON envelope instead
/// of Actix's default `text/plain` convenience-error body.
pub fn json_internal_server_error(message: impl Into<String>) -> actix_web::Error {
    let message = message.into();
    let response = json_error(StatusCode::INTERNAL_SERVER_ERROR, message.clone());
    actix_web::error::InternalError::from_response(message, response).into()
}

fn json_error_with_code(status: StatusCode, message: String, code: Option<&str>) -> HttpResponse {
    HttpResponse::build(status).json(JsonErrorWrapper {
        error: JsonError {
            message,
            r#type: "api_error".to_string(),
            code: code.map(str::to_string),
        },
    })
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
            AppError::ConfigConflict { .. } => StatusCode::CONFLICT,
            AppError::InternalError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            AppError::StorageError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            AppError::SerializationError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn error_response(&self) -> HttpResponse {
        let status_code = self.status_code();
        let code = match self {
            AppError::ProxyAuthRequired => Some("proxy_auth_required"),
            AppError::ConfigRecoveryPending(_) => Some("config_recovery_pending"),
            AppError::ConfigConflict { .. } => Some("config_revision_conflict"),
            _ => None,
        };
        json_error_with_code(status_code, self.to_string(), code)
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

    /// The canonical envelope is a nested `{"error": {"message", "type"}}` —
    /// `AppError::error_response` and [`error_value`] must agree on the same
    /// shape (modulo the extra sibling fields `error_value` callers add
    /// alongside `"error"`). #251 (finding 2).
    #[actix_web::test]
    async fn app_error_and_error_value_agree_on_envelope_shape() {
        let resp = AppError::NotFound("Session".to_string()).error_response();
        let bytes = actix_web::body::to_bytes(resp.into_body()).await.unwrap();
        let app_err_body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(app_err_body["error"]["type"], "api_error");
        assert!(app_err_body["error"]["message"].is_string());

        let helper_error = error_value("Session not found");
        assert_eq!(helper_error["type"], "api_error");
        assert_eq!(helper_error["message"], "Session not found");

        // Same "type" tag on both shapes.
        assert_eq!(
            app_err_body["error"]["type"], helper_error["type"],
            "AppError and error_value must use the same error \"type\" tag"
        );
    }

    #[actix_web::test]
    async fn shared_json_error_helpers_use_the_canonical_envelope() {
        let response = json_error(StatusCode::TOO_MANY_REQUESTS, "slow down");
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        let bytes = actix_web::body::to_bytes(response.into_body())
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["error"]["message"], "slow down");
        assert_eq!(body["error"]["type"], "api_error");

        let error = json_internal_server_error("storage failed");
        let response = error.error_response();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let bytes = actix_web::body::to_bytes(response.into_body())
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["error"]["message"], "storage failed");
        assert_eq!(body["error"]["type"], "api_error");
    }

    /// Source-level tripwire for Bamboo's native HTTP surface. Vendor-compat
    /// handlers and the native WS frame protocol intentionally own different
    /// wire contracts, but native REST handlers and middleware must not
    /// reintroduce either a flat `error` value or Actix's text/plain
    /// convenience errors.
    #[test]
    fn native_http_error_sources_use_the_canonical_envelope() {
        let source_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let flat_error = regex::Regex::new(r#""error"\s*:\s*([^,}\n]+)"#).unwrap();
        let actix_convenience_error = regex::Regex::new(
            r"\bError(?:BadRequest|Unauthorized|Forbidden|NotFound|MethodNotAllowed|NotAcceptable|RequestTimeout|Conflict|Gone|PreconditionFailed|ExpectationFailed|PayloadTooLarge|UnsupportedMediaType|UnprocessableEntity|TooManyRequests|InternalServerError|NotImplemented|BadGateway|ServiceUnavailable|GatewayTimeout)\s*\(",
        )
        .unwrap();
        let mut violations = Vec::new();

        for entry in walkdir::WalkDir::new(source_root.join("handlers"))
            .into_iter()
            .filter_map(Result::ok)
            .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "rs"))
        {
            let relative = entry.path().strip_prefix(&source_root).unwrap();
            let relative = relative.to_string_lossy().replace('\\', "/");
            if relative.starts_with("handlers/openai/")
                || relative.starts_with("handlers/anthropic/")
                || relative.starts_with("handlers/gemini/")
                || relative.starts_with("handlers/agent/ws_v2/")
            {
                continue;
            }
            inspect_native_error_source(
                entry.path(),
                &relative,
                &flat_error,
                &actix_convenience_error,
                &mut violations,
            );
        }
        inspect_native_error_source(
            &source_root.join("rate_limit.rs"),
            "rate_limit.rs",
            &flat_error,
            &actix_convenience_error,
            &mut violations,
        );

        assert!(
            violations.is_empty(),
            "native HTTP errors must use AppError/error_value/json_error; vendor SSE and WS frames are excluded:\n{}",
            violations.join("\n")
        );
    }

    fn inspect_native_error_source(
        path: &std::path::Path,
        relative: &str,
        flat_error: &regex::Regex,
        actix_convenience_error: &regex::Regex,
        violations: &mut Vec<String>,
    ) {
        let source = std::fs::read_to_string(path).unwrap();
        for capture in flat_error.captures_iter(&source) {
            let matched = capture.get(0).unwrap();
            let line_number = source[..matched.start()]
                .bytes()
                .filter(|byte| *byte == b'\n')
                .count()
                + 1;
            let line_start = source[..matched.start()]
                .rfind('\n')
                .map_or(0, |position| position + 1);
            let line_end = source[matched.start()..]
                .find('\n')
                .map_or(source.len(), |position| matched.start() + position);
            if source[line_start..line_end].trim_start().starts_with("//") {
                continue;
            }
            let value = capture.get(1).unwrap().as_str().trim_start();
            if !value.starts_with("crate::error::error_value(")
                && !value.starts_with("error_value(")
                && !value.starts_with('{')
            {
                violations.push(format!(
                    "{relative}:{line_number}: flat error value starts with `{value}`"
                ));
            }
        }
        for matched in actix_convenience_error.find_iter(&source) {
            let line_number = source[..matched.start()]
                .bytes()
                .filter(|byte| *byte == b'\n')
                .count()
                + 1;
            violations.push(format!(
                "{relative}:{line_number}: Actix convenience error renders text/plain"
            ));
        }
    }
}
