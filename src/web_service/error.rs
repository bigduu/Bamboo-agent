use actix_web::{http::StatusCode, HttpResponse, ResponseError};
use serde::Serialize;
use thiserror::Error;

pub type Result<T, E = AppError> = std::result::Result<T, E>;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("Bad request: {0}")]
    BadRequest(String),

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
            AppError::ToolNotFound(_) => StatusCode::NOT_FOUND,
            AppError::ToolExecutionError(_) => StatusCode::BAD_REQUEST,
            AppError::ToolApprovalRequired(_) => StatusCode::FORBIDDEN,
            AppError::NotFound(_) => StatusCode::NOT_FOUND,
            AppError::ProxyAuthRequired => StatusCode::PRECONDITION_REQUIRED,
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
                    _ => None,
                },
            },
        };
        HttpResponse::build(status_code).json(error_response)
    }
}
