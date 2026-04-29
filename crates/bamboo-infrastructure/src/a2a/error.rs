#[derive(Debug, thiserror::Error)]
pub enum A2AClientError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("SSE stream error: {0}")]
    Sse(String),
    #[error("A2A JSON-RPC error {code}: {message}")]
    JsonRpc {
        code: i64,
        message: String,
        data: Option<serde_json::Value>,
    },
    #[error("A2A task not found: {0}")]
    TaskNotFound(String),
    #[error("A2A task is not cancelable: {0}")]
    TaskNotCancelable(String),
    #[error("A2A operation unsupported: {0}")]
    UnsupportedOperation(String),
    #[error("A2A content type not supported: {0}")]
    ContentTypeNotSupported(String),
    #[error("A2A protocol version not supported: {0}")]
    VersionNotSupported(String),
    #[error("A2A invalid agent card: {0}")]
    InvalidAgentCard(String),
    #[error("A2A invalid stream response: {0}")]
    InvalidStreamResponse(String),
    #[error("A2A capability mismatch: {0}")]
    CapabilityMismatch(String),
}

pub type A2AClientResult<T> = Result<T, A2AClientError>;

/// Map A2A-specific JSON-RPC error codes to typed variants.
///
/// A2A v1.0 error codes:
/// - -32001: TaskNotFoundError
/// - -32002: TaskNotCancelableError
/// - -32003: PushNotificationNotSupportedError (unused in MVP)
/// - -32004: UnsupportedOperationError
/// - -32005: ContentTypeNotSupportedError
/// - -32006: InvalidAgentResponseError
/// - -32007: ExtendedAgentCardNotConfiguredError
/// - -32008: ExtensionSupportRequiredError
/// - -32009: VersionNotSupportedError
pub fn map_jsonrpc_error(
    error: super::jsonrpc::JsonRpcError,
    task_id_hint: Option<&str>,
) -> A2AClientError {
    match error.code {
        -32001 => A2AClientError::TaskNotFound(task_id_hint.unwrap_or_default().to_string()),
        -32002 => A2AClientError::TaskNotCancelable(task_id_hint.unwrap_or_default().to_string()),
        -32004 => A2AClientError::UnsupportedOperation(error.message),
        -32005 => A2AClientError::ContentTypeNotSupported(error.message),
        -32009 => A2AClientError::VersionNotSupported(error.message),
        _ => A2AClientError::JsonRpc {
            code: error.code,
            message: error.message,
            data: error.data,
        },
    }
}
