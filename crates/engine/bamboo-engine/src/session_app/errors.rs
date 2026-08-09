//! Error types for session use cases.

/// Errors that can occur when loading a session.
#[derive(Debug, thiserror::Error)]
pub enum SessionLoadError {
    #[error("session not found: {0}")]
    NotFound(String),
    #[error("storage error: {0}")]
    StorageError(String),
}

/// Errors that can occur when saving a session.
#[derive(Debug, thiserror::Error)]
pub enum SessionSaveError {
    #[error("storage error: {0}")]
    StorageError(String),
}

/// Errors from the chat use case.
#[derive(Debug, thiserror::Error)]
pub enum ChatError {
    #[error("session load failed: {0}")]
    LoadFailed(#[from] SessionLoadError),
    #[error("session save failed: {0}")]
    SaveFailed(#[from] SessionSaveError),
    #[error("invalid model: {0}")]
    InvalidModel(String),
    #[error("invalid workflow selection: {0}")]
    InvalidWorkflowSelection(String),
    #[error("session carries an invalid Project identity '{raw}': {message}")]
    InvalidProjectIdentity { raw: String, message: String },
    #[error(
        "session Project membership changed while preparing chat (expected {expected:?}, actual {actual:?})"
    )]
    ProjectIdentityConflict {
        expected: Option<bamboo_domain::ProjectId>,
        actual: Option<bamboo_domain::ProjectId>,
    },
}

/// Errors from the execute preparation use case.
#[derive(Debug, thiserror::Error)]
pub enum ExecutePreparationError {
    #[error("session not found: {0}")]
    NotFound(String),
    #[error("session load failed: {0}")]
    LoadFailed(#[from] SessionLoadError),
    #[error("session save failed: {0}")]
    SaveFailed(#[from] SessionSaveError),
    #[error("invalid model: model is required")]
    ModelRequired,
    #[error("invalid image fallback configuration: {0}")]
    ImageFallbackError(String),
}

/// Errors from the respond use case.
#[derive(Debug, thiserror::Error)]
pub enum RespondError {
    #[error("session not found: {0}")]
    NotFound(String),
    #[error("session load failed: {0}")]
    LoadFailed(#[from] SessionLoadError),
    #[error("session save failed: {0}")]
    SaveFailed(#[from] SessionSaveError),
    #[error("no pending question waiting for response")]
    NoPendingQuestion,
    #[error("pending question changed (expected tool call {expected}, actual {actual})")]
    PendingQuestionMismatch { expected: String, actual: String },
    #[error("invalid response: {0}")]
    InvalidResponse(String),
}
