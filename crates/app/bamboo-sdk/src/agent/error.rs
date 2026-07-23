//! Typed SDK error surface.
//!
//! Replaces the stringly-typed `Result<_, String>` the builder previously
//! returned (`with_defaults_for_data_dir` / `build`) with a single
//! `#[derive(thiserror::Error)]` enum, matching the runtime's own [`AgentError`]
//! (`bamboo_agent_core::AgentError`) instead of forcing callers to match on
//! substrings. Every fallible SDK entry point added alongside cancellation /
//! approval+resume / session ergonomics returns `Result<_, SdkError>`.
//!
//! `run` / `run_stream` and friends keep returning [`AgentError`] directly (it
//! was already typed) — [`SdkError::Agent`] lets callers unify the two with `?`
//! in a function that returns `Result<_, SdkError>`.

use bamboo_agent_core::AgentError;
use bamboo_engine::session_app::errors::{RespondError, SessionLoadError, SessionSaveError};

/// Errors surfaced by the `bamboo_sdk` facade.
#[derive(Debug, thiserror::Error)]
pub enum SdkError {
    /// A run/resume of the underlying agent loop failed. Wraps the runtime's
    /// own typed error so `?` composes across `Agent::run*` and the newer
    /// facade methods in a function returning `Result<_, SdkError>`.
    #[error(transparent)]
    Agent(#[from] AgentError),

    /// `AgentBuilder::build()` failed — the wrapped engine builder rejected an
    /// incomplete or inconsistent dependency set (e.g. a required dependency
    /// was never supplied).
    #[error("agent builder error: {0}")]
    Build(String),

    /// `with_defaults_for_data_dir` failed to read/parse `config.json`.
    #[error("configuration error: {0}")]
    Config(String),

    /// `with_defaults_for_data_dir` failed to construct the configured LLM
    /// provider — commonly a missing/invalid API key. See
    /// [`AgentBuilder::api_key`](super::AgentBuilder::api_key) and
    /// [`AgentBuilder::provider_name`](super::AgentBuilder::provider_name).
    #[error("provider initialization failed: {0}")]
    ProviderInit(String),

    /// [`AgentBuilder::api_key`](super::AgentBuilder::api_key) was paired with
    /// a provider that does not accept a plain API key (for example `copilot`,
    /// which uses cached OAuth) or an unknown provider name.
    #[error("provider '{provider}' does not accept a plain API key")]
    UnsupportedApiKeyProvider { provider: String },

    /// A new session was requested but neither `.model(...)` nor the assembled
    /// provider configuration supplied an effective model.
    #[error(
        "no model configured; call AgentBuilder::model or configure the active provider model"
    )]
    ModelNotConfigured,

    /// A caller supplied a Project id that is not a valid opaque/path-safe id.
    #[error("invalid Project id: {0}")]
    InvalidProjectId(String),

    /// The defaults-backed SDK could not open the first-class Project store.
    #[error("Project store initialization failed: {0}")]
    ProjectStoreInit(String),

    /// The configured Project is missing or archived in the defaults-backed store.
    #[error("Project is unavailable: {0}")]
    ProjectUnavailable(String),

    /// `with_defaults_for_data_dir` failed to open the session store at the
    /// given data directory.
    #[error("session store initialization failed: {0}")]
    StoreInit(String),

    /// `with_defaults_for_data_dir` failed to initialize the skill manager.
    #[error("skill manager initialization failed: {0}")]
    SkillInit(String),

    /// An MCP server failed to start (see
    /// [`AgentBuilder::mcp_server`](super::AgentBuilder::mcp_server)).
    #[error("MCP server '{server_id}' failed to start: {source}")]
    McpServerStart {
        server_id: String,
        #[source]
        source: bamboo_mcp::McpError,
    },

    /// The requested session does not exist.
    #[error("session not found: {0}")]
    SessionNotFound(String),

    /// Loading a session from storage failed.
    #[error("session load failed: {0}")]
    SessionLoad(String),

    /// Persisting a session failed.
    #[error("session save failed: {0}")]
    SessionSave(String),

    /// [`Agent::answer`](super::Agent::answer) was called on a session with no
    /// pending question / approval to respond to.
    #[error("no pending question waiting for response")]
    NoPendingQuestion,

    /// The response passed to [`Agent::answer`](super::Agent::answer) did not
    /// match one of the pending question's fixed options (and it does not
    /// allow a custom response).
    #[error("invalid response: {0}")]
    InvalidResponse(String),

    /// A capability that requires `with_defaults_for_data_dir` (e.g. session
    /// listing, which needs the concrete session-index handle) was called on
    /// an `Agent` assembled from manually-injected dependencies.
    #[error("unsupported: {0}")]
    Unsupported(String),

    /// Underlying I/O failure (session store reads/writes, etc).
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

impl From<SessionLoadError> for SdkError {
    fn from(error: SessionLoadError) -> Self {
        match error {
            SessionLoadError::NotFound(id) => SdkError::SessionNotFound(id),
            SessionLoadError::StorageError(message) => SdkError::SessionLoad(message),
        }
    }
}

impl From<SessionSaveError> for SdkError {
    fn from(error: SessionSaveError) -> Self {
        match error {
            SessionSaveError::StorageError(message) => SdkError::SessionSave(message),
        }
    }
}

impl From<RespondError> for SdkError {
    fn from(error: RespondError) -> Self {
        match error {
            RespondError::NotFound(id) => SdkError::SessionNotFound(id),
            RespondError::LoadFailed(inner) => inner.into(),
            RespondError::SaveFailed(inner) => inner.into(),
            RespondError::NoPendingQuestion => SdkError::NoPendingQuestion,
            RespondError::InvalidResponse(message) => SdkError::InvalidResponse(message),
        }
    }
}
