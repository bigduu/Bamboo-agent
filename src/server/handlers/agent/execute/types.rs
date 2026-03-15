use crate::core::ReasoningEffort;
use serde::{Deserialize, Serialize};

/// Response returned after triggering agent execution.
///
/// # Fields
///
/// * `session_id` - The session identifier
/// * `status` - Execution status ("started", "completed", "already_running")
/// * `events_url` - URL endpoint to subscribe to agent events (SSE)
#[derive(Serialize)]
pub struct ExecuteResponse {
    /// Session identifier for tracking this execution
    pub session_id: String,
    /// Current execution status
    pub status: String,
    /// SSE endpoint URL for receiving real-time events
    pub events_url: String,
}

/// Request payload for agent execution.
///
/// # Fields
///
/// * `model` - Required model identifier for execution
///
/// # Note
///
/// The `model` parameter is **required** and must be provided in every request.
/// It is not read from the session. This ensures explicit model selection
/// for each execution.
///
/// # Examples
///
/// ```json
/// {
///   "model": "claude-3-opus"
/// }
/// ```
#[derive(Deserialize)]
pub struct ExecuteRequest {
    /// Model to use for execution (required)
    pub model: String,
    /// Optional reasoning effort override for this execution.
    ///
    /// When omitted, the active provider default from config is used.
    #[serde(default)]
    pub reasoning_effort: Option<ReasoningEffort>,
}
