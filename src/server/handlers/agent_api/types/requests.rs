use serde::Deserialize;

/// Request body for creating a new project.
#[derive(Debug, Deserialize)]
pub struct CreateProjectRequest {
    /// File system path to the project directory.
    pub path: String,
}

/// Request body for saving Claude settings.
#[derive(Debug, Deserialize)]
pub struct SaveSettingsRequest {
    /// Settings data as JSON.
    pub settings: serde_json::Value,
}

/// Request body for saving system prompt.
#[derive(Debug, Deserialize)]
pub struct SaveSystemPromptRequest {
    /// System prompt content (markdown).
    pub content: String,
}

/// Request body for executing Claude code.
#[derive(Debug, Deserialize)]
pub struct ExecuteRequest {
    /// Project directory path.
    pub project_path: String,
    /// User prompt to execute.
    pub prompt: String,
    /// Optional session ID to resume.
    pub session_id: Option<String>,
    /// Optional override for Claude's Anthropic base URL.
    ///
    /// If omitted, Bamboo defaults to `http://127.0.0.1:{port}/anthropic` so the
    /// Claude Code CLI talks to Bamboo's embedded Anthropic-compatible API.
    pub anthropic_base_url: Option<String>,
    /// Optional JSON schema for structured output (passed to `claude --json-schema`).
    pub json_schema: Option<String>,
    /// If omitted, defaults to `true` (skip Claude's user confirmation prompts).
    pub dangerously_skip_permissions: Option<bool>,
    /// If omitted, defaults to `true` (better streaming UX).
    pub include_partial_messages: Option<bool>,
}

/// Request body for canceling execution.
#[derive(Debug, Deserialize)]
pub struct CancelRequest {
    /// Session ID to cancel.
    pub session_id: String,
}
