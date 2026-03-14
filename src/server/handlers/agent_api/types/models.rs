use serde::{Deserialize, Serialize};

/// Represents a Claude Code project with its metadata and sessions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Project {
    /// Unique project identifier.
    pub id: String,
    /// File system path to the project.
    pub path: String,
    /// List of session IDs associated with this project.
    pub sessions: Vec<String>,
    /// Unix timestamp of project creation.
    pub created_at: u64,
    /// Unix timestamp of most recent session (if any).
    pub most_recent_session: Option<u64>,
}

/// Represents a Claude Code conversation session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Session {
    /// Unique session identifier.
    pub id: String,
    /// ID of the parent project.
    pub project_id: String,
    /// File system path to the project.
    pub project_path: String,
    /// Optional TODO data for the session.
    pub todo_data: Option<serde_json::Value>,
    /// Unix timestamp of session creation.
    pub created_at: u64,
    /// First message content (for preview).
    pub first_message: Option<String>,
    /// ISO timestamp of first message.
    pub message_timestamp: Option<String>,
}

/// Claude settings configuration wrapper.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaudeSettings {
    /// Settings data as JSON.
    #[serde(flatten)]
    pub data: serde_json::Value,
}

impl Default for ClaudeSettings {
    fn default() -> Self {
        Self {
            data: serde_json::json!({}),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ClaudeSettings;

    #[test]
    fn claude_settings_default_is_empty_object() {
        let settings = ClaudeSettings::default();
        assert_eq!(settings.data, serde_json::json!({}));
    }
}
