use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct PatchMessageRequest {
    pub content: String,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum TruncateRequest {
    /// Truncate all messages *after* the last user message.
    ///
    /// This is useful for "retry/regenerate" flows: keep the last user message
    /// but drop any assistant/tool tail so `POST /execute/{session_id}` can run again.
    AfterLastUser,
}

#[derive(Debug, Deserialize)]
pub struct RestoreSessionRequest {
    pub target_message_id: String,
    #[serde(default)]
    pub restore_files: bool,
}
