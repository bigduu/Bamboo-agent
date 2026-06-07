use dashmap::DashMap;
use serde_json::json;
use std::sync::{Arc, OnceLock};
use tokio::sync::Mutex;

use bamboo_agent_core::{ToolError, ToolResult};
use bamboo_memory::memory::DEFAULT_TOPIC;
use bamboo_memory::memory_store::{count_chars, truncate_chars, MemoryStore};

pub const MAX_SESSION_NOTE_CHARS: usize = 12_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionMemoryAction {
    Read,
    Append,
    Replace,
    Clear,
    ListTopics,
}

#[derive(Debug, Clone, Copy)]
pub struct SessionMemoryActionNames {
    pub tool_name: &'static str,
    pub read: &'static str,
    pub append: &'static str,
    pub replace: &'static str,
    pub clear: &'static str,
    pub list_topics: &'static str,
}

pub const SESSION_NOTE_ACTION_NAMES: SessionMemoryActionNames = SessionMemoryActionNames {
    tool_name: "session_note",
    read: "read",
    append: "append",
    replace: "replace",
    clear: "clear",
    list_topics: "list_topics",
};

pub const MEMORY_SESSION_ACTION_NAMES: SessionMemoryActionNames = SessionMemoryActionNames {
    tool_name: "memory",
    read: "session_read",
    append: "session_append",
    replace: "session_replace",
    clear: "session_clear",
    list_topics: "session_list_topics",
};

fn note_locks() -> &'static DashMap<String, Arc<Mutex<()>>> {
    static NOTE_LOCKS: OnceLock<DashMap<String, Arc<Mutex<()>>>> = OnceLock::new();
    NOTE_LOCKS.get_or_init(DashMap::new)
}

pub fn session_memory_lock(session_id: &str) -> Arc<Mutex<()>> {
    note_locks()
        .entry(session_id.to_string())
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}

pub fn parse_session_note_action(action: &str) -> Result<SessionMemoryAction, ToolError> {
    match action.trim().to_ascii_lowercase().as_str() {
        "read" => Ok(SessionMemoryAction::Read),
        "append" => Ok(SessionMemoryAction::Append),
        "replace" => Ok(SessionMemoryAction::Replace),
        "clear" => Ok(SessionMemoryAction::Clear),
        "list_topics" => Ok(SessionMemoryAction::ListTopics),
        _ => Err(ToolError::InvalidArguments(
            "action must be one of: read, append, replace, clear, list_topics. Rewrite the session_note call with valid JSON.".to_string(),
        )),
    }
}

pub async fn execute_session_memory_action(
    memory: &MemoryStore,
    session_id: &str,
    action: SessionMemoryAction,
    topic: Option<&str>,
    content: Option<&str>,
    max_chars: Option<usize>,
    names: SessionMemoryActionNames,
) -> Result<ToolResult, ToolError> {
    let topic = topic
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(DEFAULT_TOPIC);
    let session_guard = session_memory_lock(session_id);
    let _guard = session_guard.lock().await;

    match action {
        SessionMemoryAction::Read => {
            let max_chars = max_chars
                .unwrap_or(MAX_SESSION_NOTE_CHARS)
                .clamp(1, MAX_SESSION_NOTE_CHARS);
            let content = memory.read_session_topic(session_id, topic).await.map_err(|error| {
                ToolError::Execution(format!(
                    "Failed to read note: {error}. Rewrite and retry {} with valid JSON, e.g. {{\"action\":\"{}\",\"topic\":\"{}\"}}.",
                    names.tool_name, names.read, topic,
                ))
            })?;
            let exists = content.is_some();
            let body = content.unwrap_or_default();
            let length_chars = count_chars(&body);
            let (snippet, truncated) = truncate_chars(&body, max_chars);
            Ok(ToolResult {
                success: true,
                result: json!({
                    "action": names.read,
                    "session_id": session_id,
                    "topic": topic,
                    "exists": exists,
                    "content": snippet,
                    "length_chars": length_chars,
                    "body_truncated": truncated,
                    "max_chars": max_chars,
                })
                .to_string(),
                display_preference: Some("json".to_string()),
            })
        }
        SessionMemoryAction::Clear => {
            let deleted = memory.delete_session_topic(session_id, topic).await.map_err(|error| {
                ToolError::Execution(format!(
                    "Failed to delete note: {error}. Rewrite and retry {} with valid JSON, e.g. {{\"action\":\"{}\",\"topic\":\"{}\"}}.",
                    names.tool_name, names.clear, topic,
                ))
            })?;
            Ok(ToolResult {
                success: true,
                result: json!({
                    "action": names.clear,
                    "session_id": session_id,
                    "topic": topic,
                    "deleted": deleted,
                })
                .to_string(),
                display_preference: Some("json".to_string()),
            })
        }
        SessionMemoryAction::ListTopics => {
            let topics = memory.list_session_topics(session_id).await.map_err(|error| {
                ToolError::Execution(format!(
                    "Failed to list topics: {error}. Rewrite and retry {} with valid JSON, e.g. {{\"action\":\"{}\"}}.",
                    names.tool_name, names.list_topics,
                ))
            })?;
            Ok(ToolResult {
                success: true,
                result: json!({
                    "action": names.list_topics,
                    "session_id": session_id,
                    "topics": topics,
                    "count": topics.len(),
                })
                .to_string(),
                display_preference: Some("json".to_string()),
            })
        }
        SessionMemoryAction::Replace | SessionMemoryAction::Append => {
            let content = content
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    ToolError::InvalidArguments(format!(
                        "content is required for action={}|{}. Rewrite the {} call with valid JSON and include non-empty content.",
                        names.append, names.replace, names.tool_name,
                    ))
                })?;

            if action == SessionMemoryAction::Replace {
                let length_chars = count_chars(content);
                if length_chars > MAX_SESSION_NOTE_CHARS {
                    return Err(ToolError::Execution(format!(
                        "session note too long (>{} chars). Compress it (rewrite more concisely) and call {} with action={} again.",
                        MAX_SESSION_NOTE_CHARS, names.tool_name, names.replace,
                    )));
                }

                let path = memory
                    .write_session_topic(session_id, topic, content)
                    .await
                    .map_err(|error| {
                        ToolError::Execution(format!(
                            "Failed to write note: {error}. Rewrite and retry {} with valid JSON, e.g. {{\"action\":\"{}\",\"topic\":\"{}\",\"content\":\"...\"}}.",
                            names.tool_name, names.replace, topic,
                        ))
                    })?;

                Ok(ToolResult {
                    success: true,
                    result: json!({
                        "action": names.replace,
                        "session_id": session_id,
                        "topic": topic,
                        "path": path,
                        "length_chars": length_chars,
                        "max_chars": MAX_SESSION_NOTE_CHARS,
                    })
                    .to_string(),
                    display_preference: Some("json".to_string()),
                })
            } else {
                let existing = memory.read_session_topic(session_id, topic).await.map_err(|error| {
                    ToolError::Execution(format!(
                        "Failed to read note: {error}. Rewrite and retry {} with valid JSON, e.g. {{\"action\":\"{}\",\"topic\":\"{}\",\"content\":\"...\"}}.",
                        names.tool_name, names.append, topic,
                    ))
                })?;

                let mut next = existing.unwrap_or_default();
                if !next.is_empty() {
                    next.push_str("\n\n");
                }
                next.push_str(content);

                let next_len = count_chars(&next);
                if next_len > MAX_SESSION_NOTE_CHARS {
                    return Err(ToolError::Execution(format!(
                        "session note would exceed the limit ({}>{} chars). Compress the existing note (use {} action={} topic={}), then call {} action={} with a shorter version, then append again if needed.",
                        next_len,
                        MAX_SESSION_NOTE_CHARS,
                        names.tool_name,
                        names.read,
                        topic,
                        names.tool_name,
                        names.replace,
                    )));
                }

                let path = memory
                    .write_session_topic(session_id, topic, &next)
                    .await
                    .map_err(|error| {
                        ToolError::Execution(format!(
                            "Failed to write note: {error}. Rewrite and retry {} with valid JSON, e.g. {{\"action\":\"{}\",\"topic\":\"{}\",\"content\":\"...\"}}.",
                            names.tool_name, names.append, topic,
                        ))
                    })?;

                Ok(ToolResult {
                    success: true,
                    result: json!({
                        "action": names.append,
                        "session_id": session_id,
                        "topic": topic,
                        "path": path,
                        "length_chars": next_len,
                        "max_chars": MAX_SESSION_NOTE_CHARS,
                    })
                    .to_string(),
                    display_preference: Some("json".to_string()),
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_memory::memory_store::MemoryStore;

    #[tokio::test]
    async fn execute_read_reports_length_and_truncation() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = MemoryStore::new(dir.path());
        store
            .write_session_topic("session-1", "default", &"x".repeat(32))
            .await
            .expect("write session topic");

        let result = execute_session_memory_action(
            &store,
            "session-1",
            SessionMemoryAction::Read,
            Some("default"),
            None,
            Some(8),
            SESSION_NOTE_ACTION_NAMES,
        )
        .await
        .expect("read should succeed");

        let value: serde_json::Value = serde_json::from_str(&result.result).expect("valid json");
        assert_eq!(value["action"], "read");
        assert_eq!(value["length_chars"], 32);
        assert_eq!(value["body_truncated"], true);
        assert_eq!(value["content"].as_str().unwrap().chars().count(), 8);
    }

    #[tokio::test]
    async fn execute_append_enforces_shared_limit() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = MemoryStore::new(dir.path());
        store
            .write_session_topic(
                "session-1",
                "default",
                &"x".repeat(MAX_SESSION_NOTE_CHARS - 1),
            )
            .await
            .expect("write session topic");

        let error = execute_session_memory_action(
            &store,
            "session-1",
            SessionMemoryAction::Append,
            Some("default"),
            Some("y"),
            None,
            MEMORY_SESSION_ACTION_NAMES,
        )
        .await
        .expect_err("append should fail");

        let message = error.to_string();
        assert!(message.contains("session note would exceed the limit"));
        assert!(message.contains("action=session_read"));
        assert!(message.contains("action=session_replace"));
    }
}
