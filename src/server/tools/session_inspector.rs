use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;

use crate::agent::core::storage::{SessionIndexEntry, SessionStoreV2, Storage};
use crate::agent::core::tools::{Tool, ToolError, ToolExecutionContext, ToolResult};
use crate::agent::core::{Message, Role, SessionKind};

/// Server-only tool for inspecting V2 sessions stored under the Bamboo home dir.
///
/// Design goals:
/// - Return metadata first (index-backed) so the model can narrow scope.
/// - Allow bounded reads (pagination; from end; truncation).
/// - Support lightweight search across session titles and (optionally) tail messages.
/// - Keep inspection local by default; use child-session delegation only if the user explicitly asks.
pub struct SessionInspectorTool {
    session_store: Arc<SessionStoreV2>,
    storage: Arc<dyn Storage>,
}

impl SessionInspectorTool {
    pub fn new(session_store: Arc<SessionStoreV2>, storage: Arc<dyn Storage>) -> Self {
        Self {
            session_store,
            storage,
        }
    }

    async fn load_session(
        &self,
        session_id: &str,
    ) -> Result<crate::agent::core::Session, ToolError> {
        match self.storage.load_session(session_id).await {
            Ok(Some(s)) => Ok(s),
            Ok(None) => Err(ToolError::Execution(format!(
                "session not found: {session_id}"
            ))),
            Err(e) => Err(ToolError::Execution(format!(
                "failed to load session {session_id}: {e}"
            ))),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
enum SessionInspectorArgs {
    /// List sessions from the global index, with filtering/pagination.
    List {
        #[serde(default)]
        query: Option<String>,
        #[serde(default)]
        kind: Option<String>, // "root" | "child"
        #[serde(default)]
        pinned: Option<bool>,
        #[serde(default)]
        parent_session_id: Option<String>,
        #[serde(default)]
        root_session_id: Option<String>,
        #[serde(default)]
        created_by_schedule_id: Option<String>,
        #[serde(default)]
        limit: Option<usize>,
        #[serde(default)]
        offset: Option<usize>,
    },

    /// Get a single session's index metadata (fast).
    GetMeta { session_id: String },

    /// Read message slice from a session, bounded by limit/offset and content truncation.
    ReadMessages {
        session_id: String,
        /// Read from end of the conversation (default true).
        #[serde(default)]
        from_end: Option<bool>,
        /// Offset from the chosen side (0 = start/end depending on from_end).
        #[serde(default)]
        offset: Option<usize>,
        /// Number of messages to return.
        #[serde(default)]
        limit: Option<usize>,
        /// Truncate each message content to at most this many chars.
        #[serde(default)]
        truncate_chars: Option<usize>,
        /// Include System messages (default false).
        #[serde(default)]
        include_system: Option<bool>,
        /// Include Tool role messages (default true).
        #[serde(default)]
        include_tool: Option<bool>,
        /// Include Assistant tool_calls and tool_call_id fields (default false).
        #[serde(default)]
        include_tool_calls: Option<bool>,
        /// Include image URLs found in content_parts (default true).
        #[serde(default)]
        include_image_urls: Option<bool>,
    },

    /// Search session titles and (optionally) tail messages.
    Search {
        query: String,
        /// Search mode.
        /// - "title" (default): search in titles only
        /// - "tail_messages": also scan the last `tail_messages` messages of each candidate
        #[serde(default)]
        mode: Option<String>,
        /// Max sessions to scan when mode != "title".
        #[serde(default)]
        max_sessions: Option<usize>,
        /// Tail message count per session when scanning content.
        #[serde(default)]
        tail_messages: Option<usize>,
        /// Case-sensitive search (default false).
        #[serde(default)]
        case_sensitive: Option<bool>,
        /// Max matches to return.
        #[serde(default)]
        max_matches: Option<usize>,
    },
}

fn normalize_contains(haystack: &str, needle: &str, case_sensitive: bool) -> bool {
    if case_sensitive {
        haystack.contains(needle)
    } else {
        haystack
            .to_ascii_lowercase()
            .contains(&needle.to_ascii_lowercase())
    }
}

fn truncate_string(s: &str, max_chars: usize) -> String {
    if max_chars == 0 {
        return String::new();
    }
    if s.chars().count() <= max_chars {
        return s.to_string();
    }
    let mut out = String::with_capacity(max_chars + 3);
    for (i, ch) in s.chars().enumerate() {
        if i >= max_chars {
            break;
        }
        out.push(ch);
    }
    out.push_str("...");
    out
}

fn map_index_entry(e: &SessionIndexEntry) -> serde_json::Value {
    json!({
        "id": e.id,
        "kind": e.kind,
        "title": e.title,
        "pinned": e.pinned,
        "parent_session_id": e.parent_session_id,
        "root_session_id": e.root_session_id,
        "spawn_depth": e.spawn_depth,
        "created_by_schedule_id": e.created_by_schedule_id,
        "created_at": e.created_at,
        "updated_at": e.updated_at,
        "last_activity_at": e.last_activity_at,
        "message_count": e.message_count,
        "has_attachments": e.has_attachments,
        "token_usage": e.token_usage,
        // Expose rel_path so advanced workflows can inspect the Bamboo data dir directly if needed.
        "rel_path": e.rel_path,
    })
}

fn extract_image_urls(msg: &Message) -> Vec<String> {
    let mut out = Vec::new();
    let Some(parts) = msg.content_parts.as_ref() else {
        return out;
    };
    for p in parts {
        if let crate::agent::llm::models::ContentPart::ImageUrl { image_url } = p {
            out.push(image_url.url.clone());
        }
    }
    out
}

fn role_to_str(role: &Role) -> &'static str {
    match role {
        Role::System => "system",
        Role::User => "user",
        Role::Assistant => "assistant",
        Role::Tool => "tool",
    }
}

#[async_trait]
impl Tool for SessionInspectorTool {
    fn name(&self) -> &str {
        "session_inspector"
    }

    fn description(&self) -> &str {
        "Inspect sessions stored in Bamboo home dir (V2). Use list/get_meta to fetch metadata first, then read_messages with a small limit (prefer from_end). Keep inspection local by default; only use child-session delegation if the user explicitly asks for it."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        // Keep schema permissive; Rust parsing enforces action-specific requirements.
        json!({
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["list", "get_meta", "read_messages", "search"],
                    "description": "Which inspection action to perform."
                },
                "query": { "type": "string", "description": "Search string (list/search)." },
                "kind": { "type": "string", "enum": ["root", "child"], "description": "Filter by session kind (list)." },
                "pinned": { "type": "boolean", "description": "Filter pinned sessions (list)." },
                "parent_session_id": { "type": "string", "description": "Filter child sessions by parent (list)." },
                "root_session_id": { "type": "string", "description": "Filter by root session (list)." },
                "created_by_schedule_id": { "type": "string", "description": "Filter sessions created by a schedule (list)." },
                "limit": { "type": "number", "description": "Max items/messages to return (list/read_messages)." },
                "offset": { "type": "number", "description": "Offset (list/read_messages)." },
                "session_id": { "type": "string", "description": "Target session id (get_meta/read_messages)." },
                "from_end": { "type": "boolean", "description": "Read from end (read_messages)." },
                "truncate_chars": { "type": "number", "description": "Max chars per message (read_messages)." },
                "include_system": { "type": "boolean" },
                "include_tool": { "type": "boolean" },
                "include_tool_calls": { "type": "boolean" },
                "include_image_urls": { "type": "boolean" },
                "mode": { "type": "string", "enum": ["title", "tail_messages"] },
                "max_sessions": { "type": "number" },
                "tail_messages": { "type": "number" },
                "case_sensitive": { "type": "boolean" },
                "max_matches": { "type": "number" }
            },
            "required": ["action"]
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        self.execute_with_context(args, ToolExecutionContext::none("tool_call"))
            .await
    }

    async fn execute_with_context(
        &self,
        args: serde_json::Value,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolResult, ToolError> {
        let _caller_session_id = ctx.session_id.ok_or_else(|| {
            ToolError::Execution(
                "session_inspector requires a session_id in tool context".to_string(),
            )
        })?;

        let parsed: SessionInspectorArgs = serde_json::from_value(args).map_err(|e| {
            ToolError::InvalidArguments(format!("Invalid session_inspector args: {e}"))
        })?;

        match parsed {
            SessionInspectorArgs::List {
                query,
                kind,
                pinned,
                parent_session_id,
                root_session_id,
                created_by_schedule_id,
                limit,
                offset,
            } => {
                let limit = limit.unwrap_or(50).min(200);
                let offset = offset.unwrap_or(0).min(10_000);

                let mut items = self.session_store.list_index_entries().await;

                let query = query.as_ref().map(|v| v.trim()).filter(|v| !v.is_empty());
                let kind = kind.as_ref().map(|v| v.trim().to_ascii_lowercase());
                let parent_session_id = parent_session_id
                    .as_ref()
                    .map(|v| v.trim())
                    .filter(|v| !v.is_empty());
                let root_session_id = root_session_id
                    .as_ref()
                    .map(|v| v.trim())
                    .filter(|v| !v.is_empty());
                let created_by_schedule_id = created_by_schedule_id
                    .as_ref()
                    .map(|v| v.trim())
                    .filter(|v| !v.is_empty());

                items.retain(|e| {
                    if let Some(q) = query {
                        if !normalize_contains(&e.title, q, false)
                            && !normalize_contains(&e.id, q, false)
                        {
                            return false;
                        }
                    }
                    if let Some(ref k) = kind {
                        match k.as_str() {
                            "root" if e.kind != SessionKind::Root => return false,
                            "child" if e.kind != SessionKind::Child => return false,
                            _ => {}
                        }
                    }
                    if let Some(p) = pinned {
                        if e.pinned != p {
                            return false;
                        }
                    }
                    if let Some(pid) = parent_session_id {
                        if e.parent_session_id.as_deref() != Some(pid) {
                            return false;
                        }
                    }
                    if let Some(rid) = root_session_id {
                        if e.root_session_id != rid {
                            return false;
                        }
                    }
                    if let Some(sid) = created_by_schedule_id {
                        if e.created_by_schedule_id.as_deref() != Some(sid) {
                            return false;
                        }
                    }
                    true
                });

                let total = items.len();
                let page = items
                    .into_iter()
                    .skip(offset)
                    .take(limit)
                    .map(|e| map_index_entry(&e))
                    .collect::<Vec<_>>();

                Ok(ToolResult {
                    success: true,
                    result: json!({
                        "total": total,
                        "offset": offset,
                        "limit": limit,
                        "sessions": page,
                        "note": "Use get_meta/read_messages with a small limit. Keep inspection local unless the user explicitly asks for delegated sub-session work."
                    })
                    .to_string(),
                    display_preference: Some("Collapsible".to_string()),
                })
            }

            SessionInspectorArgs::GetMeta { session_id } => {
                let session_id = session_id.trim().to_string();
                if session_id.is_empty() {
                    return Err(ToolError::InvalidArguments(
                        "session_id must be a non-empty string".to_string(),
                    ));
                }

                let Some(entry) = self.session_store.get_index_entry(&session_id).await else {
                    return Err(ToolError::Execution(format!(
                        "session not found: {session_id}"
                    )));
                };

                Ok(ToolResult {
                    success: true,
                    result: json!({ "session": map_index_entry(&entry) }).to_string(),
                    display_preference: Some("Collapsible".to_string()),
                })
            }

            SessionInspectorArgs::ReadMessages {
                session_id,
                from_end,
                offset,
                limit,
                truncate_chars,
                include_system,
                include_tool,
                include_tool_calls,
                include_image_urls,
            } => {
                let session_id = session_id.trim().to_string();
                if session_id.is_empty() {
                    return Err(ToolError::InvalidArguments(
                        "session_id must be a non-empty string".to_string(),
                    ));
                }

                let from_end = from_end.unwrap_or(true);
                let offset = offset.unwrap_or(0).min(50_000);
                let limit = limit.unwrap_or(40).min(200);
                let truncate_chars = truncate_chars.unwrap_or(800).min(4000);
                let include_system = include_system.unwrap_or(false);
                let include_tool = include_tool.unwrap_or(true);
                let include_tool_calls = include_tool_calls.unwrap_or(false);
                let include_image_urls = include_image_urls.unwrap_or(true);

                let session = self.load_session(&session_id).await?;
                let total = session.messages.len();

                let mut messages: Vec<(usize, &Message)> = session
                    .messages
                    .iter()
                    .enumerate()
                    .filter(|(_, m)| {
                        if !include_system && matches!(m.role, Role::System) {
                            return false;
                        }
                        if !include_tool && matches!(m.role, Role::Tool) {
                            return false;
                        }
                        true
                    })
                    .collect();

                // Slice by index in the filtered sequence to keep semantics stable.
                let filtered_total = messages.len();
                let (start, end) = if from_end {
                    let end = filtered_total.saturating_sub(offset);
                    let start = end.saturating_sub(limit);
                    (start, end)
                } else {
                    let start = offset.min(filtered_total);
                    let end = (start + limit).min(filtered_total);
                    (start, end)
                };

                let slice = messages
                    .drain(start..end)
                    .map(|(idx, m)| {
                        let tool_calls_count = m.tool_calls.as_ref().map(|v| v.len()).unwrap_or(0);
                        let image_urls = if include_image_urls {
                            extract_image_urls(m)
                        } else {
                            Vec::new()
                        };
                        json!({
                            "index": idx,
                            "id": m.id,
                            "role": role_to_str(&m.role),
                            "created_at": m.created_at,
                            "content_len": m.content.len(),
                            "content": truncate_string(&m.content, truncate_chars),
                            "has_images": !image_urls.is_empty(),
                            "image_urls": image_urls,
                            "tool_calls_count": tool_calls_count,
                            "tool_call_id": if include_tool_calls { m.tool_call_id.clone() } else { None },
                        })
                    })
                    .collect::<Vec<_>>();

                Ok(ToolResult {
                    success: true,
                    result: json!({
                        "session_id": session_id,
                        "message_count_total": total,
                        "message_count_filtered": filtered_total,
                        "from_end": from_end,
                        "offset": offset,
                        "limit": limit,
                        "slice_count": slice.len(),
                        "messages": slice,
                        "note": "If you need to read a lot of content, iterate with bounded read_messages calls. Only delegate to a child session if the user explicitly asks."
                    })
                    .to_string(),
                    display_preference: Some("Collapsible".to_string()),
                })
            }

            SessionInspectorArgs::Search {
                query,
                mode,
                max_sessions,
                tail_messages,
                case_sensitive,
                max_matches,
            } => {
                let q = query.trim();
                if q.is_empty() {
                    return Err(ToolError::InvalidArguments(
                        "query must be a non-empty string".to_string(),
                    ));
                }
                let case_sensitive = case_sensitive.unwrap_or(false);
                let mode = mode
                    .as_deref()
                    .map(str::trim)
                    .filter(|v| !v.is_empty())
                    .unwrap_or("title")
                    .to_ascii_lowercase();
                let max_matches = max_matches.unwrap_or(50).min(200);

                let entries = self.session_store.list_index_entries().await;
                let mut results = Vec::new();

                // Always search titles first.
                for e in entries.iter() {
                    if normalize_contains(&e.title, q, case_sensitive)
                        || normalize_contains(&e.id, q, case_sensitive)
                    {
                        results.push(json!({
                            "type": "title_match",
                            "session": map_index_entry(e),
                        }));
                        if results.len() >= max_matches {
                            break;
                        }
                    }
                }

                if mode != "title" && results.len() < max_matches {
                    let max_sessions = max_sessions.unwrap_or(30).min(200);
                    let tail_messages = tail_messages.unwrap_or(40).min(200);

                    // Scan tail messages for additional matches (bounded).
                    for e in entries.into_iter().take(max_sessions) {
                        if results.len() >= max_matches {
                            break;
                        }
                        let Ok(session) = self.storage.load_session(&e.id).await else {
                            continue;
                        };
                        let Some(session) = session else {
                            continue;
                        };
                        let start = session.messages.len().saturating_sub(tail_messages);
                        for (idx, m) in session.messages.iter().enumerate().skip(start) {
                            if results.len() >= max_matches {
                                break;
                            }
                            if !normalize_contains(&m.content, q, case_sensitive) {
                                continue;
                            }
                            results.push(json!({
                                "type": "message_match",
                                "session_id": e.id,
                                "session_title": e.title,
                                "message_index": idx,
                                "message_id": m.id,
                                "role": role_to_str(&m.role),
                                "created_at": m.created_at,
                                "content_preview": truncate_string(&m.content, 240),
                            }));
                        }
                    }
                }

                Ok(ToolResult {
                    success: true,
                    result: json!({
                        "query": q,
                        "mode": mode,
                        "case_sensitive": case_sensitive,
                        "matches": results,
                        "note": "Consider narrowing by session_id + read_messages. Keep summarization local unless the user explicitly asks for delegated child-session work."
                    })
                    .to_string(),
                    display_preference: Some("Collapsible".to_string()),
                })
            }
        }
    }
}
