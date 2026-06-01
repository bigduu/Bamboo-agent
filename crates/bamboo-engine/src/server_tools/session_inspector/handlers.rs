//! Per-action handlers for the session inspector tool.

use bamboo_agent_core::tools::{ToolError, ToolResult};
use bamboo_agent_core::{Message, Role, SessionKind};
use serde_json::json;

use super::helpers::{
    extract_image_urls, map_index_entry, normalize_contains, role_to_str, truncate_string,
};
use super::SessionInspectorTool;

#[allow(clippy::too_many_arguments)]
pub(super) async fn handle_list(
    tool: &SessionInspectorTool,
    query: Option<String>,
    kind: Option<String>,
    pinned: Option<bool>,
    parent_session_id: Option<String>,
    root_session_id: Option<String>,
    created_by_schedule_id: Option<String>,
    limit: Option<usize>,
    offset: Option<usize>,
) -> Result<ToolResult, ToolError> {
    let limit = limit.unwrap_or(50).min(200);
    let offset = offset.unwrap_or(0).min(10_000);

    let mut items = tool.session_store.list_index_entries().await;

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
            if !normalize_contains(&e.title, q, false) && !normalize_contains(&e.id, q, false) {
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

pub(super) async fn handle_get_meta(
    tool: &SessionInspectorTool,
    session_id: String,
) -> Result<ToolResult, ToolError> {
    let session_id = session_id.trim().to_string();
    if session_id.is_empty() {
        return Err(ToolError::InvalidArguments(
            "session_id must be a non-empty string".to_string(),
        ));
    }

    let Some(entry) = tool.session_store.get_index_entry(&session_id).await else {
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

#[allow(clippy::too_many_arguments)]
pub(super) async fn handle_read_messages(
    tool: &SessionInspectorTool,
    session_id: String,
    from_end: Option<bool>,
    offset: Option<usize>,
    limit: Option<usize>,
    truncate_chars: Option<usize>,
    include_system: Option<bool>,
    include_tool: Option<bool>,
    include_tool_calls: Option<bool>,
    include_image_urls: Option<bool>,
) -> Result<ToolResult, ToolError> {
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

    let session = tool.load_session(&session_id).await?;
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

pub(super) async fn handle_read_compressed_cache(
    tool: &SessionInspectorTool,
    session_id: String,
    offset: Option<usize>,
    limit: Option<usize>,
    truncate_chars: Option<usize>,
    include_summary: Option<bool>,
) -> Result<ToolResult, ToolError> {
    let session_id = session_id.trim().to_string();
    if session_id.is_empty() {
        return Err(ToolError::InvalidArguments(
            "session_id must be a non-empty string".to_string(),
        ));
    }

    let offset = offset.unwrap_or(0).min(1_000_000);
    let limit = limit.unwrap_or(40).min(200);
    let truncate_chars = truncate_chars.unwrap_or(1200).min(20_000);
    let include_summary = include_summary.unwrap_or(true);

    let sqlite_snapshot = tool
        .session_store
        .search_index()
        .read_compressed_cache(&session_id, offset, limit, truncate_chars)
        .await;

    let (source, summary, total_compressed, messages) = match sqlite_snapshot {
        Ok(snapshot) if snapshot.total_compressed_messages > 0 => (
            "sqlite_fts",
            if include_summary {
                snapshot.summary
            } else {
                None
            },
            snapshot.total_compressed_messages,
            snapshot
                .messages
                .into_iter()
                .map(|row| {
                    json!({
                        "id": row.message_id,
                        "index": row.message_index,
                        "role": row.role,
                        "created_at": row.created_at,
                        "content_len": row.content_len,
                        "content": row.content,
                    })
                })
                .collect::<Vec<_>>(),
        ),
        Ok(_) | Err(_) => {
            let session = tool.load_session(&session_id).await?;
            let summary = if include_summary {
                session
                    .conversation_summary
                    .as_ref()
                    .map(|value| value.content.clone())
            } else {
                None
            };
            let compressed_messages = session
                .messages
                .iter()
                .enumerate()
                .filter(|(_, message)| message.compressed)
                .collect::<Vec<_>>();
            let total = compressed_messages.len();
            let slice = compressed_messages
                .into_iter()
                .skip(offset)
                .take(limit)
                .map(|(index, message)| {
                    json!({
                        "id": message.id,
                        "index": index,
                        "role": role_to_str(&message.role),
                        "created_at": message.created_at,
                        "content_len": message.content.chars().count(),
                        "content": truncate_string(&message.content, truncate_chars),
                    })
                })
                .collect::<Vec<_>>();
            ("session_json_fallback", summary, total, slice)
        }
    };

    Ok(ToolResult {
        success: true,
        result: json!({
            "session_id": session_id,
            "source": source,
            "offset": offset,
            "limit": limit,
            "slice_count": messages.len(),
            "total_compressed_messages": total_compressed,
            "summary": summary,
            "messages": messages,
            "note": "Use this for bounded recall from compressed history. Prioritize current task list and recent turns when conflicts appear."
        })
        .to_string(),
        display_preference: Some("Collapsible".to_string()),
    })
}

pub(super) async fn handle_search(
    tool: &SessionInspectorTool,
    query: String,
    mode: Option<String>,
    max_sessions: Option<usize>,
    tail_messages: Option<usize>,
    case_sensitive: Option<bool>,
    max_matches: Option<usize>,
) -> Result<ToolResult, ToolError> {
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

    if !case_sensitive {
        match tool.session_store.search_index().search(q, max_matches).await {
            Ok(fts_matches) if !fts_matches.is_empty() => {
                let matches = fts_matches
                    .into_iter()
                    .map(|m| {
                        json!({
                            "type": if m.match_type == "session" { "title_match" } else { "message_match" },
                            "session_id": m.session_id,
                            "session_title": m.session_title,
                            "session_kind": m.session_kind,
                            "root_session_id": m.root_session_id,
                            "parent_session_id": m.parent_session_id,
                            "pinned": m.pinned,
                            "updated_at": m.updated_at,
                            "rank": m.rank,
                            "message_id": m.message_id,
                            "message_index": m.message_index,
                            "role": m.role,
                            "content_preview": m.content_preview,
                        })
                    })
                    .collect::<Vec<_>>();

                return Ok(ToolResult {
                    success: true,
                    result: json!({
                        "query": q,
                        "mode": mode,
                        "case_sensitive": case_sensitive,
                        "search_backend": "sqlite_fts",
                        "matches": matches,
                        "note": "Results came from the local SQLite FTS session search index. Use read_messages for bounded inspection of matched sessions."
                    })
                    .to_string(),
                    display_preference: Some("Collapsible".to_string()),
                });
            }
            Ok(_) => {}
            Err(error) => {
                tracing::warn!(
                    "session_history FTS search failed for query '{}': {}. Falling back to in-memory scan.",
                    q,
                    error
                );
            }
        }
    }

    let entries = tool.session_store.list_index_entries().await;
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
            let Ok(session) = tool.storage.load_session(&e.id).await else {
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
