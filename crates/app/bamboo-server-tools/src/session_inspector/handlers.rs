//! Per-action handlers for the session inspector tool.

use bamboo_agent_core::tools::{ToolError, ToolResult};
use bamboo_agent_core::{Message, Role, Session, SessionKind};
use serde_json::json;
use std::collections::HashSet;

use super::helpers::{
    excerpt_around_match, extract_image_urls, map_index_entry, normalize_contains, role_to_str,
    truncate_string,
};
use super::SessionInspectorTool;

const SEARCH_CURRENT_DEFAULT_LIMIT: usize = 20;
const SEARCH_CURRENT_MAX_LIMIT: usize = 50;
const SEARCH_CURRENT_MAX_QUERY_CHARS: usize = 512;
const SEARCH_CURRENT_PREVIEW_CHARS: usize = 600;

fn current_request_excluded_message_ids(
    session: &Session,
    current_request_index: Option<usize>,
) -> Vec<String> {
    current_request_index
        .and_then(|index| session.messages.get(index))
        .map(|message| vec![message.id.clone()])
        .unwrap_or_default()
}

fn search_content_matches(content: &str, query: &str) -> bool {
    bamboo_storage::search_index::session_message_content_matches(content, query)
}

pub(super) async fn handle_search_current(
    tool: &SessionInspectorTool,
    caller_session_id: &str,
    current_tool_call_id: &str,
    query: String,
    limit: Option<usize>,
) -> Result<ToolResult, ToolError> {
    let query = query.trim();
    if query.is_empty() {
        return Err(ToolError::InvalidArguments(
            "query must be a non-empty string".to_string(),
        ));
    }
    if query.chars().count() > SEARCH_CURRENT_MAX_QUERY_CHARS {
        return Err(ToolError::InvalidArguments(format!(
            "query must be at most {SEARCH_CURRENT_MAX_QUERY_CHARS} characters"
        )));
    }
    let limit = limit
        .unwrap_or(SEARCH_CURRENT_DEFAULT_LIMIT)
        .clamp(1, SEARCH_CURRENT_MAX_LIMIT);

    // Foreground saves enqueue index work. This action is an explicit
    // read-after-write boundary. Read the durable revision on both sides of
    // the Session load so a concurrent save cannot make an older/newer index
    // snapshot look complete for the loaded transcript.
    tool.session_store.flush_search_index().await;
    let source_revision_before = match tool
        .session_store
        .search_source_revision(caller_session_id)
        .await
    {
        Ok(revision) => revision,
        Err(error) => {
            tracing::warn!(
                session_id = caller_session_id,
                %error,
                "cannot read current Session search revision before load"
            );
            None
        }
    };

    // The durable Session is also the source of the current tool-call boundary
    // and generated-result exclusions. Search authorization itself comes only
    // from `caller_session_id`, which ToolCtx supplied.
    let session = tool.load_session(caller_session_id).await?;
    let source_revision_after = match tool
        .session_store
        .search_source_revision(caller_session_id)
        .await
    {
        Ok(revision) => revision,
        Err(error) => {
            tracing::warn!(
                session_id = caller_session_id,
                %error,
                "cannot read current Session search revision after load"
            );
            None
        }
    };
    let stable_source_revision = source_revision_before
        .as_ref()
        .filter(|revision| source_revision_after.as_ref() == Some(*revision));
    let (before_message_index, current_request_index) =
        super::self_history::current_history_boundary(&session, current_tool_call_id);
    let excluded_message_ids =
        current_request_excluded_message_ids(&session, current_request_index);
    let excluded = excluded_message_ids
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();

    let indexed = tool
        .session_store
        .search_index()
        .search_messages_in_session(
            caller_session_id,
            query,
            before_message_index,
            &excluded_message_ids,
            limit,
        )
        .await;

    let mut matches = Vec::new();
    let mut seen = HashSet::new();
    let mut backend = "session_json_fallback".to_string();
    let mut index_fresh = false;
    let mut index_results_complete = false;
    let mut indexed_backend = None;
    match indexed {
        Ok(page) => {
            indexed_backend = Some(format!("sqlite_{}", page.query_backend));
            let revision_matches = stable_source_revision.is_some_and(|revision| {
                Some(revision.as_str()) == page.indexed_source_revision.as_deref()
            });
            let timestamp_matches = page.indexed_updated_at == Some(session.updated_at);
            let mut candidates_valid = true;
            for hit in page.matches {
                let Some(message) = session.messages.get(hit.message_index) else {
                    candidates_valid = false;
                    continue;
                };
                if message.id != hit.message_id
                    || !search_content_matches(&message.content, query)
                    || !seen.insert(message.id.clone())
                {
                    candidates_valid = false;
                    continue;
                }
                matches.push(json!({
                    "id": message.id,
                    "index": hit.message_index,
                    "role": role_to_str(&message.role),
                    "created_at": message.created_at,
                    "compressed": message.compressed,
                    "content_len": message.content.chars().count(),
                    "content_preview": excerpt_around_match(&message.content, query, SEARCH_CURRENT_PREVIEW_CHARS),
                    "match_source": hit.match_source,
                    "rank": hit.rank,
                }));
            }
            index_results_complete = page.results_complete;
            index_fresh =
                revision_matches && timestamp_matches && candidates_valid && index_results_complete;
            if index_fresh {
                backend = indexed_backend
                    .clone()
                    .unwrap_or_else(|| "sqlite_fts_unicode".to_string());
            }
        }
        Err(error) => tracing::warn!(
            session_id = caller_session_id,
            %error,
            "session_history current-Session index search failed; using durable Session fallback"
        ),
    }

    // Supplement from the authoritative Session snapshot only if revision,
    // timestamp, candidate validation, or bounded-candidate completeness could
    // not prove that the derived result is current. A fresh zero/under-filled
    // page is complete and therefore avoids this O(messages) scan.
    let mut fallback_scanned_messages = 0usize;
    if !index_fresh {
        // A stale page is useful only as diagnostics. Rebuild the visible page
        // exclusively from canonical content so stale ranking or omissions can
        // never influence a full result page.
        matches.clear();
        seen.clear();
        let generated_artifacts =
            bamboo_storage::search_index::session_history_search_artifact_ids(&session);
        for (index, message) in session
            .messages
            .iter()
            .enumerate()
            .take(before_message_index)
            .rev()
        {
            if matches.len() == limit {
                break;
            }
            fallback_scanned_messages += 1;
            if excluded.contains(message.id.as_str())
                || generated_artifacts.contains(&message.id)
                || seen.contains(&message.id)
                || !search_content_matches(&message.content, query)
            {
                continue;
            }
            seen.insert(message.id.clone());
            matches.push(json!({
                "id": message.id,
                "index": index,
                "role": role_to_str(&message.role),
                "created_at": message.created_at,
                "compressed": message.compressed,
                "content_len": message.content.chars().count(),
                "content_preview": excerpt_around_match(&message.content, query, SEARCH_CURRENT_PREVIEW_CHARS),
                "match_source": "session_json",
                "rank": null,
            }));
        }
        backend = "session_json_fallback".to_string();
    }

    Ok(ToolResult {
        success: true,
        result: json!({
            "session_id": caller_session_id,
            "query": query,
            "limit": limit,
            "searched_before_message_index": before_message_index,
            "search_backend": backend,
            "index_backend": indexed_backend,
            "index_fresh": index_fresh,
            "index_results_complete": index_results_complete,
            "fallback_scanned_messages": fallback_scanned_messages,
            "match_count": matches.len(),
            "matches": matches,
            "note": "Matches are bounded read-only excerpts from this Session's stored messages. Compressed messages are searched directly; no message is restored and no compressed flag or context epoch is changed."
        })
        .to_string(),
        display_preference: Some("Collapsible".to_string()),
        images: Vec::new(),
    })
}

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
        images: Vec::new(),
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
        images: Vec::new(),
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
        images: Vec::new(),
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

    // Foreground saves deliberately do not wait for FTS. This action promises
    // the cached SQLite view when it exists, so it is an explicit
    // read-after-write boundary rather than pushing that latency back into all
    // session commits.
    tool.session_store.flush_search_index().await;
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
        images: Vec::new(),
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
        // Search is the other explicit consumer that requires the deferred FTS
        // queue to catch up before deciding whether to use its fallback scan.
        tool.session_store.flush_search_index().await;
        match tool
            .session_store
            .search_index()
            .search(q, max_matches)
            .await
        {
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
                    images: Vec::new(),
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
        images: Vec::new(),
    })
}
