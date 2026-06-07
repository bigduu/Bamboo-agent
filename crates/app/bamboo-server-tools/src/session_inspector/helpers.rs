//! Pure formatting / matching helpers for the session inspector tool.

use bamboo_agent_core::MessagePart;
use bamboo_agent_core::{Message, Role};
use bamboo_storage::SessionIndexEntry;
use serde_json::json;

pub(super) fn normalize_contains(haystack: &str, needle: &str, case_sensitive: bool) -> bool {
    if case_sensitive {
        haystack.contains(needle)
    } else {
        haystack
            .to_ascii_lowercase()
            .contains(&needle.to_ascii_lowercase())
    }
}

pub(super) fn truncate_string(s: &str, max_chars: usize) -> String {
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

pub(super) fn map_index_entry(e: &SessionIndexEntry) -> serde_json::Value {
    json!({
        "id": e.id,
        "kind": e.kind,
        "title": e.title,
        "pinned": e.pinned,
        "parent_session_id": e.parent_session_id,
        "root_session_id": e.root_session_id,
        "spawn_depth": e.spawn_depth,
        "created_by_schedule_id": e.created_by_schedule_id,
        "schedule_run_id": e.schedule_run_id,
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

pub(super) fn extract_image_urls(msg: &Message) -> Vec<String> {
    let mut out = Vec::new();
    let Some(parts) = msg.content_parts.as_ref() else {
        return out;
    };
    for p in parts {
        if let MessagePart::ImageUrl { image_url } = p {
            out.push(image_url.url.clone());
        }
    }
    out
}

pub(super) fn role_to_str(role: &Role) -> &'static str {
    match role {
        Role::System => "system",
        Role::User => "user",
        Role::Assistant => "assistant",
        Role::Tool => "tool",
    }
}
