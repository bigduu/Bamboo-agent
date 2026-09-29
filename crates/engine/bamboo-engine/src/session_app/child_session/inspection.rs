//! Bounded, direct-parent inspection of a durable child transcript.
//!
//! Session storage currently persists the transcript in one session snapshot.
//! These views bound what enters the parent's tool result; cursors bind to the
//! selected persisted prefix so a resident reset or rewrite fails closed.

use bamboo_domain::{Message, MessagePhase, Role, Session};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{future::Future, pin::Pin};

use super::{ChildSessionError, ChildSessionPort};

const DEFAULT_MESSAGE_LIMIT: usize = 8;
const MAX_MESSAGE_LIMIT: usize = 16;
const MESSAGE_PREVIEW_BYTES: usize = 512;
const DEFAULT_CONTENT_BYTES: usize = 4096;
const MAX_CONTENT_BYTES: usize = 8192;
const MAX_CURSOR_BYTES: usize = 2048;

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "view", rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
enum InspectionCursor {
    Messages {
        child_session_id: String,
        snapshot_message_count: usize,
        snapshot_sha256: String,
        next_index: usize,
    },
    Content {
        child_session_id: String,
        result: bool,
        message_index: usize,
        message_id: String,
        content_sha256: String,
        offset: usize,
    },
    Error {
        child_session_id: String,
        error_sha256: String,
        offset: usize,
    },
}

fn invalid_cursor() -> ChildSessionError {
    ChildSessionError::InvalidArguments(
        "invalid or stale child-inspection cursor; start a new read".to_string(),
    )
}

fn decode_cursor(raw: &str) -> Result<InspectionCursor, ChildSessionError> {
    if raw.len() > MAX_CURSOR_BYTES {
        return Err(invalid_cursor());
    }
    let bytes = URL_SAFE_NO_PAD.decode(raw).map_err(|_| invalid_cursor())?;
    serde_json::from_slice(&bytes).map_err(|_| invalid_cursor())
}

fn encode_cursor(cursor: InspectionCursor) -> String {
    URL_SAFE_NO_PAD.encode(serde_json::to_vec(&cursor).expect("inspection cursor serializes"))
}

fn sha256(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// Imported transcripts can contain arbitrary message ids. Keep the public
/// lookup handle and content cursor bounded while preserving ordinary ids.
fn inspection_message_id(message: &Message) -> String {
    if message.id.len() <= 128 && !message.id.starts_with("sha256:") {
        message.id.clone()
    } else {
        format!("sha256:{}", sha256(message.id.as_bytes()))
    }
}

fn update_field(digest: &mut Sha256, bytes: &[u8]) {
    digest.update((bytes.len() as u64).to_be_bytes());
    digest.update(bytes);
}

fn role_name(role: &Role) -> &'static str {
    match role {
        Role::System => "system",
        Role::User => "user",
        Role::Assistant => "assistant",
        Role::Tool => "tool",
    }
}

fn snapshot_sha256(messages: &[Message]) -> String {
    let mut digest = Sha256::new();
    for message in messages {
        // The cursor commits to the complete persisted message. Hashing only
        // visible preview fields could accept a rewritten tool proof, image,
        // reasoning signature, or metadata as the same transcript prefix.
        let value = serde_json::to_value(message).expect("Message serializes");
        let bytes = serde_json::to_vec(&value).expect("Message Value serializes");
        update_field(&mut digest, &bytes);
    }
    hex::encode(digest.finalize())
}

fn utf8_end(content: &str, start: usize, max_bytes: usize) -> usize {
    let mut end = content.len().min(start.saturating_add(max_bytes));
    while end > start && !content.is_char_boundary(end) {
        end -= 1;
    }
    end
}

fn preview(content: &str, max_bytes: usize) -> &str {
    &content[..utf8_end(content, 0, max_bytes)]
}

fn message_page(
    child: &Session,
    raw_cursor: Option<&str>,
    requested_limit: Option<usize>,
) -> Result<Value, ChildSessionError> {
    let limit = requested_limit
        .unwrap_or(DEFAULT_MESSAGE_LIMIT)
        .clamp(1, MAX_MESSAGE_LIMIT);
    let (total, fingerprint, start) = match raw_cursor {
        None => {
            let total = child.messages.len();
            (total, snapshot_sha256(&child.messages), 0)
        }
        Some(raw) => match decode_cursor(raw)? {
            InspectionCursor::Messages {
                child_session_id,
                snapshot_message_count,
                snapshot_sha256: fingerprint,
                next_index,
            } if child_session_id == child.id
                && next_index > 0
                && next_index < snapshot_message_count
                && snapshot_message_count <= child.messages.len()
                && snapshot_sha256(&child.messages[..snapshot_message_count]) == fingerprint =>
            {
                (snapshot_message_count, fingerprint, next_index)
            }
            _ => return Err(invalid_cursor()),
        },
    };
    let end = total.min(start.saturating_add(limit));
    let messages: Vec<Value> = child.messages[start..end]
        .iter()
        .enumerate()
        .map(|(relative_index, message)| {
            let content_preview = preview(&message.content, MESSAGE_PREVIEW_BYTES);
            let tool_calls = message.tool_calls.as_deref().unwrap_or(&[]);
            let tool_call_previews: Vec<Value> = tool_calls
                .iter()
                .take(4)
                .map(|call| {
                    json!({
                        "id": preview(&call.id, 128),
                        "name": preview(&call.function.name, 128),
                        "arguments_preview": preview(&call.function.arguments, 256),
                        "arguments_utf8_bytes": call.function.arguments.len(),
                        "arguments_sha256": sha256(call.function.arguments.as_bytes()),
                    })
                })
                .collect();
            json!({
                "index": start + relative_index,
                "message_id": inspection_message_id(message),
                "role": role_name(&message.role),
                "phase": message.phase.as_ref().map(|phase| phase.as_str()),
                "created_at": message.created_at.to_rfc3339(),
                "content_preview": content_preview,
                "content_utf8_bytes": message.content.len(),
                "content_sha256": sha256(message.content.as_bytes()),
                "content_complete": content_preview.len() == message.content.len(),
                "tool_call_id": message.tool_call_id.as_deref().map(|id| preview(id, 128)),
                "tool_success": message.tool_success,
                "tool_call_count": tool_calls.len(),
                "tool_call_previews": tool_call_previews,
                "tool_calls_truncated": tool_calls.len() > 4,
                "content_parts_count": message.content_parts.as_ref().map_or(0, Vec::len),
            })
        })
        .collect();
    let next_cursor = (end < total).then(|| {
        encode_cursor(InspectionCursor::Messages {
            child_session_id: child.id.clone(),
            snapshot_message_count: total,
            snapshot_sha256: fingerprint,
            next_index: end,
        })
    });
    Ok(json!({
        "child_session_id": child.id,
        "view": "messages",
        "snapshot_message_count": total,
        "messages": messages,
        "next_cursor": next_cursor,
        "note": "Message previews are bounded. Use view=message with message_id for UTF-8 content slices.",
    }))
}

fn content_slice(
    child: &Session,
    result: bool,
    message_id: Option<&str>,
    raw_cursor: Option<&str>,
    requested_bytes: Option<usize>,
) -> Result<Value, ChildSessionError> {
    let (index, offset, expected_digest) = match raw_cursor {
        Some(raw) => match decode_cursor(raw)? {
            InspectionCursor::Content {
                child_session_id,
                result: cursor_result,
                message_index,
                message_id: cursor_message_id,
                content_sha256,
                offset,
            } if child_session_id == child.id && cursor_result == result => {
                if message_id.is_some_and(|requested| requested != cursor_message_id) {
                    return Err(invalid_cursor());
                }
                let message = child
                    .messages
                    .get(message_index)
                    .ok_or_else(invalid_cursor)?;
                if inspection_message_id(message) != cursor_message_id
                    || sha256(message.content.as_bytes()) != content_sha256
                {
                    return Err(invalid_cursor());
                }
                (message_index, offset, Some(content_sha256))
            }
            _ => return Err(invalid_cursor()),
        },
        None if result => {
            let Some((index, message)) = child
                .messages
                .iter()
                .enumerate()
                .rev()
                .find(|(_, message)| matches!(message.role, Role::Assistant))
            else {
                return Ok(json!({
                    "child_session_id": child.id,
                    "view": "result",
                    "available": false,
                    "last_run_status": child.last_run_status(),
                }));
            };
            if message.content.trim().is_empty() {
                return Ok(json!({
                    "child_session_id": child.id,
                    "view": "result",
                    "available": false,
                    "last_run_status": child.last_run_status(),
                }));
            }
            (index, 0, None)
        }
        None => {
            let id = message_id.ok_or_else(|| {
                ChildSessionError::InvalidArguments(
                    "message_id is required for view=message".to_string(),
                )
            })?;
            let index = child
                .messages
                .iter()
                .position(|message| inspection_message_id(message) == id)
                .ok_or_else(|| ChildSessionError::NotFound(id.to_string()))?;
            (index, 0, None)
        }
    };
    let message = &child.messages[index];
    let inspection_id = inspection_message_id(message);
    if result && !matches!(message.role, Role::Assistant) {
        return Err(invalid_cursor());
    }
    if offset > message.content.len()
        || (raw_cursor.is_some() && offset == message.content.len())
        || !message.content.is_char_boundary(offset)
    {
        return Err(invalid_cursor());
    }
    let digest = expected_digest.unwrap_or_else(|| sha256(message.content.as_bytes()));
    let max_bytes = requested_bytes
        .unwrap_or(DEFAULT_CONTENT_BYTES)
        .clamp(4, MAX_CONTENT_BYTES);
    let end = utf8_end(&message.content, offset, max_bytes);
    let next_cursor = (end < message.content.len()).then(|| {
        encode_cursor(InspectionCursor::Content {
            child_session_id: child.id.clone(),
            result,
            message_index: index,
            message_id: inspection_id.clone(),
            content_sha256: digest.clone(),
            offset: end,
        })
    });
    let current_run_final = result
        && child.last_run_status().as_deref() == Some("completed")
        && message.phase != Some(MessagePhase::Commentary)
        && message.tool_calls.as_ref().is_none_or(Vec::is_empty)
        && child
            .messages
            .iter()
            .rposition(|candidate| matches!(candidate.role, Role::User))
            .is_none_or(|last_user_index| index > last_user_index);
    Ok(json!({
        "child_session_id": child.id,
        "view": if result { "result" } else { "message" },
        "available": true,
        "message_id": inspection_id,
        "message_index": index,
        "role": role_name(&message.role),
        "last_run_status": if result { child.last_run_status() } else { None },
        "current_run_final": if result { Some(current_run_final) } else { None },
        "content_sha256": digest,
        "content_utf8_bytes": message.content.len(),
        "byte_start": offset,
        "byte_end": end,
        "text": &message.content[offset..end],
        "next_cursor": next_cursor,
    }))
}

fn error_slice(
    child: &Session,
    raw_cursor: Option<&str>,
    requested_bytes: Option<usize>,
) -> Result<Value, ChildSessionError> {
    let Some(error) = child.last_run_error() else {
        if raw_cursor.is_some() {
            return Err(invalid_cursor());
        }
        return Ok(json!({
            "child_session_id": child.id,
            "view": "error",
            "available": false,
            "last_run_status": child.last_run_status(),
        }));
    };
    let digest = sha256(error.as_bytes());
    let offset = match raw_cursor {
        None => 0,
        Some(raw) => match decode_cursor(raw)? {
            InspectionCursor::Error {
                child_session_id,
                error_sha256,
                offset,
            } if child_session_id == child.id && error_sha256 == digest => offset,
            _ => return Err(invalid_cursor()),
        },
    };
    if offset > error.len()
        || (raw_cursor.is_some() && offset == error.len())
        || !error.is_char_boundary(offset)
    {
        return Err(invalid_cursor());
    }
    let max_bytes = requested_bytes
        .unwrap_or(DEFAULT_CONTENT_BYTES)
        .clamp(4, MAX_CONTENT_BYTES);
    let end = utf8_end(&error, offset, max_bytes);
    let next_cursor = (end < error.len()).then(|| {
        encode_cursor(InspectionCursor::Error {
            child_session_id: child.id.clone(),
            error_sha256: digest.clone(),
            offset: end,
        })
    });
    Ok(json!({
        "child_session_id": child.id,
        "view": "error",
        "available": true,
        "last_run_status": child.last_run_status(),
        "content_sha256": digest,
        "content_utf8_bytes": error.len(),
        "byte_start": offset,
        "byte_end": end,
        "text": &error[offset..end],
        "next_cursor": next_cursor,
    }))
}

#[allow(clippy::too_many_arguments)]
pub async fn inspect_child_action(
    port: &dyn ChildSessionPort,
    parent_id: &str,
    child_session_id: &str,
    view: &str,
    cursor: Option<&str>,
    message_id: Option<&str>,
    limit: Option<usize>,
    max_bytes: Option<usize>,
) -> Result<Value, ChildSessionError> {
    // Authorization precedes cursor parsing and every transcript/result read.
    let child = port
        .load_child_for_inspection(parent_id, child_session_id)
        .await?;
    match view {
        "diagnostics"
            if cursor.is_none()
                && message_id.is_none()
                && limit.is_none()
                && max_bytes.is_none() =>
        {
            port.inspect_child_diagnostics(&child).await
        }
        "messages" if message_id.is_none() && max_bytes.is_none() => {
            message_page(&child, cursor, limit)
        }
        "message" if limit.is_none() => content_slice(&child, false, message_id, cursor, max_bytes),
        "result" if message_id.is_none() && limit.is_none() => {
            content_slice(&child, true, None, cursor, max_bytes)
        }
        "error" if message_id.is_none() && limit.is_none() => {
            error_slice(&child, cursor, max_bytes)
        }
        "diagnostics" | "messages" | "message" | "result" | "error" => {
            Err(ChildSessionError::InvalidArguments(
                "unsupported argument for the selected child-inspection view".to_string(),
            ))
        }
        _ => Err(ChildSessionError::InvalidArguments(
            "view must be overview, diagnostics, messages, message, result, or error".to_string(),
        )),
    }
}

/// Explicit required-packet reads authorize before interpreting selectors or reports.
pub fn inspect_child_report_action<'a>(
    port: &'a dyn ChildSessionPort,
    parent_id: &'a str,
    child_id: &'a str,
    view: &'a str,
    arguments: &'a Value,
) -> Pin<Box<dyn Future<Output = Result<Value, ChildSessionError>> + Send + 'a>> {
    Box::pin(inspect_child_report_action_inner(
        port, parent_id, child_id, view, arguments,
    ))
}

async fn inspect_child_report_action_inner(
    port: &dyn ChildSessionPort,
    parent_id: &str,
    child_id: &str,
    view: &str,
    arguments: &Value,
) -> Result<Value, ChildSessionError> {
    use super::result_projection::{
        decode_report, lowercase_sha256, unavailable_child_result as unavailable,
    };
    use crate::project_context::{ProjectContextResolver, SessionProjectIdentity};
    use bamboo_domain::ChildContextBinding;
    let child = port.load_child_for_parent(parent_id, child_id).await?;
    let allowed = [
        "action",
        "child_session_id",
        "view",
        "expected_child_created_at",
        "expected_assignment_sha256",
    ];
    let invalid =
        || ChildSessionError::InvalidArguments("invalid required-child result arguments".into());
    if !matches!(view, "result_binding" | "typed_result")
        || arguments
            .as_object()
            .is_none_or(|o| o.keys().any(|k| !allowed.contains(&k.as_str())))
    {
        return Err(invalid());
    }
    let selectors = if view == "typed_result" {
        let birth = arguments["expected_child_created_at"]
            .as_str()
            .ok_or_else(invalid)?;
        let birth = chrono::DateTime::parse_from_rfc3339(birth)
            .map_err(|_| invalid())?
            .with_timezone(&chrono::Utc);
        let digest = arguments["expected_assignment_sha256"]
            .as_str()
            .filter(|s| lowercase_sha256(s))
            .ok_or_else(invalid)?;
        Some((birth, digest))
    } else {
        if arguments.get("expected_child_created_at").is_some()
            || arguments.get("expected_assignment_sha256").is_some()
        {
            return Err(invalid());
        }
        None
    };
    let parent = port.load_parent_session(parent_id).await?;
    let binding = match ChildContextBinding::from_session(&child) {
        Ok(Some(binding)) => binding,
        Ok(None) => return Ok(unavailable(view, "typed_result_unsupported")),
        Err(_) => return Ok(unavailable(view, "result_binding_invalid")),
    };
    if child
        .metadata
        .get("lifecycle")
        .is_some_and(|s| s == "resident")
    {
        return Ok(unavailable(view, "typed_result_unsupported"));
    }
    if selectors.is_some_and(|(birth, digest)| {
        birth != child.created_at || digest != binding.assignment_sha256
    }) {
        return Ok(unavailable(view, "stale_result_selector"));
    }
    let parent_project = ProjectContextResolver::session_project_identity(&parent);
    let child_project = ProjectContextResolver::session_project_identity(&child);
    if binding.payload.child_session_id != child.id
        || child.root_session_id != parent.root_session_id
        || parent.spawn_depth.checked_add(1) != Some(child.spawn_depth)
        || matches!(parent_project, SessionProjectIdentity::Invalid { .. })
        || matches!(child_project, SessionProjectIdentity::Invalid { .. })
        || parent_project != child_project
        || binding.validate_parent_sources(&parent).is_err()
    {
        return Ok(unavailable(view, "stale_parent_context"));
    }
    if view == "result_binding" {
        return Ok(
            json!({"view":view,"version":1,"available":true,"provenance_kind":"durable_snapshot",
            "child_created_at":child.created_at,"assignment_sha256":binding.assignment_sha256}),
        );
    }
    let Some((index, message)) = child
        .messages
        .iter()
        .enumerate()
        .rev()
        .find(|(_, m)| m.role == Role::Assistant)
    else {
        return Ok(unavailable(view, "report_absent"));
    };
    if child.last_run_status().as_deref() != Some("completed")
        || message.phase == Some(MessagePhase::Commentary)
        || message.tool_calls.as_ref().is_some_and(|v| !v.is_empty())
        || message
            .content_parts
            .as_ref()
            .is_some_and(|v| !v.is_empty())
        || message.content.trim().is_empty()
        || message.compressed
        || message.compressed_by_event_id.is_some()
        || message.compression_level != 0
        || message.content.starts_with("[post-compaction-recovery]")
        || child
            .messages
            .iter()
            .rposition(|m| m.role == Role::User)
            .is_some_and(|last| index <= last)
    {
        return Ok(unavailable(view, "report_not_current_final"));
    }
    let report = match decode_report(&message.content) {
        Ok(report) => report,
        Err(reason) => return Ok(unavailable(view, reason)),
    };
    let project_id = match parent_project {
        SessionProjectIdentity::Assigned(id) => Some(id.to_string()),
        _ => None,
    };
    Ok(
        json!({"view":view,"version":1,"available":true,"child_report":report,"host_observation":{
        "kind":"durable_snapshot","parent_session_id":parent.id,"parent_created_at":parent.created_at,
        "child_session_id":child.id,"child_created_at":child.created_at,"root_session_id":child.root_session_id,
        "spawn_depth":child.spawn_depth,"current_project_id":project_id,"assignment_sha256":binding.assignment_sha256,
        "source_contents_match":true,"required_source_count":binding.payload.sources.iter().filter(|s|s.required).count(),
        "optional_source_count":binding.payload.sources.iter().filter(|s|!s.required).count(),
        "message_id":inspection_message_id(message),"content_sha256":sha256(message.content.as_bytes()),
        "last_run_status":"completed","current_run_final_snapshot":true}}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn child() -> Session {
        Session::new_child("child", "root", "model", "Child")
    }

    #[test]
    fn long_utf8_result_is_recoverable_in_bounded_slices_after_round_trip() {
        let mut child = child();
        let answer = "汉字🙂résumé\n".repeat(3000);
        child.add_message(Message::assistant(answer.clone(), None));
        child.set_last_run_status("completed");
        let mut cursor: Option<String> = None;
        let mut reassembled = String::new();
        let mut reads = 0;
        loop {
            // A fresh deserialization models a storage read after server restart.
            let reloaded: Session =
                serde_json::from_slice(&serde_json::to_vec(&child).unwrap()).unwrap();
            let page = content_slice(&reloaded, true, None, cursor.as_deref(), Some(113))
                .expect("result slice");
            let text = page["text"].as_str().unwrap();
            assert!(text.len() <= 113);
            assert_eq!(page["content_utf8_bytes"], answer.len());
            reassembled.push_str(text);
            reads += 1;
            cursor = page["next_cursor"].as_str().map(str::to_string);
            if cursor.is_none() {
                break;
            }
        }
        assert!(reads > 100);
        assert_eq!(reassembled, answer);
    }

    #[test]
    fn message_cursor_excludes_appends_and_rejects_rewritten_prefix() {
        let mut child = child();
        for index in 0..20 {
            child.add_message(Message::user(format!("message {index}")));
        }
        let first = message_page(&child, None, Some(3)).unwrap();
        assert_eq!(first["messages"].as_array().unwrap().len(), 3);
        assert_eq!(first["snapshot_message_count"], 20);
        let cursor = first["next_cursor"].as_str().unwrap();
        child.add_message(Message::user("appended after page one"));
        let second = message_page(&child, Some(cursor), Some(3)).unwrap();
        assert_eq!(second["snapshot_message_count"], 20);
        assert_eq!(second["messages"][0]["index"], 3);
        child.messages[1].content = "rewritten".to_string();
        assert!(matches!(
            message_page(&child, Some(cursor), Some(3)),
            Err(ChildSessionError::InvalidArguments(_))
        ));
    }

    #[test]
    fn message_cursor_rejects_rewritten_non_preview_fields() {
        let mut child = child();
        for index in 0..4 {
            child.add_message(Message::user(format!("message {index}")));
        }
        let first = message_page(&child, None, Some(1)).unwrap();
        let cursor = first["next_cursor"].as_str().unwrap();
        child.messages[0].reasoning = Some("changed private reasoning".into());
        assert!(matches!(
            message_page(&child, Some(cursor), Some(1)),
            Err(ChildSessionError::InvalidArguments(_))
        ));
    }

    #[test]
    fn message_view_reads_large_content_and_rejects_changed_content() {
        let mut child = child();
        let content = "🙂".repeat(4000);
        child.add_message(Message::user(content.clone()));
        let message_id = child.messages[0].id.clone();
        let first = content_slice(&child, false, Some(&message_id), None, Some(99)).unwrap();
        assert!(first["text"].as_str().unwrap().len() <= 99);
        let cursor = first["next_cursor"].as_str().unwrap();
        let next = content_slice(&child, false, Some(&message_id), Some(cursor), Some(99)).unwrap();
        assert_eq!(next["byte_start"], first["byte_end"]);
        child.messages[0].content = "changed".to_string();
        assert!(matches!(
            content_slice(&child, false, Some(&message_id), Some(cursor), Some(99)),
            Err(ChildSessionError::InvalidArguments(_))
        ));
    }

    #[test]
    fn imported_long_message_id_uses_a_bounded_retrieval_handle() {
        let mut child = child();
        let mut message = Message::user("evidence".repeat(1000));
        message.id = "untrusted-id".repeat(1000);
        child.add_message(message);
        let page = message_page(&child, None, Some(1)).unwrap();
        let handle = page["messages"][0]["message_id"].as_str().unwrap();
        assert!(handle.starts_with("sha256:"));
        assert!(handle.len() < 128);
        assert!(!page.to_string().contains(&child.messages[0].id));
        let slice = content_slice(&child, false, Some(handle), None, Some(8)).unwrap();
        assert_eq!(slice["message_id"], handle);
        let cursor = slice["next_cursor"].as_str().unwrap();
        assert!(cursor.len() < MAX_CURSOR_BYTES);
        assert!(content_slice(&child, false, Some(handle), Some(cursor), Some(8)).is_ok());
    }

    #[test]
    fn empty_message_content_is_readable() {
        let mut child = child();
        child.add_message(Message::user(""));
        let page = content_slice(&child, false, Some(&child.messages[0].id), None, None).unwrap();
        assert_eq!(page["text"], "");
        assert!(page["next_cursor"].is_null());
    }

    #[test]
    fn message_page_summarizes_tool_calls_without_unbounded_arguments() {
        let mut child = child();
        let arguments = "文件🙂".repeat(3000);
        child.add_message(Message::assistant(
            "",
            Some(vec![bamboo_domain::ToolCall {
                id: "call-1".into(),
                tool_type: "function".into(),
                function: bamboo_domain::FunctionCall {
                    name: "Read".into(),
                    arguments: arguments.clone(),
                },
            }]),
        ));
        let page = message_page(&child, None, Some(1)).unwrap();
        let item = &page["messages"][0];
        assert_eq!(item["tool_call_count"], 1);
        assert_eq!(item["tool_call_previews"][0]["name"], "Read");
        assert!(
            item["tool_call_previews"][0]["arguments_preview"]
                .as_str()
                .unwrap()
                .len()
                <= 256
        );
        assert_eq!(
            item["tool_call_previews"][0]["arguments_utf8_bytes"],
            arguments.len()
        );
        assert!(!page.to_string().contains(&arguments));
    }

    #[test]
    fn long_error_is_recoverable_and_cursor_detects_replacement() {
        let mut child = child();
        child
            .metadata
            .insert("last_run_error".into(), "失败🙂".repeat(2500));
        let first = error_slice(&child, None, Some(101)).unwrap();
        assert_eq!(first["view"], "error");
        let cursor = first["next_cursor"].as_str().unwrap();
        let second = error_slice(&child, Some(cursor), Some(101)).unwrap();
        assert_eq!(second["byte_start"], first["byte_end"]);
        child
            .metadata
            .insert("last_run_error".into(), "replacement".into());
        assert!(matches!(
            error_slice(&child, Some(cursor), Some(101)),
            Err(ChildSessionError::InvalidArguments(_))
        ));
    }
}
