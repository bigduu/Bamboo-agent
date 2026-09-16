//! Bounded, self-scoped reads over the authoritative current Session.
//!
//! The opaque cursor carries continuation state only. Session authority and
//! the live current-request boundary are re-derived from `ToolCtx` on every
//! invocation.

use std::collections::{HashMap, HashSet};

use bamboo_agent_core::tools::{ToolError, ToolResult};
use bamboo_agent_core::{Message, MessagePart, Role, Session};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use super::args::HistoryReadDirection;
use super::helpers::role_to_str;
use super::SessionInspectorTool;

const HISTORY_READ_DEFAULT_TURNS: usize = 10;
const HISTORY_READ_MAX_TURNS: usize = 20;
const HISTORY_READ_DEFAULT_CHARS: usize = 12_000;
const HISTORY_READ_MAX_CHARS: usize = 20_000;
const HISTORY_READ_MAX_MESSAGES: usize = 100;
const HISTORY_CURSOR_VERSION: u8 = 1;
const HISTORY_CURSOR_PREFIX: &str = "bsh1";
const HISTORY_CURSOR_MAX_CHARS: usize = 4096;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct HistoryReadCursor {
    version: u8,
    session_binding: String,
    current_request_message_id: Option<String>,
    before_message_index: usize,
    snapshot_digest: String,
    direction: HistoryReadDirection,
    archived_only: bool,
    next_turn_position: usize,
}

#[derive(Debug, Clone, Copy)]
struct HistoryMessageRef<'a> {
    index: usize,
    message: &'a Message,
}

#[derive(Debug)]
struct HistoryTurn<'a> {
    messages: Vec<HistoryMessageRef<'a>>,
    request_message_excluded: bool,
}

impl HistoryTurn<'_> {
    fn first_index(&self) -> usize {
        self.messages
            .first()
            .map(|message| message.index)
            .unwrap_or_default()
    }

    fn last_index(&self) -> usize {
        self.messages
            .last()
            .map(|message| message.index)
            .unwrap_or_default()
    }

    fn char_count(&self) -> usize {
        self.messages
            .iter()
            .map(|message| history_message_char_count(message.message))
            .sum()
    }

    fn contains_archived(&self) -> bool {
        self.messages
            .iter()
            .any(|message| message.message.compressed)
    }

    fn to_json(&self) -> Value {
        let messages = self
            .messages
            .iter()
            .map(|message| history_message_json(*message))
            .collect::<Vec<_>>();
        json!({
            "turn_id": self.messages.first().map(|message| message.message.id.as_str()),
            "first_message_index": self.first_index(),
            "last_message_index": self.last_index(),
            "message_count": self.messages.len(),
            "char_count": self.char_count(),
            "contains_archived": self.contains_archived(),
            "current_request_message_excluded": self.request_message_excluded,
            "messages": messages,
        })
    }
}

#[derive(Debug)]
struct HistoryProjection<'a> {
    turns: Vec<HistoryTurn<'a>>,
    excluded_incomplete_protocol_messages: usize,
}

/// Locate the current tool call and the User request that owns it.
///
/// Standalone/direct tool execution has no persisted call message. In that
/// path all stored messages are treated as completed history, matching the
/// existing `search_current` behavior.
pub(super) fn current_history_boundary(
    session: &Session,
    tool_call_id: &str,
) -> (usize, Option<usize>) {
    let Some(call_index) = session.messages.iter().rposition(|message| {
        message
            .tool_calls
            .as_ref()
            .is_some_and(|calls| calls.iter().any(|call| call.id == tool_call_id))
    }) else {
        return (session.messages.len(), None);
    };

    let current_request_index = session.messages[..call_index]
        .iter()
        .rposition(|message| message.role == Role::User);
    (call_index, current_request_index)
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn handle_read_current(
    tool: &SessionInspectorTool,
    caller_session_id: &str,
    current_tool_call_id: &str,
    cursor: Option<String>,
    direction: Option<HistoryReadDirection>,
    limit: Option<usize>,
    max_chars: Option<usize>,
    archived_only: Option<bool>,
) -> Result<ToolResult, ToolError> {
    let decoded_cursor = cursor.as_deref().map(decode_cursor).transpose()?;
    let direction = direction
        .or_else(|| decoded_cursor.as_ref().map(|cursor| cursor.direction))
        .unwrap_or(HistoryReadDirection::Backward);
    let archived_only = archived_only
        .or_else(|| decoded_cursor.as_ref().map(|cursor| cursor.archived_only))
        .unwrap_or(false);
    let limit = bounded_value(
        "limit",
        limit,
        HISTORY_READ_DEFAULT_TURNS,
        1,
        HISTORY_READ_MAX_TURNS,
    )?;
    let max_chars = bounded_value(
        "max_chars",
        max_chars,
        HISTORY_READ_DEFAULT_CHARS,
        1,
        HISTORY_READ_MAX_CHARS,
    )?;

    let session = tool.load_session(caller_session_id).await?;
    let (before_message_index, current_request_index, snapshot_digest) = resolve_snapshot(
        &session,
        caller_session_id,
        current_tool_call_id,
        decoded_cursor.as_ref(),
        direction,
        archived_only,
    )?;
    let projection =
        build_history_projection(&session, before_message_index, current_request_index);
    let turns = projection
        .turns
        .iter()
        .filter(|turn| !archived_only || turn.contains_archived())
        .collect::<Vec<_>>();

    let mut position = decoded_cursor
        .as_ref()
        .map(|cursor| cursor.next_turn_position)
        .unwrap_or_else(|| match direction {
            HistoryReadDirection::Backward => turns.len(),
            HistoryReadDirection::Forward => 0,
        });
    if position > turns.len() {
        return Err(invalid_cursor(
            "turn position is outside the current snapshot",
        ));
    }

    let mut selected = Vec::new();
    let mut returned_chars = 0usize;
    let mut returned_messages = 0usize;
    let mut skipped_turns = 0usize;
    let mut truncation_reason: Option<&'static str> = None;
    let mut oversized_turn: Option<Value> = None;

    loop {
        if selected.len() == limit {
            if has_more_turns(direction, position, turns.len()) {
                truncation_reason = Some("turn_limit");
            }
            break;
        }

        let Some(turn_index) = next_turn_index(direction, position, turns.len()) else {
            break;
        };
        let turn = turns[turn_index];
        let turn_chars = turn.char_count();
        let turn_messages = turn.messages.len();

        if turn_chars > max_chars || turn_messages > HISTORY_READ_MAX_MESSAGES {
            position = advance_position(direction, turn_index);
            skipped_turns += 1;
            truncation_reason = Some(if turn_chars > max_chars {
                "turn_exceeds_max_chars"
            } else {
                "turn_exceeds_max_messages"
            });
            oversized_turn = Some(json!({
                "turn_id": turn.messages.first().map(|message| message.message.id.as_str()),
                "first_message_index": turn.first_index(),
                "last_message_index": turn.last_index(),
                "required_chars": turn_chars,
                "required_messages": turn_messages,
            }));
            break;
        }

        if returned_chars.saturating_add(turn_chars) > max_chars {
            truncation_reason = Some("max_chars");
            break;
        }
        if returned_messages.saturating_add(turn_messages) > HISTORY_READ_MAX_MESSAGES {
            truncation_reason = Some("max_messages");
            break;
        }

        selected.push(turn);
        returned_chars += turn_chars;
        returned_messages += turn_messages;
        position = advance_position(direction, turn_index);
    }

    let has_more = has_more_turns(direction, position, turns.len());
    let next_cursor = if has_more {
        Some(encode_cursor(&HistoryReadCursor {
            version: HISTORY_CURSOR_VERSION,
            session_binding: session_binding(caller_session_id),
            current_request_message_id: current_request_index
                .and_then(|index| session.messages.get(index))
                .map(|message| message.id.clone()),
            before_message_index,
            snapshot_digest,
            direction,
            archived_only,
            next_turn_position: position,
        })?)
    } else {
        None
    };
    if has_more && truncation_reason.is_none() {
        truncation_reason = Some("turn_limit");
    }
    let remaining_turns = match direction {
        HistoryReadDirection::Backward => position,
        HistoryReadDirection::Forward => turns.len().saturating_sub(position),
    };
    let complete = !has_more && truncation_reason.is_none();
    let truncated = has_more || truncation_reason.is_some();
    let returned_turns = selected
        .into_iter()
        .map(HistoryTurn::to_json)
        .collect::<Vec<_>>();

    Ok(history_result(json!({
        "contract_version": 1,
        "action": "read_current",
        "session_id": caller_session_id,
        "read_before_message_index": before_message_index,
        "current_request_message_id": current_request_index
            .and_then(|index| session.messages.get(index))
            .map(|message| message.id.as_str()),
        "direction": direction.as_str(),
        "archived_only": archived_only,
        "limit": limit,
        "max_chars": max_chars,
        "max_messages": HISTORY_READ_MAX_MESSAGES,
        "returned_turn_count": returned_turns.len(),
        "returned_message_count": returned_messages,
        "returned_char_count": returned_chars,
        "remaining_turn_count": remaining_turns,
        "skipped_turn_count": skipped_turns,
        "excluded_incomplete_protocol_message_count": projection.excluded_incomplete_protocol_messages,
        "complete": complete,
        "truncated": truncated,
        "truncation_reason": truncation_reason,
        "oversized_turn": oversized_turn,
        "next_cursor": next_cursor,
        "turns": returned_turns,
        "note": "Messages are exact bounded content from the authoritative current Session. System context, the current User request, generated history-retrieval artifacts, provider reasoning/signatures, and image bytes are not replayed. Tool calls and results are returned only as complete protocol chains."
    })))
}

fn resolve_snapshot(
    session: &Session,
    caller_session_id: &str,
    current_tool_call_id: &str,
    cursor: Option<&HistoryReadCursor>,
    direction: HistoryReadDirection,
    archived_only: bool,
) -> Result<(usize, Option<usize>, String), ToolError> {
    let (live_boundary, live_request_index) =
        current_history_boundary(session, current_tool_call_id);
    let live_request_id = live_request_index
        .and_then(|index| session.messages.get(index))
        .map(|message| message.id.as_str());

    let Some(cursor) = cursor else {
        return Ok((
            live_boundary,
            live_request_index,
            snapshot_digest(session, live_boundary)?,
        ));
    };

    if cursor.version != HISTORY_CURSOR_VERSION {
        return Err(invalid_cursor("unsupported cursor version"));
    }
    if cursor.session_binding != session_binding(caller_session_id) {
        return Err(invalid_cursor("cursor belongs to another Session"));
    }
    if cursor.direction != direction || cursor.archived_only != archived_only {
        return Err(invalid_cursor(
            "cursor direction or archived_only filter does not match this request",
        ));
    }
    if cursor.current_request_message_id.as_deref() != live_request_id {
        return Err(invalid_cursor("cursor belongs to another user request"));
    }
    if cursor.before_message_index > live_boundary
        || cursor.before_message_index > session.messages.len()
    {
        return Err(invalid_cursor("cursor boundary is no longer valid"));
    }
    let digest = snapshot_digest(session, cursor.before_message_index)?;
    if digest != cursor.snapshot_digest {
        return Err(invalid_cursor("history snapshot changed"));
    }

    let generated_artifacts =
        bamboo_storage::search_index::session_history_search_artifact_ids(session);
    if session.messages[cursor.before_message_index..live_boundary]
        .iter()
        .any(|message| !generated_artifacts.contains(&message.id))
    {
        return Err(invalid_cursor(
            "non-history messages were appended after the cursor snapshot",
        ));
    }

    let request_index = match cursor.current_request_message_id.as_deref() {
        Some(request_id) => session.messages[..cursor.before_message_index]
            .iter()
            .rposition(|message| message.id == request_id && message.role == Role::User)
            .ok_or_else(|| invalid_cursor("current request is absent from the cursor snapshot"))?
            .into(),
        None => None,
    };
    Ok((cursor.before_message_index, request_index, digest))
}

fn build_history_projection<'a>(
    session: &'a Session,
    before_message_index: usize,
    current_request_index: Option<usize>,
) -> HistoryProjection<'a> {
    let boundary = before_message_index.min(session.messages.len());
    let generated_artifacts =
        bamboo_storage::search_index::session_history_search_artifact_ids(session);
    let mut turn_marker = vec![None; boundary];
    let mut current_user = None;
    for (index, message) in session.messages[..boundary].iter().enumerate() {
        if message.role == Role::User {
            current_user = Some(index);
        }
        turn_marker[index] = current_user;
    }

    let preliminary_eligible = session.messages[..boundary]
        .iter()
        .enumerate()
        .map(|(index, message)| {
            message.role != Role::System
                && Some(index) != current_request_index
                && !generated_artifacts.contains(&message.id)
        })
        .collect::<Vec<_>>();

    let mut assistants_by_call: HashMap<&str, Vec<usize>> = HashMap::new();
    let mut results_by_call: HashMap<&str, Vec<usize>> = HashMap::new();
    for (index, message) in session.messages[..boundary].iter().enumerate() {
        if !preliminary_eligible[index] {
            continue;
        }
        if message.role == Role::Assistant {
            if let Some(calls) = message.tool_calls.as_ref() {
                for call in calls {
                    assistants_by_call
                        .entry(call.id.as_str())
                        .or_default()
                        .push(index);
                }
            }
        }
        if message.role == Role::Tool {
            if let Some(call_id) = message.tool_call_id.as_deref() {
                results_by_call.entry(call_id).or_default().push(index);
            }
        }
    }

    let mut invalid_protocol_messages = HashSet::new();
    for (assistant_index, message) in session.messages[..boundary].iter().enumerate() {
        if !preliminary_eligible[assistant_index] || message.role != Role::Assistant {
            continue;
        }
        let Some(calls) = message
            .tool_calls
            .as_ref()
            .filter(|calls| !calls.is_empty())
        else {
            continue;
        };
        let chain_complete = calls.iter().all(|call| {
            let assistants = assistants_by_call
                .get(call.id.as_str())
                .map(Vec::as_slice)
                .unwrap_or_default();
            let results = results_by_call
                .get(call.id.as_str())
                .map(Vec::as_slice)
                .unwrap_or_default();
            assistants == [assistant_index]
                && results.len() == 1
                && results[0] > assistant_index
                && turn_marker[results[0]] == turn_marker[assistant_index]
        });
        if !chain_complete {
            invalid_protocol_messages.insert(assistant_index);
            for call in calls {
                if let Some(results) = results_by_call.get(call.id.as_str()) {
                    invalid_protocol_messages.extend(results.iter().copied());
                }
            }
        }
    }
    for (result_index, message) in session.messages[..boundary].iter().enumerate() {
        if !preliminary_eligible[result_index] || message.role != Role::Tool {
            continue;
        }
        let Some(call_id) = message.tool_call_id.as_deref() else {
            invalid_protocol_messages.insert(result_index);
            continue;
        };
        let assistants = assistants_by_call
            .get(call_id)
            .map(Vec::as_slice)
            .unwrap_or_default();
        if assistants.len() != 1
            || invalid_protocol_messages.contains(&assistants[0])
            || results_by_call.get(call_id).map(Vec::len) != Some(1)
            || assistants[0] >= result_index
            || turn_marker[assistants[0]] != turn_marker[result_index]
        {
            invalid_protocol_messages.insert(result_index);
        }
    }

    let mut turns = Vec::new();
    let mut current_turn: Option<HistoryTurn<'a>> = None;
    for (index, message) in session.messages[..boundary].iter().enumerate() {
        if message.role == Role::System {
            continue;
        }
        if message.role == Role::User {
            push_nonempty_turn(&mut turns, current_turn.take());
            current_turn = Some(HistoryTurn {
                messages: Vec::new(),
                request_message_excluded: Some(index) == current_request_index,
            });
        }
        if !preliminary_eligible[index] || invalid_protocol_messages.contains(&index) {
            continue;
        }
        current_turn
            .get_or_insert_with(|| HistoryTurn {
                messages: Vec::new(),
                request_message_excluded: false,
            })
            .messages
            .push(HistoryMessageRef { index, message });
    }
    push_nonempty_turn(&mut turns, current_turn.take());

    HistoryProjection {
        turns,
        excluded_incomplete_protocol_messages: invalid_protocol_messages.len(),
    }
}

fn push_nonempty_turn<'a>(turns: &mut Vec<HistoryTurn<'a>>, turn: Option<HistoryTurn<'a>>) {
    if let Some(turn) = turn.filter(|turn| !turn.messages.is_empty()) {
        turns.push(turn);
    }
}

fn history_message_char_count(message: &Message) -> usize {
    let mut count = message.content.chars().count();
    if let Some(tool_call_id) = message.tool_call_id.as_deref() {
        count = count.saturating_add(tool_call_id.chars().count());
    }
    if let Some(calls) = message.tool_calls.as_ref() {
        for call in calls {
            count = count
                .saturating_add(call.id.chars().count())
                .saturating_add(call.tool_type.chars().count())
                .saturating_add(call.function.name.chars().count())
                .saturating_add(call.function.arguments.chars().count());
        }
    }
    count
}

fn history_message_json(message: HistoryMessageRef<'_>) -> Value {
    let image_count = message
        .message
        .content_parts
        .as_ref()
        .map(|parts| {
            parts
                .iter()
                .filter(|part| matches!(part, MessagePart::ImageUrl { .. }))
                .count()
        })
        .unwrap_or_default();
    json!({
        "index": message.index,
        "id": message.message.id,
        "role": role_to_str(&message.message.role),
        "created_at": message.message.created_at,
        "archived": message.message.compressed,
        "content_len": message.message.content.chars().count(),
        "content": message.message.content,
        "phase": message.message.phase.as_ref().map(|phase| phase.as_str()),
        "tool_calls": message.message.tool_calls,
        "tool_call_id": message.message.tool_call_id,
        "tool_success": message.message.tool_success,
        "has_images": image_count > 0,
        "image_count": image_count,
    })
}

fn bounded_value(
    name: &str,
    value: Option<usize>,
    default: usize,
    minimum: usize,
    maximum: usize,
) -> Result<usize, ToolError> {
    let value = value.unwrap_or(default);
    if !(minimum..=maximum).contains(&value) {
        return Err(ToolError::InvalidArguments(format!(
            "{name} must be between {minimum} and {maximum}"
        )));
    }
    Ok(value)
}

fn next_turn_index(
    direction: HistoryReadDirection,
    position: usize,
    turn_count: usize,
) -> Option<usize> {
    match direction {
        HistoryReadDirection::Backward => position.checked_sub(1),
        HistoryReadDirection::Forward => (position < turn_count).then_some(position),
    }
}

fn advance_position(direction: HistoryReadDirection, turn_index: usize) -> usize {
    match direction {
        HistoryReadDirection::Backward => turn_index,
        HistoryReadDirection::Forward => turn_index.saturating_add(1),
    }
}

fn has_more_turns(direction: HistoryReadDirection, position: usize, turn_count: usize) -> bool {
    match direction {
        HistoryReadDirection::Backward => position > 0,
        HistoryReadDirection::Forward => position < turn_count,
    }
}

fn session_binding(session_id: &str) -> String {
    sha256_hex(session_id.as_bytes())
}

fn snapshot_digest(session: &Session, boundary: usize) -> Result<String, ToolError> {
    let messages = session
        .messages
        .get(..boundary)
        .ok_or_else(|| invalid_cursor("snapshot boundary is outside the Session"))?;
    let encoded = serde_json::to_vec(messages).map_err(|error| {
        ToolError::Execution(format!(
            "failed to encode current-Session history snapshot: {error}"
        ))
    })?;
    Ok(sha256_hex(&encoded))
}

fn sha256_hex(value: &[u8]) -> String {
    hex::encode(Sha256::digest(value))
}

fn encode_cursor(cursor: &HistoryReadCursor) -> Result<String, ToolError> {
    let payload = serde_json::to_vec(cursor).map_err(|error| {
        ToolError::Execution(format!("failed to encode history cursor: {error}"))
    })?;
    let checksum = Sha256::digest(&payload);
    Ok(format!(
        "{HISTORY_CURSOR_PREFIX}.{}.{}",
        hex::encode(payload),
        hex::encode(&checksum[..8])
    ))
}

fn decode_cursor(cursor: &str) -> Result<HistoryReadCursor, ToolError> {
    if cursor.is_empty() || cursor.chars().count() > HISTORY_CURSOR_MAX_CHARS {
        return Err(invalid_cursor("cursor length is invalid"));
    }
    let mut parts = cursor.split('.');
    let prefix = parts.next();
    let payload_hex = parts.next();
    let checksum_hex = parts.next();
    if prefix != Some(HISTORY_CURSOR_PREFIX)
        || payload_hex.is_none()
        || checksum_hex.is_none()
        || parts.next().is_some()
    {
        return Err(invalid_cursor("cursor format is invalid"));
    }
    let payload = hex::decode(payload_hex.unwrap())
        .map_err(|_| invalid_cursor("cursor payload is invalid"))?;
    let checksum = hex::decode(checksum_hex.unwrap())
        .map_err(|_| invalid_cursor("cursor checksum is invalid"))?;
    let expected = Sha256::digest(&payload);
    if checksum.as_slice() != &expected[..8] {
        return Err(invalid_cursor("cursor checksum does not match"));
    }
    let cursor: HistoryReadCursor = serde_json::from_slice(&payload)
        .map_err(|_| invalid_cursor("cursor payload is invalid"))?;
    if cursor.version != HISTORY_CURSOR_VERSION {
        return Err(invalid_cursor("unsupported cursor version"));
    }
    Ok(cursor)
}

fn invalid_cursor(reason: &str) -> ToolError {
    ToolError::InvalidArguments(format!("invalid read_current cursor: {reason}"))
}

fn history_result(result: Value) -> ToolResult {
    ToolResult {
        success: true,
        result: result.to_string(),
        display_preference: Some("Collapsible".to_string()),
        images: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_round_trip_rejects_tampering_and_unknown_fields() {
        let cursor = HistoryReadCursor {
            version: HISTORY_CURSOR_VERSION,
            session_binding: "binding".to_string(),
            current_request_message_id: Some("request".to_string()),
            before_message_index: 4,
            snapshot_digest: "digest".to_string(),
            direction: HistoryReadDirection::Backward,
            archived_only: true,
            next_turn_position: 2,
        };
        let encoded = encode_cursor(&cursor).expect("encode cursor");
        let decoded = decode_cursor(&encoded).expect("decode cursor");
        assert_eq!(decoded.next_turn_position, 2);
        assert_eq!(decoded.direction, HistoryReadDirection::Backward);

        let unsupported = HistoryReadCursor {
            version: HISTORY_CURSOR_VERSION + 1,
            ..cursor.clone()
        };
        assert!(matches!(
            decode_cursor(&encode_cursor(&unsupported).expect("encode unsupported cursor")),
            Err(ToolError::InvalidArguments(message)) if message.contains("unsupported cursor version")
        ));

        let mut unknown = serde_json::to_value(&cursor).expect("serialize cursor");
        unknown
            .as_object_mut()
            .expect("cursor object")
            .insert("unknown".to_string(), json!(true));
        let payload = serde_json::to_vec(&unknown).expect("encode unknown-field cursor");
        let checksum = Sha256::digest(&payload);
        let unknown = format!(
            "{HISTORY_CURSOR_PREFIX}.{}.{}",
            hex::encode(payload),
            hex::encode(&checksum[..8])
        );
        assert!(matches!(
            decode_cursor(&unknown),
            Err(ToolError::InvalidArguments(_))
        ));

        let mut tampered = encoded.into_bytes();
        let payload_index = HISTORY_CURSOR_PREFIX.len() + 2;
        tampered[payload_index] = if tampered[payload_index] == b'a' {
            b'b'
        } else {
            b'a'
        };
        let tampered = String::from_utf8(tampered).unwrap();
        assert!(matches!(
            decode_cursor(&tampered),
            Err(ToolError::InvalidArguments(_))
        ));
    }
}
