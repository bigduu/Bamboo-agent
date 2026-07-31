use bytes::Bytes;
use serde::Serialize;

use super::super::super::types::{
    ResponsesCreateResponse, ResponsesFunctionCallOutputItem, ResponsesMessageOutputItem,
    ResponsesOutputItem, ResponsesStreamEvent, ResponsesTextContent,
};

/// Assign the next response-scoped sequence number immediately before an event
/// is serialized. Keeping this at the final emission boundary makes every
/// event family share one monotonic counter.
pub(super) fn sequence_event<T>(
    mut event: ResponsesStreamEvent<T>,
    next_sequence_number: &mut u64,
) -> ResponsesStreamEvent<T> {
    event.sequence_number = *next_sequence_number;
    *next_sequence_number = next_sequence_number.saturating_add(1);
    event
}

pub(super) fn created_event(
    response_id: String,
    model: String,
    created_at: u64,
) -> ResponsesStreamEvent<ResponsesCreateResponse> {
    ResponsesStreamEvent {
        event_type: "response.created".to_string(),
        response: Some(ResponsesCreateResponse {
            id: response_id,
            object: "response".to_string(),
            created_at,
            model,
            status: "in_progress".to_string(),
            output: Vec::new(),
            usage: None,
        }),
        ..Default::default()
    }
}

pub(super) fn output_text_delta_event(
    response_id: &str,
    item_id: &str,
    delta: String,
) -> ResponsesStreamEvent<ResponsesCreateResponse> {
    ResponsesStreamEvent {
        event_type: "response.output_text.delta".to_string(),
        response_id: Some(response_id.to_string()),
        item_id: Some(item_id.to_string()),
        output_index: Some(0),
        content_index: Some(0),
        delta: Some(delta),
        ..Default::default()
    }
}

pub(super) fn completed_event(
    response: ResponsesCreateResponse,
) -> ResponsesStreamEvent<ResponsesCreateResponse> {
    ResponsesStreamEvent {
        event_type: "response.completed".to_string(),
        response: Some(response),
        ..Default::default()
    }
}

/// A bare event skeleton for the per-item events below.
fn item_event(
    event_type: &str,
    response_id: &str,
    item_id: &str,
    output_index: u32,
) -> ResponsesStreamEvent<ResponsesCreateResponse> {
    ResponsesStreamEvent {
        event_type: event_type.to_string(),
        response_id: Some(response_id.to_string()),
        item_id: Some(item_id.to_string()),
        output_index: Some(output_index),
        ..Default::default()
    }
}

/// `response.output_item.added` for the assistant message item at
/// output_index 0 (in_progress, empty content) — emitted right after
/// `response.created`, so clients that assemble output from output_item
/// events (Codex) see the message item, not just its text deltas (#525).
pub(super) fn message_item_added_event(
    response_id: &str,
    message_id: &str,
) -> ResponsesStreamEvent<ResponsesCreateResponse> {
    let mut event = item_event("response.output_item.added", response_id, message_id, 0);
    event.item = Some(ResponsesOutputItem::Message(ResponsesMessageOutputItem {
        id: message_id.to_string(),
        item_type: "message".to_string(),
        role: "assistant".to_string(),
        content: Vec::new(),
        status: Some("in_progress".to_string()),
    }));
    event
}

pub(super) fn message_content_part_added_event(
    response_id: &str,
    message_id: &str,
) -> ResponsesStreamEvent<ResponsesCreateResponse> {
    let mut event = item_event("response.content_part.added", response_id, message_id, 0);
    event.content_index = Some(0);
    event.part = Some(ResponsesTextContent {
        content_type: "output_text".to_string(),
        text: String::new(),
        annotations: Vec::new(),
    });
    event
}

/// Complete the assistant text content before marking its output item done.
pub(super) fn message_item_done_events(
    response_id: &str,
    item: &ResponsesMessageOutputItem,
) -> Vec<ResponsesStreamEvent<ResponsesCreateResponse>> {
    let content = item
        .content
        .first()
        .cloned()
        .unwrap_or(ResponsesTextContent {
            content_type: "output_text".to_string(),
            text: String::new(),
            annotations: Vec::new(),
        });

    let mut text_done = item_event("response.output_text.done", response_id, &item.id, 0);
    text_done.content_index = Some(0);
    text_done.text = Some(content.text.clone());

    let mut part_done = item_event("response.content_part.done", response_id, &item.id, 0);
    part_done.content_index = Some(0);
    part_done.part = Some(content);

    let mut item_done = item_event("response.output_item.done", response_id, &item.id, 0);
    item_done.item = Some(ResponsesOutputItem::Message(item.clone()));
    vec![text_done, part_done, item_done]
}

/// The standard Responses event sequence for ONE completed function call,
/// emitted before `response.completed` (#525):
/// `response.output_item.added` (item, in_progress, empty arguments) →
/// `response.function_call_arguments.delta` (full aggregated arguments — a
/// single delta is spec-legal) → `response.function_call_arguments.done` →
/// `response.output_item.done` (item, completed, full arguments).
///
/// Codex collects function calls from `response.output_item.done`.
pub(super) fn function_call_item_events(
    response_id: &str,
    item: &ResponsesFunctionCallOutputItem,
    output_index: u32,
) -> Vec<ResponsesStreamEvent<ResponsesCreateResponse>> {
    let mut added = item_event(
        "response.output_item.added",
        response_id,
        &item.id,
        output_index,
    );
    added.item = Some(ResponsesOutputItem::FunctionCall(
        ResponsesFunctionCallOutputItem {
            arguments: String::new(),
            status: Some("in_progress".to_string()),
            ..item.clone()
        },
    ));

    let mut arguments_delta = item_event(
        "response.function_call_arguments.delta",
        response_id,
        &item.id,
        output_index,
    );
    arguments_delta.delta = Some(item.arguments.clone());

    let mut arguments_done = item_event(
        "response.function_call_arguments.done",
        response_id,
        &item.id,
        output_index,
    );
    arguments_done.arguments = Some(item.arguments.clone());
    arguments_done.name = Some(item.name.clone());

    let mut done = item_event(
        "response.output_item.done",
        response_id,
        &item.id,
        output_index,
    );
    done.item = Some(ResponsesOutputItem::FunctionCall(
        ResponsesFunctionCallOutputItem {
            status: Some("completed".to_string()),
            ..item.clone()
        },
    ));

    vec![added, arguments_delta, arguments_done, done]
}

pub(super) fn event_to_sse_bytes<T: Serialize>(event: &ResponsesStreamEvent<T>) -> Bytes {
    let payload = serde_json::to_string(event).unwrap_or_else(|_| "{}".to_string());
    Bytes::from(format!(
        "event: {}\ndata: {}\n\n",
        event.event_type, payload
    ))
}

/// Serialize an upstream Responses event while assigning Bamboo's own
/// response-scoped monotonic sequence number. All other provider fields and
/// item structure remain untouched.
pub(super) fn raw_event_to_sse_bytes(
    event_type: &str,
    data: &serde_json::Value,
    next_sequence_number: &mut u64,
) -> Bytes {
    let mut payload = data.clone();
    if !payload.is_object() {
        payload = serde_json::json!({"data": payload});
    }
    payload["type"] = serde_json::json!(event_type);
    payload["sequence_number"] = serde_json::json!(*next_sequence_number);
    *next_sequence_number = next_sequence_number.saturating_add(1);
    Bytes::from(format!("event: {event_type}\ndata: {payload}\n\n"))
}

/// A `response.failed` SSE event carrying the upstream error.
///
/// Without this, a mid-stream upstream failure is reported to the client as a
/// clean `[DONE]` (no `response.completed`, but also no failure signal), so an
/// OpenAI-SDK client treats the TRUNCATED output as the final answer. Emitting
/// `response.failed` before `[DONE]` lets the client distinguish a cut-off stream
/// from a complete one — mirroring the Anthropic handler's `error` event and the
/// chat endpoint's error chunk (#383). #355.
pub(super) fn failed_sse_bytes(message: &str, sequence_number: u64) -> Bytes {
    let payload = serde_json::json!({
        "type": "response.failed",
        "sequence_number": sequence_number,
        "response": {
            "status": "failed",
            "error": { "message": message, "type": "api_error" },
        },
    });
    Bytes::from(format!("event: response.failed\ndata: {payload}\n\n"))
}

pub(super) fn done_sse_bytes() -> Bytes {
    Bytes::from_static(b"data: [DONE]\n\n")
}
