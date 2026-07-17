use bytes::Bytes;
use serde::Serialize;

use super::super::super::types::{
    ResponsesCreateResponse, ResponsesFunctionCallOutputItem, ResponsesOutputItem,
    ResponsesStreamEvent,
};

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
        response_id: None,
        item_id: None,
        output_index: None,
        content_index: None,
        delta: None,
        item: None,
        arguments: None,
    }
}

pub(super) fn output_text_delta_event(
    response_id: &str,
    item_id: &str,
    delta: String,
) -> ResponsesStreamEvent<ResponsesCreateResponse> {
    ResponsesStreamEvent {
        event_type: "response.output_text.delta".to_string(),
        response: None,
        response_id: Some(response_id.to_string()),
        item_id: Some(item_id.to_string()),
        output_index: Some(0),
        content_index: Some(0),
        delta: Some(delta),
        item: None,
        arguments: None,
    }
}

pub(super) fn completed_event(
    response: ResponsesCreateResponse,
) -> ResponsesStreamEvent<ResponsesCreateResponse> {
    ResponsesStreamEvent {
        event_type: "response.completed".to_string(),
        response: Some(response),
        response_id: None,
        item_id: None,
        output_index: None,
        content_index: None,
        delta: None,
        item: None,
        arguments: None,
    }
}

/// A bare event skeleton for the function-call item events below.
fn item_event(
    event_type: &str,
    response_id: &str,
    item_id: &str,
    output_index: u32,
) -> ResponsesStreamEvent<ResponsesCreateResponse> {
    ResponsesStreamEvent {
        event_type: event_type.to_string(),
        response: None,
        response_id: Some(response_id.to_string()),
        item_id: Some(item_id.to_string()),
        output_index: Some(output_index),
        content_index: None,
        delta: None,
        item: None,
        arguments: None,
    }
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

/// A `response.failed` SSE event carrying the upstream error.
///
/// Without this, a mid-stream upstream failure is reported to the client as a
/// clean `[DONE]` (no `response.completed`, but also no failure signal), so an
/// OpenAI-SDK client treats the TRUNCATED output as the final answer. Emitting
/// `response.failed` before `[DONE]` lets the client distinguish a cut-off stream
/// from a complete one — mirroring the Anthropic handler's `error` event and the
/// chat endpoint's error chunk (#383). #355.
pub(super) fn failed_sse_bytes(message: &str) -> Bytes {
    let payload = serde_json::json!({
        "type": "response.failed",
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
