use bytes::Bytes;
use serde::Serialize;

use super::super::super::types::{ResponsesCreateResponse, ResponsesStreamEvent};

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
    }
}

pub(super) fn event_to_sse_bytes<T: Serialize>(event: &ResponsesStreamEvent<T>) -> Bytes {
    let payload = serde_json::to_string(event).unwrap_or_else(|_| "{}".to_string());
    Bytes::from(format!(
        "event: {}\ndata: {}\n\n",
        event.event_type, payload
    ))
}

pub(super) fn done_sse_bytes() -> Bytes {
    Bytes::from_static(b"data: [DONE]\n\n")
}
