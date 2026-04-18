use serde_json::Value;

use super::super::output::{build_completed_response, build_output_items};
use super::errors::map_provider_error;
use super::events::{
    completed_event, created_event, done_sse_bytes, event_to_sse_bytes, output_text_delta_event,
};
use crate::error::AppError;

fn decode_sse_event(bytes: bytes::Bytes) -> (String, Value) {
    let payload = std::str::from_utf8(&bytes).expect("sse bytes should be utf8");
    let mut lines = payload.lines();

    let event = lines
        .next()
        .expect("missing event line")
        .strip_prefix("event: ")
        .expect("event line should have prefix");
    let data = lines
        .next()
        .expect("missing data line")
        .strip_prefix("data: ")
        .expect("data line should have prefix");

    let event_name = event.to_string();
    let json = serde_json::from_str::<Value>(data).expect("data should be valid json");
    (event_name, json)
}

#[test]
fn created_event_serializes_in_progress_response() {
    let event = created_event("resp_1".to_string(), "gpt-5".to_string(), 123);
    let (event_name, payload) = decode_sse_event(event_to_sse_bytes(&event));

    assert_eq!(event_name, "response.created");
    assert_eq!(payload["type"], "response.created");
    assert_eq!(payload["response"]["id"], "resp_1");
    assert_eq!(payload["response"]["status"], "in_progress");
}

#[test]
fn output_text_delta_event_carries_response_and_item_identity() {
    let event = output_text_delta_event("resp_1", "msg_1", "hello".to_string());
    let (event_name, payload) = decode_sse_event(event_to_sse_bytes(&event));

    assert_eq!(event_name, "response.output_text.delta");
    assert_eq!(payload["response_id"], "resp_1");
    assert_eq!(payload["item_id"], "msg_1");
    assert_eq!(payload["delta"], "hello");
}

#[test]
fn completed_event_serializes_completed_response() {
    let output = build_output_items("msg_1", "done".to_string(), vec![]);
    let response = build_completed_response("resp_1".to_string(), 123, "gpt-5".to_string(), output);
    let event = completed_event(response);

    let (event_name, payload) = decode_sse_event(event_to_sse_bytes(&event));

    assert_eq!(event_name, "response.completed");
    assert_eq!(payload["response"]["status"], "completed");
    assert_eq!(payload["response"]["output"][0]["type"], "message");
}

#[test]
fn done_sse_bytes_emits_done_sentinel() {
    let done = done_sse_bytes();
    let payload = std::str::from_utf8(&done).expect("sse bytes should be utf8");
    assert_eq!(payload, "data: [DONE]\n\n");
}

#[test]
fn map_provider_error_returns_proxy_auth_required_for_proxy_errors() {
    let error = map_provider_error("proxy authentication required");
    assert!(matches!(error, AppError::ProxyAuthRequired));
}

#[test]
fn map_provider_error_wraps_non_proxy_errors() {
    let error = map_provider_error("boom");

    match error {
        AppError::InternalError(inner) => {
            assert!(inner.to_string().contains("LLM error: boom"));
        }
        _ => panic!("expected internal error"),
    }
}
