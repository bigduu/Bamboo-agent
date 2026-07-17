use serde_json::Value;

use super::super::output::{build_completed_response, build_output_items};
use super::errors::map_provider_error;
use super::events::{
    completed_event, created_event, done_sse_bytes, event_to_sse_bytes, failed_sse_bytes,
    output_text_delta_event,
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
fn failed_sse_bytes_signals_failure_with_error() {
    // #355: a mid-stream upstream error must surface as a `response.failed` event
    // (with the error), not a clean `[DONE]` the client reads as success.
    let (event_name, payload) = decode_sse_event(failed_sse_bytes("upstream 503"));
    assert_eq!(event_name, "response.failed");
    assert_eq!(payload["type"], "response.failed");
    assert_eq!(payload["response"]["status"], "failed");
    assert_eq!(payload["response"]["error"]["message"], "upstream 503");
    assert_eq!(payload["response"]["error"]["type"], "api_error");
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

// ── #525: function-call streaming event sequence + fragment aggregation ─

use super::events::function_call_item_events;
use bamboo_agent_core::tools::{FunctionCall, ToolCall, ToolCallAccumulator};

fn fragment(id: &str, name: &str, arguments: &str) -> ToolCall {
    ToolCall {
        id: id.to_string(),
        tool_type: "function".to_string(),
        function: FunctionCall {
            name: name.to_string(),
            arguments: arguments.to_string(),
        },
    }
}

// The exact upstream pattern that used to shatter one call into N broken
// items: a metadata fragment followed by argument-only continuation fragments
// (empty id/name), all sharing the provider index.
#[test]
fn indexed_fragments_aggregate_into_single_function_call_item() {
    let mut acc = ToolCallAccumulator::new();
    acc.extend_indexed(vec![(0, fragment("call_w1", "get_weather", ""))]);
    acc.extend_indexed(vec![(0, fragment("", "", "{\"location\":"))]);
    acc.extend_indexed(vec![(0, fragment("", "", "\"NYC\"}"))]);

    let output = build_output_items("msg_1", String::new(), acc.finalize());

    let function_items: Vec<_> = output
        .iter()
        .filter_map(|item| match item {
            super::super::super::types::ResponsesOutputItem::FunctionCall(fc) => Some(fc),
            _ => None,
        })
        .collect();
    assert_eq!(
        function_items.len(),
        1,
        "fragments must merge into ONE item"
    );
    assert_eq!(function_items[0].call_id, "call_w1");
    assert_eq!(function_items[0].name, "get_weather");
    assert_eq!(function_items[0].arguments, "{\"location\":\"NYC\"}");
    assert_eq!(function_items[0].status.as_deref(), Some("completed"));
}

#[test]
fn function_call_item_events_emit_standard_sequence() {
    let output = build_output_items(
        "msg_1",
        String::new(),
        vec![fragment("call_w1", "get_weather", "{\"location\":\"NYC\"}")],
    );
    let super::super::super::types::ResponsesOutputItem::FunctionCall(fc) = &output[1] else {
        panic!("expected function_call item at output_index 1");
    };

    let events = function_call_item_events("resp_1", fc, 1);
    let decoded: Vec<_> = events
        .iter()
        .map(|event| decode_sse_event(event_to_sse_bytes(event)))
        .collect();

    assert_eq!(
        decoded
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>(),
        vec![
            "response.output_item.added",
            "response.function_call_arguments.delta",
            "response.function_call_arguments.done",
            "response.output_item.done",
        ]
    );

    let (_, added) = &decoded[0];
    assert_eq!(added["item"]["type"], "function_call");
    assert_eq!(added["item"]["call_id"], "call_w1");
    assert_eq!(added["item"]["status"], "in_progress");
    assert_eq!(added["item"]["arguments"], "");
    assert_eq!(added["output_index"], 1);

    let (_, delta) = &decoded[1];
    assert_eq!(delta["delta"], "{\"location\":\"NYC\"}");
    assert_eq!(delta["item_id"], added["item"]["id"]);

    let (_, args_done) = &decoded[2];
    assert_eq!(args_done["arguments"], "{\"location\":\"NYC\"}");

    let (_, item_done) = &decoded[3];
    assert_eq!(item_done["item"]["status"], "completed");
    assert_eq!(item_done["item"]["arguments"], "{\"location\":\"NYC\"}");
    assert_eq!(item_done["item"]["name"], "get_weather");
}

#[test]
fn completed_response_carries_aggregated_function_call() {
    let mut acc = ToolCallAccumulator::new();
    acc.extend(vec![fragment("call_1", "search", "{\"q\":")]);
    acc.extend(vec![fragment("call_1", "", "\"hi\"}")]);

    let output = build_output_items("msg_1", "text".to_string(), acc.finalize());
    let response = build_completed_response("resp_1".to_string(), 1, "m".to_string(), output);
    let event = completed_event(response);
    let (_, payload) = decode_sse_event(event_to_sse_bytes(&event));

    assert_eq!(payload["response"]["output"][0]["type"], "message");
    assert_eq!(payload["response"]["output"][1]["type"], "function_call");
    assert_eq!(
        payload["response"]["output"][1]["arguments"],
        "{\"q\":\"hi\"}"
    );
    assert!(payload["response"]["output"].as_array().unwrap().len() == 2);
}
