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
    let (event_name, payload) = decode_sse_event(failed_sse_bytes("upstream 503", 7));
    assert_eq!(event_name, "response.failed");
    assert_eq!(payload["type"], "response.failed");
    assert_eq!(payload["response"]["status"], "failed");
    assert_eq!(payload["response"]["error"]["message"], "upstream 503");
    assert_eq!(payload["response"]["error"]["type"], "api_error");
    assert_eq!(payload["sequence_number"], 7);
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
    let response =
        build_completed_response("resp_1".to_string(), 123, "gpt-5".to_string(), output, None);
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
fn raw_protocol_events_preserve_items_and_get_monotonic_sequences() {
    use super::events::raw_event_to_sse_bytes;

    let raw = serde_json::json!({
        "type": "response.output_item.done",
        "sequence_number": 99,
        "output_index": 2,
        "item": {
            "id": "msg_2",
            "type": "message",
            "content": [{"type": "output_text", "text": "second"}]
        }
    });
    let mut sequence = 4;
    let (name, first) = decode_sse_event(raw_event_to_sse_bytes(
        "response.output_item.done",
        &raw,
        &mut sequence,
    ));
    assert_eq!(name, "response.output_item.done");
    assert_eq!(first["sequence_number"], 4);
    assert_eq!(first["output_index"], 2);
    assert_eq!(first["item"]["id"], "msg_2");

    let (_, second) = decode_sse_event(raw_event_to_sse_bytes(
        "response.completed",
        &serde_json::json!({"response":{"status":"completed"}}),
        &mut sequence,
    ));
    assert_eq!(second["sequence_number"], 5);
}

fn test_metrics() -> (bamboo_metrics::MetricsCollector, tempfile::TempDir) {
    use bamboo_metrics::storage::SqliteMetricsStorage;
    use std::sync::Arc;

    let dir = tempfile::tempdir().expect("temp metrics directory");
    let storage = Arc::new(SqliteMetricsStorage::new(dir.path().join("metrics.db")));
    (bamboo_metrics::MetricsCollector::spawn(storage, 7), dir)
}

fn drain_frames(
    rx: &mut tokio::sync::mpsc::Receiver<Result<bytes::Bytes, anyhow::Error>>,
) -> Vec<String> {
    let mut frames = Vec::new();
    while let Ok(item) = rx.try_recv() {
        frames.push(
            String::from_utf8(item.expect("worker frame").to_vec()).expect("utf8 worker frame"),
        );
    }
    frames
}

#[tokio::test]
async fn synthesized_worker_emits_usage_and_complete_message_lifecycle() {
    use super::worker::{run_stream_worker, StreamWorkerArgs};
    use bamboo_llm::provider::LLMStream;
    use bamboo_llm::types::LLMChunk;
    use futures::stream;

    let (metrics, _dir) = test_metrics();
    let chunks = vec![
        Ok(LLMChunk::ResponseId("resp_usage".to_string())),
        Ok(LLMChunk::ReasoningToken("private reasoning".to_string())),
        Ok(LLMChunk::Token("hello".to_string())),
        Ok(LLMChunk::ProviderUsage {
            input_tokens: Some(80),
            output_tokens: Some(20),
            total_tokens: Some(100),
            reasoning_tokens: Some(5),
            cache_creation_input_tokens: None,
            cache_read_input_tokens: Some(32),
            cache_write_input_tokens: Some(48),
        }),
        Ok(LLMChunk::Done),
    ];
    let stream_result: LLMStream = Box::pin(stream::iter(chunks));
    let (tx, mut rx) = tokio::sync::mpsc::channel(64);

    run_stream_worker(StreamWorkerArgs {
        stream_result,
        tx,
        metrics,
        forward_id: "forward-usage".to_string(),
        fallback_response_id: "resp_fallback".to_string(),
        message_id: "msg_usage".to_string(),
        created_at: 123,
        resolved_model: "gpt-5.6".to_string(),
        estimated_prompt_tokens: 999,
    })
    .await;

    let frames = drain_frames(&mut rx);
    let decoded: Vec<_> = frames
        .iter()
        .filter(|frame| frame.starts_with("event: "))
        .map(|frame| decode_sse_event(bytes::Bytes::copy_from_slice(frame.as_bytes())))
        .collect();
    assert_eq!(
        decoded
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>(),
        vec![
            "response.created",
            "response.output_item.added",
            "response.content_part.added",
            "response.output_text.delta",
            "response.output_text.done",
            "response.content_part.done",
            "response.output_item.done",
            "response.completed",
        ]
    );
    assert_eq!(
        decoded
            .iter()
            .map(|(_, payload)| payload["sequence_number"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        (0..8).collect::<Vec<_>>()
    );
    let completed = &decoded.last().expect("completed event").1["response"];
    assert_eq!(completed["usage"]["input_tokens"], 80);
    assert_eq!(completed["usage"]["output_tokens"], 20);
    assert_eq!(completed["usage"]["total_tokens"], 100);
    assert_eq!(
        completed["usage"]["input_tokens_details"]["cached_tokens"],
        32
    );
    assert_eq!(
        completed["usage"]["input_tokens_details"]["cache_write_tokens"],
        48
    );
    assert_eq!(
        completed["usage"]["output_tokens_details"]["reasoning_tokens"],
        5
    );
    assert!(
        frames
            .iter()
            .all(|frame| !frame.contains("private reasoning")),
        "reasoning must not be relabeled as output_text"
    );
    assert_eq!(frames.last().map(String::as_str), Some("data: [DONE]\n\n"));
}

#[tokio::test]
async fn raw_worker_preserves_mixed_item_identity_and_order() {
    use super::worker::{run_stream_worker, StreamWorkerArgs};
    use bamboo_llm::provider::LLMStream;
    use bamboo_llm::types::LLMChunk;
    use futures::stream;

    let (metrics, _dir) = test_metrics();
    let completed = serde_json::json!({
        "type": "response.completed",
        "response": {
            "id": "resp_raw",
            "status": "completed",
            "output": [
                {"id": "msg_a", "type": "message", "content": [{"type": "output_text", "text": "a"}]},
                {"id": "rs_a", "type": "reasoning", "summary": [{"type": "summary_text", "text": "why"}]},
                {"id": "fc_a", "type": "function_call", "call_id": "call_a", "name": "search", "arguments": "{}"},
                {"id": "msg_b", "type": "message", "content": [{"type": "output_text", "text": "b"}]}
            ],
            "usage": {"input_tokens": 4, "output_tokens": 2, "total_tokens": 6}
        }
    });
    let chunks = vec![
        Ok(LLMChunk::ResponsesEvent {
            event_type: "response.created".to_string(),
            data: Box::new(serde_json::json!({
                "type": "response.created",
                "response": {"id": "resp_raw", "status": "in_progress"}
            })),
        }),
        Ok(LLMChunk::ResponsesEvent {
            event_type: "response.completed".to_string(),
            data: Box::new(completed),
        }),
        Ok(LLMChunk::ProviderUsage {
            input_tokens: Some(4),
            output_tokens: Some(2),
            total_tokens: Some(6),
            reasoning_tokens: None,
            cache_creation_input_tokens: None,
            cache_read_input_tokens: Some(0),
            cache_write_input_tokens: Some(4),
        }),
        Ok(LLMChunk::Done),
    ];
    let stream_result: LLMStream = Box::pin(stream::iter(chunks));
    let (tx, mut rx) = tokio::sync::mpsc::channel(64);

    run_stream_worker(StreamWorkerArgs {
        stream_result,
        tx,
        metrics,
        forward_id: "forward-raw".to_string(),
        fallback_response_id: "resp_fallback".to_string(),
        message_id: "msg_fallback".to_string(),
        created_at: 123,
        resolved_model: "gpt-5.6".to_string(),
        estimated_prompt_tokens: 999,
    })
    .await;

    let frames = drain_frames(&mut rx);
    let decoded: Vec<_> = frames
        .iter()
        .filter(|frame| frame.starts_with("event: "))
        .map(|frame| decode_sse_event(bytes::Bytes::copy_from_slice(frame.as_bytes())))
        .collect();
    assert_eq!(
        decoded
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>(),
        vec!["response.created", "response.completed"]
    );
    assert_eq!(decoded[0].1["sequence_number"], 0);
    assert_eq!(decoded[1].1["sequence_number"], 1);
    assert_eq!(
        decoded[1].1["response"]["output"]
            .as_array()
            .expect("output")
            .iter()
            .map(|item| item["id"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["msg_a", "rs_a", "fc_a", "msg_b"]
    );
    assert_eq!(frames.last().map(String::as_str), Some("data: [DONE]\n\n"));
}

#[tokio::test]
async fn worker_reports_premature_eof_as_failure_not_completion() {
    use super::worker::{run_stream_worker, StreamWorkerArgs};
    use bamboo_llm::provider::LLMStream;
    use bamboo_llm::types::LLMChunk;
    use futures::stream;

    let (metrics, _dir) = test_metrics();
    let stream_result: LLMStream = Box::pin(stream::iter(vec![Ok(LLMChunk::Token(
        "partial".to_string(),
    ))]));
    let (tx, mut rx) = tokio::sync::mpsc::channel(64);

    run_stream_worker(StreamWorkerArgs {
        stream_result,
        tx,
        metrics,
        forward_id: "forward-eof".to_string(),
        fallback_response_id: "resp_eof".to_string(),
        message_id: "msg_eof".to_string(),
        created_at: 123,
        resolved_model: "gpt-5.6".to_string(),
        estimated_prompt_tokens: 1,
    })
    .await;

    let frames = drain_frames(&mut rx);
    assert!(frames.iter().any(|frame| {
        frame.contains("response.failed")
            && frame.contains("ended before a protocol completion event")
    }));
    assert!(!frames
        .iter()
        .any(|frame| frame.contains("response.completed")));
    assert_eq!(frames.last().map(String::as_str), Some("data: [DONE]\n\n"));
}

#[test]
fn tool_only_output_has_no_synthetic_empty_message() {
    let output = build_output_items(
        "msg_unused",
        String::new(),
        vec![bamboo_agent_core::tools::ToolCall {
            id: "call_1".to_string(),
            tool_type: "function".to_string(),
            function: bamboo_domain::FunctionCall {
                name: "search".to_string(),
                arguments: "{}".to_string(),
            },
        }],
    );
    assert_eq!(output.len(), 1);
    assert!(matches!(
        output[0],
        super::super::super::types::ResponsesOutputItem::FunctionCall(_)
    ));
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
    let super::super::super::types::ResponsesOutputItem::FunctionCall(fc) = &output[0] else {
        panic!("expected function_call item at output_index 0");
    };

    let events = function_call_item_events("resp_1", fc, 0);
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
    assert_eq!(added["output_index"], 0);

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
    let response = build_completed_response("resp_1".to_string(), 1, "m".to_string(), output, None);
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

// #525 review: the assistant message item must also surface via
// output_item.added/done — clients that assemble output from output_item
// events would otherwise never finalize the assistant's text.
#[test]
fn message_item_events_announce_and_finalize_the_message_item() {
    use super::events::{
        message_content_part_added_event, message_item_added_event, message_item_done_events,
    };

    let (name, added) = decode_sse_event(event_to_sse_bytes(&message_item_added_event(
        "resp_1", "msg_1",
    )));
    assert_eq!(name, "response.output_item.added");
    assert_eq!(added["output_index"], 0);
    assert_eq!(added["item"]["type"], "message");
    assert_eq!(added["item"]["id"], "msg_1");
    assert_eq!(added["item"]["status"], "in_progress");

    let (name, content_added) = decode_sse_event(event_to_sse_bytes(
        &message_content_part_added_event("resp_1", "msg_1"),
    ));
    assert_eq!(name, "response.content_part.added");
    assert_eq!(content_added["part"]["type"], "output_text");
    assert_eq!(content_added["part"]["text"], "");

    let output = build_output_items("msg_1", "final text".to_string(), vec![]);
    let super::super::super::types::ResponsesOutputItem::Message(message) = &output[0] else {
        panic!("expected message item at output_index 0");
    };
    let done_events = message_item_done_events("resp_1", message);
    assert_eq!(done_events.len(), 3);
    let (text_name, text_done) = decode_sse_event(event_to_sse_bytes(&done_events[0]));
    assert_eq!(text_name, "response.output_text.done");
    assert_eq!(text_done["text"], "final text");
    let (part_name, part_done) = decode_sse_event(event_to_sse_bytes(&done_events[1]));
    assert_eq!(part_name, "response.content_part.done");
    assert_eq!(part_done["part"]["text"], "final text");
    let (item_name, item_done) = decode_sse_event(event_to_sse_bytes(&done_events[2]));
    assert_eq!(item_name, "response.output_item.done");
    assert_eq!(item_done["item"]["type"], "message");
    assert_eq!(item_done["item"]["status"], "completed");
    assert_eq!(item_done["item"]["content"][0]["text"], "final text");
}
