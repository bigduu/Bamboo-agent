use eventsource_stream::Eventsource;
use futures::StreamExt;
use std::pin::Pin;

use super::error::{map_jsonrpc_error, A2AClientError, A2AClientResult};
use super::jsonrpc::JsonRpcResponse;
use super::types::StreamResponse;

pub type A2AStream = Pin<Box<dyn futures::Stream<Item = A2AClientResult<StreamResponse>> + Send>>;

/// Convert an SSE HTTP [`reqwest::Response`] into an [`A2AStream`].
///
/// Each SSE `data:` line is parsed as a `JsonRpcResponse<StreamResponse>`.
/// Empty data lines and `[DONE]` markers are skipped.
/// JSON-RPC errors are mapped to [`A2AClientError`] variants.
pub fn stream_response_from_sse(response: reqwest::Response) -> A2AStream {
    let stream = response
        .bytes_stream()
        .eventsource()
        .filter_map(|event| async move {
            let event = match event {
                Ok(e) => e,
                Err(e) => {
                    return Some(Err(A2AClientError::Sse(e.to_string())));
                }
            };

            let data = event.data.trim();
            if data.is_empty() || data == "[DONE]" {
                return None;
            }

            let envelope: JsonRpcResponse<StreamResponse> = match serde_json::from_str(data) {
                Ok(e) => e,
                Err(e) => {
                    return Some(Err(A2AClientError::Json(e)));
                }
            };

            if let Some(err) = envelope.error {
                return Some(Err(map_jsonrpc_error(err, None)));
            }

            match envelope.result {
                Some(result) => {
                    if let Err(e) = result.payload_kind() {
                        return Some(Err(e));
                    }
                    Some(Ok(result))
                }
                None => Some(Err(A2AClientError::InvalidStreamResponse(
                    "missing result and error in JSON-RPC SSE event".to_string(),
                ))),
            }
        });

    Box::pin(stream)
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    fn make_sse_response(body: &str) -> reqwest::Response {
        reqwest::Response::from(
            http::Response::builder()
                .status(200)
                .header("content-type", "text/event-stream")
                .body(body.to_string())
                .expect("http response"),
        )
    }

    #[tokio::test]
    async fn sse_parser_ignores_empty_data_and_parses_jsonrpc_result() {
        let sse_body = concat!(
            "data: {\"jsonrpc\":\"2.0\",\"id\":\"1\",\"result\":{\"statusUpdate\":{\"taskId\":\"t1\",\"contextId\":\"c1\",\"status\":{\"state\":\"TASK_STATE_WORKING\"}}}}",
            "\n\n",
            "data: \n\n",
            "data: [DONE]\n\n",
        );

        let mut stream = stream_response_from_sse(make_sse_response(sse_body));
        let item = stream.next().await.unwrap().unwrap();
        assert!(item.status_update.is_some());
        let update = item.status_update.unwrap();
        assert_eq!(update.task_id, "t1");
        assert!(matches!(
            update.status.state,
            super::super::types::TaskState::Working
        ));

        // Stream should be exhausted
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn sse_parser_maps_jsonrpc_error_event() {
        let sse_body = concat!(
            "data: {\"jsonrpc\":\"2.0\",\"id\":\"1\",\"error\":{\"code\":-32001,\"message\":\"Task not found\"}}",
            "\n\n",
        );

        let mut stream = stream_response_from_sse(make_sse_response(sse_body));
        let item = stream.next().await.unwrap();
        match item {
            Err(A2AClientError::TaskNotFound(_)) => {}
            other => panic!("expected TaskNotFound, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn sse_parser_rejects_missing_payload() {
        let sse_body = concat!(
            "data: {\"jsonrpc\":\"2.0\",\"id\":\"1\",\"result\":{}}",
            "\n\n",
        );

        let mut stream = stream_response_from_sse(make_sse_response(sse_body));
        let item = stream.next().await.unwrap();
        match item {
            Err(A2AClientError::InvalidStreamResponse(msg)) => {
                assert!(msg.contains("no payload"));
            }
            other => panic!("expected InvalidStreamResponse, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn sse_parser_rejects_multiple_payloads() {
        let sse_body = concat!(
            "data: {\"jsonrpc\":\"2.0\",\"id\":\"1\",\"result\":{\"message\":{\"messageId\":\"m1\",\"role\":\"ROLE_AGENT\",\"parts\":[{\"text\":\"hello\"}]},\"statusUpdate\":{\"taskId\":\"t1\",\"contextId\":\"c1\",\"status\":{\"state\":\"TASK_STATE_WORKING\"}}}}",
            "\n\n",
        );

        let mut stream = stream_response_from_sse(make_sse_response(sse_body));
        let item = stream.next().await.unwrap();
        match item {
            Err(A2AClientError::InvalidStreamResponse(msg)) => {
                assert!(msg.contains("multiple payloads"));
            }
            other => panic!("expected InvalidStreamResponse, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn sse_parser_skips_done_marker() {
        let sse_body = "data: [DONE]\n\n";
        let mut stream = stream_response_from_sse(make_sse_response(sse_body));
        assert!(stream.next().await.is_none());
    }
}
