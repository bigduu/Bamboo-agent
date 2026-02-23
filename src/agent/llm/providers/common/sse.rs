//! Shared SSE -> [`LLMStream`] adapter.

use eventsource_stream::Eventsource;
use futures_util::StreamExt;
use reqwest::Response;

use crate::provider::{LLMError, LLMStream, Result};
use crate::types::LLMChunk;

fn to_stream_error(err: LLMError) -> LLMError {
    match err {
        LLMError::Stream(msg) => LLMError::Stream(msg),
        other => LLMError::Stream(other.to_string()),
    }
}

/// Convert an SSE HTTP [`Response`] into an [`LLMStream`].
///
/// `handler` receives the SSE event name and data payload for each event, and can either:
/// - return `Ok(Some(chunk))` to emit a chunk
/// - return `Ok(None)` to skip an event
/// - return `Err(_)` to emit a stream error (mapped to `LLMError::Stream`)
pub fn llm_stream_from_sse<H>(response: Response, mut handler: H) -> LLMStream
where
    H: FnMut(&str, &str) -> Result<Option<LLMChunk>> + Send + 'static,
{
    let stream = response
        .bytes_stream()
        .eventsource()
        .map(move |event| {
            let event = event.map_err(|e| LLMError::Stream(e.to_string()))?;
            handler(event.event.as_str(), event.data.as_str()).map_err(to_stream_error)
        })
        .filter_map(|result| async move {
            match result {
                Ok(Some(chunk)) => Some(Ok(chunk)),
                Ok(None) => None,
                Err(err) => Some(Err(err)),
            }
        });

    Box::pin(stream)
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;
    use http;

    #[tokio::test]
    async fn llm_stream_from_sse_filters_none_and_passes_event_name_and_data() {
        let sse_body = concat!(
            "event: token\n",
            "data: hello\n",
            "\n",
            "event: token\n",
            "data: skip\n",
            "\n",
        );

        let response = reqwest::Response::from(
            http::Response::builder()
                .status(200)
                .header("content-type", "text/event-stream")
                .body(sse_body.to_string())
                .expect("http response"),
        );

        let mut stream = llm_stream_from_sse(response, |event, data| {
            if data == "skip" {
                return Ok(None);
            }
            Ok(Some(LLMChunk::Token(format!("{event}:{data}"))))
        });

        let mut out = Vec::new();
        while let Some(item) = stream.next().await {
            out.push(item.expect("chunk"));
        }

        assert_eq!(out.len(), 1);
        match &out[0] {
            LLMChunk::Token(token) => assert_eq!(token, "token:hello"),
            other => panic!("expected LLMChunk::Token, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn llm_stream_from_sse_maps_handler_errors_to_stream_error() {
        let sse_body = concat!("event: token\n", "data: boom\n", "\n");

        let response = reqwest::Response::from(
            http::Response::builder()
                .status(200)
                .header("content-type", "text/event-stream")
                .body(sse_body.to_string())
                .expect("http response"),
        );

        let mut stream = llm_stream_from_sse(response, |_event, _data| {
            Err(LLMError::Api("boom".to_string()))
        });

        let Some(item) = stream.next().await else {
            panic!("expected one stream item");
        };

        match item {
            Ok(chunk) => panic!("expected error, got chunk: {chunk:?}"),
            Err(LLMError::Stream(msg)) => assert!(msg.contains("API error")),
            Err(other) => panic!("expected LLMError::Stream, got: {other:?}"),
        }
    }
}
