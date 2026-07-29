//! Shared SSE -> [`LLMStream`] adapter.

use eventsource_stream::Eventsource;
use futures::stream;
use futures::StreamExt;
use reqwest::Response;
use serde_json::Value;

use crate::provider::{LLMError, LLMStream, Result};
use crate::types::LLMChunk;

/// True if a top-level SSE `"error"` field represents a REAL error.
///
/// Several gateways (LiteLLM, OneAPI/New-API, some Azure proxies, and some Gemini
/// gateways) emit `{"error": null}` — or `""`/`{}` — as a *no-error* marker on an
/// otherwise-normal chunk. `serde_json`'s `get("error")` returns `Some(Null)` for
/// an explicit null, so without this guard such a benign marker would abort a
/// valid stream (#26 openai-compat, #99 Gemini). Only a non-null, non-empty value
/// is a real error. Shared by every SSE parser so the guard can't drift.
pub(crate) fn sse_error_is_present(error: &Value) -> bool {
    match error {
        Value::Null => false,
        Value::String(s) => !s.trim().is_empty(),
        Value::Object(map) => !map.is_empty(),
        Value::Array(items) => !items.is_empty(),
        _ => true,
    }
}

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
/// - return `Ok(None)` when an event has no semantic chunk; the adapter emits
///   an internal [`LLMChunk::TransportActivity`] marker so liveness is retained
/// - return `Err(_)` to emit a stream error (mapped to `LLMError::Stream`)
///
/// This is the common case where each SSE event yields at most one chunk.
/// Providers whose events can carry several chunks (e.g. Gemini's final
/// `usageMetadata` folds a cache hit and output/thinking usage into one event)
/// should use [`llm_stream_from_sse_multi`] instead.
pub fn llm_stream_from_sse<H>(response: Response, mut handler: H) -> LLMStream
where
    H: FnMut(&str, &str) -> Result<Option<LLMChunk>> + Send + 'static,
{
    llm_stream_from_sse_multi(response, move |event, data| {
        Ok(handler(event, data)?.into_iter().collect())
    })
}

/// Like [`llm_stream_from_sse`], but the handler may emit **zero or more**
/// chunks per SSE event; they are flattened into the stream in order. A valid
/// event producing zero chunks becomes one [`LLMChunk::TransportActivity`]
/// marker rather than disappearing from the transport watchdog.
///
/// This is required for providers where a single SSE event must surface
/// multiple logical chunks. The motivating case is Gemini: a final
/// `usageMetadata` event carries both a prompt-cache hit AND output/thinking
/// token usage, and `streamGenerateContent?alt=sse` sends no `[DONE]`
/// sentinel — so a chunk deferred to "the next event" would be silently lost
/// when the connection closes. Returning every chunk from the one event (via
/// `Vec`) and flattening here delivers both with no buffering and no reliance
/// on a trailing event (issue #27).
pub fn llm_stream_from_sse_multi<H>(response: Response, mut handler: H) -> LLMStream
where
    H: FnMut(&str, &str) -> Result<Vec<LLMChunk>> + Send + 'static,
{
    let stream = response
        .bytes_stream()
        .eventsource()
        .map(move |event| {
            let event = event.map_err(|e| LLMError::Stream(e.to_string()))?;
            handler(event.event.as_str(), event.data.as_str()).map_err(to_stream_error)
        })
        .flat_map(|result| {
            stream::iter(match result {
                Ok(chunks) if chunks.is_empty() => vec![Ok(LLMChunk::TransportActivity)],
                Ok(chunks) => chunks.into_iter().map(Ok).collect::<Vec<_>>(),
                Err(err) => vec![Err(err)],
            })
        });

    Box::pin(stream)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::anthropic::{parse_anthropic_sse_event, AnthropicStreamState};
    use crate::providers::common::openai_responses::ResponsesSseParser;
    use futures::StreamExt;
    use serde_json::json;
    // use http; // TODO: add http crate if needed

    #[test]
    fn sse_error_is_present_distinguishes_real_errors_from_benign_markers() {
        // Benign no-error markers some gateways emit -> NOT a real error.
        assert!(!sse_error_is_present(&Value::Null));
        assert!(!sse_error_is_present(&json!("")));
        assert!(!sse_error_is_present(&json!("   ")));
        assert!(!sse_error_is_present(&json!({})));
        assert!(!sse_error_is_present(&json!([])));

        // Real errors -> present.
        assert!(sse_error_is_present(&json!("boom")));
        assert!(sse_error_is_present(
            &json!({ "message": "API key invalid" })
        ));
        assert!(sse_error_is_present(&json!(["e"])));
        assert!(sse_error_is_present(&json!(42)));
        assert!(sse_error_is_present(&json!(true)));
    }

    #[tokio::test]
    async fn llm_stream_from_sse_preserves_filtered_event_as_transport_activity() {
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

        assert_eq!(out.len(), 2);
        match &out[0] {
            LLMChunk::Token(token) => assert_eq!(token, "token:hello"),
            other => panic!("expected LLMChunk::Token, got {other:?}"),
        }
        assert!(matches!(out[1], LLMChunk::TransportActivity));
    }

    #[tokio::test]
    async fn anthropic_ping_is_preserved_as_transport_activity() {
        let response = reqwest::Response::from(
            http::Response::builder()
                .status(200)
                .header("content-type", "text/event-stream")
                .body("event: ping\ndata: {\"type\":\"ping\"}\n\n".to_string())
                .expect("http response"),
        );
        let mut state = AnthropicStreamState::default();
        let mut stream = llm_stream_from_sse(response, move |event, data| {
            parse_anthropic_sse_event(&mut state, event, data)
        });

        let chunk = stream
            .next()
            .await
            .expect("ping should yield a liveness marker")
            .expect("ping should not fail the stream");
        assert!(matches!(chunk, LLMChunk::TransportActivity));
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn openai_responses_keepalive_is_preserved_as_transport_activity() {
        let response = reqwest::Response::from(
            http::Response::builder()
                .status(200)
                .header("content-type", "text/event-stream")
                .body("event: ping\ndata: keep-alive\n\n".to_string())
                .expect("http response"),
        );
        let mut parser = ResponsesSseParser::new();
        let mut stream = llm_stream_from_sse(response, move |event, data| {
            parser.handle_event(event, data)
        });

        let chunk = stream
            .next()
            .await
            .expect("keepalive should yield a liveness marker")
            .expect("keepalive should not fail the stream");
        assert!(matches!(chunk, LLMChunk::TransportActivity));
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn openai_responses_completed_frame_flattens_every_parser_chunk() {
        let response = reqwest::Response::from(
            http::Response::builder()
                .status(200)
                .header("content-type", "text/event-stream")
                .body(
                    concat!(
                        "event: response.completed\n",
                        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_terminal\",\"output\":[{\"id\":\"msg_terminal\",\"type\":\"message\",\"content\":[{\"type\":\"output_text\",\"text\":\"terminal answer\"}]}],\"usage\":{\"input_tokens\":21,\"input_tokens_details\":{\"cached_tokens\":8}}}}\n",
                        "\n",
                    )
                    .to_string(),
                )
                .expect("http response"),
        );
        let mut parser = ResponsesSseParser::new();
        let mut stream = llm_stream_from_sse_multi(response, move |event, data| {
            parser.handle_event_multi(event, data)
        });

        let mut chunks = Vec::new();
        while let Some(item) = stream.next().await {
            chunks.push(item.expect("stream chunk"));
        }

        assert_eq!(chunks.len(), 4);
        assert!(matches!(&chunks[0], LLMChunk::ResponseId(id) if id == "resp_terminal"));
        assert!(matches!(&chunks[1], LLMChunk::Token(text) if text == "terminal answer"));
        assert!(matches!(
            chunks[2],
            LLMChunk::CacheUsage {
                cache_creation_input_tokens: 0,
                cache_read_input_tokens: 8,
                input_tokens: 13,
            }
        ));
        assert!(matches!(chunks[3], LLMChunk::Done));
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
