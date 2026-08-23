//! Shared SSE -> [`LLMStream`] adapter.

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::task::Poll;

use eventsource_stream::Eventsource;
use futures::stream;
use futures::{Stream, StreamExt};
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
/// marker rather than disappearing from the transport watchdog. Successfully
/// received, non-empty HTTP body chunks are observed before EventSource
/// dispatch, so SSE comments and incomplete event fragments also retain
/// transport liveness while remaining invisible to provider handlers.
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
    // `eventsource-stream` intentionally discards SSE comments and buffers
    // incomplete event fragments. Observe the raw body first so those bytes
    // can still refresh the independent transport watchdog (#787). A boolean
    // coalesces any number of synchronously consumed fragments into at most
    // one marker and cannot build an unbounded side queue.
    let raw_body_activity = Arc::new(AtomicBool::new(false));
    let observed_activity = Arc::clone(&raw_body_activity);
    let observed_body = response.bytes_stream().map(move |result| {
        if result.as_ref().is_ok_and(|bytes| !bytes.is_empty()) {
            observed_activity.store(true, Ordering::Relaxed);
        }
        result
    });

    let mut parsed = Box::pin(
        observed_body
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
            }),
    );

    // Poll the parser first. When it immediately produces an item, that item
    // already counts as transport activity in the engine, so suppress the
    // redundant raw marker. If comments/fragments made the parser return
    // Pending (or EOF), surface exactly one internal marker first. The next
    // poll then preserves the parser's original Pending/EOF/error contract.
    let stream = stream::poll_fn(move |cx| match parsed.as_mut().poll_next(cx) {
        Poll::Ready(Some(item)) => {
            raw_body_activity.store(false, Ordering::Relaxed);
            Poll::Ready(Some(item))
        }
        Poll::Ready(None) => {
            if raw_body_activity.swap(false, Ordering::Relaxed) {
                Poll::Ready(Some(Ok(LLMChunk::TransportActivity)))
            } else {
                Poll::Ready(None)
            }
        }
        Poll::Pending => {
            if raw_body_activity.swap(false, Ordering::Relaxed) {
                Poll::Ready(Some(Ok(LLMChunk::TransportActivity)))
            } else {
                Poll::Pending
            }
        }
    });

    Box::pin(stream)
}

/// Like [`llm_stream_from_sse_multi`], but requires an explicit protocol
/// completion chunk.
///
/// A clean HTTP/SSE EOF is transport completion, not proof that an OpenAI
/// Responses request succeeded. This adapter stops after the first `Done` or
/// error and turns a premature EOF into one stream error.
pub fn llm_stream_from_sse_multi_requiring_done<H>(
    response: Response,
    handler: H,
    protocol: &'static str,
) -> LLMStream
where
    H: FnMut(&str, &str) -> Result<Vec<LLMChunk>> + Send + 'static,
{
    require_done_terminal(llm_stream_from_sse_multi(response, handler), protocol)
}

fn require_done_terminal(upstream: LLMStream, protocol: &'static str) -> LLMStream {
    let stream = stream::unfold(
        (upstream, false),
        move |(mut upstream, terminal)| async move {
            if terminal {
                return None;
            }

            match upstream.next().await {
                Some(Ok(LLMChunk::Done)) => Some((Ok(LLMChunk::Done), (upstream, true))),
                Some(Err(error)) => Some((Err(error), (upstream, true))),
                Some(other) => Some((other, (upstream, false))),
                None => Some((
                    Err(LLMError::Stream(format!(
                        "{protocol} stream ended before a protocol terminal event"
                    ))),
                    (upstream, true),
                )),
            }
        },
    );
    Box::pin(stream)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::anthropic::{parse_anthropic_sse_event, AnthropicStreamState};
    use crate::providers::common::openai_compat::parse_openai_compat_sse_data_strict_multi;
    use crate::providers::common::openai_responses::ResponsesSseParser;
    use bytes::Bytes;
    use futures::StreamExt;
    use serde_json::json;

    /// Build a body whose chunks are separated by an actual `Poll::Pending`.
    /// This models network delivery and ensures the adapter has an opportunity
    /// to surface liveness before an incomplete SSE event becomes dispatchable.
    fn chunked_sse_response(chunks: Vec<Bytes>) -> Response {
        let body_stream = stream::unfold(chunks.into_iter(), |mut chunks| async move {
            tokio::task::yield_now().await;
            chunks
                .next()
                .map(|chunk| (Ok::<_, std::io::Error>(chunk), chunks))
        });
        let body = reqwest::Body::wrap_stream(body_stream);

        reqwest::Response::from(
            http::Response::builder()
                .status(200)
                .header("content-type", "text/event-stream")
                .body(body)
                .expect("http response"),
        )
    }

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
    async fn sse_comment_only_body_is_preserved_as_transport_activity() {
        let response = chunked_sse_response(vec![Bytes::from_static(b": keep-alive\n\n")]);
        let mut stream = llm_stream_from_sse(response, |_event, _data| {
            panic!("SSE comments must not be dispatched to the provider handler")
        });

        assert!(matches!(
            stream.next().await,
            Some(Ok(LLMChunk::TransportActivity))
        ));
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn partial_sse_fragments_refresh_transport_without_corrupting_utf8() {
        let response = chunked_sse_response(vec![
            Bytes::from_static(b"event: token\ndata: \xe4"),
            Bytes::from_static(b"\xbd\xa0\xe5"),
            Bytes::from_static(b"\xa5\xbd\n\n"),
        ]);
        let mut stream = llm_stream_from_sse(response, |event, data| {
            Ok(Some(LLMChunk::Token(format!("{event}:{data}"))))
        });

        assert!(matches!(
            stream.next().await,
            Some(Ok(LLMChunk::TransportActivity))
        ));
        assert!(matches!(
            stream.next().await,
            Some(Ok(LLMChunk::TransportActivity))
        ));
        assert!(matches!(
            stream.next().await,
            Some(Ok(LLMChunk::Token(text))) if text == "token:你好"
        ));
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn responses_comment_then_completed_emits_one_done_without_trailing_activity() {
        let response = chunked_sse_response(vec![
            Bytes::from_static(b": keep-alive\n\n"),
            Bytes::from_static(
                b"event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_done\"}}\n\n",
            ),
        ]);
        let mut parser = ResponsesSseParser::new();
        let mut stream = llm_stream_from_sse_multi_requiring_done(
            response,
            move |event, data| parser.handle_event_multi(event, data),
            "OpenAI Responses",
        );

        let mut chunks = Vec::new();
        while let Some(item) = stream.next().await {
            chunks.push(item.expect("valid Responses stream"));
        }

        assert!(matches!(chunks.first(), Some(LLMChunk::TransportActivity)));
        assert!(chunks
            .iter()
            .any(|chunk| matches!(chunk, LLMChunk::ResponseId(id) if id == "resp_done")));
        assert_eq!(
            chunks
                .iter()
                .filter(|chunk| matches!(chunk, LLMChunk::Done))
                .count(),
            1
        );
        assert!(matches!(chunks.last(), Some(LLMChunk::Done)));
    }

    #[tokio::test]
    async fn responses_comment_then_eof_emits_activity_and_one_terminal_error() {
        let response = chunked_sse_response(vec![Bytes::from_static(b": keep-alive\n\n")]);
        let mut parser = ResponsesSseParser::new();
        let mut stream = llm_stream_from_sse_multi_requiring_done(
            response,
            move |event, data| parser.handle_event_multi(event, data),
            "OpenAI Responses",
        );

        assert!(matches!(
            stream.next().await,
            Some(Ok(LLMChunk::TransportActivity))
        ));
        let error = stream
            .next()
            .await
            .expect("premature EOF error")
            .expect_err("comment-only EOF cannot complete a Responses stream");
        assert!(error
            .to_string()
            .contains("ended before a protocol terminal event"));
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
                        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_terminal\",\"output\":[{\"id\":\"msg_terminal\",\"type\":\"message\",\"content\":[{\"type\":\"output_text\",\"text\":\"terminal answer\"}]}],\"usage\":{\"input_tokens\":21,\"output_tokens\":13,\"input_tokens_details\":{\"cached_tokens\":8},\"output_tokens_details\":{\"reasoning_tokens\":5}}}}\n",
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
            LLMChunk::ProviderUsage {
                input_tokens: Some(21),
                output_tokens: Some(13),
                reasoning_tokens: Some(5),
                cache_creation_input_tokens: None,
                cache_read_input_tokens: Some(8),
                ..
            }
        ));
        assert!(matches!(chunks[3], LLMChunk::Done));
    }

    #[tokio::test]
    async fn openai_chat_frame_flattens_business_output_and_usage_before_done() {
        let response = reqwest::Response::from(
            http::Response::builder()
                .status(200)
                .header("content-type", "text/event-stream")
                .body(
                    concat!(
                        "data: {\"choices\":[{\"delta\":{\"content\":\"answer\"}}],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":4,\"prompt_tokens_details\":{\"cached_tokens\":3}}}\n",
                        "\n",
                        "data: [DONE]\n",
                        "\n",
                    )
                    .to_string(),
                )
                .expect("http response"),
        );
        let mut stream = llm_stream_from_sse_multi(response, |_event, data| {
            parse_openai_compat_sse_data_strict_multi(data)
        });

        let mut chunks = Vec::new();
        while let Some(item) = stream.next().await {
            chunks.push(item.expect("stream chunk"));
        }

        assert_eq!(chunks.len(), 3);
        assert!(matches!(&chunks[0], LLMChunk::Token(text) if text == "answer"));
        assert!(matches!(
            chunks[1],
            LLMChunk::ProviderUsage {
                input_tokens: Some(10),
                output_tokens: Some(4),
                cache_read_input_tokens: Some(3),
                ..
            }
        ));
        assert!(matches!(chunks[2], LLMChunk::Done));
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

    #[tokio::test]
    async fn required_done_turns_clean_eof_into_one_error() {
        let upstream: LLMStream = Box::pin(stream::iter(vec![Ok(LLMChunk::Token(
            "partial".to_string(),
        ))]));
        let mut stream = require_done_terminal(upstream, "Responses");

        assert!(matches!(
            stream.next().await,
            Some(Ok(LLMChunk::Token(text))) if text == "partial"
        ));
        let error = stream
            .next()
            .await
            .expect("premature EOF error")
            .expect_err("EOF must not synthesize success");
        assert!(error
            .to_string()
            .contains("ended before a protocol terminal"));
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn required_done_stops_after_first_success_terminal() {
        let upstream: LLMStream = Box::pin(stream::iter(vec![
            Ok(LLMChunk::Done),
            Ok(LLMChunk::Token("after".to_string())),
        ]));
        let mut stream = require_done_terminal(upstream, "Responses");

        assert!(matches!(stream.next().await, Some(Ok(LLMChunk::Done))));
        assert!(stream.next().await.is_none());
    }
}
