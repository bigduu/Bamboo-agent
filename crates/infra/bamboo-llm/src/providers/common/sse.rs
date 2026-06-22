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
/// - return `Ok(None)` to skip an event
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
/// chunks per SSE event; they are flattened into the stream in order.
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
                Ok(chunks) => chunks.into_iter().map(Ok).collect::<Vec<_>>(),
                Err(err) => vec![Err(err)],
            })
        });

    Box::pin(stream)
}

#[cfg(test)]
mod tests {
    use super::*;
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
