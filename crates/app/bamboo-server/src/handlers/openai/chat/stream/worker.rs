use bytes::Bytes;
use futures::StreamExt;
use tokio::sync::mpsc;

use bamboo_llm::provider::LLMStream;
use bamboo_llm::types::LLMChunk;
use bamboo_metrics::{ForwardStatus, MetricsCollector};

use super::sse::{done_marker_bytes, error_chunk_bytes, openai_chunk_bytes};
use crate::handlers::llm_compat::usage::{build_estimated_usage, estimate_completion_tokens};

pub(super) struct StreamWorkerArgs {
    pub(super) stream_result: LLMStream,
    pub(super) tx: mpsc::Sender<Result<Bytes, anyhow::Error>>,
    pub(super) model: String,
    pub(super) metrics: MetricsCollector,
    pub(super) forward_id: String,
    pub(super) estimated_prompt_tokens: u64,
}

pub(super) fn spawn_stream_worker(args: StreamWorkerArgs) {
    tokio::spawn(async move {
        run_stream_worker(args).await;
    });
}

async fn run_stream_worker(mut args: StreamWorkerArgs) {
    let mut error_message: Option<String> = None;
    let mut saw_done = false;
    let mut streamed_text = String::new();

    while let Some(chunk_result) = args.stream_result.next().await {
        match chunk_result {
            Ok(LLMChunk::ResponseId(_)) => {}
            Ok(LLMChunk::Done) => {
                saw_done = true;
                if let Some(done_chunk) = openai_chunk_bytes(LLMChunk::Done, &args.model) {
                    if args.tx.send(Ok(done_chunk)).await.is_err() {
                        break;
                    }
                }
                break;
            }
            Ok(LLMChunk::Token(text)) => {
                streamed_text.push_str(&text);
                if let Some(chunk) = openai_chunk_bytes(LLMChunk::Token(text), &args.model) {
                    if args.tx.send(Ok(chunk)).await.is_err() {
                        break;
                    }
                }
            }
            Ok(LLMChunk::ReasoningToken(_)) => {}
            Ok(LLMChunk::ToolCalls(calls)) => {
                if let Some(chunk) = openai_chunk_bytes(LLMChunk::ToolCalls(calls), &args.model) {
                    if args.tx.send(Ok(chunk)).await.is_err() {
                        break;
                    }
                }
            }
            // Indexed variant: drop indices, re-serialize the same way. #236.
            Ok(LLMChunk::ToolCallsIndexed(indexed)) => {
                let calls = indexed.into_iter().map(|(_, call)| call).collect();
                if let Some(chunk) = openai_chunk_bytes(LLMChunk::ToolCalls(calls), &args.model) {
                    if args.tx.send(Ok(chunk)).await.is_err() {
                        break;
                    }
                }
            }
            Ok(LLMChunk::TransportActivity)
            | Ok(LLMChunk::CacheUsage { .. })
            | Ok(LLMChunk::ProviderUsage { .. })
            | Ok(LLMChunk::UsageSummary { .. })
            | Ok(LLMChunk::ReasoningSignature(_)) => {}
            Err(error) => {
                tracing::error!("Stream error: {}", error);
                let message = error.to_string();
                args.metrics.forward_completed(
                    args.forward_id.clone(),
                    chrono::Utc::now(),
                    None,
                    ForwardStatus::Error,
                    None,
                    Some(message.clone()),
                );
                error_message = Some(message);
                break;
            }
        }
    }

    if let Some(message) = error_message {
        // Surface the upstream error to the client instead of a clean `[DONE]`,
        // so a truncated response isn't mistaken for a complete one. Mirrors the
        // Anthropic streaming handler, which emits an SSE error event before end.
        let _ = args.tx.send(Ok(error_chunk_bytes(&message))).await;
        let _ = args.tx.send(Ok(done_marker_bytes())).await;
    } else {
        if !saw_done {
            if let Some(done_chunk) = openai_chunk_bytes(LLMChunk::Done, &args.model) {
                let _ = args.tx.send(Ok(done_chunk)).await;
            }
        }
        let _ = args.tx.send(Ok(done_marker_bytes())).await;

        let completion_tokens = estimate_completion_tokens(&streamed_text);
        args.metrics.forward_completed(
            args.forward_id,
            chrono::Utc::now(),
            Some(200),
            ForwardStatus::Success,
            Some(build_estimated_usage(
                args.estimated_prompt_tokens,
                completion_tokens,
            )),
            None,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_llm::provider::LLMError;
    use bamboo_metrics::storage::SqliteMetricsStorage;
    use futures::stream;
    use std::sync::Arc;

    fn test_metrics() -> (MetricsCollector, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(SqliteMetricsStorage::new(dir.path().join("metrics.db")));
        (MetricsCollector::spawn(storage, 7), dir)
    }

    #[tokio::test]
    async fn stream_error_emits_error_frame_before_done() {
        // A mid-stream upstream error must surface an error frame to the client,
        // not just a clean `[DONE]` — otherwise a truncated response reads as
        // complete. Some tokens have already been forwarded when the error hits.
        let (metrics, _dir) = test_metrics();
        let chunks: Vec<Result<LLMChunk, LLMError>> = vec![
            Ok(LLMChunk::Token("partial".to_string())),
            Err(LLMError::Stream("upstream boom".to_string())),
        ];
        let stream_result: LLMStream = Box::pin(stream::iter(chunks));
        let (tx, mut rx) = mpsc::channel(16);

        run_stream_worker(StreamWorkerArgs {
            stream_result,
            tx,
            model: "gpt-x".to_string(),
            metrics,
            forward_id: "fid".to_string(),
            estimated_prompt_tokens: 0,
        })
        .await;

        let mut frames = Vec::new();
        while let Ok(item) = rx.try_recv() {
            frames.push(String::from_utf8_lossy(&item.unwrap()).to_string());
        }

        assert!(
            frames
                .iter()
                .any(|f| f.contains("\"error\"") && f.contains("upstream boom")),
            "expected an error frame carrying the upstream message, got: {frames:?}"
        );
        // The stream still terminates with the [DONE] marker.
        assert_eq!(frames.last().map(String::as_str), Some("[DONE]"));
    }
}
