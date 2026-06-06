use bytes::Bytes;
use futures::StreamExt;
use tokio::sync::mpsc;

use bamboo_engine::{ForwardStatus, MetricsCollector};
use bamboo_infrastructure::provider::LLMStream;
use bamboo_infrastructure::types::LLMChunk;

use crate::handlers::llm_compat::usage::{build_estimated_usage, estimate_completion_tokens};
use super::sse::{done_marker_bytes, openai_chunk_bytes};

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
    let mut had_error = false;
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
            Ok(LLMChunk::CacheUsage { .. }) | Ok(LLMChunk::UsageSummary { .. }) => {}
            Err(error) => {
                tracing::error!("Stream error: {}", error);
                had_error = true;
                args.metrics.forward_completed(
                    args.forward_id.clone(),
                    chrono::Utc::now(),
                    None,
                    ForwardStatus::Error,
                    None,
                    Some(error.to_string()),
                );
                break;
            }
        }
    }

    if !had_error {
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
    } else {
        let _ = args.tx.send(Ok(done_marker_bytes())).await;
    }
}
