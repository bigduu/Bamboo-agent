use bytes::Bytes;
use futures::StreamExt;
use tokio::sync::mpsc;

use bamboo_engine::{ForwardStatus, MetricsCollector};
use bamboo_infrastructure::provider::LLMStream;
use bamboo_infrastructure::types::LLMChunk;

use super::super::super::usage::{build_estimated_usage, estimate_completion_tokens};
use super::super::output::{build_completed_response, build_output_items};
use super::events::{
    completed_event, created_event, done_sse_bytes, event_to_sse_bytes, output_text_delta_event,
};

pub(super) struct StreamWorkerArgs {
    pub(super) stream_result: LLMStream,
    pub(super) tx: mpsc::Sender<Result<Bytes, anyhow::Error>>,
    pub(super) metrics: MetricsCollector,
    pub(super) forward_id: String,
    pub(super) fallback_response_id: String,
    pub(super) message_id: String,
    pub(super) created_at: u64,
    pub(super) resolved_model: String,
    pub(super) estimated_prompt_tokens: u64,
}

pub(super) fn spawn_stream_worker(args: StreamWorkerArgs) {
    tokio::spawn(async move {
        run_stream_worker(args).await;
    });
}

async fn run_stream_worker(mut args: StreamWorkerArgs) {
    let mut had_error = false;
    let mut content = String::new();
    let mut tool_calls: Vec<bamboo_agent_core::tools::ToolCall> = Vec::new();
    let mut response_id: Option<String> = None;
    let mut created_sent = false;

    async fn ensure_created_event(
        args: &mut StreamWorkerArgs,
        response_id: &str,
        created_sent: &mut bool,
    ) -> bool {
        if *created_sent {
            return true;
        }
        let event = created_event(
            response_id.to_string(),
            args.resolved_model.clone(),
            args.created_at,
        );
        if args.tx.send(Ok(event_to_sse_bytes(&event))).await.is_err() {
            return false;
        }
        *created_sent = true;
        true
    }

    while let Some(chunk_result) = args.stream_result.next().await {
        match chunk_result {
            Ok(LLMChunk::ResponseId(id)) => {
                response_id = Some(id.clone());
                if !ensure_created_event(&mut args, &id, &mut created_sent).await {
                    break;
                }
            }
            Ok(LLMChunk::Token(text)) => {
                content.push_str(&text);
                let active_response_id = response_id
                    .clone()
                    .unwrap_or_else(|| args.fallback_response_id.clone());
                if !ensure_created_event(&mut args, &active_response_id, &mut created_sent).await {
                    break;
                }
                let event = output_text_delta_event(&active_response_id, &args.message_id, text);
                if args.tx.send(Ok(event_to_sse_bytes(&event))).await.is_err() {
                    break;
                }
            }
            // Compatibility behavior: surface reasoning deltas in the same stream channel
            // so clients that only render output_text can still show interim narration.
            Ok(LLMChunk::ReasoningToken(text)) => {
                content.push_str(&text);
                let active_response_id = response_id
                    .clone()
                    .unwrap_or_else(|| args.fallback_response_id.clone());
                if !ensure_created_event(&mut args, &active_response_id, &mut created_sent).await {
                    break;
                }
                let event = output_text_delta_event(&active_response_id, &args.message_id, text);
                if args.tx.send(Ok(event_to_sse_bytes(&event))).await.is_err() {
                    break;
                }
            }
            Ok(LLMChunk::ToolCalls(calls)) => {
                let active_response_id = response_id
                    .clone()
                    .unwrap_or_else(|| args.fallback_response_id.clone());
                if !ensure_created_event(&mut args, &active_response_id, &mut created_sent).await {
                    break;
                }
                tool_calls.extend(calls)
            }
            Ok(LLMChunk::Done) => break,
            Ok(LLMChunk::CacheUsage { .. }) | Ok(LLMChunk::UsageSummary { .. }) => {}
            Err(error) => {
                had_error = true;
                tracing::error!("Stream error: {}", error);
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

    if had_error {
        let _ = args.tx.send(Ok(done_sse_bytes())).await;
        return;
    }

    let completion_tokens = estimate_completion_tokens(&content);
    let final_response_id = response_id.unwrap_or_else(|| args.fallback_response_id.clone());
    if !ensure_created_event(&mut args, &final_response_id, &mut created_sent).await {
        return;
    }
    let output = build_output_items(&args.message_id, content, tool_calls);
    let response = build_completed_response(
        final_response_id,
        args.created_at,
        args.resolved_model,
        output,
    );
    let complete = completed_event(response);

    let _ = args.tx.send(Ok(event_to_sse_bytes(&complete))).await;
    let _ = args.tx.send(Ok(done_sse_bytes())).await;

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
