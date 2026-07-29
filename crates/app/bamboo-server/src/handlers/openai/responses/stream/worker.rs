use bytes::Bytes;
use futures::StreamExt;
use tokio::sync::mpsc;

use bamboo_llm::provider::LLMStream;
use bamboo_llm::types::LLMChunk;
use bamboo_metrics::{ForwardStatus, MetricsCollector};

use bamboo_agent_core::tools::ToolCallAccumulator;

use super::super::super::types::ResponsesOutputItem;
use super::super::output::{build_completed_response, build_output_items};
use super::events::{
    completed_event, created_event, done_sse_bytes, event_to_sse_bytes, failed_sse_bytes,
    function_call_item_events, message_item_added_event, message_item_done_event,
    output_text_delta_event,
};
use crate::handlers::llm_compat::usage::{build_estimated_usage, estimate_completion_tokens};

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
    let mut error_message: Option<String> = None;
    let mut content = String::new();
    // Providers stream PARTIAL tool-call fragments (argument-only continuation
    // chunks with empty id/name). Merge them by provider index / call id via
    // the engine's accumulator instead of collecting raw fragments — a raw Vec
    // shatters one call into N broken output items (#525).
    let mut tool_calls = ToolCallAccumulator::new();
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
        // The assistant message item (output_index 0) must be announced too —
        // clients that assemble output from output_item events would
        // otherwise never associate the text deltas with an item (#525).
        let added = message_item_added_event(response_id, &args.message_id);
        if args.tx.send(Ok(event_to_sse_bytes(&added))).await.is_err() {
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
            // Indexed variant: route fragments by provider index (#236/#525).
            Ok(LLMChunk::ToolCallsIndexed(indexed)) => {
                let active_response_id = response_id
                    .clone()
                    .unwrap_or_else(|| args.fallback_response_id.clone());
                if !ensure_created_event(&mut args, &active_response_id, &mut created_sent).await {
                    break;
                }
                tool_calls.extend_indexed(indexed)
            }
            Ok(LLMChunk::Done) => break,
            Ok(LLMChunk::TransportActivity)
            | Ok(LLMChunk::CacheUsage { .. })
            | Ok(LLMChunk::ProviderUsage { .. })
            | Ok(LLMChunk::UsageSummary { .. })
            | Ok(LLMChunk::ReasoningSignature(_)) => {}
            Err(error) => {
                had_error = true;
                error_message = Some(error.to_string());
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
        // Signal the failure so the client can tell a truncated stream from a
        // completed one, instead of a clean `[DONE]` that looks like success. #355.
        let message = error_message.as_deref().unwrap_or("upstream stream error");
        let _ = args.tx.send(Ok(failed_sse_bytes(message))).await;
        let _ = args.tx.send(Ok(done_sse_bytes())).await;
        return;
    }

    let completion_tokens = estimate_completion_tokens(&content);
    let final_response_id = response_id.unwrap_or_else(|| args.fallback_response_id.clone());
    if !ensure_created_event(&mut args, &final_response_id, &mut created_sent).await {
        return;
    }

    // Merged fragments whose name never arrived are dropped by finalize();
    // make that visible instead of silently losing a call attempt (#525).
    let fragment_groups = tool_calls.parts().len();
    let finalized_calls = tool_calls.finalize();
    if finalized_calls.len() < fragment_groups {
        tracing::warn!(
            dropped = fragment_groups - finalized_calls.len(),
            "Dropping incomplete streamed tool call(s) whose name never arrived"
        );
    }
    let output = build_output_items(&args.message_id, content, finalized_calls);

    // Emit the standard per-item event sequence before response.completed:
    // output_item.done for the message item, then per function call
    // output_item.added → function_call_arguments.delta/.done →
    // output_item.done — Responses clients (Codex) collect items from
    // output_item.done, not from the completed payload alone (#525).
    // build_output_items puts the message item at output_index 0 and function
    // calls after it, so enumerate() indices are already the output indices.
    // If the client disconnects mid-burst, stop sending but fall through to
    // the metrics accounting below.
    let mut client_gone = false;
    'items: for (output_index, item) in output.iter().enumerate() {
        let events = match item {
            ResponsesOutputItem::Message(message) => {
                vec![message_item_done_event(&final_response_id, message)]
            }
            ResponsesOutputItem::FunctionCall(fc) => {
                function_call_item_events(&final_response_id, fc, output_index as u32)
            }
        };
        for event in events {
            if args.tx.send(Ok(event_to_sse_bytes(&event))).await.is_err() {
                client_gone = true;
                break 'items;
            }
        }
    }

    if !client_gone {
        let response = build_completed_response(
            final_response_id,
            args.created_at,
            args.resolved_model,
            output,
        );
        let complete = completed_event(response);

        let _ = args.tx.send(Ok(event_to_sse_bytes(&complete))).await;
        let _ = args.tx.send(Ok(done_sse_bytes())).await;
    }

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
