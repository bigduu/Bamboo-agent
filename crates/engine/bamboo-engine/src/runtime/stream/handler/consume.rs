use std::time::Duration;

use futures::StreamExt;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use bamboo_agent_core::{AgentError, AgentEvent, StreamTimeoutPhase};
use bamboo_llm::{LLMChunk, LLMStream};

use super::chunk_handling::handle_chunk_result;
use super::stream_state::StreamAccumulationState;
use super::{StreamHandlingFailure, StreamHandlingOutput, StreamTimeoutContext};

fn timeout_duration(seconds: u64) -> Duration {
    Duration::from_secs(seconds)
}

fn classify_semantic_progress(chunk: &LLMChunk) -> bool {
    chunk.is_semantic_progress()
}

fn timeout_error(
    timeout_context: &StreamTimeoutContext,
    session_id: &str,
    phase: StreamTimeoutPhase,
    deadline: Duration,
    now: tokio::time::Instant,
    last_transport_at: tokio::time::Instant,
    last_semantic_at: Option<tokio::time::Instant>,
) -> AgentError {
    timeout_context.timeout_error(
        session_id,
        phase,
        deadline,
        now.saturating_duration_since(last_transport_at),
        last_semantic_at.map(|last_semantic| now.saturating_duration_since(last_semantic)),
    )
}

fn preview_for_log(value: &str, max_chars: usize) -> String {
    let mut iter = value.chars();
    let mut preview = String::new();
    for _ in 0..max_chars {
        match iter.next() {
            Some(ch) => preview.push(ch),
            None => break,
        }
    }
    if iter.next().is_some() {
        preview.push_str("...");
    }
    preview.replace('\n', "\\n").replace('\r', "\\r")
}

pub(super) async fn consume_llm_stream_internal(
    stream: LLMStream,
    event_tx: Option<&mpsc::Sender<AgentEvent>>,
    cancel_token: &CancellationToken,
    session_id: &str,
    timeout_context: &StreamTimeoutContext,
) -> Result<StreamHandlingOutput, AgentError> {
    consume_llm_stream_internal_with_partial(
        stream,
        event_tx,
        cancel_token,
        session_id,
        timeout_context,
    )
    .await
    .map_err(|failure| failure.error)
}

pub(super) async fn consume_llm_stream_internal_with_partial(
    mut stream: LLMStream,
    event_tx: Option<&mpsc::Sender<AgentEvent>>,
    cancel_token: &CancellationToken,
    session_id: &str,
    timeout_context: &StreamTimeoutContext,
) -> Result<StreamHandlingOutput, StreamHandlingFailure> {
    let mut state = StreamAccumulationState::new();
    let policy = timeout_context.policy;
    let stream_started_at = tokio::time::Instant::now();
    let started_at = timeout_context
        .request_started_at
        .unwrap_or(stream_started_at);
    let mut last_transport_at = stream_started_at;
    let mut last_semantic_at = None;

    loop {
        let transport_timeout = timeout_duration(policy.transport_idle_timeout_secs);
        let transport_deadline = last_transport_at + transport_timeout;
        let (semantic_phase, semantic_origin, semantic_timeout) = match last_semantic_at {
            Some(last_semantic) => (
                StreamTimeoutPhase::SemanticIdle,
                last_semantic,
                timeout_duration(policy.semantic_idle_timeout_secs),
            ),
            None => (
                StreamTimeoutPhase::FirstSemantic,
                started_at,
                timeout_duration(policy.first_semantic_timeout_secs),
            ),
        };
        let semantic_deadline = semantic_origin + semantic_timeout;
        let next_deadline = transport_deadline.min(semantic_deadline);

        // Cancellation wins, then a simultaneously-ready provider frame, then
        // the earliest independent watchdog deadline.
        let chunk_result = tokio::select! {
            biased;
            _ = cancel_token.cancelled() => {
                return Err(StreamHandlingFailure {
                    error: AgentError::Cancelled,
                    partial_output: Box::new(state.into_interrupted_output()),
                });
            },
            next = stream.next() => match next {
                Some(chunk_result) => chunk_result,
                None => break,
            },
            _ = tokio::time::sleep_until(next_deadline) => {
                let now = tokio::time::Instant::now();
                let (phase, deadline) = if transport_deadline <= semantic_deadline {
                    (StreamTimeoutPhase::TransportIdle, transport_timeout)
                } else {
                    (semantic_phase, semantic_timeout)
                };
                return Err(StreamHandlingFailure {
                    error: timeout_error(
                        timeout_context,
                        session_id,
                        phase,
                        deadline,
                        now,
                        last_transport_at,
                        last_semantic_at,
                    ),
                    partial_output: Box::new(state.into_interrupted_output()),
                });
            }
        };

        if let Ok(chunk) = &chunk_result {
            let now = tokio::time::Instant::now();
            last_transport_at = now;
            if classify_semantic_progress(chunk) {
                last_semantic_at = Some(now);
            } else if now >= semantic_deadline {
                // A synchronously-ready stream of keepalives must not starve the
                // semantic timer merely because `stream.next()` is intentionally
                // ordered before the timer in the biased select. A semantic
                // chunk ready at the boundary is accepted above and resets it.
                return Err(StreamHandlingFailure {
                    error: timeout_error(
                        timeout_context,
                        session_id,
                        semantic_phase,
                        semantic_timeout,
                        now,
                        last_transport_at,
                        last_semantic_at,
                    ),
                    partial_output: Box::new(state.into_interrupted_output()),
                });
            }
        }

        if let Err(error) =
            handle_chunk_result(chunk_result, &mut state, event_tx, session_id).await
        {
            return Err(StreamHandlingFailure {
                error,
                partial_output: Box::new(state.into_interrupted_output()),
            });
        }
    }

    let output = state.into_output();

    for tool_call in &output.tool_calls {
        let args = tool_call.function.arguments.trim();
        if args.is_empty() {
            tracing::debug!(
                "[{}] Finalized tool call with empty arguments: tool_call_id={}, tool_name={}",
                session_id,
                tool_call.id,
                tool_call.function.name
            );
            continue;
        }

        if let Err(error) = serde_json::from_str::<serde_json::Value>(args) {
            tracing::warn!(
                "[{}] Finalized tool call has invalid JSON arguments: tool_call_id={}, tool_name={}, args_len={}, args_preview=\"{}\", error={}",
                session_id,
                tool_call.id,
                tool_call.function.name,
                args.len(),
                preview_for_log(args, 180),
                error
            );
        } else {
            tracing::debug!(
                "[{}] Finalized tool call ready: tool_call_id={}, tool_name={}, args_len={}",
                session_id,
                tool_call.id,
                tool_call.function.name,
                args.len()
            );
        }
    }

    Ok(output)
}
