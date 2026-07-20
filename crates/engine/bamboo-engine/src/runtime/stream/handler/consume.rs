use std::fmt;
use std::time::Duration;

use futures::StreamExt;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use bamboo_agent_core::{AgentError, AgentEvent};
use bamboo_llm::{LLMChunk, LLMStream};

use super::chunk_handling::handle_chunk_result;
use super::stream_state::StreamAccumulationState;
use super::{StreamHandlingOutput, StreamTimeoutContext};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamTimeoutPhase {
    TransportIdle,
    FirstSemantic,
    SemanticIdle,
}

impl fmt::Display for StreamTimeoutPhase {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::TransportIdle => "transport_idle",
            Self::FirstSemantic => "first_semantic",
            Self::SemanticIdle => "semantic_idle",
        })
    }
}

struct StreamTimeoutDiagnostic<'a> {
    phase: StreamTimeoutPhase,
    deadline: Duration,
    provider: Option<&'a str>,
    model: Option<&'a str>,
    transport_idle: Duration,
    semantic_idle: Option<Duration>,
    semantic_output_started: bool,
}

impl fmt::Display for StreamTimeoutDiagnostic<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let semantic_idle = self
            .semantic_idle
            .map(|duration| format!("{}ms", duration.as_millis()))
            .unwrap_or_else(|| "never".to_string());
        write!(
            formatter,
            "phase={}, deadline_ms={}, provider={}, model={}, last_transport_ms_ago={}, \
             last_semantic_ms_ago={}, semantic_output_started={}, retry_safe={}",
            self.phase,
            self.deadline.as_millis(),
            self.provider.unwrap_or("unknown"),
            self.model.unwrap_or("unknown"),
            self.transport_idle.as_millis(),
            semantic_idle,
            self.semantic_output_started,
            !self.semantic_output_started,
        )
    }
}

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
    let diagnostic = StreamTimeoutDiagnostic {
        phase,
        deadline,
        provider: timeout_context.provider.as_deref(),
        model: timeout_context.model.as_deref(),
        transport_idle: now.saturating_duration_since(last_transport_at),
        semantic_idle: last_semantic_at
            .map(|last_semantic| now.saturating_duration_since(last_semantic)),
        semantic_output_started: last_semantic_at.is_some(),
    };
    tracing::warn!(
        "[{}] LLM stream watchdog expired: {}",
        session_id,
        diagnostic,
    );
    AgentError::StreamTimeout(diagnostic.to_string())
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
    mut stream: LLMStream,
    event_tx: Option<&mpsc::Sender<AgentEvent>>,
    cancel_token: &CancellationToken,
    session_id: &str,
    timeout_context: &StreamTimeoutContext,
) -> Result<StreamHandlingOutput, AgentError> {
    let mut state = StreamAccumulationState::new();
    let policy = timeout_context.policy;
    let started_at = tokio::time::Instant::now();
    let mut last_transport_at = started_at;
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
            _ = cancel_token.cancelled() => return Err(AgentError::Cancelled),
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
                return Err(timeout_error(
                    timeout_context,
                    session_id,
                    phase,
                    deadline,
                    now,
                    last_transport_at,
                    last_semantic_at,
                ));
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
                return Err(timeout_error(
                    timeout_context,
                    session_id,
                    semantic_phase,
                    semantic_timeout,
                    now,
                    last_transport_at,
                    last_semantic_at,
                ));
            }
        }

        handle_chunk_result(chunk_result, &mut state, event_tx, session_id).await?;
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
