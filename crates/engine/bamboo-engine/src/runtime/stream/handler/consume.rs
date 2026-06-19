use std::time::Duration;

use futures::StreamExt;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use bamboo_agent_core::{AgentError, AgentEvent};
use bamboo_llm::LLMStream;

use super::chunk_handling::handle_chunk_result;
use super::stream_state::StreamAccumulationState;
use super::StreamHandlingOutput;

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
    // Inter-chunk idle deadline: the maximum gap allowed between two
    // consecutive stream chunks. The sleep is created fresh every loop
    // iteration, so this is a true *inter-chunk* deadline that resets on
    // every received chunk — NOT an overall stream cap. A legitimately long
    // stream that keeps producing chunks is never killed; only a stall with
    // no chunk for `idle_timeout` trips it (issue #28).
    idle_timeout: Duration,
) -> Result<StreamHandlingOutput, AgentError> {
    let mut state = StreamAccumulationState::new();

    loop {
        // Select on cancellation, the next chunk, *and* an inter-chunk idle
        // deadline so a stalled provider connection is bounded.
        //
        // `biased` checks branches top-to-bottom: cancellation first (so a
        // ready-but-cancelled chunk is dropped), then the pending chunk, then
        // the idle timer. The idle `sleep` is created fresh every iteration, so
        // it is a true *inter-chunk* deadline that resets on every received
        // chunk — NOT an overall stream cap. A legitimately long stream that
        // keeps producing chunks is never killed; only a stall with no chunk
        // for `idle_timeout` trips it. Without this branch,
        // `stream.next().await` blocks forever when the provider's connection
        // hangs mid-stream (issue #28). Listing `stream.next()` ahead of the
        // timer means a ready chunk always wins over the timer.
        let chunk_result = tokio::select! {
            biased;
            _ = cancel_token.cancelled() => return Err(AgentError::Cancelled),
            next = stream.next() => match next {
                Some(chunk_result) => chunk_result,
                None => break,
            },
            _ = tokio::time::sleep(idle_timeout) => {
                tracing::warn!(
                    "[{}] LLM stream timed out: no chunk received within {:?}; \
                     aborting stalled stream",
                    session_id,
                    idle_timeout,
                );
                return Err(AgentError::StreamTimeout(format!(
                    "the model stopped responding (no data received for over {idle_timeout:?})"
                )));
            }
        };

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
