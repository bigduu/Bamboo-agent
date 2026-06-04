use futures::StreamExt;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use bamboo_agent_core::{AgentError, AgentEvent};
use bamboo_infrastructure::LLMStream;

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
) -> Result<StreamHandlingOutput, AgentError> {
    let mut state = StreamAccumulationState::new();

    loop {
        // Select on cancellation *and* the next chunk so a stalled or
        // non-terminating provider stream is interruptible the instant the
        // token is cancelled — `stream.next().await` alone blocks forever and
        // the previous between-chunks `is_cancelled()` poll never ran. `biased`
        // checks cancellation first so a ready-but-cancelled chunk is dropped,
        // preserving the prior "return Cancelled without handling" semantics.
        let chunk_result = tokio::select! {
            biased;
            _ = cancel_token.cancelled() => return Err(AgentError::Cancelled),
            next = stream.next() => match next {
                Some(chunk_result) => chunk_result,
                None => break,
            },
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
