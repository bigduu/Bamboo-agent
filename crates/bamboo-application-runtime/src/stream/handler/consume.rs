use futures::StreamExt;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use bamboo_application_agent::{AgentError, AgentEvent};
use bamboo_infrastructure_llm::LLMStream;

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

    while let Some(chunk_result) = stream.next().await {
        if cancel_token.is_cancelled() {
            return Err(AgentError::Cancelled);
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
