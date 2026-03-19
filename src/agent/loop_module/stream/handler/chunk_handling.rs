use tokio::sync::mpsc;

use crate::agent::core::{AgentError, AgentEvent};
use crate::agent::llm::{provider, LLMChunk};

use super::stream_state::StreamAccumulationState;

pub(super) async fn handle_chunk_result(
    chunk_result: provider::Result<LLMChunk>,
    state: &mut StreamAccumulationState,
    event_tx: Option<&mpsc::Sender<AgentEvent>>,
    session_id: &str,
) -> Result<(), AgentError> {
    match chunk_result {
        Ok(LLMChunk::ResponseId(response_id)) => {
            tracing::debug!("[{}] Received upstream response_id={}", session_id, response_id);
            state.set_response_id(response_id);
            Ok(())
        }
        Ok(LLMChunk::Token(token)) => {
            state.append_token(&token);
            if let Some(event_tx) = event_tx {
                let _ = event_tx.send(AgentEvent::Token { content: token }).await;
            }
            Ok(())
        }
        Ok(LLMChunk::ReasoningToken(token)) => {
            state.append_reasoning_token(&token);
            if let Some(event_tx) = event_tx {
                let _ = event_tx
                    .send(AgentEvent::ReasoningToken { content: token })
                    .await;
            }
            Ok(())
        }
        Ok(LLMChunk::ToolCalls(partial_calls)) => {
            tracing::trace!(
                "[{}] Received {} tool call parts",
                session_id,
                partial_calls.len()
            );
            state.extend_tool_calls(partial_calls);
            Ok(())
        }
        Ok(LLMChunk::Done) => {
            tracing::debug!("[{}] LLM stream completed", session_id);
            Ok(())
        }
        Err(error) => {
            if let Some(event_tx) = event_tx {
                let message = format!("Stream error: {error}");
                let _ = event_tx
                    .send(AgentEvent::Error {
                        message: message.clone(),
                    })
                    .await;
            }
            Err(AgentError::LLM(error.to_string()))
        }
    }
}
