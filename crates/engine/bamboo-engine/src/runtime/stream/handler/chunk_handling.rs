use tokio::sync::mpsc;

use bamboo_agent_core::{AgentError, AgentEvent};
use bamboo_llm::{provider, LLMChunk};

use super::stream_state::StreamAccumulationState;

pub(super) async fn handle_chunk_result(
    chunk_result: provider::Result<LLMChunk>,
    state: &mut StreamAccumulationState,
    event_tx: Option<&mpsc::Sender<AgentEvent>>,
    session_id: &str,
) -> Result<(), AgentError> {
    match chunk_result {
        Ok(LLMChunk::TransportActivity) => Ok(()),
        Ok(LLMChunk::ResponseId(response_id)) => {
            tracing::debug!(
                "[{}] Received upstream response_id={}",
                session_id,
                response_id
            );
            state.set_response_id(response_id);
            Ok(())
        }
        Ok(LLMChunk::ResponsesEvent { .. }) => Ok(()),
        Ok(LLMChunk::Token(token)) => {
            state.append_token(&token);
            if let Some(event_tx) = event_tx {
                // `send().await` applies proper backpressure: it only yields
                // (waiting for capacity) while a subscriber is present, and
                // returns `Err` solely when the receiver has been dropped
                // (subscriber disconnected). A `try_send` or a bounded-timeout
                // send would *drop* the token under load — exactly the silent
                // loss this guards against — so the await is preserved. The
                // failure path (receiver gone) used to be swallowed by
                // `let _ =`; it now logs once per token only *after* the
                // subscriber is already gone (issue #23).
                if event_tx
                    .send(AgentEvent::Token { content: token })
                    .await
                    .is_err()
                {
                    tracing::warn!(
                        "[{}] event channel closed; dropping streamed token \
                         (subscriber disconnected)",
                        session_id,
                    );
                }
            }
            Ok(())
        }
        Ok(LLMChunk::ReasoningToken(token)) => {
            state.append_reasoning_token(&token);
            if let Some(event_tx) = event_tx {
                if event_tx
                    .send(AgentEvent::ReasoningToken { content: token })
                    .await
                    .is_err()
                {
                    tracing::warn!(
                        "[{}] event channel closed; dropping streamed reasoning token \
                         (subscriber disconnected)",
                        session_id,
                    );
                }
            }
            Ok(())
        }
        Ok(LLMChunk::ReasoningSignature(signature)) => {
            tracing::debug!(
                "[{}] Received reasoning signature (len={}, invalidation={})",
                session_id,
                signature.len(),
                signature.is_empty()
            );
            state.record_reasoning_signature(signature);
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
        Ok(LLMChunk::ToolCallsIndexed(partial_calls)) => {
            tracing::trace!(
                "[{}] Received {} indexed tool call parts",
                session_id,
                partial_calls.len()
            );
            state.extend_tool_calls_indexed(partial_calls);
            Ok(())
        }
        Ok(LLMChunk::Done) => {
            tracing::debug!("[{}] LLM stream completed", session_id);
            Ok(())
        }
        Ok(LLMChunk::CacheUsage {
            cache_creation_input_tokens,
            cache_read_input_tokens,
            input_tokens,
        }) => {
            tracing::debug!(
                "[{}] Cache usage: creation={}, read={}, input={}",
                session_id,
                cache_creation_input_tokens,
                cache_read_input_tokens,
                input_tokens
            );
            state.record_cache(
                cache_creation_input_tokens,
                cache_read_input_tokens,
                input_tokens,
            );
            Ok(())
        }
        Ok(LLMChunk::ProviderUsage {
            input_tokens,
            output_tokens,
            total_tokens,
            reasoning_tokens,
            cache_creation_input_tokens,
            cache_read_input_tokens,
            cache_write_input_tokens,
        }) => {
            tracing::debug!(
                "[{}] Provider usage: input={:?}, output={:?}, reasoning={:?}, \
                 total={:?}, cache_creation={:?}, cache_read={:?}, cache_write={:?}",
                session_id,
                input_tokens,
                output_tokens,
                reasoning_tokens,
                total_tokens,
                cache_creation_input_tokens,
                cache_read_input_tokens,
                cache_write_input_tokens
            );
            state.record_provider_usage(
                input_tokens,
                output_tokens,
                total_tokens,
                reasoning_tokens,
                cache_creation_input_tokens,
                cache_read_input_tokens,
                cache_write_input_tokens,
            );
            Ok(())
        }
        Ok(LLMChunk::UsageSummary {
            output_tokens,
            thinking_tokens,
        }) => {
            tracing::debug!(
                "[{}] Usage summary: output={}, thinking={}",
                session_id,
                output_tokens,
                thinking_tokens
            );
            state.record_usage(output_tokens, thinking_tokens);
            Ok(())
        }
        Err(error) => {
            let message = error.to_string();
            tracing::warn!("[{}] LLM stream error: {}", session_id, message);
            // Do not emit AgentEvent::Error here.
            // Round-level retry logic may recover from this transient stream failure.
            let _ = event_tx;
            Err(AgentError::LLM(message))
        }
    }
}
