use futures::StreamExt;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::agent::core::{AgentError, AgentEvent};
use crate::agent::llm::LLMStream;

use super::chunk_handling::handle_chunk_result;
use super::stream_state::StreamAccumulationState;
use super::StreamHandlingOutput;

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

    Ok(state.into_output())
}
