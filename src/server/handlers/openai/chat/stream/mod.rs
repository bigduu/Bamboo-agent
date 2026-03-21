use actix_web::{web, HttpResponse};
use futures::StreamExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::server::{app_state::AppState, error::AppError};

use super::PreparedChatRequest;
use crate::agent::llm::LLMRequestOptions;
use sse::wrap_sse_data;
use worker::{spawn_stream_worker, StreamWorkerArgs};

mod sse;
#[cfg(test)]
mod tests;
mod worker;

pub(super) async fn handle_streaming_chat(
    app_state: web::Data<AppState>,
    prepared: PreparedChatRequest,
    forward_id: String,
) -> Result<HttpResponse, AppError> {
    let PreparedChatRequest {
        resolved_model,
        internal_messages,
        internal_tools,
        max_tokens,
        reasoning_effort,
        parallel_tool_calls,
        estimated_prompt_tokens,
        ..
    } = prepared;

    app_state.metrics_service.collector().forward_started(
        forward_id.clone(),
        "openai.chat_completions",
        resolved_model.clone(),
        true,
        chrono::Utc::now(),
    );
    let provider = app_state.get_provider().await;

    let stream_result = provider
        .chat_stream_with_options(
            &internal_messages,
            &internal_tools,
            max_tokens,
            resolved_model.as_str(),
            Some(&LLMRequestOptions {
                reasoning_effort,
                parallel_tool_calls,
                responses: None,
            }),
        )
        .await
        .map_err(super::map_provider_error)?;

    let (tx, rx) = mpsc::channel(10);

    spawn_stream_worker(StreamWorkerArgs {
        stream_result,
        tx,
        model: resolved_model,
        metrics: app_state.metrics_service.collector(),
        forward_id,
        estimated_prompt_tokens,
    });

    let stream = ReceiverStream::new(rx)
        .map(|result| result.map(wrap_sse_data).map_err(AppError::InternalError));

    Ok(HttpResponse::Ok()
        .content_type("text/event-stream")
        .streaming(stream))
}
