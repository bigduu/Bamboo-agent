use actix_web::{web, HttpResponse};
use futures::StreamExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::agent::llm::LLMRequestOptions;
use crate::server::{app_state::AppState, error::AppError};

use super::super::helpers::now_unix_ts;
use super::PreparedResponsesRequest;
use events::{created_event, event_to_sse_bytes};
use worker::{spawn_stream_worker, StreamWorkerArgs};

mod errors;
mod events;
#[cfg(test)]
mod tests;
mod worker;

pub(super) async fn handle_streaming_response(
    app_state: web::Data<AppState>,
    prepared: PreparedResponsesRequest,
    forward_id: String,
) -> Result<HttpResponse, AppError> {
    app_state.metrics_service.collector().forward_started(
        forward_id.clone(),
        "openai.responses",
        prepared.resolved_model.clone(),
        true,
        chrono::Utc::now(),
    );

    let provider = app_state.get_provider().await;
    let request_options = LLMRequestOptions {
        reasoning_effort: prepared.reasoning_effort,
        responses: Some(prepared.responses_options.clone()),
    };
    let stream_result = provider
        .chat_stream_with_options(
            &prepared.internal_messages,
            &prepared.internal_tools,
            prepared.max_tokens,
            prepared.resolved_model.as_str(),
            Some(&request_options),
        )
        .await
        .map_err(errors::map_provider_error)?;

    let (tx, rx) = mpsc::channel::<Result<bytes::Bytes, anyhow::Error>>(10);

    let response_id = format!("resp_{}", uuid::Uuid::new_v4());
    let message_id = format!("msg_{}", uuid::Uuid::new_v4());
    let created_at = now_unix_ts();

    // Send an initial response.created event (in_progress).
    let _ = tx
        .send(Ok(event_to_sse_bytes(&created_event(
            response_id.clone(),
            prepared.resolved_model.clone(),
            created_at,
        ))))
        .await;

    spawn_stream_worker(StreamWorkerArgs {
        stream_result,
        tx,
        metrics: app_state.metrics_service.collector(),
        forward_id,
        response_id,
        message_id,
        created_at,
        resolved_model: prepared.resolved_model,
        estimated_prompt_tokens: prepared.estimated_prompt_tokens,
    });

    let stream = ReceiverStream::new(rx).map(|result| result.map_err(AppError::InternalError));

    Ok(HttpResponse::Ok()
        .content_type("text/event-stream")
        .streaming(stream))
}
